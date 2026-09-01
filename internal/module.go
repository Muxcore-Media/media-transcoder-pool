package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/Muxcore-Media/core/sdk/go/client"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	poolv1 "github.com/Muxcore-Media/media-transcoder-pool/proto/gen/muxcore/transcoderpool/v1"
)

type Module struct {
	lis                net.Listener
	store              *Store
	grpcSrv            *grpc.Server
	httpSrv            *http.Server
	disp               *Dispatcher
	dispCancel         context.CancelFunc
	meshCancel         context.CancelFunc
	mc                 *client.Client
	id                 string
	grpcAddr           string
	httpAddr           string
	dbPath             string
	staleAfterSec      int64
	dispatchTimeoutSec int64
	cfgMu              sync.RWMutex
	dispatch           bool
	mu                 sync.Mutex
}

type Config struct {
	ID, GRPCAddr, HTTPAddr, DBPath string
	StaleAfterSec                  int64
	Dispatch                       bool // forward assigned jobs to worker TranscodeService
	DispatchTimeoutSec             int64
	DisableDispatch                bool // test hook; POOL_DISPATCH env overrides when set
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "media-transcoder-pool"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9720"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":9721"
	}
	if cfg.StaleAfterSec <= 0 {
		cfg.StaleAfterSec = 60
	}
	if cfg.DispatchTimeoutSec <= 0 {
		cfg.DispatchTimeoutSec = defaultDispatchTimeoutSec
	}
	dispatch := true
	if cfg.DisableDispatch {
		dispatch = false
	}
	if v := os.Getenv("POOL_DISPATCH"); v == "0" || v == "false" {
		dispatch = false
	} else if v == "1" || v == "true" {
		dispatch = true
	}
	if v := os.Getenv("POOL_STALE_AFTER_SEC"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.StaleAfterSec = n
		}
	}
	if v := os.Getenv("POOL_DISPATCH_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			cfg.DispatchTimeoutSec = n
		}
	}
	if v := os.Getenv("POOL_DB_PATH"); v != "" {
		cfg.DBPath = v
	}
	if v := os.Getenv("MUXCORE_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	if cfg.DBPath == "" {
		cfg.DBPath = filepath.Join("data", "pool.db")
	}
	return &Module{
		id: cfg.ID, grpcAddr: cfg.GRPCAddr, httpAddr: cfg.HTTPAddr,
		dbPath: cfg.DBPath, staleAfterSec: cfg.StaleAfterSec,
		dispatch: dispatch, dispatchTimeoutSec: cfg.DispatchTimeoutSec,
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID: m.id, Name: "Transcoder Pool", Version: "0.2.0",
		Roles:        []string{"media", "transcode", "pool"},
		Description:  "Distributed transcoding pool coordinator (GPU/CPU workers on the mesh)",
		Capabilities: []string{"media.transcode.pool", "transcoder.pool", "settings"},
		HTTPAddr:     m.grpcAddr,
	}
}

func (m *Module) Init(ctx context.Context) error {
	store, err := OpenStore(ctx, m.dbPath, m.staleAfterSec)
	if err != nil {
		return err
	}
	m.store = store
	slog.Info("transcoder-pool durable store open", "db", store.Path())
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	if m.store == nil {
		return fmt.Errorf("store not initialized")
	}
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", m.grpcAddr, err)
	}
	m.lis = lis
	m.grpcAddr = lis.Addr().String()
	m.grpcSrv = grpc.NewServer()
	poolv1.RegisterTranscoderPoolServiceServer(m.grpcSrv, &poolServer{m: m})
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)
	go func() {
		slog.Info("transcoder-pool gRPC listening", "addr", m.grpcAddr)
		if serveErr := m.grpcSrv.Serve(lis); serveErr != nil {
			slog.Error("gRPC serve", "error", serveErr)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", m.handleHealthz)
	httpLis, err := lc.Listen(ctx, "tcp", m.httpAddr)
	if err != nil {
		m.grpcSrv.GracefulStop()
		return fmt.Errorf("listen HTTP %s: %w", m.httpAddr, err)
	}
	m.httpAddr = httpLis.Addr().String()
	m.httpSrv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("health listening", "addr", m.httpAddr)
		if serveErr := m.httpSrv.Serve(httpLis); serveErr != nil && serveErr != http.ErrServerClosed {
			slog.Error("health serve", "error", serveErr)
		}
	}()
	if m.dispatch {
		m.startDispatcher(ctx)
	}
	meshCtx, meshCancel := context.WithCancel(ctx)
	m.meshCancel = meshCancel
	go m.dialCoreLoop(meshCtx)
	return nil
}

func (m *Module) startDispatcher(ctx context.Context) {
	if m.dispCancel != nil {
		return
	}
	dctx, cancel := context.WithCancel(ctx)
	m.dispCancel = cancel
	disp := NewDispatcher(m.store, GRPCTranscoderDialer)
	disp.SetTimeout(m.dispatchTimeoutSec)
	m.disp = disp
	go disp.Run(dctx)
	slog.Info("transcoder-pool dispatcher enabled", "timeout_sec", m.dispatchTimeoutSec)
}

func (m *Module) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := m.Health(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (m *Module) Stop(ctx context.Context) error {
	if m.meshCancel != nil {
		m.meshCancel()
		m.meshCancel = nil
	}
	if m.dispCancel != nil {
		m.dispCancel()
		m.dispCancel = nil
	}
	m.mu.Lock()
	if m.mc != nil {
		_ = m.mc.Close()
		m.mc = nil
	}
	m.mu.Unlock()
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.httpSrv != nil {
		_ = m.httpSrv.Shutdown(ctx)
	}
	if m.store != nil {
		_ = m.store.Close()
		m.store = nil
	}
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	if m.store == nil {
		return fmt.Errorf("store not initialized")
	}
	return m.store.Ping(ctx)
}

// GRPCAddr returns the bound gRPC listen address.
func (m *Module) GRPCAddr() string { return m.grpcAddr }

// HTTPAddr returns the bound HTTP health listen address.
func (m *Module) HTTPAddr() string { return m.httpAddr }

// Store returns the durable store (nil before Init).
func (m *Module) Store() *Store { return m.store }

// Dispatcher returns the job dispatcher (nil when dispatch is disabled).
func (m *Module) Dispatcher() *Dispatcher { return m.disp }

type poolServer struct {
	poolv1.UnimplementedTranscoderPoolServiceServer
	m *Module
}

func (s *poolServer) RegisterWorker(ctx context.Context, req *poolv1.RegisterWorkerRequest) (*poolv1.RegisterWorkerResponse, error) {
	w, err := s.m.store.RegisterWorker(ctx, Worker{
		ID: req.GetId(), NodeID: req.GetNodeId(), GRPCAddr: req.GetGrpcAddr(),
		GPU: req.GetGpu(), Capacity: req.GetCapacity(), Labels: req.GetLabels(),
	})
	if err != nil {
		return nil, err
	}
	if s.m.dispatch && s.m.disp == nil {
		s.m.startDispatcher(ctx)
	}
	return &poolv1.RegisterWorkerResponse{Worker: toPBWorker(w)}, nil
}

func (s *poolServer) Heartbeat(ctx context.Context, req *poolv1.HeartbeatRequest) (*poolv1.HeartbeatResponse, error) {
	if err := s.m.store.Heartbeat(ctx, req.GetId(), req.GetActiveJobs()); err != nil {
		return nil, err
	}
	return &poolv1.HeartbeatResponse{Ok: true}, nil
}

func (s *poolServer) UnregisterWorker(ctx context.Context, req *poolv1.UnregisterWorkerRequest) (*poolv1.UnregisterWorkerResponse, error) {
	if err := s.m.store.UnregisterWorker(ctx, req.GetId()); err != nil {
		return nil, err
	}
	return &poolv1.UnregisterWorkerResponse{Success: true}, nil
}

func (s *poolServer) ListWorkers(ctx context.Context, req *poolv1.ListWorkersRequest) (*poolv1.ListWorkersResponse, error) {
	items := s.m.store.ListWorkers(ctx, req.GetGpuOnly())
	out := make([]*poolv1.Worker, 0, len(items))
	for _, w := range items {
		out = append(out, toPBWorker(w))
	}
	return &poolv1.ListWorkersResponse{Workers: out}, nil
}

func (s *poolServer) Enqueue(ctx context.Context, req *poolv1.EnqueueRequest) (*poolv1.EnqueueResponse, error) {
	j, err := s.m.store.Enqueue(ctx, Job{
		InputPath: req.GetInputPath(), OutputPath: req.GetOutputPath(),
		Profile: req.GetProfile(), PreferGPU: req.GetPreferGpu(),
	})
	if err != nil {
		return nil, err
	}
	return &poolv1.EnqueueResponse{Job: toPBJob(j)}, nil
}

func (s *poolServer) GetJob(ctx context.Context, req *poolv1.GetJobRequest) (*poolv1.GetJobResponse, error) {
	j, err := s.m.store.GetJob(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &poolv1.GetJobResponse{Job: toPBJob(j)}, nil
}

func (s *poolServer) ListJobs(ctx context.Context, req *poolv1.ListJobsRequest) (*poolv1.ListJobsResponse, error) {
	items := s.m.store.ListJobs(ctx, req.GetStatus())
	out := make([]*poolv1.Job, 0, len(items))
	for _, j := range items {
		out = append(out, toPBJob(j))
	}
	return &poolv1.ListJobsResponse{Jobs: out}, nil
}

func (s *poolServer) CancelJob(ctx context.Context, req *poolv1.CancelJobRequest) (*poolv1.CancelJobResponse, error) {
	id := req.GetId()
	j, err := s.m.store.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	if s.m.disp != nil {
		if j.RemoteJobID != "" && j.WorkerID != "" && (j.Status == "running" || j.Status == "assigned") {
			if err := s.m.disp.CancelRemoteJob(ctx, j); err != nil {
				slog.Warn("transcoder-pool: cancel remote job failed", "job", id, "error", err)
			}
		}
		s.m.disp.DropInflight(id)
	}
	if err := s.m.store.CancelJob(ctx, id); err != nil {
		return nil, err
	}
	return &poolv1.CancelJobResponse{Success: true}, nil
}

func toPBWorker(w *Worker) *poolv1.Worker {
	return &poolv1.Worker{
		Id: w.ID, NodeId: w.NodeID, GrpcAddr: w.GRPCAddr, Gpu: w.GPU,
		Capacity: w.Capacity, ActiveJobs: w.ActiveJobs, Labels: w.Labels,
		LastHeartbeatUnix: w.LastHeartbeatUnix, Status: w.Status,
	}
}

func toPBJob(j *Job) *poolv1.Job {
	return &poolv1.Job{
		Id: j.ID, InputPath: j.InputPath, OutputPath: j.OutputPath, Profile: j.Profile,
		PreferGpu: j.PreferGPU, WorkerId: j.WorkerID, Status: j.Status, Error: j.Error,
		CreatedUnix: j.CreatedUnix, UpdatedUnix: j.UpdatedUnix,
	}
}
