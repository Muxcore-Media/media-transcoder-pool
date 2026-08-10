package internal

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	poolv1 "github.com/Muxcore-Media/media-transcoder-pool/proto/gen/muxcore/transcoderpool/v1"
)

type Module struct {
	id, grpcAddr, httpAddr string
	staleAfterSec          int64
	cfgMu                  sync.RWMutex
	store                  *Store
	grpcSrv                *grpc.Server
	lis                    net.Listener
	httpSrv                *http.Server
}

type Config struct {
	ID, GRPCAddr, HTTPAddr string
	StaleAfterSec          int64
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
	if v := os.Getenv("POOL_STALE_AFTER_SEC"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.StaleAfterSec = n
		}
	}
	if v := os.Getenv("MUXCORE_HTTP_ADDR"); v != "" {
		cfg.HTTPAddr = v
	}
	return &Module{
		id: cfg.ID, grpcAddr: cfg.GRPCAddr, httpAddr: cfg.HTTPAddr,
		staleAfterSec: cfg.StaleAfterSec, store: NewStore(cfg.StaleAfterSec),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID: m.id, Name: "Transcoder Pool", Version: "0.1.0",
		Roles:        []string{"media", "transcode", "pool"},
		Description:  "Distributed transcoding pool coordinator (GPU/CPU workers on the mesh)",
		Capabilities: []string{"media.transcode.pool", "transcoder.pool", "settings"},
		HTTPAddr:     m.grpcAddr,
	}
}

func (m *Module) Init(ctx context.Context) error { return nil }

func (m *Module) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", m.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen gRPC %s: %w", m.grpcAddr, err)
	}
	m.lis = lis
	m.grpcSrv = grpc.NewServer()
	poolv1.RegisterTranscoderPoolServiceServer(m.grpcSrv, &poolServer{m: m})
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)
	go func() {
		slog.Info("transcoder-pool gRPC listening", "addr", m.grpcAddr)
		if err := m.grpcSrv.Serve(lis); err != nil {
			slog.Error("gRPC serve", "error", err)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	m.httpSrv = &http.Server{Addr: m.httpAddr, Handler: mux}
	go func() {
		slog.Info("health listening", "addr", m.httpAddr)
		if err := m.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health serve", "error", err)
		}
	}()
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	if m.httpSrv != nil {
		_ = m.httpSrv.Shutdown(ctx)
	}
	return nil
}

func (m *Module) Health(ctx context.Context) error { return nil }

type poolServer struct {
	poolv1.UnimplementedTranscoderPoolServiceServer
	m *Module
}

func (s *poolServer) RegisterWorker(_ context.Context, req *poolv1.RegisterWorkerRequest) (*poolv1.RegisterWorkerResponse, error) {
	w, err := s.m.store.RegisterWorker(Worker{
		ID: req.GetId(), NodeID: req.GetNodeId(), GRPCAddr: req.GetGrpcAddr(),
		GPU: req.GetGpu(), Capacity: req.GetCapacity(), Labels: req.GetLabels(),
	})
	if err != nil {
		return nil, err
	}
	return &poolv1.RegisterWorkerResponse{Worker: toPBWorker(w)}, nil
}

func (s *poolServer) Heartbeat(_ context.Context, req *poolv1.HeartbeatRequest) (*poolv1.HeartbeatResponse, error) {
	if err := s.m.store.Heartbeat(req.GetId(), req.GetActiveJobs()); err != nil {
		return nil, err
	}
	return &poolv1.HeartbeatResponse{Ok: true}, nil
}

func (s *poolServer) UnregisterWorker(_ context.Context, req *poolv1.UnregisterWorkerRequest) (*poolv1.UnregisterWorkerResponse, error) {
	if err := s.m.store.UnregisterWorker(req.GetId()); err != nil {
		return nil, err
	}
	return &poolv1.UnregisterWorkerResponse{Success: true}, nil
}

func (s *poolServer) ListWorkers(_ context.Context, req *poolv1.ListWorkersRequest) (*poolv1.ListWorkersResponse, error) {
	items := s.m.store.ListWorkers(req.GetGpuOnly())
	out := make([]*poolv1.Worker, 0, len(items))
	for _, w := range items {
		out = append(out, toPBWorker(w))
	}
	return &poolv1.ListWorkersResponse{Workers: out}, nil
}

func (s *poolServer) Enqueue(_ context.Context, req *poolv1.EnqueueRequest) (*poolv1.EnqueueResponse, error) {
	j, err := s.m.store.Enqueue(Job{
		InputPath: req.GetInputPath(), OutputPath: req.GetOutputPath(),
		Profile: req.GetProfile(), PreferGPU: req.GetPreferGpu(),
	})
	if err != nil {
		return nil, err
	}
	return &poolv1.EnqueueResponse{Job: toPBJob(j)}, nil
}

func (s *poolServer) GetJob(_ context.Context, req *poolv1.GetJobRequest) (*poolv1.GetJobResponse, error) {
	j, err := s.m.store.GetJob(req.GetId())
	if err != nil {
		return nil, err
	}
	return &poolv1.GetJobResponse{Job: toPBJob(j)}, nil
}

func (s *poolServer) ListJobs(_ context.Context, req *poolv1.ListJobsRequest) (*poolv1.ListJobsResponse, error) {
	items := s.m.store.ListJobs(req.GetStatus())
	out := make([]*poolv1.Job, 0, len(items))
	for _, j := range items {
		out = append(out, toPBJob(j))
	}
	return &poolv1.ListJobsResponse{Jobs: out}, nil
}

func (s *poolServer) CancelJob(_ context.Context, req *poolv1.CancelJobRequest) (*poolv1.CancelJobResponse, error) {
	if err := s.m.store.CancelJob(req.GetId()); err != nil {
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
