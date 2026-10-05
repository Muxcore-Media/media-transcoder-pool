package internal

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/sdk/go/module/meshtls"
	"google.golang.org/protobuf/proto"

	"github.com/Muxcore-Media/media-transcoder-pool/internal/transcodev1"
)

// OfflineTranscoder is a laptop/CI stand-in for media-transcoder (no FFmpeg, no GPU).
// It speaks the same TranscodeService RPCs the pool dispatcher dials.
type OfflineTranscoder struct {
	transcodev1.UnimplementedTranscodeServiceServer
	lis           net.Listener
	jobs          map[string]*transcodev1.TranscodeJob
	srv           *grpc.Server
	addr          string
	nextID        atomic.Int64
	CompleteAfter time.Duration
	mu            sync.Mutex
}

// StartOfflineTranscoder listens on addr (use "127.0.0.1:0" for an ephemeral port).
func StartOfflineTranscoder(addr string) (*OfflineTranscoder, error) {
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", addr)
	if err != nil {
		return nil, err
	}
	srvOpt, err := meshtls.ServerOption()
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	t := &OfflineTranscoder{
		jobs: make(map[string]*transcodev1.TranscodeJob),
		lis:  lis,
		addr: lis.Addr().String(),
		srv:  grpc.NewServer(srvOpt),
	}
	transcodev1.RegisterTranscodeServiceServer(t.srv, t)
	go func() { _ = t.srv.Serve(lis) }()
	return t, nil
}

// Addr returns the bound gRPC address.
func (t *OfflineTranscoder) Addr() string { return t.addr }

// Stop stops the gRPC server.
func (t *OfflineTranscoder) Stop() {
	if t.srv != nil {
		t.srv.GracefulStop()
	}
}

func (t *OfflineTranscoder) Enqueue(_ context.Context, req *transcodev1.EnqueueRequest) (*transcodev1.EnqueueResponse, error) {
	if req.GetInputPath() == "" || req.GetOutputPath() == "" {
		return nil, fmt.Errorf("input_path and output_path required")
	}
	id := fmt.Sprintf("mock_tj_%d", t.nextID.Add(1))
	now := time.Now().UTC().Format(time.RFC3339)
	job := &transcodev1.TranscodeJob{
		Id: id, InputPath: req.GetInputPath(), OutputPath: req.GetOutputPath(),
		ProfileId: req.GetProfileId(), ProfileName: req.GetProfileId(),
		Status: "running", Progress: 0.5, CreatedAt: now, UpdatedAt: now, StartedAt: now,
	}
	t.mu.Lock()
	t.jobs[id] = job
	t.mu.Unlock()

	delay := t.CompleteAfter
	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		t.mu.Lock()
		defer t.mu.Unlock()
		if j, ok := t.jobs[id]; ok && j.Status == "running" {
			j.Status = "completed"
			j.Progress = 1
			j.CompletedAt = time.Now().UTC().Format(time.RFC3339)
			j.UpdatedAt = j.CompletedAt
		}
	}()
	return &transcodev1.EnqueueResponse{JobId: id}, nil
}

func (t *OfflineTranscoder) GetJob(_ context.Context, req *transcodev1.GetJobRequest) (*transcodev1.GetJobResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	j, ok := t.jobs[req.GetJobId()]
	if !ok {
		return nil, fmt.Errorf("job not found: %s", req.GetJobId())
	}
	cloned, ok := proto.Clone(j).(*transcodev1.TranscodeJob)
	if !ok {
		return nil, fmt.Errorf("clone job failed")
	}
	return &transcodev1.GetJobResponse{Job: cloned}, nil
}

func (t *OfflineTranscoder) CancelJob(_ context.Context, req *transcodev1.CancelJobRequest) (*transcodev1.CancelJobResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	j, ok := t.jobs[req.GetJobId()]
	if !ok {
		return nil, fmt.Errorf("job not found: %s", req.GetJobId())
	}
	j.Status = "cancelled"
	j.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return &transcodev1.CancelJobResponse{}, nil
}

func (t *OfflineTranscoder) ListProfiles(context.Context, *transcodev1.ListProfilesRequest) (*transcodev1.ListProfilesResponse, error) {
	return &transcodev1.ListProfilesResponse{Profiles: []*transcodev1.TranscodeProfile{{
		Id: "h264_fast", Name: "H.264 Fast", VideoCodec: "h264", AudioCodec: "copy", Preset: "fast", Crf: 23, Container: "mkv",
	}}}, nil
}
