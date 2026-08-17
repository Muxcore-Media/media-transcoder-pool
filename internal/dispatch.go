package internal

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Muxcore-Media/media-transcoder-pool/internal/transcodev1"
)

// TranscoderSession is the minimal media-transcoder surface the pool dispatcher needs.
type TranscoderSession interface {
	Enqueue(ctx context.Context, input, output, profile string) (remoteJobID string, err error)
	GetJob(ctx context.Context, remoteJobID string) (status, errMsg string, err error)
	Close() error
}

// TranscoderDialer opens a TranscoderSession to a worker grpc_addr.
type TranscoderDialer func(ctx context.Context, grpcAddr string) (TranscoderSession, error)

// GRPCTranscoderDialer dials a real media-transcoder TranscodeService (or compatible mock).
func GRPCTranscoderDialer(ctx context.Context, grpcAddr string) (TranscoderSession, error) {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial transcoder %s: %w", grpcAddr, err)
	}
	return &grpcTranscoderSession{conn: conn, client: transcodev1.NewTranscodeServiceClient(conn)}, nil
}

type grpcTranscoderSession struct {
	conn   *grpc.ClientConn
	client transcodev1.TranscodeServiceClient
}

func (s *grpcTranscoderSession) Enqueue(ctx context.Context, input, output, profile string) (string, error) {
	if profile == "" {
		profile = "h264_fast"
	}
	resp, err := s.client.Enqueue(ctx, &transcodev1.EnqueueRequest{
		InputPath: input, OutputPath: output, ProfileId: profile,
	})
	if err != nil {
		return "", err
	}
	return resp.GetJobId(), nil
}

func (s *grpcTranscoderSession) GetJob(ctx context.Context, remoteJobID string) (string, string, error) {
	resp, err := s.client.GetJob(ctx, &transcodev1.GetJobRequest{JobId: remoteJobID})
	if err != nil {
		return "", "", err
	}
	j := resp.GetJob()
	if j == nil {
		return "", "", fmt.Errorf("empty job")
	}
	return j.GetStatus(), j.GetError(), nil
}

func (s *grpcTranscoderSession) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

// Dispatcher forwards assigned pool jobs to worker TranscodeService endpoints.
type Dispatcher struct {
	store  *Store
	dialer TranscoderDialer
	poll   time.Duration
	wait   time.Duration

	mu       sync.Mutex
	inflight map[string]struct{}
}

// NewDispatcher creates a dispatcher. Defaults: poll 200ms, wait poll 250ms.
func NewDispatcher(store *Store, dialer TranscoderDialer) *Dispatcher {
	if dialer == nil {
		dialer = GRPCTranscoderDialer
	}
	return &Dispatcher{
		store:    store,
		dialer:   dialer,
		poll:     200 * time.Millisecond,
		wait:     250 * time.Millisecond,
		inflight: make(map[string]struct{}),
	}
}

// Run polls for assigned jobs until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	t := time.NewTicker(d.poll)
	defer t.Stop()
	for {
		d.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick processes each currently assigned job once (idempotent via inflight set).
func (d *Dispatcher) Tick(ctx context.Context) {
	for _, j := range d.store.ListJobs("assigned") {
		d.mu.Lock()
		_, busy := d.inflight[j.ID]
		if !busy {
			d.inflight[j.ID] = struct{}{}
		}
		d.mu.Unlock()
		if busy {
			continue
		}
		job := *j
		go func() {
			defer func() {
				d.mu.Lock()
				delete(d.inflight, job.ID)
				d.mu.Unlock()
			}()
			if err := d.dispatchOne(ctx, &job); err != nil {
				slog.Warn("dispatch job failed", "job", job.ID, "error", err)
			}
		}()
	}
}

func (d *Dispatcher) dispatchOne(ctx context.Context, job *Job) error {
	if job.WorkerID == "" {
		return d.store.MarkJobFailed(job.ID, "no worker assigned")
	}
	w, err := d.store.GetWorker(job.WorkerID)
	if err != nil {
		return d.store.MarkJobFailed(job.ID, err.Error())
	}
	sess, err := d.dialer(ctx, w.GRPCAddr)
	if err != nil {
		return d.store.MarkJobFailed(job.ID, err.Error())
	}
	defer func() { _ = sess.Close() }()

	if err := d.store.MarkJobRunning(job.ID); err != nil {
		return err
	}
	remoteID, err := sess.Enqueue(ctx, job.InputPath, job.OutputPath, job.Profile)
	if err != nil {
		return d.store.MarkJobFailed(job.ID, err.Error())
	}

	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.wait):
		}
		status, errMsg, err := sess.GetJob(ctx, remoteID)
		if err != nil {
			return d.store.MarkJobFailed(job.ID, err.Error())
		}
		switch status {
		case "completed", "done":
			return d.store.MarkJobDone(job.ID)
		case "failed", "cancelled":
			if errMsg == "" {
				errMsg = status
			}
			return d.store.MarkJobFailed(job.ID, errMsg)
		}
	}
	return d.store.MarkJobFailed(job.ID, "worker job timed out")
}
