package internal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/media-transcoder-pool/internal/transcodev1"
)

const defaultDispatchTimeoutSec = 48 * 3600 // match media-transcoder waitForPoolJob

// TranscoderSession is the minimal media-transcoder surface the pool dispatcher needs.
type TranscoderSession interface {
	Enqueue(ctx context.Context, input, output, profile string) (remoteJobID string, err error)
	GetJob(ctx context.Context, remoteJobID string) (status, errMsg string, err error)
	CancelJob(ctx context.Context, remoteJobID string) error
	Close() error
}

// TranscoderDialer opens a TranscoderSession to a worker grpc_addr.
type TranscoderDialer func(ctx context.Context, grpcAddr string) (TranscoderSession, error)

// GRPCTranscoderDialer dials a real media-transcoder TranscodeService (or compatible mock).
func GRPCTranscoderDialer(ctx context.Context, grpcAddr string) (TranscoderSession, error) {
	opts, err := meshGRPCDialOptions()
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(grpcAddr, opts...)
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

func (s *grpcTranscoderSession) CancelJob(ctx context.Context, remoteJobID string) error {
	_, err := s.client.CancelJob(ctx, &transcodev1.CancelJobRequest{JobId: remoteJobID})
	return err
}

func (s *grpcTranscoderSession) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

// Dispatcher forwards assigned pool jobs to worker TranscodeService endpoints.
type Dispatcher struct {
	store    *Store
	dialer   TranscoderDialer
	inflight map[string]struct{}
	poll     time.Duration
	wait     time.Duration
	timeout  time.Duration
	mu       sync.Mutex
}

// NewDispatcher creates a dispatcher. Defaults: poll 200ms, wait 250ms, timeout 48h.
func NewDispatcher(store *Store, dialer TranscoderDialer) *Dispatcher {
	if dialer == nil {
		dialer = GRPCTranscoderDialer
	}
	timeout := time.Duration(dispatchTimeoutFromEnv()) * time.Second
	return &Dispatcher{
		store:    store,
		dialer:   dialer,
		poll:     200 * time.Millisecond,
		wait:     250 * time.Millisecond,
		timeout:  timeout,
		inflight: make(map[string]struct{}),
	}
}

func dispatchTimeoutFromEnv() int64 {
	if v := os.Getenv("POOL_DISPATCH_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return defaultDispatchTimeoutSec
}

// SetTimeout updates the per-job dispatch wait deadline.
func (d *Dispatcher) SetTimeout(sec int64) {
	if sec <= 0 {
		sec = defaultDispatchTimeoutSec
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.timeout = time.Duration(sec) * time.Second
}

// DropInflight removes a job from the in-flight set (e.g. after cancel).
func (d *Dispatcher) DropInflight(jobID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inflight, jobID)
}

// CancelRemoteJob asks the worker TranscodeService to cancel a remote job.
func (d *Dispatcher) CancelRemoteJob(ctx context.Context, job *Job) error {
	if job == nil || job.RemoteJobID == "" || job.WorkerID == "" {
		return nil
	}
	w, err := d.store.GetWorker(ctx, job.WorkerID)
	if err != nil {
		return err
	}
	sess, err := d.dialer(ctx, w.GRPCAddr)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close() }()
	return sess.CancelJob(ctx, job.RemoteJobID)
}

// Run polls for assigned/running jobs until ctx is cancelled.
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

// Tick processes each assigned or running job once (idempotent via inflight set).
func (d *Dispatcher) Tick(ctx context.Context) {
	jobs := append(d.store.ListJobs(ctx, "assigned"), d.store.ListJobs(ctx, "running")...)
	for _, j := range jobs {
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
	fresh, err := d.store.GetJob(ctx, job.ID)
	if err != nil {
		return err
	}
	job = fresh
	if job.Status == "done" || job.Status == "cancelled" || job.Status == "failed" {
		return nil
	}
	if job.WorkerID == "" {
		return d.store.MarkJobFailed(ctx, job.ID, "no worker assigned")
	}
	w, err := d.store.GetWorker(ctx, job.WorkerID)
	if err != nil {
		return d.store.MarkJobFailed(ctx, job.ID, err.Error())
	}
	sess, err := d.dialer(ctx, w.GRPCAddr)
	if err != nil {
		return d.store.MarkJobFailed(ctx, job.ID, err.Error())
	}
	defer func() { _ = sess.Close() }()

	remoteID := job.RemoteJobID
	if remoteID == "" {
		if markErr := d.store.MarkJobRunning(ctx, job.ID); markErr != nil {
			return markErr
		}
		remoteID, err = sess.Enqueue(ctx, job.InputPath, job.OutputPath, job.Profile)
		if err != nil {
			return d.store.MarkJobFailed(ctx, job.ID, err.Error())
		}
		if err := d.store.SetJobRemoteID(ctx, job.ID, remoteID); err != nil {
			return err
		}
	}

	d.mu.Lock()
	timeout := d.timeout
	d.mu.Unlock()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.wait):
		}
		cur, err := d.store.GetJob(ctx, job.ID)
		if err != nil {
			return err
		}
		if cur.Status == "cancelled" {
			return nil
		}
		status, errMsg, err := sess.GetJob(ctx, remoteID)
		if err != nil {
			return d.store.MarkJobFailed(ctx, job.ID, err.Error())
		}
		switch status {
		case "completed", "done":
			return d.store.MarkJobDone(ctx, job.ID)
		case "failed", "cancelled":
			if errMsg == "" {
				errMsg = status
			}
			return d.store.MarkJobFailed(ctx, job.ID, errMsg)
		}
	}
	return d.store.MarkJobFailed(ctx, job.ID, "worker job timed out")
}
