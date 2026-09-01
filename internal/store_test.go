package internal_test

import (
	"context"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Muxcore-Media/media-transcoder-pool/internal"
	poolv1 "github.com/Muxcore-Media/media-transcoder-pool/proto/gen/muxcore/transcoderpool/v1"
)

func openTempStore(t *testing.T) *internal.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pool.db")
	s, err := internal.OpenStore(context.Background(), path, 60)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestAssignPrefersGPU(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)
	cpu, err := s.RegisterWorker(ctx, internal.Worker{NodeID: "n1", GRPCAddr: ":9525", Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	gpu, err := s.RegisterWorker(ctx, internal.Worker{NodeID: "n2", GRPCAddr: ":9526", GPU: true, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Enqueue(ctx, internal.Job{
		InputPath: "/in.mkv", OutputPath: "/out.mkv", PreferGPU: true, Profile: "hevc_gpu",
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "assigned" || job.WorkerID != gpu.ID {
		t.Fatalf("job=%+v cpu=%s gpu=%s", job, cpu.ID, gpu.ID)
	}
	workers := s.ListWorkers(ctx, true)
	if len(workers) != 1 || workers[0].ActiveJobs != 1 {
		t.Fatalf("%+v", workers)
	}
}

func TestQueueWhenNoCapacity(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)
	_, err := s.RegisterWorker(ctx, internal.Worker{GRPCAddr: ":1", Capacity: 1, ActiveJobs: 0})
	if err != nil {
		t.Fatal(err)
	}
	j1, err := s.Enqueue(ctx, internal.Job{InputPath: "a", OutputPath: "b"})
	if err != nil {
		t.Fatal(err)
	}
	j2, err := s.Enqueue(ctx, internal.Job{InputPath: "c", OutputPath: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if j1.Status != "assigned" {
		t.Fatalf("j1=%+v", j1)
	}
	if j2.Status != "queued" {
		t.Fatalf("j2=%+v", j2)
	}
	if err := s.CancelJob(ctx, j1.ID); err != nil {
		t.Fatal(err)
	}
	j2After, err := s.GetJob(ctx, j2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j2After.Status != "assigned" {
		t.Fatalf("j2 not promoted after j1 cancel: %+v", j2After)
	}
}

func TestMarkJobDonePromotesQueued(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)
	w, err := s.RegisterWorker(ctx, internal.Worker{GRPCAddr: ":1", Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	j1, err := s.Enqueue(ctx, internal.Job{InputPath: "a", OutputPath: "b"})
	if err != nil {
		t.Fatal(err)
	}
	j2, err := s.Enqueue(ctx, internal.Job{InputPath: "c", OutputPath: "d"})
	if err != nil {
		t.Fatal(err)
	}
	if j2.Status != "queued" {
		t.Fatalf("j2=%+v", j2)
	}
	if err := s.MarkJobDone(ctx, j1.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, j2.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "assigned" || got.WorkerID != w.ID {
		t.Fatalf("j2 not promoted: %+v worker=%s", got, w.ID)
	}
}

func TestHeartbeatDoesNotResetActiveJobs(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)
	w, err := s.RegisterWorker(ctx, internal.Worker{ID: "w1", GRPCAddr: ":1", Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(ctx, internal.Job{InputPath: "a", OutputPath: "b"}); err != nil {
		t.Fatal(err)
	}
	workers := s.ListWorkers(ctx, false)
	if len(workers) != 1 || workers[0].ActiveJobs != 1 {
		t.Fatalf("before heartbeat: %+v", workers)
	}
	if err := s.Heartbeat(ctx, w.ID, 0); err != nil {
		t.Fatal(err)
	}
	workers = s.ListWorkers(ctx, false)
	if len(workers) != 1 || workers[0].ActiveJobs != 1 {
		t.Fatalf("heartbeat must not overwrite active_jobs: %+v", workers)
	}
}

func TestResumeRunningJobAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pool.db")
	s1, err := internal.OpenStore(ctx, path, 60)
	if err != nil {
		t.Fatal(err)
	}
	w, err := s1.RegisterWorker(ctx, internal.Worker{GRPCAddr: "127.0.0.1:1", Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s1.Enqueue(ctx, internal.Job{InputPath: "in", OutputPath: "out"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.MarkJobRunning(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s1.SetJobRemoteID(ctx, job.ID, "remote-abc"); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := internal.OpenStore(ctx, path, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err := s2.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "running" || got.RemoteJobID != "remote-abc" || got.WorkerID != w.ID {
		t.Fatalf("job not durable for resume: %+v", got)
	}
	running := s2.ListJobs(ctx, "running")
	if len(running) != 1 || running[0].ID != job.ID {
		t.Fatalf("list running: %+v", running)
	}
}

func TestUnregisterWorkerRequeuesJobs(t *testing.T) {
	ctx := context.Background()
	s := openTempStore(t)
	w, err := s.RegisterWorker(ctx, internal.Worker{ID: "w1", GRPCAddr: ":1", Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Enqueue(ctx, internal.Job{InputPath: "a", OutputPath: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UnregisterWorker(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "queued" || got.WorkerID != "" {
		t.Fatalf("job not requeued: %+v", got)
	}
}

func TestDurableJobQueueSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pool.db")
	s1, err := internal.OpenStore(ctx, path, 60)
	if err != nil {
		t.Fatal(err)
	}
	w, err := s1.RegisterWorker(ctx, internal.Worker{
		ID: "tw_cpu1", NodeID: "node-a", GRPCAddr: "127.0.0.1:19001", Capacity: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s1.Enqueue(ctx, internal.Job{
		InputPath: "/media/in.mkv", OutputPath: "/media/out.mkv", Profile: "h264_fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "assigned" || job.WorkerID != w.ID {
		t.Fatalf("expected assigned to %s, got %+v", w.ID, job)
	}
	jobID := job.ID
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := internal.OpenStore(ctx, path, 60)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	got, err := s2.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "assigned" || got.WorkerID != w.ID || got.InputPath != "/media/in.mkv" {
		t.Fatalf("job not durable: %+v", got)
	}
	workers := s2.ListWorkers(ctx, false)
	if len(workers) != 1 || workers[0].ID != w.ID || workers[0].ActiveJobs != 1 {
		t.Fatalf("workers not durable: %+v", workers)
	}
	queued := s2.ListJobs(ctx, "assigned")
	if len(queued) != 1 || queued[0].ID != jobID {
		t.Fatalf("list jobs: %+v", queued)
	}
}

func TestFakeWorkerRegisterHeartbeatEnqueue(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "pool.db")
	m := internal.NewModule(internal.Config{
		DBPath:          dbPath,
		GRPCAddr:        "127.0.0.1:0",
		HTTPAddr:        "127.0.0.1:0",
		DisableDispatch: true,
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	conn, err := grpc.NewClient(m.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := poolv1.NewTranscoderPoolServiceClient(conn)

	// Fake CPU worker — no GPU farm.
	reg, err := client.RegisterWorker(ctx, &poolv1.RegisterWorkerRequest{
		Id:       "fake-cpu-1",
		NodeId:   "laptop",
		GrpcAddr: "127.0.0.1:19999",
		Gpu:      false,
		Capacity: 2,
		Labels:   []string{"cpu", "fake"},
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if reg.Worker.GetId() != "fake-cpu-1" || reg.Worker.GetGpu() || reg.Worker.GetStatus() != "online" {
		t.Fatalf("unexpected worker: %+v", reg.Worker)
	}

	hb, err := client.Heartbeat(ctx, &poolv1.HeartbeatRequest{Id: "fake-cpu-1", ActiveJobs: 0})
	if err != nil || !hb.GetOk() {
		t.Fatalf("Heartbeat: ok=%v err=%v", hb.GetOk(), err)
	}

	enq, err := client.Enqueue(ctx, &poolv1.EnqueueRequest{
		InputPath:  "/fixtures/clip.mkv",
		OutputPath: "/tmp/clip.mp4",
		Profile:    "h264_fast",
		PreferGpu:  false,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job := enq.GetJob()
	if job.GetStatus() != "assigned" || job.GetWorkerId() != "fake-cpu-1" {
		t.Fatalf("expected assigned to fake-cpu-1, got %+v", job)
	}

	got, err := client.GetJob(ctx, &poolv1.GetJobRequest{Id: job.GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Job.GetWorkerId() != "fake-cpu-1" {
		t.Fatalf("GetJob: %+v", got.Job)
	}

	workers, err := client.ListWorkers(ctx, &poolv1.ListWorkersRequest{GpuOnly: false})
	if err != nil {
		t.Fatalf("ListWorkers: %v", err)
	}
	if len(workers.Workers) != 1 || workers.Workers[0].GetActiveJobs() != 1 {
		t.Fatalf("workers=%+v", workers.Workers)
	}
}
