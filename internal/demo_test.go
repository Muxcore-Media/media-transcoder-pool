package internal_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Muxcore-Media/media-transcoder-pool/internal"
	poolv1 "github.com/Muxcore-Media/media-transcoder-pool/proto/gen/muxcore/transcoderpool/v1"
)

// TestLocalSingleWorkerDemo wires pool + offline media-transcoder mock (no GPU, no FFmpeg).
func TestLocalSingleWorkerDemo(t *testing.T) {
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	worker, err := internal.StartOfflineTranscoder("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Stop)
	worker.CompleteAfter = 50 * time.Millisecond

	dbPath := filepath.Join(t.TempDir(), "pool.db")
	m := internal.NewModule(internal.Config{
		DBPath: dbPath, GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0",
	})
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })

	conn, err := grpc.NewClient(m.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := poolv1.NewTranscoderPoolServiceClient(conn)

	reg, err := client.RegisterWorker(ctx, &poolv1.RegisterWorkerRequest{
		Id: "local-1", NodeId: "laptop", GrpcAddr: worker.Addr(),
		Gpu: false, Capacity: 1, Labels: []string{"cpu", "offline-mock"},
	})
	if err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if reg.Worker.GetStatus() != "online" {
		t.Fatalf("worker status: %+v", reg.Worker)
	}
	if _, err := client.Heartbeat(ctx, &poolv1.HeartbeatRequest{Id: "local-1", ActiveJobs: 0}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	enq, err := client.Enqueue(ctx, &poolv1.EnqueueRequest{
		InputPath: "/fixtures/clip.mkv", OutputPath: "/tmp/clip.mp4",
		Profile: "h264_fast", PreferGpu: false,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	jobID := enq.GetJob().GetId()
	if enq.GetJob().GetWorkerId() != "local-1" || enq.GetJob().GetStatus() != "assigned" {
		t.Fatalf("expected assigned to local-1, got %+v", enq.GetJob())
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err := client.GetJob(ctx, &poolv1.GetJobRequest{Id: jobID})
		if err != nil {
			t.Fatal(err)
		}
		switch got.Job.GetStatus() {
		case "done":
			workers, err := client.ListWorkers(ctx, &poolv1.ListWorkersRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(workers.Workers) != 1 || workers.Workers[0].GetActiveJobs() != 0 {
				t.Fatalf("capacity not released: %+v", workers.Workers)
			}
			return
		case "failed":
			t.Fatalf("job failed: %+v", got.Job)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %s to complete", jobID)
}
