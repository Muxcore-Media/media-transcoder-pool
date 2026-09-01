package internal_test

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Muxcore-Media/media-transcoder-pool/internal"
	poolv1 "github.com/Muxcore-Media/media-transcoder-pool/proto/gen/muxcore/transcoderpool/v1"
)

func TestModuleHealthz(t *testing.T) {
	ctx := context.Background()
	m := internal.NewModule(internal.Config{
		DBPath:          filepath.Join(t.TempDir(), "pool.db"),
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

	if err := m.Health(ctx); err != nil {
		t.Fatalf("Health: %v", err)
	}
	resp, err := http.Get("http://" + m.HTTPAddr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status=%d", resp.StatusCode)
	}
}

func TestCancelJobStopsRemoteWorker(t *testing.T) {
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	worker, err := internal.StartOfflineTranscoder("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Stop)

	m := internal.NewModule(internal.Config{
		DBPath:   filepath.Join(t.TempDir(), "pool.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
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

	if _, err := client.RegisterWorker(ctx, &poolv1.RegisterWorkerRequest{
		Id: "local-1", GrpcAddr: worker.Addr(), Capacity: 1,
	}); err != nil {
		t.Fatal(err)
	}
	enq, err := client.Enqueue(ctx, &poolv1.EnqueueRequest{
		InputPath: "/in.mkv", OutputPath: "/out.mkv", Profile: "h264_fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	jobID := enq.GetJob().GetId()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, err := m.Store().GetJob(ctx, jobID)
		if err != nil {
			t.Fatal(err)
		}
		if got.RemoteJobID != "" {
			if _, err := client.CancelJob(ctx, &poolv1.CancelJobRequest{Id: jobID}); err != nil {
				t.Fatal(err)
			}
			final, err := m.Store().GetJob(ctx, jobID)
			if err != nil {
				t.Fatal(err)
			}
			if final.Status != "cancelled" {
				t.Fatalf("expected cancelled, got %+v", final)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for remote job id")
}
