// Command local-demo runs a laptop-only single-worker pool demo:
// pool coordinator + offline media-transcoder mock (no GPU, no FFmpeg).
//
//	go run ./cmd/local-demo
//
// Optional: POINT_AT_REAL=1 with MEDIA_TRANSCODER_ADDR=host:port to register a
// real sibling media-transcoder instead of the offline mock (still no cloud GPU).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Muxcore-Media/media-transcoder-pool/internal"
	poolv1 "github.com/Muxcore-Media/media-transcoder-pool/proto/gen/muxcore/transcoderpool/v1"
)

func main() {
	if err := run(); err != nil {
		slog.Error("local-demo failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerAddr := os.Getenv("MEDIA_TRANSCODER_ADDR")
	var offline *internal.OfflineTranscoder
	if workerAddr == "" {
		w, err := internal.StartOfflineTranscoder("127.0.0.1:0")
		if err != nil {
			return err
		}
		offline = w
		defer offline.Stop()
		offline.CompleteAfter = 100 * time.Millisecond
		workerAddr = offline.Addr()
		slog.Info("started offline media-transcoder mock", "addr", workerAddr)
	} else {
		slog.Info("using real media-transcoder", "addr", workerAddr)
	}

	dir, err := os.MkdirTemp("", "mtp-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	m := internal.NewModule(internal.Config{
		DBPath:   filepath.Join(dir, "pool.db"),
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		Dispatch: true,
	})
	if err := m.Init(ctx); err != nil {
		return err
	}
	if err := m.Start(ctx); err != nil {
		return err
	}
	defer m.Stop(context.Background())

	conn, err := grpc.NewClient(m.GRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	client := poolv1.NewTranscoderPoolServiceClient(conn)

	if _, err := client.RegisterWorker(ctx, &poolv1.RegisterWorkerRequest{
		Id: "demo-worker-1", NodeId: "laptop", GrpcAddr: workerAddr,
		Gpu: false, Capacity: 1, Labels: []string{"cpu", "local-demo"},
	}); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	if _, err := client.Heartbeat(ctx, &poolv1.HeartbeatRequest{Id: "demo-worker-1"}); err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}

	enq, err := client.Enqueue(ctx, &poolv1.EnqueueRequest{
		InputPath:  "/fixtures/demo-clip.mkv",
		OutputPath: filepath.Join(dir, "out.mp4"),
		Profile:    "h264_fast",
	})
	if err != nil {
		return fmt.Errorf("enqueue: %w", err)
	}
	jobID := enq.GetJob().GetId()
	slog.Info("enqueued", "job", jobID, "status", enq.GetJob().GetStatus(), "worker", enq.GetJob().GetWorkerId())

	for {
		got, err := client.GetJob(ctx, &poolv1.GetJobRequest{Id: jobID})
		if err != nil {
			return err
		}
		st := got.Job.GetStatus()
		slog.Info("job poll", "job", jobID, "status", st)
		switch st {
		case "done":
			fmt.Println("OK: local single-worker demo completed")
			return nil
		case "failed", "cancelled":
			return fmt.Errorf("job %s ended as %s: %s", jobID, st, got.Job.GetError())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
