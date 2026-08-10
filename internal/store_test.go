package internal_test

import (
	"testing"

	"github.com/Muxcore-Media/media-transcoder-pool/internal"
)

func TestAssignPrefersGPU(t *testing.T) {
	s := internal.NewStore(60)
	cpu, err := s.RegisterWorker(internal.Worker{NodeID: "n1", GRPCAddr: ":9525", Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	gpu, err := s.RegisterWorker(internal.Worker{NodeID: "n2", GRPCAddr: ":9526", GPU: true, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Enqueue(internal.Job{
		InputPath: "/in.mkv", OutputPath: "/out.mkv", PreferGPU: true, Profile: "hevc_gpu",
	})
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != "assigned" || job.WorkerID != gpu.ID {
		t.Fatalf("job=%+v cpu=%s gpu=%s", job, cpu.ID, gpu.ID)
	}
	workers := s.ListWorkers(true)
	if len(workers) != 1 || workers[0].ActiveJobs != 1 {
		t.Fatalf("%+v", workers)
	}
}

func TestQueueWhenNoCapacity(t *testing.T) {
	s := internal.NewStore(60)
	_, err := s.RegisterWorker(internal.Worker{GRPCAddr: ":1", Capacity: 1, ActiveJobs: 0})
	if err != nil {
		t.Fatal(err)
	}
	j1, _ := s.Enqueue(internal.Job{InputPath: "a", OutputPath: "b"})
	j2, _ := s.Enqueue(internal.Job{InputPath: "c", OutputPath: "d"})
	if j1.Status != "assigned" {
		t.Fatalf("j1=%+v", j1)
	}
	if j2.Status != "queued" {
		t.Fatalf("j2=%+v", j2)
	}
	if err := s.CancelJob(j1.ID); err != nil {
		t.Fatal(err)
	}
}
