package internal

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Worker struct {
	ID                 string
	NodeID             string
	GRPCAddr           string
	GPU                bool
	Capacity           int32
	ActiveJobs         int32
	Labels             []string
	LastHeartbeatUnix  int64
	Status             string
}

type Job struct {
	ID          string
	InputPath   string
	OutputPath  string
	Profile     string
	PreferGPU   bool
	WorkerID    string
	Status      string
	Error       string
	CreatedUnix int64
	UpdatedUnix int64
}

type Store struct {
	mu              sync.RWMutex
	workers         map[string]*Worker
	jobs            map[string]*Job
	staleAfterSec   int64
}

func NewStore(staleAfterSec int64) *Store {
	if staleAfterSec <= 0 {
		staleAfterSec = 60
	}
	return &Store{
		workers:       map[string]*Worker{},
		jobs:          map[string]*Job{},
		staleAfterSec: staleAfterSec,
	}
}

func (s *Store) RegisterWorker(w Worker) (*Worker, error) {
	if strings.TrimSpace(w.GRPCAddr) == "" {
		return nil, fmt.Errorf("grpc_addr required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.ID == "" {
		w.ID = "tw_" + uuid.NewString()[:8]
	}
	if w.Capacity <= 0 {
		w.Capacity = 1
	}
	now := time.Now().Unix()
	w.LastHeartbeatUnix = now
	w.Status = "online"
	cp := w
	s.workers[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *Store) Heartbeat(id string, active int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workers[id]
	if !ok {
		return fmt.Errorf("worker %q not found", id)
	}
	w.ActiveJobs = active
	w.LastHeartbeatUnix = time.Now().Unix()
	w.Status = "online"
	return nil
}

func (s *Store) UnregisterWorker(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workers[id]; !ok {
		return fmt.Errorf("worker %q not found", id)
	}
	delete(s.workers, id)
	return nil
}

func (s *Store) refreshStatusesLocked(now int64) {
	for _, w := range s.workers {
		age := now - w.LastHeartbeatUnix
		switch {
		case age > s.staleAfterSec*2:
			w.Status = "offline"
		case age > s.staleAfterSec:
			w.Status = "stale"
		default:
			w.Status = "online"
		}
	}
}

func (s *Store) ListWorkers(gpuOnly bool) []*Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	s.refreshStatusesLocked(now)
	out := make([]*Worker, 0, len(s.workers))
	for _, w := range s.workers {
		if gpuOnly && !w.GPU {
			continue
		}
		cp := *w
		out = append(out, &cp)
	}
	return out
}

func (s *Store) Enqueue(j Job) (*Job, error) {
	if strings.TrimSpace(j.InputPath) == "" || strings.TrimSpace(j.OutputPath) == "" {
		return nil, fmt.Errorf("input_path and output_path required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	s.refreshStatusesLocked(now)
	if j.ID == "" {
		j.ID = "tj_" + uuid.NewString()[:8]
	}
	if j.Profile == "" {
		j.Profile = "h264_fast"
	}
	j.CreatedUnix = now
	j.UpdatedUnix = now
	j.Status = "queued"

	workerID := s.pickWorkerLocked(j.PreferGPU)
	if workerID != "" {
		j.WorkerID = workerID
		j.Status = "assigned"
		s.workers[workerID].ActiveJobs++
	}
	cp := j
	s.jobs[cp.ID] = &cp
	out := cp
	return &out, nil
}

func (s *Store) pickWorkerLocked(preferGPU bool) string {
	var bestID string
	var bestLoad float64 = 1e9
	for id, w := range s.workers {
		if w.Status != "online" {
			continue
		}
		if preferGPU && !w.GPU {
			continue
		}
		if w.ActiveJobs >= w.Capacity {
			continue
		}
		load := float64(w.ActiveJobs) / float64(w.Capacity)
		// Prefer GPU workers when available even if preferGPU is false (lighter load preference).
		if w.GPU {
			load -= 0.01
		}
		if load < bestLoad {
			bestLoad = load
			bestID = id
		}
	}
	if preferGPU && bestID == "" {
		// fall back to any online CPU worker
		return s.pickWorkerLocked(false)
	}
	return bestID
}

func (s *Store) GetJob(id string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %q not found", id)
	}
	cp := *j
	return &cp, nil
}

func (s *Store) ListJobs(status string) []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if status != "" && !strings.EqualFold(j.Status, status) {
			continue
		}
		cp := *j
		out = append(out, &cp)
	}
	return out
}

func (s *Store) CancelJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	if j.Status == "done" || j.Status == "cancelled" {
		return nil
	}
	if j.WorkerID != "" {
		if w, ok := s.workers[j.WorkerID]; ok && w.ActiveJobs > 0 {
			w.ActiveJobs--
		}
	}
	j.Status = "cancelled"
	j.UpdatedUnix = time.Now().Unix()
	return nil
}
