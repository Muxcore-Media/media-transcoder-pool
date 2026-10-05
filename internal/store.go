package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type Worker struct {
	ID                string
	NodeID            string
	GRPCAddr          string
	Status            string
	Labels            []string
	LastHeartbeatUnix int64
	Capacity          int32
	ActiveJobs        int32
	GPU               bool
}

type Job struct {
	ID          string
	InputPath   string
	OutputPath  string
	Profile     string
	WorkerID    string
	RemoteJobID string
	Status      string
	Error       string
	CreatedUnix int64
	UpdatedUnix int64
	PreferGPU   bool
}

// Store is a durable SQLite-backed worker registry and job queue.
type Store struct {
	db            *sql.DB
	path          string
	staleAfterSec int64
	mu            sync.Mutex
}

// OpenStore opens or creates a SQLite database at path (WAL mode).
func OpenStore(ctx context.Context, path string, staleAfterSec int64) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("db path required")
	}
	if staleAfterSec <= 0 {
		staleAfterSec = 60
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	s := &Store{db: db, path: path, staleAfterSec: staleAfterSec}
	if err := s.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS workers (
			id TEXT PRIMARY KEY,
			node_id TEXT NOT NULL DEFAULT '',
			grpc_addr TEXT NOT NULL,
			gpu INTEGER NOT NULL DEFAULT 0,
			capacity INTEGER NOT NULL DEFAULT 1,
			active_jobs INTEGER NOT NULL DEFAULT 0,
			labels TEXT NOT NULL DEFAULT '[]',
			last_heartbeat_unix INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL DEFAULT 'online'
		);
		CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			input_path TEXT NOT NULL,
			output_path TEXT NOT NULL,
			profile TEXT NOT NULL DEFAULT 'h264_fast',
			prefer_gpu INTEGER NOT NULL DEFAULT 0,
			worker_id TEXT NOT NULL DEFAULT '',
			remote_job_id TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'queued',
			error TEXT NOT NULL DEFAULT '',
			created_unix INTEGER NOT NULL DEFAULT 0,
			updated_unix INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
		CREATE INDEX IF NOT EXISTS idx_workers_status ON workers(status);
	`)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := s.addColumnIfMissing(ctx, "jobs", "remote_job_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	return nil
}

func (s *Store) addColumnIfMissing(ctx context.Context, table, column, def string) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("pragma table_info %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, def)); err != nil {
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
}

// Path returns the SQLite file path.
func (s *Store) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// Ping verifies the SQLite connection is alive.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store closed")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.PingContext(ctx)
}

// Close closes the database.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// SetStaleAfterSec updates the heartbeat stale threshold without resetting data.
func (s *Store) SetStaleAfterSec(n int64) {
	if s == nil || n <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.staleAfterSec = n
}

func (s *Store) RegisterWorker(ctx context.Context, w Worker) (*Worker, error) {
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
	if w.Labels == nil {
		w.Labels = []string{}
	}
	now := time.Now().Unix()
	labels, err := json.Marshal(w.Labels)
	if err != nil {
		return nil, fmt.Errorf("marshal labels: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO workers (id, node_id, grpc_addr, gpu, capacity, active_jobs, labels, last_heartbeat_unix, status)
		VALUES (?, ?, ?, ?, ?, 0, ?, ?, 'online')
		ON CONFLICT(id) DO UPDATE SET
			node_id = excluded.node_id,
			grpc_addr = excluded.grpc_addr,
			gpu = excluded.gpu,
			capacity = excluded.capacity,
			labels = excluded.labels,
			last_heartbeat_unix = excluded.last_heartbeat_unix,
			status = 'online'
	`, w.ID, w.NodeID, w.GRPCAddr, boolToInt(w.GPU), w.Capacity, string(labels), now)
	if err != nil {
		return nil, fmt.Errorf("upsert worker: %w", err)
	}
	out, err := s.getWorkerLocked(ctx, w.ID, now)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) Heartbeat(ctx context.Context, id string, _ int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `
		UPDATE workers SET last_heartbeat_unix = ?, status = 'online' WHERE id = ?
	`, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("worker %q not found", id)
	}
	return nil
}

func (s *Store) UnregisterWorker(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, requeueErr := s.requeueWorkerJobsLocked(ctx, tx, id, false); requeueErr != nil {
		return requeueErr
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM workers WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("unregister: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("worker %q not found", id)
	}
	return tx.Commit()
}

// SweepStaleWorkers requeues assigned/running jobs owned by stale or offline workers.
func (s *Store) SweepStaleWorkers(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, last_heartbeat_unix FROM workers
	`)
	if err != nil {
		return 0, fmt.Errorf("list workers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	requeued := 0
	for rows.Next() {
		var id string
		var lastHB int64
		if err := rows.Scan(&id, &lastHB); err != nil {
			return requeued, err
		}
		st := statusFor(lastHB, now, s.staleAfterSec)
		if st != "stale" && st != "offline" {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return requeued, err
		}
		n, err := s.requeueWorkerJobsLocked(ctx, tx, id, true)
		if err != nil {
			_ = tx.Rollback()
			return requeued, err
		}
		if err := tx.Commit(); err != nil {
			return requeued, err
		}
		if n > 0 {
			requeued++
		}
	}
	return requeued, rows.Err()
}

func (s *Store) requeueWorkerJobsLocked(ctx context.Context, tx *sql.Tx, workerID string, promote bool) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id FROM jobs
		WHERE worker_id = ? AND status IN ('assigned', 'running')
		ORDER BY created_unix ASC, id ASC
	`, workerID)
	if err != nil {
		return 0, fmt.Errorf("list worker jobs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	jobIDs := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		jobIDs = append(jobIDs, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(jobIDs) == 0 {
		return 0, nil
	}
	now := time.Now().Unix()
	for _, id := range jobIDs {
		if _, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status = 'queued', worker_id = '', remote_job_id = '', updated_unix = ?
			WHERE id = ?
		`, now, id); err != nil {
			return 0, fmt.Errorf("requeue job %q: %w", id, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE workers SET active_jobs = CASE WHEN active_jobs > ? THEN active_jobs - ? ELSE 0 END
		WHERE id = ?
	`, len(jobIDs), len(jobIDs), workerID); err != nil {
		return 0, fmt.Errorf("release worker load: %w", err)
	}
	if promote {
		if err := s.promoteQueuedJobsLocked(ctx, tx, now); err != nil {
			return 0, err
		}
	}
	return len(jobIDs), nil
}

func (s *Store) ListWorkers(ctx context.Context, gpuOnly bool) []*Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, node_id, grpc_addr, gpu, capacity, active_jobs, labels, last_heartbeat_unix, status
		FROM workers ORDER BY id
	`)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	out := make([]*Worker, 0)
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return out
		}
		w.Status = statusFor(w.LastHeartbeatUnix, now, s.staleAfterSec)
		if gpuOnly && !w.GPU {
			continue
		}
		out = append(out, w)
	}
	return out
}

func (s *Store) Enqueue(ctx context.Context, j Job) (*Job, error) {
	if strings.TrimSpace(j.InputPath) == "" || strings.TrimSpace(j.OutputPath) == "" {
		return nil, fmt.Errorf("input_path and output_path required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	if j.ID == "" {
		j.ID = "tj_" + uuid.NewString()[:8]
	}
	if j.Profile == "" {
		j.Profile = "h264_fast"
	}
	j.CreatedUnix = now
	j.UpdatedUnix = now
	j.Status = "queued"
	j.WorkerID = ""

	workerID, err := s.pickWorkerLocked(ctx, nil, j.PreferGPU, now)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if workerID != "" {
		j.WorkerID = workerID
		j.Status = "assigned"
		if _, bumpErr := tx.ExecContext(ctx, `UPDATE workers SET active_jobs = active_jobs + 1 WHERE id = ?`, workerID); bumpErr != nil {
			return nil, fmt.Errorf("bump worker load: %w", bumpErr)
		}
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO jobs (id, input_path, output_path, profile, prefer_gpu, worker_id, remote_job_id, status, error, created_unix, updated_unix)
		VALUES (?, ?, ?, ?, ?, ?, '', ?, '', ?, ?)
	`, j.ID, j.InputPath, j.OutputPath, j.Profile, boolToInt(j.PreferGPU), j.WorkerID, j.Status, j.CreatedUnix, j.UpdatedUnix)
	if err != nil {
		return nil, fmt.Errorf("insert job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit enqueue: %w", err)
	}
	out := j
	return &out, nil
}

func (s *Store) pickWorkerLocked(ctx context.Context, tx *sql.Tx, preferGPU bool, now int64) (string, error) {
	var rows *sql.Rows
	var err error
	if tx != nil {
		rows, err = tx.QueryContext(ctx, `
			SELECT id, node_id, grpc_addr, gpu, capacity, active_jobs, labels, last_heartbeat_unix, status
			FROM workers
		`)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, node_id, grpc_addr, gpu, capacity, active_jobs, labels, last_heartbeat_unix, status
			FROM workers
		`)
	}
	if err != nil {
		return "", fmt.Errorf("list workers for pick: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var bestID string
	var bestLoad = 1e9
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return "", err
		}
		if statusFor(w.LastHeartbeatUnix, now, s.staleAfterSec) != "online" {
			continue
		}
		if preferGPU && !w.GPU {
			continue
		}
		if w.ActiveJobs >= w.Capacity {
			continue
		}
		load := float64(w.ActiveJobs) / float64(w.Capacity)
		if w.GPU {
			load -= 0.01
		}
		if load < bestLoad {
			bestLoad = load
			bestID = w.ID
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if preferGPU && bestID == "" {
		return s.pickWorkerLocked(ctx, tx, false, now)
	}
	return bestID, nil
}

func (s *Store) promoteQueuedJobsLocked(ctx context.Context, tx *sql.Tx, now int64) error {
	for {
		var jobID string
		var preferGPU int
		err := tx.QueryRowContext(ctx, `
			SELECT id, prefer_gpu FROM jobs
			WHERE status = 'queued'
			ORDER BY created_unix ASC, id ASC
			LIMIT 1
		`).Scan(&jobID, &preferGPU)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("pick queued job: %w", err)
		}
		workerID, err := s.pickWorkerLocked(ctx, tx, preferGPU != 0, now)
		if err != nil {
			return err
		}
		if workerID == "" {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `UPDATE workers SET active_jobs = active_jobs + 1 WHERE id = ?`, workerID); err != nil {
			return fmt.Errorf("bump worker load: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE jobs SET status = 'assigned', worker_id = ?, updated_unix = ?
			WHERE id = ?
		`, workerID, now, jobID); err != nil {
			return fmt.Errorf("assign queued job: %w", err)
		}
	}
}

func (s *Store) GetJob(ctx context.Context, id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getJobLocked(ctx, id)
}

func (s *Store) getJobLocked(ctx context.Context, id string) (*Job, error) {
	var j Job
	var preferGPU int
	err := s.db.QueryRowContext(ctx, `
		SELECT id, input_path, output_path, profile, prefer_gpu, worker_id, remote_job_id, status, error, created_unix, updated_unix
		FROM jobs WHERE id = ?
	`, id).Scan(
		&j.ID, &j.InputPath, &j.OutputPath, &j.Profile, &preferGPU,
		&j.WorkerID, &j.RemoteJobID, &j.Status, &j.Error, &j.CreatedUnix, &j.UpdatedUnix,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("job %q not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	j.PreferGPU = preferGPU != 0
	return &j, nil
}

func (s *Store) ListJobs(ctx context.Context, status string) []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, input_path, output_path, profile, prefer_gpu, worker_id, remote_job_id, status, error, created_unix, updated_unix
		FROM jobs
		WHERE (? = '' OR lower(status) = lower(?))
		ORDER BY created_unix ASC, id ASC
	`, status, status)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	out := make([]*Job, 0)
	for rows.Next() {
		var j Job
		var preferGPU int
		if err := rows.Scan(
			&j.ID, &j.InputPath, &j.OutputPath, &j.Profile, &preferGPU,
			&j.WorkerID, &j.RemoteJobID, &j.Status, &j.Error, &j.CreatedUnix, &j.UpdatedUnix,
		); err != nil {
			return out
		}
		j.PreferGPU = preferGPU != 0
		cp := j
		out = append(out, &cp)
	}
	return out
}

func (s *Store) CancelJob(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.getJobLocked(ctx, id)
	if err != nil {
		return err
	}
	if j.Status == "done" || j.Status == "cancelled" {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().Unix()
	if j.WorkerID != "" {
		if _, relErr := tx.ExecContext(ctx, `
			UPDATE workers SET active_jobs = CASE WHEN active_jobs > 0 THEN active_jobs - 1 ELSE 0 END
			WHERE id = ?
		`, j.WorkerID); relErr != nil {
			return fmt.Errorf("release worker load: %w", relErr)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs SET status = 'cancelled', remote_job_id = '', updated_unix = ? WHERE id = ?
	`, now, id); err != nil {
		return fmt.Errorf("cancel job: %w", err)
	}
	if err := s.promoteQueuedJobsLocked(ctx, tx, now); err != nil {
		return err
	}
	return tx.Commit()
}

// SetJobRemoteID persists the worker-side job id for resume-after-restart.
func (s *Store) SetJobRemoteID(ctx context.Context, id, remoteID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `
		UPDATE jobs SET remote_job_id = ?, updated_unix = ? WHERE id = ?
	`, remoteID, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("set remote job id: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("job %q not found", id)
	}
	return nil
}

// GetWorker returns a worker by id (status refreshed from heartbeat age).
func (s *Store) GetWorker(ctx context.Context, id string) (*Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getWorkerLocked(ctx, id, time.Now().Unix())
}

// MarkJobRunning transitions assigned → running.
func (s *Store) MarkJobRunning(ctx context.Context, id string) error {
	return s.setJobStatus(ctx, id, "running", "", false)
}

// MarkJobDone transitions a job to done and releases worker capacity.
func (s *Store) MarkJobDone(ctx context.Context, id string) error {
	return s.setJobStatus(ctx, id, "done", "", true)
}

// MarkJobFailed transitions a job to failed and releases worker capacity.
func (s *Store) MarkJobFailed(ctx context.Context, id, errMsg string) error {
	return s.setJobStatus(ctx, id, "failed", errMsg, true)
}

func (s *Store) setJobStatus(ctx context.Context, id, status, errMsg string, releaseWorker bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.getJobLocked(ctx, id)
	if err != nil {
		return err
	}
	if j.Status == "done" || j.Status == "cancelled" || j.Status == "failed" {
		if j.Status == status {
			return nil
		}
		return fmt.Errorf("job %q already terminal (%s)", id, j.Status)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := time.Now().Unix()
	if releaseWorker && j.WorkerID != "" {
		if _, relErr := tx.ExecContext(ctx, `
			UPDATE workers SET active_jobs = CASE WHEN active_jobs > 0 THEN active_jobs - 1 ELSE 0 END
			WHERE id = ?
		`, j.WorkerID); relErr != nil {
			return fmt.Errorf("release worker load: %w", relErr)
		}
	}
	remoteClear := ""
	if releaseWorker {
		remoteClear = ""
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE jobs SET status = ?, error = ?, remote_job_id = ?, updated_unix = ? WHERE id = ?
	`, status, errMsg, remoteClear, now, id); err != nil {
		return fmt.Errorf("update job status: %w", err)
	}
	if releaseWorker {
		if err := s.promoteQueuedJobsLocked(ctx, tx, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) getWorkerLocked(ctx context.Context, id string, now int64) (*Worker, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, node_id, grpc_addr, gpu, capacity, active_jobs, labels, last_heartbeat_unix, status
		FROM workers WHERE id = ?
	`, id)
	w, err := scanWorker(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("worker %q not found", id)
	}
	if err != nil {
		return nil, err
	}
	w.Status = statusFor(w.LastHeartbeatUnix, now, s.staleAfterSec)
	return w, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanWorker(row rowScanner) (*Worker, error) {
	var w Worker
	var gpu int
	var labelsJSON string
	if err := row.Scan(
		&w.ID, &w.NodeID, &w.GRPCAddr, &gpu, &w.Capacity, &w.ActiveJobs,
		&labelsJSON, &w.LastHeartbeatUnix, &w.Status,
	); err != nil {
		return nil, err
	}
	w.GPU = gpu != 0
	if labelsJSON == "" {
		w.Labels = []string{}
	} else if err := json.Unmarshal([]byte(labelsJSON), &w.Labels); err != nil {
		return nil, fmt.Errorf("unmarshal labels: %w", err)
	}
	if w.Labels == nil {
		w.Labels = []string{}
	}
	return &w, nil
}

func statusFor(lastHeartbeat, now, staleAfterSec int64) string {
	age := now - lastHeartbeat
	switch {
	case age > staleAfterSec*2:
		return "offline"
	case age > staleAfterSec:
		return "stale"
	default:
		return "online"
	}
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
