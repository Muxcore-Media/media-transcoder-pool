# Changelog

## [v0.2.0] — 2026-08-10

### Added
- SQLite durable job queue + worker registry (`modernc.org/sqlite`, WAL) via `POOL_DB_PATH`
- Fake CPU worker Register/Heartbeat/Enqueue gRPC tests (no GPU farm)
- Dispatcher (`POOL_DISPATCH`) forwards assigned jobs to worker `TranscodeService`
- Offline media-transcoder mock + `cmd/local-demo` / `scripts/local-single-worker-demo.sh`
- Job lifecycle helpers: running / done / failed with worker capacity release
- Self-hosted CI (no `-race`)

### Changed
- Settings `stale_after_sec` updates threshold without wiping persisted queue

## [v0.1.0] — 2026-08-10

### Added
- `TranscoderPoolService` (workers + job enqueue/assign)
- GPU-preferring least-loaded scheduler
- SettingsProvider (`stale_after_sec`)
- Health `:9721`
