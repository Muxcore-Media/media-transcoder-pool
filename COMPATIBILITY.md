# Compatibility

| Module Version | Core Version | Status |
|----------------|-------------|--------|
| v0.2.0         | 0.5.2+      | Current |
| v0.1.0         | 0.5.2+      | Superseded |

## Capabilities

- `media.transcode.pool` / `transcoder.pool`
- `settings`

## Persistence

- SQLite via `modernc.org/sqlite` (pure Go, `CGO_ENABLED=0`), WAL, default `data/pool.db`

## Worker protocol

- Pool dispatcher dials worker `grpc_addr` using `muxcore.transcoder.v1.TranscodeService`
  (same wire API as `media-transcoder`; mirrored stubs under `internal/transcodev1`).
- Laptop demo uses an offline mock of that service when no real worker is configured.
