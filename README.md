# Media Transcoder Pool

Distributed transcoding pool coordinator for MuxCore — registers mesh workers (GPU/CPU), heartbeats, and assigns enqueue jobs to the least-loaded matching worker.

**v0.2.0** persists workers and the job queue in SQLite (`modernc.org/sqlite`, WAL) and can dispatch assigned jobs to a worker `TranscodeService` (real `media-transcoder` or offline mock).

## Ports

| Service | Default |
|---------|---------|
| gRPC | `:9720` |
| Health | `:9721` |

## Config

| Env | Default | Meaning |
|-----|---------|---------|
| `POOL_DB_PATH` | `data/pool.db` | SQLite durable queue + worker registry |
| `POOL_STALE_AFTER_SEC` | `60` | Heartbeat age before worker marked stale |
| `POOL_DISPATCH` | off | When `1`/`true`, forward assigned jobs to worker gRPC |
| `MUXCORE_HTTP_ADDR` | `:9721` | Health listen address |

## Local single-worker demo (laptop, no GPU)

Offline mock (default — no FFmpeg):

```bash
./scripts/local-single-worker-demo.sh
# or: go run ./cmd/local-demo
```

Point at a sibling `media-transcoder` instance:

```bash
# terminal A
cd ../media-transcoder && TRANSCODER_GRPC_ADDR=127.0.0.1:9520 go run ./cmd/module
# terminal B
MEDIA_TRANSCODER_ADDR=127.0.0.1:9520 ./scripts/local-single-worker-demo.sh
```

Integration coverage: `TestLocalSingleWorkerDemo` and `TestFakeWorkerRegisterHeartbeatEnqueue`.

## Status

Durable job queue, fake-worker gRPC tests, and local single-worker demo (offline mock / optional real transcoder) are in place.
