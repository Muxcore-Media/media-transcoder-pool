# Media Transcoder Pool

Distributed transcoding pool coordinator for MuxCore — registers mesh workers (GPU/CPU), heartbeats, and assigns enqueue jobs to the least-loaded matching worker.

**v0.1.0** is an in-memory coordinator scaffold. Actual remote `Enqueue` against `media-transcoder` workers is a follow-up.

## Ports

| Service | Default |
|---------|---------|
| gRPC | `:9720` |
| Health | `:9721` |

## Status

Scaffold — worker agent sidecar, job progress fan-in, and durable queue are follow-ups.
