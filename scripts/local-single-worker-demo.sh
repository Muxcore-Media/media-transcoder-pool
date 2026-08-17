#!/usr/bin/env bash
# Local single-worker demo for media-transcoder-pool (laptop only, no cloud GPU).
#
# Default: in-process offline media-transcoder mock (no FFmpeg required).
# Optional real sibling:
#   MEDIA_TRANSCODER_ADDR=127.0.0.1:9520 ./scripts/local-single-worker-demo.sh
#   (start media-transcoder separately from ../media-transcoder)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PATH="${HOME}/.local/go/bin:${PATH:-}"
cd "$ROOT"

echo "==> media-transcoder-pool local single-worker demo"
if [[ -n "${MEDIA_TRANSCODER_ADDR:-}" ]]; then
  echo "    worker: real media-transcoder at ${MEDIA_TRANSCODER_ADDR}"
else
  SIBLING="$(cd "$ROOT/.." && pwd)/media-transcoder"
  if [[ -d "$SIBLING" ]]; then
    echo "    note: sibling media-transcoder found at $SIBLING"
    echo "          (demo uses offline mock unless MEDIA_TRANSCODER_ADDR is set)"
  fi
  echo "    worker: offline TranscodeService mock (no GPU / no FFmpeg)"
fi

exec go run ./cmd/local-demo
