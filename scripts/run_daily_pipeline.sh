#!/usr/bin/env bash
# 평일 8:50 KST — 완전 자동 감시목록 파이프라인 (Cursor 채팅 불필요)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

LOG_DIR="$ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/pipeline_$(date +%Y%m%d).log"

exec >>"$LOG_FILE" 2>&1
echo "========== $(date '+%Y-%m-%d %H:%M:%S %Z') =========="

if [[ -f "$ROOT/.venv/bin/activate" ]]; then
  source "$ROOT/.venv/bin/activate"
elif [[ -f "$ROOT/venv/bin/activate" ]]; then
  source "$ROOT/venv/bin/activate"
fi

export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
python3 -m pipeline run --step daily "$@"
