#!/usr/bin/env bash
# Daily watchlist pipeline wrapper
set -euo pipefail
cd "$(dirname "$0")/.."

STEP="${1:-technical}"
echo "▶ AutoStock Watchlist Pipeline — step: $STEP"

if [[ -f .venv/bin/activate ]]; then
  source .venv/bin/activate
fi

python -m pipeline run --step "$STEP" "${@:2}"
