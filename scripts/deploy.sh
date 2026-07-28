#!/usr/bin/env bash
# AutoStock 배포: Functions venv → Firebase deploy
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export PATH="${HOME}/.local/bin:${PATH}"

echo "▶ 1/3 Functions venv"
"$ROOT/scripts/setup_functions_venv.sh"

echo "▶ 2/2 Firebase deploy (functions)"
firebase deploy --only functions

echo "✅ 배포 완료"
echo "   API: https://api-eq2ncfx6gq-uc.a.run.app"
