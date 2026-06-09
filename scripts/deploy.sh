#!/usr/bin/env bash
# AutoStock 전체 배포: Functions venv → 웹 빌드 → Firebase deploy
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export PATH="${HOME}/.local/bin:${PATH}"

echo "▶ 1/3 Functions venv"
"$ROOT/scripts/setup_functions_venv.sh"

echo "▶ 2/3 Web build"
cd "$ROOT/web"
if [[ ! -d node_modules ]]; then
  npm ci
fi
npm run build

echo "▶ 3/3 Firebase deploy (functions + hosting)"
cd "$ROOT"
firebase deploy --only functions,hosting

echo "✅ 배포 완료"
echo "   API: https://api-eq2ncfx6gq-uc.a.run.app"
echo "   Web: https://autostock-kis.web.app"
