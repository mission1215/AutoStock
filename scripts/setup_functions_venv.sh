#!/usr/bin/env bash
# Firebase Functions 배포용 Python 3.12 venv 생성
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FUNCS="$ROOT/functions"
VENV="$FUNCS/venv"

cd "$FUNCS"

if [[ -x "$VENV/bin/python" ]]; then
  ver="$("$VENV/bin/python" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  if [[ "$ver" == "3.12" ]] && "$VENV/bin/python" -c "import firebase_functions" 2>/dev/null; then
    echo "✓ functions/venv (Python $ver) 이미 준비됨"
    exit 0
  fi
  echo "기존 venv 재생성 (Python $ver → 3.12)"
  rm -rf "$VENV"
fi

if command -v uv >/dev/null 2>&1; then
  uv venv venv --python 3.12
  uv pip install -r requirements.txt --python venv/bin/python
elif command -v python3.12 >/dev/null 2>&1; then
  python3.12 -m venv venv
  venv/bin/pip install -r requirements.txt
else
  echo "❌ Python 3.12 필요 — uv 설치: curl -LsSf https://astral.sh/uv/install.sh | sh"
  exit 1
fi

echo "✅ functions/venv 준비 완료 ($("$VENV/bin/python" --version))"
