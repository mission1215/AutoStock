#!/usr/bin/env bash
# macOS LaunchAgent 설치 — 평일 08:50 KST 자동 파이프라인
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_NAME="com.autostock.pipeline.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
TARGET="$LAUNCH_AGENTS/$PLIST_NAME"

chmod +x "$ROOT/scripts/run_daily_pipeline.sh"

sed "s|__AUTOSTOCK_ROOT__|$ROOT|g" \
  "$ROOT/scripts/com.autostock.pipeline.plist.template" > "$TARGET"

launchctl bootout "gui/$(id -u)/$PLIST_NAME" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$TARGET"
launchctl enable "gui/$(id -u)/$PLIST_NAME"

echo "✅ 설치 완료: $TARGET"
echo "   스케줄: 평일 08:50 KST (장 시작 10분 전)"
echo "   로그: $ROOT/logs/pipeline_YYYYMMDD.log"
echo ""
echo "수동 테스트:"
echo "  $ROOT/scripts/run_daily_pipeline.sh"
echo ""
echo "제거:"
echo "  launchctl bootout gui/$(id -u)/$PLIST_NAME && rm $TARGET"
