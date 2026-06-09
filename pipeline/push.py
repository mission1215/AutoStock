"""Pipeline finalize → Firebase /api/ai/picks (provider=cursor)."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger("watchlist")

_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

API_URL = os.environ.get("AUTOSTOCK_API_URL", "").rstrip("/")
API_SECRET = os.environ.get("AUTOSTOCK_API_SECRET", "")
MARKET = "KR"
SESSION = "morning"


def push_watchlist_to_server(watchlist: pd.DataFrame) -> bool:
    """
    Cursor 파이프라인 감시목록을 Firebase ai_picks 에 저장.
    claude_picker.py 와 동일한 /api/ai/picks 엔드포인트 사용.
    """
    if watchlist.empty:
        logger.warning("Push skipped — watchlist empty")
        return False
    if not API_URL:
        logger.error("AUTOSTOCK_API_URL 미설정 — .env 확인")
        print("[ERROR] AUTOSTOCK_API_URL 환경변수가 설정되지 않았습니다.")
        return False

    candidates: list[dict] = []
    for _, row in watchlist.iterrows():
        ticker = str(row.get("ticker", "")).strip().zfill(6)
        if not ticker.isdigit() or len(ticker) != 6:
            continue
        grade = str(row.get("grade", "")).strip()
        total = row.get("total_score")
        reason = str(row.get("reason_30") or row.get("reason", "")).strip()
        stop = str(row.get("stop_loss_trigger", "")).strip()
        entry = str(row.get("entry_zone", "")).strip()

        detail_parts = []
        if grade:
            detail_parts.append(grade)
        if total is not None and str(total) != "nan":
            detail_parts.append(f"{int(float(total))}pt")
        if reason:
            detail_parts.append(reason)
        if stop:
            detail_parts.append(f"손절:{stop}")
        if entry:
            detail_parts.append(f"진입:{entry}")
        detail = " · ".join(detail_parts) or "Expert Signal 추천"
        candidates.append({"code": ticker, "reason": f"[Cursor] {detail}"})

    if not candidates:
        return False

    url = f"{API_URL}/api/ai/picks"
    headers = {
        "Content-Type": "application/json",
        "X-AI-Picks-Secret": API_SECRET,
    }
    payload = {
        "provider": "cursor",
        "market": MARKET,
        "session": SESSION,
        "candidates": candidates,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok"):
            print(
                f"[OK] Cursor 감시목록 {data.get('count')}종목 → Firebase ai_picks/{data.get('date_key')}"
            )
            print(f"     종목: {', '.join(data.get('candidates', []))}")
            return True
        print(f"[ERROR] 서버 응답: {data}")
        return False
    except requests.RequestException as exc:
        print(f"[ERROR] Firebase Push 실패: {exc}")
        logger.error("Push failed: %s", exc)
        return False
