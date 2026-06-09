"""Fully automated Expert scoring — KIS data only, no Cursor chat."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from pipeline.constants import EXPERT_MAX_SCORE, EXPERT_STRONG_BUY_SCORE
from pipeline.expert_scoring import build_technicals_block, normalize_grade

logger = logging.getLogger("watchlist")


def _score_l2_technical(row: dict, tech: dict) -> tuple[int, list[str], list[str]]:
    """Layer 2 — rules from expert_signal_scoring.md."""
    score = 0
    bulls: list[str] = []
    bears: list[str] = []

    alignment = tech.get("ma_alignment", "")
    rsi = float(tech.get("rsi_14", 50) or 50)
    vol = float(tech.get("volume_ratio", 0) or 0)
    above_res = bool(tech.get("above_resistance"))
    high_pct = float(tech.get("52w_high_pct", 0) or 0)

    if alignment == "정배열":
        score += 2
        bulls.append("MA 정배열")
    elif alignment == "역배열":
        score -= 3
        bears.append("MA 역배열")

    if vol >= 2.0 and (above_res or high_pct >= 0.95):
        score += 3
        bulls.append(f"거래량 {vol:.1f}배+고점근접")
    elif vol >= 1.5 and high_pct >= 0.92:
        score += 1
        bulls.append(f"거래량 {vol:.1f}배")

    if 40 <= rsi <= 65:
        score += 2
        bulls.append(f"RSI {rsi:.0f} 건전")
    elif rsi > 75:
        score -= 2
        bears.append("RSI 과매수")

    return max(score, -3), bulls, bears


def _score_l6_sector(sector: str) -> tuple[int, list[str]]:
    blocked = ("금융", "지주", "유틸리티", "전력", "가스")
    if any(b in (sector or "") for b in blocked):
        return -2, ["섹터 역풍"]
    if sector and sector != "기타":
        return 1, [f"{sector} 테마"]
    return 0, []


def _score_l4_fundamental(row: dict) -> tuple[int, list[str]]:
    """KIS only — 시총·상대 위치 proxy."""
    mktcap = float(row.get("mktcap", 0) or 0)
    if mktcap >= 5_000_000_000_000:
        return 1, ["대형주 유동성"]
    if mktcap >= 1_000_000_000_000:
        return 0, []
    return 0, []


def _total_to_grade(total: int) -> str:
    if total >= EXPERT_STRONG_BUY_SCORE:
        return "STRONG BUY"
    if total >= 10:
        return "BUY"
    if total >= 7:
        return "WATCH"
    return "PASS"


def score_stock_auto(row: dict) -> dict[str, Any]:
    """Single filtered row → expert candidate dict."""
    tech = build_technicals_block(row)
    l2, bulls, bears = _score_l2_technical(row, tech)
    l6, l6_bulls = _score_l6_sector(str(row.get("sector", "")))
    l4, l4_bulls = _score_l4_fundamental(row)

    l1 = 0
    l3 = 0
    l5 = 0
    if l2 >= 4:
        l5 = 1

    layer_scores = {
        "L1_macro": l1,
        "L2_technical": l2,
        "L3_flow": l3,
        "L4_fundamental": l4,
        "L5_catalyst": l5,
        "L6_sector": l6,
        "L7_disqualified": False,
    }
    total = sum(v for k, v in layer_scores.items() if k != "L7_disqualified")
    grade = _total_to_grade(total)

    all_bulls = bulls + l6_bulls + l4_bulls
    if l5:
        all_bulls.append("기술 모멘텀")

    ma5 = float(row.get("ma5", 0) or 0)
    price = float(row.get("price", 0) or 0)
    name = str(row.get("name", ""))
    ticker = str(row["ticker"])

    reason_30 = f"{name} 기술{l2}pt·{tech.get('ma_alignment','')}·vol{tech.get('volume_ratio',0):.1f}x"[:30]

    return {
        "ticker": ticker,
        "name": name,
        "grade": grade,
        "total_score": total,
        "layer_scores": layer_scores,
        "bull_signals": all_bulls[:5],
        "bear_signals": bears[:3],
        "entry_strategy": "MA5 눌림 분할 (8:50~9:10)",
        "stop_loss_trigger": "20일선 이탈 or -5%",
        "reason_30": reason_30,
        "auto_scored": True,
    }


def generate_auto_candidates(
    filtered_df: pd.DataFrame,
    *,
    min_grade: str = "BUY",
    max_count: int = 15,
) -> dict[str, Any]:
    """
    Algorithmically score all filtered stocks → expert JSON payload.
    No web/LLM — suitable for 8:50 daily cron.
    """
    min_order = {"STRONG BUY": 3, "BUY": 2, "WATCH": 1, "PASS": 0}
    floor = min_order.get(normalize_grade(min_grade), 2)

    scored: list[dict] = []
    excluded: list[dict] = []

    for _, row in filtered_df.iterrows():
        cand = score_stock_auto(row.to_dict())
        g = normalize_grade(cand["grade"])
        if min_order.get(g, 0) < floor:
            excluded.append({"ticker": cand["ticker"], "reason": f"grade={g} score={cand['total_score']}"})
            continue
        if cand["layer_scores"]["L2_technical"] < 2:
            excluded.append({"ticker": cand["ticker"], "reason": "L2 기술 미달"})
            continue
        scored.append(cand)

    scored.sort(key=lambda c: (-c["total_score"], -c["layer_scores"]["L2_technical"]))
    candidates = scored[:max_count]
    top5 = [c["ticker"] for c in candidates[:5]]

    logger.info("Auto expert: %d candidates / %d excluded", len(candidates), len(excluded))

    return {
        "schema": "expert_signal_v1",
        "market_regime": "NEUTRAL",
        "market_regime_reason": "자동스코어(KIS only·L3/L5 미수집)",
        "auto_generated": True,
        "candidates": candidates,
        "excluded": excluded,
        "top5": top5,
    }
