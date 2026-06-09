"""Expert Signal Scoring — 7-layer framework (prompts/expert_signal_scoring.md)."""

from __future__ import annotations

from pathlib import Path

from pipeline.constants import (
    EXPERT_GRADES_TRADE,
    EXPERT_GRADES_WATCHLIST,
    EXPERT_MAX_SCORE,
    EXPERT_MIN_TRADE_SCORE,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPERT_PROMPT_PATH = PROJECT_ROOT / "prompts" / "expert_signal_scoring.md"


def load_expert_prompt_text() -> str:
    if EXPERT_PROMPT_PATH.exists():
        return EXPERT_PROMPT_PATH.read_text(encoding="utf-8")
    return ""


def ma_alignment_label(ma_alignment: int, price: float, ma5: float, ma20: float, ma60: float, ma120: float) -> str:
    if ma_alignment >= 3 and price > ma5 > ma20 > 0:
        return "정배열"
    if ma20 > ma5 and price < ma20:
        return "역배열"
    return "혼조"


def build_technicals_block(row: dict) -> dict:
    price = float(row.get("price", 0) or 0)
    ma5 = float(row.get("ma5", 0) or 0)
    ma20 = float(row.get("ma20", 0) or 0)
    ma60 = float(row.get("ma60", 0) or 0)
    ma120 = float(row.get("ma120", 0) or 0)
    rsi = float(row.get("rsi14", 50) or 50)
    vol_ratio = float(row.get("vol_ratio", 0) or 0)
    high_pct = float(row.get("52w_high_pct", 0) or 0)
    alignment = int(row.get("ma_alignment", 0) or 0)

    breakout = vol_ratio >= 2.0 and high_pct >= 0.95
    above_resistance = high_pct >= 0.98

    return {
        "price": price,
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "ma120": ma120,
        "ma_alignment": ma_alignment_label(alignment, price, ma5, ma20, ma60, ma120),
        "rsi_14": rsi,
        "volume_ratio": vol_ratio,
        "breakout": breakout,
        "above_resistance": above_resistance,
        "52w_high_pct": high_pct,
        "bb_width_percentile": None,
    }


def build_stock_expert_payload(row: dict) -> dict:
    """Pipeline row → expert input stock object (@web으로 flow/fundamental/news 채움)."""
    sector = str(row.get("sector", "기타"))
    return {
        "ticker": str(row["ticker"]),
        "name": str(row.get("name", "")),
        "technicals": build_technicals_block(row),
        "flow": {
            "foreign_net_3d": None,
            "inst_net_3d": None,
            "short_ratio": None,
            "short_ratio_change": None,
        },
        "fundamental": {
            "eps_surprise_last_q": None,
            "per": None,
            "pbr": None,
            "yoy_revenue_growth": None,
        },
        "news_summary": "",
        "sector_theme": sector,
        "insider_buying": None,
        "buyback_announced": None,
    }


MARKET_CONTEXT_SEARCH = [
    {"key": "vix", "query": "VIX 지수 현재"},
    {"key": "us_10y_yield", "query": "미국채 10년물 금리 today"},
    {"key": "usd_krw", "query": "원달러 환율 오늘"},
    {"key": "kospi_5d_return", "query": "코스피 5일 수익률"},
    {"key": "kosdaq_5d_return", "query": "코스닥 5일 수익률"},
    {"key": "foreign_net_buy_kospi_3d", "query": "코스피 외국인 순매수 3일"},
    {"key": "program_buy_today", "query": "코스피 프로그램 순매수 오늘"},
    {"key": "market_news", "query": "오늘 한국 주식시장 주요 뉴스"},
]


def empty_market_context(date_key: str) -> dict:
    return {
        "date": date_key,
        "vix": None,
        "usd_krw": None,
        "us_10y_yield": None,
        "kospi_5d_return": None,
        "kosdaq_5d_return": None,
        "foreign_net_buy_kospi_3d": None,
        "program_buy_today": None,
    }


def normalize_grade(raw: str) -> str:
    g = str(raw or "").strip().upper().replace("_", " ")
    if "STRONG" in g and "BUY" in g:
        return "STRONG BUY"
    if g == "BUY":
        return "BUY"
    if g == "WATCH":
        return "WATCH"
    if g in ("PASS", "DISQUALIFIED", "EXCLUDED"):
        return "PASS"
    return g or "WATCH"


def is_disqualified(candidate: dict) -> bool:
    layers = candidate.get("layer_scores") or {}
    if layers.get("L7_disqualified") is True:
        return True
    grade = normalize_grade(candidate.get("grade", ""))
    return grade == "PASS"


def expert_llm_score(candidate: dict) -> float:
    """0~1 normalized expert score."""
    if candidate.get("total_score") is not None:
        return min(max(float(candidate["total_score"]) / EXPERT_MAX_SCORE, 0.0), 1.0)
    layers = candidate.get("layer_scores") or {}
    if layers:
        layer_sum = sum(
            float(layers.get(k, 0) or 0)
            for k in ("L1_macro", "L2_technical", "L3_flow", "L4_fundamental", "L5_catalyst", "L6_sector")
        )
        return min(max(layer_sum / EXPERT_MAX_SCORE, 0.0), 1.0)
    scores = [float(candidate.get(k, 0) or 0) for k in ("A", "B", "C", "D")]
    if any(scores):
        return sum(scores) / (len(scores) * 10.0)
    return 0.0


def grade_allowed_for_watchlist(grade: str) -> bool:
    return normalize_grade(grade) in EXPERT_GRADES_WATCHLIST


def grade_allowed_for_push(grade: str) -> bool:
    return normalize_grade(grade) in EXPERT_GRADES_TRADE


def passes_expert_score_floor(candidate: dict) -> bool:
    score = float(candidate.get("total_score", 0) or 0)
    if score <= 0:
        return expert_llm_score(candidate) * EXPERT_MAX_SCORE >= EXPERT_MIN_TRADE_SCORE
    return score >= EXPERT_MIN_TRADE_SCORE


def build_reason_text(candidate: dict) -> str:
    r30 = str(candidate.get("reason_30") or candidate.get("reason") or "").strip()
    grade = normalize_grade(candidate.get("grade", ""))
    score = candidate.get("total_score")
    parts = []
    if grade:
        parts.append(grade)
    if score is not None:
        parts.append(f"{score}pt")
    if r30:
        parts.append(r30)
    bulls = candidate.get("bull_signals") or []
    if bulls and not r30:
        parts.append("/".join(str(b) for b in bulls[:2]))
    return " · ".join(parts)[:120]
