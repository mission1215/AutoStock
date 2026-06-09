"""Pipeline-wide constants."""

from __future__ import annotations

# Hard sector blacklist (R8) — substring match on sector label
SECTOR_BLACKLIST: tuple[str, ...] = ("지주사", "지주", "금융", "유틸리티", "전력", "가스")

MIN_MARKET_CAP_WON: int = 100_000_000_000  # 1,000억 KRW

MIN_COMPOSITE_SCORE: float = 0.62
MAX_WATCHLIST_SIZE: int = 10
MAX_LLM_CANDIDATES: int = 15

# Expert Signal Scoring (prompts/expert_signal_scoring.md)
EXPERT_MAX_SCORE: int = 20
EXPERT_MIN_TRADE_SCORE: int = 10       # BUY 이상
EXPERT_STRONG_BUY_SCORE: int = 14      # 실매매 우선
EXPERT_GRADES_WATCHLIST: frozenset[str] = frozenset({"STRONG BUY", "BUY"})
EXPERT_GRADES_TRADE: frozenset[str] = frozenset({"STRONG BUY"})

# Technical rule thresholds
MIN_VOLUME_RATIO: float = 1.3
RSI_MIN: float = 45.0
RSI_MAX: float = 75.0
FLOOR_FROM_52W_LOW: float = 1.15
CEILING_FROM_52W_HIGH: float = 0.98

# Composite score weights
TECH_WEIGHT: float = 0.55
LLM_WEIGHT: float = 0.45

TECH_COMPONENT_WEIGHTS: dict[str, float] = {
    "vol_ratio": 0.25,
    "rsi_distance": 0.15,
    "ma_alignment": 0.20,
    "52w_high_pct": 0.15,
    "mktcap": 0.05,
}

# KIS rank API
KR_VOLUME_RANK_PATH = "/uapi/domestic-stock/v1/quotations/volume-rank"
KR_RANK_BLNG_VOLUME = "0"
KR_RANK_BLNG_VALUE = "3"

# Fallback sector hints when KIS bstp field is empty
SECTOR_HINT_MAP: dict[str, str] = {
    "005930": "반도체", "000660": "반도체", "042700": "반도체",
    "035420": "IT플랫폼", "035720": "IT플랫폼",
    "005380": "자동차", "000270": "자동차",
    "373220": "2차전지", "006400": "2차전지", "051910": "2차전지",
    "068270": "바이오", "207940": "바이오",
    "105560": "금융", "055550": "금융", "086790": "금융", "032830": "금융",
    "028260": "지주", "034730": "지주", "003550": "지주",
    "017670": "통신", "030200": "통신", "015760": "에너지",
}
