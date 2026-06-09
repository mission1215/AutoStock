"""Step 5 — Composite scoring (Expert 7-layer + technical)."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from pipeline.config import pipeline_config
from pipeline.constants import EXPERT_STRONG_BUY_SCORE, TECH_COMPONENT_WEIGHTS, TECH_WEIGHT, LLM_WEIGHT
from pipeline.expert_scoring import expert_llm_score, grade_allowed_for_watchlist, normalize_grade

logger = logging.getLogger("watchlist")


def _min_max_norm(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    lo, hi = series.min(), series.max()
    if hi <= lo:
        return pd.Series(0.5, index=series.index)
    return (series - lo) / (hi - lo)


def _llm_score(c: dict) -> float:
    base = expert_llm_score(c)
    grade = normalize_grade(c.get("grade", ""))
    if grade == "STRONG BUY":
        base = min(base + 0.08, 1.0)
    score = float(c.get("total_score", 0) or 0)
    if score >= EXPERT_STRONG_BUY_SCORE:
        base = min(base + 0.05, 1.0)
    return base


def compute_technical_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["rsi_distance"] = (out["rsi14"] - 50).abs()
    out["norm_vol_ratio"] = _min_max_norm(out["vol_ratio"])
    out["norm_rsi_distance"] = 1.0 - _min_max_norm(out["rsi_distance"])
    out["norm_ma_alignment"] = _min_max_norm(out["ma_alignment"].astype(float))
    out["norm_52w_high_pct"] = _min_max_norm(out["52w_high_pct"])
    out["norm_mktcap"] = _min_max_norm(out["mktcap"])

    w = TECH_COMPONENT_WEIGHTS
    out["technical_score"] = (
        out["norm_vol_ratio"] * w["vol_ratio"]
        + out["norm_rsi_distance"] * w["rsi_distance"]
        + out["norm_ma_alignment"] * w["ma_alignment"]
        + out["norm_52w_high_pct"] * w["52w_high_pct"]
        + out["norm_mktcap"] * w["mktcap"]
    )
    return out


def build_watchlist(
    filtered_df: pd.DataFrame,
    validated_candidates: list[dict],
) -> pd.DataFrame:
    if not validated_candidates:
        logger.warning("No validated LLM candidates — empty watchlist")
        return pd.DataFrame()

    watchlist_candidates = [
        c for c in validated_candidates
        if not c.get("grade") or grade_allowed_for_watchlist(c.get("grade", ""))
    ]
    if not watchlist_candidates:
        watchlist_candidates = validated_candidates

    llm_map = {c["ticker"]: c for c in watchlist_candidates}
    tickers = list(llm_map.keys())
    subset = filtered_df[filtered_df["ticker"].isin(tickers)].copy()
    if subset.empty:
        return pd.DataFrame()

    scored = compute_technical_scores(subset)
    scored["llm_score"] = scored["ticker"].map(lambda t: _llm_score(llm_map[t]))
    scored["composite"] = (
        scored["technical_score"] * TECH_WEIGHT + scored["llm_score"] * LLM_WEIGHT
    ).round(4)

    scored["reason"] = scored["ticker"].map(lambda t: llm_map[t].get("reason", ""))
    scored["grade"] = scored["ticker"].map(lambda t: llm_map[t].get("grade") or "")
    scored["total_score"] = scored["ticker"].map(lambda t: llm_map[t].get("total_score"))
    scored["stop_loss_trigger"] = scored["ticker"].map(
        lambda t: llm_map[t].get("stop_loss_trigger", "")
    )
    scored["entry_strategy"] = scored["ticker"].map(
        lambda t: llm_map[t].get("entry_strategy", "")
    )

    min_score = pipeline_config.min_composite_score
    max_size = pipeline_config.max_watchlist_size
    ranked = (
        scored[scored["composite"] >= min_score]
        .sort_values(["llm_score", "composite"], ascending=False)
        .head(max_size)
    )

    ranked["entry_low"] = (ranked["ma5"] * 0.995).round(0)
    ranked["entry_high"] = (
        pd.concat([ranked["price"] * 1.005, ranked["ma5"] * 1.02], axis=1).min(axis=1)
    ).round(0)
    ranked["entry_zone"] = ranked.apply(
        lambda r: f"{int(r['entry_low']):,}~{int(r['entry_high']):,}",
        axis=1,
    )

    cols = [
        "ticker", "name", "grade", "total_score", "composite", "technical_score", "llm_score",
        "entry_zone", "entry_low", "entry_high", "entry_strategy", "stop_loss_trigger", "reason",
        "price", "vol_ratio", "rsi14", "sector",
    ]
    return ranked[[c for c in cols if c in ranked.columns]]


def filter_for_push(watchlist: pd.DataFrame) -> pd.DataFrame:
    """Firebase Push용 — STRONG BUY 등 설정 등급만."""
    if watchlist.empty or "grade" not in watchlist.columns:
        return watchlist
    allowed = pipeline_config.expert_push_grade_set()
    mask = watchlist["grade"].map(lambda g: normalize_grade(str(g)) in allowed)
    filtered = watchlist[mask]
    if filtered.empty:
        logger.warning("Push grade filter empty — falling back to top 3 by composite")
        return watchlist.head(3)
    return filtered


def watchlist_to_records(watchlist: pd.DataFrame) -> list[dict[str, Any]]:
    return watchlist.to_dict(orient="records")
