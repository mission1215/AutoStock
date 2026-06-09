"""Step 4 — Hallucination / code validation (Expert + legacy A/B/C/D)."""

from __future__ import annotations

import logging
import re

import pandas as pd

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover
    fuzz = None  # type: ignore[assignment]

from pipeline.config import pipeline_config
from pipeline.expert_scoring import (
    build_reason_text,
    is_disqualified,
    normalize_grade,
    passes_expert_score_floor,
)

logger = logging.getLogger("watchlist")

TICKER_RE = re.compile(r"^\d{6}$")


def _normalize_ticker(raw: str) -> str | None:
    s = str(raw or "").strip()
    if not s.isdigit():
        return None
    s = s.zfill(6)
    return s if TICKER_RE.match(s) else None


def _name_matches(candidate_name: str, master_name: str, threshold: float) -> bool:
    if not master_name:
        return True
    if not candidate_name:
        return False
    if candidate_name.strip() == master_name.strip():
        return True
    if fuzz is None:
        return candidate_name in master_name or master_name in candidate_name
    score = fuzz.ratio(candidate_name.strip(), master_name.strip()) / 100.0
    return score >= threshold


def validate_candidates(
    candidates: list[dict],
    universe_df: pd.DataFrame,
) -> tuple[list[dict], int]:
    """
    Remove hallucinated, disqualified, or invalid tickers.
    Supports expert_signal_v1 and legacy A/B/C/D format.
    """
    threshold = pipeline_config.name_fuzzy_threshold
    name_map = {
        str(r["ticker"]): str(r.get("name", ""))
        for _, r in universe_df.iterrows()
    }
    allowed = set(name_map.keys())
    valid: list[dict] = []
    removed = 0

    for c in candidates:
        ticker = _normalize_ticker(c.get("ticker") or c.get("code", ""))
        if not ticker:
            logger.warning("INVALID format removed: %s", c)
            removed += 1
            continue
        if ticker not in allowed:
            logger.warning("HALLUCINATION removed: %s", c)
            removed += 1
            continue
        master_name = name_map.get(ticker, "")
        cand_name = str(c.get("name", "")).strip()
        if cand_name and not _name_matches(cand_name, master_name, threshold):
            logger.warning("NAME MISMATCH removed: %s (LLM=%s master=%s)", ticker, cand_name, master_name)
            removed += 1
            continue

        if is_disqualified(c):
            logger.warning("L7 DISQUALIFIED removed: %s", ticker)
            removed += 1
            continue

        grade = normalize_grade(c.get("grade", ""))
        if grade == "PASS":
            logger.warning("PASS grade removed: %s", ticker)
            removed += 1
            continue

        is_expert = c.get("total_score") is not None or c.get("layer_scores")
        if is_expert and not passes_expert_score_floor(c):
            logger.warning("Score floor removed: %s (score=%s)", ticker, c.get("total_score"))
            removed += 1
            continue

        layers = c.get("layer_scores") or {}
        l3 = float(layers.get("L3_flow", 0) or 0)
        if is_expert and l3 < 0:
            logger.warning("L3 flow negative removed: %s", ticker)
            removed += 1
            continue

        clean: dict = {
            "ticker": ticker,
            "name": master_name or cand_name,
            "reason": build_reason_text(c),
            "grade": grade or None,
            "total_score": c.get("total_score"),
            "layer_scores": c.get("layer_scores"),
            "bull_signals": c.get("bull_signals") or [],
            "bear_signals": c.get("bear_signals") or [],
            "entry_strategy": str(c.get("entry_strategy", "")),
            "stop_loss_trigger": str(c.get("stop_loss_trigger", "")),
            "reason_30": str(c.get("reason_30") or c.get("reason", ""))[:60],
        }
        if not is_expert:
            clean.update({
                "A": int(c.get("A", 0)),
                "B": int(c.get("B", 0)),
                "C": int(c.get("C", 0)),
                "D": int(c.get("D", 0)),
            })
        valid.append(clean)

    logger.info("Validation: %d valid / %d removed", len(valid), removed)
    return valid, removed
