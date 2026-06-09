"""Step 2 — Hard technical rule filter."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from pipeline.config import pipeline_config
from pipeline.constants import (
    CEILING_FROM_52W_HIGH,
    FLOOR_FROM_52W_LOW,
    MIN_MARKET_CAP_WON,
    MIN_VOLUME_RATIO,
    RSI_MAX,
    RSI_MIN,
    SECTOR_BLACKLIST,
)
from pipeline.universe import today_yyyymmdd

logger = logging.getLogger("watchlist")


@dataclass
class FilterStats:
    universe_count: int = 0
    pass_count: int = 0
    fail_r1_uptrend: int = 0
    fail_r2_momentum: int = 0
    fail_r3_volume: int = 0
    fail_r4_rsi: int = 0
    fail_r5_floor: int = 0
    fail_r6_ceiling: int = 0
    fail_r7_mktcap: int = 0
    fail_r8_sector: int = 0
    passed_tickers: list[str] = field(default_factory=list)


def _sector_blocked(sector: str) -> bool:
    s = (sector or "").strip()
    return any(bl in s for bl in SECTOR_BLACKLIST)


def apply_rule_filter(df: pd.DataFrame) -> tuple[pd.DataFrame, FilterStats]:
    """Apply R1–R8 hard gates. Returns (filtered_df, stats)."""
    stats = FilterStats(universe_count=len(df))
    passed_rows: list[dict] = []

    for _, row in df.iterrows():
        price = float(row.get("price", 0) or 0)
        ma5 = float(row.get("ma5", 0) or 0)
        ma20 = float(row.get("ma20", 0) or 0)
        vol_ratio = float(row.get("vol_ratio", 0) or 0)
        rsi = float(row.get("rsi14", 50) or 50)
        low52 = float(row.get("52w_low", 0) or 0)
        high52 = float(row.get("52w_high", 0) or 0)
        mktcap = float(row.get("mktcap", 0) or 0)
        sector = str(row.get("sector", ""))

        if price <= 0 or ma20 <= 0:
            stats.fail_r1_uptrend += 1
            continue
        if price <= ma20:
            stats.fail_r1_uptrend += 1
            continue
        if ma5 <= ma20:
            stats.fail_r2_momentum += 1
            continue
        if vol_ratio <= MIN_VOLUME_RATIO:
            stats.fail_r3_volume += 1
            continue
        if rsi < RSI_MIN or rsi > RSI_MAX:
            stats.fail_r4_rsi += 1
            continue
        if low52 > 0 and price <= low52 * FLOOR_FROM_52W_LOW:
            stats.fail_r5_floor += 1
            continue
        if high52 > 0 and price >= high52 * CEILING_FROM_52W_HIGH:
            stats.fail_r6_ceiling += 1
            continue
        if mktcap < MIN_MARKET_CAP_WON:
            stats.fail_r7_mktcap += 1
            continue
        if _sector_blocked(sector):
            stats.fail_r8_sector += 1
            continue

        passed_rows.append(row.to_dict())
        stats.passed_tickers.append(str(row["ticker"]))

    filtered = pd.DataFrame(passed_rows)
    stats.pass_count = len(filtered)
    logger.info(
        "Rule filter: %d / %d passed (RSI=%d vol=%d sector=%d mktcap=%d)",
        stats.pass_count,
        stats.universe_count,
        stats.fail_r4_rsi,
        stats.fail_r3_volume,
        stats.fail_r8_sector,
        stats.fail_r7_mktcap,
    )
    return filtered, stats


def save_filtered(df: pd.DataFrame, date_str: str | None = None) -> str:
    pipeline_config.ensure_dirs()
    d = date_str or today_yyyymmdd()
    path = pipeline_config.data_dir / f"filtered_{d}.parquet"
    df.to_parquet(path, index=False)
    logger.info("저장: %s", path)
    return str(path)


def load_filtered(date_str: str | None = None) -> pd.DataFrame:
    d = date_str or today_yyyymmdd()
    path = pipeline_config.data_dir / f"filtered_{d}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Filtered data not found: {path}")
    return pd.read_parquet(path)
