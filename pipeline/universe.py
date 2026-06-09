"""Step 1 — Universe collection via KIS Open API."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from config import Config
from pipeline.config import pipeline_config
from pipeline.constants import KR_RANK_BLNG_VALUE, KR_RANK_BLNG_VOLUME
from pipeline.indicators import compute_indicators, ohlcv_rows_to_frame
from pipeline.kis_client import (
    PipelineKISClient,
    extract_mktcap_won,
    extract_name,
    extract_sector,
)

logger = logging.getLogger("watchlist")
KST = ZoneInfo("Asia/Seoul")


def today_yyyymmdd() -> str:
    return datetime.now(KST).strftime("%Y%m%d")


def today_iso() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _collect_ticker_universe(client: PipelineKISClient) -> list[str]:
    """Liquid KOSPI + KOSDAQ names from KIS rank APIs + watchlist."""
    limit = pipeline_config.universe_rank_limit
    seen: set[str] = set()
    ordered: list[str] = []

    def add_codes(codes: list[str]) -> None:
        for c in codes:
            if c in seen:
                continue
            seen.add(c)
            ordered.append(c)
            if len(ordered) >= pipeline_config.universe_max_tickers:
                return

    rank_jobs = (
        ("J", "거래량", KR_RANK_BLNG_VOLUME),
        ("J", "거래대금", KR_RANK_BLNG_VALUE),
        ("Q", "거래량", KR_RANK_BLNG_VOLUME),
        ("Q", "거래대금", KR_RANK_BLNG_VALUE),
    )
    for market, label, blng in rank_jobs:
        if len(ordered) >= pipeline_config.universe_max_tickers:
            break
        try:
            add_codes(client.fetch_volume_rank(market, blng, limit=limit))
        except Exception as exc:
            logger.warning("순위 API 스킵 (%s/%s): %s", market, label, exc)

    for raw in Config.WATCHLIST:
        c = str(raw).strip().zfill(6)
        if c.isdigit() and len(c) == 6:
            add_codes([c])

    logger.info("유니버스 종목코드 %d개 수집 (KOSPI+KOSDAQ 순위)", len(ordered))
    return ordered


def fetch_universe(client: PipelineKISClient | None = None) -> pd.DataFrame:
    """
    Returns DataFrame with columns:
    ticker, name, price, ohlcv_20d (JSON), rsi14, ma5, ma20, ma60, ma120,
    vol_ratio, 52w_high, 52w_low, 52w_high_pct, mktcap, sector, ma_alignment
    """
    client = client or PipelineKISClient()
    tickers = _collect_ticker_universe(client)
    rows: list[dict] = []
    failures: list[dict] = []

    for i, ticker in enumerate(tickers, 1):
        try:
            price_data = client.get_current_price_multi(ticker)
            out = price_data.get("output") or {}
            if not isinstance(out, dict):
                raise ValueError("empty price output")

            ohlcv_raw = client.get_daily_ohlcv_multi(ticker)
            if not ohlcv_raw:
                raise ValueError("empty OHLCV")

            df = ohlcv_rows_to_frame(ohlcv_raw)
            ind = compute_indicators(df, out)
            name = extract_name(out)
            sector = extract_sector(out, ticker)
            mktcap = extract_mktcap_won(out)

            ohlcv_tail = ohlcv_raw[: min(20, len(ohlcv_raw))]

            rows.append(
                {
                    "ticker": ticker,
                    "name": name or ticker,
                    "price": ind["price"],
                    "ohlcv_20d": json.dumps(ohlcv_tail, ensure_ascii=False),
                    "rsi14": ind["rsi14"],
                    "ma5": ind["ma5"],
                    "ma20": ind["ma20"],
                    "ma60": ind["ma60"],
                    "ma120": ind["ma120"],
                    "vol_ratio": ind["vol_ratio"],
                    "52w_high": ind["52w_high"],
                    "52w_low": ind["52w_low"],
                    "52w_high_pct": ind["52w_high_pct"],
                    "mktcap": mktcap,
                    "sector": sector,
                    "ma_alignment": ind["ma_alignment"],
                    "today_vol": ind["today_vol"],
                    "avg20_vol": ind["avg20_vol"],
                }
            )
            if i % 25 == 0:
                logger.info("  … %d/%d 종목 처리", i, len(tickers))

        except Exception as exc:
            failures.append({"ticker": ticker, "error": str(exc)})
            logger.warning("SKIP %s: %s", ticker, exc)

    if failures:
        fail_path = pipeline_config.data_dir / f"universe_failures_{today_yyyymmdd()}.json"
        fail_path.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("실패 %d건 → %s", len(failures), fail_path)

    df = pd.DataFrame(rows)
    logger.info("유니버스 완료: %d/%d 성공", len(df), len(tickers))
    return df


def save_universe(df: pd.DataFrame, date_str: str | None = None) -> str:
    pipeline_config.ensure_dirs()
    d = date_str or today_yyyymmdd()
    path = pipeline_config.data_dir / f"universe_{d}.parquet"
    df.to_parquet(path, index=False)
    logger.info("저장: %s", path)
    return str(path)


def load_universe(date_str: str | None = None) -> pd.DataFrame:
    d = date_str or today_yyyymmdd()
    path = pipeline_config.data_dir / f"universe_{d}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Universe not found: {path}")
    return pd.read_parquet(path)
