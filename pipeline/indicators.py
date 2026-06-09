"""Technical indicators — pandas + pandas_ta with pure-pandas fallback."""

from __future__ import annotations

from typing import Any

import pandas as pd

try:
    import pandas_ta as ta
except ImportError:  # pragma: no cover
    ta = None  # type: ignore[assignment]


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None:
        return default
    s = str(val).strip().replace(",", "")
    if not s or s in ("-", ".", "--"):
        return default
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


def ohlcv_rows_to_frame(rows: list[dict]) -> pd.DataFrame:
    """KIS daily rows (newest first) → ascending DataFrame."""
    if not rows:
        return pd.DataFrame()

    records = []
    for r in rows:
        records.append(
            {
                "date": r.get("stck_bsop_date", ""),
                "open": _safe_float(r.get("stck_oprc")),
                "high": _safe_float(r.get("stck_hgpr")),
                "low": _safe_float(r.get("stck_lwpr")),
                "close": _safe_float(r.get("stck_clpr")),
                "volume": int(_safe_float(r.get("acml_vol"))),
            }
        )
    df = pd.DataFrame(records)
    if df.empty:
        return df
    return df.sort_values("date").reset_index(drop=True)


def calc_rsi_wilder(closes: list[float], period: int = 14) -> float:
    """Wilder RSI on [newest→oldest] closes."""
    if len(closes) < period + 2:
        return 50.0
    c = list(reversed(closes))
    diffs = [c[i] - c[i - 1] for i in range(1, len(c))]
    gains = [max(d, 0.0) for d in diffs]
    losses = [abs(min(d, 0.0)) for d in diffs]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def compute_indicators(df: pd.DataFrame, price_output: dict | None = None) -> dict[str, float | int | bool]:
    """Compute MA, RSI, volume ratio, 52w stats from OHLCV + optional price snapshot."""
    out: dict[str, float | int | bool] = {
        "price": 0.0,
        "ma5": 0.0,
        "ma20": 0.0,
        "ma60": 0.0,
        "ma120": 0.0,
        "rsi14": 50.0,
        "vol_ratio": 0.0,
        "avg20_vol": 0.0,
        "today_vol": 0.0,
        "52w_high": 0.0,
        "52w_low": 0.0,
        "52w_high_pct": 0.0,
        "ma_alignment": 0,
    }
    if df.empty:
        return out

    closes = df["close"].tolist()
    volumes = df["volume"].tolist()
    price = closes[-1] if closes else 0.0

    if price_output:
        api_price = _safe_float(price_output.get("stck_prpr"))
        if api_price > 0:
            price = api_price
        w52_h = _safe_float(price_output.get("w52_hgpr") or price_output.get("d250_hgpr"))
        w52_l = _safe_float(price_output.get("w52_lwpr") or price_output.get("d250_lwpr"))
        if w52_h > 0:
            out["52w_high"] = w52_h
        if w52_l > 0:
            out["52w_low"] = w52_l

    out["price"] = price

    if ta is not None and len(df) >= 20:
        s = pd.Series(closes)
        out["ma5"] = float(ta.sma(s, length=5).iloc[-1] or 0)
        out["ma20"] = float(ta.sma(s, length=20).iloc[-1] or 0)
        if len(df) >= 60:
            out["ma60"] = float(ta.sma(s, length=60).iloc[-1] or 0)
        if len(df) >= 120:
            out["ma120"] = float(ta.sma(s, length=120).iloc[-1] or 0)
        rsi_series = ta.rsi(s, length=14)
        if rsi_series is not None and not rsi_series.empty:
            out["rsi14"] = float(rsi_series.iloc[-1] or 50)
    else:
        if len(closes) >= 5:
            out["ma5"] = sum(closes[-5:]) / 5
        if len(closes) >= 20:
            out["ma20"] = sum(closes[-20:]) / 20
        if len(closes) >= 60:
            out["ma60"] = sum(closes[-60:]) / 60
        if len(closes) >= 120:
            out["ma120"] = sum(closes[-120:]) / 120
        out["rsi14"] = calc_rsi_wilder(list(reversed(closes)), 14)

    if len(volumes) >= 21:
        today_vol = volumes[-1]
        avg20 = sum(volumes[-21:-1]) / 20
        out["today_vol"] = today_vol
        out["avg20_vol"] = avg20
        out["vol_ratio"] = round(today_vol / avg20, 3) if avg20 > 0 else 0.0
    elif len(volumes) >= 2:
        today_vol = volumes[-1]
        avg20 = sum(volumes[:-1]) / max(len(volumes) - 1, 1)
        out["today_vol"] = today_vol
        out["avg20_vol"] = avg20
        out["vol_ratio"] = round(today_vol / avg20, 3) if avg20 > 0 else 0.0

    if out["52w_high"] <= 0 and len(df) >= 20:
        lookback = min(252, len(df))
        out["52w_high"] = float(df["high"].tail(lookback).max())
        out["52w_low"] = float(df["low"].tail(lookback).min())

    if out["52w_high"] > 0:
        out["52w_high_pct"] = round(price / out["52w_high"], 4)

    alignment = 0
    ma_vals = [price, out["ma5"], out["ma20"], out["ma60"], out["ma120"]]
    for i in range(len(ma_vals) - 1):
        if ma_vals[i] > ma_vals[i + 1] > 0:
            alignment += 1
    out["ma_alignment"] = alignment

    return out
