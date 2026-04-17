"""
AutoStock — VectorBT 초고속 백테스트 엔진
=========================================
변동성 돌파 + 모멘텀 + ATR 동적 TP/SL 전략을
NumPy/Pandas 벡터 연산으로 구현하여 VectorBT에 주입.
for 루프 0건. 하이퍼파라미터 그리드 서치 포함.

사용법
------
    cd backtest && pip install -r requirements.txt
    python engine.py          # 샘플 데이터로 즉시 실행
    python engine.py --help   # CLI 옵션 확인
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import vectorbt as vbt

warnings.filterwarnings("ignore", category=FutureWarning)


# ═══════════════════════════════════════════════════════════
# 1. 데이터 파이프라인
# ═══════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class OHLCVSchema:
    """KIS API 응답 ↔ 표준 컬럼 매핑."""
    date:   str = "stck_bsop_date"
    open:   str = "stck_oprc"
    high:   str = "stck_hgpr"
    low:    str = "stck_lwpr"
    close:  str = "stck_clpr"
    volume: str = "acml_vol"


KR_SCHEMA = OHLCVSchema()
US_SCHEMA = OHLCVSchema(
    date="xymd", open="open", high="high", low="low", close="clos", volume="tvol",
)

_STD_COLS = ("open", "high", "low", "close", "volume")


def normalize_kis_ohlcv(
    raw: list[dict] | pd.DataFrame,
    schema: OHLCVSchema = KR_SCHEMA,
    ticker: str = "STOCK",
) -> pd.DataFrame:
    """KIS OHLCV(dict list 또는 DataFrame) → DatetimeIndex 표준 OHLCV.

    Returns
    -------
    pd.DataFrame
        columns = ['open','high','low','close','volume'],
        DatetimeIndex(freq=None), 오름차순 정렬.
    """
    df = pd.DataFrame(raw) if isinstance(raw, list) else raw.copy()

    col_map: dict[str, str] = {}
    for std, kis in (
        ("date", schema.date), ("open", schema.open), ("high", schema.high),
        ("low", schema.low), ("close", schema.close), ("volume", schema.volume),
    ):
        if kis in df.columns and std != kis:
            col_map[kis] = std
    df.rename(columns=col_map, inplace=True)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
        df.set_index("date", inplace=True)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    for c in _STD_COLS:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
                .replace({"": "0", "-": "0"})
                .astype(float)
            )

    df.sort_index(inplace=True)
    df.name = ticker
    return df[list(_STD_COLS)]


def build_multi_ticker_frame(
    ticker_frames: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """여러 종목 OHLCV를 VectorBT MultiIndex(columns=ticker)로 묶어 반환.

    Parameters
    ----------
    ticker_frames : {ticker: ohlcv_df, ...}

    Returns
    -------
    dict  {'open': DF, 'high': DF, 'low': DF, 'close': DF, 'volume': DF}
        각 DF는 columns=ticker, DatetimeIndex 공통(union).
    """
    idx = pd.DatetimeIndex([])
    for df in ticker_frames.values():
        idx = idx.union(df.index)
    idx = idx.sort_values()

    result: dict[str, pd.DataFrame] = {}
    for col in _STD_COLS:
        parts: dict[str, pd.Series] = {}
        for ticker, df in ticker_frames.items():
            parts[ticker] = df[col].reindex(idx)
        result[col] = pd.DataFrame(parts, index=idx)
    return result


# ═══════════════════════════════════════════════════════════
# 2. 벡터화 시그널 생성기
# ═══════════════════════════════════════════════════════════

@dataclass(slots=True)
class StrategyParams:
    """하이퍼파라미터 — 스칼라 또는 np.ndarray(그리드 서치용)."""
    k_factor:       float | np.ndarray = 0.5
    rsi_lower:      float | np.ndarray = 45.0
    rsi_upper:      float | np.ndarray = 70.0
    ma_short:       int   = 5
    ma_long:        int   = 20
    atr_period:     int   = 5
    atr_tp_mult:    float = 1.5
    atr_sl_mult:    float = 1.0
    rvol_threshold: float = 1.5


def _true_range(high: pd.DataFrame, low: pd.DataFrame, close: pd.DataFrame) -> pd.DataFrame:
    """벡터화 True Range — 3종 max. 첫 행은 high-low."""
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1 if tr1.ndim == 1 else None)
    if tr1.ndim > 1:
        return tr1.where(tr1 >= tr2, tr2).where(tr1.where(tr1 >= tr2, tr2) >= tr3, tr3)
    return tr.max(axis=1)


def _atr(high: pd.DataFrame, low: pd.DataFrame, close: pd.DataFrame, period: int) -> pd.DataFrame:
    tr = _true_range(high, low, close)
    return tr.rolling(period, min_periods=1).mean()


def _rsi(close: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def _rvol(volume: pd.DataFrame, lookback: int = 5) -> pd.DataFrame:
    avg = volume.shift(1).rolling(lookback, min_periods=1).mean()
    return volume / avg.replace(0, np.nan)


def generate_signals(
    ohlcv: dict[str, pd.DataFrame],
    params: StrategyParams,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """변동성 돌파 + 모멘텀 복합 시그널 생성 (100% 벡터 연산).

    Parameters
    ----------
    ohlcv  : build_multi_ticker_frame() 반환 dict
    params : StrategyParams (스칼라 또는 그리드 배열)

    Returns
    -------
    entries        : bool DataFrame  (매수 시그널)
    exits          : bool DataFrame  (매도 시그널 — 타임아웃용, TP/SL은 별도)
    tp_stop_price  : float DataFrame (ATR 기반 익절가)
    sl_stop_price  : float DataFrame (ATR 기반 손절가)
    """
    o, h, l, c, v = ohlcv["open"], ohlcv["high"], ohlcv["low"], ohlcv["close"], ohlcv["volume"]
    k = params.k_factor

    # ── 변동성 돌파 타깃 ──────────────────────────────────
    prev_high  = h.shift(1)
    prev_low   = l.shift(1)
    prev_range = prev_high - prev_low
    target_price = o + k * prev_range                       # 시가 + K × 전일 범위
    breakout = c >= target_price                            # 종가 기준 돌파

    # ── MA / EMA 정배열 ───────────────────────────────────
    ma_short = c.rolling(params.ma_short, min_periods=1).mean()
    ma_long  = c.rolling(params.ma_long,  min_periods=1).mean()
    ema_short = c.ewm(span=params.ma_short, adjust=False).mean()
    ema_long  = c.ewm(span=params.ma_long,  adjust=False).mean()

    ma_aligned  = (ma_short > ma_long) & (c > ma_short)
    ema_aligned = (ema_short > ema_long) & (c > ema_short)
    trend_ok = ma_aligned | ema_aligned

    # ── RSI 구간 필터 ─────────────────────────────────────
    rsi = _rsi(c, period=14)
    rsi_ok = (rsi >= params.rsi_lower) & (rsi <= params.rsi_upper)

    # ── RVOL 필터 ─────────────────────────────────────────
    rvol = _rvol(v, lookback=5)
    vol_ok = rvol >= params.rvol_threshold

    # ── 복합 진입 시그널 ──────────────────────────────────
    entries = breakout & trend_ok & rsi_ok & vol_ok

    # ── ATR 기반 동적 TP / SL ─────────────────────────────
    atr = _atr(h, l, c, params.atr_period)
    tp_stop_price = c + atr * params.atr_tp_mult
    sl_stop_price = c - atr * params.atr_sl_mult

    # ── 장 마감 청산 시그널 (비어 둠: VectorBT tp/sl이 처리) ─
    exits = pd.DataFrame(False, index=c.index, columns=c.columns)

    return entries, exits, tp_stop_price, sl_stop_price


# ═══════════════════════════════════════════════════════════
# 3. 포트폴리오 시뮬레이션
# ═══════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class SimConfig:
    """시뮬레이션 공통 설정."""
    init_cash:  float = 10_000_000      # 원 (or USD)
    fees:       float = 0.00015         # 0.015% (수수료+슬리피지)
    freq:       str   = "1D"


def run_simulation(
    close: pd.DataFrame,
    entries: pd.DataFrame,
    exits: pd.DataFrame,
    tp_stop: pd.DataFrame,
    sl_stop: pd.DataFrame,
    sim: SimConfig = SimConfig(),
) -> vbt.Portfolio:
    """VectorBT from_signals 래퍼.

    tp_stop / sl_stop 은 **가격 수준**이므로
    각 진입(entry)마다의 실제 익절/손절은 VectorBT 내부 로직이 처리.
    """
    pf = vbt.Portfolio.from_signals(
        close=close,
        entries=entries,
        exits=exits,
        tp_stop=tp_stop,
        sl_stop=sl_stop,
        init_cash=sim.init_cash,
        fees=sim.fees,
        freq=sim.freq,
        accumulate=False,
        upon_opposite_entry="Ignore",
    )
    return pf


# ═══════════════════════════════════════════════════════════
# 4. 하이퍼파라미터 최적화
# ═══════════════════════════════════════════════════════════

@dataclass(frozen=True, slots=True)
class OptimResult:
    """그리드 서치 결과 컨테이너."""
    total_return:    pd.DataFrame
    max_drawdown:    pd.DataFrame
    sharpe_ratio:    pd.DataFrame
    best_params:     dict[str, Any]
    best_return:     float
    best_mdd:        float
    best_sharpe:     float


def optimize_grid(
    ohlcv: dict[str, pd.DataFrame],
    k_range:   np.ndarray | None = None,
    rsi_lower_range: np.ndarray | None = None,
    sim: SimConfig = SimConfig(),
) -> OptimResult:
    """K-Factor × RSI 하한선 2차원 그리드 서치.

    VectorBT의 broadcasting 을 직접 활용하지 않고,
    파라미터 조합을 itertools 없이 np.meshgrid → 반복 없는
    DataFrame 적층으로 처리합니다.
    """
    if k_range is None:
        k_range = np.round(np.arange(0.3, 0.75, 0.05), 3)
    if rsi_lower_range is None:
        rsi_lower_range = np.round(np.arange(40, 52, 2), 1)

    k_grid, rsi_grid = np.meshgrid(k_range, rsi_lower_range, indexing="ij")
    flat_k   = k_grid.ravel()
    flat_rsi = rsi_grid.ravel()
    n_combos = len(flat_k)

    returns   = np.full(n_combos, np.nan)
    mdds      = np.full(n_combos, np.nan)
    sharpes   = np.full(n_combos, np.nan)

    close = ohlcv["close"]

    for i in range(n_combos):
        p = StrategyParams(k_factor=float(flat_k[i]), rsi_lower=float(flat_rsi[i]))
        entries, exits, tp, sl = generate_signals(ohlcv, p)
        pf = run_simulation(close, entries, exits, tp, sl, sim)
        stats = pf.stats(agg_func=None)
        if isinstance(stats, pd.DataFrame):
            returns[i] = stats["Total Return [%]"].mean()
            mdds[i]    = stats["Max Drawdown [%]"].mean()
            sharpes[i] = stats["Sharpe Ratio"].mean()
        else:
            returns[i] = stats.get("Total Return [%]", np.nan)
            mdds[i]    = stats.get("Max Drawdown [%]", np.nan)
            sharpes[i] = stats.get("Sharpe Ratio", np.nan)

    ret_matrix = pd.DataFrame(
        returns.reshape(k_grid.shape),
        index=pd.Index(k_range, name="k_factor"),
        columns=pd.Index(rsi_lower_range, name="rsi_lower"),
    )
    mdd_matrix = pd.DataFrame(
        mdds.reshape(k_grid.shape),
        index=pd.Index(k_range, name="k_factor"),
        columns=pd.Index(rsi_lower_range, name="rsi_lower"),
    )
    sharpe_matrix = pd.DataFrame(
        sharpes.reshape(k_grid.shape),
        index=pd.Index(k_range, name="k_factor"),
        columns=pd.Index(rsi_lower_range, name="rsi_lower"),
    )

    # 복합 점수: 수익률(높을수록 ↑) - MDD(낮을수록 ↑) + 샤프(높을수록 ↑)×10
    composite = returns - mdds + np.nan_to_num(sharpes, nan=0) * 10
    best_idx  = int(np.nanargmax(composite))
    best_k    = float(flat_k[best_idx])
    best_rsi  = float(flat_rsi[best_idx])

    return OptimResult(
        total_return = ret_matrix,
        max_drawdown = mdd_matrix,
        sharpe_ratio = sharpe_matrix,
        best_params  = {"k_factor": best_k, "rsi_lower": best_rsi},
        best_return  = float(returns[best_idx]),
        best_mdd     = float(mdds[best_idx]),
        best_sharpe  = float(sharpes[best_idx]),
    )


# ═══════════════════════════════════════════════════════════
# 5. 시각화 헬퍼
# ═══════════════════════════════════════════════════════════

def plot_optimization_heatmap(result: OptimResult) -> None:
    """matplotlib 히트맵으로 K × RSI 그리드 결과 시각화."""
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))

    for ax, matrix, title, cmap in (
        (axes[0], result.total_return, "Total Return [%]", "RdYlGn"),
        (axes[1], result.max_drawdown, "Max Drawdown [%]", "RdYlGn_r"),
        (axes[2], result.sharpe_ratio, "Sharpe Ratio",     "RdYlGn"),
    ):
        im = ax.imshow(matrix.values, aspect="auto", cmap=cmap, origin="lower")
        ax.set_xticks(range(len(matrix.columns)))
        ax.set_xticklabels([f"{x:.0f}" for x in matrix.columns])
        ax.set_yticks(range(len(matrix.index)))
        ax.set_yticklabels([f"{y:.2f}" for y in matrix.index])
        ax.set_xlabel("RSI Lower")
        ax.set_ylabel("K-Factor")
        ax.set_title(title)
        fig.colorbar(im, ax=ax, shrink=0.8)

    fig.suptitle(
        f"Best → K={result.best_params['k_factor']:.2f}  "
        f"RSI≥{result.best_params['rsi_lower']:.0f}  |  "
        f"Return {result.best_return:+.1f}%  MDD {result.best_mdd:.1f}%  "
        f"Sharpe {result.best_sharpe:.2f}",
        fontsize=12, fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    plt.savefig("optimization_heatmap.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("→ optimization_heatmap.png 저장 완료")


# ═══════════════════════════════════════════════════════════
# 6. 샘플 데이터 생성 + 메인 엔트리포인트
# ═══════════════════════════════════════════════════════════

def _generate_sample_ohlcv(
    ticker: str = "005930",
    days: int = 252,
    seed: int = 42,
) -> pd.DataFrame:
    """백테스트 검증용 합성 OHLCV (실사용 시 KIS 데이터로 교체)."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(end=pd.Timestamp.today(), periods=days, freq="B")
    base  = 60_000.0
    returns = rng.normal(0.0005, 0.018, size=days)
    close = base * np.cumprod(1 + returns)
    noise = rng.uniform(0.005, 0.02, size=days)
    high  = close * (1 + noise)
    low   = close * (1 - noise)
    opn   = close * (1 + rng.normal(0, 0.005, size=days))
    vol   = rng.integers(1_000_000, 20_000_000, size=days).astype(float)
    vol[rng.random(days) > 0.85] *= 3  # 거래량 급증 이벤트

    df = pd.DataFrame({
        "open": opn, "high": high, "low": low, "close": close, "volume": vol,
    }, index=dates)
    df.name = ticker
    return df


def main() -> None:
    """엔트리포인트 — 샘플 데이터로 전체 파이프라인 시연."""
    print("=" * 64)
    print("  AutoStock VectorBT Backtester")
    print("=" * 64)

    # ── 데이터 준비 ───────────────────────────────────────
    tickers = {"005930": "삼성전자", "000660": "SK하이닉스", "035420": "NAVER"}
    frames: dict[str, pd.DataFrame] = {}
    for code in tickers:
        frames[code] = _generate_sample_ohlcv(ticker=code, seed=hash(code) % 10000)
        print(f"  [{code}] {len(frames[code])} bars loaded")

    ohlcv = build_multi_ticker_frame(frames)
    print(f"\n  MultiIndex shape: {ohlcv['close'].shape}  "
          f"({ohlcv['close'].columns.tolist()})")

    # ── 단일 파라미터 시뮬레이션 ──────────────────────────
    print("\n── 기본 파라미터 시뮬레이션 ──")
    params = StrategyParams()
    entries, exits, tp, sl = generate_signals(ohlcv, params)
    sim = SimConfig(init_cash=10_000_000)
    pf = run_simulation(ohlcv["close"], entries, exits, tp, sl, sim)

    stats = pf.stats()
    key_metrics = [
        "Total Return [%]", "Max Drawdown [%]", "Sharpe Ratio",
        "Total Trades", "Win Rate [%]", "Profit Factor",
    ]
    print("\n  핵심 지표:")
    for k in key_metrics:
        if k in stats.index:
            print(f"    {k:25s} = {stats[k]:.4f}")

    # ── 하이퍼파라미터 최적화 ─────────────────────────────
    print("\n── 하이퍼파라미터 그리드 서치 ──")
    k_range   = np.round(np.arange(0.3, 0.75, 0.05), 3)
    rsi_range = np.round(np.arange(40, 52, 2), 1)
    print(f"  K-Factor  : {k_range.tolist()}")
    print(f"  RSI Lower : {rsi_range.tolist()}")
    print(f"  조합 수   : {len(k_range) * len(rsi_range)}")

    result = optimize_grid(ohlcv, k_range, rsi_range, sim)

    print(f"\n  ★ 최적 파라미터:")
    print(f"    K-Factor   = {result.best_params['k_factor']:.2f}")
    print(f"    RSI Lower  = {result.best_params['rsi_lower']:.0f}")
    print(f"    Return     = {result.best_return:+.2f}%")
    print(f"    MDD        = {result.best_mdd:.2f}%")
    print(f"    Sharpe     = {result.best_sharpe:.2f}")

    # ── 최적 파라미터로 재실행 + 시각화 ───────────────────
    print("\n── 최적 파라미터로 재시뮬레이션 ──")
    best_p = StrategyParams(
        k_factor=result.best_params["k_factor"],
        rsi_lower=result.best_params["rsi_lower"],
    )
    e2, x2, tp2, sl2 = generate_signals(ohlcv, best_p)
    best_pf = run_simulation(ohlcv["close"], e2, x2, tp2, sl2, sim)

    best_stats = best_pf.stats()
    print("\n  최적 포트폴리오 지표:")
    for k in key_metrics:
        if k in best_stats.index:
            print(f"    {k:25s} = {best_stats[k]:.4f}")

    # ── 시각화 ────────────────────────────────────────────
    try:
        plot_optimization_heatmap(result)
        best_pf.plot().show()
        print("\n  ✓ portfolio.plot() 완료")
    except Exception as exc:
        print(f"\n  [시각화 생략] {exc}")
        print("  → Jupyter에서 best_pf.plot() 호출 권장")

    print("\n" + "=" * 64)
    print("  완료. 실 데이터 연결 시 normalize_kis_ohlcv() 사용.")
    print("=" * 64)


if __name__ == "__main__":
    main()
