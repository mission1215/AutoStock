"""
AutoStock VectorBT Backtest Engine
===================================
전문 퀀트 수준의 벡터화 백테스트 엔진.

주요 구성:
  - KisOhlcvLoader   : KIS API OHLCV dict → vbt 호환 MultiIndex DataFrame
  - Indicators       : 완전 벡터화 지표 계산 (MA/EMA/RSI/ATR/RVOL)
  - SignalFactory    : KR/US 진입·청산 신호 + ATR 기반 TP/SL
  - BacktestConfig   : 전략·포트폴리오 파라미터 데이터클래스
  - StrategyBacktest : vbt.Portfolio.from_signals() 래퍼
  - HyperOptimizer   : K-Factor × RSI 2D 그리드 최적화

빠른 시작:
  python backtest.py           # yfinance 샘플 실행
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import NamedTuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# ──────────────────────────────────────────────────────────────────────────────
# 선택적 의존성
# ──────────────────────────────────────────────────────────────────────────────
try:
    import vectorbt as vbt
    _VBT_AVAILABLE = True
except ImportError:
    _VBT_AVAILABLE = False
    warnings.warn(
        "vectorbt 미설치 — pip install 'vectorbt>=0.24' 후 재실행하세요.",
        ImportWarning,
        stacklevel=2,
    )

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False


# ═══════════════════════════════════════════════════════════════════════════════
# 1. KisOhlcvLoader
# ═══════════════════════════════════════════════════════════════════════════════

class KisOhlcvLoader:
    """KIS Open API OHLCV 원시 데이터를 백테스트용 DataFrame으로 변환.

    KIS `/uapi/domestic-stock/v1/quotations/inquire-daily-price` 또는
    `/uapi/overseas-price/v1/quotations/dailyprice` 응답의 `output2` 리스트를
    입력으로 받아 vbt/pandas 공용 OHLCV DataFrame을 반환합니다.

    Parameters
    ----------
    raw : list[dict]
        KIS API output2 레코드 리스트 (최신→과거 순서여도 자동 정렬됨).
    market : {"KR", "US"}
        종목 시장. 날짜/필드 파싱에 영향을 줍니다.
    ticker : str
        종목 코드 (예: "005930", "AAPL"). MultiIndex 컬럼 생성에 사용.
    """

    # KIS 필드명 매핑 (KR)
    _KR_FIELDS = {
        "date": "stck_bsop_date",   # YYYYMMDD
        "open": "stck_oprc",
        "high": "stck_hgpr",
        "low":  "stck_lwpr",
        "close": "stck_clpr",
        "volume": "acml_vol",
    }
    # KIS 필드명 매핑 (US)
    _US_FIELDS = {
        "date": "xymd",             # YYYYMMDD
        "open": "open",
        "high": "high",
        "low":  "low",
        "close": "clos",
        "volume": "tvol",
    }

    def __init__(self, raw: list[dict], market: str = "KR", ticker: str = "UNKNOWN"):
        if market not in ("KR", "US"):
            raise ValueError(f"market은 'KR' 또는 'US' 이어야 합니다. 입력값: {market!r}")
        self.raw = raw
        self.market = market
        self.ticker = ticker
        self._fields = self._KR_FIELDS if market == "KR" else self._US_FIELDS

    # ------------------------------------------------------------------
    def load(self) -> pd.DataFrame:
        """OHLCV DataFrame 반환 (DatetimeIndex, 오름차순 정렬).

        Returns
        -------
        pd.DataFrame
            컬럼: Open, High, Low, Close, Volume
        """
        if not self.raw:
            return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

        f = self._fields
        records = [
            {
                "Date":   r.get(f["date"], "19700101"),
                "Open":   float(str(r.get(f["open"],  0)).replace(",", "") or 0),
                "High":   float(str(r.get(f["high"],  0)).replace(",", "") or 0),
                "Low":    float(str(r.get(f["low"],   0)).replace(",", "") or 0),
                "Close":  float(str(r.get(f["close"], 0)).replace(",", "") or 0),
                "Volume": float(str(r.get(f["volume"], 0)).replace(",", "") or 0),
            }
            for r in self.raw
        ]

        df = pd.DataFrame(records)
        df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
        df = df.set_index("Date").sort_index()
        df = df[df["Close"] > 0]  # 0원 데이터 제거
        return df

    # ------------------------------------------------------------------
    def to_multi(self) -> pd.DataFrame:
        """MultiIndex 컬럼 DataFrame 반환 (vbt 멀티 심볼 분석 호환).

        Returns
        -------
        pd.DataFrame
            컬럼 레벨: (OHLCV, ticker)
        """
        df = self.load()
        df.columns = pd.MultiIndex.from_product([df.columns, [self.ticker]])
        return df

    # ------------------------------------------------------------------
    @classmethod
    def from_yfinance(cls, ticker: str, period: str = "2y", market: str = "KR") -> pd.DataFrame:
        """yfinance에서 OHLCV 로드 (KIS API 없이 빠른 테스트용).

        Parameters
        ----------
        ticker : str
            yfinance 심볼 (KR: "005930.KS", US: "AAPL").
        period : str
            yfinance 기간 문자열 (예: "1y", "2y", "5y").
        market : str
            사용하지 않음. 호환성을 위해 유지.
        """
        if not _YF_AVAILABLE:
            raise ImportError("pip install yfinance")
        raw_df = yf.download(ticker, period=period, auto_adjust=True, progress=False)
        raw_df = raw_df[["Open", "High", "Low", "Close", "Volume"]].dropna()
        raw_df.index.name = "Date"
        return raw_df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Indicators — 완전 벡터화 지표
# ═══════════════════════════════════════════════════════════════════════════════

class Indicators(NamedTuple):
    """벡터화 지표 계산 결과 컨테이너.

    모든 시리즈는 입력 DataFrame과 동일한 DatetimeIndex를 공유합니다.
    for-loop 없이 numpy/pandas 연산만 사용합니다.

    Attributes
    ----------
    ma5, ma20     : 단순 이동평균
    ema5, ema20   : 지수 이동평균 (KR 정배열 판정)
    ema9, ema21   : 지수 이동평균 (US 모멘텀 판정)
    rsi           : RSI(14)
    atr           : ATR(14) — 변동성 기반 TP/SL 산출에 사용
    rvol          : 상대 거래량 (당일 / 20일 평균)
    """
    ma5:   pd.Series
    ma20:  pd.Series
    ema5:  pd.Series
    ema20: pd.Series
    ema9:  pd.Series
    ema21: pd.Series
    rsi:   pd.Series
    atr:   pd.Series
    rvol:  pd.Series

    # ------------------------------------------------------------------
    @classmethod
    def compute(cls, df: pd.DataFrame) -> "Indicators":
        """OHLCV DataFrame → Indicators 계산.

        Parameters
        ----------
        df : pd.DataFrame
            컬럼: Open, High, Low, Close, Volume (DatetimeIndex)

        Returns
        -------
        Indicators
        """
        close  = df["Close"]
        high   = df["High"]
        low    = df["Low"]
        volume = df["Volume"]

        # ── 이동평균 ────────────────────────────────────────────────────
        ma5  = close.rolling(5,  min_periods=1).mean()
        ma20 = close.rolling(20, min_periods=1).mean()

        # pandas ewm은 완전 벡터화 (C 레벨 루프)
        ema5  = close.ewm(span=5,  adjust=False, min_periods=1).mean()
        ema20 = close.ewm(span=20, adjust=False, min_periods=1).mean()
        ema9  = close.ewm(span=9,  adjust=False, min_periods=1).mean()
        ema21 = close.ewm(span=21, adjust=False, min_periods=1).mean()

        # ── RSI(14) — pandas ewm 기반 완전 벡터화 ───────────────────────
        delta  = close.diff()
        gain   = delta.clip(lower=0)
        loss   = (-delta).clip(lower=0)
        avg_g  = gain.ewm(com=13, adjust=False, min_periods=14).mean()
        avg_l  = loss.ewm(com=13, adjust=False, min_periods=14).mean()
        rs     = avg_g / avg_l.replace(0, np.nan)
        rsi    = 100 - (100 / (1 + rs))
        rsi    = rsi.fillna(50)

        # ── ATR(14) — Wilder 스무딩 ──────────────────────────────────────
        prev_close = close.shift(1)
        tr = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low  - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr = tr.ewm(com=13, adjust=False, min_periods=1).mean()

        # ── 상대 거래량 ──────────────────────────────────────────────────
        vol_ma20 = volume.rolling(20, min_periods=1).mean().replace(0, np.nan)
        rvol     = (volume / vol_ma20).fillna(1.0)

        return cls(
            ma5=ma5, ma20=ma20,
            ema5=ema5, ema20=ema20,
            ema9=ema9, ema21=ema21,
            rsi=rsi, atr=atr, rvol=rvol,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SignalFactory — 진입·청산 신호 생성
# ═══════════════════════════════════════════════════════════════════════════════

class SignalFactory:
    """KR/US 전략 신호 + ATR 기반 TP/SL 비율 생성.

    모든 신호는 bool Series (True = 조건 충족).
    for-loop 없이 pandas 불리언 연산으로만 생성됩니다.

    Parameters
    ----------
    df   : OHLCV DataFrame
    ind  : Indicators (compute() 결과)
    cfg  : BacktestConfig
    """

    def __init__(self, df: pd.DataFrame, ind: Indicators, cfg: "BacktestConfig"):
        self.df  = df
        self.ind = ind
        self.cfg = cfg

    # ------------------------------------------------------------------
    # KR 전략 신호
    # ------------------------------------------------------------------
    def kr_entry(self) -> pd.Series:
        """KR 매수 신호.

        조건 (AND):
        1. EMA5 > EMA20 (정배열)
        2. Close > MA5
        3. RSI > rsi_lower (과매도 탈출)
        4. RVOL >= 1.0 (평균 이상 거래량)
        5. K-팩터 돌파: Close > prev_high + K × (prev_high - prev_low)
        """
        ind = self.ind
        cfg = self.cfg
        close = self.df["Close"]
        high  = self.df["High"]
        low   = self.df["Low"]

        prev_high = high.shift(1)
        prev_low  = low.shift(1)
        target    = prev_high + cfg.k_factor_kr * (prev_high - prev_low)

        ema_align   = ind.ema5 > ind.ema20
        above_ma5   = close > ind.ma5
        rsi_ok      = ind.rsi > cfg.rsi_lower
        vol_ok      = ind.rvol >= cfg.rvol_threshold
        k_breakout  = close > target

        entry = ema_align & above_ma5 & rsi_ok & vol_ok & k_breakout
        return entry.fillna(False)

    def kr_exit(self) -> pd.Series:
        """KR 매도 신호 (EMA 역배열 또는 RSI 과매수).

        조건 (OR):
        - EMA5 < EMA20 (역배열 전환)
        - RSI > rsi_upper
        - EOD: 마지막 바 (포트폴리오 레벨에서도 처리)
        """
        ind = self.ind
        ema_cross  = self.ind.ema5 < self.ind.ema20
        rsi_top    = self.ind.rsi > self.cfg.rsi_upper
        return (ema_cross | rsi_top).fillna(False)

    # ------------------------------------------------------------------
    # US 전략 신호
    # ------------------------------------------------------------------
    def us_entry(self) -> pd.Series:
        """US 매수 신호.

        조건 (AND):
        1. EMA9 > EMA21 (모멘텀 정배열)
        2. Close > EMA9
        3. RSI > rsi_lower
        4. RVOL >= 1.2 (미국은 더 엄격한 거래량 필터)
        5. K-팩터 돌파 (US K-factor 적용)
        6. 10일 신고가 돌파 (High == rolling max)
        """
        ind   = self.ind
        cfg   = self.cfg
        close = self.df["Close"]
        high  = self.df["High"]
        low   = self.df["Low"]

        prev_high = high.shift(1)
        prev_low  = low.shift(1)
        target    = prev_high + cfg.k_factor_us * (prev_high - prev_low)

        rolling_max_10 = high.rolling(10, min_periods=1).max()
        new_high_10    = high >= rolling_max_10

        ema_align   = ind.ema9 > ind.ema21
        above_ema9  = close > ind.ema9
        rsi_ok      = ind.rsi > cfg.rsi_lower
        vol_ok      = ind.rvol >= cfg.rvol_threshold_us
        k_breakout  = close > target

        entry = ema_align & above_ema9 & rsi_ok & vol_ok & k_breakout & new_high_10
        return entry.fillna(False)

    def us_exit(self) -> pd.Series:
        """US 매도 신호 (EMA 역배열 또는 RSI 과매수)."""
        ema_cross = self.ind.ema9 < self.ind.ema21
        rsi_top   = self.ind.rsi > self.cfg.rsi_upper
        return (ema_cross | rsi_top).fillna(False)

    # ------------------------------------------------------------------
    # ATR 기반 TP/SL
    # ------------------------------------------------------------------
    def atr_sl_fraction(self, atr_mult: float | None = None) -> pd.Series:
        """ATR 배수 기반 손절 비율 시리즈.

        예) ATR=1200원, Close=50000원, mult=2.0 → sl_stop=0.048 (4.8%)

        Parameters
        ----------
        atr_mult : float, optional
            ATR 배수. None이면 cfg.atr_sl_mult 사용.

        Returns
        -------
        pd.Series
            각 바에서의 손절 비율 (0~1)
        """
        mult  = atr_mult if atr_mult is not None else self.cfg.atr_sl_mult
        close = self.df["Close"].replace(0, np.nan)
        frac  = (self.ind.atr * mult / close).clip(upper=0.15)  # 최대 15% 캡
        return frac.fillna(self.cfg.fallback_sl)

    def atr_tp_fraction(self, atr_mult: float | None = None) -> pd.Series:
        """ATR 배수 기반 익절 비율 시리즈.

        Parameters
        ----------
        atr_mult : float, optional
            ATR 배수. None이면 cfg.atr_tp_mult 사용.
        """
        mult  = atr_mult if atr_mult is not None else self.cfg.atr_tp_mult
        close = self.df["Close"].replace(0, np.nan)
        frac  = (self.ind.atr * mult / close).clip(upper=0.30)
        return frac.fillna(self.cfg.fallback_tp)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. BacktestConfig — 전략 파라미터 데이터클래스
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class BacktestConfig:
    """백테스트 전략 및 포트폴리오 파라미터.

    모든 값에 기본값이 있으므로 필요한 항목만 오버라이드하면 됩니다.

    Examples
    --------
    >>> cfg = BacktestConfig(k_factor_kr=0.6, rsi_lower=45)
    """

    # ── K-팩터 ────────────────────────────────────────────────────────────────
    k_factor_kr: float = 0.5   # KR 변동성 돌파 계수
    k_factor_us: float = 0.3   # US 모멘텀 돌파 계수

    # ── RSI 경계 ─────────────────────────────────────────────────────────────
    rsi_lower:   float = 45.0  # 매수 하한 (최적화 타깃: 40~50)
    rsi_upper:   float = 75.0  # 매도 상한

    # ── 거래량 필터 ──────────────────────────────────────────────────────────
    rvol_threshold:    float = 1.0  # KR 최소 RVOL
    rvol_threshold_us: float = 1.2  # US 최소 RVOL

    # ── ATR 기반 TP/SL ───────────────────────────────────────────────────────
    atr_sl_mult:  float = 2.0   # 손절 = ATR × 2.0
    atr_tp_mult:  float = 4.0   # 익절 = ATR × 4.0
    fallback_sl:  float = 0.02  # ATR 산출 불가 시 고정 손절 2%
    fallback_tp:  float = 0.04  # ATR 산출 불가 시 고정 익절 4%

    # ── 포트폴리오 ───────────────────────────────────────────────────────────
    init_cash:          float = 10_000_000.0  # 초기자금 (원 또는 USD)
    fees:               float = 0.00015       # 거래비용 편도 0.015%
    slippage:           float = 0.001         # 슬리피지 0.1%
    size_pct:           float = 0.10          # 진입당 자본 비율 10%

    # ── 최적화 그리드 ────────────────────────────────────────────────────────
    opt_k_factor_range: list[float] = field(
        default_factory=lambda: [round(v, 2) for v in np.arange(0.3, 0.71, 0.1)]
    )
    opt_rsi_lower_range: list[float] = field(
        default_factory=lambda: list(range(40, 51, 2))
    )


# ═══════════════════════════════════════════════════════════════════════════════
# 5. StrategyBacktest — vbt.Portfolio 래퍼
# ═══════════════════════════════════════════════════════════════════════════════

class StrategyBacktest:
    """단일 종목/파라미터 세트에 대한 전략 백테스트 실행기.

    Parameters
    ----------
    df     : OHLCV DataFrame (DatetimeIndex)
    cfg    : BacktestConfig
    market : "KR" | "US"

    Examples
    --------
    >>> df  = KisOhlcvLoader.from_yfinance("005930.KS")
    >>> bt  = StrategyBacktest(df, BacktestConfig(), market="KR")
    >>> pf  = bt.run()
    >>> print(bt.summary())
    >>> bt.plot()
    """

    def __init__(self, df: pd.DataFrame, cfg: BacktestConfig, market: str = "KR"):
        if not _VBT_AVAILABLE:
            raise RuntimeError("vectorbt 미설치. pip install 'vectorbt>=0.24'")
        if market not in ("KR", "US"):
            raise ValueError("market은 'KR' 또는 'US' 이어야 합니다.")
        self.df     = df.copy()
        self.cfg    = cfg
        self.market = market
        self._portfolio: "vbt.Portfolio | None" = None

    # ------------------------------------------------------------------
    def run(self) -> "vbt.Portfolio":
        """백테스트 실행 후 vbt.Portfolio 반환.

        Returns
        -------
        vbt.Portfolio
            성과 분석, 플롯 등 vbt 전체 API 사용 가능.
        """
        df  = self.df
        cfg = self.cfg
        ind = Indicators.compute(df)
        sf  = SignalFactory(df, ind, cfg)

        if self.market == "KR":
            entries = sf.kr_entry()
            exits   = sf.kr_exit()
        else:
            entries = sf.us_entry()
            exits   = sf.us_exit()

        sl_stop = sf.atr_sl_fraction()
        tp_stop = sf.atr_tp_fraction()

        size = cfg.size_pct * cfg.init_cash / df["Close"]

        self._portfolio = vbt.Portfolio.from_signals(
            close        = df["Close"],
            open         = df["Open"],
            high         = df["High"],
            low          = df["Low"],
            entries      = entries,
            exits        = exits,
            sl_stop      = sl_stop,
            tp_stop      = tp_stop,
            size         = size,
            size_type    = "shares",
            fees         = cfg.fees,
            slippage     = cfg.slippage,
            init_cash    = cfg.init_cash,
            freq         = "1D",
            accumulate   = False,  # 포지션 중복 방지
        )
        return self._portfolio

    # ------------------------------------------------------------------
    def summary(self) -> pd.Series:
        """핵심 성과 지표 반환.

        Returns
        -------
        pd.Series
            총수익률, CAGR, 샤프, 소티노, 최대낙폭, 승률, 거래횟수 등.
        """
        if self._portfolio is None:
            self.run()
        pf = self._portfolio
        stats = pf.stats()
        keys = [
            "Start", "End", "Period",
            "Total Return [%]", "Annualized Return [%]",
            "Sharpe Ratio", "Sortino Ratio",
            "Max Drawdown [%]", "Max Drawdown Duration",
            "Win Rate [%]", "Total Trades",
            "Profit Factor",
        ]
        return stats[[k for k in keys if k in stats.index]]

    # ------------------------------------------------------------------
    def plot(self, show: bool = True) -> None:
        """포트폴리오 성과 시각화.

        vbt.Portfolio.plot() 활용:
        - 상단: 자산가치 추이 (equity curve)
        - 하단: 낙폭 (drawdown)
        - 진입/청산 지점 마커 표시

        Parameters
        ----------
        show : bool
            True면 즉시 브라우저/창 표시, False면 Figure 반환 (Jupyter 등).
        """
        if self._portfolio is None:
            self.run()
        # vbt는 plotly 기반 — 브라우저 또는 Jupyter에서 인터랙티브 시각화
        fig = self._portfolio.plot()
        if show:
            fig.show()
        return fig


# ═══════════════════════════════════════════════════════════════════════════════
# 6. PaperTrader — 백테스트 파라미터 검증용 페이퍼 트레이딩
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class PaperTrade:
    """단일 페이퍼 트레이드 기록."""
    date:        str
    ticker:      str
    side:        str          # "buy" | "sell"
    price:       float
    quantity:    float
    pnl:         float = 0.0  # 청산 시 실현 손익
    reason:      str   = ""   # "entry" | "sl" | "tp" | "exit"


class PaperTrader:
    """백테스트에서 최적화된 파라미터로 최근 데이터에 페이퍼 트레이딩을 실행.

    HyperOptimizer로 찾은 최적 K-Factor/RSI를 실제 투입 전에
    가장 최근 N일 데이터로 앞방향(forward) 검증합니다.

    사용 흐름::

        # 1. 백테스트로 최적 파라미터 탐색
        opt    = HyperOptimizer(hist_df, cfg, market="KR")
        result = opt.run()
        best   = result.best_params   # {'k_factor': 0.5, 'rsi_lower': 44}

        # 2. 최적 파라미터로 BacktestConfig 생성
        best_cfg = BacktestConfig(k_factor_kr=best['k_factor'],
                                  rsi_lower=best['rsi_lower'])

        # 3. 최근 60일로 페이퍼 트레이딩 (실투입 전 검증)
        pt = PaperTrader(recent_df, best_cfg, market="KR", window=60)
        pt.run()
        print(pt.summary())
        pt.plot_equity()

        # 4. 백테스트 대비 성과 비교
        pt.compare(bt_stats)   # StrategyBacktest.summary() 결과와 비교

    Parameters
    ----------
    df     : OHLCV DataFrame (DatetimeIndex, 충분히 긴 기간)
    cfg    : BacktestConfig
    market : "KR" | "US"
    window : 페이퍼 트레이딩 적용 기간 (최근 N 거래일). 나머지는 인디케이터 워밍업.
    """

    def __init__(
        self,
        df:     pd.DataFrame,
        cfg:    BacktestConfig,
        market: str = "KR",
        window: int = 60,
    ):
        if market not in ("KR", "US"):
            raise ValueError("market은 'KR' 또는 'US'")
        self.df      = df.copy()
        self.cfg     = cfg
        self.market  = market
        self.window  = window
        self.trades: list[PaperTrade] = []
        self._equity: list[tuple[str, float]] = []  # (date, equity)

    # ------------------------------------------------------------------
    def run(self) -> "PaperTrader":
        """페이퍼 트레이딩 실행.

        전체 df로 인디케이터를 계산하되, 마지막 window 바만 신호 평가 적용.
        ATR 기반 TP/SL을 각 바에서 동적으로 계산하여 관리합니다.

        Returns
        -------
        self (메서드 체이닝 가능)
        """
        df  = self.df
        cfg = self.cfg
        ind = Indicators.compute(df)
        sf  = SignalFactory(df, ind, cfg)

        if self.market == "KR":
            entries = sf.kr_entry()
            exits   = sf.kr_exit()
        else:
            entries = sf.us_entry()
            exits   = sf.us_exit()

        sl_frac = sf.atr_sl_fraction()
        tp_frac = sf.atr_tp_fraction()

        # 페이퍼 트레이딩 구간 (마지막 window 바)
        paper_idx = df.index[-self.window:]
        cash      = cfg.init_cash
        position: dict | None = None  # {'price', 'qty', 'sl', 'tp', 'date'}

        equity_series: list[float] = []

        for dt in paper_idx:
            row     = df.loc[dt]
            close   = float(row["Close"])
            entry   = bool(entries.loc[dt])
            exit_   = bool(exits.loc[dt])
            sl_stop = float(sl_frac.loc[dt])
            tp_stop = float(tp_frac.loc[dt])

            date_str = dt.strftime("%Y-%m-%d")
            fee_rate = cfg.fees + cfg.slippage

            # ── 포지션 보유 중 ───────────────────────────────────
            if position is not None:
                sl_price = position["sl"]
                tp_price = position["tp"]

                # 손절
                if close <= sl_price:
                    pnl = (close - position["price"]) * position["qty"] * (1 - fee_rate)
                    cash += position["price"] * position["qty"] + pnl
                    self.trades.append(PaperTrade(
                        date=date_str, ticker=self.market,
                        side="sell", price=close,
                        quantity=position["qty"], pnl=pnl, reason="sl",
                    ))
                    position = None

                # 익절
                elif close >= tp_price:
                    pnl = (close - position["price"]) * position["qty"] * (1 - fee_rate)
                    cash += position["price"] * position["qty"] + pnl
                    self.trades.append(PaperTrade(
                        date=date_str, ticker=self.market,
                        side="sell", price=close,
                        quantity=position["qty"], pnl=pnl, reason="tp",
                    ))
                    position = None

                # 신호 청산
                elif exit_:
                    pnl = (close - position["price"]) * position["qty"] * (1 - fee_rate)
                    cash += position["price"] * position["qty"] + pnl
                    self.trades.append(PaperTrade(
                        date=date_str, ticker=self.market,
                        side="sell", price=close,
                        quantity=position["qty"], pnl=pnl, reason="exit",
                    ))
                    position = None

            # ── 포지션 없음 + 진입 신호 ──────────────────────────
            if position is None and entry and close > 0:
                invest = cash * cfg.size_pct
                qty    = invest / close
                cost   = invest * (1 + fee_rate)
                if cost <= cash:
                    cash -= cost
                    position = {
                        "price": close,
                        "qty":   qty,
                        "sl":    close * (1 - sl_stop),
                        "tp":    close * (1 + tp_stop),
                        "date":  date_str,
                    }
                    self.trades.append(PaperTrade(
                        date=date_str, ticker=self.market,
                        side="buy", price=close, quantity=qty, reason="entry",
                    ))

            # ── 자산 기록 ─────────────────────────────────────────
            open_value = position["qty"] * close if position else 0
            equity_series.append(cash + open_value)

        self._equity = list(zip(
            [dt.strftime("%Y-%m-%d") for dt in paper_idx],
            equity_series,
        ))
        return self

    # ------------------------------------------------------------------
    def summary(self) -> pd.Series:
        """페이퍼 트레이딩 성과 요약.

        Returns
        -------
        pd.Series
            총수익률, 승률, 거래횟수, 최대낙폭, 평균 손익비 등.
        """
        if not self._equity:
            raise RuntimeError("run()을 먼저 호출하세요.")

        equities    = np.array([e for _, e in self._equity])
        init_eq     = self.cfg.init_cash
        final_eq    = equities[-1]
        total_ret   = (final_eq - init_eq) / init_eq * 100

        # 낙폭
        peak        = np.maximum.accumulate(equities)
        dd          = (peak - equities) / peak
        max_dd      = float(dd.max() * 100)

        # 거래 분석
        sells = [t for t in self.trades if t.side == "sell"]
        wins  = [t for t in sells if t.pnl > 0]
        loss_ = [t for t in sells if t.pnl <= 0]
        win_rate    = len(wins) / len(sells) * 100 if sells else 0
        avg_win     = np.mean([t.pnl for t in wins])  if wins  else 0
        avg_loss    = np.mean([t.pnl for t in loss_]) if loss_ else 0
        pf          = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

        return pd.Series({
            "Period (days)":       self.window,
            "Total Return [%]":    round(total_ret, 2),
            "Max Drawdown [%]":    round(max_dd, 2),
            "Win Rate [%]":        round(win_rate, 2),
            "Total Trades":        len(sells),
            "Profit Factor":       round(pf, 2),
            "Avg Win":             round(avg_win, 2),
            "Avg Loss":            round(avg_loss, 2),
            "Final Equity":        round(final_eq, 0),
        })

    # ------------------------------------------------------------------
    def compare(self, bt_stats: pd.Series) -> pd.DataFrame:
        """백테스트 통계와 페이퍼 트레이딩 통계를 나란히 비교.

        Parameters
        ----------
        bt_stats : StrategyBacktest.summary() 결과

        Returns
        -------
        pd.DataFrame
            컬럼: [Backtest, Paper], 행: 공통 지표
        """
        paper_stats = self.summary()
        common_keys = ["Total Return [%]", "Max Drawdown [%]", "Win Rate [%]",
                       "Total Trades", "Profit Factor"]

        rows = {}
        for k in common_keys:
            bt_val = bt_stats.get(k, "—")
            pp_val = paper_stats.get(k, "—")
            rows[k] = {"Backtest": bt_val, "Paper": pp_val}

        df = pd.DataFrame(rows).T
        print("\n=== 백테스트 vs 페이퍼 트레이딩 비교 ===")
        try:
            from tabulate import tabulate
            print(tabulate(df, headers="keys", floatfmt=".2f"))
        except ImportError:
            print(df.to_string())
        return df

    # ------------------------------------------------------------------
    def plot_equity(self) -> None:
        """자산곡선 시각화."""
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            raise ImportError("pip install matplotlib")

        dates   = [d for d, _ in self._equity]
        equities = [e for _, e in self._equity]

        fig, ax = plt.subplots(figsize=(12, 4))
        ax.plot(dates, equities, linewidth=1.5, color="#2563EB", label="페이퍼 자산")
        ax.axhline(self.cfg.init_cash, linestyle="--", color="gray", alpha=0.5,
                   label=f"초기 자금 {self.cfg.init_cash:,.0f}")

        # 매수/매도 마커
        for t in self.trades:
            if t.date in dates:
                i = dates.index(t.date)
                if t.side == "buy":
                    ax.scatter(t.date, equities[i], marker="^", color="#16a34a", s=80, zorder=5)
                else:
                    color = "#dc2626" if t.pnl < 0 else "#f97316"
                    ax.scatter(t.date, equities[i], marker="v", color=color, s=80, zorder=5)

        ax.set_title(f"페이퍼 트레이딩 자산곡선 ({self.market}, 최근 {self.window}일)")
        ax.set_ylabel("자산 (원)")
        ax.legend()
        ax.grid(alpha=0.3)
        plt.xticks(rotation=45, fontsize=7)
        plt.tight_layout()
        plt.show()


# ═══════════════════════════════════════════════════════════════════════════════
# 7. HyperOptimizer — K-Factor × RSI 2D 그리드 최적화
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class OptimResult:
    """최적화 결과 컨테이너.

    Attributes
    ----------
    heatmap_sharpe : pd.DataFrame
        행=K-Factor, 열=RSI lower, 값=샤프 비율.
    heatmap_return : pd.DataFrame
        행=K-Factor, 열=RSI lower, 값=총수익률(%).
    best_params    : dict
        최고 샤프 기준 최적 파라미터.
    best_sharpe    : float
        최적 파라미터의 샤프 비율.
    all_stats      : pd.DataFrame
        전체 파라미터 조합의 성과 롱폼 DataFrame.
    """
    heatmap_sharpe: pd.DataFrame
    heatmap_return: pd.DataFrame
    best_params:    dict
    best_sharpe:    float
    all_stats:      pd.DataFrame


class HyperOptimizer:
    """K-Factor × RSI lower bound 2D 그리드 최적화.

    vbt의 파라미터 그리드를 활용하여 단일 from_signals() 호출로
    모든 조합을 벡터화 실행합니다 (for-loop 없음).

    Parameters
    ----------
    df     : OHLCV DataFrame
    cfg    : BacktestConfig (기본 파라미터 + 그리드 범위 설정)
    market : "KR" | "US"

    Examples
    --------
    >>> opt = HyperOptimizer(df, BacktestConfig(), market="KR")
    >>> result = opt.run()
    >>> print(result.best_params)
    >>> opt.plot_heatmap(result)
    """

    def __init__(self, df: pd.DataFrame, cfg: BacktestConfig, market: str = "KR"):
        if not _VBT_AVAILABLE:
            raise RuntimeError("vectorbt 미설치. pip install 'vectorbt>=0.24'")
        self.df     = df.copy()
        self.cfg    = cfg
        self.market = market

    # ------------------------------------------------------------------
    def run(self) -> OptimResult:
        """2D 그리드 최적화 실행.

        최적화 축:
        - K-Factor : cfg.opt_k_factor_range  (기본 0.3, 0.4, 0.5, 0.6, 0.7)
        - RSI lower: cfg.opt_rsi_lower_range (기본 40, 42, 44, 46, 48, 50)

        Returns
        -------
        OptimResult
        """
        cfg = self.cfg
        df  = self.df

        k_vals   = cfg.opt_k_factor_range
        rsi_vals = cfg.opt_rsi_lower_range

        # ── 각 파라미터 조합에 대한 시리즈를 미리 벡터화 빌드 ─────────────
        # vbt.Portfolio.from_signals에 2D entries/exits/sl_stop/tp_stop 전달
        # 열 = 파라미터 조합, 행 = 날짜 → vbt가 내부적으로 벡터화 처리

        n_combos = len(k_vals) * len(rsi_vals)
        combos   = [(k, r) for k in k_vals for r in rsi_vals]

        # 각 조합별 신호/sl/tp를 열로 쌓기
        entry_cols = []
        exit_cols  = []
        sl_cols    = []
        tp_cols    = []
        col_names  = []

        for k, r in combos:
            combo_cfg = BacktestConfig(
                k_factor_kr   = k if self.market == "KR" else cfg.k_factor_kr,
                k_factor_us   = k if self.market == "US" else cfg.k_factor_us,
                rsi_lower     = r,
                rsi_upper     = cfg.rsi_upper,
                rvol_threshold    = cfg.rvol_threshold,
                rvol_threshold_us = cfg.rvol_threshold_us,
                atr_sl_mult   = cfg.atr_sl_mult,
                atr_tp_mult   = cfg.atr_tp_mult,
                fallback_sl   = cfg.fallback_sl,
                fallback_tp   = cfg.fallback_tp,
                init_cash     = cfg.init_cash,
                fees          = cfg.fees,
                slippage      = cfg.slippage,
                size_pct      = cfg.size_pct,
            )
            ind = Indicators.compute(df)
            sf  = SignalFactory(df, ind, combo_cfg)

            if self.market == "KR":
                entry_cols.append(sf.kr_entry())
                exit_cols.append(sf.kr_exit())
            else:
                entry_cols.append(sf.us_entry())
                exit_cols.append(sf.us_exit())

            sl_cols.append(sf.atr_sl_fraction())
            tp_cols.append(sf.atr_tp_fraction())
            col_names.append(f"k={k:.1f}_rsi={int(r)}")

        entries_df = pd.concat(entry_cols, axis=1, keys=col_names)
        exits_df   = pd.concat(exit_cols,  axis=1, keys=col_names)
        sl_df      = pd.concat(sl_cols,    axis=1, keys=col_names)
        tp_df      = pd.concat(tp_cols,    axis=1, keys=col_names)

        size = cfg.size_pct * cfg.init_cash / df["Close"]
        size_df = pd.concat([size] * n_combos, axis=1, keys=col_names)

        # ── vbt 벡터화 실행 ────────────────────────────────────────────
        pf = vbt.Portfolio.from_signals(
            close      = df["Close"],
            open       = df["Open"],
            high       = df["High"],
            low        = df["Low"],
            entries    = entries_df,
            exits      = exits_df,
            sl_stop    = sl_df,
            tp_stop    = tp_df,
            size       = size_df,
            size_type  = "shares",
            fees       = cfg.fees,
            slippage   = cfg.slippage,
            init_cash  = cfg.init_cash,
            freq       = "1D",
            accumulate = False,
        )

        stats_all = pf.stats(silence_warnings=True)

        # ── 히트맵 DataFrame 구성 ──────────────────────────────────────
        sharpe_vals = {}
        return_vals = {}

        for (k, r), col in zip(combos, col_names):
            col_stats = stats_all[col] if isinstance(stats_all, pd.DataFrame) else stats_all
            sh = float(col_stats.get("Sharpe Ratio", 0) or 0)
            tr = float(col_stats.get("Total Return [%]", 0) or 0)
            sharpe_vals.setdefault(k, {})[r]  = sh
            return_vals.setdefault(k, {})[r]  = tr

        heatmap_sharpe = pd.DataFrame(sharpe_vals).T.sort_index()
        heatmap_sharpe.index.name   = "K-Factor"
        heatmap_sharpe.columns.name = "RSI Lower"

        heatmap_return = pd.DataFrame(return_vals).T.sort_index()
        heatmap_return.index.name   = "K-Factor"
        heatmap_return.columns.name = "RSI Lower"

        # ── 최적 파라미터 탐색 ─────────────────────────────────────────
        flat_sharpe = {
            (k, r): sharpe_vals[k][r]
            for k in sharpe_vals
            for r in sharpe_vals[k]
        }
        best_kr     = max(flat_sharpe, key=flat_sharpe.get)
        best_sharpe = flat_sharpe[best_kr]

        best_params = {
            "k_factor" : best_kr[0],
            "rsi_lower": best_kr[1],
            "market"   : self.market,
        }

        # ── 롱폼 성과 DataFrame ────────────────────────────────────────
        rows = []
        for (k, r), col in zip(combos, col_names):
            col_stats = stats_all[col] if isinstance(stats_all, pd.DataFrame) else stats_all
            rows.append({
                "k_factor"  : k,
                "rsi_lower" : r,
                "sharpe"    : float(col_stats.get("Sharpe Ratio",         0) or 0),
                "return_pct": float(col_stats.get("Total Return [%]",     0) or 0),
                "max_dd_pct": float(col_stats.get("Max Drawdown [%]",     0) or 0),
                "win_rate"  : float(col_stats.get("Win Rate [%]",         0) or 0),
                "n_trades"  : int(col_stats.get("Total Trades",           0) or 0),
            })
        all_stats = pd.DataFrame(rows).sort_values("sharpe", ascending=False)

        return OptimResult(
            heatmap_sharpe = heatmap_sharpe,
            heatmap_return = heatmap_return,
            best_params    = best_params,
            best_sharpe    = best_sharpe,
            all_stats      = all_stats,
        )

    # ------------------------------------------------------------------
    def plot_heatmap(self, result: OptimResult, metric: str = "sharpe") -> None:
        """최적화 결과 히트맵 시각화.

        Parameters
        ----------
        result : OptimResult
        metric : "sharpe" | "return"
        """
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
        except ImportError:
            raise ImportError("pip install matplotlib seaborn")

        data  = result.heatmap_sharpe if metric == "sharpe" else result.heatmap_return
        title = f"{'Sharpe Ratio' if metric == 'sharpe' else 'Total Return (%)'} — {self.market} 전략"

        plt.figure(figsize=(10, 6))
        sns.heatmap(
            data.astype(float),
            annot=True,
            fmt=".2f",
            cmap="RdYlGn",
            linewidths=0.5,
            cbar_kws={"label": metric},
        )
        plt.title(title)
        plt.xlabel("RSI Lower Bound")
        plt.ylabel("K-Factor")
        plt.tight_layout()
        plt.show()

        bp = result.best_params
        print(f"\n최적 파라미터: K-Factor={bp['k_factor']:.1f}, RSI Lower={bp['rsi_lower']:.0f}")
        print(f"최고 샤프 비율: {result.best_sharpe:.4f}")
        print("\n상위 5개 조합:")
        try:
            from tabulate import tabulate
            print(tabulate(result.all_stats.head(5), headers="keys", floatfmt=".3f", showindex=False))
        except ImportError:
            print(result.all_stats.head(5).to_string(index=False))


# ═══════════════════════════════════════════════════════════════════════════════
# 8. 빠른 시작 예제
# ═══════════════════════════════════════════════════════════════════════════════

def run_example_kr(
    ticker: str = "005930.KS",
    period: str = "3y",
    optimize: bool = True,
) -> None:
    """KR 전략 백테스트 + 최적화 예제 (삼성전자 기본값).

    Parameters
    ----------
    ticker   : yfinance KR 심볼 (예: "005930.KS", "000660.KS")
    period   : 데이터 기간 (예: "1y", "3y", "5y")
    optimize : True면 K-Factor × RSI 그리드 최적화도 실행
    """
    if not _YF_AVAILABLE:
        print("yfinance 미설치: pip install yfinance")
        return
    if not _VBT_AVAILABLE:
        print("vectorbt 미설치: pip install 'vectorbt>=0.24'")
        return

    print(f"[KR] {ticker} 데이터 로딩 ({period})...")
    df = KisOhlcvLoader.from_yfinance(ticker, period=period, market="KR")
    print(f"  기간: {df.index[0].date()} ~ {df.index[-1].date()}, {len(df)}개 바")

    cfg = BacktestConfig(k_factor_kr=0.5, rsi_lower=45)
    bt  = StrategyBacktest(df, cfg, market="KR")

    print("\n백테스트 실행 중...")
    bt.run()
    print("\n=== 성과 요약 ===")
    print(bt.summary().to_string())

    print("\n차트 표시 (브라우저 또는 Jupyter 필요)...")
    bt.plot()

    if optimize:
        print("\n최적화 실행 중 (K-Factor × RSI 그리드)...")
        opt    = HyperOptimizer(df, cfg, market="KR")
        result = opt.run()
        opt.plot_heatmap(result, metric="sharpe")


def run_example_us(
    ticker: str = "AAPL",
    period: str = "3y",
    optimize: bool = True,
) -> None:
    """US 전략 백테스트 + 최적화 예제 (Apple 기본값).

    Parameters
    ----------
    ticker   : yfinance US 심볼 (예: "AAPL", "MSFT", "NVDA")
    period   : 데이터 기간
    optimize : True면 K-Factor × RSI 그리드 최적화도 실행
    """
    if not _YF_AVAILABLE:
        print("yfinance 미설치: pip install yfinance")
        return
    if not _VBT_AVAILABLE:
        print("vectorbt 미설치: pip install 'vectorbt>=0.24'")
        return

    print(f"[US] {ticker} 데이터 로딩 ({period})...")
    df = KisOhlcvLoader.from_yfinance(ticker, period=period, market="US")
    print(f"  기간: {df.index[0].date()} ~ {df.index[-1].date()}, {len(df)}개 바")

    cfg = BacktestConfig(
        k_factor_us        = 0.3,
        rsi_lower          = 45,
        rvol_threshold_us  = 1.2,
        init_cash          = 100_000.0,  # USD
    )
    bt  = StrategyBacktest(df, cfg, market="US")

    print("\n백테스트 실행 중...")
    bt.run()
    print("\n=== 성과 요약 ===")
    print(bt.summary().to_string())

    print("\n차트 표시 (브라우저 또는 Jupyter 필요)...")
    bt.plot()

    if optimize:
        print("\n최적화 실행 중 (K-Factor × RSI 그리드)...")
        opt    = HyperOptimizer(df, cfg, market="US")
        result = opt.run()
        opt.plot_heatmap(result, metric="sharpe")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI 진입점
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AutoStock VectorBT Backtest Engine")
    parser.add_argument("--market",   choices=["KR", "US"], default="KR")
    parser.add_argument("--ticker",   default=None, help="yfinance 심볼 (미지정 시 시장 기본값 사용)")
    parser.add_argument("--period",   default="3y")
    parser.add_argument("--no-opt",   action="store_true", help="최적화 건너뜀")
    args = parser.parse_args()

    if args.market == "KR":
        run_example_kr(
            ticker   = args.ticker or "005930.KS",
            period   = args.period,
            optimize = not args.no_opt,
        )
    else:
        run_example_us(
            ticker   = args.ticker or "AAPL",
            period   = args.period,
            optimize = not args.no_opt,
        )
