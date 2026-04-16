"""
strategy.py — 래리 윌리엄스 변동성 돌파 전략 + 리스크 관리

전략 원리:
  목표가 = 당일 시가 + K × (전일 고가 - 전일 저가)
  필터   = 현재가 > N일 이동평균선

리스크 규칙:
  1. 손절선   — 매수가 대비 -2% 도달 시 즉시 매도
  2. 최대 비중 — 종목당 총자산의 10% 이내
  3. 일일 목표 — 당일 수익률이 목표치 달성 시 매매 종료
"""

import logging
from dataclasses import dataclass, field
from datetime import date

from config import Config
from api_client import KISClient

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """보유 포지션 정보"""
    stock_code: str
    buy_price: float
    quantity: int
    stop_loss_price: float = field(init=False)

    def __post_init__(self):
        self.stop_loss_price = self.buy_price * (1 - Config.STOP_LOSS_RATIO)

    @property
    def market_value(self) -> float:
        return self.buy_price * self.quantity

    def pnl(self, current_price: float) -> float:
        """미실현 손익 (원)"""
        return (current_price - self.buy_price) * self.quantity

    def pnl_ratio(self, current_price: float) -> float:
        """미실현 수익률"""
        return (current_price - self.buy_price) / self.buy_price

    def is_stop_loss_triggered(self, current_price: float) -> bool:
        return current_price <= self.stop_loss_price


class VolatilityBreakoutStrategy:
    """
    변동성 돌파 전략 엔진

    사용법:
        strategy = VolatilityBreakoutStrategy(api_client)
        strategy.run_cycle()   # 스케줄러에서 1분마다 호출
    """

    def __init__(self, client: KISClient) -> None:
        self._client = client
        self._positions: dict[str, Position] = {}    # 보유 포지션
        self._realized_pnl: float = 0.0              # 당일 실현 손익
        self._start_equity: float | None = None      # 장 시작 시 총자산 (일일 수익률 기준)
        self._today: date = date.today()
        self._trading_halted: bool = False            # 일일 목표 달성 → 매매 중단 플래그

    # ── 공개 인터페이스 ────────────────────────────────

    def prepare_market_open(self) -> None:
        """
        장 시작 전 호출 — 기준 자산 스냅샷, 플래그 초기화
        스케줄러에서 08:50에 호출합니다.
        """
        self._today = date.today()
        self._trading_halted = False
        self._realized_pnl = 0.0

        equity = self._get_total_equity()
        self._start_equity = equity
        logger.info(f"[장 시작 준비] 기준 총자산: {equity:,.0f}원")

    def run_cycle(self) -> None:
        """
        전략 1회 실행 사이클 (스케줄러에서 1분마다 호출)
        1. 매매 중단 여부 확인
        2. 보유 포지션 손절 체크
        3. 감시 종목 매수 신호 체크
        """
        if self._trading_halted:
            logger.debug("매매 중단 상태 (일일 목표 달성 또는 수동 중단)")
            return

        self._check_stop_losses()
        self._check_buy_signals()
        self._check_daily_target()

    def close_all_positions(self) -> None:
        """
        장 마감 전 강제 청산 (15:20 호출)
        당일 내 청산 원칙이 없는 경우 제거 가능.
        """
        if not self._positions:
            logger.info("청산할 보유 종목 없음")
            return

        logger.info(f"장 마감 강제 청산 시작 — {len(self._positions)}개 종목")
        for code in list(self._positions.keys()):
            price_data = self._client.get_current_price(code)
            current = int(price_data["output"]["stck_prpr"])
            self._execute_sell(code, current, reason="장마감_청산")

    def halt_trading(self) -> None:
        """외부에서 매매를 즉시 중단시킬 때 사용"""
        self._trading_halted = True
        logger.warning("매매가 외부 요청으로 중단되었습니다.")

    # ── 신호 계산 ──────────────────────────────────────

    def _calc_target_price(self, stock_code: str) -> float | None:
        """
        변동성 돌파 목표가 계산
        목표가 = 당일 시가 + K × (전일 고가 - 전일 저가)
        """
        ohlcv = self._client.get_daily_ohlcv(stock_code)
        if len(ohlcv) < 2:
            logger.warning(f"[{stock_code}] 일봉 데이터 부족")
            return None

        # ohlcv[0] = 오늘 (장중이면 아직 미완성), ohlcv[1] = 전일
        today_row = ohlcv[0]
        prev_row = ohlcv[1]

        today_open = float(today_row["stck_oprc"])
        prev_high = float(prev_row["stck_hgpr"])
        prev_low = float(prev_row["stck_lwpr"])

        range_ = prev_high - prev_low
        target = today_open + Config.K_FACTOR * range_

        logger.debug(
            f"[{stock_code}] 시가={today_open:,.0f} "
            f"전일범위={range_:,.0f} 목표가={target:,.0f}"
        )
        return target

    def _calc_moving_average(self, stock_code: str) -> float | None:
        """N일 단순이동평균 계산"""
        ohlcv = self._client.get_daily_ohlcv(stock_code)
        if len(ohlcv) < Config.MA_PERIOD:
            logger.warning(f"[{stock_code}] MA 계산에 필요한 데이터 부족")
            return None

        closes = [float(row["stck_clpr"]) for row in ohlcv[:Config.MA_PERIOD]]
        return sum(closes) / len(closes)

    def _has_buy_signal(self, stock_code: str, current_price: float) -> bool:
        """매수 신호: 현재가 > 목표가 AND 현재가 > MA"""
        target = self._calc_target_price(stock_code)
        ma = self._calc_moving_average(stock_code)

        if target is None or ma is None:
            return False

        above_target = current_price > target
        above_ma = current_price > ma

        logger.debug(
            f"[{stock_code}] 현재가={current_price:,.0f} "
            f"목표가={target:,.0f}({above_target}) "
            f"MA{Config.MA_PERIOD}={ma:,.0f}({above_ma})"
        )
        return above_target and above_ma

    # ── 포지션 관리 ────────────────────────────────────

    def _check_stop_losses(self) -> None:
        """보유 포지션 손절 체크"""
        for code, pos in list(self._positions.items()):
            try:
                data = self._client.get_current_price(code)
                current = int(data["output"]["stck_prpr"])
                if pos.is_stop_loss_triggered(current):
                    logger.warning(
                        f"[{code}] 손절 발동! "
                        f"매수가={pos.buy_price:,.0f} 현재가={current:,.0f} "
                        f"손절선={pos.stop_loss_price:,.0f} "
                        f"손익={pos.pnl_ratio(current)*100:.2f}%"
                    )
                    self._execute_sell(code, current, reason="손절")
            except Exception as e:
                logger.error(f"[{code}] 손절 체크 오류: {e}")

    def _check_buy_signals(self) -> None:
        """감시 종목 매수 신호 체크"""
        for code in Config.WATCHLIST:
            if code in self._positions:
                continue  # 이미 보유 중
            try:
                data = self._client.get_current_price(code)
                current = int(data["output"]["stck_prpr"])
                if self._has_buy_signal(code, current):
                    self._execute_buy(code, current)
            except Exception as e:
                logger.error(f"[{code}] 매수 신호 체크 오류: {e}")

    def _check_daily_target(self) -> None:
        """당일 수익률이 목표치 도달 시 매매 중단"""
        if self._start_equity is None or self._start_equity == 0:
            return
        ratio = self._realized_pnl / self._start_equity
        if ratio >= Config.DAILY_PROFIT_TARGET:
            logger.info(
                f"일일 수익 목표 달성! "
                f"실현손익={self._realized_pnl:,.0f}원 "
                f"수익률={ratio*100:.2f}% — 금일 매매 종료"
            )
            self._trading_halted = True

    # ── 주문 래퍼 (order_executor 에 위임) ───────────────

    def _execute_buy(self, stock_code: str, current_price: float) -> None:
        """매수 수량 계산 후 order_executor 에 위임"""
        # 이 메서드는 order_executor.py 의 OrderExecutor 가 오버라이드/주입합니다.
        # 직접 호출 시 기본 동작을 수행합니다.
        raise NotImplementedError("OrderExecutor 를 주입하거나 서브클래스에서 구현하세요.")

    def _execute_sell(self, stock_code: str, current_price: float, reason: str) -> None:
        raise NotImplementedError("OrderExecutor 를 주입하거나 서브클래스에서 구현하세요.")

    # ── 유틸 ──────────────────────────────────────────

    def _get_total_equity(self) -> float:
        """총자산 (예수금 + 보유 주식 평가금액)"""
        try:
            balance = self._client.get_balance()
            summary = balance.get("output2", [{}])
            if summary:
                raw = summary[0].get("tot_evlu_amt", "0")
                return float(raw.replace(",", "") or 0)
        except Exception as e:
            logger.error(f"총자산 조회 실패: {e}")
        return 0.0

    def register_buy(self, stock_code: str, buy_price: float, quantity: int) -> None:
        """매수 체결 후 포지션 등록 (order_executor 에서 호출)"""
        self._positions[stock_code] = Position(stock_code, buy_price, quantity)
        logger.info(
            f"[{stock_code}] 포지션 등록 | "
            f"매수가={buy_price:,.0f} 수량={quantity} "
            f"손절선={self._positions[stock_code].stop_loss_price:,.0f}"
        )

    def register_sell(self, stock_code: str, sell_price: float) -> None:
        """매도 체결 후 포지션 제거 및 손익 반영 (order_executor 에서 호출)"""
        pos = self._positions.pop(stock_code, None)
        if pos:
            pnl = pos.pnl(sell_price)
            self._realized_pnl += pnl
            logger.info(
                f"[{stock_code}] 포지션 청산 | "
                f"매수가={pos.buy_price:,.0f} 매도가={sell_price:,.0f} "
                f"손익={pnl:+,.0f}원 | 당일누적={self._realized_pnl:+,.0f}원"
            )

    @property
    def positions(self) -> dict[str, Position]:
        return dict(self._positions)

    @property
    def trading_halted(self) -> bool:
        return self._trading_halted
