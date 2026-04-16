"""
order_executor.py — 주문 실행 엔진

책임:
  1. 매수 전 예수금 조회 → 종목당 최대 비중(10%) 검증
  2. 매도 전 보유 수량 확인
  3. strategy.py 의 _execute_buy / _execute_sell 을 구체화
  4. 체결 결과를 strategy 에 피드백 (포지션 등록/제거)

retry 로직은 api_client._with_retry 데코레이터가 처리하므로
이 레이어에서는 비즈니스 로직에만 집중합니다.
"""

import logging
import math

from config import Config
from api_client import KISClient, ApiError
from strategy import VolatilityBreakoutStrategy

logger = logging.getLogger(__name__)


class OrderExecutor:
    """
    주문 실행 + strategy 콜백 연결

    사용법:
        client   = KISClient()
        strategy = VolatilityBreakoutStrategy(client)
        executor = OrderExecutor(client, strategy)

        # strategy 의 추상 메서드를 executor 로 바인딩
        strategy._execute_buy  = executor.buy
        strategy._execute_sell = executor.sell
    """

    def __init__(self, client: KISClient, strategy: VolatilityBreakoutStrategy) -> None:
        self._client = client
        self._strategy = strategy

        # strategy 의 추상 메서드를 이 인스턴스로 바인딩
        import types
        self._strategy._execute_buy = types.MethodType(
            lambda s, code, price: self.buy(code, price), strategy
        )
        self._strategy._execute_sell = types.MethodType(
            lambda s, code, price, reason="": self.sell(code, price, reason), strategy
        )

    # ── 공개 주문 메서드 ───────────────────────────────

    def buy(self, stock_code: str, current_price: float) -> bool:
        """
        매수 주문 실행

        절차:
          1. 예수금 조회
          2. 종목당 최대 비중(10%) 기준 매수 금액 산출
          3. 수량 계산 (floor)
          4. 수량 > 0 이면 시장가 주문
          5. strategy 에 포지션 등록
        """
        mode_label = "[모의]" if Config.IS_MOCK else "[실전]"
        logger.info(f"{mode_label} 매수 주문 시도 | {stock_code} @ {current_price:,.0f}원")

        # 1. 예수금 조회
        try:
            available_cash = self._client.get_available_cash(stock_code)
        except Exception as e:
            logger.error(f"[{stock_code}] 예수금 조회 실패 — 매수 취소: {e}")
            return False

        # 2. 총자산 기준 최대 투자 금액 산출
        try:
            total_equity = self._get_total_equity()
        except Exception as e:
            logger.warning(f"총자산 조회 실패, 예수금 기준으로 대체: {e}")
            total_equity = available_cash

        max_invest = total_equity * Config.MAX_POSITION_RATIO
        invest_amount = min(available_cash, max_invest)

        if invest_amount < current_price:
            logger.warning(
                f"[{stock_code}] 투자 가능 금액({invest_amount:,.0f}원)이 "
                f"현재가({current_price:,.0f}원)보다 낮음 — 매수 건너뜀"
            )
            return False

        # 3. 수량 계산 (정수 내림)
        quantity = math.floor(invest_amount / current_price)
        if quantity <= 0:
            logger.warning(f"[{stock_code}] 계산된 수량 0 — 매수 건너뜀")
            return False

        actual_amount = quantity * current_price
        logger.info(
            f"[{stock_code}] 매수 계획 | "
            f"예수금={available_cash:,.0f}원 "
            f"최대비중={max_invest:,.0f}원 "
            f"투자금액={actual_amount:,.0f}원 "
            f"수량={quantity}주"
        )

        # 4. 시장가 매수 주문 (retry 는 api_client 가 처리)
        try:
            result = self._client.place_order(
                stock_code=stock_code,
                side="buy",
                quantity=quantity,
                price=0,   # 시장가
            )
            order_no = result.get("output", {}).get("ODNO", "N/A")
            logger.info(f"[{stock_code}] 매수 주문 접수 완료 | 주문번호={order_no}")
        except ApiError as e:
            logger.error(f"[{stock_code}] 매수 API 오류: {e}")
            return False
        except Exception as e:
            logger.error(f"[{stock_code}] 매수 주문 예외: {e}")
            return False

        # 5. 포지션 등록 (시장가이므로 현재가로 근사)
        self._strategy.register_buy(stock_code, current_price, quantity)
        return True

    def sell(self, stock_code: str, current_price: float, reason: str = "신호") -> bool:
        """
        매도 주문 실행

        절차:
          1. 보유 포지션에서 수량 확인
          2. 시장가 전량 매도
          3. strategy 에서 포지션 제거 및 손익 반영
        """
        mode_label = "[모의]" if Config.IS_MOCK else "[실전]"
        pos = self._strategy.positions.get(stock_code)
        if not pos:
            logger.warning(f"[{stock_code}] 보유 포지션 없음 — 매도 건너뜀")
            return False

        quantity = pos.quantity
        logger.info(
            f"{mode_label} 매도 주문 시도 | {stock_code} @ {current_price:,.0f}원 "
            f"사유={reason} 수량={quantity}주"
        )

        try:
            result = self._client.place_order(
                stock_code=stock_code,
                side="sell",
                quantity=quantity,
                price=0,   # 시장가
            )
            order_no = result.get("output", {}).get("ODNO", "N/A")
            logger.info(f"[{stock_code}] 매도 주문 접수 완료 | 주문번호={order_no}")
        except ApiError as e:
            logger.error(f"[{stock_code}] 매도 API 오류: {e}")
            return False
        except Exception as e:
            logger.error(f"[{stock_code}] 매도 주문 예외: {e}")
            return False

        # 포지션 제거 및 손익 반영
        self._strategy.register_sell(stock_code, current_price)
        return True

    # ── 유틸 ──────────────────────────────────────────

    def _get_total_equity(self) -> float:
        """계좌 총평가금액 조회"""
        balance = self._client.get_balance()
        summary = balance.get("output2", [{}])
        if summary:
            raw = summary[0].get("tot_evlu_amt", "0")
            return float(raw.replace(",", "") or 0)
        return 0.0
