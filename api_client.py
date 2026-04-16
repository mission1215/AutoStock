"""
api_client.py — 한국투자증권 KIS REST API 클라이언트

모든 API 호출을 담당합니다:
  - 토큰 갱신 데코레이터: 401 응답 시 자동으로 토큰 재발급 후 재시도
  - 네트워크 오류 재시도: 최대 3회, 지수 백오프 적용
  - 모의/실전 투자 TR ID 자동 분기
"""

import logging
import time
import functools
from typing import Any

import requests

from config import Config
from token_manager import token_manager

logger = logging.getLogger(__name__)


# ── 커스텀 예외 ────────────────────────────────────────────

class TokenExpiredError(Exception):
    """KIS API 가 401 또는 토큰 만료 코드를 반환했을 때"""


class ApiError(Exception):
    """KIS API 가 rt_cd != '0' 인 비즈니스 오류를 반환했을 때"""
    def __init__(self, msg: str, rt_cd: str = "", msg_cd: str = "") -> None:
        super().__init__(msg)
        self.rt_cd = rt_cd
        self.msg_cd = msg_cd


# ── 재시도 데코레이터 ──────────────────────────────────────

def _with_retry(max_retries: int = 3, backoff_base: float = 2.0):
    """
    토큰 만료 및 네트워크 오류 자동 재시도 데코레이터

    동작 순서:
      1. TokenExpiredError → 토큰 무효화 후 즉시 재발급, 재시도
      2. requests.RequestException → 지수 백오프 후 재시도 (최대 max_retries회)
      3. ApiError → 재시도 없이 즉시 예외 전파 (비즈니스 오류는 재시도 불필요)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except TokenExpiredError as exc:
                    logger.warning(
                        f"[{func.__name__}] 토큰 만료 감지 "
                        f"({attempt}/{max_retries}) — 재발급 중..."
                    )
                    token_manager.invalidate()
                    token_manager.get_token()   # 새 토큰 선발급
                    last_exc = exc

                except requests.exceptions.RequestException as exc:
                    logger.warning(
                        f"[{func.__name__}] 네트워크 오류 "
                        f"({attempt}/{max_retries}): {exc}"
                    )
                    last_exc = exc
                    if attempt < max_retries:
                        sleep_sec = backoff_base ** (attempt - 1)
                        logger.info(f"  {sleep_sec:.0f}초 후 재시도...")
                        time.sleep(sleep_sec)

            raise last_exc or RuntimeError("알 수 없는 재시도 오류")
        return wrapper
    return decorator


# ── KIS API 클라이언트 ─────────────────────────────────────

class KISClient:
    """한국투자증권 REST API 래퍼"""

    def __init__(self) -> None:
        self._session = requests.Session()

    # ── 내부 헬퍼 ──────────────────────────────────────

    def _headers(self, tr_id: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token_manager.get_token()}",
            "appkey": Config.APP_KEY,
            "appsecret": Config.APP_SECRET,
            "tr_id": tr_id,
            "custtype": "P",
        }

    def _parse(self, resp: requests.Response) -> dict[str, Any]:
        """응답 파싱 및 오류 분류"""
        if resp.status_code == 401:
            raise TokenExpiredError("HTTP 401 — 토큰 만료 또는 인증 오류")

        resp.raise_for_status()
        data: dict = resp.json()

        rt_cd = data.get("rt_cd", "")
        msg1 = data.get("msg1", "")
        msg_cd = data.get("msg_cd", "")

        # EGW00123: 토큰 만료 코드
        if rt_cd != "0" or msg_cd in ("EGW00123", "EGW00121"):
            if msg_cd in ("EGW00123", "EGW00121"):
                raise TokenExpiredError(f"토큰 만료 코드: {msg_cd}")
            raise ApiError(msg1, rt_cd=rt_cd, msg_cd=msg_cd)

        return data

    def _tr_id(self, real_id: str, mock_id: str) -> str:
        return mock_id if Config.IS_MOCK else real_id

    # ── 시세 조회 ───────────────────────────────────────

    @_with_retry()
    def get_current_price(self, stock_code: str) -> dict[str, Any]:
        """현재가 조회 (FHKST01010100)"""
        resp = self._session.get(
            Config.base_url() + "/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=self._headers("FHKST01010100"),
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
            },
            timeout=10,
        )
        return self._parse(resp)

    @_with_retry()
    def get_daily_ohlcv(self, stock_code: str) -> list[dict]:
        """
        일봉 OHLCV 조회 (FHKST01010400)
        반환: 최신 순으로 정렬된 리스트 [{'stck_bsop_date','stck_oprc','stck_hgpr','stck_lwpr','stck_clpr'}, ...]
        """
        resp = self._session.get(
            Config.base_url() + "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            headers=self._headers("FHKST01010400"),
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
            timeout=10,
        )
        data = self._parse(resp)
        return data.get("output2", [])

    # ── 잔고·예수금 조회 ────────────────────────────────

    @_with_retry()
    def get_balance(self) -> dict[str, Any]:
        """
        주식 잔고 조회 (TTTC8434R / VTTC8434R)
        반환 output1: 보유 종목 리스트, output2: 계좌 요약
        """
        tr_id = self._tr_id("TTTC8434R", "VTTC8434R")
        resp = self._session.get(
            Config.base_url() + "/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=self._headers(tr_id),
            params={
                "CANO": Config.account_prefix(),
                "ACNT_PRDT_CD": Config.account_suffix(),
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "02",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "01",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            timeout=10,
        )
        return self._parse(resp)

    @_with_retry()
    def get_available_cash(self, stock_code: str = "005930") -> int:
        """
        주문 가능 예수금 조회 (TTTC8908R / VTTC8908R)
        반환: 주문 가능 금액 (원)
        """
        tr_id = self._tr_id("TTTC8908R", "VTTC8908R")
        resp = self._session.get(
            Config.base_url() + "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            headers=self._headers(tr_id),
            params={
                "CANO": Config.account_prefix(),
                "ACNT_PRDT_CD": Config.account_suffix(),
                "PDNO": stock_code,
                "ORD_UNPR": "0",
                "ORD_DVSN": "01",      # 시장가
                "CMA_EVLU_AMT_ICLD_YN": "Y",
                "OVRS_ICLD_YN": "N",
            },
            timeout=10,
        )
        data = self._parse(resp)
        raw = data.get("output", {}).get("ord_psbl_cash", "0")
        return int(raw.replace(",", "") or 0)

    # ── 주문 실행 ────────────────────────────────────────

    @_with_retry()
    def place_order(
        self,
        stock_code: str,
        side: str,          # "buy" | "sell"
        quantity: int,
        price: int = 0,     # 0 = 시장가
    ) -> dict[str, Any]:
        """
        현금 매수/매도 주문 (TTTC0802U/TTTC0801U / VTTC0802U/VTTC0801U)

        Args:
            stock_code: 종목코드 6자리
            side: "buy" 또는 "sell"
            quantity: 주문 수량
            price: 지정가 (0이면 시장가)
        """
        if side == "buy":
            tr_id = self._tr_id("TTTC0802U", "VTTC0802U")
        elif side == "sell":
            tr_id = self._tr_id("TTTC0801U", "VTTC0801U")
        else:
            raise ValueError(f"side 는 'buy' 또는 'sell' 이어야 합니다: {side}")

        ord_dvsn = "01" if price == 0 else "00"   # 01: 시장가, 00: 지정가
        resp = self._session.post(
            Config.base_url() + "/uapi/domestic-stock/v1/trading/order-cash",
            headers=self._headers(tr_id),
            json={
                "CANO": Config.account_prefix(),
                "ACNT_PRDT_CD": Config.account_suffix(),
                "PDNO": stock_code,
                "ORD_DVSN": ord_dvsn,
                "ORD_QTY": str(quantity),
                "ORD_UNPR": str(price),
            },
            timeout=10,
        )
        return self._parse(resp)
