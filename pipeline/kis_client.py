"""Extended KIS REST helpers for the watchlist pipeline."""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from api_client import ApiError, KISClient, TokenExpiredError, _with_retry
from config import Config
from pipeline.config import pipeline_config
from pipeline.constants import (
    KR_RANK_BLNG_VALUE,
    KR_RANK_BLNG_VOLUME,
    KR_VOLUME_RANK_PATH,
)

logger = logging.getLogger("watchlist")


class PipelineKISClient(KISClient):
    """KISClient with universe/rank helpers and request pacing."""

    def __init__(self) -> None:
        super().__init__()
        self._last_call = 0.0

    def _pace(self) -> None:
        delay = pipeline_config.kis_request_delay_sec
        elapsed = time.monotonic() - self._last_call
        if elapsed < delay:
            time.sleep(delay - elapsed)
        self._last_call = time.monotonic()

    def _get(self, path: str, tr_id: str, params: dict[str, str]) -> dict[str, Any]:
        self._pace()
        resp = self._session.get(
            Config.base_url() + path,
            headers=self._headers(tr_id),
            params=params,
            timeout=15,
        )
        return self._parse(resp)

    @_with_retry()
    def get_current_price_multi(self, stock_code: str) -> dict[str, Any]:
        """Try KOSPI (J) then KOSDAQ (Q) for current price snapshot."""
        last_err: Exception | None = None
        for div in ("J", "Q"):
            try:
                self._pace()
                resp = self._session.get(
                    Config.base_url() + "/uapi/domestic-stock/v1/quotations/inquire-price",
                    headers=self._headers("FHKST01010100"),
                    params={
                        "FID_COND_MRKT_DIV_CODE": div,
                        "FID_INPUT_ISCD": stock_code,
                    },
                    timeout=10,
                )
                data = self._parse(resp)
                out = data.get("output") or {}
                if isinstance(out, dict) and float(str(out.get("stck_prpr", 0)).replace(",", "") or 0) > 0:
                    return data
            except (ApiError, TokenExpiredError) as exc:
                last_err = exc
                continue
        if last_err:
            raise last_err
        raise ApiError(f"현재가 조회 실패: {stock_code}")

    @_with_retry()
    def get_daily_ohlcv_multi(self, stock_code: str) -> list[dict]:
        """Daily OHLCV — tries KOSPI then KOSDAQ."""
        best: list[dict] = []
        for div in ("J", "Q"):
            try:
                self._pace()
                resp = self._session.get(
                    Config.base_url() + "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
                    headers=self._headers("FHKST01010400"),
                    params={
                        "FID_COND_MRKT_DIV_CODE": div,
                        "FID_INPUT_ISCD": stock_code,
                        "FID_PERIOD_DIV_CODE": "D",
                        "FID_ORG_ADJ_PRC": "0",
                    },
                    timeout=10,
                )
                data = self._parse(resp)
                rows = data.get("output") or data.get("output2") or []
                if isinstance(rows, dict):
                    rows = [rows]
                if isinstance(rows, list) and len(rows) > len(best):
                    best = rows
                if best:
                    break
            except (ApiError, requests.exceptions.HTTPError):
                continue
        return best

    def fetch_volume_rank(
        self,
        market_div: str,
        fid_blng_cls_code: str,
        *,
        limit: int = 80,
    ) -> list[str]:
        """Volume/value rank → 6-digit ticker list."""
        params = {
            "FID_COND_MRKT_DIV_CODE": market_div,
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": fid_blng_cls_code,
            "FID_TRGT_CLS_CODE": "111111111",
            "FID_TRGT_EXLS_CLS_CODE": "0000000000",
            "FID_INPUT_PRICE_1": "0",
            "FID_INPUT_PRICE_2": "0",
            "FID_VOL_CNT": "0",
            "FID_INPUT_DATE_1": "0",
        }
        # KIS 공식 샘플·모의 VTS 모두 FHPST01710000 (VHPST 는 미지원)
        tr_candidates = ["FHPST01710000"]
        last_err: Exception | None = None
        data: dict | None = None
        for tr_id in tr_candidates:
            try:
                data = self._get(KR_VOLUME_RANK_PATH, tr_id, params)
                break
            except ApiError as exc:
                last_err = exc
        if data is None:
            raise last_err or ApiError("거래량순위 조회 실패")
        rows = data.get("output")
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            rows = data.get("output2") or []

        codes: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            raw = row.get("mksc_shrn_iscd") or row.get("stck_shrn_iscd")
            if raw is None:
                continue
            s = str(raw).strip()
            if s.isdigit() and len(s) <= 6:
                codes.append(s.zfill(6))
            if len(codes) >= limit:
                break
        return codes


def extract_name(output: dict) -> str:
    for key in ("hts_kor_isnm", "prdt_name", "prdt_abrv_name", "bstp_kor_isnm"):
        val = (output.get(key) or "").strip()
        if val and not (val.isdigit() and len(val) <= 10):
            return val
    return ""


def extract_sector(output: dict, ticker: str) -> str:
    sector = (output.get("bstp_kor_isnm") or output.get("bstp_kor_isnm") or "").strip()
    if sector:
        return sector
    from pipeline.constants import SECTOR_HINT_MAP
    return SECTOR_HINT_MAP.get(ticker, "기타")


def extract_mktcap_won(output: dict) -> float:
    try:
        shares = float(str(output.get("lstn_stcn") or "0").replace(",", ""))
        price = float(str(output.get("stck_prpr") or "0").replace(",", ""))
        return abs(shares * price)
    except (TypeError, ValueError):
        return 0.0
