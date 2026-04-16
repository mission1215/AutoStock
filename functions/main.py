"""
AutoStock Firebase Functions — 멀티유저 KIS 자동매매 시스템 (한국 + 미국주식)

인증: Firebase Auth (Google 로그인) — Bearer 토큰으로 uid 추출
데이터: Firestore users/{uid}/... 경로로 유저별 격리

HTTP API (/api/*):
  POST /api/setup           — 최초 설정 (KIS 키 저장)
  GET  /api/status          — 대시보드 전체 상태
  POST /api/order           — 수동 매수/매도 (KR/US)
  POST /api/bot             — 봇 시작/중지
  GET  /api/config          — 설정 조회
  POST /api/config          — 설정 변경
  GET  /api/trades          — 매매 이력
  GET  /api/logs            — 최근 로그
  GET  /api/recommendations — AI 추천 이력
  POST /api/ai/run          — AI 추천 수동 실행
"""

import os
import math
import logging
import time as time_module
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Any

import json
import re
import requests as http_requests
import firebase_admin
from firebase_admin import firestore, auth as fb_auth
from firebase_functions import https_fn, scheduler_fn, options
from flask import Flask, request, jsonify
from google import genai
from google.genai import types as genai_types

KST = ZoneInfo("Asia/Seoul")
ET  = ZoneInfo("America/New_York")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

flask_app = Flask(__name__)
_firebase_app = None
_db = None

# ── 인메모리 가격 캐시 (인스턴스 재사용 시 유효, TTL=60초) ──
_price_cache: dict[str, dict] = {}   # key: "uid:market:code"  value: {data, ts}
_balance_cache: dict[str, dict] = {} # key: uid                value: {data, ts}
_ohlcv_cache: dict[str, dict] = {}   # key: "uid:market:code"  value: {data, ts}
_PRICE_TTL  = 60   # 초
_BALANCE_TTL = 30  # 초
_OHLCV_TTL = 300   # 초 (일봉은 초단위로 자주 바뀌지 않음)

def _cached_price(uid: str, cfg: dict, code: str, market: str) -> dict:
    """현재가를 인메모리 캐시에서 반환. 만료 시 KIS API 재조회."""
    key = f"{uid}:{market}:{code}"
    now = time_module.time()
    if key in _price_cache and now - _price_cache[key]["ts"] < _PRICE_TTL:
        return _price_cache[key]["data"]
    if market == "KR":
        data = get_current_price_kr(uid, cfg, code)
    else:
        data = get_current_price_us(uid, cfg, code)
    _price_cache[key] = {"data": data, "ts": now}
    return data

def _cached_balance(uid: str, cfg: dict) -> dict:
    """잔고를 인메모리 캐시에서 반환. 만료 시 KIS API 재조회."""
    now = time_module.time()
    if uid in _balance_cache and now - _balance_cache[uid]["ts"] < _BALANCE_TTL:
        return _balance_cache[uid]["data"]
    data = get_balance_kr(uid, cfg)
    _balance_cache[uid] = {"data": data, "ts": now}
    return data

def _cached_ohlcv(uid: str, cfg: dict, code: str, market: str) -> list:
    """일봉 데이터를 인메모리 캐시에서 반환. 만료 시 KIS API 재조회."""
    key = f"{uid}:{market}:{code}"
    now = time_module.time()
    if key in _ohlcv_cache and now - _ohlcv_cache[key]["ts"] < _OHLCV_TTL:
        return _ohlcv_cache[key]["data"]
    if market == "KR":
        data = get_daily_ohlcv_kr(uid, cfg, code)
    else:
        data = get_daily_ohlcv_us(uid, cfg, code)
    _ohlcv_cache[key] = {"data": data, "ts": now}
    return data


# ── 주요 한국 종목명 맵 ──────────────────────────────────────
KR_STOCK_NAMES: dict[str, str] = {
    "005930": "삼성전자", "000660": "SK하이닉스", "035420": "NAVER",
    "035720": "카카오", "005380": "현대차", "000270": "기아",
    "051910": "LG화학", "006400": "삼성SDI", "373220": "LG에너지솔루션",
    "207940": "삼성바이오로직스", "068270": "셀트리온", "003550": "LG",
    "096770": "SK이노베이션", "034730": "SK", "030200": "KT",
    "017670": "SK텔레콤", "015760": "한국전력", "105560": "KB금융",
    "055550": "신한지주", "086790": "하나금융지주", "316140": "우리금융지주",
    "032830": "삼성생명", "033780": "KT&G", "028260": "삼성물산",
    "012330": "현대모비스", "009150": "삼성전기", "011070": "LG이노텍",
    "042700": "한미반도체", "000810": "삼성화재", "005490": "POSCO홀딩스",
    "010130": "고려아연", "004020": "현대제철", "009830": "한화솔루션",
    "010950": "S-Oil", "011200": "HMM", "003490": "대한항공",
    "018260": "삼성SDS", "036570": "엔씨소프트", "251270": "넷마블",
    "263750": "펄어비스", "293490": "카카오게임즈", "352820": "하이브",
    "041510": "에스엠", "035900": "JYP Ent.", "247540": "에코프로비엠",
    "086520": "에코프로", "112610": "씨에스윈드", "128940": "한미약품",
    "000100": "유한양행", "326030": "SK바이오팜", "024110": "기업은행",
    "039490": "키움증권", "071050": "한국금융지주", "030600": "현대글로비스",
    "000720": "현대건설", "267250": "HD현대중공업", "009540": "HD한국조선해양",
    "064350": "현대로템", "090430": "아모레퍼시픽", "051900": "LG생활건강",
    "004170": "신세계", "139480": "이마트", "271560": "오리온",
    "003230": "삼양식품", "097950": "CJ제일제당",
}

# 미국 주요 종목명 맵
US_STOCK_NAMES: dict[str, str] = {
    "AAPL": "애플", "MSFT": "마이크로소프트", "GOOGL": "알파벳(구글)",
    "AMZN": "아마존", "NVDA": "엔비디아", "META": "메타",
    "TSLA": "테슬라", "AVGO": "브로드컴", "JPM": "JP모건",
    "V": "비자", "MA": "마스터카드", "UNH": "유나이티드헬스",
    "JNJ": "존슨앤존슨", "WMT": "월마트", "PG": "P&G",
    "HD": "홈디포", "ORCL": "오라클", "NFLX": "넷플릭스",
    "AMD": "AMD", "INTC": "인텔", "QCOM": "퀄컴",
    "CRM": "세일즈포스", "NOW": "서비스나우", "ADBE": "어도비",
    "PLTR": "팔란티어", "COIN": "코인베이스", "SOFI": "소파이",
    "RIVN": "리비안", "NIO": "니오", "BIDU": "바이두",
    "TSM": "TSMC", "ASML": "ASML", "ARM": "ARM홀딩스",
    "MU": "마이크론", "AMAT": "어플라이드머티리얼즈",
    "SPY": "S&P500 ETF", "QQQ": "나스닥100 ETF",
    "SOXL": "반도체 3X ETF", "TQQQ": "나스닥 3X ETF",
}


def _stock_name(api_name: str, code: str, market: str = "KR") -> str:
    name = (api_name or "").strip()
    if name:
        return name
    return (US_STOCK_NAMES if market == "US" else KR_STOCK_NAMES).get(code, code)


def _us_price_from_output(out: dict, ohlcv: list | None = None) -> float:
    """미국 현재가 필드 변동 대응 + 실패 시 최근 종가 fallback."""
    for key in ("last", "stck_prpr", "ovrs_nmix_prpr", "ovrs_now_pric", "clos"):
        raw = out.get(key)
        if raw is None:
            continue
        try:
            val = float(str(raw).replace(",", ""))
            if val > 0:
                return val
        except Exception:
            continue
    if ohlcv:
        try:
            return float(str(ohlcv[0].get("clos", 0)).replace(",", ""))
        except Exception:
            return 0.0
    return 0.0


# ══════════════════════════════════════════════════════════
# Firebase 초기화 (지연 로딩)
# ══════════════════════════════════════════════════════════

def get_db():
    global _firebase_app, _db
    if _firebase_app is None:
        _firebase_app = firebase_admin.initialize_app()
    if _db is None:
        _db = firestore.client()
    return _db


def _uref(uid: str):
    """유저 루트 컬렉션 참조"""
    return get_db().collection("users").document(uid)


# ══════════════════════════════════════════════════════════
# 인증 (Firebase ID Token 검증)
# ══════════════════════════════════════════════════════════

def verify_token(req) -> str:
    """Authorization: Bearer <token> 검증 → uid 반환"""
    # Firebase 초기화 보장
    if _firebase_app is None:
        get_db()
    hdr = req.headers.get("Authorization", "")
    if not hdr.startswith("Bearer "):
        raise PermissionError("인증 토큰이 없습니다")
    id_token = hdr.split("Bearer ", 1)[1].strip()
    decoded = fb_auth.verify_id_token(id_token)
    return decoded["uid"]


# ══════════════════════════════════════════════════════════
# 설정 관리 (per-user)
# ══════════════════════════════════════════════════════════

def get_config(uid: str) -> dict:
    doc = _uref(uid).collection("config").document("settings").get()
    if doc.exists:
        return doc.to_dict()
    return {}


def save_config(uid: str, data: dict):
    _uref(uid).collection("config").document("settings").set(data, merge=True)


def _base_url(is_mock: bool) -> str:
    if is_mock:
        return "https://openapivts.koreainvestment.com:29443"
    return "https://openapi.koreainvestment.com:9443"


def _account_prefix(account_no: str) -> str:
    return account_no.split("-")[0] if "-" in account_no else account_no[:8]


def _account_suffix(account_no: str) -> str:
    return account_no.split("-")[1] if "-" in account_no else "01"


# ══════════════════════════════════════════════════════════
# 토큰 관리 (per-user)
# ══════════════════════════════════════════════════════════

def get_token(uid: str, cfg: dict) -> str:
    doc = _uref(uid).collection("state").document("token").get()
    now = datetime.now(KST)
    if doc.exists:
        data = doc.to_dict()
        access_token = data.get("access_token", "")
        expires_at = data.get("expires_at")
        stored_mock = data.get("is_mock", True)
        if access_token and expires_at and stored_mock == cfg.get("is_mock", True):
            if isinstance(expires_at, datetime):
                exp = expires_at if expires_at.tzinfo else expires_at.replace(tzinfo=KST)
            else:
                exp = expires_at
            if exp > now + timedelta(minutes=5):
                return access_token
    return _issue_token(uid, cfg)


def _issue_token(uid: str, cfg: dict) -> str:
    url = _base_url(cfg.get("is_mock", True)) + "/oauth2/tokenP"
    resp = http_requests.post(
        url,
        json={
            "grant_type": "client_credentials",
            "appkey": cfg["app_key"],
            "appsecret": cfg["app_secret"],
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise ValueError(f"토큰 응답 오류: {data}")
    access_token = data["access_token"]
    expires_in = int(data.get("expires_in", 86400))
    expires_at = datetime.now(KST) + timedelta(seconds=expires_in)
    _uref(uid).collection("state").document("token").set({
        "access_token": access_token,
        "expires_at": expires_at,
        "is_mock": cfg.get("is_mock", True),
        "issued_at": datetime.now(KST),
    })
    _add_log(uid, "INFO", f"토큰 발급 성공 | 만료: {expires_at.strftime('%H:%M:%S')}")
    return access_token


def invalidate_token(uid: str):
    _uref(uid).collection("state").document("token").delete()


# ══════════════════════════════════════════════════════════
# KIS API 클라이언트
# ══════════════════════════════════════════════════════════

class ApiError(Exception):
    def __init__(self, msg: str, rt_cd: str = "", msg_cd: str = "") -> None:
        super().__init__(msg)
        self.rt_cd = rt_cd
        self.msg_cd = msg_cd


def _headers(uid: str, cfg: dict, tr_id: str) -> dict:
    return {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {get_token(uid, cfg)}",
        "appkey": cfg["app_key"],
        "appsecret": cfg["app_secret"],
        "tr_id": tr_id,
        "custtype": "P",
    }


def _parse(resp: http_requests.Response, uid: str, cfg: dict) -> dict:
    if resp.status_code == 401:
        invalidate_token(uid)
        raise ApiError("HTTP 401 — 토큰 만료", rt_cd="401")
    resp.raise_for_status()
    data = resp.json()
    rt_cd = data.get("rt_cd", "")
    msg_cd = data.get("msg_cd", "")
    if msg_cd in ("EGW00123", "EGW00121"):
        invalidate_token(uid)
        raise ApiError(f"토큰 만료: {msg_cd}", rt_cd=rt_cd, msg_cd=msg_cd)
    if rt_cd != "0":
        raise ApiError(data.get("msg1", "API 오류"), rt_cd=rt_cd, msg_cd=msg_cd)
    return data


def _tr_id(cfg: dict, real_id: str, mock_id: str) -> str:
    return mock_id if cfg.get("is_mock", True) else real_id


def _with_retry(func, *args, retries: int = 3, **kwargs):
    last_exc = None
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except ApiError as e:
            if e.rt_cd == "401" and attempt < retries - 1:
                time_module.sleep(1)
                last_exc = e
                continue
            raise
        except Exception as e:
            last_exc = e
            if attempt < retries - 1:
                time_module.sleep(2 ** attempt)
    raise last_exc


# ── 한국 주식 API ──────────────────────────────────────────

def get_current_price_kr(uid: str, cfg: dict, stock_code: str) -> dict:
    def _call():
        resp = http_requests.get(
            _base_url(cfg.get("is_mock", True)) + "/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=_headers(uid, cfg, "FHKST01010100"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": stock_code},
            timeout=10,
        )
        return _parse(resp, uid, cfg)
    return _with_retry(_call)


def get_daily_ohlcv_kr(uid: str, cfg: dict, stock_code: str) -> list:
    resp = http_requests.get(
        _base_url(cfg.get("is_mock", True)) + "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
        headers=_headers(uid, cfg, "FHKST01010400"),
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
        timeout=10,
    )
    return _parse(resp, uid, cfg).get("output2", [])


def get_balance_kr(uid: str, cfg: dict) -> dict:
    tr_id = _tr_id(cfg, "TTTC8434R", "VTTC8434R")
    resp = http_requests.get(
        _base_url(cfg.get("is_mock", True)) + "/uapi/domestic-stock/v1/trading/inquire-balance",
        headers=_headers(uid, cfg, tr_id),
        params={
            "CANO": _account_prefix(cfg["account_no"]),
            "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
            "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
            "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
        },
        timeout=10,
    )
    return _parse(resp, uid, cfg)


def get_available_cash_kr(uid: str, cfg: dict, stock_code: str = "005930") -> int:
    tr_id = _tr_id(cfg, "TTTC8908R", "VTTC8908R")
    resp = http_requests.get(
        _base_url(cfg.get("is_mock", True)) + "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
        headers=_headers(uid, cfg, tr_id),
        params={
            "CANO": _account_prefix(cfg["account_no"]),
            "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
            "PDNO": stock_code, "ORD_UNPR": "0", "ORD_DVSN": "01",
            "CMA_EVLU_AMT_ICLD_YN": "Y", "OVRS_ICLD_YN": "N",
        },
        timeout=10,
    )
    raw = _parse(resp, uid, cfg).get("output", {}).get("ord_psbl_cash", "0")
    return int(raw.replace(",", "") or 0)


def place_order_kr(uid: str, cfg: dict, stock_code: str, side: str, quantity: int, price: int = 0) -> dict:
    tr_id = _tr_id(cfg, "TTTC0802U" if side == "buy" else "TTTC0801U",
                        "VTTC0802U" if side == "buy" else "VTTC0801U")
    ord_dvsn = "01" if price == 0 else "00"
    resp = http_requests.post(
        _base_url(cfg.get("is_mock", True)) + "/uapi/domestic-stock/v1/trading/order-cash",
        headers=_headers(uid, cfg, tr_id),
        json={
            "CANO": _account_prefix(cfg["account_no"]),
            "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
            "PDNO": stock_code, "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(quantity), "ORD_UNPR": str(price),
        },
        timeout=10,
    )
    return _parse(resp, uid, cfg)


# ── 미국 주식 API ──────────────────────────────────────────

US_MARKET_MAP = {"NASD": "NASD", "NYSE": "NYSE", "AMEX": "AMEX"}

def _us_excd(stock_code: str) -> str:
    """종목 코드로 거래소 추정 (기본 NASD)"""
    return "NASD"


def get_current_price_us(uid: str, cfg: dict, stock_code: str) -> dict:
    """미국 주식 현재가 조회"""
    def _call():
        resp = http_requests.get(
            _base_url(False) + "/uapi/overseas-stock/v1/quotations/price",
            headers=_headers(uid, cfg, "HHDFS00000300"),
            params={"AUTH": "", "EXCD": _us_excd(stock_code), "SYMB": stock_code},
            timeout=10,
        )
        parsed = _parse(resp, uid, cfg)
        # KIS 응답은 환경/버전에 따라 output 또는 output1을 사용함
        if "output" not in parsed and "output1" in parsed:
            parsed["output"] = parsed.get("output1", {})
        return parsed
    return _with_retry(_call)


def get_daily_ohlcv_us(uid: str, cfg: dict, stock_code: str) -> list:
    """미국 주식 일봉 조회"""
    resp = http_requests.get(
        _base_url(False) + "/uapi/overseas-stock/v1/quotations/dailyprice",
        headers=_headers(uid, cfg, "HHDFS76240000"),
        params={
            "AUTH": "", "EXCD": _us_excd(stock_code),
            "SYMB": stock_code, "GUBN": "0", "BYMD": "", "MODP": "0",
        },
        timeout=10,
    )
    return _parse(resp, uid, cfg).get("output2", [])


def get_balance_us(uid: str, cfg: dict) -> dict:
    """미국 주식 잔고 조회"""
    tr_id = _tr_id(cfg, "JTTT3012R", "VTTT3012R")
    resp = http_requests.get(
        _base_url(cfg.get("is_mock", True)) + "/uapi/overseas-stock/v1/trading/inquire-balance",
        headers=_headers(uid, cfg, tr_id),
        params={
            "CANO": _account_prefix(cfg["account_no"]),
            "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
            "OVRS_EXCG_CD": "NASD", "TR_CRCY_CD": "USD",
            "CTX_AREA_FK200": "", "CTX_AREA_NK200": "",
        },
        timeout=10,
    )
    return _parse(resp, uid, cfg)


def place_order_us(uid: str, cfg: dict, stock_code: str, side: str, quantity: int, price: float = 0) -> dict:
    """미국 주식 매수/매도 주문"""
    if side == "buy":
        tr_id = _tr_id(cfg, "JTTT1002U", "VTTT1002U")
    else:
        tr_id = _tr_id(cfg, "JTTT1006U", "VTTT1006U")
    ord_dvsn = "00" if price > 0 else "01"  # 00=지정가, 01=시장가
    resp = http_requests.post(
        _base_url(cfg.get("is_mock", True)) + "/uapi/overseas-stock/v1/trading/order",
        headers=_headers(uid, cfg, tr_id),
        json={
            "CANO": _account_prefix(cfg["account_no"]),
            "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
            "OVRS_EXCG_CD": _us_excd(stock_code),
            "PDNO": stock_code,
            "ORD_DVSN": ord_dvsn,
            "ORD_QTY": str(quantity),
            "OVRS_ORD_UNPR": f"{price:.2f}",
        },
        timeout=10,
    )
    return _parse(resp, uid, cfg)


# ══════════════════════════════════════════════════════════
# 봇 상태 / 포지션 / 로그 (per-user)
# ══════════════════════════════════════════════════════════

def get_bot_state(uid: str) -> dict:
    doc = _uref(uid).collection("state").document("bot").get()
    if doc.exists:
        return doc.to_dict()
    return {
        "is_market_open": False, "trading_halted": False,
        "bot_enabled": True, "start_equity": 0.0,
        "realized_pnl": 0.0, "today": date.today().isoformat(),
    }


def update_bot_state(uid: str, updates: dict):
    _uref(uid).collection("state").document("bot").set(updates, merge=True)


def get_positions(uid: str, market: str = "KR") -> dict:
    return {
        doc.id: doc.to_dict()
        for doc in _uref(uid).collection(f"positions_{market}").stream()
    }


def register_buy(uid: str, market: str, stock_code: str, buy_price: float,
                 quantity: int, stop_loss_ratio: float,
                 target_sell_price: float = 0.0, source: str = "수동",
                 stock_name: str = ""):
    stop_loss_price = buy_price * (1 - stop_loss_ratio)
    _uref(uid).collection(f"positions_{market}").document(stock_code).set({
        "stock_code": stock_code, "stock_name": stock_name, "market": market,
        "buy_price": buy_price, "quantity": quantity,
        "stop_loss_price": stop_loss_price, "target_sell_price": target_sell_price,
        "source": source, "entry_time": datetime.now(KST),
    })


def register_sell(uid: str, market: str, stock_code: str, sell_price: float) -> float:
    pos_doc = _uref(uid).collection(f"positions_{market}").document(stock_code).get()
    pnl = 0.0
    if pos_doc.exists:
        pos = pos_doc.to_dict()
        pnl = (sell_price - pos["buy_price"]) * pos["quantity"]
        _uref(uid).collection(f"positions_{market}").document(stock_code).delete()
    return pnl


def add_trade(uid: str, market: str, stock_code: str, side: str,
              price: float, quantity: int, reason: str = "", pnl: float = 0.0):
    _uref(uid).collection("trades").add({
        "stock_code": stock_code, "market": market, "side": side,
        "price": price, "quantity": quantity, "reason": reason,
        "pnl": pnl, "timestamp": datetime.now(KST),
    })


def _add_log(uid: str, level: str, message: str):
    _uref(uid).collection("logs").add({
        "level": level, "message": message, "timestamp": datetime.now(KST),
    })
    logger.info("[%s][%s] %s", uid[:8], level, message)


# ══════════════════════════════════════════════════════════
# 전략 로직
# ══════════════════════════════════════════════════════════

def _get_total_equity_kr(uid: str, cfg: dict) -> float:
    try:
        bal = get_balance_kr(uid, cfg)
        summary = bal.get("output2", [{}])
        if summary:
            raw = summary[0].get("tot_evlu_amt", "0")
            return float(raw.replace(",", "") or 0)
    except Exception as e:
        _add_log(uid, "ERROR", f"총자산 조회 실패: {e}")
    return 0.0


def _get_available_cash_us(uid: str, cfg: dict) -> float:
    """미국 주식 매수 가능 USD 잔고 조회.
    KIS US 잔고 API output2[0].frcr_dncl_amt_2 필드 사용.
    조회 실패 또는 0원이면 0.0 반환 (호출 측에서 max_us_qty 폴백 처리).
    """
    try:
        bal = get_balance_us(uid, cfg)
        summary = bal.get("output2", [{}])
        if summary:
            raw = summary[0].get("frcr_dncl_amt_2", "0")
            val = float(str(raw).replace(",", "") or 0)
            if val > 0:
                return val
    except Exception as e:
        _add_log(uid, "ERROR", f"USD 잔고 조회 실패: {e}")
    return 0.0


def _is_kr_market_open() -> bool:
    """한국 정규장 오픈 여부 (09:00~15:30 KST, 평일)"""
    now_kst = datetime.now(KST)
    if now_kst.weekday() >= 5:
        return False
    market_open  = now_kst.replace(hour=9,  minute=0,  second=0, microsecond=0)
    market_close = now_kst.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now_kst <= market_close


def _is_us_market_open() -> bool:
    """미국 동부시간 기준 정규장 오픈 여부"""
    now_et = datetime.now(ET)
    if now_et.weekday() >= 5:
        return False
    market_open  = now_et.replace(hour=9,  minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0,  second=0, microsecond=0)
    return market_open <= now_et <= market_close


def _calc_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def _calc_atr(ohlcv: list[dict], period: int = 5) -> float:
    rows = ohlcv[:period + 1]
    if len(rows) < 2:
        return 0.0
    true_ranges = []
    for i in range(min(period, len(rows) - 1)):
        high = float(rows[i].get("stck_hgpr", rows[i].get("high", 0)))
        low = float(rows[i].get("stck_lwpr", rows[i].get("low", 0)))
        prev_close = float(rows[i + 1].get("stck_clpr", rows[i + 1].get("clos", 0)))
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0


def _calc_ema(prices: list[float], period: int) -> list[float]:
    """지수이동평균(EMA) 계산 — prices는 [과거→현재] 순서"""
    if len(prices) < period:
        return []
    k = 2.0 / (period + 1)
    emas = [sum(prices[:period]) / period]
    for p in prices[period:]:
        emas.append(p * k + emas[-1] * (1 - k))
    return emas


def _calc_atr_us(ohlcv: list[dict], period: int = 5) -> float:
    """미국 주식 ATR (KIS US OHLCV 필드명 기준)"""
    rows = ohlcv[:period + 1]
    if len(rows) < 2:
        return 0.0
    true_ranges = []
    for i in range(min(period, len(rows) - 1)):
        high       = float(rows[i].get("high",  rows[i].get("stck_hgpr", 0)))
        low        = float(rows[i].get("low",   rows[i].get("stck_lwpr", 0)))
        prev_close = float(rows[i+1].get("clos", rows[i+1].get("stck_clpr", 0)))
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
    return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0


def score_us_stock_algorithm(current_price: float, ohlcv: list[dict], cfg: dict) -> dict:
    """
    미국 주식 전용 스코어링 알고리즘 (수익률 최적화)

    구성 요소 (최대 125점):
      1. 변동성 돌파 (K=0.3, 갭 보정) — 30점
      2. 갭업 보너스 (1.5~5%)          — 15점
      3. 거래량 폭증 (RVOL)            — 25점
      4. EMA 정배열 (9/21 EMA)         — 20점
      5. RSI 45~70 구간                — 15점
      6. 변화율 1~6% (스윗스팟)        — 10점
      7. 10일 고점 근접 돌파            — 10점

    70점 이상 = 매수 신호
    """
    score  = 0
    detail: dict = {}
    try:
        # KIS US OHLCV 필드: clos, open, high, low, tvol
        closes  = [float(r.get("clos", r.get("stck_clpr", 0))) for r in ohlcv]
        opens   = [float(r.get("open", r.get("stck_oprc", 0))) for r in ohlcv]
        highs   = [float(r.get("high", r.get("stck_hgpr", 0))) for r in ohlcv]
        lows    = [float(r.get("low",  r.get("stck_lwpr", 0))) for r in ohlcv]
        volumes = [int(str(r.get("tvol", r.get("acml_vol", "0"))).replace(",", "") or 0)
                   for r in ohlcv]

        today_open = opens[0] if opens else current_price

        # ── 1. 변동성 돌파 (K 보정 포함) ────────────────────
        k_us = cfg.get("k_factor", 0.3)          # US 기본 K=0.3 (KR은 0.5)
        if len(ohlcv) >= 2:
            prev_high  = highs[1]
            prev_low   = lows[1]
            prev_close = closes[1]
            gap_ratio  = (today_open - prev_close) / prev_close * 100 if prev_close > 0 else 0
            # 갭업이 크면 K를 줄임 (갭이 이미 변동성을 소비했으므로)
            k_adj   = k_us * 0.5 if gap_ratio > 2.0 else (k_us * 0.7 if gap_ratio > 1.0 else k_us)
            target  = today_open + k_adj * (prev_high - prev_low)
            breakout = current_price >= target
            score   += 30 if breakout else 0
            detail["breakout"]  = breakout
            detail["gap_ratio"] = round(gap_ratio, 2)
            detail["target"]    = round(target, 2)

            # ── 2. 갭업 보너스 ────────────────────────────────
            if 1.5 <= gap_ratio <= 5.0:
                score += 15
                detail["gap_bonus"] = True

        # ── 3. 거래량 폭증 (RVOL) ────────────────────────────
        if len(volumes) >= 6 and volumes[0] > 0:
            avg_vol = sum(volumes[1:6]) / 5
            rvol    = volumes[0] / avg_vol if avg_vol > 0 else 1.0
            if   rvol >= 2.0: score += 25
            elif rvol >= 1.5: score += 15
            elif rvol >= 1.2: score += 8
            detail["rvol"] = round(rvol, 2)

        # ── 4. EMA 정배열 (9 EMA > 21 EMA, 가격도 위에) ─────
        if len(closes) >= 21:
            closes_asc = list(reversed(closes[:21]))   # 오래된→현재
            ema9_list  = _calc_ema(closes_asc, 9)
            ema21_list = _calc_ema(closes_asc, 21)
            if ema9_list and ema21_list:
                ema9  = ema9_list[-1]
                ema21 = ema21_list[-1]
                aligned = current_price > ema9 > ema21
                score  += 20 if aligned else (10 if current_price > ema9 else 0)
                detail["ema9"]     = round(ema9, 2)
                detail["ema21"]    = round(ema21, 2)
                detail["ema_aligned"] = aligned
        elif len(closes) >= 9:
            closes_asc = list(reversed(closes[:9]))
            ema9_list  = _calc_ema(closes_asc, 9)
            if ema9_list:
                ema9 = ema9_list[-1]
                if current_price > ema9:
                    score += 10
                detail["ema9"] = round(ema9, 2)

        # ── 5. RSI (미국 최적 범위: 45~70) ───────────────────
        rsi = _calc_rsi(closes[:20])
        if   45 <= rsi <= 70: score += 15
        elif 40 <= rsi <= 75: score += 8
        detail["rsi"] = rsi

        # ── 6. 변화율 스윗스팟 (1~6%) ───────────────────────
        if len(closes) >= 2 and closes[1] > 0:
            change_rate = (current_price - closes[1]) / closes[1] * 100
            if   1.0 <= change_rate <= 6.0: score += 10
            elif 0   < change_rate <= 9.0:  score += 5
            detail["change_rate"] = round(change_rate, 2)

        # ── 7. 10일 고점 근접 돌파 ───────────────────────────
        if len(highs) >= 10:
            high_10d    = max(highs[:10])
            near_high   = current_price >= high_10d * 0.98
            above_high  = current_price >= high_10d
            score      += 10 if above_high else (5 if near_high else 0)
            detail["near_10d_high"]  = near_high
            detail["above_10d_high"] = above_high

    except Exception as e:
        detail["error"] = str(e)

    return {"score": score, "detail": detail}


def calculate_optimal_prices_us(current_price: float, ohlcv: list[dict], cfg: dict) -> dict:
    """미국 주식 최적 매수/매도/손절가 (ATR 1.8배 목표, RR ≥ 2.0 추구)"""
    atr = _calc_atr_us(ohlcv)
    buy_price = current_price
    if atr > 0:
        sell_price    = buy_price + atr * 1.8          # 미국 목표 배수 (KR 1.5 대비 넓게)
        stop_by_atr   = buy_price - atr * 0.8          # 빠른 손절 (KR 1.0 대비 좁게)
        stop_by_ratio = buy_price * (1 - cfg.get("stop_loss_ratio", 0.025))
        stop_loss     = max(stop_by_atr, stop_by_ratio)
    else:
        sell_price = buy_price * 1.025
        stop_loss  = buy_price * (1 - cfg.get("stop_loss_ratio", 0.025))

    profit_ratio = (sell_price - buy_price) / buy_price * 100
    risk_ratio   = (buy_price - stop_loss)  / buy_price * 100
    rr_ratio     = profit_ratio / risk_ratio if risk_ratio > 0 else 0

    return {
        "buy_price":    round(buy_price,    2),
        "sell_price":   round(sell_price,   2),
        "stop_loss":    round(stop_loss,    2),
        "atr":          round(atr,          2),
        "profit_ratio": round(profit_ratio, 2),
        "risk_ratio":   round(risk_ratio,   2),
        "rr_ratio":     round(rr_ratio,     2),
    }


def score_stock_algorithm(current_price: float, ohlcv: list[dict], cfg: dict) -> dict:
    score = 0
    detail = {}
    try:
        closes = [float(r.get("stck_clpr", r.get("clos", 0))) for r in ohlcv]
        volumes = [int(r.get("acml_vol", r.get("tvol", "0")).replace(",", "") or 0) for r in ohlcv]
        today_open = float(ohlcv[0].get("stck_oprc", ohlcv[0].get("open", current_price))) if ohlcv else current_price

        if len(ohlcv) >= 2:
            prev_high = float(ohlcv[1].get("stck_hgpr", ohlcv[1].get("high", 0)))
            prev_low = float(ohlcv[1].get("stck_lwpr", ohlcv[1].get("low", 0)))
            target = today_open + cfg.get("k_factor", 0.5) * (prev_high - prev_low)
            breakout = current_price >= target
            score += 30 if breakout else 0
            detail["breakout"] = breakout

        if len(volumes) >= 6 and volumes[0] > 0:
            avg_vol = sum(volumes[1:6]) / 5
            vol_ratio = volumes[0] / avg_vol if avg_vol > 0 else 1
            if vol_ratio >= 2.0: score += 25
            elif vol_ratio >= 1.5: score += 15
            elif vol_ratio >= 1.2: score += 8
            detail["volume_ratio"] = round(vol_ratio, 2)

        ma5 = sum(closes[:5]) / 5 if len(closes) >= 5 else 0
        if ma5 > 0 and current_price > ma5: score += 10
        detail["ma5"] = round(ma5, 0)

        # EMA5/EMA20 정배열 — MA5>MA20 단순비교 대체 (최근 데이터 가중 반영)
        if len(closes) >= 20:
            closes_asc = list(reversed(closes[:25]))   # oldest → newest
            ema5_list  = _calc_ema(closes_asc, 5)
            ema20_list = _calc_ema(closes_asc, 20)
            if ema5_list and ema20_list:
                ema5  = ema5_list[-1]
                ema20 = ema20_list[-1]
                if ema5 > ema20:
                    score += 10   # 정배열 가점
                detail["ema5"]  = round(ema5, 0)
                detail["ema20"] = round(ema20, 0)

        rsi = _calc_rsi(closes[:20])
        if 45 <= rsi <= 65: score += 15
        elif 40 <= rsi <= 70: score += 8
        detail["rsi"] = rsi

        if len(closes) >= 2 and closes[1] > 0:
            change_rate = (current_price - closes[1]) / closes[1] * 100
            if 0.5 <= change_rate <= 4.0: score += 10
            elif 0 < change_rate <= 6.0: score += 5
            detail["change_rate"] = round(change_rate, 2)
    except Exception as e:
        detail["error"] = str(e)
    return {"score": score, "detail": detail}


def calculate_optimal_prices(current_price: float, ohlcv: list[dict], cfg: dict) -> dict:
    atr = _calc_atr(ohlcv)
    buy_price = current_price
    if atr > 0:
        sell_price = buy_price + atr * 1.5
        stop_by_atr = buy_price - atr * 1.0
        stop_by_ratio = buy_price * (1 - cfg.get("stop_loss_ratio", 0.02))
        stop_loss = max(stop_by_atr, stop_by_ratio)
    else:
        sell_price = buy_price * 1.02
        stop_loss = buy_price * (1 - cfg.get("stop_loss_ratio", 0.02))
    profit_ratio = (sell_price - buy_price) / buy_price * 100
    risk_ratio = (buy_price - stop_loss) / buy_price * 100
    rr_ratio = profit_ratio / risk_ratio if risk_ratio > 0 else 0
    return {
        "buy_price": round(buy_price, 2), "sell_price": round(sell_price, 2),
        "stop_loss": round(stop_loss, 2), "atr": round(atr, 2),
        "profit_ratio": round(profit_ratio, 2), "risk_ratio": round(risk_ratio, 2),
        "rr_ratio": round(rr_ratio, 2),
    }


def run_strategy_cycle_kr(uid: str, cfg: dict):
    state = get_bot_state(uid)
    if not state.get("bot_enabled", True): return
    # is_market_open 플래그가 False여도 실시간 시간 기준으로 재확인 (스케줄러 누락 대비)
    if not state.get("is_market_open", False) and not _is_kr_market_open(): return
    if state.get("trading_halted", False): return

    positions = get_positions(uid, "KR")
    for code, pos in list(positions.items()):
        try:
            data = get_current_price_kr(uid, cfg, code)
            current = int(data["output"]["stck_prpr"])
            target = pos.get("target_sell_price", 0)
            if target > 0 and current >= target:
                qty = pos["quantity"]
                place_order_kr(uid, cfg, code, "sell", qty, 0)
                pnl = register_sell(uid, "KR", code, current)
                add_trade(uid, "KR", code, "sell", current, qty, "목표가_달성", pnl)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO", f"[KR][{code}] 목표가 달성 매도 | 현재={current:,} 목표={target:,}")
                continue
            if current <= pos["stop_loss_price"]:
                qty = pos["quantity"]
                place_order_kr(uid, cfg, code, "sell", qty, 0)
                pnl = register_sell(uid, "KR", code, current)
                add_trade(uid, "KR", code, "sell", current, qty, "손절", pnl)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "WARNING", f"[KR][{code}] 손절 | 현재={current:,}")
        except Exception as e:
            _add_log(uid, "ERROR", f"[KR][{code}] 포지션 체크 오류: {e}")

    positions = get_positions(uid, "KR")
    for code in cfg.get("kr_watchlist", []):
        if code in positions: continue
        try:
            data = get_current_price_kr(uid, cfg, code)
            current = int(data["output"]["stck_prpr"])
            ohlcv = get_daily_ohlcv_kr(uid, cfg, code)
            if len(ohlcv) >= 2:
                today_open = float(ohlcv[0]["stck_oprc"])
                prev_range = float(ohlcv[1]["stck_hgpr"]) - float(ohlcv[1]["stck_lwpr"])
                target = today_open + cfg.get("k_factor", 0.5) * prev_range
                ma5 = sum(float(r["stck_clpr"]) for r in ohlcv[:5]) / min(5, len(ohlcv))
                if current > target and current > ma5:
                    available = get_available_cash_kr(uid, cfg, code)
                    equity = _get_total_equity_kr(uid, cfg) or available
                    invest = min(available, equity * cfg.get("max_position_ratio", 0.1))
                    qty = math.floor(invest / current)
                    if qty > 0:
                        result = place_order_kr(uid, cfg, code, "buy", qty, 0)
                        order_no = result.get("output", {}).get("ODNO", "N/A")
                        sname = _stock_name(data["output"].get("hts_kor_isnm", ""), code, "KR")
                        register_buy(uid, "KR", code, current, qty, cfg.get("stop_loss_ratio", 0.02),
                                     source="자동", stock_name=sname)
                        add_trade(uid, "KR", code, "buy", current, qty, "자동매수")
                        _add_log(uid, "INFO", f"[KR][{code}] 매수 | {qty}주@{current:,} 주문={order_no}")
        except Exception as e:
            _add_log(uid, "ERROR", f"[KR][{code}] 매수 체크 오류: {e}")


# ══════════════════════════════════════════════════════════
# Gemini AI 추천
# ══════════════════════════════════════════════════════════

def _collect_kr_stock_data(uid: str, cfg: dict) -> list[dict]:
    result = []
    for code in cfg.get("kr_watchlist", []):
        try:
            price_data = get_current_price_kr(uid, cfg, code)
            out = price_data.get("output", {})
            current = int(out.get("stck_prpr", 0))
            ohlcv = get_daily_ohlcv_kr(uid, cfg, code)
            result.append({
                "code": code, "current_price": current,
                "change_rate": out.get("prdy_ctrt", "0"),
                "volume": out.get("acml_vol", "0"),
                "recent_ohlcv": [
                    {"date": r.get("stck_bsop_date"), "open": r.get("stck_oprc"),
                     "high": r.get("stck_hgpr"), "low": r.get("stck_lwpr"), "close": r.get("stck_clpr")}
                    for r in (ohlcv[:5] if len(ohlcv) >= 5 else ohlcv)
                ],
            })
        except Exception as e:
            _add_log(uid, "WARNING", f"[{code}] 데이터 수집 실패: {e}")
    return result


def _collect_us_stock_data(uid: str, cfg: dict) -> list[dict]:
    """미국 감시 종목 데이터 수집"""
    result = []
    for code in cfg.get("us_watchlist", []):
        try:
            price_data = get_current_price_us(uid, cfg, code)
            out        = price_data.get("output", {})
            current    = float(out.get("last", 0))
            ohlcv      = get_daily_ohlcv_us(uid, cfg, code)
            result.append({
                "code": code, "current_price": current,
                "change_rate": out.get("rate", out.get("diff", "0")),
                "volume": out.get("tvol", out.get("pvol", "0")),
                "recent_ohlcv": [
                    {"date":  r.get("xymd"),
                     "open":  r.get("open"),   "high": r.get("high"),
                     "low":   r.get("low"),     "close": r.get("clos"),
                     "volume": r.get("tvol")}
                    for r in (ohlcv[:5] if len(ohlcv) >= 5 else ohlcv)
                ],
            })
        except Exception as e:
            _add_log(uid, "WARNING", f"[US][{code}] 데이터 수집 실패: {e}")
    return result


def query_gemini_candidates(uid: str, stock_data: list[dict], session: str, market: str = "KR") -> tuple[list[str], dict[str, str]]:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError("GEMINI_API_KEY가 설정되지 않았습니다.")
    client = genai.Client(api_key=api_key)
    session_labels = {"morning": "오전 (09:30)", "afternoon": "오후 (13:00)", "late": "마감 (15:30)"}
    us_sessions    = {"morning": "오전 (ET 10:30)", "afternoon": "오후 (ET 13:00)", "late": "마감 (ET 15:30)"}
    session_label  = (us_sessions if market == "US" else session_labels).get(session, session)
    data_json = json.dumps(stock_data, ensure_ascii=False, indent=2)

    if market == "US":
        prompt = f"""당신은 미국 나스닥/NYSE 주식 단기 트레이딩 전문가입니다.
오늘 {session_label} 세션 기준으로 아래 미국 주식들 중 당일~2일 단기 매매 가능성이 높은 후보를 최대 20개 선정하세요.
각 종목의 추천 이유를 1~2문장으로 작성해주세요.

분석할 종목 데이터:
{data_json}

미국 주식 선정 기준 (우선순위 순):
1. RVOL(상대거래량)이 1.5배 이상 — 기관/세력 진입 신호
2. 9 EMA > 21 EMA 정배열 상태 (추세 확인)
3. RSI 45~70 구간 (과매수 아닌 상승 모멘텀)
4. 갭업 1.5~5% 후 지속 상승 패턴
5. 10일 신고가 근접 또는 돌파 (저항 돌파 신호)
6. 당일 변화율 1~6% (스윗스팟, 너무 급등 제외)
7. 섹터 모멘텀 고려 (반도체, AI, 핀테크 선호)

제외 기준:
- 당일 8% 이상 급등 종목 (이미 과매수)
- 거래량이 평소보다 적은 종목
- RSI 75 초과 (단기 과매수)

아래 JSON 형식으로만 응답하세요:
{{
  "candidates": [
    {{"code": "티커1", "reason": "추천 이유 1~2문장"}},
    {{"code": "티커2", "reason": "추천 이유 1~2문장"}}
  ]
}}"""
    else:
        prompt = f"""당신은 한국 코스피/코스닥 주식 단기 트레이딩 전문가입니다.
오늘 {session_label} 세션 기준으로 아래 종목들 중 단기 매매(당일~2일) 가능성이 높은 후보를 최대 20개 선정하고, 각 종목의 추천 이유를 1~2문장으로 작성해주세요.

분석할 종목 데이터:
{data_json}

선정 기준:
- 거래량이 평소보다 많은 종목
- 변동성 돌파 신호가 나타나는 종목 (오늘 시가 + K×전일변동폭 돌파)
- 5일 이동평균 위에 있는 종목
- RSI 45~65 구간 (과매수 아닌 상승 구간)
- 변동성이 적당한 종목 (당일 4% 초과 급등 제외)

아래 JSON 형식으로만 응답하세요:
{{
  "candidates": [
    {{"code": "종목코드1", "reason": "추천 이유 1~2문장"}},
    {{"code": "종목코드2", "reason": "추천 이유 1~2문장"}}
  ]
}}"""
    response = client.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    raw_text = response.text.strip()
    clean = re.sub(r"```(?:json)?", "", raw_text).strip().rstrip("`").strip()
    parsed = json.loads(clean)
    raw_candidates = parsed.get("candidates", [])[:20]
    candidate_codes: list[str] = []
    reasons_map: dict[str, str] = {}
    for item in raw_candidates:
        if isinstance(item, dict):
            code = str(item.get("code", "")).strip()
            reason = str(item.get("reason", "")).strip()
        else:
            code = str(item).strip()
            reason = ""
        if code:
            candidate_codes.append(code)
            if reason:
                reasons_map[code] = reason
    _add_log(uid, "INFO", f"[Gemini] {session_label} 후보 {len(candidate_codes)}종목")
    return candidate_codes, reasons_map


def run_ai_session(uid: str, cfg: dict, session: str, market: str = "KR"):
    state = get_bot_state(uid)
    if not state.get("bot_enabled", True):
        _add_log(uid, "INFO", f"[AI {session}] 봇 비활성 — 건너뜀")
        return
    if state.get("trading_halted", False):
        _add_log(uid, "INFO", f"[AI {session}] 매매 중단 — 건너뜀")
        return

    _add_log(uid, "INFO", f"[AI {session}][{market}] 이중 필터링 시작")

    # ── 시장별 데이터 수집 ─────────────────────────────────
    try:
        if market == "US":
            stock_data = _collect_us_stock_data(uid, cfg)
        else:
            stock_data = _collect_kr_stock_data(uid, cfg)
        if not stock_data:
            _add_log(uid, "ERROR", f"[AI][{market}] 데이터 없음"); return
    except Exception as e:
        _add_log(uid, "ERROR", f"[AI][{market}] 데이터 수집 오류: {e}"); return

    reasons_map: dict[str, str] = {}
    try:
        candidate_codes, reasons_map = query_gemini_candidates(uid, stock_data, session, market)
    except Exception as e:
        _add_log(uid, "ERROR", f"[AI] Gemini 오류: {e}")
        candidate_codes = cfg.get("us_watchlist" if market == "US" else "kr_watchlist", [])

    if not candidate_codes:
        _add_log(uid, "WARNING", "[AI] 후보 없음"); return

    # ── 시장별 스코어링 ────────────────────────────────────
    scored: list[tuple] = []
    stock_details: dict = {}
    for code in candidate_codes:
        try:
            if market == "US":
                data    = get_current_price_us(uid, cfg, code)
                out     = data["output"]
                current = float(out.get("last", out.get("stck_prpr", 0)))
                ohlcv   = get_daily_ohlcv_us(uid, cfg, code)
                score_result = score_us_stock_algorithm(current, ohlcv, cfg)
            else:
                data    = get_current_price_kr(uid, cfg, code)
                current = int(data["output"]["stck_prpr"])
                ohlcv   = get_daily_ohlcv_kr(uid, cfg, code)
                score_result = score_stock_algorithm(current, ohlcv, cfg)
            stock_details[code] = {"current": current, "ohlcv": ohlcv, "score": score_result}
            scored.append((code, score_result["score"], score_result["detail"]))
        except Exception as e:
            _add_log(uid, "WARNING", f"[AI][{code}] 스코어링 오류: {e}")

    if not scored:
        _add_log(uid, "ERROR", "[AI] 스코어링 결과 없음"); return

    scored.sort(key=lambda x: x[1], reverse=True)

    # 스코어 요약 로그 (상위 5개) — 매수 미발생 원인 파악용
    score_summary = " | ".join(f"{c}={s}pt" for c, s, _ in scored[:5])
    _add_log(uid, "INFO", f"[AI][{market}] 스코어 상위: {score_summary}")

    n = min(max(int(cfg.get("ai_stock_count", 3)), 3), 5)
    top_stocks = scored[:n]

    # ── 추천 목록 생성 ─────────────────────────────────────
    recommendations = []
    for code, score, detail in top_stocks:
        info    = stock_details.get(code, {})
        current = info.get("current", 0)
        ohlcv   = info.get("ohlcv", [])
        if market == "US":
            prices = calculate_optimal_prices_us(current, ohlcv, cfg)
        else:
            prices = calculate_optimal_prices(current, ohlcv, cfg)
        sname = _stock_name("", code, market)
        rec = {
            "code": code, "stock_name": sname, "score": score,
            "reason": reasons_map.get(code, ""),
            **prices, "detail": detail,
        }
        recommendations.append(rec)

    session_id = datetime.now(KST).strftime("%Y%m%d_") + session + "_" + market
    _uref(uid).collection("recommendations").document(session_id).set({
        "session_id": session_id, "session": session, "market": market,
        "timestamp": datetime.now(KST), "candidates": candidate_codes,
        "recommendations": recommendations, "status": "executing",
    })

    # ── 시장별 매수 실행 ───────────────────────────────────
    executed = []
    positions = get_positions(uid, market)
    # 미국 최소 점수: cfg에서 조절 가능 (기본 55점 / 125점 만점)
    min_score_us = int(cfg.get("min_score_us", 55))

    for rec in recommendations:
        code = rec["code"]
        if code in positions:
            continue
        if market == "US" and rec["score"] < min_score_us:
            _add_log(uid, "INFO", f"[AI][US][{code}] 점수 미달({rec['score']}/{min_score_us}) — 건너뜀")
            continue
        try:
            if market == "US":
                data    = get_current_price_us(uid, cfg, code)
                current = float(data["output"].get("last", 0))
                if current <= 0:
                    continue
                # 미국: USD 잔고 기반 수량 계산 (실패 시 max_us_qty 폴백)
                available_usd = _get_available_cash_us(uid, cfg)
                if available_usd > 0:
                    invest_usd = available_usd * cfg.get("max_position_ratio", 0.1)
                    qty = max(1, min(math.floor(invest_usd / current),
                                    int(cfg.get("max_us_qty", 5))))
                else:
                    qty = max(1, int(cfg.get("max_us_qty", 1)))
                result   = place_order_us(uid, cfg, code, "buy", qty, 0)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                sname    = _stock_name("", code, "US")
                register_buy(uid, "US", code, current, qty,
                             cfg.get("stop_loss_ratio", 0.025),
                             target_sell_price=float(rec["sell_price"]),
                             source=f"AI_{session}(점수{rec['score']})",
                             stock_name=sname)
                add_trade(uid, "US", code, "buy", current, qty, f"AI_{session}_US", 0.0)
                _add_log(uid, "INFO", f"[AI][US][{code}] 매수 | {qty}주@${current:.2f} 주문={order_no}")
            else:
                data      = get_current_price_kr(uid, cfg, code)
                current   = int(data["output"]["stck_prpr"])
                available = get_available_cash_kr(uid, cfg, code)
                equity    = _get_total_equity_kr(uid, cfg) or available
                invest    = min(available, equity * cfg.get("max_position_ratio", 0.1))
                qty       = math.floor(invest / current)
                if qty <= 0:
                    continue
                result   = place_order_kr(uid, cfg, code, "buy", qty, 0)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                register_buy(uid, "KR", code, current, qty, cfg.get("stop_loss_ratio", 0.02),
                             target_sell_price=float(rec["sell_price"]),
                             source=f"AI_{session}(점수{rec['score']})",
                             stock_name=rec.get("stock_name", ""))
                add_trade(uid, "KR", code, "buy", current, qty, f"AI_{session}", 0.0)
                _add_log(uid, "INFO", f"[AI][{code}] 매수 | {qty}주@{current:,} 주문={order_no}")
            executed.append(code)
        except Exception as e:
            _add_log(uid, "ERROR", f"[AI][{code}] 매수 오류: {e}")

    _uref(uid).collection("recommendations").document(session_id).update({
        "status": "completed", "executed_codes": executed,
        "executed_count": len(executed), "completed_at": datetime.now(KST),
    })
    _add_log(uid, "INFO", f"[AI {session}] 완료 — {len(executed)}/{len(recommendations)}종목 매수")


def run_strategy_cycle_us(uid: str, cfg: dict):
    """
    미국 주식 전략 사이클
      1. 보유 포지션: 목표가 도달 → 익절, 손절가 이탈 → 손절
      2. 신규 매수: US 알고리즘 70점 이상인 종목만 매수
    """
    state = get_bot_state(uid)
    if not state.get("bot_enabled", True): return
    if state.get("trading_halted", False): return

    # ── 보유 포지션 관리 ────────────────────────────────────
    positions = get_positions(uid, "US")
    for code, pos in list(positions.items()):
        try:
            data    = get_current_price_us(uid, cfg, code)
            current = float(data["output"].get("last", 0))
            if current <= 0:
                continue
            target  = float(pos.get("target_sell_price", 0))
            slp     = float(pos.get("stop_loss_price", 0))
            qty     = pos["quantity"]

            if target > 0 and current >= target:
                place_order_us(uid, cfg, code, "sell", qty, 0)
                pnl = register_sell(uid, "US", code, current)
                add_trade(uid, "US", code, "sell", current, qty, "목표가_달성", pnl)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO", f"[US][{code}] 익절 | ${current:.2f} → 목표 ${target:.2f} | PnL ${pnl:+.2f}")
                continue
            if slp > 0 and current <= slp:
                place_order_us(uid, cfg, code, "sell", qty, 0)
                pnl = register_sell(uid, "US", code, current)
                add_trade(uid, "US", code, "sell", current, qty, "손절", pnl)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "WARNING", f"[US][{code}] 손절 | ${current:.2f} ≤ 손절 ${slp:.2f}")
        except Exception as e:
            _add_log(uid, "ERROR", f"[US][{code}] 포지션 체크 오류: {e}")

    # ── 신규 매수 검토 ──────────────────────────────────────
    positions = get_positions(uid, "US")
    for code in cfg.get("us_watchlist", []):
        if code in positions:
            continue
        try:
            data    = get_current_price_us(uid, cfg, code)
            out     = data["output"]
            current = float(out.get("last", 0))
            if current <= 0:
                continue
            ohlcv        = get_daily_ohlcv_us(uid, cfg, code)
            score_result = score_us_stock_algorithm(current, ohlcv, cfg)
            score        = score_result["score"]
            min_score    = int(cfg.get("min_score_us", 55))

            if score < min_score:
                _add_log(uid, "INFO",
                         f"[US][{code}] 스코어 미달 {score}pt/{min_score}pt "
                         f"| detail={score_result['detail']}")
                continue

            prices = calculate_optimal_prices_us(current, ohlcv, cfg)
            available_usd = _get_available_cash_us(uid, cfg)
            if available_usd > 0:
                invest_usd = available_usd * cfg.get("max_position_ratio", 0.1)
                qty = max(1, min(math.floor(invest_usd / current),
                                int(cfg.get("max_us_qty", 5))))
            else:
                qty = max(1, int(cfg.get("max_us_qty", 1)))
            result   = place_order_us(uid, cfg, code, "buy", qty, 0)
            order_no = result.get("output", {}).get("ODNO", "N/A")
            sname    = _stock_name(out.get("rsym", ""), code, "US")
            register_buy(uid, "US", code, current, qty,
                         cfg.get("stop_loss_ratio", 0.025),
                         target_sell_price=prices["sell_price"],
                         source="자동_US", stock_name=sname)
            add_trade(uid, "US", code, "buy", current, qty, "자동매수_US")
            _add_log(uid, "INFO",
                     f"[US][{code}] 매수 | {qty}주@${current:.2f} "
                     f"목표=${prices['sell_price']:.2f} 손절=${prices['stop_loss']:.2f} "
                     f"점수={score_result['score']} 주문={order_no}")
        except Exception as e:
            _add_log(uid, "ERROR", f"[US][{code}] 매수 체크 오류: {e}")


# ══════════════════════════════════════════════════════════
# 스케줄 Functions (모든 유저 순회)
# ══════════════════════════════════════════════════════════

def _get_all_users() -> list[tuple[str, dict]]:
    """설정 완료된 유저 목록 반환 [(uid, cfg), ...]"""
    result = []
    for user_doc in get_db().collection("users").stream():
        uid = user_doc.id
        cfg_doc = _uref(uid).collection("config").document("settings").get()
        if cfg_doc.exists:
            cfg = cfg_doc.to_dict()
            if cfg.get("app_key") and cfg.get("app_secret") and cfg.get("account_no"):
                result.append((uid, cfg))
    return result


def _purge_old_docs(uid: str, collection_name: str, days: int) -> int:
    """timestamp 기준 오래된 문서 삭제."""
    cutoff = datetime.now(KST) - timedelta(days=days)
    deleted = 0
    while True:
        docs = list(
            _uref(uid)
            .collection(collection_name)
            .where("timestamp", "<", cutoff)
            .limit(200)
            .stream()
        )
        if not docs:
            break
        batch = get_db().batch()
        for doc in docs:
            batch.delete(doc.reference)
        batch.commit()
        deleted += len(docs)
        if len(docs) < 200:
            break
    return deleted


@scheduler_fn.on_schedule(
    schedule="10 3 * * *", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_cleanup_history(event: scheduler_fn.ScheduledEvent) -> None:
    """
    데이터 보존정리:
    - logs: 14일
    - recommendations: 30일
    - trades: 180일
    """
    for uid, _ in _get_all_users():
        try:
            logs_deleted = _purge_old_docs(uid, "logs", 14)
            recs_deleted = _purge_old_docs(uid, "recommendations", 30)
            trades_deleted = _purge_old_docs(uid, "trades", 180)
            if logs_deleted or recs_deleted or trades_deleted:
                logger.info(
                    "[%s] cleanup done logs=%d recs=%d trades=%d",
                    uid[:8], logs_deleted, recs_deleted, trades_deleted
                )
        except Exception as e:
            logger.error("[%s] cleanup failed: %s", uid[:8], e)


@scheduler_fn.on_schedule(
    schedule="50 8 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_prepare(event: scheduler_fn.ScheduledEvent) -> None:
    for uid, cfg in _get_all_users():
        try:
            invalidate_token(uid)
            get_token(uid, cfg)
            equity = _get_total_equity_kr(uid, cfg)
            update_bot_state(uid, {
                "trading_halted": False, "realized_pnl": 0.0,
                "start_equity": equity, "today": date.today().isoformat(),
            })
            _add_log(uid, "INFO", f"[08:50] 준비 완료 | 기준자산={equity:,.0f}원")
        except Exception as e:
            _add_log(uid, "ERROR", f"[08:50] 초기화 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="0 9 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_market_open(event: scheduler_fn.ScheduledEvent) -> None:
    for uid, cfg in _get_all_users():
        update_bot_state(uid, {"is_market_open": True})
        _add_log(uid, "INFO", f"[09:00] 장 시작 | {'모의' if cfg.get('is_mock') else '실전'}")


@scheduler_fn.on_schedule(
    schedule="* * * * *", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_strategy_cycle(event: scheduler_fn.ScheduledEvent) -> None:
    now = datetime.now(KST)
    if now.weekday() >= 5: return
    market_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_end = now.replace(hour=15, minute=20, second=0, microsecond=0)  # 정규장 15:30, 여유 10분
    if not (market_start <= now <= market_end): return
    for uid, cfg in _get_all_users():
        try:
            run_strategy_cycle_kr(uid, cfg)
        except Exception as e:
            _add_log(uid, "ERROR", f"전략 사이클 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="20 15 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_close_positions(event: scheduler_fn.ScheduledEvent) -> None:
    for uid, cfg in _get_all_users():
        positions = get_positions(uid, "KR")
        for code, pos in list(positions.items()):
            try:
                data = get_current_price_kr(uid, cfg, code)
                current = int(data["output"]["stck_prpr"])
                qty = pos["quantity"]
                place_order_kr(uid, cfg, code, "sell", qty, 0)
                pnl = register_sell(uid, "KR", code, current)
                add_trade(uid, "KR", code, "sell", current, qty, "장마감_청산", pnl)
                state = get_bot_state(uid)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
            except Exception as e:
                _add_log(uid, "ERROR", f"[{code}] 청산 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="31 15 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_market_close(event: scheduler_fn.ScheduledEvent) -> None:
    for uid, _ in _get_all_users():
        update_bot_state(uid, {"is_market_open": False})
        _add_log(uid, "INFO", "[15:31] 장 마감")


@scheduler_fn.on_schedule(
    schedule="30 9 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_512,
)
def scheduled_ai_morning(event: scheduler_fn.ScheduledEvent) -> None:
    for uid, cfg in _get_all_users():
        try:
            run_ai_session(uid, cfg, "morning")
        except Exception as e:
            _add_log(uid, "ERROR", f"AI 오전 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="0 13 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_512,
)
def scheduled_ai_afternoon(event: scheduler_fn.ScheduledEvent) -> None:
    for uid, cfg in _get_all_users():
        try:
            run_ai_session(uid, cfg, "afternoon")
        except Exception as e:
            _add_log(uid, "ERROR", f"AI 오후 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="30 15 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_512,
)
def scheduled_ai_late(event: scheduler_fn.ScheduledEvent) -> None:
    for uid, cfg in _get_all_users():
        try:
            run_ai_session(uid, cfg, "late")
        except Exception as e:
            _add_log(uid, "ERROR", f"AI 마감 오류: {e}")


# ══════════════════════════════════════════════════════════
# 미국 주식 스케줄 Functions (ET 기준)
# ══════════════════════════════════════════════════════════

@scheduler_fn.on_schedule(
    schedule="*/5 9-15 * * 1-5", timezone=scheduler_fn.Timezone("America/New_York"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_us_strategy_cycle(event: scheduler_fn.ScheduledEvent) -> None:
    """미국 장 중 5분마다 전략 사이클 실행 (ET 9:00~16:00)"""
    if not _is_us_market_open():
        return
    for uid, cfg in _get_all_users():
        try:
            run_strategy_cycle_us(uid, cfg)
        except Exception as e:
            _add_log(uid, "ERROR", f"[US] 전략 사이클 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="30 10 * * 1-5", timezone=scheduler_fn.Timezone("America/New_York"),
    memory=options.MemoryOption.MB_512,
)
def scheduled_us_ai_morning(event: scheduler_fn.ScheduledEvent) -> None:
    """미국 오전 세션 AI 추천 (ET 10:30 — 장 개시 1시간 후 모멘텀 확인)"""
    for uid, cfg in _get_all_users():
        try:
            run_ai_session(uid, cfg, "morning", "US")
        except Exception as e:
            _add_log(uid, "ERROR", f"[US] AI 오전 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="0 13 * * 1-5", timezone=scheduler_fn.Timezone("America/New_York"),
    memory=options.MemoryOption.MB_512,
)
def scheduled_us_ai_afternoon(event: scheduler_fn.ScheduledEvent) -> None:
    """미국 오후 세션 AI 추천 (ET 13:00)"""
    for uid, cfg in _get_all_users():
        try:
            run_ai_session(uid, cfg, "afternoon", "US")
        except Exception as e:
            _add_log(uid, "ERROR", f"[US] AI 오후 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="30 15 * * 1-5", timezone=scheduler_fn.Timezone("America/New_York"),
    memory=options.MemoryOption.MB_512,
)
def scheduled_us_ai_late(event: scheduler_fn.ScheduledEvent) -> None:
    """미국 마감 세션 AI 추천 (ET 15:30 — 마감 30분 전 최종 정리)"""
    for uid, cfg in _get_all_users():
        try:
            run_ai_session(uid, cfg, "late", "US")
        except Exception as e:
            _add_log(uid, "ERROR", f"[US] AI 마감 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="50 15 * * 1-5", timezone=scheduler_fn.Timezone("America/New_York"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_us_close_positions(event: scheduler_fn.ScheduledEvent) -> None:
    """미국 장 마감 10분 전 포지션 전량 청산 (ET 15:50)"""
    for uid, cfg in _get_all_users():
        positions = get_positions(uid, "US")
        for code, pos in list(positions.items()):
            try:
                data    = get_current_price_us(uid, cfg, code)
                current = float(data["output"].get("last", 0))
                qty     = pos["quantity"]
                place_order_us(uid, cfg, code, "sell", qty, 0)
                pnl = register_sell(uid, "US", code, current)
                add_trade(uid, "US", code, "sell", current, qty, "US장마감_청산", pnl)
                state = get_bot_state(uid)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO", f"[US][{code}] 마감 청산 | ${current:.2f} | PnL ${pnl:+.2f}")
            except Exception as e:
                _add_log(uid, "ERROR", f"[US][{code}] 마감 청산 오류: {e}")


# ══════════════════════════════════════════════════════════
# Flask HTTP API Routes
# ══════════════════════════════════════════════════════════

def _ts_to_str(ts) -> str:
    if ts is None: return ""
    if isinstance(ts, datetime): return ts.isoformat()
    if hasattr(ts, "seconds"): return datetime.fromtimestamp(ts.seconds, KST).isoformat()
    return str(ts)


def _require_auth():
    """토큰 검증 → uid 반환. 실패 시 Flask response tuple 반환."""
    try:
        return verify_token(request), None
    except PermissionError as e:
        return None, (jsonify({"ok": False, "error": str(e)}), 401)
    except Exception as e:
        return None, (jsonify({"ok": False, "error": f"인증 오류: {e}"}), 401)


@flask_app.route("/api/setup", methods=["POST"])
def route_setup():
    """최초 설정 — KIS API 키 + 계좌번호 저장"""
    uid, err = _require_auth()
    if err: return err
    body = request.get_json() or {}
    required = ["app_key", "app_secret", "account_no"]
    for f in required:
        if not body.get(f):
            return jsonify({"ok": False, "error": f"{f} 필수"}), 400
    cfg = {
        "app_key": body["app_key"].strip(),
        "app_secret": body["app_secret"].strip(),
        "account_no": body["account_no"].strip(),
        "is_mock": body.get("is_mock", True),
        "kr_watchlist": body.get("kr_watchlist", ["005930", "000660", "035420"]),
        "us_watchlist": body.get("us_watchlist", ["AAPL", "NVDA", "TSLA"]),
        "k_factor": float(body.get("k_factor", 0.5)),
        "ma_period": int(body.get("ma_period", 5)),
        "stop_loss_ratio": float(body.get("stop_loss_ratio", 0.02)),
        "max_position_ratio": float(body.get("max_position_ratio", 0.10)),
        "daily_profit_target": float(body.get("daily_profit_target", 0.03)),
        "ai_stock_count": int(body.get("ai_stock_count", 3)),
        "bot_enabled": True,
        "display_name": body.get("display_name", ""),
        "email": body.get("email", ""),
        "setup_complete": True,
        "created_at": datetime.now(KST).isoformat(),
    }
    save_config(uid, cfg)
    update_bot_state(uid, {"bot_enabled": True, "trading_halted": False,
                            "is_market_open": False, "realized_pnl": 0.0})
    _add_log(uid, "INFO", f"계정 설정 완료 | 모드={'모의' if cfg['is_mock'] else '실전'}")
    return jsonify({"ok": True, "message": "설정 완료"})


@flask_app.route("/api/status")
def route_status():
    uid, err = _require_auth()
    if err: return err
    try:
        cfg = get_config(uid)
        if not cfg.get("setup_complete"):
            return jsonify({"ok": True, "setup_required": True})

        state = get_bot_state(uid)
        positions_kr = get_positions(uid, "KR")
        positions_us = get_positions(uid, "US")

        balance_data: dict[str, Any] = {}
        kis_error: str = ""
        try:
            bal = _cached_balance(uid, cfg)
            summary = bal.get("output2", [{}])
            if summary:
                s = summary[0]
                balance_data = {
                    "total_equity": s.get("tot_evlu_amt", "0").replace(",", ""),
                    "available_cash": s.get("prvs_rcdl_excc_amt", "0").replace(",", ""),
                    "stock_value": s.get("scts_evlu_amt", "0").replace(",", ""),
                }
        except Exception as e:
            kis_error = str(e)
            balance_data = {"error": kis_error}
            _add_log(uid, "ERROR", f"잔고 조회 실패: {kis_error}")

        def enrich_positions(positions: dict, market: str) -> dict:
            detail = {}
            for code, pos in positions.items():
                try:
                    if market == "KR":
                        data = _cached_price(uid, cfg, code, "KR")
                        out = data["output"]
                        current = int(out["stck_prpr"])
                        sname = _stock_name(out.get("hts_kor_isnm", ""), code, "KR")
                        change_rate = out.get("prdy_ctrt", "0")
                    else:
                        data = _cached_price(uid, cfg, code, "US")
                        out = data["output"]
                        current = _us_price_from_output(out)
                        sname = _stock_name(out.get("rsym", ""), code, "US")
                        change_rate = out.get("diff", "0")
                    pnl = (current - pos["buy_price"]) * pos["quantity"]
                    pnl_ratio = (current - pos["buy_price"]) / pos["buy_price"] * 100
                    detail[code] = {
                        **pos,
                        "entry_time": _ts_to_str(pos.get("entry_time")),
                        "current_price": current,
                        "stock_name": pos.get("stock_name") or sname,
                        "change_rate": change_rate,
                        "pnl": round(pnl, 2),
                        "pnl_ratio": round(pnl_ratio, 2),
                    }
                except Exception:
                    detail[code] = {**pos, "entry_time": _ts_to_str(pos.get("entry_time"))}
            return detail

        positions_kr_detail = enrich_positions(positions_kr, "KR")
        positions_us_detail = enrich_positions(positions_us, "US")

        # 감시 종목 데이터 (KR)
        watchlist_data: dict[str, Any] = {}
        for code in cfg.get("kr_watchlist", []):
            if code in positions_kr_detail:
                p = positions_kr_detail[code]
                watchlist_data[code] = {
                    "current_price": p.get("current_price", 0),
                    "stock_name": p.get("stock_name", code),
                    "change_rate": p.get("change_rate", "0"),
                }
            else:
                try:
                    data = _cached_price(uid, cfg, code, "KR")
                    out = data["output"]
                    ohlcv = _cached_ohlcv(uid, cfg, code, "KR")
                    entry: dict[str, Any] = {
                        "current_price": int(out.get("stck_prpr", 0)),
                        "stock_name": _stock_name(out.get("hts_kor_isnm", ""), code, "KR"),
                        "change_rate": out.get("prdy_ctrt", "0"),
                    }
                    if len(ohlcv) >= 2:
                        today_open = float(ohlcv[0].get("stck_oprc", 0))
                        prev_h = float(ohlcv[1].get("stck_hgpr", 0))
                        prev_l = float(ohlcv[1].get("stck_lwpr", 0))
                        entry["target_breakout"] = int(today_open + cfg.get("k_factor", 0.5) * (prev_h - prev_l))
                    if len(ohlcv) >= 5:
                        entry["ma5"] = int(sum(float(r.get("stck_clpr", 0)) for r in ohlcv[:5]) / 5)
                    watchlist_data[code] = entry
                except Exception:
                    watchlist_data[code] = {"current_price": 0, "stock_name": _stock_name("", code, "KR"), "change_rate": "0"}

        # 미국 감시 종목 데이터
        us_watchlist_data: dict[str, Any] = {}
        for code in cfg.get("us_watchlist", []):
            if code in positions_us_detail:
                p = positions_us_detail[code]
                us_watchlist_data[code] = {
                    "current_price": p.get("current_price", 0),
                    "stock_name": p.get("stock_name", code),
                    "change_rate": p.get("change_rate", "0"),
                }
            else:
                try:
                    data = _cached_price(uid, cfg, code, "US")
                    out = data["output"]
                    ohlcv_us = _cached_ohlcv(uid, cfg, code, "US")
                    us_watchlist_data[code] = {
                        "current_price": _us_price_from_output(out, ohlcv_us),
                        "stock_name": _stock_name(out.get("rsym", ""), code, "US"),
                        "change_rate": out.get("diff", "0"),
                    }
                except Exception:
                    us_watchlist_data[code] = {"current_price": 0, "stock_name": _stock_name("", code, "US"), "change_rate": "0"}

        safe_cfg = {k: v for k, v in cfg.items() if k not in ("app_key", "app_secret")}

        return jsonify({
            "ok": True,
            "state": state, "balance": balance_data,
            "positions_kr": positions_kr_detail, "positions_us": positions_us_detail,
            "watchlist_data": watchlist_data, "us_watchlist_data": us_watchlist_data,
            "config": safe_cfg,
            "mode": "모의투자" if cfg.get("is_mock") else "실전투자",
            "updated_at": datetime.now(KST).strftime("%H:%M:%S"),
            "kis_error": kis_error,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/api/order", methods=["POST"])
def route_order():
    uid, err = _require_auth()
    if err: return err
    body = request.get_json() or {}
    stock_code = str(body.get("stock_code", "")).strip().upper()
    side = str(body.get("side", ""))
    quantity = int(body.get("quantity", 0))
    price = float(body.get("price", 0))
    market = str(body.get("market", "KR")).upper()

    if not stock_code or side not in ("buy", "sell"):
        return jsonify({"ok": False, "error": "stock_code, side(buy/sell) 필수"}), 400
    try:
        cfg = get_config(uid)
        if not cfg.get("setup_complete"):
            return jsonify({"ok": False, "error": "설정을 먼저 완료해주세요"}), 400

        if market == "KR":
            data = get_current_price_kr(uid, cfg, stock_code)
            current = int(data["output"]["stck_prpr"])
            sname = _stock_name(data["output"].get("hts_kor_isnm", ""), stock_code, "KR")
            if side == "buy":
                if quantity <= 0:
                    available = get_available_cash_kr(uid, cfg, stock_code)
                    equity = _get_total_equity_kr(uid, cfg) or available
                    invest = min(available, equity * cfg.get("max_position_ratio", 0.1))
                    quantity = math.floor(invest / current)
                if quantity <= 0:
                    return jsonify({"ok": False, "error": "수량 계산 실패"}), 400
                result = place_order_kr(uid, cfg, stock_code, "buy", quantity, int(price))
                order_no = result.get("output", {}).get("ODNO", "N/A")
                register_buy(uid, "KR", stock_code, current, quantity,
                             cfg.get("stop_loss_ratio", 0.02), stock_name=sname, source="수동")
                add_trade(uid, "KR", stock_code, "buy", current, quantity, "수동매수")
                _add_log(uid, "INFO", f"[수동매수][KR] {stock_code} {quantity}주@{current:,} 주문={order_no}")
                return jsonify({"ok": True, "order_no": order_no, "quantity": quantity, "price": current})
            else:
                positions = get_positions(uid, "KR")
                pos = positions.get(stock_code)
                if quantity <= 0:
                    if not pos: return jsonify({"ok": False, "error": "보유 포지션 없음"}), 400
                    quantity = pos["quantity"]
                result = place_order_kr(uid, cfg, stock_code, "sell", quantity, int(price))
                order_no = result.get("output", {}).get("ODNO", "N/A")
                if pos:
                    pnl = register_sell(uid, "KR", stock_code, current)
                    add_trade(uid, "KR", stock_code, "sell", current, quantity, "수동매도", pnl)
                    state = get_bot_state(uid)
                    update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO", f"[수동매도][KR] {stock_code} {quantity}주@{current:,}")
                return jsonify({"ok": True, "order_no": order_no, "quantity": quantity, "price": current})
        else:
            data = get_current_price_us(uid, cfg, stock_code)
            out = data["output"]
            current = float(out.get("last", out.get("stck_prpr", 0)))
            sname = _stock_name(out.get("rsym", ""), stock_code, "US")
            if side == "buy":
                if quantity <= 0:
                    return jsonify({"ok": False, "error": "미국 주식은 수량을 직접 입력해주세요"}), 400
                result = place_order_us(uid, cfg, stock_code, "buy", quantity, price)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                register_buy(uid, "US", stock_code, current, quantity,
                             cfg.get("stop_loss_ratio", 0.02), stock_name=sname, source="수동")
                add_trade(uid, "US", stock_code, "buy", current, quantity, "수동매수")
                _add_log(uid, "INFO", f"[수동매수][US] {stock_code} {quantity}주@${current:.2f}")
                return jsonify({"ok": True, "order_no": order_no, "quantity": quantity, "price": current})
            else:
                positions = get_positions(uid, "US")
                pos = positions.get(stock_code)
                if quantity <= 0:
                    if not pos: return jsonify({"ok": False, "error": "보유 포지션 없음"}), 400
                    quantity = pos["quantity"]
                result = place_order_us(uid, cfg, stock_code, "sell", quantity, price)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                if pos:
                    pnl = register_sell(uid, "US", stock_code, current)
                    add_trade(uid, "US", stock_code, "sell", current, quantity, "수동매도", pnl)
                    state = get_bot_state(uid)
                    update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO", f"[수동매도][US] {stock_code} {quantity}주@${current:.2f}")
                return jsonify({"ok": True, "order_no": order_no, "quantity": quantity, "price": current})

    except ApiError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/api/bot", methods=["POST"])
def route_bot():
    uid, err = _require_auth()
    if err: return err
    body = request.get_json() or {}
    action = str(body.get("action", ""))
    actions = {
        "start": ({"bot_enabled": True, "trading_halted": False}, "봇 시작됨"),
        "stop": ({"bot_enabled": False}, "봇 중지됨"),
        "resume": ({"trading_halted": False}, "매매 재개됨"),
    }
    if action not in actions:
        return jsonify({"ok": False, "error": "action: start/stop/resume"}), 400
    updates, message = actions[action]
    update_bot_state(uid, updates)
    _add_log(uid, "INFO", f"봇 제어: {message}")
    return jsonify({"ok": True, "message": message})


@flask_app.route("/api/config", methods=["GET", "POST"])
def route_config():
    uid, err = _require_auth()
    if err: return err
    if request.method == "GET":
        cfg = get_config(uid)
        safe = {k: v for k, v in cfg.items() if k not in ("app_key", "app_secret")}
        return jsonify({"ok": True, "config": safe})
    body = request.get_json() or {}
    allowed = {"is_mock", "kr_watchlist", "us_watchlist", "k_factor", "ma_period",
                "stop_loss_ratio", "max_position_ratio", "daily_profit_target",
                "bot_enabled", "ai_stock_count", "min_score_us", "max_us_qty"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        return jsonify({"ok": False, "error": "변경할 설정 없음"}), 400
    save_config(uid, updates)
    _add_log(uid, "INFO", f"설정 변경: {list(updates.keys())}")
    return jsonify({"ok": True, "updated": updates})


@flask_app.route("/api/trades")
def route_trades():
    uid, err = _require_auth()
    if err: return err
    try:
        docs = (_uref(uid).collection("trades")
                .order_by("timestamp", direction="DESCENDING").limit(50).stream())
        trades = []
        for doc in docs:
            t = doc.to_dict()
            t["timestamp"] = _ts_to_str(t.get("timestamp"))
            trades.append(t)
        return jsonify({"ok": True, "trades": trades})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/api/recommendations")
def route_recommendations():
    uid, err = _require_auth()
    if err: return err
    try:
        docs = (_uref(uid).collection("recommendations")
                .order_by("timestamp", direction="DESCENDING").limit(10).stream())
        recs = []
        for doc in docs:
            r = doc.to_dict()
            r["timestamp"] = _ts_to_str(r.get("timestamp"))
            r["completed_at"] = _ts_to_str(r.get("completed_at"))
            recs.append(r)
        return jsonify({"ok": True, "recommendations": recs})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/api/chart")
def route_chart():
    uid, err = _require_auth()
    if err: return err
    code = str(request.args.get("code", "")).strip().upper()
    market = str(request.args.get("market", "KR")).strip().upper()
    if not code:
        return jsonify({"ok": False, "error": "code 필수"}), 400
    if market not in ("KR", "US"):
        market = "KR"
    try:
        cfg = get_config(uid)
        if not cfg.get("setup_complete"):
            return jsonify({"ok": False, "error": "설정을 먼저 완료해주세요"}), 400
        ohlcv = _cached_ohlcv(uid, cfg, code, market)
        points: list[float] = []
        for row in ohlcv[:30]:
            raw = row.get("stck_clpr", row.get("clos", 0))
            try:
                val = float(str(raw).replace(",", ""))
                if val > 0:
                    points.append(val)
            except Exception:
                continue
        if not points:
            # 일봉이 비어도 현재가로 최소 표시 (UI 비노출 방지)
            if market == "US":
                cur_data = _cached_price(uid, cfg, code, "US")
                out = cur_data.get("output", {})
                current = _us_price_from_output(out, None)
            else:
                cur_data = _cached_price(uid, cfg, code, "KR")
                out = cur_data.get("output", {})
                current = float(out.get("stck_prpr", 0) or 0)
            if current > 0:
                points = [current, current]
            else:
                return jsonify({"ok": False, "error": "차트 데이터 없음"}), 404
        latest = points[0]
        prev = points[1] if len(points) > 1 else latest
        change_pct = ((latest - prev) / prev * 100) if prev else 0.0
        return jsonify({
            "ok": True,
            "code": code,
            "market": market,
            "points": list(reversed(points)),
            "latest": latest,
            "change_pct": round(change_pct, 2),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/api/ai/run", methods=["POST"])
def route_ai_run():
    uid, err = _require_auth()
    if err: return err
    body = request.get_json() or {}
    session = body.get("session", "morning")
    market = str(body.get("market", "KR")).upper()
    if session not in ("morning", "afternoon", "late"):
        return jsonify({"ok": False, "error": "session: morning/afternoon/late"}), 400
    try:
        cfg = get_config(uid)
        run_ai_session(uid, cfg, session, market)
        return jsonify({"ok": True, "message": f"AI {session} 세션 완료"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/api/myip")
def route_myip():
    """이 서버의 외부 IP 반환 — KIS API IP 등록용"""
    uid, err = _require_auth()
    if err: return err
    try:
        resp = http_requests.get("https://api64.ipify.org?format=json", timeout=5)
        ip = resp.json().get("ip", "unknown")
    except Exception:
        ip = "조회 실패"
    return jsonify({"ok": True, "server_ip": ip})


@flask_app.route("/api/logs")
def route_logs():
    uid, err = _require_auth()
    if err: return err
    try:
        docs = (_uref(uid).collection("logs")
                .order_by("timestamp", direction="DESCENDING").limit(100).stream())
        logs = []
        for doc in docs:
            l = doc.to_dict()
            l["timestamp"] = _ts_to_str(l.get("timestamp"))
            logs.append(l)
        return jsonify({"ok": True, "logs": logs})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ══════════════════════════════════════════════════════════
# Firebase Functions HTTP 진입점
# ══════════════════════════════════════════════════════════

@https_fn.on_request(
    cors=options.CorsOptions(
        cors_origins="*",
        cors_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    ),
    memory=options.MemoryOption.MB_256,
)
def api(req: https_fn.Request) -> https_fn.Response:
    with flask_app.request_context(req.environ):
        return flask_app.full_dispatch_request()
