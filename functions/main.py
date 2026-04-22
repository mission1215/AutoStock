"""
AutoStock Firebase Functions — 멀티유저 KIS 자동매매 시스템 (한국 + 미국주식)

▣ 기본 구조
  - 인증: Firebase Auth (Google 로그인) → Bearer 토큰으로 uid 추출
  - 데이터: Firestore users/{uid}/...  (config / state / positions_KR / positions_US /
            trades / logs / recommendations 컬렉션) — 유저 단위로 격리
  - 외부 API: 한국투자증권 KIS OpenAPI (실서버/모의서버), Google Gemini

▣ HTTP API (/api/*):
  POST /api/setup           — 최초 설정 (KIS 키 저장)
  GET  /api/status          — 대시보드 전체 상태
  POST /api/order           — 수동 매수/매도 (KR/US)
  POST /api/bot             — 봇 시작/중지
  GET  /api/config          — 설정 조회
  POST /api/config          — 설정 변경 (allowed 키 화이트리스트)
  GET  /api/trades          — 매매 이력
  GET  /api/logs            — 최근 로그
  GET  /api/recommendations — AI 추천 이력
  GET  /api/research        — 오늘의 시장 리서치 (Gemini, 일 1회 캐시)
  GET  /api/quote           — 종목 현재가·이름 (주문 전 참고용)
  POST /api/ai/run          — AI 추천 수동 실행

▣ 자동매매 사이클 (스케줄 함수)
  - scheduled_strategy_cycle (KR, * * * * *):
      run_strategy_cycle_kr 호출. 보유 포지션 점검 → 신규 매수 스캔.
  - scheduled_strategy_cycle_us (US, */5 9-15 * * 1-5 ET):
      run_strategy_cycle_us 호출. 5분 간격(잔고/지수 API 비용 절약).
  - scheduled_close_positions (KR 15:20 KST):
      장 마감 직전 전 포지션 청산.
  - scheduled_close_positions_us (US 15:50 ET): 동일.
  - scheduled_reconcile_kr / _us (30분 간격, 장중):
      Firestore 포지션 ↔ KIS 실재 잔고 비교. 외부 청산·부분 매도 자동 보정.
  - 그 외 09:00, 13:00, 15:30 등 시점에 AI 추천·요약 함수.

▣ 리스크 관리 레이어 (전부 cfg 키로 켜고 끌 수 있음)
  ① 사이징     : ATR 기반 리스크-패리티 (`_risk_based_qty`)
                 → 1회 진입 손실 = equity × risk_per_trade_pct(기본 1%)
  ② 단일 포지션:
       - ATR 기반 손절가 보존 (register_buy stop_loss_price 인자)
       - 분할익절 + 손절선 본전 위로 타이트닝 (partial_tp_*)
       - 본전 스탑 (breakeven_*)
       - 트레일링 스탑 (trailing_stop_*)
       - 시간 기반 청산 (time_stop_*)
       - 물타기 후 평단/손절/고점 리셋 (avg_down_*)
  ③ 진입 시간대:
       - 개장·마감 블랙아웃 (kr_skip_buy_first_min / kr_skip_buy_last_min, US 동일)
       - 월요일 오전 N분 차단 옵션 (monday_morning_skip_*)
  ④ 포트폴리오:
       - 일간 수익 잠금 / 손실 한도 (`_daily_pnl_buy_gate`,
                                    daily_profit_target / daily_loss_limit)
       - 피크 대비 드로다운 서킷 브레이커 (`_check_drawdown`, max_drawdown_pct)
  ⑤ 시장 레짐:
       - KOSPI 급락 신규매수 게이트 (`_kr_index_buy_gate`,
                                     kr_index_drop_limit_pct)
       - SPY 급락 신규매수 게이트 (`_us_index_buy_gate`,
                                  us_index_drop_limit_pct)

▣ 운영 안전망
  - 포지션 reconcile (`reconcile_positions`):
      Firestore와 KIS 잔고 차이를 자동 보정 (외부 청산 시 포지션 삭제,
      부분 매도 시 quantity down-update). 30분 주기 자동 실행.
  - 체결가 사후 조회 (`_log_sell_fill` + inquire_order_fill_kr/us):
      매도 직후 실제 평균 체결가를 조회하여 시그널가 대비 슬리피지를 로그로 남김
      (옵저버블리티 전용 — register_sell PnL 계산은 시그널가 그대로 사용).

▣ 게이트 적용 순서 (run_strategy_cycle_kr / _us의 신규매수 진입부)
  ① 시간대 블랙아웃 (개장/마감/월요일)
  ② 일간 P&L 게이트 (수익 잠금 / 손실 한도)
  ③ 시장 레짐 게이트 (KOSPI / SPY)
  → 모두 통과해야 신규 매수 스캔 진행. 매도/포지션 관리는 위 게이트와 무관하게 항상 동작.
"""

import os
import math
import logging
import time as time_module
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from typing import Any

import json
import re
import requests as http_requests
import firebase_admin
from firebase_admin import firestore, auth as fb_auth
from google.cloud.firestore_v1 import transactional
from firebase_functions import https_fn, scheduler_fn, options
from flask import Flask, request, jsonify
from google import genai
from google.genai import types as genai_types

KST = ZoneInfo("Asia/Seoul")
ET  = ZoneInfo("America/New_York")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# KIS OpenAPI: 초당 거래(조회·주문) 건수 제한 — HTTP 500 "초당 거래건수를 초과하였습니다"
# 주문·주문가능까지 같은 간격으로 묶이므로 0.2 미만이면 AI 연속 매수에서 자주 걸림
KIS_MIN_INTERVAL_SEC = 0.26
_kis_last_http_ts: float = 0.0


def _kis_pace() -> None:
    """국내·해외 시세 등 KIS REST 요청 직전 호출(프로세스당 간격)."""
    global _kis_last_http_ts
    now = time_module.time()
    gap = now - _kis_last_http_ts
    if gap < KIS_MIN_INTERVAL_SEC:
        time_module.sleep(KIS_MIN_INTERVAL_SEC - gap)
    _kis_last_http_ts = time_module.time()


def _is_kis_tps_exceeded(exc: BaseException) -> bool:
    s = str(exc)
    return "초당" in s and ("초과" in s or "거래건수" in s)


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
    """현재가를 인메모리 캐시에서 반환. 만료 시 KIS API 재조회.

    조회 우선순위:
    1. 인메모리 캐시 (TTL=60초)
    2. Firestore realtime_prices/{code} — kis_ws.py 데몬이 15초마다 갱신 (KR만)
    3. KIS REST API (폴백)
    """
    key = f"{uid}:{market}:{code}"
    now = time_module.time()

    # 1. 인메모리 캐시
    if key in _price_cache and now - _price_cache[key]["ts"] < _PRICE_TTL:
        return _price_cache[key]["data"]

    # 2. Firestore 실시간 가격 (WebSocket 데몬 기록, KR 전용)
    if market == "KR":
        try:
            rt_doc = get_db().collection("realtime_prices").document(code).get()
            if rt_doc.exists:
                rt = rt_doc.to_dict()
                rt_ts = rt.get("timestamp")
                if rt_ts:
                    rt_age = (datetime.now(KST) - rt_ts.astimezone(KST)).total_seconds()
                    if rt_age < 15:  # 15초 이내의 WebSocket 데이터
                        price = int(rt.get("price", 0))
                        if price > 0:
                            data = {
                                "output": {
                                    "stck_prpr": str(price),
                                    "stck_clpr": str(price),
                                    "acml_vol":  str(rt.get("volume", 0)),
                                    "_source":   "ws",
                                }
                            }
                            _price_cache[key] = {"data": data, "ts": now}
                            return data
        except Exception:
            pass  # WebSocket 데몬 미실행 시 REST API 폴백

    # 3. KIS REST API
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


def _invalidate_balance_cache(uid: str) -> None:
    """주문 직후 잔고·주문가능 금액이 바로 반영되도록 캐시 제거."""
    _balance_cache.pop(uid, None)

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
# REST API에서 획득한 KR 종목명 인메모리 캐시
# Cloud Function 인스턴스가 재사용될 때 유지되므로 WebSocket 폴백 시 활용
_kr_name_cache: dict[str, str] = {}

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

# ── 섹터 분류 맵 ───────────────────────────────────────────────────
KR_SECTOR_MAP: dict[str, str] = {
    # 반도체
    "005930": "반도체", "000660": "반도체", "042700": "반도체",
    "009150": "반도체", "011070": "반도체",
    # IT/플랫폼
    "035420": "IT플랫폼", "035720": "IT플랫폼", "018260": "IT플랫폼",
    # 자동차
    "005380": "자동차", "000270": "자동차", "012330": "자동차", "030600": "자동차",
    # 2차전지
    "247540": "2차전지", "086520": "2차전지", "373220": "2차전지",
    "006400": "2차전지", "051910": "2차전지",
    # 바이오/제약
    "068270": "바이오", "128940": "바이오", "000100": "바이오",
    "326030": "바이오", "207940": "바이오",
    # 금융
    "105560": "금융", "055550": "금융", "086790": "금융",
    "316140": "금융", "024110": "금융", "039490": "금융",
    "071050": "금융", "032830": "금융",
    # 조선/방산
    "267250": "조선방산", "009540": "조선방산", "064350": "조선방산",
    # 엔터/게임
    "352820": "엔터게임", "036570": "엔터게임", "251270": "엔터게임",
    "263750": "엔터게임", "041510": "엔터게임", "035900": "엔터게임",
    "293490": "엔터게임",
    # 철강/소재
    "005490": "철강소재", "010130": "철강소재", "004020": "철강소재",
    # 기타
    "017670": "통신", "030200": "통신", "015760": "에너지",
    "028260": "지주", "034730": "지주", "003550": "지주",
}

US_SECTOR_MAP: dict[str, str] = {
    # 반도체
    "NVDA": "반도체", "AMD": "반도체", "QCOM": "반도체",
    "INTC": "반도체", "AVGO": "반도체", "MU": "반도체",
    "AMAT": "반도체", "TSM": "반도체", "ASML": "반도체",
    "ARM": "반도체", "SOXL": "반도체",
    # 빅테크
    "AAPL": "빅테크", "MSFT": "빅테크", "GOOGL": "빅테크",
    "AMZN": "빅테크", "META": "빅테크", "ORCL": "빅테크",
    "ADBE": "빅테크", "CRM": "빅테크", "NOW": "빅테크",
    "TQQQ": "빅테크", "QQQ": "빅테크",
    # EV/자동차
    "TSLA": "EV자동차", "RIVN": "EV자동차", "NIO": "EV자동차",
    # 금융
    "JPM": "금융", "V": "금융", "MA": "금융", "GS": "금융",
    # 바이오
    "UNH": "바이오", "JNJ": "바이오",
    # AI/플랫폼
    "PLTR": "AI플랫폼", "NFLX": "AI플랫폼",
    # 기타
    "COIN": "암호화폐", "SOFI": "핀테크",
    "WMT": "유통", "HD": "유통",
    "BIDU": "중국빅테크",
}


def _stock_name(api_name: str, code: str, market: str = "KR") -> str:
    name = (api_name or "").strip()
    if name:
        # KR은 REST API에서 얻은 이름을 인스턴스 캐시에 저장 (WebSocket 폴백용)
        if market == "KR":
            _kr_name_cache[code] = name
        return name
    if market == "KR":
        # 인스턴스 캐시 → 정적 dict → 티커 순으로 폴백
        return _kr_name_cache.get(code) or KR_STOCK_NAMES.get(code, code)
    # US: US_STOCK_NAMES에 없으면 티커 그대로 반환 (rsym 같은 Reuters 코드는 사용 안 함)
    return US_STOCK_NAMES.get(code, code)


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


def _parse_num_kr(raw: Any) -> int:
    """한국 시세 숫자 필드 — 콤마·대시 포함 문자열 대응 (int('217,500') 방지)."""
    if raw is None:
        return 0
    try:
        s = str(raw).strip().replace(",", "")
        if not s or s in ("-", ".", "--"):
            return 0
        return int(float(s))
    except Exception:
        return 0


def _kr_closes_from_ohlcv(ohlcv: list) -> list[int]:
    """KIS inquire-daily-price output2 일자별 종가. 오래된 날→최근 순 (스파크라인용)."""
    newest_first: list[int] = []
    for r in ohlcv[:20]:
        if not isinstance(r, dict):
            continue
        raw = r.get("stck_clpr", r.get("clos", 0))
        v = _parse_num_kr(raw)
        if v > 0:
            newest_first.append(v)
    return list(reversed(newest_first))


def _us_closes_from_ohlcv(ohlcv: list) -> list[float]:
    """미국 일봉 종가. 오래된 날→최근 순."""
    newest_first: list[float] = []
    for r in ohlcv[:20]:
        if not isinstance(r, dict):
            continue
        raw = r.get("clos", r.get("stck_clpr", 0))
        try:
            v = float(str(raw).replace(",", "").strip() or 0)
        except Exception:
            continue
        if v > 0:
            newest_first.append(v)
    return list(reversed(newest_first))


def _ensure_sparkline_closes_kr(
    closes: list[int], current: int, buy: int | None = None
) -> list[int]:
    """스파크라인은 최소 2포인트 필요. 일봉 실패 시 현재가·매수가로 대체."""
    if len(closes) >= 2:
        return closes
    if len(closes) == 1:
        c = closes[0]
        return [c, c]
    cur = int(current) if current and current > 0 else 0
    bp = int(buy) if buy and buy > 0 else 0
    if cur > 0 and bp > 0:
        return [bp, cur]
    if cur > 0:
        return [cur, cur]
    return []


def _ensure_sparkline_closes_us(
    closes: list[float], current: float, buy: float | None = None
) -> list[float]:
    if len(closes) >= 2:
        return closes
    if len(closes) == 1:
        c = closes[0]
        return [c, c]
    cur = float(current) if current and current > 0 else 0.0
    bp = float(buy) if buy and buy > 0 else 0.0
    if cur > 0 and bp > 0:
        return [bp, cur]
    if cur > 0:
        return [cur, cur]
    return []


def _normalize_kr_stock_code(code: Any) -> str:
    """감시·API 키 통일 — 숫자만 6자리 (5930 → 005930, Firestore 타입 혼용 대응)."""
    s = str(code).strip()
    if s.isdigit() and len(s) <= 6:
        return s.zfill(6)
    return s


def _kr_price_from_output(out: dict | Any, ohlcv: list | None = None) -> int:
    """국내 주식현재가 시세 output에서 가격 추출. 누락·0이면 일봉·전일가 폴백."""
    if not isinstance(out, dict):
        out = {}
    for key in ("stck_prpr", "antc_cnpr", "prdy_clpr", "bfdy_clpr"):
        v = _parse_num_kr(out.get(key))
        if v > 0:
            return v
    if ohlcv:
        for row in ohlcv[:10]:
            if not isinstance(row, dict):
                continue
            for k in ("stck_clpr", "stck_oprc", "stck_hgpr"):
                v = _parse_num_kr(row.get(k))
                if v > 0:
                    return v
    return 0


def _kr_holdings_prpr_by_code(bal: dict) -> dict[str, int]:
    """잔고 조회 output1의 종목별 현재가(prpr) — 시세 단독 실패 시 보유 종목 표시용."""
    m: dict[str, int] = {}
    for row in bal.get("output1") or []:
        if not isinstance(row, dict):
            continue
        pd = str(row.get("pdno", "")).strip()
        if not pd:
            continue
        if pd.isdigit() and len(pd) <= 6:
            pd = pd.zfill(6)
        pr = _parse_num_kr(row.get("prpr"))
        if pr > 0:
            m[pd] = pr
    return m


def _kr_price_from_api_data(data: dict, ohlcv: list | None = None) -> int:
    out = data.get("output") or {}
    if not isinstance(out, dict):
        out = {}
    return _kr_price_from_output(out, ohlcv)


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


def _ensure_user_root_doc(uid: str) -> None:
    """`users` 컬렉션 `.stream()` 에 잡히도록 `users/{uid}` 문서를 보장.

    서브컬렉션만 있고 루트가 비어 있으면 나열되지 않아 `_get_all_users` 가 0명이 될 수 있음.
    이미 루트가 있으면 쓰지 않는다(폴링/저장 반복 시 불필요한 갱신 방지).
    """
    r = _uref(uid)
    if r.get().exists:
        return
    r.set(
        {"autostock_user": True, "user_doc_at": datetime.now(KST).isoformat()},
        merge=True,
    )


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
# 설정 관리 (per-user) — 모의/실전 프로필 분리
# ══════════════════════════════════════════════════════════

# Firestore config/settings: { is_mock, setup_complete, profiles: { mock: {...}, live: {...} } }
# 레거시(평면 1문서)는 get_config 시 그대로 읽고, save_config 시 profiles 로 이관.
_CONFIG_TOP_KEYS = frozenset({"is_mock", "setup_complete", "display_name", "email", "created_at"})
_CONFIG_PROFILE_KEYS = frozenset({
    "app_key", "app_secret", "account_no",
    "kr_watchlist", "us_watchlist", "k_factor", "ma_period",
    "stop_loss_ratio", "max_position_ratio", "daily_profit_target",
    "ai_stock_count", "min_score_kr", "min_score_us", "min_score_us_ai",
    "ai_afford_one_share", "max_us_qty",
    "partial_tp_enabled", "partial_tp_trigger_pct", "partial_tp_sell_ratio",
    "partial_tp_tighten_stop",
    "avg_down_enabled", "avg_down_trigger_pct", "avg_down_max_times",
    "avg_down_qty_ratio", "avg_down_min_interval_hours",
    "trailing_stop_enabled", "trailing_stop_pct", "trailing_stop_activate_pct",
    "breakeven_stop_enabled", "breakeven_trigger_pct",
    "time_stop_enabled", "time_stop_days", "time_stop_flat_pct",
    "partial_tp_tighten_buffer_pct",
    "kr_sell_ord_dvsn",
    "kr_skip_buy_first_min", "kr_skip_buy_last_min",
    "us_skip_buy_first_min", "us_skip_buy_last_min",
    "daily_loss_limit", "kr_index_drop_limit_pct",
    "us_index_drop_limit_pct", "us_index_proxy",
    "risk_per_trade_pct",
    "reconcile_enabled", "fill_check_enabled",
    "monday_morning_skip_enabled", "monday_morning_skip_min",
    "max_entry_slip_pct", "max_entry_slip_pct_mock", "max_entry_slip_pct_live",
    "bot_enabled",
})


def _sanitize_profile_for_client(prof: dict) -> dict:
    return {k: v for k, v in prof.items() if k not in ("app_key", "app_secret")}


def _profiles_for_client_payload(raw: dict) -> dict:
    """GET 응답용 mock/live 프로필 (비밀키 제거). 레거시 평면 문서는 ensure 후 양쪽에 복제."""
    if not raw:
        return {"mock": {}, "live": {}}
    merged = _ensure_profiles_structure(dict(raw))
    profs = merged.get("profiles") or {}
    return {
        "mock": _sanitize_profile_for_client(dict(profs.get("mock") or {})),
        "live": _sanitize_profile_for_client(dict(profs.get("live") or {})),
    }


def _ensure_profiles_structure(raw: dict) -> dict:
    if "profiles" in raw and isinstance(raw.get("profiles"), dict):
        p = raw["profiles"]
        return {
            **raw,
            "profiles": {
                "mock": dict(p.get("mock") or {}),
                "live": dict(p.get("live") or {}),
            },
        }
    prof = {k: raw[k] for k in _CONFIG_PROFILE_KEYS if k in raw}
    cleaned = {k: v for k, v in raw.items() if k not in _CONFIG_PROFILE_KEYS}
    cleaned["profiles"] = {"mock": prof.copy(), "live": prof.copy()}
    return cleaned


def get_config_raw(uid: str) -> dict:
    doc = _uref(uid).collection("config").document("settings").get()
    if not doc.exists:
        return {}
    return doc.to_dict() or {}


def get_config(uid: str) -> dict:
    """활성 모드(mock/live) 프로필 + 공통 메타를 평면 dict 로 병합 (기존 코드 호환).

    활성 모드 프로필에 자격증명이 없으면 다른 모드 프로필에서 폴백.
    """
    raw = get_config_raw(uid)
    if not raw:
        return {}
    if "profiles" not in raw or not isinstance(raw.get("profiles"), dict):
        return raw
    raw = _ensure_profiles_structure(raw)
    mode = "mock" if raw.get("is_mock", True) else "live"
    p = raw["profiles"].get(mode) or {}
    # 자격증명이 없으면 다른 모드에서 폴백 (모드 전환 후 재설정 전까지 동작 보장)
    if not (p.get("app_key") and p.get("app_secret") and p.get("account_no")):
        other_mode = "live" if mode == "mock" else "mock"
        other_p = raw["profiles"].get(other_mode) or {}
        if other_p.get("app_key") and other_p.get("app_secret") and other_p.get("account_no"):
            p = {**other_p, **{k: v for k, v in p.items() if v is not None}}
    out = {**p}
    out["is_mock"] = raw.get("is_mock", True)
    for k in ("setup_complete", "display_name", "email", "created_at"):
        if k in raw:
            out[k] = raw[k]
    return out


def save_config(uid: str, data: dict):
    """병합 저장: is_mock 등은 최상위, 전략·API키는 현재 모드 프로필에만 기록."""
    ref = _uref(uid).collection("config").document("settings")
    snap = ref.get()
    raw = snap.to_dict() if snap.exists else {}
    raw = _ensure_profiles_structure(raw)
    profiles = raw.get("profiles") or {"mock": {}, "live": {}}
    for k in _CONFIG_TOP_KEYS:
        if k in data:
            raw[k] = data[k]
    is_m = raw.get("is_mock", True)
    if isinstance(is_m, str):
        is_m = str(is_m).lower() not in ("false", "0", "no", "")
    raw["is_mock"] = bool(is_m)
    mode = "mock" if raw["is_mock"] else "live"
    prof = dict(profiles.get(mode) or {})
    for k, v in data.items():
        if k in _CONFIG_PROFILE_KEYS:
            prof[k] = v
    profiles[mode] = prof
    # 자격증명 크로스-프로필 동기화: 한 쪽에만 있으면 다른 쪽에도 복사
    _cred_keys = ("app_key", "app_secret", "account_no")
    for src, dst in (("mock", "live"), ("live", "mock")):
        sp = profiles.get(src) or {}
        dp = profiles.get(dst) or {}
        if all(sp.get(k) for k in _cred_keys) and not all(dp.get(k) for k in _cred_keys):
            for k in _cred_keys:
                dp[k] = sp[k]
            profiles[dst] = dp
    raw["profiles"] = profiles
    for k in list(raw.keys()):
        if k in _CONFIG_PROFILE_KEYS:
            del raw[k]
    ref.set(raw)
    _ensure_user_root_doc(uid)


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


def get_token_real(uid: str, cfg: dict) -> str:
    """미국 주식 전용 실서버 토큰.
    is_mock 설정과 무관하게 항상 실서버(openapi.koreainvestment.com)에서 발급.
    모의 토큰으로 실서버 US 시세 API를 호출하면 401 → 이 함수로 해결.
    """
    doc = _uref(uid).collection("state").document("token_real").get()
    now = datetime.now(KST)
    if doc.exists:
        data = doc.to_dict()
        token = data.get("access_token", "")
        expires_at = data.get("expires_at")
        if token and expires_at:
            exp = expires_at if getattr(expires_at, "tzinfo", None) else expires_at.replace(tzinfo=KST)
            if exp > now + timedelta(minutes=5):
                return token
    url = _base_url(False) + "/oauth2/tokenP"
    resp = http_requests.post(
        url,
        json={"grant_type": "client_credentials",
              "appkey": cfg["app_key"], "appsecret": cfg["app_secret"]},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if "access_token" not in data:
        raise ValueError(f"실서버 토큰 오류: {data}")
    token = data["access_token"]
    expires_at = datetime.now(KST) + timedelta(seconds=int(data.get("expires_in", 86400)))
    _uref(uid).collection("state").document("token_real").set({
        "access_token": token, "expires_at": expires_at,
        "issued_at": datetime.now(KST),
    })
    _add_log(uid, "INFO", f"[US] 실서버 토큰 발급 | 만료: {expires_at.strftime('%H:%M:%S')}")
    return token


def _headers_us(uid: str, cfg: dict, tr_id: str) -> dict:
    """미국 주식 시세 API 헤더 — 항상 실서버 토큰 사용 (모의/실전 무관)"""
    return {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {get_token_real(uid, cfg)}",
        "appkey": cfg["app_key"],
        "appsecret": cfg["app_secret"],
        "tr_id": tr_id,
        "custtype": "P",
    }


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
    if resp.status_code >= 400:
        try:
            data = resp.json()
        except ValueError:
            raise ApiError(f"KIS HTTP {resp.status_code}: {resp.text[:600]}") from None
        rt_cd = str(data.get("rt_cd", ""))
        msg_cd = data.get("msg_cd", "")
        msg1 = data.get("msg1", str(data))
        if msg_cd in ("EGW00123", "EGW00121"):
            invalidate_token(uid)
            raise ApiError(f"토큰 만료: {msg_cd}", rt_cd=rt_cd, msg_cd=msg_cd)
        raise ApiError(f"KIS HTTP {resp.status_code}: {msg1}", rt_cd=rt_cd, msg_cd=msg_cd)
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
            if _is_kis_tps_exceeded(e) and attempt < retries - 1:
                time_module.sleep(0.55 + 0.45 * attempt)
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
    """주식현재가 시세 — 코스피(J)·코스닥(Q) 순으로 조회해 빈 시세 완화."""

    def _fetch(div: str):
        _kis_pace()
        resp = http_requests.get(
            _base_url(cfg.get("is_mock", True)) + "/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=_headers(uid, cfg, "FHKST01010100"),
            params={"FID_COND_MRKT_DIV_CODE": div, "FID_INPUT_ISCD": stock_code},
            timeout=10,
        )
        return _parse(resp, uid, cfg)

    def _call():
        last_ok: dict | None = None
        for div in ("J", "Q"):
            try:
                data = _fetch(div)
                out = data.get("output") or {}
                if isinstance(out, dict) and _kr_price_from_output(out, None) > 0:
                    return data
                last_ok = data
            except ApiError:
                continue
        if last_ok is not None:
            return last_ok
        return _fetch("J")

    return _with_retry(_call)


def get_daily_ohlcv_kr(uid: str, cfg: dict, stock_code: str) -> list:
    last_exc: BaseException | None = None
    for attempt in range(4):
        try:
            _kis_pace()
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
        except ApiError as e:
            last_exc = e
            if attempt < 3 and _is_kis_tps_exceeded(e):
                time_module.sleep(0.55 + 0.45 * attempt)
                continue
            raise
    assert last_exc is not None
    raise last_exc


def get_balance_kr(uid: str, cfg: dict) -> dict:
    tr_id = _tr_id(cfg, "TTTC8434R", "VTTC8434R")

    def _call():
        _kis_pace()
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

    return _with_retry(_call, retries=4)


def get_available_cash_kr(uid: str, cfg: dict, stock_code: str = "005930") -> int:
    tr_id = _tr_id(cfg, "TTTC8908R", "VTTC8908R")

    def _call():
        _kis_pace()
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

    return _with_retry(_call, retries=4)


def place_order_kr(uid: str, cfg: dict, stock_code: str, side: str, quantity: int, price: int = 0) -> dict:
    """국내 주식 주문.

    ORD_DVSN 선정:
      - price > 0 → 00(지정가)
      - price == 0 (시장 성격 주문):
          · 매수 → 01(시장가)
          · 매도 → cfg["kr_sell_ord_dvsn"] (기본 03=최유리지정가). 시장가(01)보다 슬리피지 완화.
            허용값: "01"(시장가), "03"(최유리), "04"(최우선). 잘못된 값이면 01로 폴백.
    """
    tr_id = _tr_id(cfg, "TTTC0802U" if side == "buy" else "TTTC0801U",
                        "VTTC0802U" if side == "buy" else "VTTC0801U")
    if price > 0:
        ord_dvsn = "00"
    else:
        if side == "buy":
            ord_dvsn = "01"
        else:
            cand = str(cfg.get("kr_sell_ord_dvsn", "03"))
            ord_dvsn = cand if cand in ("01", "03", "04") else "01"
    # KIS 문서·커뮤니티: 매도 시 SLL_TYPE 등 누락 시 order-cash 가 HTTP 500 반환하는 사례 있음
    body: dict[str, str] = {
        "CANO": _account_prefix(cfg["account_no"]),
        "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
        "PDNO": stock_code,
        "ORD_DVSN": ord_dvsn,
        "ORD_QTY": str(quantity),
        "ORD_UNPR": str(int(price)),
    }
    if side == "sell":
        body["SLL_TYPE"] = "01"  # 01: 일반매도
        body["CTAC_TLNO"] = ""
        body["ALGO_NO"] = ""

    def _call():
        _kis_pace()
        resp = http_requests.post(
            _base_url(cfg.get("is_mock", True)) + "/uapi/domestic-stock/v1/trading/order-cash",
            headers=_headers(uid, cfg, tr_id),
            json=body,
            timeout=15,
        )
        return _parse(resp, uid, cfg)

    return _with_retry(_call, retries=5)


# ── 미국 주식 API ──────────────────────────────────────────

US_MARKET_MAP = {"NASD": "NASD", "NYSE": "NYSE", "AMEX": "AMEX"}

def _us_excd(stock_code: str) -> str:
    """매매 API용 거래소 코드 (4자리, 기본 NASD)"""
    return "NASD"


def _us_excd_quote(stock_code: str) -> str:
    """시세 조회 API용 거래소 코드 (3자리, 기본 NAS)
    HHDFS00000300 / HHDFS76240000 등 조회 API는 3자리 코드 사용.
    """
    return "NAS"


def get_current_price_us(uid: str, cfg: dict, stock_code: str) -> dict:
    """미국 주식 현재가 조회 — 항상 실서버 + 실서버 토큰"""
    def _call():
        _kis_pace()
        resp = http_requests.get(
            _base_url(False) + "/uapi/overseas-price/v1/quotations/price",
            headers=_headers_us(uid, cfg, "HHDFS00000300"),
            params={"AUTH": "", "EXCD": _us_excd_quote(stock_code), "SYMB": stock_code},
            timeout=10,
        )
        parsed = _parse(resp, uid, cfg)
        # KIS 응답은 환경/버전에 따라 output 또는 output1을 사용함
        if "output" not in parsed and "output1" in parsed:
            parsed["output"] = parsed.get("output1", {})
        return parsed
    return _with_retry(_call)


def get_daily_ohlcv_us(uid: str, cfg: dict, stock_code: str) -> list:
    """미국 주식 일봉 조회 — 항상 실서버 + 실서버 토큰"""
    last_exc: BaseException | None = None
    for attempt in range(4):
        try:
            _kis_pace()
            resp = http_requests.get(
                _base_url(False) + "/uapi/overseas-price/v1/quotations/dailyprice",
                headers=_headers_us(uid, cfg, "HHDFS76240000"),
                params={
                    "AUTH": "", "EXCD": _us_excd_quote(stock_code),
                    "SYMB": stock_code, "GUBN": "0", "BYMD": "", "MODP": "0",
                },
                timeout=10,
            )
            return _parse(resp, uid, cfg).get("output2", [])
        except ApiError as e:
            last_exc = e
            if attempt < 3 and _is_kis_tps_exceeded(e):
                time_module.sleep(0.55 + 0.45 * attempt)
                continue
            raise
    assert last_exc is not None
    raise last_exc


def get_balance_us(uid: str, cfg: dict) -> dict:
    """미국 주식 잔고 조회 — 항상 실서버 (VTS 미지원)"""
    def _call():
        _kis_pace()
        resp = http_requests.get(
            _base_url(False) + "/uapi/overseas-stock/v1/trading/inquire-balance",
            headers=_headers_us(uid, cfg, "JTTT3012R"),
            params={
                "CANO": _account_prefix(cfg["account_no"]),
                "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
                "OVRS_EXCG_CD": "NASD", "TR_CRCY_CD": "USD",
                "CTX_AREA_FK200": "", "CTX_AREA_NK200": "",
            },
            timeout=10,
        )
        return _parse(resp, uid, cfg)

    return _with_retry(_call, retries=4)


def place_order_us(uid: str, cfg: dict, stock_code: str, side: str, quantity: int, price: float = 0) -> dict:
    """미국 주식 매수/매도 주문"""
    # KIS VTS(모의)는 미국 주식 매매 미지원 → 항상 실서버 + 실서버 TR ID
    tr_id = "JTTT1002U" if side == "buy" else "JTTT1006U"
    ord_dvsn = "00" if price > 0 else "01"  # 00=지정가, 01=시장가
    body = {
        "CANO": _account_prefix(cfg["account_no"]),
        "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
        "OVRS_EXCG_CD": _us_excd(stock_code),
        "PDNO": stock_code,
        "ORD_DVSN": ord_dvsn,
        "ORD_QTY": str(quantity),
        "OVRS_ORD_UNPR": f"{price:.2f}",
    }

    def _call():
        _kis_pace()
        resp = http_requests.post(
            _base_url(False) + "/uapi/overseas-stock/v1/trading/order",
            headers=_headers_us(uid, cfg, tr_id),
            json=body,
            timeout=15,
        )
        return _parse(resp, uid, cfg)

    return _with_retry(_call, retries=5)


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
                 stock_name: str = "",
                 stop_loss_price: float | None = None):
    """Firestore positions_{market}/{stock_code} 에 신규 포지션 등록.

    저장되는 필드 (포지션 문서 스키마):
      - stock_code        : 종목코드 (Document ID와 동일)
      - stock_name        : 종목명 (한글/영문)
      - market            : "KR" | "US"
      - buy_price         : 평균 매수가 (원/USD). 물타기 시 갱신됨.
      - quantity          : 보유 수량.
      - stop_loss_price   : 손절가. 인자로 받으면 그대로, 아니면 buy*(1-ratio).
      - target_sell_price : 목표 매도가 (ATR 기반 또는 AI 추천).
      - source            : 매수 출처 ("수동" / "자동" / "자동_US" / "AI_..." 등)
      - entry_time        : 매수 일시 (datetime, KST). 시간 청산·age 계산에 사용.
      - partial_tp_done   : 분할익절 1회 실행 여부 — 평단 변경(물타기) 시 False로 reset.
      - avg_down_count    : 물타기 누적 횟수 (`avg_down_max_times` 에 캡됨).
      - highest_price     : 트레일링 스탑 기준 최고가 — 신규 진입 시 buy_price 로 시작.

    인자 stop_loss_price 처리:
      - None 또는 0 이하  → buy_price * (1 - stop_loss_ratio) 으로 계산 (폴백).
      - >0 (ATR 손절가)   → 그대로 저장. ATR 기반 손절선을 우선시할 때 사용.
        호출 측이 calculate_optimal_prices/_us 의 stop_loss 를 넘겨 활용한다.

    사이드 이펙트:
      - 호출 직후 add_trade 로 매매 기록을 별도로 저장하는 것은 호출 측 책임.
      - `merge_position_after_avg_down` 도 동일한 문서를 update 하므로
        추가 필드(avg_down_last_at 등)가 후행으로 들어올 수 있음.
    """
    if stop_loss_price is None or stop_loss_price <= 0:
        stop_loss_price = buy_price * (1 - stop_loss_ratio)
    _uref(uid).collection(f"positions_{market}").document(stock_code).set({
        "stock_code": stock_code, "stock_name": stock_name, "market": market,
        "buy_price": buy_price, "quantity": quantity,
        "stop_loss_price": stop_loss_price, "target_sell_price": target_sell_price,
        "source": source, "entry_time": datetime.now(KST),
        "partial_tp_done": False,
        "avg_down_count": 0,
        "highest_price": buy_price,
    })


def register_partial_sell(
    uid: str, market: str, stock_code: str, sell_price: float, sell_qty: int,
    tighten_stop_to_breakeven: bool = True,
    tighten_buffer_pct: float = 0.0,
) -> tuple[float, bool]:
    """일부 매도 후 잔여 수량·손절선 갱신.

    호출 시점:
      - 분할익절 (KR/US 사이클의 partial_tp 분기)
      - 수동 매도 시 quantity < 보유수량 인 경우 (`/api/order` 라우트에서 분기 처리)

    동작:
      1) sell_qty 를 보유수량에 상한 클램프.
      2) PnL = (sell_price - buy_price) × sell_qty 계산.
      3) 잔여 수량 = 0 이면 포지션 문서 삭제 → (pnl, True) 반환.
      4) 잔여 > 0 이면:
         - quantity 차감, partial_tp_done = True
         - tighten_stop_to_breakeven=True 면 손절선을
           max(기존 손절, buy_price*(1+tighten_buffer_pct)) 으로 상향.
           buffer_pct 는 수수료/세금/슬리피지 흡수용 ("본전+α" 락인).

    반환:
      (pnl, all_closed) — all_closed=True면 포지션이 완전히 청산됐다는 뜻.
    """
    ref = _uref(uid).collection(f"positions_{market}").document(stock_code)
    doc_snap = ref.get()
    if not doc_snap.exists:
        return 0.0, True
    pos = doc_snap.to_dict()
    bp = float(pos["buy_price"])
    q = int(pos["quantity"])
    sell_qty = max(0, min(sell_qty, q))
    if sell_qty <= 0:
        return 0.0, False
    pnl = (sell_price - bp) * sell_qty
    new_q = q - sell_qty
    if new_q <= 0:
        ref.delete()
        return pnl, True
    upd: dict = {"quantity": new_q, "partial_tp_done": True}
    if tighten_stop_to_breakeven:
        old_sl = float(pos.get("stop_loss_price") or 0)
        new_floor = bp * (1 + max(0.0, float(tighten_buffer_pct)))
        upd["stop_loss_price"] = max(old_sl, new_floor)
    ref.update(upd)
    return pnl, False


def register_sell(uid: str, market: str, stock_code: str, sell_price: float) -> float:
    """포지션 전량 청산: 문서를 삭제하고 PnL = (sell - buy) × qty 반환.

    주의:
      - 호출자는 반드시 별도로 add_trade 와 update_bot_state(realized_pnl 누적)도 처리해야 한다.
      - 외부 청산 reconcile 의 경우 pnl 을 0 으로 기록하기 위해 이 함수 대신
        직접 문서 삭제 + add_trade(pnl=0) 패턴을 사용한다 (`reconcile_positions` 참조).
    """
    pos_doc = _uref(uid).collection(f"positions_{market}").document(stock_code).get()
    pnl = 0.0
    if pos_doc.exists:
        pos = pos_doc.to_dict()
        pnl = (sell_price - pos["buy_price"]) * pos["quantity"]
        _uref(uid).collection(f"positions_{market}").document(stock_code).delete()
    return pnl


def add_trade(uid: str, market: str, stock_code: str, side: str,
              price: float, quantity: int, reason: str = "", pnl: float = 0.0,
              stock_name: str = ""):
    _uref(uid).collection("trades").add({
        "stock_code": stock_code, "stock_name": stock_name,
        "market": market, "side": side,
        "price": price, "quantity": quantity, "reason": reason,
        "pnl": pnl, "timestamp": datetime.now(KST),
    })
    s = (side or "").lower()
    if s in ("buy", "sell"):
        try:
            _notify_telegram_trade(uid, market, stock_code, side, price, quantity, reason, pnl, stock_name)
        except Exception as e:
            logger.warning("[Telegram] 매매 알림 생략: %s", e)


def _add_log(uid: str, level: str, message: str):
    _uref(uid).collection("logs").add({
        "level": level, "message": message, "timestamp": datetime.now(KST),
    })
    logger.info("[%s][%s] %s", uid[:8], level, message)


def _send_telegram(
    text: str,
    *,
    parse_mode: str | None = "HTML",
    log_if_unconfigured: bool = True,
) -> bool:
    """텔레그램 봇 전송. parse_mode=None 이면 일반 텍스트(종목명 특수문자 이슈 회피)."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        if log_if_unconfigured:
            logger.warning("[Telegram] TELEGRAM_BOT_TOKEN 또는 TELEGRAM_CHAT_ID 미설정")
        return False
    try:
        payload: dict = {"chat_id": chat_id, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = http_requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload,
            timeout=10,
        )
        return resp.status_code == 200
    except Exception as e:
        logger.error("[Telegram] 전송 실패: %s", e)
        return False


def _notify_telegram_trade(
    uid: str,
    market: str,
    stock_code: str,
    side: str,
    price: float,
    quantity: int,
    reason: str,
    pnl: float,
    stock_name: str,
) -> None:
    if not (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip() or not (os.environ.get("TELEGRAM_CHAT_ID") or "").strip():
        return
    label = "매수" if (side or "").lower() == "buy" else "매도"
    mkt = (market or "KR").upper()
    name = (stock_name or "").strip() or stock_code
    if mkt == "KR":
        pr = f"{float(price):,.0f}원"
    else:
        pr = f"${float(price):.2f}"
    lines = [
        f"📌 [AutoStock] {label} ({mkt})  uid={uid[:8]}…",
        f"종목: {stock_code}  {name}",
        f"수량: {quantity}  /  가격: {pr}",
        f"사유: {reason or '-'}",
    ]
    if (side or "").lower() == "sell":
        if mkt == "KR":
            lines.append(f"손익: {float(pnl):+,.0f}원")
        else:
            lines.append(f"손익: ${float(pnl):+.2f}")
    _send_telegram("\n".join(lines), parse_mode=None, log_if_unconfigured=False)


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


# ══════════════════════════════════════════════════════════════════════════
# 운영 안전망 #1 — 포지션 Reconcile
# --------------------------------------------------------------------------
# 목적:
#   봇 외부에서 발생한 매매(HTS 수동매매·취소·체결 누락 등)와 Firestore
#   포지션 상태 사이의 드리프트를 자동 감지/보정한다.
#
# 흐름:
#   1) `_get_kis_holdings_kr/us` 가 KIS 잔고 API에서 실제 보유 dict를 만든다.
#   2) `reconcile_positions` 가 Firestore 포지션과 dict를 비교해 케이스별로 처리.
#   3) `scheduled_reconcile_kr/_us` (30분 주기)가 이를 호출한다.
#
# 안전 원칙:
#   - 자동 삭제/수량 down-update 만 수행. 외부 매수로 보이는 케이스는
#     평단·손절 정보가 없어 자동 등록하지 않고 INFO 로그만 남긴다.
#   - Reconcile로 만들어지는 trades 레코드의 PnL은 0 — 실제 체결가가 미상이라
#     장부의 일관성을 깨지 않기 위함.
# ══════════════════════════════════════════════════════════════════════════

def _get_kis_holdings_kr(uid: str, cfg: dict) -> dict[str, int]:
    """국내 KIS 잔고 → {종목코드(6자리): 보유수량}. 실패 시 빈 dict."""
    return {k: v["qty"] for k, v in _get_kis_holdings_full_kr(uid, cfg).items()}


def _get_kis_holdings_full_kr(uid: str, cfg: dict) -> dict[str, dict]:
    """국내 KIS 잔고 → {종목코드: {qty, avg_price, stock_name}}. 실패 시 빈 dict."""
    try:
        bal = get_balance_kr(uid, cfg)
    except Exception as e:
        _add_log(uid, "WARNING", f"[reconcile][KR] 잔고 조회 실패: {e}")
        return {}
    holdings: dict[str, dict] = {}
    for row in bal.get("output1") or []:
        if not isinstance(row, dict):
            continue
        code = str(row.get("pdno", "")).strip()
        if not code:
            continue
        if code.isdigit() and len(code) <= 6:
            code = code.zfill(6)
        try:
            qty = int(str(row.get("hldg_qty", "0")).replace(",", "") or 0)
        except Exception:
            qty = 0
        if qty > 0:
            avg_price = float(str(row.get("pchs_avg_pric", "0")).replace(",", "") or 0)
            stock_name = str(row.get("prdt_name", "") or "").strip()
            holdings[code] = {"qty": qty, "avg_price": avg_price, "stock_name": stock_name}
    return holdings


def _get_kis_holdings_us(uid: str, cfg: dict) -> dict[str, int]:
    """미국 KIS 잔고 → {종목코드: 보유수량}. 실패 시 빈 dict.

    `get_balance_us` 의 `output1[*]` 배열에서 `ovrs_pdno` /
    `ovrs_cblc_qty` 사용. 종목코드는 대문자 정규화.
    """
    try:
        bal = get_balance_us(uid, cfg)
    except Exception as e:
        _add_log(uid, "WARNING", f"[reconcile][US] 잔고 조회 실패: {e}")
        return {}
    holdings: dict[str, int] = {}
    for row in bal.get("output1") or []:
        if not isinstance(row, dict):
            continue
        code = str(row.get("ovrs_pdno", "")).strip().upper()
        if not code:
            continue
        try:
            qty = int(str(row.get("ovrs_cblc_qty", "0")).replace(",", "") or 0)
        except Exception:
            qty = 0
        if qty > 0:
            holdings[code] = qty
    return holdings


def reconcile_positions(uid: str, cfg: dict, market: str) -> dict:
    """Firestore 포지션 ↔ KIS 실재 보유 비교 후 자동 보정.

    호출 경로:
      `scheduled_reconcile_kr/_us` (30분 주기) — cfg["reconcile_enabled"]=True 인 유저만.

    케이스별 처리:
      - Firestore(qty>0) ∧ KIS(0)
          → 외부 청산 추정.
            Firestore 포지션 삭제 + 거래기록(reason="외부청산_보정", pnl=0) + WARNING 로그.
      - Firestore(qty=N) ∧ KIS(qty=M) ∧ M<N
          → 외부 부분 매도 추정.
            quantity = M 으로 update + 거래기록(reason="외부부분매도_보정", pnl=0) + WARNING.
      - Firestore(qty=N) ∧ KIS(qty=M) ∧ M>N
          → 외부 추가 매수 추정.
            평단/손절 정보가 없으므로 Firestore 자동 갱신하지 않음 — INFO 로그만.
      - KIS만 보유 (Firestore 미등록)
          → 자동 등록하지 않음 — INFO 로그만.

    PnL을 0으로 기록하는 이유:
      외부 청산의 실제 체결가를 알 수 없어 임의의 손익을 잡으면 realized_pnl /
      drawdown 계산이 왜곡됨. 추후 reconcile 결과 분석 시 reason 키로 식별 가능.

    반환:
      {"deleted": [코드, ...],
       "down_updated": [(코드, 이전 qty, 보정 qty), ...],
       "external_extra": [(코드, FS qty, KIS qty), ...],
       "external_only": [(코드, KIS qty), ...]}
      — 호출 측은 보통 무시해도 되며, 디버깅·테스트에서 검증용으로 사용.
    """
    if market == "KR":
        kis_hold = _get_kis_holdings_kr(uid, cfg)
    elif market == "US":
        kis_hold = _get_kis_holdings_us(uid, cfg)
    else:
        return {}
    fs_pos = get_positions(uid, market)

    summary = {"deleted": [], "down_updated": [], "external_extra": [], "external_only": []}

    for code, pos in list(fs_pos.items()):
        try:
            fs_qty = int(pos.get("quantity", 0) or 0)
        except Exception:
            fs_qty = 0
        kis_qty = int(kis_hold.get(code, 0))
        if fs_qty <= 0:
            continue
        if kis_qty == 0:
            # 외부 청산
            _uref(uid).collection(f"positions_{market}").document(code).delete()
            add_trade(
                uid, market, code, "sell",
                float(pos.get("buy_price", 0) or 0), fs_qty,
                "외부청산_보정", 0.0, stock_name=pos.get("stock_name", ""),
            )
            _add_log(
                uid, "WARNING",
                f"[reconcile][{market}][{code}] Firestore {fs_qty}주↔KIS 0주 — 외부 청산 추정. 포지션 삭제",
            )
            summary["deleted"].append(code)
            _invalidate_balance_cache(uid)
        elif kis_qty < fs_qty:
            # 외부 부분 매도
            _uref(uid).collection(f"positions_{market}").document(code).update({
                "quantity": kis_qty,
            })
            add_trade(
                uid, market, code, "sell",
                float(pos.get("buy_price", 0) or 0), fs_qty - kis_qty,
                "외부부분매도_보정", 0.0, stock_name=pos.get("stock_name", ""),
            )
            _add_log(
                uid, "WARNING",
                f"[reconcile][{market}][{code}] Firestore {fs_qty}주↔KIS {kis_qty}주 — 외부 부분 매도 추정. 수량 보정",
            )
            summary["down_updated"].append((code, fs_qty, kis_qty))
            _invalidate_balance_cache(uid)
        elif kis_qty > fs_qty:
            _add_log(
                uid, "INFO",
                f"[reconcile][{market}][{code}] Firestore {fs_qty}주↔KIS {kis_qty}주 — "
                f"외부 추가 매수로 추정 (Firestore 자동 갱신 안 함)",
            )
            summary["external_extra"].append((code, fs_qty, kis_qty))

    # KIS에만 있는 종목 → 수동 매수로 Firestore에 자동 등록
    kis_full = _get_kis_holdings_full_kr(uid, cfg) if market == "KR" else {}
    for code, kis_qty in kis_hold.items():
        if code in fs_pos:
            continue
        detail = kis_full.get(code, {})
        avg_price = float(detail.get("avg_price", 0) or 0)
        sname = detail.get("stock_name", "") or _stock_name("", code, market)
        if avg_price > 0:
            # 수동 매수: Firestore에 등록 (target/stop 미설정 → 봇이 장마감 청산만 담당)
            _uref(uid).collection(f"positions_{market}").document(code).set({
                "stock_code": code,
                "stock_name": sname,
                "buy_price": avg_price,
                "quantity": kis_qty,
                "target_sell_price": 0,
                "stop_loss_price": 0,
                "source": "수동",
                "entry_time": datetime.now(KST),
                "partial_tp_done": False,
                "breakeven_applied": False,
            })
            add_trade(uid, market, code, "buy", avg_price, kis_qty, "수동매수_자동등록", 0.0, stock_name=sname)
            _add_log(uid, "INFO",
                f"[reconcile][{market}][{code}] 수동매수 감지 → Firestore 등록 | "
                f"{kis_qty}주@{avg_price:,.0f} ({sname})")
        else:
            _add_log(uid, "INFO",
                f"[reconcile][{market}][{code}] KIS {kis_qty}주 보유, 평단가 조회 불가 — 수동 확인 필요")
        summary["external_only"].append((code, kis_qty))

    return summary


# ══════════════════════════════════════════════════════════════════════════
# 운영 안전망 #2 — 체결가 사후 조회 (옵저버블리티)
# --------------------------------------------------------------------------
# 목적:
#   - 주문 직후의 시그널가(`current`)와 실제 평균 체결가(`avg_prvs`)의 차이를
#     로그로 남겨 슬리피지 분포를 가시화한다.
#   - 자동매매 중 KIS가 부분 체결하거나 다른 호가에 체결되는 케이스를 검증.
#
# 설계 원칙:
#   - 옵저버블리티 전용. register_sell PnL/수량 등에는 영향 없음
#     (이 단계에서 PnL을 재계산하면 로직 복잡도가 크게 증가하고 회귀 위험).
#   - 추후 `[fill]` 로그 분석으로 슬리피지 패턴이 일관되면 다음 단계에서
#     실제 체결가 기반 PnL 보정 / 주문 방식 튜닝의 근거 데이터로 사용.
#
# 사용 API:
#   - KR: /uapi/domestic-stock/v1/trading/inquire-daily-ccld (TR_ID TTTC8001R/VTTC8001R)
#   - US: /uapi/overseas-stock/v1/trading/inquire-ccnl       (TR_ID JTTT3001R)
# ══════════════════════════════════════════════════════════════════════════

def _today_kst_yyyymmdd() -> str:
    """주문체결조회 API의 INQR_*_DT 파라미터에 사용할 오늘 YYYYMMDD (KST)."""
    return datetime.now(KST).strftime("%Y%m%d")


def inquire_order_fill_kr(uid: str, cfg: dict, order_no: str) -> dict | None:
    """국내 주문체결조회 — order_no 와 일치하는 항목 반환 (없으면 None).

    KIS API: /uapi/domestic-stock/v1/trading/inquire-daily-ccld
       TR_ID: TTTC8001R(실서버) / VTTC8001R(모의)
    당일 체결 내역을 조회한다 (INQR_STRT_DT == INQR_END_DT == 오늘).

    파라미터 노트:
      - "ODNO": 서버측 검색이 보장되지 않는 환경이 있어, 응답 rows 를 클라이언트에서
        다시 일치시킨다 (lstrip("0") 후 문자열 비교).
      - 호출 실패 / 일치 row 없음 시 None 반환 (호출 측에서 안전하게 무시).

    반환 dict (정상 시):
      - "order_no"    : 주문번호 (입력 그대로)
      - "ord_qty"     : 주문 수량
      - "tot_ccld_qty": 총 체결 수량
      - "avg_prvs"    : 평균 체결가 (float, 원단위)
    """
    if not order_no or order_no in ("N/A", "0"):
        return None
    tr_id = _tr_id(cfg, "TTTC8001R", "VTTC8001R")
    today = _today_kst_yyyymmdd()

    def _call():
        _kis_pace()
        resp = http_requests.get(
            _base_url(cfg.get("is_mock", True))
            + "/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            headers=_headers(uid, cfg, tr_id),
            params={
                "CANO": _account_prefix(cfg["account_no"]),
                "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
                "INQR_STRT_DT": today, "INQR_END_DT": today,
                "SLL_BUY_DVSN_CD": "00", "INQR_DVSN": "01",
                "PDNO": "", "CCLD_DVSN": "01", "ORD_GNO_BRNO": "",
                "ODNO": str(order_no), "INQR_DVSN_3": "00", "INQR_DVSN_1": "",
                "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
            },
            timeout=10,
        )
        return _parse(resp, uid, cfg)

    try:
        data = _with_retry(_call, retries=2)
    except Exception:
        return None
    rows = data.get("output1") or data.get("output") or []
    if isinstance(rows, dict):
        rows = [rows]
    target = str(order_no).lstrip("0") or "0"
    for row in rows:
        if not isinstance(row, dict):
            continue
        rod = str(row.get("odno", "")).lstrip("0") or "0"
        if rod != target:
            continue
        try:
            tot_ccld = int(str(row.get("tot_ccld_qty", "0")).replace(",", "") or 0)
            ord_qty = int(str(row.get("ord_qty", "0")).replace(",", "") or 0)
            avg_raw = row.get("avg_prvs") or row.get("ccld_avg_pric") or "0"
            avg = float(str(avg_raw).replace(",", "") or 0)
        except Exception:
            continue
        return {
            "order_no": str(order_no),
            "ord_qty": ord_qty,
            "tot_ccld_qty": tot_ccld,
            "avg_prvs": avg,
        }
    return None


def inquire_order_fill_us(uid: str, cfg: dict, order_no: str) -> dict | None:
    """미국 주문체결조회 — order_no 와 일치하는 항목 반환 (없으면 None).

    KIS API: /uapi/overseas-stock/v1/trading/inquire-ccnl
       TR_ID: JTTT3001R (US는 모의서버 미지원이라 실서버 고정)
    평균 체결가 필드 우선순위: ft_ccld_unpr3 → avg_prvs → ovrs_ord_unpr.

    반환 dict 키:
      - "order_no", "ord_qty", "tot_ccld_qty", "avg_prvs" (USD 단위)
    """
    if not order_no or order_no in ("N/A", "0"):
        return None
    today = _today_kst_yyyymmdd()
    tr_id = "JTTT3001R"

    def _call():
        _kis_pace()
        resp = http_requests.get(
            _base_url(False)
            + "/uapi/overseas-stock/v1/trading/inquire-ccnl",
            headers=_headers_us(uid, cfg, tr_id),
            params={
                "CANO": _account_prefix(cfg["account_no"]),
                "ACNT_PRDT_CD": _account_suffix(cfg["account_no"]),
                "PDNO": "%", "ORD_STRT_DT": today, "ORD_END_DT": today,
                "SLL_BUY_DVSN": "00", "CCLD_NCCS_DVSN": "00",
                "OVRS_EXCG_CD": "%", "SORT_SQN": "DS", "ORD_DT": "",
                "ORD_GNO_BRNO": "", "ODNO": str(order_no),
                "CTX_AREA_NK200": "", "CTX_AREA_FK200": "",
            },
            timeout=10,
        )
        return _parse(resp, uid, cfg)

    try:
        data = _with_retry(_call, retries=2)
    except Exception:
        return None
    rows = data.get("output") or []
    if isinstance(rows, dict):
        rows = [rows]
    target = str(order_no).lstrip("0") or "0"
    for row in rows:
        if not isinstance(row, dict):
            continue
        rod = str(row.get("odno", "")).lstrip("0") or "0"
        if rod != target:
            continue
        try:
            tot_ccld = int(str(row.get("tot_ccld_qty", "0")).replace(",", "") or 0)
            ord_qty = int(str(row.get("ft_ord_qty") or row.get("ord_qty", "0")).replace(",", "") or 0)
            avg_raw = (
                row.get("ft_ccld_unpr3")
                or row.get("avg_prvs")
                or row.get("ovrs_ord_unpr")
                or "0"
            )
            avg = float(str(avg_raw).replace(",", "") or 0)
        except Exception:
            continue
        return {
            "order_no": str(order_no),
            "ord_qty": ord_qty,
            "tot_ccld_qty": tot_ccld,
            "avg_prvs": avg,
        }
    return None


def _log_sell_fill(
    uid: str, market: str, code: str, side: str, order_no: str,
    signal_price: float, qty: int, cfg: dict,
) -> dict | None:
    """매도(또는 매수) 후 실제 체결가를 조회하고 슬리피지를 로그로 남긴다.

    호출 시점:
      각 매도 경로(분할익절·트레일링·목표가·손절·시간청산·장마감)에서
      `add_trade(...)` + `_add_log(... PnL ...)` 직후. side 인자는 현재 "sell"이지만
      향후 매수 슬리피지 추적으로 확장하기 쉽도록 노출.

    슬리피지 정의:
      slip_pct = (avg_prvs - signal_price) / signal_price × 100
      - sell: avg ≥ signal 이면 "유리" (더 비싸게 팔림)
      - buy : avg ≤ signal 이면 "유리" (더 싸게 사짐)

    옵트아웃:
      cfg["fill_check_enabled"] 가 False 면 즉시 None 반환 (API 호출도 생략).

    반환:
      체결 정보 dict (디버그용) 또는 None. 호출 측은 일반적으로 무시.
    """
    if not cfg.get("fill_check_enabled", True):
        return None
    if not order_no or order_no == "N/A":
        return None
    try:
        if market == "KR":
            info = inquire_order_fill_kr(uid, cfg, order_no)
        else:
            info = inquire_order_fill_us(uid, cfg, order_no)
    except Exception as e:
        _add_log(uid, "DEBUG", f"[fill][{market}][{code}] 체결조회 실패: {e}")
        return None
    if not info:
        return None
    avg = float(info.get("avg_prvs") or 0)
    tot = int(info.get("tot_ccld_qty") or 0)
    if avg <= 0 or tot <= 0:
        return info
    # 슬리피지: (체결가 - 시그널가) / 시그널가
    slip_pct = ((avg - signal_price) / signal_price * 100) if signal_price > 0 else 0
    sign_label = "유리" if (
        (side == "sell" and avg >= signal_price)
        or (side == "buy" and avg <= signal_price)
    ) else "불리"
    if market == "KR":
        _add_log(
            uid, "INFO",
            f"[fill][KR][{code}] {side} 체결 {tot}/{info.get('ord_qty', '?')}주 "
            f"평균체결={avg:,.0f}원 vs 시그널={signal_price:,.0f}원 "
            f"슬리피지 {slip_pct:+.2f}%({sign_label}) 주문={order_no}",
        )
    else:
        _add_log(
            uid, "INFO",
            f"[fill][US][{code}] {side} 체결 {tot}/{info.get('ord_qty', '?')}주 "
            f"평균체결=${avg:.2f} vs 시그널=${signal_price:.2f} "
            f"슬리피지 {slip_pct:+.2f}%({sign_label}) 주문={order_no}",
        )
    return info


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


def _kr_buy_window_ok(cfg: dict) -> tuple[bool, str]:
    """신규 매수 허용 시간대인지 판정 (KR).

    설계 의도:
      - 개장 직후 N분(기본 5분): 갭/슬리피지가 크고 호가가 불안정 → 신규 진입 회피
      - 마감 전 N분(기본 30분): 유동성 저하 + 마감 청산 영향 → 신규 진입 회피
      - (옵션) 월요일 오전 N분: 주말 갭/리스크 이벤트 다음날 변동성 회피
    매도/손절/트레일링 등 포지션 관리 로직에는 영향 없음 (신규 매수만 차단).

    cfg 키:
      - kr_skip_buy_first_min       : 기본 5
      - kr_skip_buy_last_min        : 기본 30
      - monday_morning_skip_enabled : 기본 False (옵션)
      - monday_morning_skip_min     : 기본 60 (분)
    """
    now = datetime.now(KST)
    open_t = now.replace(hour=9, minute=0, second=0, microsecond=0)
    close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)
    skip_first = int(cfg.get("kr_skip_buy_first_min", 5))
    skip_last = int(cfg.get("kr_skip_buy_last_min", 30))
    if now < open_t + timedelta(minutes=skip_first):
        return False, f"개장후{skip_first}분"
    if now > close_t - timedelta(minutes=skip_last):
        return False, f"마감전{skip_last}분"
    if cfg.get("monday_morning_skip_enabled", False) and now.weekday() == 0:
        mon_skip = int(cfg.get("monday_morning_skip_min", 60))
        if now < open_t + timedelta(minutes=mon_skip):
            return False, f"월요일오전{mon_skip}분"
    return True, ""


def _us_buy_window_ok(cfg: dict) -> tuple[bool, str]:
    """신규 매수 허용 시간대인지 판정 (US, ET 기준).

    cfg 키:
      - us_skip_buy_first_min       : 기본 10  (개장 09:30 ET 직후 차단 분)
      - us_skip_buy_last_min        : 기본 20  (마감 16:00 ET 직전 차단 분)
      - monday_morning_skip_enabled : 기본 False
      - monday_morning_skip_min     : 기본 60
    """
    now = datetime.now(ET)
    open_t = now.replace(hour=9, minute=30, second=0, microsecond=0)
    close_t = now.replace(hour=16, minute=0, second=0, microsecond=0)
    skip_first = int(cfg.get("us_skip_buy_first_min", 10))
    skip_last = int(cfg.get("us_skip_buy_last_min", 20))
    if now < open_t + timedelta(minutes=skip_first):
        return False, f"개장후{skip_first}분"
    if now > close_t - timedelta(minutes=skip_last):
        return False, f"마감전{skip_last}분"
    if cfg.get("monday_morning_skip_enabled", False) and now.weekday() == 0:
        mon_skip = int(cfg.get("monday_morning_skip_min", 60))
        if now < open_t + timedelta(minutes=mon_skip):
            return False, f"월요일오전{mon_skip}분"
    return True, ""


def _daily_pnl_buy_gate(state: dict, cfg: dict) -> tuple[bool, str]:
    """일간 실현손익 기반 신규매수 게이트 (포트폴리오 레벨 서킷 브레이커).

    설계 의도:
      - **수익 잠금**: 당일 일정 수익 도달 후 추가 진입을 막아 이익을 보호한다
        (=과도한 회전매매로 수익을 토해내는 패턴 방지).
      - **손실 한도**: 당일 누적 손실이 한계를 넘으면 신규 진입을 멈춰
        "오늘은 더 들어가지 않는다"는 규율을 강제한다.
      - 단, 포지션 관리(트레일링/손절/시간청산 등)는 그대로 동작 →
        이미 잡힌 리스크는 끝까지 관리.

    기준:
      - 분자: state["realized_pnl"]  (오늘 자정 또는 시작 시점 reset)
      - 분모: state["start_equity"]  (장 시작 직전의 총자산 스냅샷)

    cfg 키:
      - daily_profit_target : 기본 0.03 (3%)  — 수익률 ≥ 이면 신규매수 중단
      - daily_loss_limit   : 기본 0.02 (2%)  — 수익률 ≤ -이면 신규매수 중단
        (둘 다 0 또는 음수로 두면 해당 게이트 비활성)

    반환:
      (ok: bool, reason: str)  — ok=False면 reason은 로그용 사유 문자열.
    """
    start_eq = float(state.get("start_equity", 0) or 0)
    if start_eq <= 0:
        return True, ""
    pnl = float(state.get("realized_pnl", 0) or 0)
    ratio = pnl / start_eq
    tgt = float(cfg.get("daily_profit_target", 0.03))
    if tgt > 0 and ratio >= tgt:
        return False, f"수익목표달성({ratio * 100:+.2f}%≥+{tgt * 100:.1f}%)"
    loss_lim = abs(float(cfg.get("daily_loss_limit", 0.02)))
    if loss_lim > 0 and ratio <= -loss_lim:
        return False, f"손실한도({ratio * 100:+.2f}%≤-{loss_lim * 100:.1f}%)"
    return True, ""


# KOSPI 지수 등락률 간단 캐시 (같은 사이클 내 여러 번 호출 방지)
_KOSPI_CHG_CACHE: dict[str, tuple[float, float]] = {}
_KOSPI_CHG_TTL_SEC = 60


def _get_kr_index_change_pct(uid: str, cfg: dict, index_code: str = "0001") -> float:
    """국내 지수 당일 등락률(%) — KIS `inquire-index-price` 호출.

    파라미터:
      - index_code: "0001"=코스피, "1001"=코스닥
    응답 필드: output.bstp_nmix_prdy_ctrt (전일 대비 등락률 %), 부재 시 prdy_ctrt.
    실패 시 0.0 반환 → 호출 측 게이트가 자동 무효화 (안전: API 장애로 매수 막히지 않게).
    `_KOSPI_CHG_CACHE` 메모리 캐시(60초)로 동일 사이클 내 반복 호출 억제.
    """
    cache_key = f"{uid}:{index_code}"
    now_ts = time_module.time()
    hit = _KOSPI_CHG_CACHE.get(cache_key)
    if hit and now_ts - hit[1] < _KOSPI_CHG_TTL_SEC:
        return hit[0]

    def _call():
        _kis_pace()
        resp = http_requests.get(
            _base_url(cfg.get("is_mock", True))
            + "/uapi/domestic-stock/v1/quotations/inquire-index-price",
            headers=_headers(uid, cfg, "FHPUP02100000"),
            params={"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": index_code},
            timeout=10,
        )
        return _parse(resp, uid, cfg)

    try:
        data = _with_retry(_call, retries=2)
        out = data.get("output") or {}
        raw = out.get("bstp_nmix_prdy_ctrt") or out.get("prdy_ctrt") or "0"
        chg = float(str(raw).replace(",", "") or 0)
    except Exception:
        chg = 0.0
    _KOSPI_CHG_CACHE[cache_key] = (chg, now_ts)
    return chg


def _kr_index_buy_gate(uid: str, cfg: dict) -> tuple[bool, str]:
    """코스피 급락 시 KR 신규매수 차단 (시장 레짐 필터).

    설계 의도:
      개별 종목이 강해 보여도 지수가 급락 중이면 베타 손실 확률이 높음.
      "지수가 -1.5% 떨어진 날엔 새로 들어가지 말자"는 규율을 자동으로 강제.
    cfg 키:
      - kr_index_drop_limit_pct : 기본 1.5 (절대값 %로 해석). 0 이하 시 비활성.
    매도/포지션 관리 로직은 영향 없음 (신규 매수만 차단).
    """
    limit = abs(float(cfg.get("kr_index_drop_limit_pct", 1.5)))
    if limit <= 0:
        return True, ""
    chg = _get_kr_index_change_pct(uid, cfg, "0001")
    if chg <= -limit:
        return False, f"KOSPI급락({chg:+.2f}%≤-{limit:.1f}%)"
    return True, ""


# US 지수(SPY) 당일 등락률 캐시
_US_INDEX_CACHE: dict[str, tuple[float, float]] = {}
_US_INDEX_TTL_SEC = 60


def _get_us_index_change_pct(uid: str, cfg: dict, symbol: str = "SPY") -> float:
    """미국 지수 프록시(기본 SPY) 당일 등락률(%) — `get_current_price_us` 재사용.

    국내 지수 API와 달리 KIS US 지수 TR이 별도라, ETF 시세를 프록시로 사용.
    우선순위:
      1) output.rate (KIS가 제공하는 등락률 %)
      2) (last - base) / base * 100  — base는 전일 종가 추정
    실패 시 0.0 반환 → 호출 측 게이트 자동 무효화. 60초 메모리 캐시.
    cfg["us_index_proxy"]로 SPY 외 다른 ETF (예: QQQ) 사용 가능.
    """
    cache_key = f"{uid}:{symbol}"
    now_ts = time_module.time()
    hit = _US_INDEX_CACHE.get(cache_key)
    if hit and now_ts - hit[1] < _US_INDEX_TTL_SEC:
        return hit[0]
    try:
        data = get_current_price_us(uid, cfg, symbol)
        out = data.get("output") or {}
        # 우선순위: rate(%), 그다음 (last-base)/base
        raw = out.get("rate")
        if raw is None:
            last = float(out.get("last", 0) or 0)
            base = float(out.get("base", 0) or 0)
            chg = ((last - base) / base * 100) if base > 0 else 0.0
        else:
            chg = float(str(raw).replace(",", "") or 0)
    except Exception:
        chg = 0.0
    _US_INDEX_CACHE[cache_key] = (chg, now_ts)
    return chg


def _us_index_buy_gate(uid: str, cfg: dict) -> tuple[bool, str]:
    """SPY(또는 cfg["us_index_proxy"]) 급락 시 US 신규매수 차단.

    cfg 키:
      - us_index_drop_limit_pct : 기본 1.5 (절대값 %). 0 이하 시 비활성.
      - us_index_proxy           : 기본 "SPY". QQQ 등 다른 ETF 가능.
    매도/포지션 관리는 영향 없음.
    """
    limit = abs(float(cfg.get("us_index_drop_limit_pct", 1.5)))
    if limit <= 0:
        return True, ""
    chg = _get_us_index_change_pct(uid, cfg, cfg.get("us_index_proxy", "SPY"))
    if chg <= -limit:
        return False, f"SPY급락({chg:+.2f}%≤-{limit:.1f}%)"
    return True, ""


def _risk_based_qty(
    equity: float,
    available: float,
    current_price: float,
    stop_loss_price: float,
    cfg: dict,
) -> tuple[int, str]:
    """ATR 기반 리스크-패리티 포지션 사이징.

    설계 의도:
      "한 종목 1회 진입에서 잃어도 되는 금액"을 자기자본의 고정 비율로 정규화한다.
      변동성이 큰 종목(=1R 폭이 넓음)은 자동으로 수량이 줄고,
      손절폭이 좁은 종목은 수량이 커져 종목별 리스크가 균등화된다.

    수식:
        1R       = 매수가 - 손절가                          (한 주당 위험 = ATR 기반)
        qty_risk = (equity × risk_per_trade_pct) / 1R       (위험 정규화 결과)
        qty_cap  = (equity × max_position_ratio) / 매수가   (단일 포지션 비중 캡)
        qty_cash = available / 매수가                       (실제 가용 잔고 한도)
        qty      = min(qty_risk, qty_cap, qty_cash) (모두 floor)

    cfg 키:
      - risk_per_trade_pct : 기본 0.01 (1%). 0 이하면 위험 정규화 OFF.
      - max_position_ratio : 기본 0.10 (10%). 단일 종목 비중 상한.

    폴백 (위험 정규화 비활성/손절 정보 부실 시):
      qty = min(qty_cap, qty_cash) — 기존 max_position_ratio 방식과 동일.

    반환:
      (qty, reason) — reason은 로그용 사이징 근거 문자열 (예: "risk=1.00%·1R=2,350·qtyR=12/cap=27/cash=120").
      호출 측에서 0주가 나오면 cfg/잔고에 따라 1주 안전 보정 처리한다.
    """
    if current_price <= 0:
        return 0, "현재가0"
    max_ratio = float(cfg.get("max_position_ratio", 0.10))
    cap_by_ratio = math.floor((equity * max_ratio) / current_price) if equity > 0 else 0
    cap_by_cash = math.floor(available / current_price) if available > 0 else 0

    risk_pct = float(cfg.get("risk_per_trade_pct", 0.01))
    use_risk = (
        risk_pct > 0
        and equity > 0
        and stop_loss_price > 0
        and stop_loss_price < current_price
    )
    if not use_risk:
        qty = max(0, min(cap_by_ratio, cap_by_cash))
        reason = f"폴백(ratio≤{max_ratio:.0%})"
        return qty, reason

    risk_amount = equity * risk_pct
    stop_dist = current_price - stop_loss_price
    qty_risk = math.floor(risk_amount / stop_dist) if stop_dist > 0 else 0

    qty = max(0, min(qty_risk, cap_by_ratio, cap_by_cash))
    reason = (
        f"risk={risk_pct * 100:.2f}%·1R={stop_dist:,.2f}·"
        f"qtyR={qty_risk}/cap={cap_by_ratio}/cash={cap_by_cash}"
    )
    return qty, reason


def _check_drawdown(uid: str, cfg: dict) -> bool:
    """포트폴리오 드로우다운 체크 — 임계치 초과 시 매매 자동 중단.

    peak_equity 대비 현재 자산이 max_drawdown_pct(기본 5%) 이상 하락하면
    trading_halted=True로 설정하고 True를 반환합니다.
    start_equity가 0이면 체크를 건너뜁니다 (장 시작 전 초기화 미완료).

    Returns
    -------
    bool
        True → 드로우다운 한도 초과, 이 사이클은 매매 건너뜀
        False → 정상
    """
    max_dd = float(cfg.get("max_drawdown_pct", 0.05))
    try:
        state        = get_bot_state(uid)
        start_equity = float(state.get("start_equity", 0))
        if start_equity <= 0:
            return False

        current_equity = _get_total_equity_kr(uid, cfg)
        if current_equity <= 0:
            return False

        # peak equity 갱신
        peak = float(state.get("peak_equity", start_equity))
        if current_equity > peak:
            peak = current_equity
            update_bot_state(uid, {"peak_equity": peak})

        dd_pct = (peak - current_equity) / peak
        if dd_pct >= max_dd:
            update_bot_state(uid, {
                "trading_halted": True,
                "halt_reason": f"드로우다운 {dd_pct:.1%} ≥ 한도 {max_dd:.1%}",
            })
            _add_log(uid, "WARNING",
                     f"[리스크] 드로우다운 {dd_pct:.1%} ≥ 한도 {max_dd:.1%} "
                     f"| 고점={peak:,.0f} 현재={current_equity:,.0f} — 매매 자동 중단")
            return True
    except Exception as e:
        _add_log(uid, "ERROR", f"[리스크] 드로우다운 체크 오류: {e}")
    return False


def _get_sector_exposure(uid: str, market: str) -> dict[str, int]:
    """현재 보유 포지션의 섹터별 종목 수 반환.

    Returns
    -------
    dict[str, int]
        {"반도체": 2, "바이오": 1, ...}
    """
    positions  = get_positions(uid, market)
    sector_map = KR_SECTOR_MAP if market == "KR" else US_SECTOR_MAP
    exposure: dict[str, int] = {}
    for code in positions:
        sector = sector_map.get(code, "기타")
        exposure[sector] = exposure.get(sector, 0) + 1
    return exposure


@transactional
def _buy_lock_txn(transaction, lock_ref, market: str, code: str) -> bool:
    """트랜잭션 내에서 매수 락 획득 시도 (내부용)."""
    snap = lock_ref.get(transaction=transaction)
    if snap.exists:
        data = snap.to_dict() or {}
        locked_at = data.get("locked_at")
        if locked_at is not None:
            age = _firestore_dt_age_seconds(locked_at)
            if age < 180:
                return False
    transaction.set(lock_ref, {"locked_at": datetime.now(KST), "market": market, "code": code})
    return True


def _try_acquire_buy_lock(uid: str, market: str, code: str) -> bool:
    """Firestore 트랜잭션으로 매수 락을 획득합니다.

    두 개의 Functions 인스턴스가 동시에 같은 종목을 매수하는 것을 방지합니다.
    락은 3분 후 자동 만료됩니다.

    Returns
    -------
    bool
        True  → 락 획득 성공, 매수 진행 가능
        False → 이미 다른 인스턴스가 매수 중 (또는 트랜잭션 실패)
    """
    lock_ref = _uref(uid).collection("locks").document(f"{market}_{code}")
    try:
        txn = get_db().transaction()
        return _buy_lock_txn(txn, lock_ref, market, code)
    except Exception as e:
        _add_log(uid, "WARNING", f"[락] {market}/{code} 락 획득 실패: {e}")
        return False


def _release_buy_lock(uid: str, market: str, code: str) -> None:
    """매수 완료 또는 실패 후 락을 해제합니다."""
    try:
        _uref(uid).collection("locks").document(f"{market}_{code}").delete()
    except Exception:
        pass  # 락 해제 실패는 무시 (3분 후 자동 만료)


def _firestore_dt_age_seconds(ts) -> float:
    """Firestore에 저장된 시각의 나이(초). 파싱 실패 시 오래된 것으로 간주."""
    if ts is None:
        return float("inf")
    try:
        if hasattr(ts, "timestamp"):
            return max(0.0, datetime.now(KST).timestamp() - float(ts.timestamp()))
        if isinstance(ts, datetime):
            t = ts
            if t.tzinfo is None:
                t = t.replace(tzinfo=KST)
            return (datetime.now(KST) - t.astimezone(KST)).total_seconds()
        if isinstance(ts, str):
            raw = ts.replace("Z", "+00:00")
            t = datetime.fromisoformat(raw)
            if t.tzinfo is None:
                t = t.replace(tzinfo=KST)
            return (datetime.now(KST) - t.astimezone(KST)).total_seconds()
    except Exception:
        pass
    return float("inf")


def _position_age_days(entry_time) -> int:
    """포지션 보유 일수. 파싱 실패/미기록 시 0."""
    age_sec = _firestore_dt_age_seconds(entry_time)
    if age_sec == float("inf"):
        return 0
    return int(age_sec // 86400)


AI_SESSION_LOCK_SEC = 600  # 동시에 두 AI 세션(스케줄+즉시) 방지


@transactional
def _ai_session_lock_txn(transaction, lock_ref, market: str, session: str) -> bool:
    """트랜잭션 내에서 AI 세션 락 획득 시도 (내부용)."""
    snap = lock_ref.get(transaction=transaction)
    if snap.exists:
        data = snap.to_dict() or {}
        started = data.get("started_at")
        if _firestore_dt_age_seconds(started) < AI_SESSION_LOCK_SEC:
            return False
    transaction.set(
        lock_ref,
        {"started_at": datetime.now(KST), "market": market, "session": session},
    )
    return True


def _try_acquire_ai_session_lock(uid: str, market: str, session: str) -> bool:
    """동일 uid·시장에서 run_ai_session 중복 실행 방지 (스케줄 AI와 즉시 버튼 충돌)."""
    lock_ref = _uref(uid).collection("locks").document(f"ai_session_{market}")
    try:
        txn = get_db().transaction()
        return _ai_session_lock_txn(txn, lock_ref, market, session)
    except Exception as e:
        _add_log(uid, "WARNING", f"[AI][세션락] 획득 실패: {e}")
        return False


def _release_ai_session_lock(uid: str, market: str) -> None:
    try:
        _uref(uid).collection("locks").document(f"ai_session_{market}").delete()
    except Exception:
        pass


def _sector_ok(code: str, market: str, exposure: dict[str, int], max_per_sector: int) -> tuple[bool, str]:
    """섹터 한도 초과 여부 판정.

    Returns
    -------
    (ok: bool, sector: str)
    """
    sector_map = KR_SECTOR_MAP if market == "KR" else US_SECTOR_MAP
    sector     = sector_map.get(code, "기타")
    count      = exposure.get(sector, 0)
    return count < max_per_sector, sector


def _calc_rsi(closes: list[float], period: int = 14) -> float:
    """Wilder EMA 방식 RSI.

    Parameters
    ----------
    closes : list[float]
        종가 배열 — [최신→과거] 순서. 정확한 결과를 위해 30개 이상 권장.
    period : int
        RSI 기간 (기본 14).

    Notes
    -----
    Wilder 스무딩: avg = (prev_avg × (n-1) + current) / n
    단순 평균 방식보다 최근 데이터에 더 적절한 가중치를 부여합니다.
    """
    if len(closes) < period + 2:
        return 50.0
    # [최신→과거] → [과거→최신] 변환
    c = list(reversed(closes))
    diffs  = [c[i] - c[i - 1] for i in range(1, len(c))]
    gains  = [max(d, 0.0) for d in diffs]
    losses = [abs(min(d, 0.0)) for d in diffs]

    # 초기 단순 평균 (Wilder 초기값)
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # Wilder EMA 스무딩 (나머지 기간 적용)
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

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
        rsi = _calc_rsi(closes[:30])   # Wilder RSI: 30개 이상 권장
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
    sl_ratio = cfg.get("stop_loss_ratio", 0.03)
    if atr > 0:
        # 목표: ATR 3.5배 또는 최소 10% 중 큰 값 (미국은 변동성 더 큼)
        sell_by_atr = buy_price + atr * 3.5
        sell_by_min = buy_price * 1.10
        sell_price  = max(sell_by_atr, sell_by_min)
        stop_by_atr   = buy_price - atr * 1.0
        stop_by_ratio = buy_price * (1 - sl_ratio)
        stop_loss     = max(stop_by_atr, stop_by_ratio)
    else:
        sell_price = buy_price * 1.10
        stop_loss  = buy_price * (1 - sl_ratio)

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

        rsi = _calc_rsi(closes[:30])   # Wilder RSI: 30개 이상 권장
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
    sl_ratio = cfg.get("stop_loss_ratio", 0.03)
    if atr > 0:
        # 목표: ATR 3배 또는 최소 8% 중 큰 값
        sell_by_atr = buy_price + atr * 3.0
        sell_by_min = buy_price * 1.08
        sell_price = max(sell_by_atr, sell_by_min)
        stop_by_atr = buy_price - atr * 1.0
        stop_by_ratio = buy_price * (1 - sl_ratio)
        stop_loss = max(stop_by_atr, stop_by_ratio)
    else:
        sell_price = buy_price * 1.08
        stop_loss = buy_price * (1 - sl_ratio)
    profit_ratio = (sell_price - buy_price) / buy_price * 100
    risk_ratio = (buy_price - stop_loss) / buy_price * 100
    rr_ratio = profit_ratio / risk_ratio if risk_ratio > 0 else 0
    return {
        "buy_price": round(buy_price, 2), "sell_price": round(sell_price, 2),
        "stop_loss": round(stop_loss, 2), "atr": round(atr, 2),
        "profit_ratio": round(profit_ratio, 2), "risk_ratio": round(risk_ratio, 2),
        "rr_ratio": round(rr_ratio, 2),
    }


def merge_position_after_avg_down(
    uid: str, market: str, stock_code: str, add_price: float, add_qty: int,
    ohlcv: list, cfg: dict,
) -> None:
    """물타기 체결 후 포지션 필드 재계산 + 트래킹 상태 reset.

    재계산 항목:
      - buy_price       : 가중평균 ((old_bp×old_q + add_price×add_qty) / 신수량)
      - quantity        : old_q + add_qty
      - target_sell_price : 새 평단 기준 ATR로 다시 산출
      - stop_loss_price : min(기존 손절, ATR 기반 새 손절) — 보수적.
                           평단이 낮아졌다고 손절선을 위로 올리면 도리어 작은
                           반등에 손절될 수 있어 더 낮은 쪽 유지.
      - avg_down_count  : +1
      - avg_down_last_at: 현재 시각 (ISO8601, KST) — 다음 물타기 간격 체크용
      - partial_tp_done : False reset → 새 평단 기준 분할익절 다시 가능
      - highest_price   : new_avg reset → 트레일링 스탑 기준 재시작
                           (조기 트레일링 발동으로 추가 손실 회피)
    """
    ref = _uref(uid).collection(f"positions_{market}").document(stock_code)
    doc_snap = ref.get()
    if not doc_snap.exists:
        return
    pos = doc_snap.to_dict()
    old_bp = float(pos["buy_price"])
    old_q = int(pos["quantity"])
    old_sl = float(pos.get("stop_loss_price") or 0)
    new_avg = (old_bp * old_q + add_price * add_qty) / (old_q + add_qty)
    new_q = old_q + add_qty
    if market == "KR":
        prices = calculate_optimal_prices(new_avg, ohlcv, cfg)
        tp, new_sl = float(prices["sell_price"]), float(prices["stop_loss"])
    else:
        prices = calculate_optimal_prices_us(new_avg, ohlcv, cfg)
        tp, new_sl = float(prices["sell_price"]), float(prices["stop_loss"])
    # 손절선은 기존과 새 ATR 기반 중 더 낮은 값 사용 (보수적)
    sl = min(old_sl, new_sl) if old_sl > 0 else new_sl
    cnt = int(pos.get("avg_down_count", 0)) + 1
    ref.update({
        "buy_price": new_avg,
        "quantity": new_q,
        "target_sell_price": tp,
        "stop_loss_price": sl,
        "avg_down_count": cnt,
        "avg_down_last_at": datetime.now(KST).isoformat(),
        # 물타기 후 평단이 바뀌므로 분할익절 기준도 새 평단 기준으로 재활성화
        "partial_tp_done": False,
        # 트레일링 스탑 기준 전고점도 새 평단 기준으로 리셋 (조기 이탈 방지)
        "highest_price": new_avg,
    })


def run_strategy_cycle_kr(uid: str, cfg: dict):
    state = get_bot_state(uid)
    now_min = datetime.now(KST).minute
    if not state.get("bot_enabled", True):
        if now_min % 10 == 0:
            _add_log(uid, "INFO", "[KR] 봇 비활성 상태 — 전략 사이클 건너뜀 (UI에서 시작 필요)")
        return
    if not state.get("is_market_open", False) and not _is_kr_market_open():
        return
    if state.get("trading_halted", False):
        if now_min % 10 == 0:
            reason = state.get("halt_reason", "")
            _add_log(uid, "WARNING", f"[KR] 매매 중단 상태 — halt_reason={reason or '없음'} (resume 필요)")
        return
    # 드로우다운 체크는 밸런스 API 호출 비용이 커 5분마다로 제한 (이미 halt면 위에서 return)
    if now_min % 5 == 0 and _check_drawdown(uid, cfg):
        return

    verbose = (now_min % 5 == 0)

    positions = get_positions(uid, "KR")
    for code, pos in list(positions.items()):
        try:
            data = get_current_price_kr(uid, cfg, code)
            ohlcv_pos = get_daily_ohlcv_kr(uid, cfg, code)
            current = _kr_price_from_api_data(data, ohlcv_pos)
            if current <= 0:
                _add_log(uid, "WARNING", f"[KR][{code}] 현재가 0 — 목표/손절 건너뜀")
                continue
            buy_avg = float(pos["buy_price"])
            qty = int(pos["quantity"])
            target = float(pos.get("target_sell_price") or 0)
            slp = float(pos.get("stop_loss_price") or 0)
            sn = pos.get("stock_name", "")
            pnl_pct = (current - buy_avg) / buy_avg * 100 if buy_avg > 0 else 0

            # 레거시 포지션 백필: highest_price / entry_time 초기값 보정
            backfill: dict = {}
            prev_high = float(pos.get("highest_price") or 0)
            if prev_high <= 0:
                prev_high = max(buy_avg, current)
                backfill["highest_price"] = prev_high
            new_high = max(prev_high, current)
            if new_high > prev_high:
                backfill["highest_price"] = new_high
            if not pos.get("entry_time"):
                backfill["entry_time"] = datetime.now(KST)
            if backfill:
                try:
                    _uref(uid).collection("positions_KR").document(code).update(backfill)
                except Exception:
                    pass

            # 손절선 본전 이동 (Break-even stop): +breakeven_trigger_pct 도달 시 slp를 매수가로 상향, 1회 적용
            if cfg.get("breakeven_stop_enabled", True) and not pos.get("breakeven_applied"):
                be_pct = float(cfg.get("breakeven_trigger_pct", 0.02))
                if current >= buy_avg * (1 + be_pct):
                    new_slp = max(slp, buy_avg)
                    if new_slp > slp:
                        try:
                            _uref(uid).collection("positions_KR").document(code).update({
                                "stop_loss_price": new_slp,
                                "breakeven_applied": True,
                            })
                            _add_log(uid, "INFO",
                                     f"[KR][{code}] 손절 본전이동 | {slp:,.0f}→{new_slp:,.0f} "
                                     f"(수익 +{pnl_pct:.2f}%)")
                            slp = new_slp

                        except Exception:
                            pass

            # 10분마다 포지션 현황 로그 (자동매도 동작 확인용)
            if verbose:
                _add_log(uid, "INFO",
                         f"[KR][{code}] 포지션체크 | 현재={current:,} 평단={buy_avg:,} "
                         f"수익={pnl_pct:+.2f}% | 목표={target:,} 손절={slp:,} "
                         f"고점={new_high:,}")

            # 분할 익절 (포지션당 1회): 평단 대비 +N% 도달 시 일부 매도 후 손절선을 본전 부근으로 상향
            if cfg.get("partial_tp_enabled", True) and not pos.get("partial_tp_done") and qty > 1:
                trig_pct = float(cfg.get("partial_tp_trigger_pct", 0.05))
                if current >= buy_avg * (1 + trig_pct):
                    sell_ratio = float(cfg.get("partial_tp_sell_ratio", 0.30))
                    sell_qty = max(1, math.floor(qty * sell_ratio))
                    if sell_qty >= qty:
                        sell_qty = qty - 1
                    if sell_qty >= 1:
                        res = place_order_kr(uid, cfg, code, "sell", sell_qty, 0)
                        order_no = (res.get("output") or {}).get("ODNO", "N/A")
                        pnl, _closed = register_partial_sell(
                            uid, "KR", code, current, sell_qty,
                            cfg.get("partial_tp_tighten_stop", True),
                            float(cfg.get("partial_tp_tighten_buffer_pct", 0.005)),
                        )
                        add_trade(uid, "KR", code, "sell", current, sell_qty, "분할익절", pnl, stock_name=sn)
                        st = get_bot_state(uid)
                        update_bot_state(uid, {"realized_pnl": st.get("realized_pnl", 0) + pnl})
                        _add_log(uid, "INFO",
                                 f"[KR][{code}] 분할익절 | {sell_qty}주@{current:,} "
                                 f"PnL≈{pnl:,.0f}원 주문={order_no}")
                        _log_sell_fill(uid, "KR", code, "sell", order_no, float(current), sell_qty, cfg)
                        _invalidate_balance_cache(uid)
                        continue

            # 트레일링 스탑: 평단 대비 +activate_pct 이상 상승한 이후, 전고점 대비 trail_pct 이상 되돌림 시 청산
            if cfg.get("trailing_stop_enabled", True) and buy_avg > 0:
                activate_pct = float(cfg.get("trailing_stop_activate_pct", 0.03))
                trail_pct = float(cfg.get("trailing_stop_pct", 0.04))
                if new_high >= buy_avg * (1 + activate_pct):
                    trail_line = new_high * (1 - trail_pct)
                    if current <= trail_line and current > slp:
                        res = place_order_kr(uid, cfg, code, "sell", qty, 0)
                        order_no = (res.get("output") or {}).get("ODNO", "N/A")
                        pnl = register_sell(uid, "KR", code, current)
                        add_trade(uid, "KR", code, "sell", current, qty, "트레일링스탑", pnl, stock_name=sn)
                        update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                        _add_log(uid, "INFO",
                                 f"[KR][{code}] 트레일링 스탑 | 현재={current:,} 고점={new_high:,} "
                                 f"트레일선={trail_line:,.0f} PnL≈{pnl:,.0f}원 주문={order_no}")
                        _log_sell_fill(uid, "KR", code, "sell", order_no, float(current), qty, cfg)
                        _invalidate_balance_cache(uid)
                        continue

            if target > 0 and current >= target:
                res = place_order_kr(uid, cfg, code, "sell", qty, 0)
                order_no = (res.get("output") or {}).get("ODNO", "N/A")
                pnl = register_sell(uid, "KR", code, current)
                add_trade(uid, "KR", code, "sell", current, qty, "목표가_달성", pnl, stock_name=sn)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO",
                         f"[KR][{code}] 목표가 달성 매도 | 현재={current:,} 목표={target:,} "
                         f"PnL≈{pnl:,.0f}원 주문={order_no}")
                _log_sell_fill(uid, "KR", code, "sell", order_no, float(current), qty, cfg)
                _invalidate_balance_cache(uid)
                continue
            if slp > 0 and current <= slp:
                res = place_order_kr(uid, cfg, code, "sell", qty, 0)
                order_no = (res.get("output") or {}).get("ODNO", "N/A")
                pnl = register_sell(uid, "KR", code, current)
                add_trade(uid, "KR", code, "sell", current, qty, "손절", pnl, stock_name=sn)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "WARNING",
                         f"[KR][{code}] 손절 | 현재={current:,} 손절가={slp:,} "
                         f"PnL≈{pnl:,.0f}원 주문={order_no}")
                _log_sell_fill(uid, "KR", code, "sell", order_no, float(current), qty, cfg)
                _invalidate_balance_cache(uid)
                continue

            # 시간 기반 청산 (Time stop): 보유 N일 경과 + 수익률 ±flat_pct 이내면 자본 회수
            if cfg.get("time_stop_enabled", True):
                hold_days = int(cfg.get("time_stop_days", 5))
                flat_pct = float(cfg.get("time_stop_flat_pct", 0.02))
                age_days = _position_age_days(pos.get("entry_time"))
                if age_days >= hold_days and abs(pnl_pct) < flat_pct * 100:
                    res = place_order_kr(uid, cfg, code, "sell", qty, 0)
                    order_no = (res.get("output") or {}).get("ODNO", "N/A")
                    pnl = register_sell(uid, "KR", code, current)
                    add_trade(uid, "KR", code, "sell", current, qty, "시간청산", pnl, stock_name=sn)
                    update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                    _add_log(uid, "INFO",
                             f"[KR][{code}] 시간청산 | 보유 {age_days}일 수익 {pnl_pct:+.2f}% "
                             f"< ±{flat_pct*100:.1f}% | 주문={order_no}")
                    _log_sell_fill(uid, "KR", code, "sell", order_no, float(current), qty, cfg)
                    _invalidate_balance_cache(uid)
                    continue

            # 물타기: 평단 대비 일정 하락 + 손절선 위 + 횟수/간격/총비중 + 추세 필터
            if cfg.get("avg_down_enabled", True):
                max_ad = int(cfg.get("avg_down_max_times", 2))
                last_at = pos.get("avg_down_last_at")
                min_h = float(cfg.get("avg_down_min_interval_hours", 20))
                interval_ok = True
                if last_at:
                    try:
                        raw = str(last_at).replace("Z", "+00:00")
                        ld = datetime.fromisoformat(raw)
                        if ld.tzinfo is None:
                            ld = ld.replace(tzinfo=KST)
                        else:
                            ld = ld.astimezone(KST)
                        if (datetime.now(KST) - ld).total_seconds() < min_h * 3600:
                            interval_ok = False
                    except Exception:
                        interval_ok = True
                dip_pct = float(cfg.get("avg_down_trigger_pct", 0.04))
                ad_cnt = int(pos.get("avg_down_count", 0))
                price_dipped = current <= buy_avg * (1 - dip_pct)
                above_stop = current > slp * 1.002

                # 추세 필터: RSI > 25 (극심한 과매도 탈출) + MA5 정배열
                closes_kr = [float(str(r.get("stck_clpr", r.get("clos", 0))).replace(",", "") or 0) for r in ohlcv_pos]
                rsi_kr = _calc_rsi(closes_kr[:30]) if len(closes_kr) >= 16 else 50.0
                ma5_kr = sum(closes_kr[:5]) / 5 if len(closes_kr) >= 5 else current
                ma20_kr = sum(closes_kr[:20]) / 20 if len(closes_kr) >= 20 else ma5_kr
                trend_ok = rsi_kr >= 25 and (current >= ma5_kr * 0.97 or ma5_kr >= ma20_kr * 0.98)

                skip_reason = None
                if ad_cnt >= max_ad:
                    skip_reason = f"횟수한도({ad_cnt}/{max_ad})"
                elif not interval_ok:
                    skip_reason = f"간격미달({min_h}h)"
                elif not price_dipped:
                    skip_reason = f"하락폭미달({(1 - current / buy_avg) * 100:.1f}%<{dip_pct * 100:.0f}%)"
                elif not above_stop:
                    skip_reason = "손절임박"
                elif not trend_ok:
                    skip_reason = f"추세불량(RSI={rsi_kr:.0f},MA5={ma5_kr:,.0f})"

                if skip_reason is None:
                    available = get_available_cash_kr(uid, cfg, code)
                    equity = _get_total_equity_kr(uid, cfg) or available
                    max_ratio = float(cfg.get("max_position_ratio", 0.1))
                    pos_cap = equity * max_ratio * 1.02
                    pos_val = float(current * qty)
                    room = pos_cap - pos_val
                    ad_ratio = float(cfg.get("avg_down_qty_ratio", 0.35))
                    wish = max(1, math.floor(qty * ad_ratio))
                    max_add_cash = math.floor(available / current) if current > 0 else 0
                    max_add_room = math.floor(room / current) if room > 0 and current > 0 else 0
                    add_qty = min(wish, max_add_cash, max_add_room)
                    if add_qty < 1:
                        skip_reason = f"수량부족(현금{max_add_cash},비중여유{max_add_room})"
                    elif not _try_acquire_buy_lock(uid, "KR", code):
                        skip_reason = "락획득실패"
                    else:
                        try:
                            res = place_order_kr(uid, cfg, code, "buy", int(add_qty), 0)
                            order_no = res.get("output", {}).get("ODNO", "N/A")
                            merge_position_after_avg_down(uid, "KR", code, current, int(add_qty), ohlcv_pos, cfg)
                            add_trade(uid, "KR", code, "buy", current, int(add_qty), "물타기", stock_name=sn)
                            _add_log(uid, "INFO", f"[KR][{code}] 물타기 | +{add_qty}주@{current:,} 주문={order_no} RSI={rsi_kr:.0f}")
                            _invalidate_balance_cache(uid)
                        finally:
                            _release_buy_lock(uid, "KR", code)
                # 물타기 스킵 로그 (10분마다 verbose 또는 하락폭 충족 시)
                if skip_reason and (verbose or price_dipped):
                    _add_log(uid, "INFO", f"[KR][{code}] 물타기스킵 | {skip_reason} (현재={current:,} 평단={buy_avg:,})")
        except Exception as e:
            _add_log(uid, "ERROR", f"[KR][{code}] 포지션 체크 오류: {e}")

    positions = get_positions(uid, "KR")
    watchlist = cfg.get("kr_watchlist", [])
    diag_parts: list[str] = []
    max_per_sector  = int(cfg.get("max_positions_per_sector", 2))
    sector_exposure = _get_sector_exposure(uid, "KR")

    buy_ok, buy_block_reason = _kr_buy_window_ok(cfg)
    if not buy_ok:
        if verbose:
            _add_log(uid, "INFO", f"[KR] 신규매수 블랙아웃 | {buy_block_reason}")
        # 포지션 관리는 이미 위에서 끝났으므로 여기서 return
        return

    pnl_ok, pnl_reason = _daily_pnl_buy_gate(state, cfg)
    if not pnl_ok:
        _add_log(uid, "INFO", f"[KR] 신규매수 중단 | {pnl_reason}")
        return

    regime_ok, regime_reason = _kr_index_buy_gate(uid, cfg)
    if not regime_ok:
        _add_log(uid, "INFO", f"[KR] 신규매수 보류 | {regime_reason}")
        return

    for code in watchlist:
        if code in positions:
            diag_parts.append(f"{code}=보유중")
            continue
        try:
            data = get_current_price_kr(uid, cfg, code)
            ohlcv = get_daily_ohlcv_kr(uid, cfg, code)
            current = _kr_price_from_api_data(data, ohlcv)
            if current <= 0:
                diag_parts.append(f"{code}=시세없음")
                continue
            if len(ohlcv) < 2:
                diag_parts.append(f"{code}=일봉부족({len(ohlcv)})")
                continue

            today_open = float(str(ohlcv[0].get("stck_oprc", 0)).replace(",", "") or 0)
            prev_range = float(str(ohlcv[1].get("stck_hgpr", 0)).replace(",", "") or 0) - float(
                str(ohlcv[1].get("stck_lwpr", 0)).replace(",", "") or 0
            )
            k = cfg.get("k_factor", 0.5)
            target = today_open + k * prev_range
            ma5 = sum(
                float(str(r.get("stck_clpr", 0)).replace(",", "") or 0) for r in ohlcv[:5]
            ) / min(5, len(ohlcv))

            # K팩터 진입 슬리피지 필터:
            # 목표가를 이미 max_entry_slip(기본 2%) 이상 지나쳤으면 추격 매수 금지.
            # 돌파 직후 시장가로 들어가야 기대값이 있음.
            _is_mock_kr = cfg.get("is_mock", True)
            max_slip    = cfg.get(
                "max_entry_slip_pct_mock" if _is_mock_kr else "max_entry_slip_pct_live",
                cfg.get("max_entry_slip_pct", 0.05 if _is_mock_kr else 0.03)
            )
            above_target = target <= current <= target * (1 + max_slip)
            above_ma5    = current > ma5

            if above_target and above_ma5:
                # 최소 스코어 체크 (KR 기본 40점 / 100점 만점)
                score_result = score_stock_algorithm(current, ohlcv, cfg)
                min_score_kr = int(cfg.get("min_score_kr", 40))
                if score_result["score"] < min_score_kr:
                    diag_parts.append(
                        f"{code}=점수미달({score_result['score']}/{min_score_kr})"
                    )
                    continue
                # 섹터 한도 체크
                sec_ok, sector = _sector_ok(code, "KR", sector_exposure, max_per_sector)
                if not sec_ok:
                    diag_parts.append(f"{code}=섹터한도({sector}{sector_exposure.get(sector,0)}/{max_per_sector})")
                    continue
                available = get_available_cash_kr(uid, cfg, code)
                equity = _get_total_equity_kr(uid, cfg) or available
                prices_kr = calculate_optimal_prices(current, ohlcv, cfg)
                qty, qty_reason = _risk_based_qty(
                    equity, available, current,
                    float(prices_kr.get("stop_loss") or 0), cfg,
                )
                if qty <= 0 and available >= current and equity > 0:
                    # 안전망: 폴백 모드에서도 1주 가능하면 1주
                    qty = 1
                    qty_reason = "최소1주"
                if qty > 0:
                    if not _try_acquire_buy_lock(uid, "KR", code):
                        diag_parts.append(f"{code}=매수중복방지(락)")
                        continue
                    try:
                        result = place_order_kr(uid, cfg, code, "buy", qty, 0)
                        order_no = result.get("output", {}).get("ODNO", "N/A")
                        out = data.get("output") or {}
                        sname = _stock_name(out.get("hts_kor_isnm", "") if isinstance(out, dict) else "", code, "KR")
                        register_buy(
                            uid, "KR", code, current, qty, cfg.get("stop_loss_ratio", 0.03),
                            float(prices_kr["sell_price"]), "자동", sname,
                            stop_loss_price=float(prices_kr["stop_loss"]),
                        )
                        add_trade(uid, "KR", code, "buy", current, qty, "자동매수", stock_name=sname)
                        _add_log(uid, "INFO",
                                 f"[KR][{code}] 자동매수 | {qty}주@{current:,} 주문={order_no} "
                                 f"목표={float(prices_kr['sell_price']):,.0f} 손절={float(prices_kr['stop_loss']):,.0f} "
                                 f"사이징({qty_reason})")
                        diag_parts.append(f"{code}=✅매수{qty}주")
                        sector_exposure[sector] = sector_exposure.get(sector, 0) + 1
                    finally:
                        _release_buy_lock(uid, "KR", code)
                else:
                    diag_parts.append(f"{code}=돌파했으나잔액부족(가격{current:,}/잔액{available:,.0f})")
            else:
                gap = current - target
                reason = []
                if current < target:
                    reason.append(f"미돌파(현재{current:,}/목표{target:,.0f}/차이{gap:+,.0f})")
                elif current > target * (1 + max_slip):
                    reason.append(f"추격금지(현재{current:,}/상한{target*(1+max_slip):,.0f})")
                if not above_ma5:
                    reason.append(f"MA5하회(현재{current:,}/MA5{ma5:,.0f})")
                diag_parts.append(f"{code}:{'·'.join(reason) or '조건미달'}")
        except Exception as e:
            diag_parts.append(f"{code}=오류")
            _add_log(uid, "ERROR", f"[KR][{code}] 매수 체크 오류: {e}")

    bought = [p for p in diag_parts if "✅" in p]
    if verbose:
        if diag_parts:
            label = "스캔결과" if bought else "매수없음"
            _add_log(uid, "INFO", f"[KR] {label} | {' | '.join(diag_parts)}")
        else:
            wl = cfg.get("kr_watchlist", [])
            _add_log(uid, "WARNING", f"[KR] 감시종목 없음 — kr_watchlist={wl} (설정 필요)")


# ══════════════════════════════════════════════════════════
# Gemini AI 추천
# ══════════════════════════════════════════════════════════

# AI 세션: 속도 우선 (시간 단축)
MAX_AI_UNIVERSE_STOCKS = 20   # 시세 수집 호출 수 ∝ 이 값
GEMINI_MAX_CANDIDATES = 12    # Gemini 출력·스코어링·매수 후보 길이
GEMINI_UNIVERSE_KR = [
    "005930", "000660", "035420", "051910", "006400", "035720", "000270", "068270",
    "207940", "005380", "012330", "066570", "105560", "055550", "086790", "003550",
    "034730", "096770", "017670", "015760", "028260", "032830", "009150", "033780",
    "018260", "011200", "024110", "316140", "042700", "086520", "247540", "352820",
    "373220", "323410", "000810", "302440", "090430", "006260", "001040", "000100",
    "034220", "009540", "010130", "000720", "005490", "267250", "036570", "052400",
    "196170", "003670", "010950", "036460",
]
GEMINI_UNIVERSE_US = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX",
    "COST", "PEP", "KO", "PG", "JPM", "V", "MA", "UNH", "JNJ", "HD",
    "XOM", "CVX", "LLY", "ABBV", "MRK", "BAC", "DIS", "CSCO", "ADBE",
    "CRM", "INTC", "QCOM", "TXN", "AMAT", "HON", "IBM", "GE", "CAT", "UPS",
    "PM", "TMO", "SPGI", "COP", "BLK", "BKNG", "AXP", "NOW", "SBUX", "GILD",
]


def _merge_ai_universe_kr(cfg: dict) -> list[str]:
    """감시목록 우선 + 대표 풀, 중복 제거, API 부담 상한까지."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in list(cfg.get("kr_watchlist", [])) + GEMINI_UNIVERSE_KR:
        s = str(raw).strip()
        if not s.isdigit():
            continue
        c = s.zfill(6)
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
        if len(out) >= MAX_AI_UNIVERSE_STOCKS:
            break
    return out


def _merge_ai_universe_us(cfg: dict) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in list(cfg.get("us_watchlist", [])) + GEMINI_UNIVERSE_US:
        s = str(raw).strip().upper()
        if not s or not s.replace(".", "").isalnum():
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= MAX_AI_UNIVERSE_STOCKS:
            break
    return out


def _collect_kr_stock_data_for_codes(uid: str, cfg: dict, codes: list[str]) -> list[dict]:
    result = []
    for code in codes:
        try:
            price_data = get_current_price_kr(uid, cfg, code)
            out = price_data.get("output", {})
            if not isinstance(out, dict):
                out = {}
            ohlcv = get_daily_ohlcv_kr(uid, cfg, code)
            current = _kr_price_from_output(out, ohlcv)
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


def _collect_kr_stock_data(uid: str, cfg: dict) -> list[dict]:
    return _collect_kr_stock_data_for_codes(uid, cfg, cfg.get("kr_watchlist", []))


def _collect_kr_stock_data_for_ai(uid: str, cfg: dict) -> list[dict]:
    return _collect_kr_stock_data_for_codes(uid, cfg, _merge_ai_universe_kr(cfg))


def _collect_us_stock_data_for_codes(uid: str, cfg: dict, codes: list[str]) -> list[dict]:
    """미국 종목 데이터 수집 (코드 목록 지정)"""
    result = []
    for code in codes:
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


def _collect_us_stock_data(uid: str, cfg: dict) -> list[dict]:
    return _collect_us_stock_data_for_codes(uid, cfg, cfg.get("us_watchlist", []))


def _collect_us_stock_data_for_ai(uid: str, cfg: dict) -> list[dict]:
    return _collect_us_stock_data_for_codes(uid, cfg, _merge_ai_universe_us(cfg))


def _research_date_key(market: str) -> str:
    """캐시 키용 캘린더 날짜 (KR=KST, US=ET)."""
    if market == "US":
        return datetime.now(ET).date().isoformat()
    return datetime.now(KST).date().isoformat()


def _parse_gemini_json_object(raw_text: str) -> dict:
    clean = re.sub(r"```(?:json)?", "", raw_text.strip()).strip().rstrip("`").strip()
    return json.loads(clean)


def _is_gemini_quota_error(exc: BaseException) -> bool:
    """429 / RESOURCE_EXHAUSTED / free tier limit 등."""
    msg = str(exc).upper()
    if "429" in str(exc):
        return True
    if "RESOURCE_EXHAUSTED" in msg:
        return True
    if "QUOTA" in msg and ("EXCEED" in msg or "LIMIT" in msg or "0" in msg):
        return True
    return False


class GeminiQuotaBudgetExhausted(Exception):
    """환경변수 GEMINI_DAILY_CALL_BUDGET(일일 API 호출 상한) 소진."""


def _gemini_daily_budget() -> int:
    """0이면 무제한(기본). 양수면 UTC 자정 기준 일일 카운터 상한."""
    raw = (os.environ.get("GEMINI_DAILY_CALL_BUDGET") or "0").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _gemini_quota_doc_ref():
    dk = datetime.now(timezone.utc).date().isoformat()
    return get_db().collection("gemini_quota").document(dk)


def _gemini_calls_today() -> int:
    try:
        snap = _gemini_quota_doc_ref().get()
        if not snap.exists:
            return 0
        return int((snap.to_dict() or {}).get("calls", 0))
    except Exception:
        return 0


def _gemini_allow_new_call() -> bool:
    b = _gemini_daily_budget()
    if b <= 0:
        return True
    return _gemini_calls_today() < b


@transactional
def _gemini_increment_txn(transaction, ref):
    snap = ref.get(transaction=transaction)
    c = int((snap.to_dict() or {}).get("calls", 0))
    transaction.set(ref, {"calls": c + 1}, merge=True)


def _gemini_increment_call() -> None:
    try:
        ref = _gemini_quota_doc_ref()
        txn = get_db().transaction()
        _gemini_increment_txn(txn, ref)
    except Exception as e:
        logger.warning("[gemini_quota] increment failed: %s", e)


def _ai_candidates_cache_doc_id(market: str, session: str, dk: str) -> str:
    return f"ai_candidates_{market}_{session}_{dk}"


def _load_ai_candidates_cache(
    uid: str, market: str, session: str, dk: str,
) -> tuple[list[str], dict[str, str]] | None:
    ref = _uref(uid).collection("cache").document(_ai_candidates_cache_doc_id(market, session, dk))
    snap = ref.get()
    if not snap.exists:
        return None
    d = snap.to_dict() or {}
    codes = d.get("candidate_codes")
    reasons = d.get("reasons_map") or {}
    if not isinstance(codes, list) or not codes:
        return None
    return [str(x).strip() for x in codes if str(x).strip()], dict(reasons)


def _save_ai_candidates_cache(
    uid: str, market: str, session: str, dk: str,
    candidate_codes: list[str], reasons_map: dict[str, str],
) -> None:
    try:
        _uref(uid).collection("cache").document(_ai_candidates_cache_doc_id(market, session, dk)).set(
            {
                "candidate_codes": candidate_codes,
                "reasons_map": reasons_map,
                "market": market,
                "session": session,
                "date_key": dk,
                "updated_at": datetime.now(KST).isoformat(),
            },
            merge=True,
        )
    except Exception as e:
        logger.warning("[Gemini] 후보 캐시 저장 실패: %s", e)


def _load_any_fallback_ai_cache(
    uid: str, market: str, dk: str,
) -> tuple[list[str], dict[str, str]] | None:
    """같은 거래일·같은 시장에서 이미 성공한 세션 캐시를 late → afternoon → morning 순으로 탐색."""
    for sess in ("late", "afternoon", "morning"):
        hit = _load_ai_candidates_cache(uid, market, sess, dk)
        if hit and hit[0]:
            return hit
    return None


def _research_fallback_response(uid: str, market: str, dk: str, quota: bool, detail: str = "") -> dict:
    """Gemini 실패 시 Firestore에 쓰지 않고 UI용 문구만 반환."""
    if quota:
        bullets = [
            "Google Gemini API 무료(또는 현재 플랜) 호출 한도가 찼습니다(429). 잠시 후 다시 시도하거나 요금제·할당량을 확인해 주세요.",
            "Google AI Studio 또는 Cloud Console에서 결제·쿼터를 설정하면 동일 키로 한도가 늘어날 수 있습니다.",
            "문서: https://ai.google.dev/gemini-api/docs/rate-limits · 사용량: https://ai.dev/rate-limit",
        ]
        title = "AI 리서치 — 할당량 초과"
    else:
        bullets = [
            "AI 요약 생성에 실패했습니다. 잠시 후 새로고침 해 주세요.",
            (detail[:180] + "…") if len(detail) > 180 else detail or "Gemini API 오류",
        ]
        title = "AI 리서치 — 생성 실패"
    logger.warning("[research] uid=%s market=%s fallback quota=%s %s", uid, market, quota, detail[:120] if detail else "")
    return {
        "market": market,
        "date": dk,
        "title": title,
        "bullets": bullets,
        "cached": False,
        "fallback": True,
        "quota_exceeded": quota,
    }


# Gemini 호출 실패 시 순차 시도 순서 (무료 티어는 모델·메트릭별 한도가 분리되는 경우가 있음)
_GEMINI_FALLBACK_MODELS: tuple[str, ...] = (
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash-lite",
)


def _gemini_generate_with_fallback(
    client: genai.Client,
    prompt: str,
    *,
    log_prefix: str = "gemini",
    primary_env: str | None = None,
    default_model: str = "gemini-2.0-flash",
) -> str:
    """여러 모델 순차 시도 — 429·모델 미지원 시 다음 후보.

    무료 플랜에서 특정 *-flash-lite 만 limit:0 으로 막히는 경우가 있어
    동일 계열 비-lite 모델을 먼저 시도합니다. 429 직후 짧은 대기 후 다음 모델.

    GEMINI_DAILY_CALL_BUDGET>0 이면 성공 응답 1회당 gemini_quota/{UTC날짜}.calls 를 +1.
    """
    if not _gemini_allow_new_call():
        raise GeminiQuotaBudgetExhausted("GEMINI_DAILY_CALL_BUDGET 초과")
    primary = (os.environ.get(primary_env) or default_model).strip() if primary_env else default_model
    models: list[str] = []
    for m in (primary, *_GEMINI_FALLBACK_MODELS):
        if m and m not in models:
            models.append(m)
    last_exc: BaseException | None = None
    for i, model in enumerate(models):
        try:
            response = client.models.generate_content(model=model, contents=prompt)
            text = (response.text or "").strip()
            if text:
                logger.info("[%s] ok model=%s", log_prefix, model)
                _gemini_increment_call()
                return text
        except Exception as e:
            last_exc = e
            logger.warning("[%s] model=%s error: %s", log_prefix, model, e)
            if _is_gemini_quota_error(e) and i + 1 < len(models):
                time_module.sleep(1.2)
            continue
    if last_exc:
        raise last_exc
    raise RuntimeError("Gemini 응답이 비었습니다.")


def _generate_research_content(client: genai.Client, prompt: str) -> str:
    return _gemini_generate_with_fallback(
        client, prompt, log_prefix="research", primary_env="GEMINI_MODEL_RESEARCH", default_model="gemini-2.0-flash"
    )


def get_or_create_daily_research(uid: str, market: str, force_refresh: bool = False) -> dict:
    """
    일별 시장 리서치 (Firestore 캐시 + Gemini).
    """
    dk = _research_date_key(market)
    doc_ref = _uref(uid).collection("cache").document(f"market_research_{market}_{dk}")
    if not force_refresh:
        snap = doc_ref.get()
        if snap.exists:
            d = snap.to_dict() or {}
            return {
                "market": market,
                "date": dk,
                "title": d.get("title", ""),
                "bullets": d.get("bullets", []),
                "cached": True,
            }

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return {
            "market": market,
            "date": dk,
            "title": "시장 리서치",
            "bullets": [
                "GEMINI_API_KEY가 Cloud Functions 환경에 없어 요약을 생성할 수 없습니다.",
                "Firebase Console → Functions → 환경 변수에 키를 등록한 뒤 다시 시도해 주세요.",
            ],
            "cached": False,
            "fallback": True,
        }

    if not _gemini_allow_new_call():
        return {
            "market": market,
            "date": dk,
            "title": "AI 리서치 — 호출 예산",
            "bullets": [
                "오늘 설정된 Gemini 일일 호출 예산(GEMINI_DAILY_CALL_BUDGET)을 모두 사용했습니다.",
                "카운터는 UTC 자정 기준으로 갱신됩니다. 내일 다시 시도하거나 예산을 늘리세요.",
            ],
            "cached": False,
            "fallback": True,
            "budget_exceeded": True,
        }

    client = genai.Client(api_key=api_key)
    if market == "US":
        prompt = """You are a US equity strategist. Write a concise Korean summary for retail investors about what to watch today in US markets (indices mood, key sectors, volatility/risk). Do not claim real-time prices.
Respond ONLY with valid JSON:
{"title":"한 줄 제목 (한국어)","bullets":["불릿1 한국어","불릿2","불릿3"]}
Use exactly 3 bullets, each under 120 characters."""
    else:
        prompt = """당신은 한국 증시 전략가입니다. 개인 투자자가 오늘 장에서 확인하면 좋은 포인트를 한국어로 짧게 정리하세요. 실시간 시세 수치는 쓰지 말고 일반적인 관점만 제시하세요.
반드시 아래 JSON만 출력하세요:
{"title":"한 줄 제목","bullets":["불릿1","불릿2","불릿3"]}
불릿은 정확히 3개, 각 120자 이내."""

    try:
        raw_text = _generate_research_content(client, prompt)
    except GeminiQuotaBudgetExhausted:
        return {
            "market": market,
            "date": dk,
            "title": "AI 리서치 — 호출 예산",
            "bullets": [
                "Gemini 호출 직전에 일일 예산이 소진되었습니다(동시 요청 등).",
                "잠시 후 새로고침하거나 GEMINI_DAILY_CALL_BUDGET을 조정해 주세요.",
            ],
            "cached": False,
            "fallback": True,
            "budget_exceeded": True,
        }
    except Exception as e:
        return _research_fallback_response(
            uid, market, dk, quota=_is_gemini_quota_error(e), detail=str(e)
        )

    try:
        parsed = _parse_gemini_json_object(raw_text)
    except Exception:
        parsed = {
            "title": "오늘의 시장 포인트",
            "bullets": [raw_text[:200] + ("…" if len(raw_text) > 200 else "")],
        }
    title = str(parsed.get("title", "오늘의 시장")).strip()
    bullets_raw = parsed.get("bullets", [])
    if isinstance(bullets_raw, str):
        bullets = [bullets_raw]
    else:
        bullets = [str(b).strip() for b in bullets_raw if str(b).strip()][:5]
    if len(bullets) < 1:
        bullets = ["요약을 생성하지 못했습니다. 잠시 후 새로고침 해 주세요."]

    doc_ref.set(
        {
            "title": title,
            "bullets": bullets[:5],
            "market": market,
            "date_key": dk,
            "updated_at": datetime.now(KST).isoformat(),
        }
    )
    return {
        "market": market,
        "date": dk,
        "title": title,
        "bullets": bullets[:5],
        "cached": False,
    }


def _resolve_allowed_stock_code(raw: str, stock_data: list[dict], market: str) -> str | None:
    """Gemini가 낸 code를 감시 목록(stock_data)의 code와 매칭. 미허용·환각 티커는 None."""
    s = str(raw).strip()
    if not s:
        return None
    codes = [str(d.get("code", "")).strip() for d in stock_data if d.get("code")]
    if not codes:
        return None
    if market == "US":
        u = s.upper()
        return u if u in codes else None
    if s in codes:
        return s
    if s.isdigit():
        z = s.zfill(6)
        if z in codes:
            return z
    return None


def _gemini_reason_lookup(code: str, reasons_map: dict[str, str], market: str) -> str:
    """reasons_map 키와 감시목록 code 표기(5930 vs 005930) 차이 보정."""
    if not reasons_map:
        return ""
    if market == "US":
        return (reasons_map.get(str(code).strip().upper()) or "").strip()
    s = str(code).strip()
    if s in reasons_map:
        return (reasons_map.get(s) or "").strip()
    if s.isdigit():
        z = s.zfill(6)
        if z in reasons_map:
            return (reasons_map.get(z) or "").strip()
    return ""


def query_gemini_candidates(uid: str, stock_data: list[dict], session: str, market: str = "KR") -> tuple[list[str], dict[str, str]]:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError("GEMINI_API_KEY가 설정되지 않았습니다.")

    dk = _research_date_key(market)
    hit = _load_ai_candidates_cache(uid, market, session, dk)
    if hit and hit[0]:
        _add_log(uid, "INFO", f"[Gemini] 세션 캐시 사용 ({market}/{session})")
        return hit

    if not _gemini_allow_new_call():
        fb = _load_any_fallback_ai_cache(uid, market, dk)
        if fb and fb[0]:
            _add_log(uid, "INFO", "[Gemini] 일일 예산 소진 — 다른 세션 캐시 사용")
            return fb
        _add_log(uid, "WARNING", "[Gemini] 일일 예산 소진·캐시 없음 — 알고리즘만으로 진행")
        return [], {}

    client = genai.Client(api_key=api_key)
    session_labels = {"morning": "오전 (09:30)", "afternoon": "오후 (13:00)", "late": "마감 (15:30)"}
    us_sessions    = {"morning": "오전 (ET 10:30)", "afternoon": "오후 (ET 13:00)", "late": "마감 (ET 15:30)"}
    session_label  = (us_sessions if market == "US" else session_labels).get(session, session)
    data_json = json.dumps(stock_data, ensure_ascii=False, indent=2)
    allowed_flat = ", ".join(
        str(d.get("code", "")).strip()
        for d in stock_data
        if d.get("code")
    ) or "(수집된 종목 없음)"

    nu = len(stock_data)
    gmax = GEMINI_MAX_CANDIDATES
    if nu >= gmax:
        us_cand_line = (
            f"- If the universe has ≥{gmax} rows, output EXACTLY {gmax} distinct candidates ordered by conviction (best first); "
            "tickers only from the JSON."
        )
        kr_cand_line = (
            f"- 입력 종목이 {gmax}개 이상이면 후보는 반드시 {gmax}개를 채우세요(확신 순, 중복 없음, 위 JSON의 code만)."
        )
    else:
        us_cand_line = (
            f"- Prefer up to {gmax} candidates; fewer is OK if the universe has fewer rows. Order by conviction (best first)."
        )
        kr_cand_line = (
            f"- 최대 {gmax}개까지 후보를 제시하세요(입력 종목 수가 적으면 그에 맞게 줄 수 있음). 확신 순으로 나열하세요."
        )

    if market == "US":
        prompt = f"""[System Persona]
You are a top-tier sell-side quant and short-term momentum specialist for US equities (NASDAQ/NYSE). Your task is to rank symbols from the PROVIDED DATA ONLY for the highest probability of favorable short-term (same session ~ 2 trading days) price action.

[Input Data — authoritative]
The JSON below is the ONLY universe you may recommend from. Each row has: code (ticker), current_price, change_rate (%), volume, recent_ohlcv (up to 5 recent bars: date, open, high, low, close, volume when available).
Session context: {session_label}

{data_json}

[Strict rules]
- You MUST ONLY output tickers that appear in the JSON "code" field above. Never invent or guess tickers. Uppercase tickers as in the data.
{us_cand_line}
- Output MUST be a single JSON object, no markdown, no code fences, no commentary before or after the JSON.

[Selection criteria — apply in order of importance]
1) Liquidity / participation: favor names with strong volume vs peers in the same list (relative activity within this universe).
2) Trend / momentum from OHLCV: avoid names that are clearly in a sharp breakdown; prefer stabilization, higher lows, or breakout-like structure using the bars provided.
3) Theme / narrative (inferred only from symbol context + price/volume behavior in the data — do not claim external news).
4) Session fit: align with a typical intraday/swing setup appropriate for {session_label} (e.g., avoid chasing extreme exhaustion spikes unless data supports it).

[Output schema — exact keys]
{{
  "candidates": [
    {{"code": "TICKER", "reason": "1–2 sentences in Korean, why this symbol within the data"}},
    ...
  ]
}}

Allowed tickers (subset of codes you may use): {allowed_flat}
"""
    else:
        prompt = f"""[System Persona]
당신은 월스트리트급 헤지펀드 출신 수석 퀀트 애널리스트이자, 한국 주식(KOSPI/KOSDAQ) 단기 모멘텀·수급 관점의 권위자입니다. 목표는 아래 [Input Data]만을 근거로, 단기(당일~2거래일) 상승 확률이 상대적으로 높은 종목을 고르는 것입니다.

[Input Data — 유일한 근거]
아래 JSON은 우리 서비스가 KIS API로 수집한 실데이터입니다. 각 행: code(종목코드), current_price, change_rate(전일대비%), volume(누적거래량 문자열), recent_ohlcv(최근 최대 5일: date, open, high, low, close).
세션: {session_label}

{data_json}

[절대 규칙]
- 추천 종목의 code는 반드시 위 JSON에 존재하는 종목코드만 사용하세요. 목록에 없는 코드·임의 종목·비상장명을 넣지 마세요. 6자리 숫자 형식을 데이터와 동일하게 맞추세요.
{kr_cand_line}
- 응답은 JSON 하나만. 마크다운·코드블록·앞뒤 설명 금지.

[선정 기준 — 아래 4가지를 엄격히 반영]
1) 거래대금/거래량: 동일 유니버스 안에서 누적거래량·가격 변동을 함께 볼 때 수급·관심이 상대적으로 큰 종목을 우선합니다.
2) 과거 데이터·추세: recent_ohlcv로 최근 하락만 반복하는 형태보다, 지지·되돌림 후 재상승 시도, 또는 변동성 수축 후 방향성이 나오는 패턴을 선호합니다(데이터로 설명 가능할 때만).
3) 인기·테마: 외부 뉴스를 사실로 단정하지 말고, 종목명·코드·섹터 연상이 가능할 때만 "테마"를 이유에 언급하세요.
4) 시장·세션 부합: {session_label} 기준으로 과도한 이미 급등·유동성 극소 등은 피합니다.

[출력 형식 — 키 이름 고정]
{{
  "candidates": [
    {{"code": "종목코드", "reason": "한국어 1~2문장, 위 데이터 근거만"}},
    ...
  ]
}}

허용 종목코드(이 중에서만 선택): {allowed_flat}
"""
    try:
        raw_text = _gemini_generate_with_fallback(
            client, prompt, log_prefix="candidates", primary_env="GEMINI_MODEL_AI", default_model="gemini-2.0-flash"
        )
    except GeminiQuotaBudgetExhausted:
        fb = _load_any_fallback_ai_cache(uid, market, dk)
        if fb and fb[0]:
            _add_log(uid, "INFO", "[Gemini] 예산 한도 직전 경쟁 — 캐시 사용")
            return fb
        return [], {}
    clean = re.sub(r"```(?:json)?", "", raw_text).strip().rstrip("`").strip()
    parsed = json.loads(clean)
    raw_candidates = parsed.get("candidates", [])[:GEMINI_MAX_CANDIDATES]
    candidate_codes: list[str] = []
    reasons_map: dict[str, str] = {}
    for item in raw_candidates:
        if isinstance(item, dict):
            raw_code = str(item.get("code", "")).strip()
            reason = str(item.get("reason", "")).strip()
        else:
            raw_code = str(item).strip()
            reason = ""
        code = _resolve_allowed_stock_code(raw_code, stock_data, market)
        if not code:
            if raw_code:
                _add_log(uid, "WARNING", f"[Gemini] 허용 목록 외 코드 무시: {raw_code!r}")
            continue
        if code in candidate_codes:
            continue
        candidate_codes.append(code)
        if reason:
            reasons_map[code] = reason
    if candidate_codes:
        _save_ai_candidates_cache(uid, market, session, dk, candidate_codes, reasons_map)
    _add_log(uid, "INFO", f"[Gemini] {session_label} 후보 {len(candidate_codes)}종목")
    return candidate_codes, reasons_map


def run_ai_session(
    uid: str,
    cfg: dict,
    session: str,
    market: str = "KR",
    add_buy_count: int | None = None,
):
    state = get_bot_state(uid)
    if not state.get("bot_enabled", True):
        _add_log(uid, "INFO", f"[AI {session}] 봇 비활성 — 건너뜀")
        return
    if state.get("trading_halted", False):
        _add_log(uid, "INFO", f"[AI {session}] 매매 중단 — 건너뜀")
        return
    if not _try_acquire_ai_session_lock(uid, market, session):
        _add_log(
            uid, "WARNING",
            "[AI] 이미 AI 세션이 진행 중입니다 (정각 오후 AI와 '즉시'를 동시에 누르면 겹칩니다). "
            "약 1~2분 후 다시 시도해 주세요.",
        )
        return
    try:
        _run_ai_session_impl(uid, cfg, session, market, add_buy_count=add_buy_count)
    finally:
        _release_ai_session_lock(uid, market)


def _run_ai_session_impl(
    uid: str,
    cfg: dict,
    session: str,
    market: str = "KR",
    add_buy_count: int | None = None,
):
    _add_log(uid, "INFO", f"[AI {session}][{market}] 이중 필터링 시작")

    # ── 시장별 데이터 수집 (Gemini 입력 유니버스: 감시목록 + 대표 풀, 최대 MAX_AI_UNIVERSE_STOCKS) ──
    try:
        if market == "US":
            stock_data = _collect_us_stock_data_for_ai(uid, cfg)
        else:
            stock_data = _collect_kr_stock_data_for_ai(uid, cfg)
        if not stock_data:
            _add_log(uid, "ERROR", f"[AI][{market}] 데이터 없음"); return
    except Exception as e:
        _add_log(uid, "ERROR", f"[AI][{market}] 데이터 수집 오류: {e}"); return

    _add_log(uid, "INFO", f"[AI][{market}] Gemini 입력 유니버스 {len(stock_data)}종목 수집")

    reasons_map: dict[str, str] = {}
    try:
        candidate_codes, reasons_map = query_gemini_candidates(uid, stock_data, session, market)
    except Exception as e:
        if _is_gemini_quota_error(e):
            _add_log(
                uid,
                "WARNING",
                "[AI] Gemini 할당량 초과(429) 또는 쿼터 없음 — 알고리즘만으로 후보를 고릅니다. "
                "Google AI Studio / Cloud Billing에서 요금제·일일 한도를 확인하세요.",
            )
        else:
            _add_log(uid, "ERROR", f"[AI] Gemini 오류: {e}")
        candidate_codes = []
        reasons_map = {}

    # 스코어링 대상: (1) Gemini가 준 후보(최대 20)만 — 알고리즘으로 재정렬 후 상위 N 노출
    # (2) Gemini 실패 시: 입력 유니버스 전체를 스코어링(폴백)
    codes_to_score: list[str] = []
    seen_score: set[str] = set()
    if candidate_codes:
        for raw in candidate_codes:
            c = str(raw).strip()
            if not c:
                continue
            if market == "US":
                c = c.upper()
            elif c.isdigit() and len(c) <= 6:
                c = c.zfill(6)
            if c in seen_score:
                continue
            seen_score.add(c)
            codes_to_score.append(c)
    else:
        _add_log(uid, "WARNING", "[AI] Gemini 유효 후보 없음 — 입력 유니버스 전체로 알고리즘 스코어링만 진행")
        for row in stock_data:
            c = str(row.get("code", "")).strip()
            if not c:
                continue
            if market == "US":
                c = c.upper()
            elif c.isdigit() and len(c) <= 6:
                c = c.zfill(6)
            if c in seen_score:
                continue
            seen_score.add(c)
            codes_to_score.append(c)

    if not codes_to_score:
        _add_log(uid, "ERROR", "[AI] 스코어링 대상 종목이 없음"); return

    # ── 시장별 스코어링 ────────────────────────────────────
    scored: list[tuple] = []
    stock_details: dict = {}
    for code in codes_to_score:
        try:
            if market == "US":
                data    = get_current_price_us(uid, cfg, code)
                out     = data["output"]
                current = float(out.get("last", out.get("stck_prpr", 0)))
                ohlcv   = get_daily_ohlcv_us(uid, cfg, code)
                score_result = score_us_stock_algorithm(current, ohlcv, cfg)
            else:
                data    = get_current_price_kr(uid, cfg, code)
                ohlcv   = get_daily_ohlcv_kr(uid, cfg, code)
                current = _kr_price_from_api_data(data, ohlcv)
                score_result = score_stock_algorithm(current, ohlcv, cfg)
            stock_details[code] = {"current": current, "ohlcv": ohlcv, "score": score_result}
            scored.append((code, score_result["score"], score_result["detail"]))
        except Exception as e:
            _add_log(uid, "WARNING", f"[AI][{code}] 스코어링 오류: {e}")

    if not scored:
        _add_log(uid, "ERROR", "[AI] 스코어링 결과 없음"); return

    if candidate_codes:
        gem_order = {c: i for i, c in enumerate(codes_to_score)}
    else:
        gem_order = {}
    scored.sort(key=lambda x: (-x[1], gem_order.get(x[0], 999)))

    # 스코어 요약 로그 (상위 5개) — 매수 미발생 원인 파악용
    score_summary = " | ".join(f"{c}={s}pt" for c, s, _ in scored[:5])
    _add_log(uid, "INFO", f"[AI][{market}] 스코어 상위: {score_summary}")

    # ai_stock_count = 해당 시장 최대 보유 종목 수(상한 3~5). 추천 카드는 상한만큼 노출.
    cap = min(max(int(cfg.get("ai_stock_count", 3)), 3), 5)
    positions_early = get_positions(uid, market)
    held = len(positions_early)
    slots = max(0, cap - held)

    if add_buy_count is None:
        # 스케줄·API에서 미지정: 상한까지 자동으로만 매수
        n_buy = slots
        buy_mode = "auto"
    else:
        # 사용자 지정: 0~5, 실제로는 잔여 슬롯·상한을 넘지 않음
        n_buy = max(0, min(int(add_buy_count), 5, slots))
        buy_mode = "user"

    if len(scored) < cap:
        _add_log(
            uid, "INFO",
            f"[AI] 스코어 산출 {len(scored)}개 — 상한 {cap}종 중 최대 {len(scored)}개만 추천 가능",
        )
    # _ai_build_rec: 추천 카드 dict 생성 헬퍼 (슬롯 없음 분기에서도 사용하므로 먼저 정의)
    def _ai_build_rec(code: str, score: float, detail: dict) -> dict:
        info    = stock_details.get(code, {})
        current = info.get("current", 0)
        ohlcv   = info.get("ohlcv", [])
        if market == "US":
            prices = calculate_optimal_prices_us(current, ohlcv, cfg)
        else:
            prices = calculate_optimal_prices(current, ohlcv, cfg)
        sname = _stock_name("", code, market)
        gem_reason = _gemini_reason_lookup(code, reasons_map, market)
        if not gem_reason:
            gem_reason = "Gemini 후보 중 알고리즘 스코어 상위"
        return {
            "code": code, "stock_name": sname, "score": score,
            "reason": gem_reason,
            **prices, "detail": detail,
        }

    top_stocks = scored[:cap]

    if n_buy == 0:
        # 포트폴리오 상한 도달: 추천 카드만 저장하고 매수 없이 종료
        recommendations = [_ai_build_rec(c, s, d) for c, s, d in top_stocks]
        session_id = datetime.now(KST).strftime("%Y%m%d_") + session + "_" + market
        _uref(uid).collection("recommendations").document(session_id).set({
            "session_id": session_id, "session": session, "market": market,
            "timestamp": datetime.now(KST), "candidates": candidate_codes,
            "recommendations": recommendations, "status": "completed",
            "portfolio_cap": cap, "held_before": held, "slots_remaining": 0,
            "target_new_buys": 0, "executed_codes": [], "executed_count": 0,
            "completed_at": datetime.now(KST),
        })
        _add_log(uid, "INFO",
                 f"[AI {session}] 포트폴리오 상한 {cap}종 중 {held}종 보유 — 슬롯 없음. "
                 f"현재 보유 종목을 매도하거나 'AI 추천 종목 수'를 늘려야 신규 매수 가능합니다.")
        _add_log(uid, "INFO",
                 f"[AI {session}] 완료 — 신규 0/0건 (추천 카드 {len(recommendations)}종목 · 상한 {cap}종 중 보유 {held})")
        return
    _add_log(
        uid, "INFO",
        f"[AI] 포트폴리오 상한 {cap}종 · 현재 보유 {held}종 · 잔여 슬롯 {slots} — "
        f"이번 신규 매수 목표 {n_buy}건 ({'자동' if buy_mode == 'auto' else '사용자 지정'})",
    )

    # ── 추천 목록 생성 (화면·이력용 상위 cap종목) ───────────────
    recommendations = [_ai_build_rec(c, s, d) for c, s, d in top_stocks]

    session_id = datetime.now(KST).strftime("%Y%m%d_") + session + "_" + market
    _uref(uid).collection("recommendations").document(session_id).set({
        "session_id": session_id, "session": session, "market": market,
        "timestamp": datetime.now(KST), "candidates": candidate_codes,
        "recommendations": recommendations, "status": "executing",
        "portfolio_cap": cap, "held_before": held, "slots_remaining": slots,
        "target_new_buys": n_buy, "buy_mode": buy_mode,
    })

    # ── 시장별 매수 실행 ───────────────────────────────────
    # 스코어 순 전체(scored)를 내려가며 실제 매수 n_buy건이 될 때까지 시도
    executed = []
    positions = positions_early
    # 미국: 자동매수(min_score_us)보다 낮게 — Gemini 후보는 이미 선별됨 (기본 40 / 125점)
    min_score_us_ai = int(cfg.get("min_score_us_ai", 40))
    max_per_sector   = int(cfg.get("max_positions_per_sector", 2))
    sector_exposure  = _get_sector_exposure(uid, market)
    skip_cnt: dict[str, int] = {}

    def _skip(reason: str) -> None:
        skip_cnt[reason] = skip_cnt.get(reason, 0) + 1

    for code, score, detail in scored:
        if len(executed) >= n_buy:
            break
        rec = _ai_build_rec(code, score, detail)
        code = rec["code"]
        if code in positions:
            _add_log(uid, "INFO", f"[AI][{market}][{code}] 이미 보유 — 매수 건너뜀")
            _skip("보유")
            continue
        if market == "US" and rec["score"] < min_score_us_ai:
            _add_log(
                uid, "INFO",
                f"[AI][US][{code}] 점수 미달({rec['score']}/{min_score_us_ai}, AI전용 기준) — 건너뜀",
            )
            _skip("US점수")
            continue
        # 섹터 노출 한도 체크
        sec_ok, sector = _sector_ok(code, market, sector_exposure, max_per_sector)
        if not sec_ok:
            _add_log(uid, "INFO",
                     f"[AI][{market}][{code}] 섹터 한도 | {sector} "
                     f"{sector_exposure.get(sector, 0)}/{max_per_sector}개 — 건너뜀")
            _skip("섹터")
            continue
        if not _try_acquire_buy_lock(uid, market, code):
            _add_log(
                uid, "INFO",
                f"[AI][{market}][{code}] 매수 락 선점 — 다른 AI/전략 스레드가 같은 종목 처리 중. "
                f"1~2분 뒤 '즉시' 재실행 또는 스케줄과 시간을 어긋나게 해 보세요.",
            )
            _skip("락")
            continue
        try:
            if market == "US":
                data    = get_current_price_us(uid, cfg, code)
                current = float(data["output"].get("last", 0))
                if current <= 0:
                    _add_log(uid, "WARNING", f"[AI][US][{code}] 현재가 0 — 매수 건너뜀")
                    _skip("시세0")
                    continue
                # 미국: USD 잔고 기반 수량 계산 (리스크-패리티, 실패 시 폴백)
                available_usd = _get_available_cash_us(uid, cfg)
                max_us_qty = int(cfg.get("max_us_qty", 5))
                if available_usd > 0:
                    qty_raw, qty_reason = _risk_based_qty(
                        available_usd, available_usd, current,
                        float(rec.get("stop_loss") or 0), cfg,
                    )
                    qty = max(1, min(qty_raw, max_us_qty)) if qty_raw > 0 else 1
                else:
                    qty = max(1, int(cfg.get("max_us_qty", 1)))
                    qty_reason = "잔고조회실패→폴백"
                result   = place_order_us(uid, cfg, code, "buy", qty, 0)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                sname    = _stock_name("", code, "US")
                register_buy(uid, "US", code, current, qty,
                             cfg.get("stop_loss_ratio", 0.03),
                             target_sell_price=float(rec["sell_price"]),
                             source=f"AI_{session}(점수{rec['score']})",
                             stock_name=sname,
                             stop_loss_price=float(rec.get("stop_loss") or 0))
                add_trade(uid, "US", code, "buy", current, qty, f"AI_{session}_US", 0.0, stock_name=sname)
                _add_log(uid, "INFO",
                         f"[AI][US][{code}] 매수 | {qty}주@${current:.2f} 주문={order_no} 사이징({qty_reason})")
            else:
                data      = get_current_price_kr(uid, cfg, code)
                ohlcv_ai  = get_daily_ohlcv_kr(uid, cfg, code)
                current   = _kr_price_from_api_data(data, ohlcv_ai)
                if current <= 0:
                    _add_log(uid, "WARNING", f"[AI][KR][{code}] 현재가 0 — 매수 건너뜀")
                    _skip("시세0")
                    continue
                available = get_available_cash_kr(uid, cfg, code)
                equity    = _get_total_equity_kr(uid, cfg) or available
                qty, qty_reason = _risk_based_qty(
                    equity, available, current,
                    float(rec.get("stop_loss") or 0), cfg,
                )
                # AI: 한 종목당 상한이 1주가보다 작으면 수량이 0이 되는 경우 방지 (주문가능 범위 내)
                if qty <= 0 and cfg.get("ai_afford_one_share", True) and available >= current:
                    qty = 1
                    qty_reason = "최소1주(AI)"
                if qty <= 0:
                    _add_log(uid, "WARNING",
                             f"[AI][KR][{code}] 매수불가 | 현재가={current:,} 주문가능={available:,.0f} "
                             f"자산={equity:,.0f} 사이징({qty_reason})")
                    _skip("잔고부족")
                    continue
                result   = place_order_kr(uid, cfg, code, "buy", qty, 0)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                register_buy(uid, "KR", code, current, qty, cfg.get("stop_loss_ratio", 0.03),
                             target_sell_price=float(rec["sell_price"]),
                             source=f"AI_{session}(점수{rec['score']})",
                             stock_name=rec.get("stock_name", ""),
                             stop_loss_price=float(rec.get("stop_loss") or 0))
                add_trade(uid, "KR", code, "buy", current, qty, f"AI_{session}", 0.0, stock_name=rec.get("stock_name", ""))
                _add_log(uid, "INFO",
                         f"[AI][{code}] 매수 | {qty}주@{current:,} 주문={order_no} 사이징({qty_reason})")
            executed.append(code)
            # 섹터 노출 카운트 업데이트 (루프 내 중복 매수 방지)
            sector_exposure[sector] = sector_exposure.get(sector, 0) + 1
        except Exception as e:
            _add_log(uid, "ERROR", f"[AI][{code}] 매수 오류: {e}")
            _skip("주문오류")
        finally:
            _release_buy_lock(uid, market, code)

    if len(executed) < n_buy and skip_cnt:
        _add_log(
            uid, "INFO",
            "[AI] 매수 건너뜀 요약 — " + ", ".join(f"{k}={v}" for k, v in sorted(skip_cnt.items())),
        )

    _uref(uid).collection("recommendations").document(session_id).update({
        "status": "completed", "executed_codes": executed,
        "executed_count": len(executed), "completed_at": datetime.now(KST),
        "target_new_buys": n_buy,
    })

    # ※ watchlist는 변경하지 않음.
    # AI 추천 결과는 recommendations 컬렉션에만 저장합니다.
    # 이전에 save_config(wl_key, rec_codes)를 호출했는데,
    # 이는 원래 25개 감시종목이 3~5개로 축소되는 부작용이 있었음.
    if len(executed) < n_buy:
        _add_log(
            uid, "INFO",
            f"[AI] 신규 매수 목표 {n_buy}건 중 {len(executed)}건 체결 — "
            f"섹터한도·점수·잔고 등으로 건너뛰었거나 후보가 부족할 수 있음",
        )
    _add_log(
        uid, "INFO",
        f"[AI {session}] 완료 — 신규 {len(executed)}/{n_buy}건 (추천 카드 {len(recommendations)}종목 · 상한 {cap}종 중 보유 {held})",
    )


def run_strategy_cycle_us(uid: str, cfg: dict):
    """
    미국 주식 전략 사이클
      1. 보유 포지션: 목표가 도달 → 익절, 손절가 이탈 → 손절
      2. 신규 매수: US 알고리즘 70점 이상인 종목만 매수
    """
    state = get_bot_state(uid)
    if not state.get("bot_enabled", True): return
    if state.get("trading_halted", False): return
    # US 사이클은 5분 간격이라 드로우다운은 매번 체크
    if _check_drawdown(uid, cfg): return

    # ── 보유 포지션 관리 ────────────────────────────────────
    positions = get_positions(uid, "US")
    for code, pos in list(positions.items()):
        try:
            data        = get_current_price_us(uid, cfg, code)
            ohlcv_pos_u = get_daily_ohlcv_us(uid, cfg, code)
            current     = float(data["output"].get("last", 0))
            if current <= 0:
                continue
            buy_avg = float(pos["buy_price"])
            qty     = int(pos["quantity"])
            target  = float(pos.get("target_sell_price", 0))
            slp     = float(pos.get("stop_loss_price", 0))
            sn      = pos.get("stock_name", "")
            pnl_pct_us = (current - buy_avg) / buy_avg * 100 if buy_avg > 0 else 0

            # 레거시 포지션 백필
            backfill_us: dict = {}
            prev_high_us = float(pos.get("highest_price") or 0)
            if prev_high_us <= 0:
                prev_high_us = max(buy_avg, current)
                backfill_us["highest_price"] = prev_high_us
            new_high_us = max(prev_high_us, current)
            if new_high_us > prev_high_us:
                backfill_us["highest_price"] = new_high_us
            if not pos.get("entry_time"):
                backfill_us["entry_time"] = datetime.now(KST)
            if backfill_us:
                try:
                    _uref(uid).collection("positions_US").document(code).update(backfill_us)
                except Exception:
                    pass

            # 손절선 본전 이동 (Break-even stop)
            if cfg.get("breakeven_stop_enabled", True) and not pos.get("breakeven_applied"):
                be_pct_us = float(cfg.get("breakeven_trigger_pct", 0.02))
                if current >= buy_avg * (1 + be_pct_us):
                    new_slp_us = max(slp, buy_avg)
                    if new_slp_us > slp:
                        try:
                            _uref(uid).collection("positions_US").document(code).update({
                                "stop_loss_price": new_slp_us,
                                "breakeven_applied": True,
                            })
                            _add_log(uid, "INFO",
                                     f"[US][{code}] 손절 본전이동 | ${slp:.2f}→${new_slp_us:.2f} "
                                     f"(수익 +{pnl_pct_us:.2f}%)")
                            slp = new_slp_us
                        except Exception:
                            pass

            # 5분마다 포지션 현황 로그 (자동매도 동작 확인용)
            now_us = datetime.now(KST)
            if now_us.minute % 5 == 0:
                _add_log(uid, "INFO",
                         f"[US][{code}] 포지션체크 | 현재=${current:.2f} 평단=${buy_avg:.2f} "
                         f"수익={pnl_pct_us:+.2f}% | 목표=${target:.2f} 손절=${slp:.2f} "
                         f"고점=${new_high_us:.2f}")

            if cfg.get("partial_tp_enabled", True) and not pos.get("partial_tp_done") and qty > 1:
                trig_pct = float(cfg.get("partial_tp_trigger_pct", 0.05))
                if current >= buy_avg * (1 + trig_pct):
                    sell_ratio = float(cfg.get("partial_tp_sell_ratio", 0.30))
                    sell_qty = max(1, math.floor(qty * sell_ratio))
                    if sell_qty >= qty:
                        sell_qty = qty - 1
                    if sell_qty >= 1:
                        res = place_order_us(uid, cfg, code, "sell", sell_qty, 0)
                        order_no = (res.get("output") or {}).get("ODNO", "N/A")
                        pnl, _ = register_partial_sell(
                            uid, "US", code, current, sell_qty,
                            cfg.get("partial_tp_tighten_stop", True),
                            float(cfg.get("partial_tp_tighten_buffer_pct", 0.005)),
                        )
                        add_trade(uid, "US", code, "sell", current, sell_qty, "분할익절", pnl, stock_name=sn)
                        _log_sell_fill(uid, "US", code, "sell", order_no, float(current), sell_qty, cfg)
                        st = get_bot_state(uid)
                        update_bot_state(uid, {"realized_pnl": st.get("realized_pnl", 0) + pnl})
                        _add_log(uid, "INFO",
                                 f"[US][{code}] 분할익절 | {sell_qty}주@${current:.2f} "
                                 f"PnL ${pnl:+.2f} 주문={order_no}")
                        _invalidate_balance_cache(uid)
                        continue

            # 트레일링 스탑: 평단 대비 +activate_pct 이상 상승 후, 전고점 대비 trail_pct 이상 되돌림 시 청산
            if cfg.get("trailing_stop_enabled", True) and buy_avg > 0:
                activate_pct = float(cfg.get("trailing_stop_activate_pct", 0.03))
                trail_pct = float(cfg.get("trailing_stop_pct", 0.04))
                if new_high_us >= buy_avg * (1 + activate_pct):
                    trail_line = new_high_us * (1 - trail_pct)
                    if current <= trail_line and current > slp:
                        res = place_order_us(uid, cfg, code, "sell", qty, 0)
                        order_no = (res.get("output") or {}).get("ODNO", "N/A")
                        pnl = register_sell(uid, "US", code, current)
                        add_trade(uid, "US", code, "sell", current, qty, "트레일링스탑", pnl, stock_name=sn)
                        update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                        _add_log(uid, "INFO",
                                 f"[US][{code}] 트레일링 스탑 | ${current:.2f} ≤ 트레일 ${trail_line:.2f} "
                                 f"(고점 ${new_high_us:.2f}) PnL ${pnl:+.2f} 주문={order_no}")
                        _log_sell_fill(uid, "US", code, "sell", order_no, float(current), qty, cfg)
                        _invalidate_balance_cache(uid)
                        continue

            if target > 0 and current >= target:
                res = place_order_us(uid, cfg, code, "sell", qty, 0)
                order_no = (res.get("output") or {}).get("ODNO", "N/A")
                pnl = register_sell(uid, "US", code, current)
                add_trade(uid, "US", code, "sell", current, qty, "목표가_달성", pnl, stock_name=sn)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO",
                         f"[US][{code}] 익절 | ${current:.2f} → 목표 ${target:.2f} "
                         f"| PnL ${pnl:+.2f} 주문={order_no}")
                _log_sell_fill(uid, "US", code, "sell", order_no, float(current), qty, cfg)
                _invalidate_balance_cache(uid)
                continue
            if slp > 0 and current <= slp:
                res = place_order_us(uid, cfg, code, "sell", qty, 0)
                order_no = (res.get("output") or {}).get("ODNO", "N/A")
                pnl = register_sell(uid, "US", code, current)
                add_trade(uid, "US", code, "sell", current, qty, "손절", pnl, stock_name=sn)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "WARNING",
                         f"[US][{code}] 손절 | ${current:.2f} ≤ 손절 ${slp:.2f} "
                         f"PnL ${pnl:+.2f} 주문={order_no}")
                _log_sell_fill(uid, "US", code, "sell", order_no, float(current), qty, cfg)
                _invalidate_balance_cache(uid)
                continue

            # 시간 기반 청산 (Time stop)
            if cfg.get("time_stop_enabled", True):
                hold_days_us = int(cfg.get("time_stop_days", 5))
                flat_pct_us = float(cfg.get("time_stop_flat_pct", 0.02))
                age_days_us = _position_age_days(pos.get("entry_time"))
                if age_days_us >= hold_days_us and abs(pnl_pct_us) < flat_pct_us * 100:
                    res = place_order_us(uid, cfg, code, "sell", qty, 0)
                    order_no = (res.get("output") or {}).get("ODNO", "N/A")
                    pnl = register_sell(uid, "US", code, current)
                    add_trade(uid, "US", code, "sell", current, qty, "시간청산", pnl, stock_name=sn)
                    update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                    _add_log(uid, "INFO",
                             f"[US][{code}] 시간청산 | 보유 {age_days_us}일 수익 {pnl_pct_us:+.2f}% "
                             f"< ±{flat_pct_us*100:.1f}% | 주문={order_no}")
                    _log_sell_fill(uid, "US", code, "sell", order_no, float(current), qty, cfg)
                    _invalidate_balance_cache(uid)
                    continue

            # 물타기: 평단 대비 일정 하락 + 손절선 위 + 횟수/간격/총비중 + 추세 필터
            if cfg.get("avg_down_enabled", True):
                max_ad = int(cfg.get("avg_down_max_times", 2))
                last_at = pos.get("avg_down_last_at")
                min_h = float(cfg.get("avg_down_min_interval_hours", 20))
                interval_ok = True
                if last_at:
                    try:
                        raw = str(last_at).replace("Z", "+00:00")
                        ld = datetime.fromisoformat(raw)
                        if ld.tzinfo is None:
                            ld = ld.replace(tzinfo=KST)
                        else:
                            ld = ld.astimezone(KST)
                        if (datetime.now(KST) - ld).total_seconds() < min_h * 3600:
                            interval_ok = False
                    except Exception:
                        interval_ok = True
                dip_pct = float(cfg.get("avg_down_trigger_pct", 0.04))
                ad_cnt = int(pos.get("avg_down_count", 0))
                price_dipped = current <= buy_avg * (1 - dip_pct)
                above_stop = current > slp * 1.002

                # 추세 필터: RSI > 25 (극심한 과매도 탈출) + MA5 정배열
                closes_us = [float(r.get("clos", r.get("stck_clpr", 0)) or 0) for r in ohlcv_pos_u]
                rsi_us = _calc_rsi(closes_us[:30]) if len(closes_us) >= 16 else 50.0
                ma5_us = sum(closes_us[:5]) / 5 if len(closes_us) >= 5 else current
                ma20_us = sum(closes_us[:20]) / 20 if len(closes_us) >= 20 else ma5_us
                trend_ok = rsi_us >= 25 and (current >= ma5_us * 0.97 or ma5_us >= ma20_us * 0.98)

                skip_reason = None
                if ad_cnt >= max_ad:
                    skip_reason = f"횟수한도({ad_cnt}/{max_ad})"
                elif not interval_ok:
                    skip_reason = f"간격미달({min_h}h)"
                elif not price_dipped:
                    skip_reason = f"하락폭미달({(1 - current / buy_avg) * 100:.1f}%<{dip_pct * 100:.0f}%)"
                elif not above_stop:
                    skip_reason = "손절임박"
                elif not trend_ok:
                    skip_reason = f"추세불량(RSI={rsi_us:.0f},MA5=${ma5_us:.2f})"

                if skip_reason is None:
                    available_usd = _get_available_cash_us(uid, cfg)
                    mv = float(current * qty)
                    est_pf_usd = available_usd + mv
                    max_ratio = float(cfg.get("max_position_ratio", 0.1))
                    pos_cap = est_pf_usd * max_ratio * 1.02
                    room = pos_cap - mv
                    ad_ratio = float(cfg.get("avg_down_qty_ratio", 0.35))
                    wish = max(1, math.floor(qty * ad_ratio))
                    max_add_cash = math.floor(available_usd / current) if current > 0 else 0
                    max_add_room = math.floor(room / current) if room > 0 and current > 0 else 0
                    add_qty = min(wish, max_add_cash, max_add_room)
                    if add_qty < 1:
                        skip_reason = f"수량부족(현금{max_add_cash},비중여유{max_add_room})"
                    elif not _try_acquire_buy_lock(uid, "US", code):
                        skip_reason = "락획득실패"
                    else:
                        try:
                            res = place_order_us(uid, cfg, code, "buy", int(add_qty), 0)
                            order_no = res.get("output", {}).get("ODNO", "N/A")
                            merge_position_after_avg_down(uid, "US", code, current, int(add_qty), ohlcv_pos_u, cfg)
                            add_trade(uid, "US", code, "buy", current, int(add_qty), "물타기", stock_name=sn)
                            _add_log(uid, "INFO", f"[US][{code}] 물타기 | +{add_qty}@${current:.2f} 주문={order_no} RSI={rsi_us:.0f}")
                            _invalidate_balance_cache(uid)
                        finally:
                            _release_buy_lock(uid, "US", code)
                # 물타기 스킵 로그 (하락폭 충족 시에만 — 왜 안 샀는지 추적용)
                if skip_reason and price_dipped:
                    _add_log(uid, "INFO", f"[US][{code}] 물타기스킵 | {skip_reason} (${current:.2f} 평단${buy_avg:.2f})")
        except Exception as e:
            _add_log(uid, "ERROR", f"[US][{code}] 포지션 체크 오류: {e}")

    # ── 신규 매수 검토 ──────────────────────────────────────
    buy_ok_us, buy_block_reason_us = _us_buy_window_ok(cfg)
    if not buy_ok_us:
        _add_log(uid, "INFO", f"[US] 신규매수 블랙아웃 | {buy_block_reason_us}")
        return

    pnl_ok_us, pnl_reason_us = _daily_pnl_buy_gate(state, cfg)
    if not pnl_ok_us:
        _add_log(uid, "INFO", f"[US] 신규매수 중단 | {pnl_reason_us}")
        return

    regime_ok_us, regime_reason_us = _us_index_buy_gate(uid, cfg)
    if not regime_ok_us:
        _add_log(uid, "INFO", f"[US] 신규매수 보류 | {regime_reason_us}")
        return

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

            # K팩터 진입 슬리피지 필터 (US)
            if len(ohlcv) >= 2:
                opens_us   = [float(r.get("open", r.get("stck_oprc", 0))) for r in ohlcv]
                highs_us   = [float(r.get("high", r.get("stck_hgpr", 0))) for r in ohlcv]
                lows_us    = [float(r.get("low",  r.get("stck_lwpr", 0))) for r in ohlcv]
                today_open = opens_us[0] if opens_us else current
                k_us       = cfg.get("k_factor", 0.3)
                target_us  = today_open + k_us * (highs_us[1] - lows_us[1])
                _is_mock_us = cfg.get("is_mock", True)
                max_slip   = cfg.get(
                    "max_entry_slip_pct_mock" if _is_mock_us else "max_entry_slip_pct_live",
                    cfg.get("max_entry_slip_pct", 0.05 if _is_mock_us else 0.03)
                )
                if target_us > 0 and current > target_us * (1 + max_slip):
                    _add_log(uid, "INFO",
                             f"[US][{code}] 돌파 후 추격 금지 | "
                             f"현재=${current:.2f} 목표=${target_us:.2f} "
                             f"(+{(current/target_us-1)*100:.1f}% 초과)")
                    continue

            prices = calculate_optimal_prices_us(current, ohlcv, cfg)
            available_usd = _get_available_cash_us(uid, cfg)
            equity_usd = available_usd  # USD 자기자본 추정값(가용 잔고로 근사)
            qty_raw, qty_reason = _risk_based_qty(
                equity_usd, available_usd, current,
                float(prices.get("stop_loss") or 0), cfg,
            )
            max_us_qty = int(cfg.get("max_us_qty", 5))
            if available_usd <= 0:
                qty = max(1, int(cfg.get("max_us_qty", 1)))
                qty_reason = "잔고조회실패→폴백"
            else:
                qty = max(1, min(qty_raw, max_us_qty)) if qty_raw > 0 else 1
            result   = place_order_us(uid, cfg, code, "buy", qty, 0)
            order_no = result.get("output", {}).get("ODNO", "N/A")
            sname    = _stock_name("", code, "US")
            register_buy(uid, "US", code, current, qty,
                         cfg.get("stop_loss_ratio", 0.03),
                         target_sell_price=prices["sell_price"],
                         source="자동_US", stock_name=sname,
                         stop_loss_price=float(prices.get("stop_loss") or 0))
            add_trade(uid, "US", code, "buy", current, qty, "자동매수_US", stock_name=sname)
            _add_log(uid, "INFO",
                     f"[US][{code}] 매수 | {qty}주@${current:.2f} "
                     f"목표=${prices['sell_price']:.2f} 손절=${prices['stop_loss']:.2f} "
                     f"점수={score_result['score']} 주문={order_no} 사이징({qty_reason})")
        except Exception as e:
            _add_log(uid, "ERROR", f"[US][{code}] 매수 체크 오류: {e}")


# ══════════════════════════════════════════════════════════
# 스케줄 Functions (모든 유저 순회)
# ══════════════════════════════════════════════════════════

def _get_all_users() -> list[tuple[str, dict]]:
    """설정 완료된 유저 목록 반환 [(uid, cfg), ...].

    활성 모드 프로필에 자격증명이 없으면 다른 모드 프로필로 폴백.
    """
    result = []
    total = 0
    for user_doc in get_db().collection("users").stream():
        uid = user_doc.id
        total += 1
        cfg = get_config(uid)
        if not (cfg.get("app_key") and cfg.get("app_secret") and cfg.get("account_no")):
            # 활성 모드 프로필에 자격증명 없음 → 다른 모드 프로필에서 폴백 시도
            raw = get_config_raw(uid)
            profs = raw.get("profiles") or {}
            active_mode = "mock" if raw.get("is_mock", True) else "live"
            fallback_mode = "live" if active_mode == "mock" else "mock"
            fallback_prof = profs.get(fallback_mode) or {}
            if fallback_prof.get("app_key") and fallback_prof.get("app_secret") and fallback_prof.get("account_no"):
                cfg = {**fallback_prof, "is_mock": raw.get("is_mock", True)}
                for k in ("setup_complete", "display_name", "email", "created_at"):
                    if k in raw:
                        cfg[k] = raw[k]
                logging.warning(
                    f"[_get_all_users] {uid[:8]}… 활성모드={active_mode} 자격증명 없음 "
                    f"→ {fallback_mode} 프로필로 폴백"
                )
            else:
                missing = [k for k in ("app_key", "app_secret", "account_no") if not cfg.get(k)]
                logging.warning(f"[_get_all_users] {uid[:8]}… 자격증명 미완료 {missing} — 스킵")
                continue
        result.append((uid, cfg))
    if not result:
        logging.warning(f"[_get_all_users] 유효 유저 0명 (전체 {total}명 검색) — 전략 사이클 실행 안됨")
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
            # 전날 로그 정리 (오늘 날짜 기준 2일 이전 삭제 → 로그 무한 누적 방지)
            cutoff = datetime.now(KST) - timedelta(days=2)
            old_logs = list(
                _uref(uid).collection("logs")
                .where("timestamp", "<", cutoff)
                .limit(500)
                .stream()
            )
            for doc in old_logs:
                doc.reference.delete()

            invalidate_token(uid)
            get_token(uid, cfg)
            equity = _get_total_equity_kr(uid, cfg)
            update_bot_state(uid, {
                "trading_halted": False, "halt_reason": "",
                "realized_pnl": 0.0,
                "start_equity": equity, "peak_equity": equity,
                "today": date.today().isoformat(),
            })
            _add_log(uid, "INFO", f"[08:50] 준비 완료 | 기준자산={equity:,.0f}원 | 로그초기화완료")
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
    """KR 전략 메인 사이클 — 1분 간격, 평일 09:00~15:20 KST.

    `run_strategy_cycle_kr` 만 호출. 한 사이클 안에서:
      1) 보유 포지션 점검 (백필/본전스탑/분할익절/트레일링/목표·손절/시간청산/물타기)
      2) 진입 게이트 (블랙아웃 → 일간 P&L → KOSPI 레짐) 통과 시 신규 매수 스캔
    예외는 유저 단위로 격리 (한 유저 오류가 다른 유저를 막지 않음).
    """
    now = datetime.now(KST)
    if now.weekday() >= 5: return
    market_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_end = now.replace(hour=15, minute=20, second=0, microsecond=0)
    if not (market_start <= now <= market_end): return
    all_users = _get_all_users()
    for uid, cfg in all_users:
        try:
            run_strategy_cycle_kr(uid, cfg)
        except Exception as e:
            _add_log(uid, "ERROR", f"전략 사이클 오류: {e}")
    if now.minute % 5 == 0:
        logging.info(f"[scheduled_strategy_cycle] heartbeat users={len(all_users)} {now.strftime('%H:%M')}")


@scheduler_fn.on_schedule(
    schedule="20 15 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_close_positions(event: scheduler_fn.ScheduledEvent) -> None:
    """KR 장 마감 직전(15:20 KST) 보유 포지션 전량 청산.

    당일 결제 vs 익일 결제 리스크와 익일 갭 리스크를 회피하기 위해 전량 시장가 매도.
    스윙 모드 운영을 원할 경우 이 함수의 스케줄을 끄거나 건너뛰는 분기 추가가 필요.
    각 매도는 _log_sell_fill 로 슬리피지 기록.
    """
    for uid, cfg in _get_all_users():
        positions = get_positions(uid, "KR")
        for code, pos in list(positions.items()):
            try:
                data = get_current_price_kr(uid, cfg, code)
                ohlcv_c = get_daily_ohlcv_kr(uid, cfg, code)
                current = _kr_price_from_api_data(data, ohlcv_c)
                if current <= 0:
                    _add_log(uid, "WARNING", f"[{code}] 장마감 청산: 현재가 0 — 건너뜀")
                    continue
                qty = pos["quantity"]
                res = place_order_kr(uid, cfg, code, "sell", qty, 0)
                order_no = (res.get("output") or {}).get("ODNO", "N/A")
                pnl = register_sell(uid, "KR", code, current)
                add_trade(uid, "KR", code, "sell", current, qty, "장마감_청산", pnl, stock_name=pos.get("stock_name", ""))
                state = get_bot_state(uid)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO",
                         f"[KR][{code}] 장마감 청산 | {qty}주@{current:,} "
                         f"PnL≈{pnl:,.0f}원 주문={order_no}")
                _log_sell_fill(uid, "KR", code, "sell", order_no, float(current), qty, cfg)
                _invalidate_balance_cache(uid)
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
    schedule="*/30 9-15 * * 1-5", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_reconcile_kr(event: scheduler_fn.ScheduledEvent) -> None:
    """KR 포지션 reconcile — 평일 KST 09:15~15:15, 30분 간격.

    `reconcile_positions(uid, cfg, "KR")` 호출.
    개장 직후 09:00은 잔고 갱신이 안정화되지 않을 수 있어 09:15부터,
    마감 청산 함수(15:20)와의 충돌을 피해 15:15까지로 제한.

    cfg["reconcile_enabled"]=False 인 유저는 스킵.
    상세 동작은 `reconcile_positions` 의 docstring 참조.
    """
    now = datetime.now(KST)
    if now.weekday() >= 5:
        return
    market_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_end = now.replace(hour=15, minute=15, second=0, microsecond=0)
    if not (market_start <= now <= market_end):
        return
    for uid, cfg in _get_all_users():
        if not cfg.get("reconcile_enabled", True):
            continue
        try:
            reconcile_positions(uid, cfg, "KR")
        except Exception as e:
            _add_log(uid, "ERROR", f"[reconcile][KR] 사이클 오류: {e}")


@scheduler_fn.on_schedule(
    schedule="*/30 9-16 * * 1-5", timezone=scheduler_fn.Timezone("America/New_York"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_reconcile_us(event: scheduler_fn.ScheduledEvent) -> None:
    """US 포지션 reconcile — 평일 ET 09:45~15:45, 30분 간격.

    개장 09:30 직후는 KIS 잔고 동기화 지연 가능성을 고려해 09:45부터,
    마감 청산(15:50)과의 충돌을 피해 15:45까지.
    상세는 `reconcile_positions` docstring 참조.
    """
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return
    market_start = now.replace(hour=9, minute=45, second=0, microsecond=0)
    market_end = now.replace(hour=15, minute=45, second=0, microsecond=0)
    if not (market_start <= now <= market_end):
        return
    for uid, cfg in _get_all_users():
        if not cfg.get("reconcile_enabled", True):
            continue
        try:
            reconcile_positions(uid, cfg, "US")
        except Exception as e:
            _add_log(uid, "ERROR", f"[reconcile][US] 사이클 오류: {e}")


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
                res = place_order_us(uid, cfg, code, "sell", qty, 0)
                order_no = (res.get("output") or {}).get("ODNO", "N/A")
                pnl = register_sell(uid, "US", code, current)
                add_trade(uid, "US", code, "sell", current, qty, "US장마감_청산", pnl, stock_name=pos.get("stock_name", ""))
                state = get_bot_state(uid)
                update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO",
                         f"[US][{code}] 마감 청산 | ${current:.2f} | PnL ${pnl:+.2f} 주문={order_no}")
                _log_sell_fill(uid, "US", code, "sell", order_no, float(current), qty, cfg)
                _invalidate_balance_cache(uid)
            except Exception as e:
                _add_log(uid, "ERROR", f"[US][{code}] 마감 청산 오류: {e}")


# ══════════════════════════════════════════════════════════
# 텔레그램 모니터링 리포트 (매 시 정각)
# ══════════════════════════════════════════════════════════

@scheduler_fn.on_schedule(
    schedule="0 * * * *", timezone=scheduler_fn.Timezone("Asia/Seoul"),
    memory=options.MemoryOption.MB_256,
)
def scheduled_telegram_monitoring(event: scheduler_fn.ScheduledEvent) -> None:
    """매 시 정각 — 시스템 상태 요약을 Telegram으로 발송."""
    now_kst = datetime.now(KST)
    now_et  = now_kst.astimezone(ET)
    is_weekend = now_kst.weekday() >= 5

    kr_open = _is_kr_market_open()
    us_open = _is_us_market_open()

    if is_weekend:
        market_line = "📅 주말 — KR·US 휴장"
    else:
        parts = []
        if kr_open:
            parts.append("🟢 KR 장중")
        else:
            parts.append("⚪ KR 장외")
        if us_open:
            parts.append("🟢 US 장중")
        else:
            parts.append("⚪ US 장외")
        market_line = " | ".join(parts)

    user_lines = []
    for uid, cfg in _get_all_users():
        try:
            state = get_bot_state(uid)
            bot_on = "ON" if state.get("bot_enabled", True) else "OFF"
            halted = " ⚠️정지" if state.get("trading_halted") else ""
            mock   = " [모의]" if cfg.get("is_mock", True) else " [실전]"
            pnl    = state.get("realized_pnl", 0)
            pnl_str = f"{pnl:+,.0f}원" if pnl else "0원"
            user_lines.append(f"  • {uid[:8]}… 봇:{bot_on}{halted}{mock} | 실현손익:{pnl_str}")
        except Exception:
            user_lines.append(f"  • {uid[:8]}… 상태조회 실패")

    users_block = "\n".join(user_lines) if user_lines else "  (등록 유저 없음)"

    text = (
        f"<b>[AutoStock 모니터링]</b> "
        f"{now_kst.strftime('%H:%M')} KST / {now_et.strftime('%H:%M')} ET\n"
        f"\n"
        f"🖥 Firebase API: 정상 (Cloud Functions 내부 실행)\n"
        f"{market_line}\n"
        f"\n"
        f"<b>유저 상태</b>\n{users_block}\n"
        f"\n"
        f"⏰ 다음 체크: 1시간 후"
    )
    _send_telegram(text)


# ══════════════════════════════════════════════════════════
# Flask HTTP API Routes
# ══════════════════════════════════════════════════════════

def _ts_to_str(ts) -> str:
    """Firestore·datetime → 한국 시간 문자열 (API·JSON 응답용)."""
    if ts is None:
        return ""
    dt: datetime | None = None
    if isinstance(ts, datetime):
        dt = ts
    elif hasattr(ts, "seconds") and not isinstance(ts, datetime):
        try:
            nanos = int(getattr(ts, "nanoseconds", 0) or 0)
            sec = float(ts.seconds) + nanos / 1e9
            dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        except Exception:
            return str(ts)
    if dt is None:
        return str(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # +09:00 포함 — JSON·JS Date 파싱이 브라우저마다 일관되도록
    return dt.astimezone(KST).isoformat(timespec="seconds")


def _require_auth():
    """토큰 검증 → uid 반환. 실패 시 Flask response tuple 반환."""
    try:
        return verify_token(request), None
    except PermissionError as e:
        return None, (jsonify({"ok": False, "error": str(e)}), 401)
    except Exception as e:
        return None, (jsonify({"ok": False, "error": f"인증 오류: {e}"}), 401)


def _compute_risk_gates(uid: str, cfg: dict, state: dict) -> dict:
    """UI 상단 리스크-게이트 상태 배너용 요약.

    전략 사이클과 동일한 게이트 함수를 호출해 "왜 지금 신규매수가 가능/불가능한가"를
    사용자가 한눈에 볼 수 있게 한다. 모든 필드는 표시 목적이며, 매매 결정에는 직접
    참여하지 않는다 (실제 게이트 판정은 여전히 run_strategy_cycle_* 안에서 이뤄짐).

    지수 API는 `_get_kr_index_change_pct` / `_get_us_index_change_pct` 의 60초 캐시를
    재사용하므로 status 폴링으로 추가 부담이 크지 않다. 장외 시간엔 아예 호출하지 않는다.
    """
    now_kst = datetime.now(KST)
    now_et = datetime.now(ET)

    kr_open = now_kst.replace(hour=9, minute=0, second=0, microsecond=0)
    kr_close = now_kst.replace(hour=15, minute=30, second=0, microsecond=0)
    kr_is_weekday = now_kst.weekday() < 5
    kr_open_now = kr_is_weekday and kr_open <= now_kst <= kr_close
    kr_remain_sec = int((kr_close - now_kst).total_seconds()) if kr_open_now else 0
    try:
        kr_win_ok, kr_win_reason = _kr_buy_window_ok(cfg)
    except Exception:
        kr_win_ok, kr_win_reason = True, ""

    us_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    us_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    us_is_weekday = now_et.weekday() < 5
    us_open_now = us_is_weekday and us_open <= now_et <= us_close
    us_remain_sec = int((us_close - now_et).total_seconds()) if us_open_now else 0
    try:
        us_win_ok, us_win_reason = _us_buy_window_ok(cfg)
    except Exception:
        us_win_ok, us_win_reason = True, ""

    try:
        pnl_ok, pnl_reason = _daily_pnl_buy_gate(state, cfg)
    except Exception:
        pnl_ok, pnl_reason = True, ""
    start_eq = float(state.get("start_equity", 0) or 0)
    pnl_abs = float(state.get("realized_pnl", 0) or 0)
    pnl_ratio = (pnl_abs / start_eq * 100) if start_eq > 0 else 0.0

    kospi_chg: float | None = None
    kospi_gate_ok = True
    if kr_open_now:
        try:
            kospi_chg = float(_get_kr_index_change_pct(uid, cfg, "0001"))
            lim = abs(float(cfg.get("kr_index_drop_limit_pct", 1.5)))
            kospi_gate_ok = (kospi_chg > -lim) if lim > 0 else True
        except Exception:
            kospi_chg = None

    spy_chg: float | None = None
    spy_gate_ok = True
    if us_open_now:
        try:
            spy_chg = float(_get_us_index_change_pct(uid, cfg, cfg.get("us_index_proxy", "SPY")))
            lim = abs(float(cfg.get("us_index_drop_limit_pct", 1.5)))
            spy_gate_ok = (spy_chg > -lim) if lim > 0 else True
        except Exception:
            spy_chg = None

    # peak_equity 대비 현재 드로우다운 % (total_equity는 호출 측에서 알고 있지만, 백엔드에선 state만 사용)
    peak_eq = float(state.get("peak_equity", 0) or 0)

    return {
        "now_kst": now_kst.strftime("%H:%M:%S"),
        "now_et": now_et.strftime("%H:%M:%S"),
        "kr": {
            "market_open": kr_open_now,
            "remain_sec": max(0, kr_remain_sec),
            "buy_window_ok": bool(kr_win_ok),
            "buy_window_reason": kr_win_reason or "",
            "index_change_pct": kospi_chg,
            "index_gate_ok": bool(kospi_gate_ok),
            "index_limit_pct": float(cfg.get("kr_index_drop_limit_pct", 1.5)),
        },
        "us": {
            "market_open": us_open_now,
            "remain_sec": max(0, us_remain_sec),
            "buy_window_ok": bool(us_win_ok),
            "buy_window_reason": us_win_reason or "",
            "index_change_pct": spy_chg,
            "index_gate_ok": bool(spy_gate_ok),
            "index_limit_pct": float(cfg.get("us_index_drop_limit_pct", 1.5)),
            "index_proxy": cfg.get("us_index_proxy", "SPY"),
        },
        "daily_pnl": {
            "ok": bool(pnl_ok),
            "reason": pnl_reason or "",
            "ratio_pct": round(pnl_ratio, 2),
            "target_pct": round(float(cfg.get("daily_profit_target", 0.03)) * 100, 2),
            "loss_limit_pct": round(abs(float(cfg.get("daily_loss_limit", 0.02))) * 100, 2),
            "realized_pnl": pnl_abs,
            "start_equity": start_eq,
            "peak_equity": peak_eq,
        },
        "halt": {
            "halted": bool(state.get("trading_halted", False)),
            "reason": state.get("halt_reason", "") or "",
            "bot_enabled": bool(state.get("bot_enabled", True)),
        },
    }


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
    is_m = body.get("is_mock", True)
    if isinstance(is_m, str):
        is_m = str(is_m).lower() not in ("false", "0", "no", "")
    cfg_flat = {
        "app_key": body["app_key"].strip(),
        "app_secret": body["app_secret"].strip(),
        "account_no": body["account_no"].strip(),
        "is_mock": is_m,
        "kr_watchlist": body.get("kr_watchlist", ["005930", "000660", "035420"]),
        "us_watchlist": body.get("us_watchlist", ["AAPL", "NVDA", "TSLA"]),
        "k_factor": float(body.get("k_factor", 0.5)),
        "max_entry_slip_pct": float(body.get("max_entry_slip_pct", 0.05)),
        "max_entry_slip_pct_mock": float(body.get("max_entry_slip_pct_mock", 0.05)),
        "max_entry_slip_pct_live": float(body.get("max_entry_slip_pct_live", 0.03)),
        "ma_period": int(body.get("ma_period", 5)),
        "min_score_kr": int(body.get("min_score_kr", 40)),
        "stop_loss_ratio": float(body.get("stop_loss_ratio", 0.03)),
        "max_position_ratio": float(body.get("max_position_ratio", 0.10)),
        "daily_profit_target": float(body.get("daily_profit_target", 0.03)),
        "ai_stock_count": int(body.get("ai_stock_count", 3)),
        "bot_enabled": True,
        "display_name": body.get("display_name", ""),
        "email": body.get("email", ""),
        "setup_complete": True,
        "created_at": datetime.now(KST).isoformat(),
    }
    prof = {k: cfg_flat[k] for k in _CONFIG_PROFILE_KEYS if k in cfg_flat}
    mode = "mock" if is_m else "live"
    other = "live" if mode == "mock" else "mock"
    # 두 프로필 모두 자격증명 포함: 모드 전환 시 _get_all_users 드롭 방지
    seed_other = dict(prof)
    doc_out = {
        "is_mock": is_m,
        "setup_complete": True,
        "display_name": cfg_flat.get("display_name", ""),
        "email": cfg_flat.get("email", ""),
        "created_at": cfg_flat.get("created_at", ""),
        "profiles": {
            mode: prof,
            other: seed_other,
        },
    }
    _uref(uid).collection("config").document("settings").set(doc_out)
    _ensure_user_root_doc(uid)
    update_bot_state(uid, {"bot_enabled": True, "trading_halted": False,
                            "is_market_open": False, "realized_pnl": 0.0})
    _add_log(uid, "INFO", f"계정 설정 완료 | 모드={'모의' if is_m else '실전'} (프로필 분리 저장)")
    return jsonify({"ok": True, "message": "설정 완료"})


@flask_app.route("/api/status")
def route_status():
    uid, err = _require_auth()
    if err: return err
    try:
        cfg = get_config(uid)
        raw_cfg = get_config_raw(uid)
        profiles_payload = _profiles_for_client_payload(raw_cfg)
        if not cfg.get("setup_complete"):
            return jsonify({
                "ok": True,
                "setup_required": True,
                "profiles": profiles_payload,
            })

        # users/{uid} 루트가 없으면 collection("users").stream() 에 0명 → 스케줄 전부 스킵
        _ensure_user_root_doc(uid)

        state = get_bot_state(uid)
        positions_kr = get_positions(uid, "KR")
        positions_us = get_positions(uid, "US")

        balance_data: dict[str, Any] = {}
        balance_prices_kr: dict[str, int] = {}
        kis_error: str = ""
        try:
            bal = _cached_balance(uid, cfg)
            balance_prices_kr = _kr_holdings_prpr_by_code(bal)
            summary = bal.get("output2", [{}])
            if summary:
                s = summary[0]
                # 주문가능: inquire-balance 의 prvs_rcdl_excc_amt 는 모의투자에서 매수 후에도
                # 초기 예수금처럼 보이는 경우가 있어, 매수가능조회(8908R) ord_psbl_cash 를 사용
                try:
                    ord_psbl = get_available_cash_kr(uid, cfg, "005930")
                except Exception:
                    ord_psbl = int(str(s.get("dnca_tot_amt", "0")).replace(",", "") or 0)
                balance_data = {
                    "total_equity": s.get("tot_evlu_amt", "0").replace(",", ""),
                    "available_cash": str(ord_psbl),
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
                        out = data.get("output") or {}
                        if not isinstance(out, dict):
                            out = {}
                        ohlcv_kr = _cached_ohlcv(uid, cfg, code, "KR")
                        current = _kr_price_from_output(out, ohlcv_kr)
                        if current <= 0:
                            current = balance_prices_kr.get(code, 0)
                        sname = _stock_name(out.get("hts_kor_isnm", ""), code, "KR")
                        change_rate = out.get("prdy_ctrt", "0")
                    else:
                        data = _cached_price(uid, cfg, code, "US")
                        out = data["output"]
                        current = _us_price_from_output(out)
                        sname = _stock_name("", code, "US")
                        change_rate = out.get("diff", "0")
                    bp = float(pos.get("buy_price") or 0)
                    qty = int(pos.get("quantity") or 0)
                    pnl = (current - bp) * qty
                    pnl_ratio = ((current - bp) / bp * 100) if bp else 0.0
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
        for raw_code in cfg.get("kr_watchlist", []):
            code = _normalize_kr_stock_code(raw_code)
            if code in positions_kr_detail:
                p = positions_kr_detail[code]
                wl_entry: dict[str, Any] = {
                    "current_price": p.get("current_price", 0),
                    "stock_name": p.get("stock_name", code),
                    "change_rate": p.get("change_rate", "0"),
                }
                try:
                    ohlcv_p = _cached_ohlcv(uid, cfg, code, "KR")
                    if len(ohlcv_p) >= 2:
                        today_open = float(str(ohlcv_p[0].get("stck_oprc", 0)).replace(",", "") or 0)
                        prev_h = float(str(ohlcv_p[1].get("stck_hgpr", 0)).replace(",", "") or 0)
                        prev_l = float(str(ohlcv_p[1].get("stck_lwpr", 0)).replace(",", "") or 0)
                        wl_entry["target_breakout"] = int(
                            today_open + cfg.get("k_factor", 0.5) * (prev_h - prev_l)
                        )
                    if len(ohlcv_p) >= 5:
                        wl_entry["ma5"] = int(
                            sum(float(str(r.get("stck_clpr", 0)).replace(",", "") or 0) for r in ohlcv_p[:5]) / 5
                        )
                    wl_entry["closes"] = _ensure_sparkline_closes_kr(
                        _kr_closes_from_ohlcv(ohlcv_p),
                        int(p.get("current_price") or 0),
                        int(p.get("buy_price") or 0) or None,
                    )
                except Exception:
                    wl_entry["closes"] = _ensure_sparkline_closes_kr(
                        [],
                        int(p.get("current_price") or 0),
                        int(p.get("buy_price") or 0) or None,
                    )
                watchlist_data[code] = wl_entry
            else:
                try:
                    data = _cached_price(uid, cfg, code, "KR")
                    out = data.get("output") or {}
                    if not isinstance(out, dict):
                        out = {}
                    ohlcv = _cached_ohlcv(uid, cfg, code, "KR")
                    cur_wl = _kr_price_from_output(out, ohlcv)
                    if cur_wl <= 0:
                        cur_wl = balance_prices_kr.get(code, 0)
                    entry: dict[str, Any] = {
                        "current_price": cur_wl,
                        "stock_name": _stock_name(out.get("hts_kor_isnm", ""), code, "KR"),
                        "change_rate": out.get("prdy_ctrt", "0"),
                    }
                    if len(ohlcv) >= 2:
                        today_open = float(str(ohlcv[0].get("stck_oprc", 0)).replace(",", "") or 0)
                        prev_h = float(str(ohlcv[1].get("stck_hgpr", 0)).replace(",", "") or 0)
                        prev_l = float(str(ohlcv[1].get("stck_lwpr", 0)).replace(",", "") or 0)
                        entry["target_breakout"] = int(today_open + cfg.get("k_factor", 0.5) * (prev_h - prev_l))
                    if len(ohlcv) >= 5:
                        entry["ma5"] = int(
                            sum(float(str(r.get("stck_clpr", 0)).replace(",", "") or 0) for r in ohlcv[:5]) / 5
                        )
                    entry["closes"] = _ensure_sparkline_closes_kr(
                        _kr_closes_from_ohlcv(ohlcv),
                        int(cur_wl),
                        None,
                    )
                    watchlist_data[code] = entry
                except Exception:
                    watchlist_data[code] = {"current_price": 0, "stock_name": _stock_name("", code, "KR"), "change_rate": "0"}

        # 미국 감시 종목 데이터
        us_watchlist_data: dict[str, Any] = {}
        for raw_us in cfg.get("us_watchlist", []):
            code = str(raw_us).strip().upper()
            if code in positions_us_detail:
                p = positions_us_detail[code]
                us_wl: dict[str, Any] = {
                    "current_price": p.get("current_price", 0),
                    "stock_name": p.get("stock_name", code),
                    "change_rate": p.get("change_rate", "0"),
                }
                try:
                    ohlcv_u = _cached_ohlcv(uid, cfg, code, "US")
                    us_wl["closes"] = [
                        round(c, 2)
                        for c in _ensure_sparkline_closes_us(
                            _us_closes_from_ohlcv(ohlcv_u),
                            float(p.get("current_price") or 0),
                            float(p.get("buy_price") or 0) or None,
                        )
                    ]
                except Exception:
                    us_wl["closes"] = [
                        round(c, 2)
                        for c in _ensure_sparkline_closes_us(
                            [],
                            float(p.get("current_price") or 0),
                            float(p.get("buy_price") or 0) or None,
                        )
                    ]
                us_watchlist_data[code] = us_wl
            else:
                try:
                    data = _cached_price(uid, cfg, code, "US")
                    out = data["output"]
                    ohlcv_us = _cached_ohlcv(uid, cfg, code, "US")
                    cur_us = _us_price_from_output(out, ohlcv_us)
                    us_watchlist_data[code] = {
                        "current_price": cur_us,
                        "stock_name": _stock_name("", code, "US"),
                        "change_rate": out.get("diff", "0"),
                        "closes": [
                            round(c, 2)
                            for c in _ensure_sparkline_closes_us(
                                _us_closes_from_ohlcv(ohlcv_us),
                                float(cur_us),
                                None,
                            )
                        ],
                    }
                except Exception:
                    us_watchlist_data[code] = {"current_price": 0, "stock_name": _stock_name("", code, "US"), "change_rate": "0"}

        safe_cfg = {k: v for k, v in cfg.items() if k not in ("app_key", "app_secret")}

        try:
            risk_gates = _compute_risk_gates(uid, cfg, state)
        except Exception as e:
            risk_gates = {"error": str(e)}

        # 최근 매도 5건 (알림 팝업용)
        recent_sells: list[dict] = []
        try:
            sell_docs = (
                _uref(uid).collection("trades")
                .where("side", "==", "sell")
                .order_by("timestamp", direction="DESCENDING")
                .limit(5)
                .stream()
            )
            for d in sell_docs:
                td = d.to_dict()
                recent_sells.append({
                    "id": d.id,
                    "stock_code": td.get("stock_code", ""),
                    "stock_name": td.get("stock_name", ""),
                    "market": td.get("market", "KR"),
                    "price": td.get("price", 0),
                    "qty": td.get("qty", 0),
                    "pnl": td.get("pnl", 0),
                    "strategy": td.get("strategy", ""),
                    "timestamp": _ts_to_str(td.get("timestamp")),
                })
        except Exception:
            pass

        return jsonify({
            "ok": True,
            "state": state, "balance": balance_data,
            "positions_kr": positions_kr_detail, "positions_us": positions_us_detail,
            "watchlist_data": watchlist_data, "us_watchlist_data": us_watchlist_data,
            "config": safe_cfg,
            "profiles": profiles_payload,
            "mode": "모의투자" if cfg.get("is_mock") else "실전투자",
            "updated_at": datetime.now(KST).strftime("%H:%M:%S"),
            "kis_error": kis_error,
            "risk_gates": risk_gates,
            "recent_sells": recent_sells,
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
            ohlcv_o = get_daily_ohlcv_kr(uid, cfg, stock_code)
            current = _kr_price_from_api_data(data, ohlcv_o)
            if current <= 0:
                return jsonify({"ok": False, "error": "현재가를 조회할 수 없습니다. 종목코드와 시장 구분을 확인해 주세요."}), 400
            out_o = data.get("output") or {}
            sname = _stock_name(out_o.get("hts_kor_isnm", ""), stock_code, "KR") if isinstance(out_o, dict) else _stock_name("", stock_code, "KR")
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
                prices_manual_kr = calculate_optimal_prices(current, ohlcv_o, cfg)
                register_buy(
                    uid, "KR", stock_code, current, quantity,
                    cfg.get("stop_loss_ratio", 0.03), float(prices_manual_kr["sell_price"]),
                    "수동", sname,
                    stop_loss_price=float(prices_manual_kr.get("stop_loss") or 0),
                )
                add_trade(uid, "KR", stock_code, "buy", current, quantity, "수동매수", stock_name=sname)
                _add_log(uid, "INFO", f"[수동매수][KR] {stock_code} {quantity}주@{current:,} 주문={order_no}")
                _invalidate_balance_cache(uid)
                return jsonify({
                    "ok": True, "order_no": order_no, "quantity": quantity, "price": current,
                    "stock_name": sname, "note": "시장가 주문 시 아래 가격은 주문 직전 조회 시세이며, 실제 체결가는 거래소 확정입니다.",
                })
            else:
                positions = get_positions(uid, "KR")
                pos = positions.get(stock_code)
                if quantity <= 0:
                    if not pos: return jsonify({"ok": False, "error": "보유 포지션 없음"}), 400
                    quantity = pos["quantity"]
                elif pos and quantity > int(pos["quantity"]):
                    return jsonify({"ok": False, "error": "매도 수량이 보유 수량을 초과합니다"}), 400
                result = place_order_kr(uid, cfg, stock_code, "sell", quantity, int(price))
                order_no = result.get("output", {}).get("ODNO", "N/A")
                if pos:
                    pos_qty = int(pos["quantity"])
                    if quantity < pos_qty:
                        pnl, _ = register_partial_sell(
                            uid, "KR", stock_code, current, quantity, False,
                        )
                    else:
                        pnl = register_sell(uid, "KR", stock_code, current)
                    add_trade(uid, "KR", stock_code, "sell", current, quantity, "수동매도", pnl, stock_name=pos.get("stock_name", sname))
                    state = get_bot_state(uid)
                    update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO", f"[수동매도][KR] {stock_code} {quantity}주@{current:,}")
                _invalidate_balance_cache(uid)
                return jsonify({
                    "ok": True, "order_no": order_no, "quantity": quantity, "price": current,
                    "stock_name": sname,
                })
        else:
            data = get_current_price_us(uid, cfg, stock_code)
            out = data["output"]
            current = float(out.get("last", out.get("stck_prpr", 0)))
            sname = _stock_name("", stock_code, "US")
            if side == "buy":
                if quantity <= 0:
                    return jsonify({"ok": False, "error": "미국 주식은 수량을 직접 입력해주세요"}), 400
                result = place_order_us(uid, cfg, stock_code, "buy", quantity, price)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                ohlcv_us_m = get_daily_ohlcv_us(uid, cfg, stock_code)
                prices_manual_us = calculate_optimal_prices_us(current, ohlcv_us_m, cfg)
                register_buy(
                    uid, "US", stock_code, current, quantity,
                    cfg.get("stop_loss_ratio", 0.03),
                    float(prices_manual_us["sell_price"]), "수동", sname,
                    stop_loss_price=float(prices_manual_us.get("stop_loss") or 0),
                )
                add_trade(uid, "US", stock_code, "buy", current, quantity, "수동매수", stock_name=sname)
                _add_log(uid, "INFO", f"[수동매수][US] {stock_code} {quantity}주@${current:.2f}")
                _invalidate_balance_cache(uid)
                return jsonify({
                    "ok": True, "order_no": order_no, "quantity": quantity, "price": current,
                    "stock_name": sname, "note": "Market order: price shown is quote at request time; fill may differ.",
                })
            else:
                positions = get_positions(uid, "US")
                pos = positions.get(stock_code)
                if quantity <= 0:
                    if not pos: return jsonify({"ok": False, "error": "보유 포지션 없음"}), 400
                    quantity = pos["quantity"]
                elif pos and quantity > int(pos["quantity"]):
                    return jsonify({"ok": False, "error": "매도 수량이 보유 수량을 초과합니다"}), 400
                result = place_order_us(uid, cfg, stock_code, "sell", quantity, price)
                order_no = result.get("output", {}).get("ODNO", "N/A")
                if pos:
                    pos_qty = int(pos["quantity"])
                    if quantity < pos_qty:
                        pnl, _ = register_partial_sell(
                            uid, "US", stock_code, current, quantity, False,
                        )
                    else:
                        pnl = register_sell(uid, "US", stock_code, current)
                    add_trade(uid, "US", stock_code, "sell", current, quantity, "수동매도", pnl, stock_name=pos.get("stock_name", sname))
                    state = get_bot_state(uid)
                    update_bot_state(uid, {"realized_pnl": state.get("realized_pnl", 0) + pnl})
                _add_log(uid, "INFO", f"[수동매도][US] {stock_code} {quantity}주@${current:.2f}")
                _invalidate_balance_cache(uid)
                return jsonify({"ok": True, "order_no": order_no, "quantity": quantity, "price": current, "stock_name": sname})

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
        raw_cfg = get_config_raw(uid)
        profiles_payload = _profiles_for_client_payload(raw_cfg)
        safe = {k: v for k, v in cfg.items() if k not in ("app_key", "app_secret")}
        return jsonify({"ok": True, "config": safe, "profiles": profiles_payload})
    body = request.get_json() or {}
    allowed = {"is_mock", "kr_watchlist", "us_watchlist", "k_factor", "ma_period",
                "stop_loss_ratio", "max_position_ratio", "daily_profit_target",
                "bot_enabled", "ai_stock_count", "min_score_kr", "min_score_us", "min_score_us_ai",
                "ai_afford_one_share", "max_us_qty",
                "partial_tp_enabled", "partial_tp_trigger_pct", "partial_tp_sell_ratio",
                "partial_tp_tighten_stop",
                "avg_down_enabled", "avg_down_trigger_pct", "avg_down_max_times",
                "avg_down_qty_ratio", "avg_down_min_interval_hours",
                "trailing_stop_enabled", "trailing_stop_pct", "trailing_stop_activate_pct",
                "breakeven_stop_enabled", "breakeven_trigger_pct",
                "time_stop_enabled", "time_stop_days", "time_stop_flat_pct",
                "partial_tp_tighten_buffer_pct",
                "kr_sell_ord_dvsn",
                "kr_skip_buy_first_min", "kr_skip_buy_last_min",
                "us_skip_buy_first_min", "us_skip_buy_last_min",
                "daily_loss_limit", "kr_index_drop_limit_pct",
                "us_index_drop_limit_pct", "us_index_proxy",
                "risk_per_trade_pct",
                "reconcile_enabled", "fill_check_enabled",
                "monday_morning_skip_enabled", "monday_morning_skip_min",
                "max_entry_slip_pct", "max_entry_slip_pct_mock", "max_entry_slip_pct_live"}
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


@flask_app.route("/api/research")
def route_research():
    """오늘의 시장 리서치 (일 1회 Firestore 캐시, ?refresh=1 로 강제 재생성)"""
    uid, err = _require_auth()
    if err:
        return err
    market = str(request.args.get("market", "KR")).upper()
    if market not in ("KR", "US"):
        market = "KR"
    force = str(request.args.get("refresh", "")).strip() == "1"
    try:
        # KIS/설정과 무관 — Firebase 로그인만 되면 제공 (캐시는 users/{uid}/cache)
        payload = get_or_create_daily_research(uid, market, force_refresh=force)
        return jsonify({"ok": True, **payload})
    except Exception as e:
        logger.exception("route_research")
        return jsonify({"ok": False, "error": str(e)}), 500


@flask_app.route("/api/quote")
def route_quote():
    """단일 종목 시세·종목명 — 수동 주문 전 현재가 표시용."""
    uid, err = _require_auth()
    if err:
        return err
    code = str(request.args.get("stock_code", "")).strip().upper()
    market = str(request.args.get("market", "KR")).strip().upper()
    if not code:
        return jsonify({"ok": False, "error": "stock_code 필수"}), 400
    if market not in ("KR", "US"):
        market = "KR"
    try:
        cfg = get_config(uid)
        if not cfg.get("setup_complete"):
            return jsonify({"ok": False, "error": "설정을 먼저 완료해주세요"}), 400
        if market == "KR":
            data = get_current_price_kr(uid, cfg, code)
            ohlcv_q = get_daily_ohlcv_kr(uid, cfg, code)
            out = data.get("output") or {}
            if not isinstance(out, dict):
                out = {}
            price = _kr_price_from_output(out, ohlcv_q)
            if price <= 0:
                return jsonify({"ok": False, "error": "시세를 조회할 수 없습니다"}), 400
            name = _stock_name(out.get("hts_kor_isnm", ""), code, "KR")
        else:
            data = get_current_price_us(uid, cfg, code)
            out = data["output"]
            price = float(out.get("last", out.get("stck_prpr", 0)) or 0)
            name = _stock_name("", code, "US")
        return jsonify({
            "ok": True,
            "stock_code": code,
            "market": market,
            "name": name,
            "price": price,
        })
    except ApiError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
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
                if not isinstance(out, dict):
                    out = {}
                current = float(_kr_price_from_output(out, ohlcv))
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
    if session not in ("morning", "afternoon", "late", "manual"):
        return jsonify({"ok": False, "error": "session: morning/afternoon/late/manual"}), 400
    add_buy_count: int | None = None
    raw_add = body.get("add_buy_count")
    if raw_add is not None and raw_add != "":
        try:
            add_buy_count = max(0, min(int(raw_add), 5))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "add_buy_count: 0~5 정수"}), 400
    try:
        cfg = get_config(uid)
        run_ai_session(uid, cfg, session, market, add_buy_count=add_buy_count)
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
    # 기본 60s 초과 시 Cloud Run이 502 반환 — AI 유니버스 수집·Gemini·스코어링은 수 분 걸릴 수 있음
    memory=options.MemoryOption.MB_512,
    timeout_sec=540,
)
def api(req: https_fn.Request) -> https_fn.Response:
    with flask_app.request_context(req.environ):
        return flask_app.full_dispatch_request()
