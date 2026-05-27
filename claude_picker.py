"""
claude_picker.py — Claude Cowork 스케줄러용 국내주식 AI 종목 추천 스크립트

실행 방식: Cowork 스케줄러가 매일 평일 08:40 KST에 자동 실행
동작 흐름:
  1. 전일 거래대금 상위 종목 / 시황 뉴스 WebSearch
  2. Claude가 종목 분석 후 추천 리스트 선정
  3. Firebase /api/ai/picks 에 POST → 공유 ai_picks/{date} 저장
  4. 09:00 장 시작 시 기존 매매 엔진이 해당 종목으로 변동성 돌파 전략 실행

환경변수 (.env):
  AUTOSTOCK_API_URL       Firebase Functions 베이스 URL (예: https://us-central1-xxx.cloudfunctions.net/api)
  AUTOSTOCK_API_SECRET    /api/ai/picks 인증 시크릿 (AI_PICKS_SECRET 와 동일값)
  AUTOSTOCK_MAX_PICKS     추천 종목 수 (기본 10)
"""

import os
import json
import datetime
import requests
from zoneinfo import ZoneInfo
from pathlib import Path

# 프로젝트 루트 .env 자동 로드
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

KST = ZoneInfo("Asia/Seoul")

# ── 설정 ────────────────────────────────────────────────
API_URL    = os.environ.get("AUTOSTOCK_API_URL", "").rstrip("/")
API_SECRET = os.environ.get("AUTOSTOCK_API_SECRET", "")
MAX_PICKS  = int(os.environ.get("AUTOSTOCK_MAX_PICKS", "10"))
MARKET     = "KR"
SESSION    = "morning"

# ── 분석 유니버스: 초기 후보군 (거래대금 상위 + 관심 섹터) ──────────────
# 스케줄러 실행 시 WebSearch로 당일 상위 종목을 동적으로 보완합니다.
BASE_UNIVERSE = [
    "005930",  # 삼성전자
    "000660",  # SK하이닉스
    "035420",  # NAVER
    "035720",  # 카카오
    "051910",  # LG화학
    "006400",  # 삼성SDI
    "005380",  # 현대차
    "000270",  # 기아
    "068270",  # 셀트리온
    "207940",  # 삼성바이오로직스
    "373220",  # LG에너지솔루션
    "003550",  # LG
    "096770",  # SK이노베이션
    "034730",  # SK
    "030200",  # KT
    "017670",  # SK텔레콤
    "032830",  # 삼성생명
    "105560",  # KB금융
    "055550",  # 신한지주
    "086790",  # 하나금융지주
]


def today_kst() -> str:
    return datetime.datetime.now(KST).strftime("%Y-%m-%d")


def search_market_info() -> str:
    """
    WebSearch 결과를 프롬프트용 텍스트로 반환.
    실제 실행 시 Claude가 WebSearch 도구를 직접 호출합니다.
    이 함수는 검색 키워드만 정의합니다.
    """
    date_str = today_kst()
    return f"""
오늘({date_str}) 한국 주식시장 분석을 위해 다음 정보를 검색하세요:
1. 오늘 KOSPI/KOSDAQ 시황 전망 및 주요 이슈
2. 전일 거래대금 상위 20개 종목
3. 오늘 주목할 테마주 및 섹터 (반도체, 2차전지, AI, 바이오 등)
4. 외국인/기관 순매수 상위 종목
5. 오늘 실적 발표 예정 또는 주요 공시 종목
"""


# ── 메인 프롬프트 ─────────────────────────────────────────────────────────
ANALYSIS_PROMPT = """
당신은 한국 주식시장 단기 모멘텀 전문가입니다.

[전략 배경]
- 래리 윌리엄스 변동성 돌파 전략: 당일 시가 + K × (전일 고가 - 전일 저가) 를 목표가로 설정
- 목표가 돌파 + 5일 이동평균선 위 종목 매수 → 당일 장 마감 전 청산
- 따라서 "당일 변동성이 살아있고, 수급이 좋은 종목"이 핵심

[선정 기준 — 우선순위 순]
1. 거래대금/거래량: 수급 집중 여부 (전일 거래대금 상위)
2. 모멘텀: 상승 추세 또는 박스권 돌파 시도 중인 종목
3. 촉매: 테마 편승 가능성 (AI, 반도체, 2차전지, 바이오 등 당일 핫 섹터)
4. 변동성: 일중 변동폭이 2~5% 수준 (너무 좁으면 돌파 없음, 너무 넓으면 리스크)

[검색 결과를 바탕으로]
위에서 수집한 시황 정보를 분석하여 오늘 변동성 돌파 전략에 적합한 종목 {max_picks}개를 선정하세요.

[출력 형식 — 반드시 이 JSON만 출력, 마크다운 없음]
{{
  "candidates": [
    {{"code": "6자리 종목코드", "reason": "한국어 1~2문장, 수급/모멘텀 근거"}},
    ...
  ]
}}

허용 종목코드 기준: 6자리 숫자 (예: 005930)
최대 {max_picks}개, 확신 순으로 정렬
""".format(max_picks=MAX_PICKS)


def push_picks_to_server(candidates: list[dict]) -> bool:
    """
    Firebase /api/ai/picks 엔드포인트에 추천 결과 전송.
    성공 시 True 반환.
    """
    if not API_URL:
        print("[ERROR] AUTOSTOCK_API_URL 환경변수가 설정되지 않았습니다.")
        return False

    url = f"{API_URL}/api/ai/picks"
    headers = {
        "Content-Type": "application/json",
        "X-AI-Picks-Secret": API_SECRET,
    }
    payload = {
        "provider": "claude",
        "market":   MARKET,
        "session":  SESSION,
        "candidates": candidates,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok"):
            print(f"[OK] {data.get('count')}개 종목 저장 완료 → {data.get('date_key')}")
            print(f"     종목: {', '.join(data.get('candidates', []))}")
            return True
        else:
            print(f"[ERROR] 서버 응답 오류: {data}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API 전송 실패: {e}")
        return False


def validate_candidates(raw_candidates: list) -> list[dict]:
    """JSON 파싱 결과에서 유효한 종목만 필터링."""
    result = []
    seen = set()
    for item in raw_candidates:
        if not isinstance(item, dict):
            continue
        code   = str(item.get("code", "")).strip()
        reason = str(item.get("reason", "")).strip()
        # 6자리 숫자 형식 검증
        if not code.isdigit():
            continue
        code = code.zfill(6)
        if len(code) != 6:
            continue
        if code in seen:
            continue
        seen.add(code)
        result.append({"code": code, "reason": reason or "Claude 추천"})
    return result[:MAX_PICKS]


# ── 스케줄러 진입점 ──────────────────────────────────────────────────────
# Cowork 스케줄러는 이 파일을 실행하며, Claude가 아래 지시를 따릅니다.

"""
[Cowork 스케줄러 실행 지시]

다음 단계를 순서대로 실행하세요:

STEP 1: 시황 조사
아래 키워드로 WebSearch를 실행하세요:
- "{today} 코스피 코스닥 시황 전망"
- "{today} 거래대금 상위 종목"
- "{today} 테마주 반도체 2차전지 AI"
- "{today} 외국인 기관 순매수"
(today = 오늘 날짜 KST 기준)

STEP 2: 종목 분석 및 선정
수집한 정보를 바탕으로 ANALYSIS_PROMPT 기준에 따라 종목 {max_picks}개를 선정하세요.
출력은 반드시 JSON 형식:
{{"candidates": [{{"code": "...", "reason": "..."}}]}}

STEP 3: 서버 전송
선정된 종목을 push_picks_to_server() 함수를 통해 Firebase에 전송하세요.
환경변수 AUTOSTOCK_API_URL, AUTOSTOCK_API_SECRET 을 사용합니다.

STEP 4: 결과 확인
전송 성공 시 "✅ [날짜] Claude 종목 추천 완료: 종목코드1, 종목코드2, ..." 를 출력하세요.
실패 시 에러 내용을 출력하고 재시도하세요.
""".format(max_picks=MAX_PICKS)
