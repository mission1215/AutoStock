"""
KIS WebSocket 실시간 체결가 데몬
==================================
Firebase Cloud Functions는 stateless(무상태)이므로 WebSocket 연결을 유지할 수 없습니다.
이 스크립트를 별도 프로세스로 실행하면 KIS WebSocket에서 실시간 체결가를 수신하여
Firestore `realtime_prices/{code}` 에 기록합니다.

main.py의 _cached_price()는 이 데이터를 15초 이내면 REST API 대신 사용합니다.

사용법:
  # 의존성 설치
  pip install websocket-client firebase-admin python-dotenv

  # 실행 (functions/.env 자동 로드)
  python functions/kis_ws.py

  # 백그라운드 실행 (nohup)
  nohup python functions/kis_ws.py >> logs/ws.log 2>&1 &

  # 종료
  kill $(cat /tmp/kis_ws.pid)

환경변수 (functions/.env):
  KIS_APP_KEY, KIS_APP_SECRET, KIS_ACCOUNT_NO, KIS_IS_MOCK, WATCHLIST
  FIREBASE_SA_KEY  — 서비스 어카운트 JSON 경로 (없으면 ADC 사용)
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# ── 로깅 설정 ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("kis_ws")

# ── .env 로드 (python-dotenv 없어도 동작) ────────────────────────────
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_path)
        logger.info(".env 로드 완료")
    except ImportError:
        # python-dotenv 없을 때 직접 파싱
        for line in _env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())
        logger.info(".env 수동 로드 완료")

# ── 의존성 체크 ───────────────────────────────────────────────────────
try:
    import websocket
except ImportError:
    logger.error("websocket-client 미설치: pip install websocket-client")
    sys.exit(1)

try:
    import requests
except ImportError:
    logger.error("requests 미설치: pip install requests")
    sys.exit(1)

try:
    import firebase_admin
    from firebase_admin import credentials, firestore as fs_admin
except ImportError:
    logger.error("firebase-admin 미설치: pip install firebase-admin")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════
# 설정
# ══════════════════════════════════════════════════════════════════════

APP_KEY    = os.environ.get("KIS_APP_KEY", "")
APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
IS_MOCK    = os.environ.get("KIS_IS_MOCK", "true").lower() == "true"
WATCHLIST  = [c.strip() for c in os.environ.get("WATCHLIST", "").split(",") if c.strip()]

# KIS WebSocket 엔드포인트
WS_URL_REAL = "ws://ops.koreainvestment.com:21000"
WS_URL_MOCK = "ws://ops.koreainvestment.com:31000"
WS_URL      = WS_URL_MOCK if IS_MOCK else WS_URL_REAL

# REST API
API_BASE_REAL = "https://openapi.koreainvestment.com:9443"
API_BASE_MOCK = "https://openapivts.koreainvestment.com:29443"
API_BASE      = API_BASE_MOCK if IS_MOCK else API_BASE_REAL

# TR ID — H0STCNT0: 실시간 체결가 (모의/실전 공통)
TR_ID_PRICE = "H0STCNT0"

# Firestore 컬렉션
RT_COLLECTION = "realtime_prices"

# 재연결 대기
RECONNECT_DELAY = 5   # 초
MAX_RECONNECTS  = 999 # 사실상 무한 재연결

# PID 파일
PID_FILE = "/tmp/kis_ws.pid"


# ══════════════════════════════════════════════════════════════════════
# Firebase 초기화
# ══════════════════════════════════════════════════════════════════════

def _init_firebase() -> "fs_admin.Client":
    if not firebase_admin._apps:
        sa_path = os.environ.get("FIREBASE_SA_KEY", "")
        if sa_path and Path(sa_path).exists():
            cred = credentials.Certificate(sa_path)
            firebase_admin.initialize_app(cred)
            logger.info(f"Firebase 초기화 (서비스 어카운트: {sa_path})")
        else:
            # Application Default Credentials (gcloud auth application-default login)
            firebase_admin.initialize_app()
            logger.info("Firebase 초기화 (ADC)")
    return fs_admin.client()


# ══════════════════════════════════════════════════════════════════════
# KIS WebSocket 승인키 발급
# ══════════════════════════════════════════════════════════════════════

def _get_ws_approval_key() -> str:
    """KIS WebSocket 접속 승인키 발급.

    Returns
    -------
    str
        approval_key (WebSocket 연결 시 헤더에 사용)
    """
    url  = API_BASE + "/oauth2/Approval"
    body = {
        "grant_type": "client_credentials",
        "appkey":     APP_KEY,
        "secretkey":  APP_SECRET,
    }
    resp = requests.post(url, json=body, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    key  = data.get("approval_key", "")
    if not key:
        raise ValueError(f"승인키 발급 실패: {data}")
    logger.info(f"WebSocket 승인키 발급 완료 ({key[:12]}...)")
    return key


# ══════════════════════════════════════════════════════════════════════
# KIS WebSocket 구독 메시지
# ══════════════════════════════════════════════════════════════════════

def _sub_msg(approval_key: str, code: str, tr_type: str = "1") -> str:
    """종목 구독 요청 메시지 생성.

    Parameters
    ----------
    approval_key : WebSocket 승인키
    code         : 종목코드 (예: "005930")
    tr_type      : "1"=구독, "2"=해제
    """
    return json.dumps({
        "header": {
            "approval_key": approval_key,
            "custtype":     "P",
            "tr_type":      tr_type,
            "content-type": "utf-8",
        },
        "body": {
            "input": {
                "tr_id":     TR_ID_PRICE,
                "tr_key":    code,
            }
        }
    })


# ══════════════════════════════════════════════════════════════════════
# 체결 메시지 파싱
# ══════════════════════════════════════════════════════════════════════

def _parse_realtime(raw: str) -> dict | None:
    """H0STCNT0 실시간 체결 데이터 파싱.

    KIS WebSocket 응답 형식:
      0|H0STCNT0|004|005930^152345^75100^...

    필드 순서 (주요 필드만):
      0: 종목코드, 1: 체결시간(HHMMSS), 2: 현재가, 3: 전일대비부호,
      4: 전일대비, 5: 전일대비율, 12: 누적거래량, ...
    """
    try:
        if raw.startswith("{"):
            # JSON 형식 — 구독 응답 등 제어 메시지
            obj = json.loads(raw)
            body = obj.get("body", {})
            rt_cd = body.get("rt_cd", "")
            if rt_cd == "0":
                msg = body.get("msg1", "")
                logger.info(f"[WS] 제어 메시지: {msg}")
            return None

        parts = raw.split("|")
        if len(parts) < 4:
            return None

        tr_id = parts[1]
        if tr_id != TR_ID_PRICE:
            return None

        fields = parts[3].split("^")
        if len(fields) < 13:
            return None

        code   = fields[0].strip()
        price  = int(fields[2])   # 현재가
        volume = int(fields[12])  # 누적거래량

        if price <= 0:
            return None

        return {"code": code, "price": price, "volume": volume}
    except Exception as e:
        logger.debug(f"파싱 오류: {e} | raw={raw[:80]}")
        return None


# ══════════════════════════════════════════════════════════════════════
# WebSocket 데몬 클래스
# ══════════════════════════════════════════════════════════════════════

class KisWebSocketDaemon:
    """KIS 실시간 체결가 WebSocket 데몬.

    - WATCHLIST 종목들을 구독하여 Firestore realtime_prices/{code} 에 기록
    - 연결 끊김 시 자동 재연결 (최대 MAX_RECONNECTS회)
    - SIGTERM/SIGINT 수신 시 안전하게 종료

    Firestore 기록 형식:
        {
            "price":     75100,
            "volume":    1234567,
            "timestamp": <Firestore ServerTimestamp>,
            "code":      "005930",
        }
    """

    def __init__(self, watchlist: list[str]):
        self.watchlist     = watchlist
        self.db            = _init_firebase()
        self.approval_key  = ""
        self._ws: websocket.WebSocketApp | None = None
        self._stop_event   = threading.Event()
        self._reconnects   = 0
        self._write_count  = 0
        self._last_stat_ts = time.time()

        # 5초마다 통계 로그
        self._stat_timer   = threading.Timer(30, self._log_stats)
        self._stat_timer.daemon = True
        self._stat_timer.start()

    # ------------------------------------------------------------------
    def run(self):
        """메인 루프 — 재연결 포함."""
        self.approval_key = _get_ws_approval_key()

        while not self._stop_event.is_set() and self._reconnects < MAX_RECONNECTS:
            try:
                logger.info(f"[WS] 연결 시도 #{self._reconnects + 1} → {WS_URL}")
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open    = self._on_open,
                    on_message = self._on_message,
                    on_error   = self._on_error,
                    on_close   = self._on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"[WS] 연결 오류: {e}")

            if self._stop_event.is_set():
                break

            self._reconnects += 1
            logger.info(f"[WS] {RECONNECT_DELAY}초 후 재연결...")
            time.sleep(RECONNECT_DELAY)

            # 승인키 갱신 (12시간마다)
            if self._reconnects % 100 == 0:
                try:
                    self.approval_key = _get_ws_approval_key()
                except Exception as e:
                    logger.error(f"[WS] 승인키 갱신 실패: {e}")

        logger.info("[WS] 데몬 종료")

    # ------------------------------------------------------------------
    def stop(self, *_):
        """안전 종료 (SIGTERM/SIGINT 핸들러)."""
        logger.info("[WS] 종료 신호 수신 — 연결 닫는 중...")
        self._stop_event.set()
        if self._ws:
            self._ws.close()

    # ------------------------------------------------------------------
    def _on_open(self, ws):
        self._reconnects = 0
        logger.info(f"[WS] 연결 성공 | {len(self.watchlist)}개 종목 구독")
        for code in self.watchlist:
            ws.send(_sub_msg(self.approval_key, code, "1"))
            time.sleep(0.05)  # 구독 요청 과부하 방지

    # ------------------------------------------------------------------
    def _on_message(self, ws, message: str):
        parsed = _parse_realtime(message)
        if parsed is None:
            return
        self._write_to_firestore(parsed)

    # ------------------------------------------------------------------
    def _on_error(self, ws, error):
        logger.warning(f"[WS] 오류: {error}")

    # ------------------------------------------------------------------
    def _on_close(self, ws, close_status_code, close_msg):
        logger.info(f"[WS] 연결 종료 | code={close_status_code} msg={close_msg}")

    # ------------------------------------------------------------------
    def _write_to_firestore(self, data: dict):
        """체결 데이터를 Firestore에 기록."""
        try:
            self.db.collection(RT_COLLECTION).document(data["code"]).set({
                "code":      data["code"],
                "price":     data["price"],
                "volume":    data["volume"],
                "timestamp": datetime.now(KST),
            })
            self._write_count += 1
        except Exception as e:
            logger.warning(f"[FS] 기록 실패 [{data.get('code')}]: {e}")

    # ------------------------------------------------------------------
    def _log_stats(self):
        """30초마다 수신 통계 로그."""
        elapsed = time.time() - self._last_stat_ts
        rate    = self._write_count / elapsed if elapsed > 0 else 0
        logger.info(
            f"[통계] 기록={self._write_count}건 | 속도={rate:.1f}건/초 | "
            f"재연결={self._reconnects}"
        )
        self._write_count  = 0
        self._last_stat_ts = time.time()
        if not self._stop_event.is_set():
            self._stat_timer = threading.Timer(30, self._log_stats)
            self._stat_timer.daemon = True
            self._stat_timer.start()


# ══════════════════════════════════════════════════════════════════════
# 진입점
# ══════════════════════════════════════════════════════════════════════

def main():
    if not APP_KEY or not APP_SECRET:
        logger.error("KIS_APP_KEY / KIS_APP_SECRET 환경변수 미설정")
        sys.exit(1)

    if not WATCHLIST:
        logger.error("WATCHLIST 환경변수 미설정 (쉼표 구분 종목코드)")
        sys.exit(1)

    # PID 파일 기록
    Path(PID_FILE).write_text(str(os.getpid()))
    logger.info(f"PID={os.getpid()} 기록 → {PID_FILE}")
    logger.info(f"모드={'모의' if IS_MOCK else '실전'} | WS={WS_URL}")
    logger.info(f"감시종목({len(WATCHLIST)}): {', '.join(WATCHLIST[:10])}{'...' if len(WATCHLIST) > 10 else ''}")

    daemon = KisWebSocketDaemon(WATCHLIST)

    # 종료 시그널 등록
    signal.signal(signal.SIGTERM, daemon.stop)
    signal.signal(signal.SIGINT,  daemon.stop)

    daemon.run()

    # PID 파일 정리
    try:
        Path(PID_FILE).unlink()
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    main()
