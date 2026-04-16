"""
token_manager.py — KIS API 접근 토큰 자동 발급·갱신 관리

핵심 설계:
  - 토큰 만료 5분 전에 선제적 갱신
  - 멀티스레드 안전(Lock) — 스케줄러와 전략이 동시에 호출해도 토큰을 중복 발급하지 않음
  - invalidate() 로 강제 재발급 요청 가능 (401 응답 시 호출)
"""

import logging
import requests
from datetime import datetime, timedelta
from threading import Lock

from config import Config

logger = logging.getLogger(__name__)


class TokenManager:
    _TOKEN_ENDPOINT = "/oauth2/tokenP"
    # 만료 N분 전에 선제적으로 갱신
    _REFRESH_BUFFER_MINUTES = 5

    def __init__(self) -> None:
        self._access_token: str | None = None
        self._expires_at: datetime | None = None
        self._lock = Lock()

    # ── 공개 인터페이스 ────────────────────────────────

    def get_token(self) -> str:
        """
        유효한 접근 토큰을 반환합니다.
        토큰이 없거나 만료 임박이면 자동으로 재발급합니다.
        """
        with self._lock:
            if self._is_valid():
                remaining = (self._expires_at - datetime.now()).seconds // 60
                logger.debug(f"기존 토큰 사용 중 (잔여 유효시간: 약 {remaining}분)")
                return self._access_token
            return self._issue()

    def invalidate(self) -> None:
        """
        토큰을 즉시 무효화합니다.
        401 Unauthorized 응답 등 외부에서 토큰 오류를 감지했을 때 호출하세요.
        다음 get_token() 호출 시 새 토큰을 발급받습니다.
        """
        with self._lock:
            logger.warning("토큰 강제 무효화 — 다음 호출 시 재발급됩니다.")
            self._access_token = None
            self._expires_at = None

    @property
    def is_valid(self) -> bool:
        """현재 토큰이 유효한지 외부에서 확인하는 프로퍼티"""
        with self._lock:
            return self._is_valid()

    # ── 내부 메서드 ────────────────────────────────────

    def _is_valid(self) -> bool:
        """(Lock 내부에서만 호출) 토큰이 유효한지 확인"""
        if not self._access_token or not self._expires_at:
            return False
        cutoff = datetime.now() + timedelta(minutes=self._REFRESH_BUFFER_MINUTES)
        return self._expires_at > cutoff

    def _issue(self) -> str:
        """(Lock 내부에서만 호출) KIS 서버에 새 토큰을 요청"""
        url = Config.base_url() + self._TOKEN_ENDPOINT
        payload = {
            "grant_type": "client_credentials",
            "appkey": Config.APP_KEY,
            "appsecret": Config.APP_SECRET,
        }

        logger.info("접근 토큰 발급 요청 중...")
        try:
            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"토큰 발급 HTTP 오류: {e} — 응답: {e.response.text[:200]}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"토큰 발급 네트워크 오류: {e}")
            raise

        if "access_token" not in data:
            raise ValueError(f"토큰 응답에 access_token 없음: {data}")

        self._access_token = data["access_token"]

        # 서버가 expires_in(초)을 주면 그것을 사용, 없으면 24시간 기본값
        expires_in_sec: int = int(data.get("expires_in", 86400))
        self._expires_at = datetime.now() + timedelta(seconds=expires_in_sec)

        mode_label = "모의투자" if Config.IS_MOCK else "실전투자"
        logger.info(
            f"[{mode_label}] 토큰 발급 성공 | "
            f"만료: {self._expires_at.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        return self._access_token


# 모듈 수준 싱글톤 — 모든 모듈이 같은 인스턴스를 공유합니다.
token_manager = TokenManager()
