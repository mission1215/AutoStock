"""
config.py — 환경 변수 로드 및 설정 관리
.env 파일에서 모든 설정값을 안전하게 읽어들입니다.

KIS 자격증명 — 모의/실전 프로필 (웹앱 Firebase 구조와 동일 개념)
  KIS_MOCK_APP_KEY / SECRET / ACCOUNT_NO  — 모의투자
  KIS_LIVE_APP_KEY / SECRET / ACCOUNT_NO  — 실전투자
  KIS_IS_MOCK=true|false                  — 활성 프로필 선택

레거시 KIS_APP_KEY 등은 모의 프로필이 비어 있을 때만 폴백됩니다.
"""

from __future__ import annotations

import os
from typing import NamedTuple

from dotenv import load_dotenv

load_dotenv()

_PLACEHOLDER_KEYS = frozenset({"발급받은_APP_KEY", "발급받은_APP_SECRET"})
_PLACEHOLDER_ACCOUNTS = frozenset({"12345678-01", "00000000-01", ""})


class KISCredentials(NamedTuple):
    app_key: str
    app_secret: str
    account_no: str

    @property
    def configured(self) -> bool:
        return bool(
            self.app_key
            and self.app_secret
            and self.account_no
            and self.app_key not in _PLACEHOLDER_KEYS
            and self.app_secret not in _PLACEHOLDER_KEYS
            and self.account_no not in _PLACEHOLDER_ACCOUNTS
        )

    def missing_fields(self, prefix: str) -> list[str]:
        missing: list[str] = []
        if not self.app_key or self.app_key in _PLACEHOLDER_KEYS:
            missing.append(f"{prefix}APP_KEY")
        if not self.app_secret or self.app_secret in _PLACEHOLDER_KEYS:
            missing.append(f"{prefix}APP_SECRET")
        if not self.account_no or self.account_no in _PLACEHOLDER_ACCOUNTS:
            missing.append(f"{prefix}ACCOUNT_NO")
        return missing


def _strip(value: str | None) -> str:
    return (value or "").strip()


def _read_profile(prefix: str) -> KISCredentials:
    return KISCredentials(
        app_key=_strip(os.getenv(f"{prefix}APP_KEY")),
        app_secret=_strip(os.getenv(f"{prefix}APP_SECRET")),
        account_no=_strip(os.getenv(f"{prefix}ACCOUNT_NO")),
    )


def _legacy_profile() -> KISCredentials:
    return KISCredentials(
        app_key=_strip(os.getenv("KIS_APP_KEY")),
        app_secret=_strip(os.getenv("KIS_APP_SECRET")),
        account_no=_strip(os.getenv("KIS_ACCOUNT_NO")),
    )


def _merge_mock() -> KISCredentials:
    """모의 프로필 — KIS_MOCK_* 우선, 비어 있으면 레거시 KIS_APP_* 폴백."""
    specific = _read_profile("KIS_MOCK_")
    legacy = _legacy_profile()
    return KISCredentials(
        app_key=specific.app_key or legacy.app_key,
        app_secret=specific.app_secret or legacy.app_secret,
        account_no=specific.account_no or legacy.account_no,
    )


def _resolve_active() -> tuple[bool, KISCredentials, KISCredentials, KISCredentials]:
    is_mock = _strip(os.getenv("KIS_IS_MOCK", "true")).lower() == "true"
    mock = _merge_mock()
    live = _read_profile("KIS_LIVE_")
    active = mock if is_mock else live
    return is_mock, active, mock, live


_IS_MOCK, _ACTIVE, MOCK_PROFILE, LIVE_PROFILE = _resolve_active()


class Config:
    # ── 활성 KIS 프로필 (KIS_IS_MOCK 기준) ─────────────
    APP_KEY: str = _ACTIVE.app_key
    APP_SECRET: str = _ACTIVE.app_secret
    _ACCOUNT_NO: str = _ACTIVE.account_no or "00000000-01"

    IS_MOCK: bool = _IS_MOCK

    # ── 감시 종목 ──────────────────────────────────────
    WATCHLIST: list[str] = [
        s.strip()
        for s in os.getenv("WATCHLIST", "005930,000660,035420,035720,051910").split(",")
        if s.strip()
    ]

    # ── 전략 파라미터 ──────────────────────────────────
    K_FACTOR: float = float(os.getenv("K_FACTOR", "0.5"))
    MA_PERIOD: int = int(os.getenv("MA_PERIOD", "5"))

    # ── 리스크 관리 ────────────────────────────────────
    STOP_LOSS_RATIO: float = float(os.getenv("STOP_LOSS_RATIO", "0.02"))
    MAX_POSITION_RATIO: float = float(os.getenv("MAX_POSITION_RATIO", "0.10"))
    DAILY_PROFIT_TARGET: float = float(os.getenv("DAILY_PROFIT_TARGET", "0.03"))

    # ── 스케줄러 ───────────────────────────────────────
    CHECK_INTERVAL_SECONDS: int = int(os.getenv("CHECK_INTERVAL_SECONDS", "60"))

    # ── 파생 속성 ──────────────────────────────────────
    @classmethod
    def base_url(cls) -> str:
        if cls.IS_MOCK:
            return "https://openapivts.koreainvestment.com:29443"
        return "https://openapi.koreainvestment.com:9443"

    @classmethod
    def account_prefix(cls) -> str:
        return cls._ACCOUNT_NO.split("-")[0] if "-" in cls._ACCOUNT_NO else cls._ACCOUNT_NO[:8]

    @classmethod
    def account_suffix(cls) -> str:
        return cls._ACCOUNT_NO.split("-")[1] if "-" in cls._ACCOUNT_NO else "01"

    @classmethod
    def mode_label(cls) -> str:
        return "모의투자" if cls.IS_MOCK else "실전투자"

    @classmethod
    def active_env_prefix(cls) -> str:
        return "KIS_MOCK_" if cls.IS_MOCK else "KIS_LIVE_"

    @classmethod
    def mock_credentials(cls) -> KISCredentials:
        return MOCK_PROFILE

    @classmethod
    def live_credentials(cls) -> KISCredentials:
        return LIVE_PROFILE

    @classmethod
    def profile_configured(cls, *, mock: bool | None = None) -> bool:
        cred = MOCK_PROFILE if (mock if mock is not None else cls.IS_MOCK) else LIVE_PROFILE
        return cred.configured

    @classmethod
    def validate(cls) -> None:
        """활성 모드 프로필 필수값 검증."""
        prefix = cls.active_env_prefix()
        cred = MOCK_PROFILE if cls.IS_MOCK else LIVE_PROFILE
        missing = cred.missing_fields(prefix)
        if missing:
            mode = cls.mode_label()
            hint = (
                "모의→실전 전환: KIS_IS_MOCK=false 후 KIS_LIVE_* 3항목을 채우세요."
                if cls.IS_MOCK
                else "실전 프로필이 비어 있으면 KIS_LIVE_APP_KEY/SECRET/ACCOUNT_NO 를 입력하세요."
            )
            raise EnvironmentError(
                f".env [{mode}] 프로필 미완성:\n"
                + "\n".join(f"  • {m}" for m in missing)
                + f"\n  → {hint}"
            )

    @classmethod
    def profile_status_lines(cls) -> list[str]:
        """시작 배너·검증 스크립트용 요약."""
        mock_ok = MOCK_PROFILE.configured
        live_ok = LIVE_PROFILE.configured
        active = "모의" if cls.IS_MOCK else "실전"
        return [
            f"활성 모드: {active} (KIS_IS_MOCK={str(cls.IS_MOCK).lower()})",
            f"모의 프로필: {'✓ 준비됨' if mock_ok else '✗ 미설정'}",
            f"실전 프로필: {'✓ 준비됨' if live_ok else '— 미설정 (실전 전환 전 채우기)'}",
        ]
