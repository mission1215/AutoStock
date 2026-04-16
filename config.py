"""
config.py — 환경 변수 로드 및 설정 관리
.env 파일에서 모든 설정값을 안전하게 읽어들입니다.
"""

import os
from dotenv import load_dotenv

# 프로젝트 루트의 .env 파일 로드
load_dotenv()


class Config:
    # ── 계정 정보 ──────────────────────────────────────
    APP_KEY: str = os.getenv("KIS_APP_KEY", "")
    APP_SECRET: str = os.getenv("KIS_APP_SECRET", "")
    _ACCOUNT_NO: str = os.getenv("KIS_ACCOUNT_NO", "00000000-01")

    # ── 투자 모드 ──────────────────────────────────────
    # True  = 모의투자 (openapivts.koreainvestment.com:29443)
    # False = 실전투자 (openapi.koreainvestment.com:9443)
    IS_MOCK: bool = os.getenv("KIS_IS_MOCK", "true").lower() == "true"

    # ── 감시 종목 ──────────────────────────────────────
    WATCHLIST: list[str] = [
        s.strip()
        for s in os.getenv("WATCHLIST", "005930,000660,035420").split(",")
        if s.strip()
    ]

    # ── 전략 파라미터 ──────────────────────────────────
    K_FACTOR: float = float(os.getenv("K_FACTOR", "0.5"))
    MA_PERIOD: int = int(os.getenv("MA_PERIOD", "5"))

    # ── 리스크 관리 ────────────────────────────────────
    STOP_LOSS_RATIO: float = float(os.getenv("STOP_LOSS_RATIO", "0.02"))       # -2%
    MAX_POSITION_RATIO: float = float(os.getenv("MAX_POSITION_RATIO", "0.10")) # 10%
    DAILY_PROFIT_TARGET: float = float(os.getenv("DAILY_PROFIT_TARGET", "0.03"))  # +3%

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
        """계좌번호 앞 8자리"""
        return cls._ACCOUNT_NO.split("-")[0] if "-" in cls._ACCOUNT_NO else cls._ACCOUNT_NO[:8]

    @classmethod
    def account_suffix(cls) -> str:
        """계좌번호 뒤 2자리 (상품코드)"""
        return cls._ACCOUNT_NO.split("-")[1] if "-" in cls._ACCOUNT_NO else "01"

    @classmethod
    def validate(cls) -> None:
        """필수 설정값이 모두 채워져 있는지 검증"""
        errors = []
        if not cls.APP_KEY or cls.APP_KEY == "발급받은_APP_KEY":
            errors.append("KIS_APP_KEY 가 설정되지 않았습니다.")
        if not cls.APP_SECRET or cls.APP_SECRET == "발급받은_APP_SECRET":
            errors.append("KIS_APP_SECRET 이 설정되지 않았습니다.")
        if not cls._ACCOUNT_NO or cls._ACCOUNT_NO == "12345678-01":
            errors.append("KIS_ACCOUNT_NO 가 설정되지 않았습니다.")
        if errors:
            raise EnvironmentError(
                ".env 파일을 확인해주세요:\n" + "\n".join(f"  • {e}" for e in errors)
            )
