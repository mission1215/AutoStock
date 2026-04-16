"""
main.py — AutoStock 자동매매 시스템 진입점

실행 방법:
    # 모의투자 (기본값, .env: KIS_IS_MOCK=true)
    python main.py

    # 실전투자 (.env: KIS_IS_MOCK=false)
    python main.py

로그는 콘솔과 logs/autostock_YYYYMMDD.log 에 동시 출력됩니다.
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def _setup_logging() -> None:
    """콘솔 + 파일 동시 로깅 설정"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_filename = log_dir / f"autostock_{datetime.now().strftime('%Y%m%d')}.log"

    fmt = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # 루트 로거
    logging.basicConfig(
        level=logging.DEBUG,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            # 1. 콘솔 (INFO 이상)
            logging.StreamHandler(sys.stdout),
            # 2. 파일 (DEBUG 이상 — 전체 기록)
            logging.FileHandler(log_filename, encoding="utf-8"),
        ],
    )

    # 콘솔 핸들러는 INFO 레벨로 제한
    console_handler = logging.root.handlers[0]
    console_handler.setLevel(logging.INFO)

    # 외부 라이브러리 노이즈 억제
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info(f"로그 파일: {log_filename.resolve()}")


def _print_banner() -> None:
    """시작 배너 출력"""
    from config import Config
    mode = "모의투자  [MOCK]" if Config.IS_MOCK else "실전투자  [LIVE] ⚠️"
    print()
    print("╔══════════════════════════════════════════════════════╗")
    print("║          AutoStock — KIS 자동매매 시스템             ║")
    print("║          변동성 돌파 전략 + 리스크 관리               ║")
    print(f"║  투자 모드 : {mode:<38}║")
    print(f"║  감시 종목 : {', '.join(Config.WATCHLIST):<38}║")
    print("╚══════════════════════════════════════════════════════╝")
    print()

    if not Config.IS_MOCK:
        print("⚠️  실전투자 모드입니다. 실제 자산이 거래됩니다.")
        answer = input("계속 진행하시겠습니까? (yes 입력): ").strip().lower()
        if answer != "yes":
            print("실행 취소됨.")
            sys.exit(0)
        print()


def main() -> None:
    _setup_logging()

    # 설정 검증
    from config import Config
    try:
        Config.validate()
    except EnvironmentError as e:
        logging.critical(str(e))
        sys.exit(1)

    _print_banner()

    # 스케줄러 시작 (blocking)
    from scheduler import TradingScheduler
    scheduler = TradingScheduler()
    scheduler.start()


if __name__ == "__main__":
    main()
