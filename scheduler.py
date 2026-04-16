"""
scheduler.py — 장 시간 자동화 스케줄러

APScheduler 를 사용하여 평일 장 시간에만 동작합니다.

스케줄:
  08:50  — 토큰 갱신 + 전략 초기화 (장 시작 10분 전 준비)
  09:00  — 장 시작 로그
  매 N초 — 전략 사이클 실행 (기본 60초, .env CHECK_INTERVAL_SECONDS)
  15:20  — 장 마감 전 강제 청산
  15:31  — 세션 종료 로그
"""

import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config import Config
from token_manager import token_manager
from api_client import KISClient
from strategy import VolatilityBreakoutStrategy
from order_executor import OrderExecutor

logger = logging.getLogger(__name__)


class TradingScheduler:
    """
    자동매매 스케줄러

    사용법:
        scheduler = TradingScheduler()
        scheduler.start()   # blocking — Ctrl+C 로 종료
    """

    def __init__(self) -> None:
        self._client = KISClient()
        self._strategy = VolatilityBreakoutStrategy(self._client)
        self._executor = OrderExecutor(self._client, self._strategy)
        self._scheduler = BlockingScheduler(timezone="Asia/Seoul")
        self._is_market_open: bool = False

        self._register_jobs()

    # ── 스케줄 등록 ────────────────────────────────────

    def _register_jobs(self) -> None:
        mode = "모의투자" if Config.IS_MOCK else "실전투자"
        logger.info(f"스케줄 등록 중 [{mode}]")

        # 08:50 — 토큰 갱신 + 전략 초기화
        self._scheduler.add_job(
            self._job_prepare,
            CronTrigger(
                day_of_week="mon-fri",
                hour=8,
                minute=50,
                timezone="Asia/Seoul",
            ),
            id="prepare",
            name="장 시작 준비 (토큰 갱신 + 전략 초기화)",
        )

        # 09:00 — 장 시작 알림
        self._scheduler.add_job(
            self._job_market_open,
            CronTrigger(
                day_of_week="mon-fri",
                hour=9,
                minute=0,
                timezone="Asia/Seoul",
            ),
            id="market_open",
            name="장 시작",
        )

        # 09:00 ~ 15:20 — 매 N초마다 전략 실행
        interval_sec = Config.CHECK_INTERVAL_SECONDS
        self._scheduler.add_job(
            self._job_strategy_cycle,
            "interval",
            seconds=interval_sec,
            start_date=datetime.now().replace(hour=9, minute=0, second=0, microsecond=0),
            id="strategy_cycle",
            name=f"전략 사이클 (매 {interval_sec}초)",
        )

        # 15:20 — 강제 청산
        self._scheduler.add_job(
            self._job_close_positions,
            CronTrigger(
                day_of_week="mon-fri",
                hour=15,
                minute=20,
                timezone="Asia/Seoul",
            ),
            id="close_positions",
            name="장 마감 전 강제 청산",
        )

        # 15:31 — 세션 종료
        self._scheduler.add_job(
            self._job_market_close,
            CronTrigger(
                day_of_week="mon-fri",
                hour=15,
                minute=31,
                timezone="Asia/Seoul",
            ),
            id="market_close",
            name="장 마감 세션 종료",
        )

        logger.info("스케줄 등록 완료")

    # ── 개별 Job 구현 ──────────────────────────────────

    def _job_prepare(self) -> None:
        """08:50 — 토큰 갱신 + 전략 초기화"""
        logger.info("=" * 60)
        logger.info("[08:50] 장 시작 준비 — 토큰 갱신 및 전략 초기화")
        try:
            token_manager.invalidate()
            token_manager.get_token()
            logger.info("토큰 갱신 완료")
        except Exception as e:
            logger.error(f"토큰 갱신 실패: {e}")

        try:
            self._strategy.prepare_market_open()
        except Exception as e:
            logger.error(f"전략 초기화 실패: {e}")

    def _job_market_open(self) -> None:
        """09:00 — 장 시작"""
        self._is_market_open = True
        mode = "모의투자" if Config.IS_MOCK else "실전투자"
        logger.info("=" * 60)
        logger.info(f"[09:00] 장 시작 [{mode}] — 자동매매 시작")
        logger.info(f"감시 종목: {', '.join(Config.WATCHLIST)}")
        logger.info(f"전략 파라미터: K={Config.K_FACTOR}, MA{Config.MA_PERIOD}")
        logger.info(
            f"리스크: 손절={Config.STOP_LOSS_RATIO*100:.0f}% "
            f"최대비중={Config.MAX_POSITION_RATIO*100:.0f}% "
            f"일일목표={Config.DAILY_PROFIT_TARGET*100:.0f}%"
        )

    def _job_strategy_cycle(self) -> None:
        """매 N초 — 전략 사이클"""
        if not self._is_market_open:
            return
        if self._strategy.trading_halted:
            return

        now = datetime.now()
        # 장중(09:00 ~ 15:20)에만 실행
        market_start = now.replace(hour=9, minute=0, second=0, microsecond=0)
        market_end = now.replace(hour=15, minute=20, second=0, microsecond=0)
        if not (market_start <= now <= market_end):
            return

        try:
            self._strategy.run_cycle()
        except Exception as e:
            logger.error(f"전략 사이클 오류: {e}")

    def _job_close_positions(self) -> None:
        """15:20 — 장 마감 전 강제 청산"""
        logger.info("=" * 60)
        logger.info("[15:20] 장 마감 전 강제 청산 시작")
        try:
            self._strategy.close_all_positions()
        except Exception as e:
            logger.error(f"청산 중 오류: {e}")

    def _job_market_close(self) -> None:
        """15:31 — 세션 종료"""
        self._is_market_open = False
        logger.info("=" * 60)
        logger.info("[15:31] 장 마감 — 금일 자동매매 종료")
        pos = self._strategy.positions
        if pos:
            logger.warning(f"미청산 포지션 {len(pos)}개: {list(pos.keys())}")
        else:
            logger.info("모든 포지션 정상 청산됨")

    # ── 시작 / 종료 ────────────────────────────────────

    def start(self) -> None:
        """스케줄러 시작 (blocking)"""
        mode = "모의투자" if Config.IS_MOCK else "실전투자"
        logger.info("=" * 60)
        logger.info(f"AutoStock 자동매매 시스템 시작 [{mode}]")
        logger.info("종료하려면 Ctrl+C 를 누르세요.")
        logger.info("=" * 60)

        # 테스트용: 스케줄러 시작 직후 준비 작업 즉시 실행
        now = datetime.now()
        market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=31, second=0, microsecond=0)

        if market_open <= now <= market_close:
            logger.info("장중 시작 감지 — 즉시 준비 작업 실행")
            self._job_prepare()
            self._job_market_open()

        try:
            self._scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("사용자 종료 요청 — 시스템을 안전하게 종료합니다.")
            self._safe_shutdown()

    def _safe_shutdown(self) -> None:
        """안전 종료: 진행 중인 포지션 확인 후 스케줄러 중단"""
        if self._strategy.positions:
            logger.warning(
                f"종료 시 미청산 포지션: {list(self._strategy.positions.keys())}"
            )
            logger.warning("수동으로 확인 후 처리하세요.")
        self._scheduler.shutdown(wait=False)
        logger.info("스케줄러 종료 완료")
