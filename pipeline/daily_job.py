"""Daily automated pipeline — technical → auto expert → finalize → push."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from config import Config

from pipeline.auto_expert import generate_auto_candidates
from pipeline.briefing import BriefingMeta, save_outputs
from pipeline.config import pipeline_config
from pipeline.llm_prompt import save_llm_candidates
from pipeline.logging_setup import setup_logging
from pipeline.push import push_watchlist_to_server
from pipeline.rule_filter import apply_rule_filter, load_filtered, save_filtered
from pipeline.runner import _load_filter_stats, _save_filter_stats, run_universe
from pipeline.scoring import build_watchlist, filter_for_push
from pipeline.universe import fetch_universe, load_universe, save_universe, today_iso, today_yyyymmdd
from pipeline.validate import validate_candidates

logger = logging.getLogger("watchlist")
KST = ZoneInfo("Asia/Seoul")


def is_kr_trading_day() -> bool:
    now = datetime.now(KST)
    return now.weekday() < 5


def run_daily_automated(*, push: bool = True, force_universe: bool = False) -> int:
    """
    Full unattended daily job (평일 8:50 KST 권장).
    Returns 0 on success, 1 on failure.
    """
    setup_logging(today_yyyymmdd())

    if not is_kr_trading_day():
        logger.info("주말 — 파이프라인 스킵")
        print("⏭ 주말 — 스킵")
        return 0

    try:
        Config.validate()
    except EnvironmentError as exc:
        logger.error("Config: %s", exc)
        print(f"❌ .env 설정 오류: {exc}")
        return 1

    d = today_yyyymmdd()
    mode = Config.mode_label()
    print(
        f"▶ AutoStock Daily Pipeline — {today_iso()} {datetime.now(KST):%H:%M} KST "
        f"[{mode}]"
    )

    try:
        if force_universe:
            df = fetch_universe()
            save_universe(df, d)
        else:
            try:
                load_universe(d)
            except FileNotFoundError:
                df = fetch_universe()
                save_universe(df, d)

        universe = load_universe(d)
        filtered, stats = apply_rule_filter(universe)
        save_filtered(filtered, d)
        _save_filter_stats(stats, d)

        if filtered.empty:
            print("⚠ 필터 통과 종목 없음")
            return 1

        auto_payload = generate_auto_candidates(filtered)
        auto_payload["analysis_date"] = d
        cand_path = pipeline_config.data_dir / f"llm_candidates_{d}.json"
        cand_path.write_text(json.dumps(auto_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Auto candidates → %s (%d)", cand_path, len(auto_payload["candidates"]))

        raw_candidates = auto_payload["candidates"]
        valid, removed = validate_candidates(raw_candidates, universe)
        watchlist = build_watchlist(filtered, valid)

        meta = BriefingMeta(
            date=today_iso(),
            universe_count=len(universe),
            filter_pass=len(filtered),
            llm_count=len(raw_candidates),
            valid_count=len(valid),
            final_count=len(watchlist),
            filter_stats=stats,
            llm_hallucination_removed=removed,
            market_regime=auto_payload.get("market_regime", ""),
            market_regime_reason=auto_payload.get("market_regime_reason", ""),
            llm_excluded=auto_payload.get("excluded", []),
        )
        csv_path, md_path = save_outputs(watchlist, meta, d)

        print(f"  Universe {stats.universe_count} → Filter {stats.pass_count} → Watch {len(watchlist)}")
        print(f"  CSV: {csv_path}")
        print(f"  MD:  {md_path}")

        if push and not watchlist.empty:
            push_df = filter_for_push(watchlist)
            if push_watchlist_to_server(push_df):
                grades = ", ".join(sorted(pipeline_config.expert_push_grade_set()))
                print(f"  ✅ Firebase Push {len(push_df)}종목 ({grades})")
            else:
                print("  ⚠ Firebase Push 실패")
                return 1
        elif watchlist.empty:
            print("  ⚠ 감시목록 비어 있음 — Push 생략")
            return 1

        print("✅ Daily pipeline 완료")
        return 0

    except Exception as exc:
        logger.exception("Daily pipeline failed")
        print(f"❌ 오류: {exc}")
        return 1


if __name__ == "__main__":
    push = "--no-push" not in sys.argv
    force = "--force-universe" in sys.argv
    sys.exit(run_daily_automated(push=push, force_universe=force))
