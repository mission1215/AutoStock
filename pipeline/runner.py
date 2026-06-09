"""
Watchlist pipeline orchestrator.

Usage:
  python -m pipeline run --step all          # full run (needs llm_candidates JSON)
  python -m pipeline run --step technical    # steps 1-2 + LLM prompt bundle
  python -m pipeline run --step finalize     # steps 4-6 (needs llm_candidates JSON)
  python -m pipeline run --step universe     # step 1 only
"""

from __future__ import annotations

import argparse
import sys

from config import Config

from pipeline.briefing import BriefingMeta, save_outputs
from pipeline.llm_prompt import load_llm_candidates, load_llm_excluded, load_llm_response, write_llm_bundle
from pipeline.logging_setup import setup_logging
from pipeline.rule_filter import apply_rule_filter, load_filtered, save_filtered
from pipeline.scoring import build_watchlist, filter_for_push
from pipeline.universe import (
    fetch_universe,
    load_universe,
    save_universe,
    today_iso,
    today_yyyymmdd,
)
from pipeline.config import pipeline_config
from pipeline.validate import validate_candidates


def run_universe(force: bool = False) -> None:
    d = today_yyyymmdd()
    if not force:
        try:
            df = load_universe(d)
            print(f"✓ Universe cached: {len(df)} tickers (data/universe_{d}.parquet)")
            return
        except FileNotFoundError:
            pass
    Config.validate()
    df = fetch_universe()
    save_universe(df, d)


def _save_filter_stats(stats, d: str) -> None:
    from pipeline.config import pipeline_config
    path = pipeline_config.data_dir / f"filter_stats_{d}.json"
    path.write_text(
        __import__("json").dumps(
            {k: getattr(stats, k) for k in stats.__dataclass_fields__},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _load_filter_stats(d: str):
    from dataclasses import fields
    from pipeline.config import pipeline_config
    from pipeline.rule_filter import FilterStats

    path = pipeline_config.data_dir / f"filter_stats_{d}.json"
    if not path.exists():
        return None
    raw = __import__("json").loads(path.read_text(encoding="utf-8"))
    kwargs = {f.name: raw.get(f.name) for f in fields(FilterStats)}
    return FilterStats(**kwargs)


def run_technical(force_universe: bool = False) -> None:
    run_universe(force=force_universe)
    d = today_yyyymmdd()
    universe = load_universe(d)
    filtered, stats = apply_rule_filter(universe)
    save_filtered(filtered, d)
    _save_filter_stats(stats, d)
    json_path, md_path = write_llm_bundle(filtered)

    print()
    print("=" * 60)
    print(f"  Technical pipeline complete — {today_iso()}")
    print(f"  Universe: {stats.universe_count} → Filtered: {stats.pass_count}")
    print("=" * 60)
    print()
    print("Next: Cursor Agent — Expert Signal Scoring")
    print(f"  1. Open: {md_path}")
    print("  2. @web — market_context + 종목별 news/flow/earnings (expert_signal_scoring.md)")
    print(f"  3. Save STRICT JSON → data/llm_candidates_{d}.json")
    print(f"  4. Run: python -m pipeline run --step finalize")
    print()


def run_finalize(*, push: bool = True) -> None:
    d = today_yyyymmdd()
    universe = load_universe(d)
    filtered = load_filtered(d)
    raw_candidates = load_llm_candidates(d)
    llm_response = load_llm_response(d)
    valid, removed = validate_candidates(raw_candidates, universe)
    watchlist = build_watchlist(filtered, valid)

    stats = _load_filter_stats(d) or apply_rule_filter(universe)[1]

    meta = BriefingMeta(
        date=today_iso(),
        universe_count=len(universe),
        filter_pass=len(filtered),
        llm_count=len(raw_candidates),
        valid_count=len(valid),
        final_count=len(watchlist),
        filter_stats=stats,
        llm_hallucination_removed=removed,
        market_regime=str(llm_response.get("market_regime", "")),
        market_regime_reason=str(llm_response.get("market_regime_reason", "")),
        llm_excluded=load_llm_excluded(d),
    )
    csv_path, md_path = save_outputs(watchlist, meta, d)

    print()
    print("=" * 60)
    print(f"  Watchlist ready — {meta.final_count} names")
    print("=" * 60)
    print(f"  CSV: {csv_path}")
    print(f"  MD:  {md_path}")
    if not watchlist.empty:
        print()
        for _, r in watchlist.iterrows():
            grade = r.get("grade", "") or ""
            ts = r.get("total_score", "")
            print(
                f"  {r['ticker']} {str(r['name'])[:8]:8s}  "
                f"{grade} {ts}pt  composite={r['composite']:.2f}  {r['entry_zone']}"
            )

    if push and not watchlist.empty:
        print()
        from pipeline.push import push_watchlist_to_server
        push_df = filter_for_push(watchlist)
        if push_watchlist_to_server(push_df):
            grades = ", ".join(sorted(pipeline_config.expert_push_grade_set()))
            print(f"  → Firebase Push {len(push_df)}종목 (등급: {grades})")
            print("     전략 설정 → AI 추천 엔진 = Cursor")
        else:
            print("  ⚠ Firebase Push 실패 — AUTOSTOCK_API_URL / AUTOSTOCK_API_SECRET 확인")
    print()


def run_all(force_universe: bool = False) -> None:
    run_technical(force_universe=force_universe)
    d = today_yyyymmdd()
    cand_path = f"data/llm_candidates_{d}.json"
    try:
        load_llm_candidates(d)
    except FileNotFoundError:
        print(f"⚠ LLM candidates missing ({cand_path}). Complete Cursor step first.")
        sys.exit(0)
    run_finalize()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="AutoStock Korean watchlist pipeline")
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run pipeline step(s)")
    run_p.add_argument(
        "--step",
        choices=["all", "technical", "finalize", "universe", "daily"],
        default="all",
        help="Pipeline stage (daily=fully automated)",
    )
    run_p.add_argument(
        "--force-universe",
        action="store_true",
        help="Re-fetch universe even if cached",
    )
    run_p.add_argument(
        "--no-push",
        action="store_true",
        help="finalize 시 Firebase Push 생략",
    )

    args = parser.parse_args(argv)
    if args.command != "run":
        parser.print_help()
        sys.exit(1)

    logger = setup_logging(today_yyyymmdd())

    if args.step == "universe":
        run_universe(force=args.force_universe)
    elif args.step == "technical":
        run_technical(force_universe=args.force_universe)
    elif args.step == "daily":
        from pipeline.daily_job import run_daily_automated
        code = run_daily_automated(push=not args.no_push, force_universe=args.force_universe)
        sys.exit(code)
    elif args.step == "finalize":
        run_finalize(push=not args.no_push)
    elif args.step == "all":
        run_all(force_universe=args.force_universe)


if __name__ == "__main__":
    main()
