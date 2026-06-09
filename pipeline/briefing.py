"""Step 6 — Daily briefing (Expert Signal format)."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd

from pipeline.config import pipeline_config
from pipeline.rule_filter import FilterStats
from pipeline.universe import today_iso, today_yyyymmdd

logger = logging.getLogger("watchlist")


@dataclass
class BriefingMeta:
    date: str
    universe_count: int
    filter_pass: int
    llm_count: int
    valid_count: int
    final_count: int
    filter_stats: FilterStats
    llm_hallucination_removed: int = 0
    market_regime: str = ""
    market_regime_reason: str = ""
    llm_excluded: list[dict] = field(default_factory=list)


def _exclusion_section(stats: FilterStats, llm_removed: int, llm_excluded: list[dict]) -> str:
    lines = [
        "## ⚠️ 오늘의 제외 사유",
        "",
        f"- RSI 과매수/과매도 제거: {stats.fail_r4_rsi}종목",
        f"- 거래량 부족 (vol_ratio≤1.3): {stats.fail_r3_volume}종목",
        f"- MA 추세 미달: {stats.fail_r1_uptrend + stats.fail_r2_momentum}종목",
        f"- 52주 저점/고점 필터: {stats.fail_r5_floor + stats.fail_r6_ceiling}종목",
        f"- 시총 미달 (<1,000억): {stats.fail_r7_mktcap}종목",
        f"- 섹터 블랙리스트: {stats.fail_r8_sector}종목",
        f"- LLM 환각/검증 실패 제거: {llm_removed}종목",
    ]
    if llm_excluded:
        lines.append(f"- Expert L7/등급 제외: {len(llm_excluded)}종목")
        for ex in llm_excluded[:5]:
            lines.append(f"  - {ex.get('ticker', '?')}: {ex.get('reason', '')}")
    return "\n".join(lines)


def render_briefing(watchlist: pd.DataFrame, meta: BriefingMeta) -> str:
    rows = []
    for rank, (_, r) in enumerate(watchlist.iterrows(), 1):
        grade = r.get("grade", "") or "—"
        ts = r.get("total_score")
        score_txt = f"{ts}pt" if ts is not None and str(ts) != "nan" else f"{r.get('composite', 0):.2f}"
        stop = r.get("stop_loss_trigger", "") or "—"
        rows.append(
            f"| {rank} | {r.get('name','')} | {r['ticker']} | {grade} | {score_txt} | "
            f"{r.get('entry_zone', '-')} | {stop} | {r.get('reason', '')} |"
        )

    table = "\n".join(rows) if rows else "| — | — | — | — | — | — | — | 후보 없음 |"

    regime_line = ""
    if meta.market_regime:
        regime_line = f"\n시장 레짐: **{meta.market_regime}** — {meta.market_regime_reason}\n"

    return f"""# 📊 일일 감시목록 — {meta.date}
필터 통과: {meta.filter_pass} / {meta.universe_count}
LLM 추천: {meta.llm_count} → 검증 후: {meta.valid_count}
최종 감시: {meta.final_count}
{regime_line}
## 🏆 TOP {meta.final_count} 감시종목 (Expert Signal)

| 순위 | 종목 | 코드 | 등급 | 점수 | 진입구간 | 손절 | 핵심이유 |
|------|------|------|------|------|---------|------|---------|
{table}

{_exclusion_section(meta.filter_stats, meta.llm_hallucination_removed, meta.llm_excluded)}

---
_Expert Signal Scoring · AutoStock Watchlist Pipeline_
"""


def save_outputs(
    watchlist: pd.DataFrame,
    meta: BriefingMeta,
    date_str: str | None = None,
) -> tuple[str, str]:
    pipeline_config.ensure_dirs()
    d = date_str or today_yyyymmdd()

    csv_path = pipeline_config.output_dir / f"watchlist_{d}.csv"
    export_cols = [
        c for c in [
            "ticker", "name", "grade", "total_score", "composite",
            "reason", "entry_zone", "stop_loss_trigger", "entry_strategy",
        ]
        if c in watchlist.columns
    ]
    watchlist[export_cols].to_csv(csv_path, index=False, encoding="utf-8-sig")

    md_path = pipeline_config.output_dir / f"watchlist_{d}.md"
    md_path.write_text(render_briefing(watchlist, meta), encoding="utf-8")

    logger.info("Briefing → %s, %s", csv_path, md_path)
    return str(csv_path), str(md_path)
