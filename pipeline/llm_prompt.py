"""Step 3 — Expert Signal Scoring LLM bundle (prompts/expert_signal_scoring.md)."""

from __future__ import annotations

import json
import logging

import pandas as pd

from pipeline.config import pipeline_config
from pipeline.constants import MAX_LLM_CANDIDATES
from pipeline.expert_scoring import (
    MARKET_CONTEXT_SEARCH,
    build_stock_expert_payload,
    empty_market_context,
    load_expert_prompt_text,
)
from pipeline.universe import today_iso, today_yyyymmdd

logger = logging.getLogger("watchlist")

EXPERT_OUTPUT_SCHEMA = """{
  "analysis_date": "YYYYMMDD",
  "market_regime": "BULL|NEUTRAL|BEAR",
  "market_regime_reason": "<30자>",
  "candidates": [
    {
      "ticker": "005930",
      "name": "삼성전자",
      "grade": "STRONG BUY",
      "total_score": 16,
      "layer_scores": {
        "L1_macro": 3, "L2_technical": 5, "L3_flow": 4,
        "L4_fundamental": 2, "L5_catalyst": 2, "L6_sector": 3,
        "L7_disqualified": false
      },
      "bull_signals": ["..."],
      "bear_signals": ["..."],
      "entry_strategy": "...",
      "stop_loss_trigger": "...",
      "reason_30": "<30자 한국어>"
    }
  ],
  "excluded": [{"ticker": "000660", "reason": "..."}],
  "top5": ["005930"]
}"""


def build_web_search_queries(filtered_df: pd.DataFrame) -> list[dict[str, str]]:
    queries: list[dict[str, str]] = []
    for _, row in filtered_df.iterrows():
        ticker = str(row["ticker"])
        name = str(row.get("name", ticker))
        queries.append(
            {
                "ticker": ticker,
                "name": name,
                "news": f"{name} {ticker} 최근 3일 뉴스 site:finance.naver.com",
                "flow": f"{name} 외국인 기관 순매수 3일 program",
                "earnings": f"{name} 어닝 서프라이즈 실적 1분기 PER PBR",
                "catalyst": f"{name} 계약 수주 정책 자사주 매입",
                "risk": f"{name} 투자주의 관리종목 실적발표 일정",
            }
        )
    return queries


def build_llm_bundle(filtered_df: pd.DataFrame) -> dict:
    d = today_yyyymmdd()
    stocks = [build_stock_expert_payload(row.to_dict()) for _, row in filtered_df.iterrows()]
    return {
        "schema": "expert_signal_v1",
        "date": today_iso(),
        "date_key": d,
        "stock_count": len(stocks),
        "expert_prompt_ref": "prompts/expert_signal_scoring.md",
        "market_context": empty_market_context(d),
        "market_context_search_queries": MARKET_CONTEXT_SEARCH,
        "stocks": stocks,
        "web_search_queries": build_web_search_queries(filtered_df),
        "output_instructions": {
            "max_candidates": MAX_LLM_CANDIDATES,
            "trade_grades": ["STRONG BUY", "BUY"],
            "push_grades": list(pipeline_config.expert_push_grade_set()),
            "min_trade_score": pipeline_config.expert_min_trade_score,
            "strict_json_only": True,
        },
    }


def write_llm_bundle(filtered_df: pd.DataFrame) -> tuple[str, str]:
    pipeline_config.ensure_dirs()
    d = today_yyyymmdd()
    bundle = build_llm_bundle(filtered_df)
    expert_doc = load_expert_prompt_text()

    json_path = pipeline_config.data_dir / f"llm_input_{d}.json"
    json_path.write_text(json.dumps(bundle, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = [
        f"# Expert Signal Scoring — {bundle['date']}",
        "",
        "> `prompts/expert_signal_scoring.md` 7레이어 프레임워크 적용",
        "",
        "## Cursor Agent 실행 순서",
        "",
        "### 1. 시장 컨텍스트 (@web)",
        "아래 검색어로 `market_context` 필드를 채우세요:",
        "",
        "```json",
        json.dumps(bundle["market_context_search_queries"], ensure_ascii=False, indent=2),
        "```",
        "",
        "### 2. 종목별 수급·뉴스·실적 (@web)",
        "`data/llm_input_{d}.json` → `web_search_queries` 참조",
        "",
        "### 3. 7레이어 스코어링",
        "전문가 프롬프트 전문을 따르세요 → `prompts/expert_signal_scoring.md`",
        "",
        "**핵심 규칙:**",
        "- 3개 이상 레이어 양호 신호 정렬 필수",
        "- L3(수급) 마이너스 → 추천 금지",
        "- L7 리스크 게이트 해당 → `excluded` 로 이동",
        "- 할루시네이션 금지 — 모르면 0/null",
        "",
        "### 4. 출력 저장",
        f"`data/llm_candidates_{d}.json` — **STRICT JSON only**",
        "",
        "```json",
        EXPERT_OUTPUT_SCHEMA,
        "```",
        "",
        "## 입력 데이터 (기술적 지표 pre-fill)",
        "",
        "```json",
        json.dumps(
            {"market_context": bundle["market_context"], "stocks": bundle["stocks"][:20]},
            ensure_ascii=False,
            indent=2,
        ),
        "```",
        "",
        f"_상위 20/{len(bundle['stocks'])}종목. 전체는 `{json_path.name}`._",
        "",
        "## Expert Prompt Reference",
        "",
        expert_doc if expert_doc else "_expert_signal_scoring.md not found_",
    ]
    md_path = pipeline_config.prompts_dir / f"cursor_llm_scoring_{d}.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    logger.info("Expert LLM bundle → %s, %s", json_path, md_path)
    return str(json_path), str(md_path)


def load_llm_response(date_str: str | None = None) -> dict:
    d = date_str or today_yyyymmdd()
    path = pipeline_config.data_dir / f"llm_candidates_{d}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"LLM candidates not found: {path}\n"
            f"Run Cursor with prompts/cursor_llm_scoring_{d}.md (Expert Signal Scoring)."
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {"candidates": data, "excluded": [], "schema": "legacy_abcd"}
    return data


def load_llm_candidates(date_str: str | None = None) -> list[dict]:
    return load_llm_response(date_str).get("candidates") or []


def load_llm_excluded(date_str: str | None = None) -> list[dict]:
    return load_llm_response(date_str).get("excluded") or []


def save_llm_candidates(candidates: list[dict], date_str: str | None = None, **meta) -> str:
    d = date_str or today_yyyymmdd()
    path = pipeline_config.data_dir / f"llm_candidates_{d}.json"
    payload = {"candidates": candidates, "schema": "expert_signal_v1", **meta}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)
