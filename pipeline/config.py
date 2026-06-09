"""Pipeline configuration — env-driven, separate from trading bot Config."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
PROMPTS_DIR = PROJECT_ROOT / "prompts"


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime knobs for the watchlist pipeline."""

    data_dir: Path = field(default_factory=lambda: DATA_DIR)
    output_dir: Path = field(default_factory=lambda: OUTPUT_DIR)
    prompts_dir: Path = field(default_factory=lambda: PROMPTS_DIR)

    # Universe
    universe_rank_limit: int = int(os.getenv("PIPELINE_RANK_LIMIT", "80"))
    universe_max_tickers: int = int(os.getenv("PIPELINE_MAX_TICKERS", "300"))
    kis_request_delay_sec: float = float(os.getenv("PIPELINE_KIS_DELAY", "0.22"))
    ohlcv_bars: int = int(os.getenv("PIPELINE_OHLCV_BARS", "130"))

    # Scoring
    min_composite_score: float = float(os.getenv("PIPELINE_MIN_SCORE", "0.62"))
    max_watchlist_size: int = int(os.getenv("PIPELINE_MAX_WATCHLIST", "10"))

    # Expert scoring (expert_signal_scoring.md)
    expert_min_trade_score: int = int(os.getenv("PIPELINE_EXPERT_MIN_SCORE", "10"))
    expert_push_grades: str = os.getenv("PIPELINE_EXPERT_PUSH_GRADES", "STRONG BUY")
    expert_prompt_file: Path = field(
        default_factory=lambda: PROMPTS_DIR / "expert_signal_scoring.md"
    )

    # Validation
    name_fuzzy_threshold: float = float(os.getenv("PIPELINE_NAME_FUZZY", "0.85"))

    def expert_push_grade_set(self) -> frozenset[str]:
        parts = {g.strip().upper().replace("_", " ") for g in self.expert_push_grades.split(",") if g.strip()}
        normalized: set[str] = set()
        for g in parts:
            if "STRONG" in g and "BUY" in g:
                normalized.add("STRONG BUY")
            elif g == "BUY":
                normalized.add("BUY")
            elif g == "WATCH":
                normalized.add("WATCH")
        return frozenset(normalized or {"STRONG BUY"})

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.prompts_dir.mkdir(parents=True, exist_ok=True)


pipeline_config = PipelineConfig()
