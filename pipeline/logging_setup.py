"""Structured logging for the watchlist pipeline."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from pipeline.config import pipeline_config


def setup_logging(date_str: str | None = None) -> logging.Logger:
    pipeline_config.ensure_dirs()
    logger = logging.getLogger("watchlist")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if date_str:
        log_path = pipeline_config.data_dir / f"pipeline_{date_str}.log"
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
