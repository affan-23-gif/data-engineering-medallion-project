#!/usr/bin/env python3
"""Run the complete local Delta Lake pipeline."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from medallion_pipeline.pipeline import run_pipeline  # noqa: E402


if __name__ == "__main__":
    run_pipeline(PROJECT_ROOT)
