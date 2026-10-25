#!/usr/bin/env python3
"""Print the Gold layer aggregates for a quick demo."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from medallion_pipeline.config import GOLD_DIR  # noqa: E402
from medallion_pipeline.pipeline import build_spark_session  # noqa: E402


def main() -> None:
    spark = build_spark_session("InspectGoldTables")
    try:
        for name in ("monthly_sales", "customer_summary", "product_performance"):
            path = GOLD_DIR / name
            print(f"\n=== GOLD: {name} ===")
            spark.read.format("delta").load(str(path)).show(50, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
