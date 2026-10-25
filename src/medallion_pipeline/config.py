"""Project paths and data-quality rules."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
LAKE_DIR = PROJECT_ROOT / "data" / "lake"
BRONZE_DIR = LAKE_DIR / "bronze"
SILVER_DIR = LAKE_DIR / "silver"
GOLD_DIR = LAKE_DIR / "gold"
QUARANTINE_DIR = SILVER_DIR / "_quarantine"
REPORTS_DIR = PROJECT_ROOT / "reports"

VALID_ORDER_STATUSES = (
    "PENDING",
    "PROCESSING",
    "SHIPPED",
    "DELIVERED",
    "COMPLETED",
    "CANCELLED",
)
REVENUE_STATUSES = ("DELIVERED", "COMPLETED")
