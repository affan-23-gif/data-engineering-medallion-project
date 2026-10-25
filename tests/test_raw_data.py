"""Dependency-free checks for the included synthetic source data."""

import csv
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"


def read_csv(name: str) -> list[dict[str, str]]:
    with (RAW / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_all_three_source_files_exist_and_have_rows() -> None:
    for name in ("customers.csv", "products.csv", "orders.csv"):
        path = RAW / name
        assert path.is_file(), f"Missing required input file: {path}"
        assert len(read_csv(name)) > 0


def test_source_data_contains_duplicate_customer_and_product_keys() -> None:
    customers = read_csv("customers.csv")
    products = read_csv("products.csv")
    customer_counts = Counter(row["customer_id"] for row in customers)
    product_counts = Counter(row["product_id"] for row in products)
    assert customer_counts["3"] == 2
    assert product_counts["102"] == 2


def test_orders_include_examples_for_quarantine_rules() -> None:
    orders = read_csv("orders.csv")
    assert any(row["quantity"] and int(row["quantity"]) <= 0 for row in orders)
    assert any(row["order_date"] == "not-a-date" for row in orders)
    assert any(row["customer_id"] == "999" for row in orders)
    assert any(row["product_id"] == "999" for row in orders)
    assert any(row["status"] == "unknown_status" for row in orders)
    assert any(row["unit_price"] == "" for row in orders)


def test_input_columns_match_pipeline_contract() -> None:
    expected = {
        "customers.csv": {"customer_id", "full_name", "email", "city", "signup_date"},
        "products.csv": {"product_id", "product_name", "category", "unit_price"},
        "orders.csv": {"order_id", "customer_id", "product_id", "quantity", "order_date", "status", "unit_price"},
    }
    for filename, expected_columns in expected.items():
        with (RAW / filename).open(newline="", encoding="utf-8") as handle:
            actual_columns = set(next(csv.reader(handle)))
        assert actual_columns == expected_columns
