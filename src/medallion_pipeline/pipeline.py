"""End-to-end batch pipeline: CSV -> Bronze -> Silver -> Gold (Delta Lake)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from medallion_pipeline.config import (
    BRONZE_DIR,
    GOLD_DIR,
    LAKE_DIR,
    PROJECT_ROOT,
    QUARANTINE_DIR,
    RAW_DATA_DIR,
    REPORTS_DIR,
    REVENUE_STATUSES,
    SILVER_DIR,
    VALID_ORDER_STATUSES,
)


EMAIL_PATTERN = r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def build_spark_session(app_name: str = "MedallionEcommercePipeline") -> SparkSession:
    """Create a local Spark session configured for Delta Lake."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _delta_write(df: DataFrame, path: Path) -> None:
    """Write a Delta table, replacing the previous local demo run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def _delta_read(spark: SparkSession, path: Path) -> DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Delta table not found: {path}. Run the pipeline first.")
    return spark.read.format("delta").load(str(path))


def _bronze_ingest(spark: SparkSession, entity: str) -> DataFrame:
    """Read raw CSV columns as strings and persist source metadata to Bronze."""
    source_path = RAW_DATA_DIR / f"{entity}.csv"
    if not source_path.is_file():
        raise FileNotFoundError(f"Missing source file: {source_path}")

    raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("emptyValue", "")
        .load(str(source_path))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.lit(source_path.name))
    )
    _delta_write(raw, BRONZE_DIR / entity)
    return raw


def _reasoned_split(df: DataFrame, reason_expression: Any) -> tuple[DataFrame, DataFrame]:
    """Return (valid, rejected) frames, with a rejection_reason on rejected rows."""
    tagged = df.withColumn("_rejection_reason", reason_expression)
    valid = tagged.filter(F.col("_rejection_reason").isNull()).drop("_rejection_reason")
    rejected = (
        tagged.filter(F.col("_rejection_reason").isNotNull())
        .withColumnRenamed("_rejection_reason", "rejection_reason")
    )
    return valid, rejected


def _deduplicate(
    df: DataFrame,
    key: str,
    order_columns: list[Any],
    rejection_code: str,
) -> tuple[DataFrame, DataFrame]:
    """Keep one deterministic record per key and quarantine extra occurrences."""
    window = Window.partitionBy(key).orderBy(*order_columns)
    ranked = df.withColumn("_dedupe_rank", F.row_number().over(window))
    clean = ranked.filter(F.col("_dedupe_rank") == 1).drop("_dedupe_rank")
    duplicates = (
        ranked.filter(F.col("_dedupe_rank") > 1)
        .drop("_dedupe_rank")
        .withColumn("rejection_reason", F.lit(rejection_code))
    )
    return clean, duplicates


def _save_silver_and_rejects(
    entity: str, valid: DataFrame, rejects: DataFrame
) -> tuple[DataFrame, int, int]:
    _delta_write(valid, SILVER_DIR / entity)
    _delta_write(rejects, QUARANTINE_DIR / entity)
    return valid, valid.count(), rejects.count()


def _clean_customers(spark: SparkSession) -> tuple[DataFrame, int, int, int]:
    raw = _delta_read(spark, BRONZE_DIR / "customers")
    parsed = raw.select(
        F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
        F.trim(F.col("full_name")).alias("full_name"),
        F.when(F.length(F.trim(F.col("email"))) > 0, F.lower(F.trim(F.col("email"))))
        .otherwise(F.lit(None).cast("string"))
        .alias("email"),
        F.when(F.length(F.trim(F.col("city"))) > 0, F.initcap(F.trim(F.col("city"))))
        .otherwise(F.lit("Unknown"))
        .alias("city"),
        F.expr("try_cast(signup_date AS DATE)").alias("signup_date"),
        "ingested_at",
        "source_file",
    )

    invalid_reason = (
        F.when(F.col("customer_id").isNull() | (F.col("customer_id") <= 0), "INVALID_CUSTOMER_ID")
        .when(F.col("full_name").isNull() | (F.length(F.col("full_name")) == 0), "MISSING_CUSTOMER_NAME")
        .when(F.col("signup_date").isNull(), "INVALID_SIGNUP_DATE")
        .when(F.col("email").isNotNull() & ~F.col("email").rlike(EMAIL_PATTERN), "INVALID_EMAIL")
    )
    valid_fields, bad_fields = _reasoned_split(parsed, invalid_reason)
    valid_deduped, duplicate_rows = _deduplicate(
        valid_fields,
        "customer_id",
        [F.col("signup_date").desc_nulls_last(), F.col("email").desc_nulls_last(), F.col("full_name").asc()],
        "DUPLICATE_CUSTOMER_ID",
    )
    rejects = bad_fields.unionByName(duplicate_rows)
    valid, clean_count, reject_count = _save_silver_and_rejects("customers", valid_deduped, rejects)
    return valid, parsed.count(), clean_count, reject_count


def _clean_products(spark: SparkSession) -> tuple[DataFrame, int, int, int]:
    raw = _delta_read(spark, BRONZE_DIR / "products")
    parsed = raw.select(
        F.expr("try_cast(product_id AS BIGINT)").alias("product_id"),
        F.trim(F.col("product_name")).alias("product_name"),
        F.when(F.length(F.trim(F.col("category"))) > 0, F.initcap(F.trim(F.col("category"))))
        .otherwise(F.lit("Uncategorized"))
        .alias("category"),
        F.expr("try_cast(unit_price AS DECIMAL(12,2))").alias("unit_price"),
        "ingested_at",
        "source_file",
    )
    invalid_reason = (
        F.when(F.col("product_id").isNull() | (F.col("product_id") <= 0), "INVALID_PRODUCT_ID")
        .when(F.col("product_name").isNull() | (F.length(F.col("product_name")) == 0), "MISSING_PRODUCT_NAME")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "INVALID_UNIT_PRICE")
    )
    valid_fields, bad_fields = _reasoned_split(parsed, invalid_reason)
    valid_deduped, duplicate_rows = _deduplicate(
        valid_fields,
        "product_id",
        [F.col("product_name").asc(), F.col("unit_price").desc_nulls_last()],
        "DUPLICATE_PRODUCT_ID",
    )
    rejects = bad_fields.unionByName(duplicate_rows)
    valid, clean_count, reject_count = _save_silver_and_rejects("products", valid_deduped, rejects)
    return valid, parsed.count(), clean_count, reject_count


def _clean_orders(
    spark: SparkSession, customers: DataFrame, products: DataFrame
) -> tuple[DataFrame, int, int, int]:
    raw = _delta_read(spark, BRONZE_DIR / "orders")
    parsed = raw.select(
        F.expr("try_cast(order_id AS BIGINT)").alias("order_id"),
        F.expr("try_cast(customer_id AS BIGINT)").alias("customer_id"),
        F.expr("try_cast(product_id AS BIGINT)").alias("product_id"),
        F.expr("try_cast(quantity AS INT)").alias("quantity"),
        F.expr("try_cast(order_date AS DATE)").alias("order_date"),
        F.upper(F.trim(F.col("status"))).alias("status"),
        F.expr("try_cast(unit_price AS DECIMAL(12,2))").alias("unit_price"),
        "ingested_at",
        "source_file",
    )

    valid_status = F.col("status").isin(*VALID_ORDER_STATUSES)
    invalid_reason = (
        F.when(F.col("order_id").isNull() | (F.col("order_id") <= 0), "INVALID_ORDER_ID")
        .when(F.col("customer_id").isNull() | (F.col("customer_id") <= 0), "INVALID_CUSTOMER_ID")
        .when(F.col("product_id").isNull() | (F.col("product_id") <= 0), "INVALID_PRODUCT_ID")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY")
        .when(F.col("order_date").isNull(), "INVALID_ORDER_DATE")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "INVALID_UNIT_PRICE")
        .when(~valid_status, "INVALID_ORDER_STATUS")
    )
    valid_fields, bad_fields = _reasoned_split(parsed, invalid_reason)

    customer_keys = customers.select("customer_id").withColumn("_customer_found", F.lit(True))
    product_keys = products.select("product_id").withColumn("_product_found", F.lit(True))
    with_references = (
        valid_fields.join(customer_keys, on="customer_id", how="left")
        .join(product_keys, on="product_id", how="left")
    )
    reference_reason = (
        F.when(F.col("_customer_found").isNull(), "UNKNOWN_CUSTOMER_ID")
        .when(F.col("_product_found").isNull(), "UNKNOWN_PRODUCT_ID")
    )
    valid_refs, bad_refs_with_flags = _reasoned_split(with_references, reference_reason)
    bad_refs = bad_refs_with_flags.drop("_customer_found", "_product_found")
    valid_refs = valid_refs.drop("_customer_found", "_product_found")

    valid_deduped, duplicate_rows = _deduplicate(
        valid_refs,
        "order_id",
        [F.col("order_date").desc_nulls_last(), F.col("customer_id").asc(), F.col("product_id").asc()],
        "DUPLICATE_ORDER_ID",
    )
    rejects = bad_fields.unionByName(bad_refs).unionByName(duplicate_rows)
    valid, clean_count, reject_count = _save_silver_and_rejects("orders", valid_deduped, rejects)
    return valid, parsed.count(), clean_count, reject_count


def _build_gold(spark: SparkSession, customers: DataFrame, products: DataFrame, orders: DataFrame) -> dict[str, DataFrame]:
    """Build business-facing aggregate tables from completed/delivered orders."""
    revenue_orders = (
        orders.filter(F.col("status").isin(*REVENUE_STATUSES))
        .withColumn("order_revenue", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)"))
    )

    product_dim = products.select("product_id", "product_name", "category")
    enriched = revenue_orders.join(product_dim, on="product_id", how="inner")

    monthly_sales = (
        enriched.withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .groupBy("order_year", "order_month", "category")
        .agg(
            F.countDistinct("order_id").alias("completed_orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("order_revenue"), 2).alias("total_revenue"),
        )
        .orderBy("order_year", "order_month", "category")
    )

    customer_spend = revenue_orders.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("completed_orders"),
        F.round(F.sum("order_revenue"), 2).alias("total_spend"),
        F.round(F.avg("order_revenue"), 2).alias("average_order_value"),
    )
    customer_summary = (
        customers.select("customer_id", "full_name", "email", "city", "signup_date")
        .join(customer_spend, on="customer_id", how="left")
        .fillna({"completed_orders": 0})
        .withColumn("total_spend", F.coalesce(F.col("total_spend"), F.lit(0).cast("decimal(18,2)")))
        .withColumn("average_order_value", F.coalesce(F.col("average_order_value"), F.lit(0).cast("decimal(18,2)")))
        .orderBy(F.col("total_spend").desc(), F.col("customer_id").asc())
    )

    product_sales = revenue_orders.groupBy("product_id").agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("completed_orders"),
        F.round(F.sum("order_revenue"), 2).alias("total_revenue"),
    )
    product_performance = (
        products.select("product_id", "product_name", "category", "unit_price")
        .join(product_sales, on="product_id", how="left")
        .fillna({"units_sold": 0, "completed_orders": 0})
        .withColumn("total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0).cast("decimal(18,2)")))
        .orderBy(F.col("total_revenue").desc(), F.col("product_id").asc())
    )

    outputs = {
        "monthly_sales": monthly_sales,
        "customer_summary": customer_summary,
        "product_performance": product_performance,
    }
    for name, frame in outputs.items():
        _delta_write(frame, GOLD_DIR / name)
    return outputs


def _has_duplicate_keys(df: DataFrame, key: str) -> bool:
    return df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count() > 0


def run_pipeline(project_root: Path | None = None) -> dict[str, Any]:
    """Execute the full-refresh medallion pipeline and return a run report."""
    global RAW_DATA_DIR, LAKE_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, QUARANTINE_DIR, REPORTS_DIR
    if project_root is not None:
        root = Path(project_root).resolve()
        RAW_DATA_DIR = root / "data" / "raw"
        LAKE_DIR = root / "data" / "lake"
        BRONZE_DIR = LAKE_DIR / "bronze"
        SILVER_DIR = LAKE_DIR / "silver"
        GOLD_DIR = LAKE_DIR / "gold"
        QUARANTINE_DIR = SILVER_DIR / "_quarantine"
        REPORTS_DIR = root / "reports"

    LAKE_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    spark = build_spark_session()
    started_at = datetime.now(timezone.utc).isoformat()

    try:
        bronze_counts: dict[str, int] = {}
        for entity in ("customers", "products", "orders"):
            bronze_counts[entity] = _bronze_ingest(spark, entity).count()

        customers, customer_input, customer_clean, customer_rejected = _clean_customers(spark)
        products, product_input, product_clean, product_rejected = _clean_products(spark)
        orders, order_input, order_clean, order_rejected = _clean_orders(spark, customers, products)

        gold = _build_gold(spark, customers, products, orders)
        checks = {
            "silver_customer_ids_are_unique": not _has_duplicate_keys(customers, "customer_id"),
            "silver_product_ids_are_unique": not _has_duplicate_keys(products, "product_id"),
            "silver_order_ids_are_unique": not _has_duplicate_keys(orders, "order_id"),
            "gold_revenue_is_non_negative": gold["monthly_sales"].filter(F.col("total_revenue") < 0).limit(1).count() == 0,
        }
        if not all(checks.values()):
            failed = [name for name, ok in checks.items() if not ok]
            raise RuntimeError(f"Required data-quality checks failed: {failed}")

        report: dict[str, Any] = {
            "project": "Ecommerce Medallion Data Engineering Pipeline",
            "run_started_at_utc": started_at,
            "run_finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "execution_mode": "local full refresh; synthetic input data",
            "source_rows": {"customers": customer_input, "products": product_input, "orders": order_input},
            "bronze_rows": bronze_counts,
            "silver_rows": {"customers": customer_clean, "products": product_clean, "orders": order_clean},
            "quarantined_rows": {
                "customers": customer_rejected,
                "products": product_rejected,
                "orders": order_rejected,
            },
            "gold_rows": {name: frame.count() for name, frame in gold.items()},
            "quality_checks": checks,
            "revenue_policy": "Only COMPLETED and DELIVERED orders contribute to Gold revenue metrics.",
        }
        report_path = REPORTS_DIR / "quality_report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print("\nPipeline completed successfully.")
        print(json.dumps(report, indent=2))
        print(f"\nQuality report: {report_path}")
        print("Gold tables: data/lake/gold/monthly_sales, customer_summary, product_performance")
        return report
    finally:
        spark.stop()


def main() -> None:
    run_pipeline(PROJECT_ROOT)


if __name__ == "__main__":
    main()
