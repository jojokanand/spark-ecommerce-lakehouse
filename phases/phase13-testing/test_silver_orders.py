"""
test_silver_orders.py — data-quality contract for the silver orders table.

These are the invariants silver PROMISES downstream consumers. If any fail,
the silver job has a bug or the data drifted -- either way, block the pipeline.
"""

from pyspark.sql import functions as F

SILVER = "data/silver/orders/"
BRONZE = "data/bronze/orders/"
REJECTED = "data/silver/rejected_orders/"

def test_no_null_keys(spark):
    """tenant_id / order_id / customer_id / order_date must never be null."""
    df = spark.read.parquet(SILVER)
    bad = df.filter(
        F.col("tenant_id").isNull() | F.col("order_id").isNull() |
        F.col("customer_id").isNull() | F.col("order_date").isNull()
    ).count()
    assert bad == 0, f"{bad} rows with null keys survived into silver"


def test_no_negative_totals(spark):
    """total_amount must be >= 0 (we quarantine negatives)."""
    df = spark.read.parquet(SILVER)
    assert df.filter(F.col("total_amount") < 0).count() == 0


def test_order_id_unique_per_tenant(spark):
    """(tenant_id, order_id) is the grain -> must be unique after dedup."""
    df = spark.read.parquet(SILVER)
    total = df.count()
    distinct = df.select("tenant_id", "order_id").distinct().count()
    assert total == distinct, f"{total - distinct} duplicate (tenant, order) keys remain"


def test_row_counts_reconcile_and_retain_expected_share(spark):
    """Silver and rejected rows must reconcile with bronze at any data scale."""
    bronze_count = spark.read.parquet(BRONZE).count()
    silver_count = spark.read.parquet(SILVER).count()
    rejected_count = spark.read.parquet(REJECTED).count()

    assert bronze_count > 0, "bronze orders must not be empty"
    assert silver_count + rejected_count == bronze_count, (
        f"row counts do not reconcile: "
        f"{silver_count:,} silver + {rejected_count:,} rejected "
        f"!= {bronze_count:,} bronze"
    )

    retention = silver_count / bronze_count
    assert retention >= 0.95, (
        f"silver retained only {retention:.1%} of bronze rows; expected at least 95%"
    )
