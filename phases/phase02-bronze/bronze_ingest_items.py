"""bronze_ingest_items.py — Phase 2 pattern for order_items (raw CSV -> Parquet)."""

import os
import sys

sys.path.append(os.path.dirname(__file__))
from _spark import get_spark
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ITEMS_SCHEMA = StructType([
    StructField("tenant_id",     StringType(),  False),
    StructField("order_id",      StringType(),  False),
    StructField("order_item_id", StringType(),  False),
    StructField("product_id",    StringType(),  True),
    StructField("quantity",      IntegerType(), True),
    StructField("unit_price",    DoubleType(),  True),
    StructField("item_revenue",  DoubleType(),  True),
])


def main():
    spark = get_spark("bronze_ingest_items")
    items = (
        spark.read.schema(ITEMS_SCHEMA).option("header", "true")
        .csv("data/landing/order_items/")
    )
    items.write.mode("overwrite").parquet("data/bronze/order_items/")
    print(f"landing rows : {items.count():,}")
    print(f"bronze rows  : {spark.read.parquet('data/bronze/order_items/').count():,}")
    spark.stop()


if __name__ == "__main__":
    main()
