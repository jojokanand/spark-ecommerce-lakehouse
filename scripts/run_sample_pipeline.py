"""Build and validate the reproducible local sample lakehouse."""

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PIPELINE_STEPS = (
    ("Bronze orders", "phases/phase02-bronze/bronze_ingest_orders.py"),
    ("Bronze customers", "phases/phase02-bronze/bronze_ingest_customers.py"),
    ("Bronze order items", "phases/phase02-bronze/bronze_ingest_items.py"),
    ("Bronze products", "phases/phase02-bronze/bronze_ingest_products.py"),
    ("Silver orders", "phases/phase03-silver/silver_orders.py"),
    ("Silver customers", "phases/phase03-silver/silver_customers.py"),
    ("Silver order items", "phases/phase03-silver/silver_order_items.py"),
    ("Silver products", "phases/phase03-silver/silver_products.py"),
    ("Gold daily revenue", "phases/phase05-gold/gold_daily_revenue.py"),
    (
        "Gold customer ML features",
        "phases/phase12-ml-features/gold_customer_features_ml.py",
    ),
)


def run_step(label: str, command: list[str]) -> None:
    """Run one pipeline step from the repository root."""
    environment = os.environ.copy()
    environment["PYSPARK_PYTHON"] = sys.executable

    print(f"\n==> {label}", flush=True)
    subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and validate the reproducible local sample lakehouse."
    )
    parser.add_argument(
        "--scale",
        type=int,
        default=1,
        help="synthetic-data scale multiplier (default: 1)",
    )
    args = parser.parse_args()

    if args.scale < 1:
        parser.error("--scale must be at least 1")

    run_step(
        "Generate deterministic landing data",
        [
            sys.executable,
            "phases/phase01-data-generation/generate_data.py",
            "--scale",
            str(args.scale),
        ],
    )

    for label, script in PIPELINE_STEPS:
        run_step(label, [sys.executable, script])

    run_step(
        "Phase 13 data contracts",
        [
            sys.executable,
            "-m",
            "pytest",
            "phases/phase13-testing/",
            "-q",
        ],
    )

    print("\nSample pipeline completed successfully.", flush=True)


if __name__ == "__main__":
    main()