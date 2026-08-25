# Spark Ecommerce Lakehouse — a hands-on textbook

Build **one evolving project** — a multi-tenant ecommerce analytics lakehouse — and
learn Apache Spark end to end: DataFrames, the medallion architecture (bronze →
silver → gold), joins, window functions, partitioning, plan/UI debugging,
performance & skew tuning, incremental processing, Structured Streaming, ML feature
engineering, testing, and production hardening.

Every phase follows one loop: **build a job → run it on real data → inspect the
output and the physical plan → break it on purpose → fix it → write down the lesson.**

---

## How this repo is organized

```
book/        the textbook — one chapter per phase (read these)
phases/      the code — one folder per phase, each self-contained & runnable
docs/        reference: architecture, data model, runbook, tuning notes
data/        the lake (bronze/silver/gold) — gitignored, regenerate locally
```

Each `phases/phaseNN-*/` folder carries its own copy of `_spark.py` so it runs
standalone. **Run everything from the repo root** (paths like `data/silver/...` are
relative to it).

## The textbook (read in order)

| # | Chapter | What you learn |
|---|---|---|
| 0 | [Setup & mental model](book/00-setup.md) | SparkSession, driver/executors, lazy eval, actions vs transformations, the shuffle |
| 1 | [Generate realistic data](book/01-data-generation.md) | schemas, cardinality, **skew**, injected data-quality problems, reproducibility |
| 2 | [Bronze ingestion](book/02-bronze.md) | explicit schemas, CSV/JSON → Parquet, column pruning, predicate pushdown |
| 3 | [Silver cleaning](book/03-silver.md) | validation, the `rejection_reason` tag pattern, dedup windows, reconciliation |
| 4 | [Joins & enrichment](book/04-joins.md) | composite keys, the multi-tenant trap, SortMergeJoin vs BroadcastHashJoin |
| 5 | [Gold marts](book/05-gold.md) | groupBy, wide transformations, the shuffle, multi-key aggregation |
| 6 | [Window functions](book/06-windows.md) | LTV, ranking, `row_number`/`rank`/`dense_rank`, window vs groupBy |
| 7 | [Partitioning lab](book/07-partitioning.md) | pruning, small files, `partitionBy` vs `repartition`/`coalesce` |
| 8 | [Plans & the Spark UI](book/08-plans-ui.md) | logical→physical plans, Jobs/Stages/Tasks, finding the slow stage |
| 9 | [Performance & skew](book/09-performance.md) | shuffle partitions, spill, broadcast, skew diagnosis, salting |
| 10 | [Incremental & backfills](book/10-incremental.md) | date windows, dynamic partition overwrite, idempotency, the static footgun |
| 11 | [Structured Streaming](book/11-streaming.md) | readStream/writeStream, micro-batches, watermarks, checkpoints |
| 12 | [ML feature engineering](book/12-ml-features.md) | point-in-time correctness, leakage, self-joins for affinity |
| 13 | [Testing & data contracts](book/13-testing.md) | pytest, invariants, referential integrity, metric reconciliation |
| 14 | [Production hardening](book/14-production.md) | runbooks, job metrics, ownership, SLAs |
| 15 | [OLTP vs OLAP (Spanner)](book/15-spanner.md) | system boundaries, CDC, why Spark is not your serving layer |
| — | [Appendix: conventions & gotchas](book/appendix-gotchas.md) | env setup, `F`/`lit` conventions, and the traps that cost us time |

---

## Setup

Requires **Python 3.12**, **Java 17 or later**, and
[`uv`](https://docs.astral.sh/uv/).

```bash
# create .venv and install the exact locked dependencies
uv sync --locked

# verify that local Spark runs
uv run python phases/phase00-setup/smoke.py
```

## Quickstart — build and validate the sample pipeline

The default command generates a deterministic scale-1 dataset with 100,000 base
orders, builds the Parquet-backed Bronze and Silver layers, materializes the daily
revenue mart and point-in-time customer features, and runs the Phase 13 data
contracts.

```bash
uv run python scripts/run_sample_pipeline.py
```

A complete scale-1 run takes approximately 1–2 minutes on a modern laptop.
Generated data is written under `data/` and is excluded from version control.

To exercise the same pipeline with approximately one million base orders:

```bash
uv run python scripts/run_sample_pipeline.py --scale 10
```

Every phase remains directly executable for focused learning; the runner provides
the tested end-to-end path.

## Explore interactively
```bash
uv run python -i phases/phase08-plans-ui/explore.py
# then, e.g.:  daily_revenue.orderBy(F.desc('revenue')).show(5)
```

## The one idea that matters most
`filter` is a **narrow** transformation (per-row, no data movement). `groupBy`/`join`
are **wide** transformations — they trigger a **shuffle** (`Exchange` in the plan),
which is where Spark jobs get expensive. Learn to read a physical plan, find the
shuffles, and you're no longer just *using* Spark — you *understand* it.
