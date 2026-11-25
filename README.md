# Solar fleet health pipeline

![tests](https://github.com/Inzela2/solar-fleet-health/actions/workflows/tests.yml/badge.svg)

A production-style data pipeline for **300 real rooftop solar installations** (Ausgrid Solar Home data, Sydney, 2012–13, 10.5 million half-hourly readings). It loads telemetry into PostgreSQL, validates it, builds the daily metrics a dashboard would read, and raises alerts when an installation fails or underperforms.

**The question:** can we spot a faulty solar system before its owner notices, without flooding the support team with false alarms?

## Results

| Fault injected into 250 healthy sites | Caught within 7 days | Median days to first alert |
|---|---|---|
| Complete outage | 100% | same day |
| Output halved | 99.6% | 1 day |
| Output down one-third (e.g. one panel string failed) | 92.4% | 2 days |

False alarms: 9.2 per 1,000 site-days at the chosen threshold. A stricter threshold (0.6) cut false alarms to 2.9 but caught only 30% of one-third drops, so 0.7 was chosen as the balance (full table in `results/detection_evaluation.csv`).

On the real data, the pipeline found **35 sites with whole days of zero generation** (some for 100+ days), **2 sites whose meters record generation at night**, and **15 sites producing more than their listed capacity allows**, meaning the site metadata is wrong.

## Data problems found and how the pipeline handles them

| Problem | Evidence | Handling |
|---|---|---|
| Meters record clock time, not standard time | Midday generation shifts by exactly 1 hour on daylight-saving dates | Timestamps localised to Australia/Sydney |
| Day clocks go forward: 2 slots for an hour that never happened | Zero for all 300 homes | Quarantined with reason (1,200 rows), never stored as real zeros |
| Day clocks go back: 2 slots hold two half-hours each | Values ~2× neighbours | Kept (preserves daily totals) and flagged `merged_dst_hour` |
| Wrong capacity metadata | Peak output > 110% of rated | Site flagged, data still loaded |
| Night generation | > 0.05 kWh between 10pm and 4am on 7+ days | Site flagged |

## Design decisions

| Decision | Why |
|---|---|
| Staging table + `INSERT ... ON CONFLICT DO UPDATE` | Re-running any date range (late data, corrections, backfills) updates in place, never duplicates. Quarantine rows for a range are replaced on re-run too (a bug I found and fixed). |
| Quarantine table instead of dropping bad rows | Every rejected row keeps its reason, so data loss is visible and auditable. |
| `ingest_runs` table | Each batch records rows in, loaded and quarantined, plus status; a failed run is marked failed. |
| Primary key `(site_id, channel, ts)` | The dashboard's main query is one site over time: a single index range scan. |
| Precomputed `daily_site_metrics` | Dashboards and alerts read daily numbers constantly; computing them once is cheaper than re-aggregating raw readings. |
| Compare each site to the **fleet median** and its **own baseline** | The fleet median removes weather; the site's own 28-day baseline removes roof angle and shading differences. |
| Thresholds chosen by injecting known faults | Real faults have no labels, so detection was measured, not guessed. |
| `health` command exits non-zero | Can run on a schedule and alert when the pipeline itself is stale or failing. |
| ClickHouse alongside Postgres, not instead | Postgres keeps small transactional tables (runs, alerts) and point lookups; ClickHouse takes wide analytical scans. |
| ClickHouse loads by **partition swap** | ClickHouse has no `ON CONFLICT`. Each month is built in a staging table and swapped in atomically, so reloads are exact and queries never need `FINAL` ([ADR 0001](docs/adr/0001-idempotent-loads.md)). |
| ClickHouse partitions by **local** month, with `day` stored | The pipeline loads by Sydney month; UTC partitions would split each one in two. |
| `quantileExactInclusive`, not `quantileExact` | Matches Postgres `percentile_cont` (interpolates). The parity test fails if this is changed. |
| Validation runs once for both stores | `load.prepare` builds one batch; both stores receive identical rows. |

## Query tuning (10.5M rows, 523 MB table)

| Recompute one day (backfill) | Time | Index size |
|---|---|---|
| Primary key only | 549 ms | – |
| BRIN index on `ts` | 67 ms | 32 kB |
| B-tree index on `ts` | 10 ms | 69 MB |

BRIN gives an 8× speed-up for a tiny index because data arrives in time order. B-tree is faster again but 2,000× larger. For a daily batch job BRIN is the better trade-off; for interactive time queries, B-tree.

## PostgreSQL vs ClickHouse

The same validated data also loads into ClickHouse (`--backend both`), so the two can be
compared on real questions. Postgres stays the system of record (run bookkeeping, alerts,
health checks); ClickHouse is the analytics store. Every timing below only counts if both
databases returned the same answer (`python -m fleethealth compare`, full output in
`results/pg_vs_clickhouse.json`).

**Setup:** 10.5M readings, PostgreSQL 16 vs ClickHouse 24.8, same machine, **1 CPU core and
3 GB RAM**. Postgres gets its best index per query (BRIN or B-tree on `ts`, both tried).
Median of warm runs, including fetching results.

| Query | Postgres | ClickHouse | Faster |
|---|---|---|---|
| One site, one month, from the daily table | 0.1 ms | 2.7 ms | Postgres, 27× |
| One site, one month, from raw readings | 0.7 ms | 3.5 ms | Postgres, 5× |
| All readings for one day | 9.5 ms | 8.9 ms | about the same |
| Avg performance per postcode per month (daily table) | 34 ms | 9.1 ms | ClickHouse, 3.8× |
| Median daily yield per postcode, full year (raw) | 2.7 s | 170 ms | ClickHouse, 16× |
| Small vs large systems, monthly totals, full year (raw) | 16.8 s | 572 ms | ClickHouse, 29× |
| Rebuild a year of daily metrics | 6.4 s | 460 ms | ClickHouse, 14× |
| Re-load one month of readings (893k rows) | 13.4 s | 699 ms | ClickHouse, 19× |

| Storage for `readings` | Size |
|---|---|
| Postgres table | 523 MB |
| Postgres primary-key index | 316 MB |
| ClickHouse, compressed on disk | 21 MB (25× smaller than the Postgres table alone, 40× with its index) |

**What this says:** Postgres wins on point lookups, which is what its primary key is built for,
and at these sizes those queries are fast enough on either. ClickHouse wins wherever a query
reads most of the table: cohort breakdowns, fleet-wide rebuilds and bulk reloads. At 10.5M
rows Postgres is still usable for the nightly batch; the case for ClickHouse is interactive
cohort slicing (seconds become sub-second) and storage. ClickHouse normally spreads a query
across all cores, so with one core these numbers understate its advantage on a normal
machine; re-run `compare` on your own hardware.

**Correctness:** daily metrics agree on all 109,419 site-days to float32 rounding (max relative
difference 1e-6), with identical NULLs and quarantine counts. One thing the comparison found:
Postgres `sum()` over a `real` column accumulates in single precision, so the fleet's total
generation is off by 0.1% (2,328,310 vs 2,330,578 kWh). Daily sums of 48 values are
unaffected; the benchmark sums as `float8`.

**Compression choice, measured** (kWh column, 10.5M values): ZSTD(1) 18.9 MB, LZ4 22.0 MB,
Gorilla+ZSTD 26.9 MB, FPC+ZSTD 26.3 MB. Gorilla is designed for slowly changing floats, but
half-hourly solar readings jump around and are often zero, so plain ZSTD wins. Storing whole
watt-hours as integers with T64+ZSTD would be 8.7 MB, a possible next step.

## Run it

```bash
docker compose up -d                       # PostgreSQL 16 + ClickHouse 24.8
pip install -e ".[dev]"
pytest -m "not integration"                # 13 quick tests, no database needed
pytest -m integration                      # Postgres/ClickHouse parity and reload tests
python -m fleethealth backfill --backend both   # download, load both stores, metrics, alerts
python -m fleethealth health --as-of 2013-06-30
python -m fleethealth compare              # benchmark both, writes results/pg_vs_clickhouse.json
```

Data: [Ausgrid Solar Home Electricity Data](https://www.ausgrid.com.au/Industry/Our-Research/Data-to-share/Solar-home-electricity-data) (CC BY), via a pinned GitHub mirror.

**Deliberately left out:** Airflow/Dagster and cloud services. One daily batch job doesn't need an orchestrator yet; the CLI is designed so a scheduler can call `backfill` and `health` directly.
