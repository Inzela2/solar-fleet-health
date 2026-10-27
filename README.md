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

## Query tuning (10.5M rows, 523 MB table)

| Recompute one day (backfill) | Time | Index size |
|---|---|---|
| Primary key only | 549 ms | – |
| BRIN index on `ts` | 67 ms | 32 kB |
| B-tree index on `ts` | 10 ms | 69 MB |

BRIN gives an 8× speed-up for a tiny index because data arrives in time order. B-tree is faster again but 2,000× larger. For a daily batch job BRIN is the better trade-off; for interactive time queries, B-tree.

## Run it

```bash
docker compose up -d                       # PostgreSQL 16
pip install -e ".[dev]"
pytest                                     # 12 tests, no database needed
python -m fleethealth backfill             # download, load, metrics, alerts (~5 min)
python -m fleethealth health --as-of 2013-06-30
uvicorn fleethealth.api:app                # /sites/{id}/daily, /alerts, /health
```

Data: [Ausgrid Solar Home Electricity Data](https://www.ausgrid.com.au/Industry/Our-Research/Data-to-share/Solar-home-electricity-data) (CC BY), via a pinned GitHub mirror.

**Deliberately left out:** Airflow/Dagster and cloud services. One daily batch job doesn't need an orchestrator yet; the CLI is designed so a scheduler can call `backfill` and `health` directly.
