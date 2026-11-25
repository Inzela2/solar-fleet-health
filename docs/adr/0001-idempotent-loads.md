# ADR 0001: Re-runnable loads into ClickHouse

**Status:** accepted

## Context

The Postgres pipeline makes every load re-runnable with a staging table and
`INSERT ... ON CONFLICT DO UPDATE`: re-loading a date range (late data, a corrected file, a
backfill) updates rows in place and never duplicates them. ClickHouse has no `ON CONFLICT`
and no unique keys, so inserting the same month twice stores it twice. The ClickHouse store
needs the same guarantee by a different route.

## Options

**1. `ReplacingMergeTree` keyed on `(site_id, channel, ts)`.** Duplicates are removed only
when ClickHouse merges parts in the background, at a time it chooses. Until then every query
must add `FINAL` (or deduplicate by hand) to be correct. `FINAL` makes scans slower, and
forgetting it in one query gives silently inflated sums, which is the worst kind of bug in a
reporting system.

**2. Partition swap.** Partition fact tables by local month. To load a month, build its full
contents in a staging table (`CREATE TABLE stage AS readings`), then
`ALTER TABLE readings REPLACE PARTITION <month> FROM stage`. The swap is atomic per partition
and exact: after it, the month holds exactly the staged rows. Queries never need `FINAL`.
This is the same shape as the Postgres design (stage, then apply), at month granularity.

**3. Lightweight `DELETE` then `INSERT`.** Two statements, not atomic: a query between them
sees the month missing, and a crash between them loses it.

## Decision

Partition swap (option 2) for `readings`, `quarantine` and `daily_site_metrics`, partitioned
by `toYYYYMM(day)` where `day` is the **local** (Australia/Sydney) date. The pipeline loads by
local month, and a UTC-month partition would split every local month across two partitions.

`ReplacingMergeTree` is used only for `sites`: 300 rows, where `FINAL` costs nothing.

Details:

- **Partial-month reloads.** If a reload covers only some days of a month, the other days are
  copied from the live partition into staging first, so they survive the swap. Tested in
  `test_partial_month_reload_changes_only_its_days`.
- **Metrics rebuild whole months.** `ch.rebuild(a, b)` recomputes every month touching
  `[a, b]` and swaps each in. Days outside the range come out identical unless their readings
  changed, in which case recomputing them is right anyway. Tested against Postgres's
  exact-range rebuild in `test_metrics_rebuild_for_one_day_matches_full_rebuild`.
- **Crash safety.** The staging table name is deterministic per table and month, dropped
  before use and in a `finally`, so a crashed run leaves nothing that a re-run won't clean up.
- **Empty months.** If staging ends up empty, `REPLACE PARTITION` clears the month (checked
  on ClickHouse 24.8), which is the right result when a reload leaves no valid rows.

## Why not an incremental materialised view for daily metrics

A materialised view aggregates each insert as it arrives. Two problems here:

1. Re-loading a month inserts it again, so the view's sums would double. Swapping the base
   partition does not retract what the view already counted.
2. The fleet median for a day needs every site's total for that day before it can be
   computed, and a median cannot be built up incrementally from partial sums the way a sum
   can.

Recomputing a month from raw readings takes well under a second in ClickHouse, so a batch
rebuild is simpler and always exact.

## Consequences

- A load replaces a whole month's partition, so concurrent writers to the same month would
  race. The pipeline is a single batch job, so this is acceptable; a scheduler should not run
  two loads for the same month at once.
- Postgres stays the system of record for run bookkeeping (`ingest_runs`), alerts and health
  checks, which are small, transactional, and row-updated.
