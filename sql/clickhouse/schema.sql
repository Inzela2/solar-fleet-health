-- ClickHouse analytics store. Same data as the Postgres tables, laid out for wide scans.
-- Idempotent: safe to run on every pipeline start. Statements are separated by ';'.
--
-- Every fact table is partitioned by LOCAL (Australia/Sydney) month, because that is the unit
-- the pipeline loads and re-loads. Re-running a month builds it in a staging table and swaps
-- the whole partition in atomically (see docs/adr/0001-idempotent-loads.md).

-- Tiny dimension table. ReplacingMergeTree keeps the latest row per site; queries read it
-- with FINAL, which costs nothing at 300 rows (unlike on `readings`).
CREATE TABLE IF NOT EXISTS sites (
    site_id      UInt16,
    capacity_kw  Float64,
    postcode     Nullable(UInt16),
    loaded_at    DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY site_id;

-- One row per site, channel and half-hour.
-- `day` is the local date the half-hour belongs to, stored rather than recomputed from `ts`
-- in every query: it is the partition key, and it lets a one-day query read one day's granules.
-- Sort key leads with site_id, so one site over time reads a contiguous range; fleet-wide
-- queries scan whole columns, which is what ClickHouse is built for.
CREATE TABLE IF NOT EXISTS readings (
    site_id  UInt16,
    channel  LowCardinality(String),
    day      Date                           CODEC(Delta, ZSTD(1)),
    ts       DateTime('UTC')                CODEC(Delta, ZSTD(1)),  -- end of the half-hour
    kwh      Float32                        CODEC(ZSTD(1)),          -- codec chosen by measurement, see README
    flag     LowCardinality(Nullable(String))
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (site_id, channel, ts);

CREATE TABLE IF NOT EXISTS quarantine (
    run_id      Nullable(UInt32),
    site_id     Nullable(UInt16),
    channel     LowCardinality(Nullable(String)),
    local_date  Date,
    slot        Nullable(String),
    value       Nullable(Float32),
    reason      LowCardinality(String),
    quarantined_at DateTime DEFAULT now()
) ENGINE = MergeTree
PARTITION BY toYYYYMM(local_date)
ORDER BY (local_date, reason);

-- Same columns as the Postgres table. Rebuilt a whole month at a time by partition swap.
CREATE TABLE IF NOT EXISTS daily_site_metrics (
    site_id          UInt16,
    day              Date,
    gen_kwh          Nullable(Float32),
    cons_kwh         Nullable(Float32),
    gen_intervals    UInt16,
    yield_kwh_per_kw Nullable(Float32),
    fleet_yield      Nullable(Float32),
    perf_ratio       Nullable(Float32)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (site_id, day);
