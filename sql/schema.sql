-- Rooftop solar fleet: raw telemetry, derived daily metrics, alerts and pipeline bookkeeping.
-- Idempotent: safe to run on every pipeline start.

CREATE TABLE IF NOT EXISTS sites (
    site_id      integer PRIMARY KEY,
    capacity_kw  numeric(6,2) NOT NULL CHECK (capacity_kw > 0),
    postcode     integer
);

-- One row per site, channel and half-hour. The primary key leads with site_id because the
-- dashboard's main question is "show me this installation over time", which becomes one
-- index range scan. Fleet-wide time queries read daily_site_metrics instead.
CREATE TABLE IF NOT EXISTS readings (
    site_id     integer     NOT NULL REFERENCES sites,
    channel     text        NOT NULL CHECK (channel IN ('GG', 'GC')),  -- generation, consumption
    ts          timestamptz NOT NULL,                                   -- end of the half-hour
    kwh         real        NOT NULL CHECK (kwh >= 0),
    flag        text,                                                   -- e.g. merged_dst_hour
    PRIMARY KEY (site_id, channel, ts)
);

-- Rows that failed validation are kept, with the reason, rather than silently dropped.
CREATE TABLE IF NOT EXISTS quarantine (
    id          bigserial PRIMARY KEY,
    run_id      integer,
    site_id     integer,
    channel     text,
    local_date  date,
    slot        text,
    value       real,
    reason      text NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id       serial PRIMARY KEY,
    started_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    date_from    date,
    date_to      date,
    rows_in      integer,
    rows_loaded  integer,
    rows_quarantined integer,
    status       text NOT NULL DEFAULT 'running'
);

-- Derived table the dashboard and alerting read. Rebuilt per date range, so backfills are a
-- delete-and-recompute of exactly the affected days.
CREATE TABLE IF NOT EXISTS daily_site_metrics (
    site_id          integer NOT NULL REFERENCES sites,
    day              date    NOT NULL,
    gen_kwh          real,
    cons_kwh         real,
    gen_intervals    smallint,
    yield_kwh_per_kw real,
    fleet_yield      real,     -- median yield of all reporting sites that day (weather proxy)
    perf_ratio       real,     -- site yield / fleet median yield
    PRIMARY KEY (site_id, day)
);
CREATE INDEX IF NOT EXISTS daily_site_metrics_day ON daily_site_metrics (day);

CREATE TABLE IF NOT EXISTS alerts (
    site_id     integer NOT NULL REFERENCES sites,
    day         date    NOT NULL,
    alert_type  text    NOT NULL,
    severity    text    NOT NULL CHECK (severity IN ('critical', 'warning', 'info')),
    detail      text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (site_id, day, alert_type)
);
CREATE INDEX IF NOT EXISTS alerts_day ON alerts (day);
