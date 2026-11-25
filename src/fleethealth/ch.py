"""ClickHouse analytics store: the same readings and daily metrics as Postgres, for wide queries.

Postgres stays the system of record (ingest_runs, alerts, health checks). ClickHouse receives
the same validated batches (load.prepare runs once) and answers fleet-wide questions.

Re-runnable loads without ON CONFLICT
-------------------------------------
ClickHouse has no upsert. Every write here goes through `_swap`: build the complete new
contents of one month in a staging table, then `ALTER TABLE ... REPLACE PARTITION ... FROM`
staging. The swap is atomic per partition and exact, so re-loading a month gives the same
rows, never duplicates, and queries never need FINAL. If only part of a month is re-loaded,
the untouched days are copied into staging first so they survive the swap.
"""
import datetime as dt
from pathlib import Path
import clickhouse_connect
import pandas as pd
from .config import CLICKHOUSE_DSN
from .metrics import MIN_INTERVALS_FOR_FLEET

SCHEMA = Path(__file__).resolve().parents[2] / "sql" / "clickhouse" / "schema.sql"


def connect(dsn: str = CLICKHOUSE_DSN, database: str | None = None):
    kw = {"database": database} if database else {}
    return clickhouse_connect.get_client(dsn=dsn, **kw)


def init_schema(client) -> None:
    sql = "\n".join(l for l in SCHEMA.read_text().splitlines() if not l.strip().startswith("--"))
    for stmt in sql.split(";"):
        if stmt.strip():
            client.command(stmt)


def _months(a: dt.date, b: dt.date) -> list[tuple[int, dt.date, dt.date]]:
    """(YYYYMM, first day, last day) for each calendar month overlapping [a, b]."""
    out, m = [], dt.date(a.year, a.month, 1)
    while m <= b:
        nxt = dt.date(m.year + (m.month == 12), m.month % 12 + 1, 1)
        out.append((m.year * 100 + m.month, m, nxt - dt.timedelta(days=1)))
        m = nxt
    return out


def _swap(client, table: str, day_col: str, month: int, keep_outside: tuple | None, fill) -> None:
    """Replace one monthly partition of `table` with freshly built contents.

    keep_outside=(a, b): copy existing rows of this month whose day is outside [a, b] into
    staging first, so a partial-month reload only changes the days it covers.
    fill(stage_name): writes the new rows into the staging table.
    """
    stage = f"{table}_stage_{month}"
    client.command(f"DROP TABLE IF EXISTS {stage}")          # leftover from a crashed run
    client.command(f"CREATE TABLE {stage} AS {table}")       # same columns, engine and partitioning
    try:
        if keep_outside:
            client.command(
                f"INSERT INTO {stage} SELECT * FROM {table} "
                f"WHERE toYYYYMM({day_col}) = {{m:UInt32}} "
                f"AND {day_col} NOT BETWEEN {{a:Date}} AND {{b:Date}}",
                parameters={"m": month, "a": keep_outside[0], "b": keep_outside[1]})
        fill(stage)
        # An empty stage clears the partition, which is right when a month has no rows left.
        # month is an int we computed, never user input, so inlining it is safe.
        client.command(f"ALTER TABLE {table} REPLACE PARTITION {int(month)} FROM {stage}")
    finally:
        client.command(f"DROP TABLE IF EXISTS {stage}")


def _readings_frame(valid: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "site_id": valid["site_id"].astype("uint16"),
        "channel": valid["channel"].astype(str),
        "day": pd.to_datetime(valid["local_date"]),
        "ts": valid["ts"].dt.tz_convert("UTC").dt.tz_localize(None),
        "kwh": valid["kwh"].astype("float32"),
        "flag": valid["flag"].astype(object).where(valid["flag"].notna(), None),
    })


def _quarantine_frame(rejected: pd.DataFrame, run_id) -> pd.DataFrame:
    return pd.DataFrame({
        "run_id": run_id,
        "site_id": rejected["site_id"].astype("Int64"),
        "channel": rejected["channel"].astype(object),
        "local_date": pd.to_datetime(rejected["local_date"]),
        "slot": rejected["slot"].astype(object),
        "value": rejected["kwh"].astype("float32"),
        "reason": rejected["reason"].astype(str),
    })


def ingest(client, batch, run_id=None) -> dict:
    """Write one validated batch (load.prepare) into ClickHouse, one partition swap per month."""
    a, b = batch.date_from, batch.date_to
    if len(batch.sites):
        s = batch.sites.assign(site_id=batch.sites["site_id"].astype("uint16"),
                               capacity_kw=batch.sites["capacity_kw"].astype(float))
        client.insert_df("sites", s[["site_id", "capacity_kw", "postcode"]])

    readings = _readings_frame(batch.valid)
    quarantine = _quarantine_frame(batch.rejected, run_id)
    r_month = readings["day"].dt.year * 100 + readings["day"].dt.month
    q_month = quarantine["local_date"].dt.year * 100 + quarantine["local_date"].dt.month

    for month, first, last in _months(a, b):
        partial = (a > first or b < last) and (a, b)
        rows = readings[r_month == month]
        _swap(client, "readings", "day", month, partial,
              lambda stage, rows=rows: len(rows) and client.insert_df(stage, rows))
        qrows = quarantine[q_month == month]
        _swap(client, "quarantine", "local_date", month, partial,
              lambda stage, qrows=qrows: len(qrows) and client.insert_df(
                  stage, qrows, column_names=list(qrows.columns)))
    return {"loaded": len(readings), "quarantined": len(quarantine)}


# Port of metrics.INSERT_SQL. Differences from the Postgres version, all deliberate:
# * sumIfOrNull / countIf replace sum(...) FILTER: OrNull keeps "no GG rows" as NULL, not 0.
# * quantileExactInclusive(0.5) matches percentile_cont(0.5) (interpolates between the two
#   middle values). Plain quantileExact does not interpolate and would differ on even counts.
# * The fleet median is a window over the same rows, so readings are scanned once.
# * `day` is stored on each reading, so there is no timezone arithmetic per row.
METRICS_SQL = f"""
INSERT INTO {{stage:Identifier}}
SELECT site_id, day, gen_kwh, cons_kwh, gen_intervals, yld, fleet_yield,
       yld / nullIf(fleet_yield, 0)
FROM (
    SELECT d.site_id AS site_id, d.day AS day, gen_kwh, cons_kwh, gen_intervals,
           gen_kwh / s.capacity_kw AS yld,
           quantileExactInclusive(0.5)(if(gen_intervals >= {MIN_INTERVALS_FOR_FLEET}, yld, NULL))
               OVER (PARTITION BY d.day) AS fleet_yield
    FROM (
        SELECT site_id, day,
               sumIfOrNull(kwh, channel = 'GG') AS gen_kwh,
               sumIfOrNull(kwh, channel = 'GC') AS cons_kwh,
               countIf(channel = 'GG')          AS gen_intervals
        FROM readings
        WHERE day BETWEEN {{a:Date}} AND {{b:Date}}
        GROUP BY site_id, day
    ) AS d
    INNER JOIN (SELECT site_id, capacity_kw FROM sites FINAL) AS s ON s.site_id = d.site_id
)
"""


def rebuild(client, date_from: dt.date, date_to: dt.date) -> int:
    """Recompute daily metrics for every month touching [date_from, date_to].

    Whole months are recomputed (cheap here) so each month can be swapped in as one partition.
    Days outside the requested range come out identical unless their readings changed, in
    which case recomputing them is correct anyway.
    """
    for month, first, last in _months(date_from, date_to):
        _swap(client, "daily_site_metrics", "day", month, None,
              lambda stage, first=first, last=last: client.command(
                  METRICS_SQL, parameters={"stage": stage, "a": first, "b": last}))
    return client.command("SELECT count() FROM daily_site_metrics WHERE day BETWEEN {a:Date} AND {b:Date}",
                          parameters={"a": date_from, "b": date_to})


def fetch(client, date_from=None, date_to=None) -> pd.DataFrame:
    """Same shape and dtypes as metrics.fetch, so detect/evaluate run unchanged on either store."""
    q, params = "SELECT * FROM daily_site_metrics", {}
    if date_from:
        q += " WHERE day BETWEEN {a:Date} AND {b:Date}"
        params = {"a": date_from, "b": date_to}
    df = client.query_df(q + " ORDER BY site_id, day", parameters=params, use_extended_dtypes=False)
    df["site_id"] = df["site_id"].astype(int)
    df["gen_intervals"] = df["gen_intervals"].astype(int)
    df["day"] = pd.to_datetime(df["day"])
    num = ["gen_kwh", "cons_kwh", "yield_kwh_per_kw", "fleet_yield", "perf_ratio"]
    df[num] = df[num].astype(float)
    return df
