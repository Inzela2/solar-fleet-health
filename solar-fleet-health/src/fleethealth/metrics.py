"""Daily per-site metrics, the table the dashboard and fault detection both read.

Rebuilding a date range is delete-then-insert inside one transaction, so a backfill of any
range gives the same result as the original run (tested).
"""
import datetime as dt
import pandas as pd
from .config import TZ

MIN_INTERVALS_FOR_FLEET = 46   # a site-day only counts towards the fleet median if (nearly) complete

DELETE_SQL = "DELETE FROM daily_site_metrics WHERE day BETWEEN %(a)s AND %(b)s"

INSERT_SQL = f"""
INSERT INTO daily_site_metrics
WITH r AS (
    SELECT site_id, channel, kwh,
           ((ts - interval '30 minutes') AT TIME ZONE '{TZ}')::date AS day
    FROM readings
    WHERE ts >  (%(a)s::timestamp AT TIME ZONE '{TZ}')
      AND ts <= ((%(b)s::date + 1)::timestamp AT TIME ZONE '{TZ}')
), d AS (
    SELECT site_id, day,
           sum(kwh) FILTER (WHERE channel = 'GG')  AS gen_kwh,
           sum(kwh) FILTER (WHERE channel = 'GC')  AS cons_kwh,
           count(*) FILTER (WHERE channel = 'GG')  AS gen_intervals
    FROM r GROUP BY site_id, day
), y AS (
    SELECT d.*, d.gen_kwh / s.capacity_kw AS yld FROM d JOIN sites s USING (site_id)
), f AS (
    SELECT day, percentile_cont(0.5) WITHIN GROUP (ORDER BY yld) AS fleet_yield
    FROM y WHERE gen_intervals >= {MIN_INTERVALS_FOR_FLEET} GROUP BY day
)
SELECT site_id, day, gen_kwh, cons_kwh, gen_intervals, yld, fleet_yield,
       yld / NULLIF(fleet_yield, 0)
FROM y LEFT JOIN f USING (day);
"""


def rebuild(conn, date_from: dt.date, date_to: dt.date) -> int:
    params = {"a": date_from, "b": date_to}
    with conn.transaction():            # delete + insert succeed or fail together
        conn.execute(DELETE_SQL, params)
        conn.execute(INSERT_SQL, params)
    return conn.execute("SELECT count(*) FROM daily_site_metrics WHERE day BETWEEN %s AND %s",
                        (date_from, date_to)).fetchone()[0]


def fetch(conn, date_from=None, date_to=None) -> pd.DataFrame:
    q = "SELECT * FROM daily_site_metrics"
    params = ()
    if date_from:
        q += " WHERE day BETWEEN %s AND %s"
        params = (date_from, date_to)
    cur = conn.execute(q + " ORDER BY site_id, day", params)
    cols = [c.name for c in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    df["day"] = pd.to_datetime(df["day"])
    num = ["gen_kwh", "cons_kwh", "yield_kwh_per_kw", "fleet_yield", "perf_ratio"]
    df[num] = df[num].astype(float)
    return df
