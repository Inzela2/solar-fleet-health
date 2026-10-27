"""Measure the two queries that matter most, under different index choices.

1. Backfill one day: recompute metrics for a single date. Filters readings by time only, so
   the (site_id, channel, ts) primary key cannot help.
2. Dashboard: one installation's daily totals for a month, from raw readings vs the daily table.
"""
import time

ONE_DAY = """SELECT count(*), sum(kwh) FROM readings
             WHERE ts >  ('2013-01-15'::timestamp AT TIME ZONE 'Australia/Sydney')
               AND ts <= ('2013-01-16'::timestamp AT TIME ZONE 'Australia/Sydney')"""
SITE_RAW = """SELECT ((ts - interval '30 minutes') AT TIME ZONE 'Australia/Sydney')::date d, sum(kwh)
              FROM readings WHERE site_id = 42 AND channel = 'GG'
                AND ts > '2013-01-01' AND ts <= '2013-02-01' GROUP BY d"""
SITE_DAILY = """SELECT day, gen_kwh FROM daily_site_metrics
                WHERE site_id = 42 AND day BETWEEN '2013-01-01' AND '2013-01-31'"""


def _time(conn, sql, repeats=5):
    conn.execute(sql).fetchall()                     # warm cache
    t = time.perf_counter()
    for _ in range(repeats):
        conn.execute(sql).fetchall()
    return round(1000 * (time.perf_counter() - t) / repeats, 1)


def _size(conn, name):
    return conn.execute("SELECT pg_size_pretty(pg_relation_size(%s))", (name,)).fetchone()[0]


def run(conn) -> dict:
    conn.commit()
    conn.autocommit = True
    conn.execute("DROP INDEX IF EXISTS readings_ts_brin")
    conn.execute("DROP INDEX IF EXISTS readings_ts_btree")
    conn.execute("ANALYZE readings")
    out = {"rows": conn.execute("SELECT count(*) FROM readings").fetchone()[0],
           "table_size": _size(conn, "readings"),
           "one_day_ms": {"primary_key_only": _time(conn, ONE_DAY)}}
    conn.execute("CREATE INDEX readings_ts_brin ON readings USING brin (ts)")
    conn.execute("ANALYZE readings")
    out["one_day_ms"]["brin_on_ts"] = _time(conn, ONE_DAY)
    out["brin_size"] = _size(conn, "readings_ts_brin")
    conn.execute("DROP INDEX readings_ts_brin")
    conn.execute("CREATE INDEX readings_ts_btree ON readings (ts)")
    conn.execute("ANALYZE readings")
    out["one_day_ms"]["btree_on_ts"] = _time(conn, ONE_DAY)
    out["btree_size"] = _size(conn, "readings_ts_btree")
    conn.execute("DROP INDEX readings_ts_btree")
    out["dashboard_month_ms"] = {"from_raw_readings": _time(conn, SITE_RAW),
                                 "from_daily_table": _time(conn, SITE_DAILY)}
    conn.autocommit = False
    return out
