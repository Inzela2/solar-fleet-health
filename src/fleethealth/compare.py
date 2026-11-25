"""Time the same questions on Postgres and ClickHouse, after checking they give the same answers.

Each database uses its natural design: Postgres has its primary key plus its best index for
time filters (BRIN and B-tree on ts are both tried, the faster one is reported); ClickHouse has
its sort key and monthly partitions. Times are wall-clock from Python, including fetching the
result, median of several warm runs. A timing only counts if both databases returned the same
result (row count and numeric checksum within float32 rounding).
"""
import datetime as dt
import statistics
import time
from . import ch, metrics
from .config import TZ

FIRST, LAST = dt.date(2012, 7, 1), dt.date(2013, 6, 30)
SITE, DAY, MONTH = 42, "2013-01-15", ("2013-01-01", "2013-01-31")
LOCAL_DAY = f"((ts - interval '30 minutes') AT TIME ZONE '{TZ}')::date"

# name -> (description, postgres sql, clickhouse sql)
QUERIES = {
    "one_day_raw": (
        "Backfill read: all readings for one day",
        f"""SELECT count(*), sum(kwh::float8) FROM readings
            WHERE ts >  ('{DAY}'::timestamp AT TIME ZONE '{TZ}')
              AND ts <= (('{DAY}'::date + 1)::timestamp AT TIME ZONE '{TZ}')""",
        f"SELECT count(), sum(kwh) FROM readings WHERE day = '{DAY}'"),
    "site_month_raw": (
        "Dashboard: one site's daily generation for a month, from raw readings",
        f"""SELECT {LOCAL_DAY} AS d, sum(kwh::float8) FROM readings
            WHERE site_id = {SITE} AND channel = 'GG'
              AND ts >  ('{MONTH[0]}'::timestamp AT TIME ZONE '{TZ}')
              AND ts <= (('{MONTH[1]}'::date + 1)::timestamp AT TIME ZONE '{TZ}')
            GROUP BY d ORDER BY d""",
        f"""SELECT day, sum(kwh) FROM readings
            WHERE site_id = {SITE} AND channel = 'GG' AND day BETWEEN '{MONTH[0]}' AND '{MONTH[1]}'
            GROUP BY day ORDER BY day"""),
    "site_month_daily": (
        "Dashboard: same, from the precomputed daily table",
        f"""SELECT day, gen_kwh FROM daily_site_metrics
            WHERE site_id = {SITE} AND day BETWEEN '{MONTH[0]}' AND '{MONTH[1]}' ORDER BY day""",
        f"""SELECT day, gen_kwh FROM daily_site_metrics
            WHERE site_id = {SITE} AND day BETWEEN '{MONTH[0]}' AND '{MONTH[1]}' ORDER BY day"""),
    "postcode_median_year_raw": (
        "Cohort: median daily yield per postcode, every day of the year, from raw readings",
        f"""WITH d AS (SELECT site_id, {LOCAL_DAY} AS day, sum(kwh::float8) AS gen
                       FROM readings WHERE channel = 'GG' GROUP BY 1, 2)
            SELECT s.postcode, d.day,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY d.gen / s.capacity_kw), count(*)
            FROM d JOIN sites s USING (site_id) GROUP BY 1, 2 ORDER BY 1, 2""",
        """SELECT s.postcode, d.day, quantileExactInclusive(0.5)(d.gen / s.capacity_kw), count()
           FROM (SELECT site_id, day, sum(kwh) AS gen FROM readings
                 WHERE channel = 'GG' GROUP BY site_id, day) AS d
           INNER JOIN (SELECT site_id, postcode, capacity_kw FROM sites FINAL) AS s
                 ON s.site_id = d.site_id
           GROUP BY s.postcode, d.day ORDER BY 1, 2"""),
    "cohort_compare_year_raw": (
        "Cohort comparison: monthly generation and use, small vs large systems, from raw readings",
        f"""SELECT CASE WHEN s.capacity_kw < 1.5 THEN 'small' ELSE 'large' END AS band,
                   date_trunc('month', {LOCAL_DAY})::date AS month,
                   sum(kwh::float8) FILTER (WHERE channel = 'GG'), sum(kwh::float8) FILTER (WHERE channel = 'GC'),
                   count(DISTINCT r.site_id)
            FROM readings r JOIN sites s USING (site_id) GROUP BY 1, 2 ORDER BY 1, 2""",
        """SELECT if(s.capacity_kw < 1.5, 'small', 'large') AS band, toStartOfMonth(r.day) AS month,
                  sumIf(kwh, channel = 'GG'), sumIf(kwh, channel = 'GC'), uniqExact(r.site_id)
           FROM readings AS r
           INNER JOIN (SELECT site_id, capacity_kw FROM sites FINAL) AS s ON s.site_id = r.site_id
           GROUP BY band, month ORDER BY 1, 2"""),
    "postcode_month_daily": (
        "API-style: average performance ratio per postcode per month, from the daily table",
        """SELECT s.postcode, date_trunc('month', m.day)::date AS month, avg(m.perf_ratio), count(*)
           FROM daily_site_metrics m JOIN sites s USING (site_id) GROUP BY 1, 2 ORDER BY 1, 2""",
        """SELECT s.postcode, toStartOfMonth(m.day) AS month, avg(m.perf_ratio), count()
           FROM daily_site_metrics AS m
           INNER JOIN (SELECT site_id, postcode FROM sites FINAL) AS s ON s.site_id = m.site_id
           GROUP BY s.postcode, month ORDER BY 1, 2"""),
}


def _median_ms(fn, repeats: int) -> float:
    fn()                                                # warm-up, not timed
    runs = []
    for _ in range(repeats):
        t = time.perf_counter()
        fn()
        runs.append(1000 * (time.perf_counter() - t))
    return round(statistics.median(runs), 1)


def _checksum(rows) -> tuple[int, float]:
    total = 0.0
    for r in rows:
        for v in r:
            if isinstance(v, (int, float)) and not isinstance(v, bool) and v == v:
                total += float(v)
            elif hasattr(v, "__float__") and not isinstance(v, (dt.date, str)):
                total += float(v)
    return len(rows), total


def _agree(pg_rows, ch_rows, rel_tol=1e-5) -> bool:
    (n1, s1), (n2, s2) = _checksum(pg_rows), _checksum(ch_rows)
    return n1 == n2 and abs(s1 - s2) <= rel_tol * max(abs(s1), 1.0)


def _pg_indexes(conn, which: str | None):
    conn.execute("DROP INDEX IF EXISTS readings_ts_brin")
    conn.execute("DROP INDEX IF EXISTS readings_ts_btree")
    if which == "brin":
        conn.execute("CREATE INDEX readings_ts_brin ON readings USING brin (ts)")
    elif which == "btree":
        conn.execute("CREATE INDEX readings_ts_btree ON readings (ts)")
    conn.execute("ANALYZE readings")


def _storage(conn, client) -> dict:
    pg = conn.execute("""SELECT pg_relation_size('readings'), pg_indexes_size('readings'),
                                pg_total_relation_size('readings')""").fetchone()
    chs = client.query("""SELECT sum(data_compressed_bytes), sum(data_uncompressed_bytes),
                                 sum(bytes_on_disk), sum(rows)
                          FROM system.parts WHERE active AND database = currentDatabase()
                            AND table = 'readings'""").result_rows[0]
    mb = lambda b: round(b / 2**20, 1)
    return {"rows": int(chs[3]),
            "postgres_mb": {"table": mb(pg[0]), "primary_key_index": mb(pg[1]), "total": mb(pg[2])},
            "clickhouse_mb": {"on_disk": mb(chs[2]), "uncompressed": mb(chs[1])},
            "postgres_to_clickhouse_ratio": round(pg[2] / chs[2], 1)}


def codec_study(client) -> dict:
    """Pick the kwh codec by measurement: copy kwh into one column per codec and compare sizes."""
    codecs = {"lz4_default": "LZ4", "zstd1": "ZSTD(1)", "gorilla_zstd": "Gorilla, ZSTD(1)",
              "fpc_zstd": "FPC, ZSTD(1)", "t64_on_wh_int": None}
    cols = ", ".join(f"{k} Float32 CODEC({v})" for k, v in codecs.items() if v)
    client.command("DROP TABLE IF EXISTS codec_study")
    client.command(f"""CREATE TABLE codec_study (site_id UInt16, channel LowCardinality(String),
                       ts DateTime('UTC'), {cols}, t64_on_wh_int UInt32 CODEC(T64, ZSTD(1)))
                       ENGINE = MergeTree ORDER BY (site_id, channel, ts)""")
    names = [k for k, v in codecs.items() if v]
    client.command(f"""INSERT INTO codec_study SELECT site_id, channel, ts,
                       {', '.join('kwh' for _ in names)}, toUInt32(round(kwh * 1000))
                       FROM readings""")
    client.command("OPTIMIZE TABLE codec_study FINAL")
    rows = client.query("""SELECT name, data_compressed_bytes, data_uncompressed_bytes
                           FROM system.columns WHERE database = currentDatabase()
                             AND table = 'codec_study' AND name NOT IN ('site_id','channel','ts')""").result_rows
    client.command("DROP TABLE codec_study")
    return {n: {"mb": round(c / 2**20, 1), "ratio": round(u / c, 1)} for n, c, u in rows}


def month_reload(conn, client, month=(dt.date(2013, 1, 1), dt.date(2013, 1, 31)), repeats=3) -> dict:
    """Re-load one month of already-validated readings into each store (the monthly batch).
    Validation is shared, so it is done once and not timed."""
    from . import load, source
    wide = source.read_wide(source.download())
    batch = load.prepare(wide, *month)
    out = {"rows": len(batch.valid),
           "postgres_ms": _median_ms(lambda: load.ingest(conn, wide, *month, batch=batch), repeats),
           "clickhouse_ms": _median_ms(lambda: ch.ingest(client, batch), repeats)}
    conn.commit()
    conn.autocommit = True
    conn.execute("VACUUM ANALYZE readings")        # upserts leave dead rows; don't bloat later runs
    conn.autocommit = False
    return out


def run(conn, client, repeats: int = 5, heavy_repeats: int = 3) -> dict:
    conn.commit()
    conn.autocommit = True
    out = {"storage": _storage(conn, client), "queries": {}}

    ch_rows = {k: client.query(q[2]).result_rows for k, q in QUERIES.items()}
    ch_ms = {k: _median_ms(lambda q=q: client.query(q[2]).result_rows,
                           heavy_repeats if "year" in k else repeats) for k, q in QUERIES.items()}

    pg_ms = {}
    for idx in ("brin", "btree"):
        _pg_indexes(conn, idx)
        for k, (_, pg_sql, _) in QUERIES.items():
            rows = conn.execute(pg_sql).fetchall()
            if not _agree(rows, ch_rows[k]):
                raise AssertionError(f"{k}: Postgres and ClickHouse results differ")
            ms = _median_ms(lambda s=pg_sql: conn.execute(s).fetchall(),
                            heavy_repeats if "year" in k else repeats)
            if k not in pg_ms or ms < pg_ms[k][0]:
                pg_ms[k] = (ms, idx)
    _pg_indexes(conn, None)                             # leave the schema as the pipeline expects

    for k, (desc, _, _) in QUERIES.items():
        p, c = pg_ms[k][0], ch_ms[k]
        idx = pg_ms[k][1] if "FROM readings" in QUERIES[k][1] else "n/a"
        out["queries"][k] = {"what": desc, "postgres_ms": p, "postgres_best_index": idx,
                             "clickhouse_ms": c, "results_match": True,
                             "speedup": round(p / c, 1) if c else None}

    # Pipeline operations, not just reads: rebuild a year of metrics.
    conn.autocommit = False
    out["operations"] = {"rebuild_year_of_daily_metrics": {
        "postgres_ms": _median_ms(lambda: metrics.rebuild(conn, FIRST, LAST), heavy_repeats),
        "clickhouse_ms": _median_ms(lambda: ch.rebuild(client, FIRST, LAST), heavy_repeats)}}
    out["operations"]["reload_one_month_of_readings"] = month_reload(conn, client)
    for op in out["operations"].values():
        op["speedup"] = round(op["postgres_ms"] / op["clickhouse_ms"], 1)
    out["kwh_codec_study"] = codec_study(client)
    return out
