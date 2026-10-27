"""Idempotent ingest: load a date range of readings, safe to re-run for the same range.

Rows go into a temporary staging table first, then into `readings` with
INSERT ... ON CONFLICT DO UPDATE. Re-running a day (late data, a corrected file, a backfill)
updates values in place instead of duplicating them, and the run is recorded in ingest_runs.
"""
import datetime as dt
import pandas as pd
from . import db, source, validate


def ingest(conn, wide: pd.DataFrame, date_from: dt.date, date_to: dt.date) -> dict:
    part = wide[(wide["local_date"] >= date_from) & (wide["local_date"] <= date_to)]
    run_id = conn.execute(
        "INSERT INTO ingest_runs (date_from, date_to) VALUES (%s, %s) RETURNING run_id",
        (date_from, date_to)).fetchone()[0]
    conn.commit()
    try:
        long, rejected_time = source.to_long(part)
        valid, rejected_rows = validate.check_rows(long)
        rejected = pd.concat([rejected_time, rejected_rows], ignore_index=True)

        sites = source.sites(part)
        with conn.transaction():
            conn.execute("""CREATE TEMP TABLE stage_sites (LIKE sites) ON COMMIT DROP""")
            db.copy_frame(conn, "stage_sites", sites, ["site_id", "capacity_kw", "postcode"])
            conn.execute("""INSERT INTO sites SELECT * FROM stage_sites
                            ON CONFLICT (site_id) DO UPDATE
                            SET capacity_kw = EXCLUDED.capacity_kw, postcode = EXCLUDED.postcode""")

            conn.execute("""CREATE TEMP TABLE stage_readings (LIKE readings) ON COMMIT DROP""")
            out = valid.assign(ts=valid["ts"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S+00"))
            db.copy_frame(conn, "stage_readings", out, ["site_id", "channel", "ts", "kwh", "flag"])
            conn.execute("""INSERT INTO readings SELECT * FROM stage_readings
                            ON CONFLICT (site_id, channel, ts) DO UPDATE
                            SET kwh = EXCLUDED.kwh, flag = EXCLUDED.flag""")

            # re-running a range replaces its quarantine rows instead of piling up duplicates
            conn.execute("DELETE FROM quarantine WHERE local_date BETWEEN %s AND %s", (date_from, date_to))
            if len(rejected):
                q = rejected.assign(run_id=run_id, value=rejected["kwh"])
                db.copy_frame(conn, "quarantine", q,
                              ["run_id", "site_id", "channel", "local_date", "slot", "value", "reason"])
            conn.execute("""UPDATE ingest_runs SET finished_at = now(), status = 'ok',
                            rows_in = %s, rows_loaded = %s, rows_quarantined = %s WHERE run_id = %s""",
                         (len(long) + len(rejected_time), len(valid), len(rejected), run_id))
        return {"run_id": run_id, "loaded": len(valid), "quarantined": len(rejected)}
    except Exception:
        conn.rollback()
        conn.execute("UPDATE ingest_runs SET finished_at = now(), status = 'failed' WHERE run_id = %s", (run_id,))
        conn.commit()
        raise
