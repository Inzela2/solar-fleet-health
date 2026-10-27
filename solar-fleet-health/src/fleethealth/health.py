"""Pipeline health check. Exits non-zero when the PIPELINE (not a site) needs attention, so it
can run on a schedule and page someone: last ingest failed, data stale, or many sites silent."""
import datetime as dt

MAX_SILENT_SHARE = 0.05


def check(conn, as_of: dt.date) -> dict:
    last = conn.execute("""SELECT run_id, status, finished_at, rows_loaded, rows_quarantined
                           FROM ingest_runs ORDER BY run_id DESC LIMIT 1""").fetchone()
    latest_day = conn.execute("SELECT max(day) FROM daily_site_metrics").fetchone()[0]
    n_sites = conn.execute("SELECT count(*) FROM sites").fetchone()[0]
    reporting = conn.execute("SELECT count(*) FROM daily_site_metrics WHERE day = %s",
                             (latest_day,)).fetchone()[0] if latest_day else 0
    alerts = dict(conn.execute("""SELECT alert_type, count(*) FROM alerts WHERE day = %s
                                  GROUP BY alert_type""", (latest_day,)).fetchall()) if latest_day else {}
    quarantine = dict(conn.execute("SELECT reason, count(*) FROM quarantine GROUP BY reason").fetchall())

    problems = []
    if last is None or last[1] != "ok":
        problems.append("last ingest run did not succeed")
    stuck = conn.execute("""SELECT count(*) FROM ingest_runs WHERE status = 'running'
                            AND started_at < now() - interval '1 hour'""").fetchone()[0]
    if stuck:
        problems.append(f"{stuck} ingest run(s) started over an hour ago and never finished")
    if latest_day is None or (as_of - latest_day).days > 2:
        problems.append(f"data is stale (latest day {latest_day})")
    silent = 1 - reporting / n_sites if n_sites else 1
    if silent > MAX_SILENT_SHARE:
        problems.append(f"{silent:.0%} of sites silent on {latest_day}")
    return {"ok": not problems, "problems": problems, "latest_day": str(latest_day),
            "sites": n_sites, "reporting_latest_day": reporting, "alerts_latest_day": alerts,
            "quarantine_by_reason": quarantine,
            "last_run": None if last is None else {"run_id": last[0], "status": last[1],
                                                   "rows_loaded": last[3], "rows_quarantined": last[4]}}
