"""python -m fleethealth <command>

  init                      create tables
  ingest  --from --to       load readings for a date range, one month per batch
  backfill --from --to      re-ingest, rebuild metrics and re-run detection for a range
                            (ingest/backfill take --backend pg|ch|both, default pg)
  evaluate                  measure detection on injected faults
  health  --as-of DATE      pipeline health check (exit code 1 if unhealthy)
  bench                     time key queries under different indexes
  compare                   time the same queries on Postgres and ClickHouse, write results/
  report                    run everything end to end and write results/
"""
import argparse
import datetime as dt
import json
import sys
from pathlib import Path
import pandas as pd
from . import db, ch, source, validate, load, metrics, detect, evaluate, health, bench

RESULTS = Path("results")
FIRST, LAST = dt.date(2012, 7, 1), dt.date(2013, 6, 30)


def _months(a: dt.date, b: dt.date):
    """Split [a, b] into calendar-month batches, the way monthly files would arrive."""
    start = a
    while start <= b:
        end = min((pd.Timestamp(start) + pd.offsets.MonthEnd(0)).date(), b)
        yield start, end
        start = end + dt.timedelta(days=1)


def ingest_range(conn, wide, a, b, client=None):
    """Load [a, b] one month at a time. conn=None skips Postgres; client=None skips ClickHouse.
    Each month is validated once and the same rows go to every store."""
    results = []
    for s, e in _months(a, b):
        batch = load.prepare(wide, s, e)
        r = load.ingest(conn, wide, s, e, batch=batch) if conn is not None else {}
        if client is not None:
            r = {**ch.ingest(client, batch, run_id=r.get("run_id")), **r}
        results.append(r)
    return {"runs": len(results), "loaded": sum(r["loaded"] for r in results),
            "quarantined": sum(r["quarantined"] for r in results)}


def backfill(conn, wide, a, b, client=None):
    stats = ingest_range(conn, wide, a, b, client)
    if client is not None:
        ch.rebuild(client, a, b)
    if conn is None:                    # alerts live in Postgres; ClickHouse-only stops here
        return stats
    metrics.rebuild(conn, a, b)
    # detection needs history before `a` for its baseline, so read a wider window
    m = metrics.fetch(conn, a - dt.timedelta(days=40), b)
    stats["alerts"] = detect.write(conn, detect.detect(m), a, b)
    return stats


def report():
    RESULTS.mkdir(exist_ok=True)
    wide = source.read_wide(source.download())
    with db.connect() as conn:
        db.init_schema(conn)
        stats = backfill(conn, wide, FIRST, LAST)
        long, _ = source.to_long(wide)
        site_flags = validate.check_sites(long, source.sites(wide))
        m = metrics.fetch(conn)
        cur = conn.execute("""SELECT alert_type, count(*), count(DISTINCT site_id) FROM alerts
                              GROUP BY alert_type ORDER BY alert_type""")
        alert_summary = {r[0]: {"site_days": r[1], "sites": r[2]} for r in cur.fetchall()}
        ev = evaluate.run(m, evaluate.healthy_sites(m, set(site_flags["site_id"])))
        timing = bench.run(conn)
        h = health.check(conn, LAST)
    ev.to_csv(RESULTS / "detection_evaluation.csv", index=False)
    site_flags.to_csv(RESULTS / "site_metadata_flags.csv", index=False)
    out = {"ingest": stats, "alerts": alert_summary,
           "site_metadata_flags": {"night_generation": int(site_flags["night_generation"].sum()),
                                   "over_capacity": int(site_flags["over_capacity"].sum())},
           "healthy_sites_used_for_evaluation": int(len(evaluate.healthy_sites(m, set(site_flags["site_id"])))),
           "benchmark": timing, "health": h}
    (RESULTS / "summary.json").write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out, indent=2, default=str))
    print(ev.to_string(index=False))


def main():
    p = argparse.ArgumentParser(prog="fleethealth")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init")
    for name in ("ingest", "backfill"):
        s = sub.add_parser(name)
        s.add_argument("--from", dest="a", type=dt.date.fromisoformat, default=FIRST)
        s.add_argument("--to", dest="b", type=dt.date.fromisoformat, default=LAST)
        s.add_argument("--backend", choices=["pg", "ch", "both"], default="pg")
    h = sub.add_parser("health")
    h.add_argument("--as-of", type=dt.date.fromisoformat, default=dt.date.today())
    sub.add_parser("bench")
    sub.add_parser("compare")
    sub.add_parser("evaluate")
    sub.add_parser("report")
    a = p.parse_args()

    if a.cmd == "report":
        return report()
    if a.cmd in ("ingest", "backfill") and a.backend == "ch":
        client = ch.connect()
        ch.init_schema(client)
        fn = ingest_range if a.cmd == "ingest" else backfill
        print(json.dumps(fn(None, source.read_wide(source.download()), a.a, a.b, client), default=str))
        return
    with db.connect() as conn:
        db.init_schema(conn)
        if a.cmd in ("ingest", "backfill"):
            client = None
            if a.backend == "both":
                client = ch.connect()
                ch.init_schema(client)
            wide = source.read_wide(source.download())
            fn = ingest_range if a.cmd == "ingest" else backfill
            print(json.dumps(fn(conn, wide, a.a, a.b, client), default=str))
        elif a.cmd == "compare":
            from . import compare
            RESULTS.mkdir(exist_ok=True)
            client = ch.connect()
            out = compare.run(conn, client)
            (RESULTS / "pg_vs_clickhouse.json").write_text(json.dumps(out, indent=2))
            print(json.dumps(out, indent=2))
        elif a.cmd == "health":
            r = health.check(conn, a.as_of)
            print(json.dumps(r, indent=2, default=str))
            sys.exit(0 if r["ok"] else 1)
        elif a.cmd == "bench":
            print(json.dumps(bench.run(conn), indent=2))
        elif a.cmd == "evaluate":
            m = metrics.fetch(conn)
            print(evaluate.run(m, evaluate.healthy_sites(m)).to_string(index=False))


if __name__ == "__main__":
    main()
