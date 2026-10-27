"""Fault detection on daily metrics.

The fleet is in one region, so on any day the median yield (kWh per kW of panels) across all
sites captures the weather. Each site is then judged against that, relative to its OWN normal
level, because roof angle and shading make healthy sites differ by a lot.

Rules (thresholds chosen by the injected-fault evaluation in evaluate.py):
* no_generation (critical): a complete day with zero output while the fleet had a normal day.
* underperforming (warning): the site's 3-day median performance has dropped below
  RATIO_THRESHOLD of its own 28-day baseline. The baseline skips the most recent 3 days so a
  new fault cannot drag its own baseline down.
* missing_data (warning): the fleet reported but this site did not.
"""
import pandas as pd

MIN_FLEET_YIELD = 1.0     # kWh/kW: below this the whole fleet had a very dark day; don't judge
RATIO_THRESHOLD = 0.7
SHORT_WINDOW = 3
BASELINE_WINDOW = 28
BASELINE_MIN_DAYS = 14


def detect(metrics: pd.DataFrame, ratio_threshold: float = RATIO_THRESHOLD) -> pd.DataFrame:
    days = pd.Index(sorted(metrics["day"].unique()), name="day")
    out = []
    for site, m in metrics.groupby("site_id"):
        m = m.set_index("day").reindex(days)
        reported = m["gen_intervals"].notna()
        fleet_ok = m["fleet_yield"].fillna(0) >= MIN_FLEET_YIELD

        dead = reported & (m["gen_kwh"] == 0) & (m["gen_intervals"] >= 46) & fleet_ok
        for d in m.index[dead]:
            out.append((site, d, "no_generation", "critical", "zero output on a normal fleet day"))

        for d in m.index[~reported & m.index.isin(metrics["day"])]:
            out.append((site, d, "missing_data", "warning", "no readings received"))

        pr = m["perf_ratio"].where(fleet_ok & reported & ~dead)
        recent = pr.rolling(SHORT_WINDOW, min_periods=2).median()
        baseline = pr.shift(SHORT_WINDOW).rolling(BASELINE_WINDOW, min_periods=BASELINE_MIN_DAYS).median()
        low = (recent < ratio_threshold * baseline) & fleet_ok & reported & ~dead
        for d in m.index[low]:
            out.append((site, d, "underperforming", "warning",
                        f"3-day performance {recent[d]:.2f} vs baseline {baseline[d]:.2f}"))
    return pd.DataFrame(out, columns=["site_id", "day", "alert_type", "severity", "detail"])


def write(conn, alerts: pd.DataFrame, date_from, date_to) -> int:
    conn.commit()                         # close any open read transaction so this commits
    with conn.transaction():
        conn.execute("DELETE FROM alerts WHERE day BETWEEN %s AND %s", (date_from, date_to))
        rows = alerts[(alerts["day"] >= pd.Timestamp(date_from)) & (alerts["day"] <= pd.Timestamp(date_to))]
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO alerts (site_id, day, alert_type, severity, detail) VALUES (%s,%s,%s,%s,%s)",
                [(int(r.site_id), r.day.date(), r.alert_type, r.severity, r.detail) for r in rows.itertuples()])
    return len(rows)
