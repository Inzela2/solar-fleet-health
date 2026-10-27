"""How well, and how fast, does detection catch faults?

Real faults in the data have no ground-truth labels, so we inject known faults into healthy
sites and measure: share caught within 7 days, days until the first alert, and false alarms
on the same sites without faults. Each site is faulted on its own, leaving the fleet median
unchanged, which is what happens in reality when one installation fails.
"""
import numpy as np
import pandas as pd
from .detect import detect

FAULTS = {"outage": 0.0, "half_output": 0.5, "one_third_down": 0.67}


def healthy_sites(metrics: pd.DataFrame, flagged_sites=()) -> list:
    clean = []
    n_days = metrics["day"].nunique()
    for site, m in metrics.groupby("site_id"):
        if site in flagged_sites or len(m) < n_days or (m["gen_kwh"] == 0).any():
            continue
        clean.append(site)
    return clean


def inject(m: pd.DataFrame, start: pd.Timestamp, factor: float) -> pd.DataFrame:
    m = m.copy()
    hit = m["day"] >= start
    m.loc[hit, "gen_kwh"] *= factor
    m["yield_kwh_per_kw"] = np.where(hit, m["yield_kwh_per_kw"] * factor, m["yield_kwh_per_kw"])
    m["perf_ratio"] = m["yield_kwh_per_kw"] / m["fleet_yield"]
    return m


def run(metrics: pd.DataFrame, sites: list, thresholds=(0.6, 0.7, 0.8), seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    days = sorted(metrics["day"].unique())
    starts = {s: days[rng.integers(60, len(days) - 30)] for s in sites}
    clean = metrics[metrics["site_id"].isin(sites)]
    rows = []
    for th in thresholds:
        fa = detect(clean, ratio_threshold=th)
        false_alarms = len(fa[fa["alert_type"] != "missing_data"])
        site_days = len(clean)
        for name, factor in FAULTS.items():
            faulty = pd.concat([inject(clean[clean.site_id == s], pd.Timestamp(starts[s]), factor)
                                for s in sites])
            a = detect(faulty, ratio_threshold=th)
            delays = []
            for s in sites:
                st = pd.Timestamp(starts[s])
                hits = a[(a.site_id == s) & (a.day >= st) & (a.alert_type != "missing_data")]
                delays.append((hits["day"].min() - st).days if len(hits) else np.inf)
            delays = np.array(delays, dtype=float)
            rows.append({"threshold": th, "fault": name,
                         "caught_within_7_days": float(np.mean(delays <= 7)),
                         "median_days_to_alert": float(np.median(delays[np.isfinite(delays)])) if np.isfinite(delays).any() else None,
                         "false_alarms_per_1000_site_days": round(1000 * false_alarms / site_days, 2)})
    return pd.DataFrame(rows)
