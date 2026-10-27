import numpy as np
import pandas as pd
from fleethealth.detect import detect
from fleethealth.validate import check_rows


def _metrics(days=60, fleet=4.0, site_yield=4.0, start=None, factor=1.0, dark_day=None):
    d = pd.date_range("2013-01-01", periods=days)
    fy = np.full(days, fleet)
    if dark_day is not None:
        fy[dark_day] = 0.5
    y = np.full(days, site_yield) * (fy / fleet)
    if start is not None:
        y[start:] *= factor
    return pd.DataFrame({"site_id": 1, "day": d, "gen_kwh": y * 2, "gen_intervals": 48,
                         "yield_kwh_per_kw": y, "fleet_yield": fy, "perf_ratio": y / fy})


def test_outage_is_critical_on_the_first_day():
    a = detect(_metrics(start=40, factor=0.0))
    first = a[a.alert_type == "no_generation"]["day"].min()
    assert first == pd.Timestamp("2013-02-10")


def test_partial_drop_is_caught_within_two_days():
    a = detect(_metrics(start=40, factor=0.5))
    first = a[a.alert_type == "underperforming"]["day"].min()
    assert (first - pd.Timestamp("2013-02-10")).days <= 2


def test_healthy_site_and_dark_fleet_day_raise_nothing():
    assert detect(_metrics(dark_day=30)).empty


def test_validation_quarantines_duplicates_and_negatives():
    df = pd.DataFrame({"site_id": [1, 1, 1], "channel": "GG",
                       "ts": pd.to_datetime(["2013-01-01 10:00"] * 2 + ["2013-01-01 10:30"], utc=True),
                       "kwh": [0.5, 0.5, -1.0]})
    ok, bad = check_rows(df)
    assert len(ok) == 1 and sorted(bad["reason"]) == ["duplicate", "negative_value"]
