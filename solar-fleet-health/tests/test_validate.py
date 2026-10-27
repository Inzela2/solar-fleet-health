import datetime as dt
import pandas as pd
from fleethealth.validate import check_rows, check_sites


def _long(values, site=1, channel="GG", start="2012-08-01 00:30"):
    ts = pd.date_range(pd.Timestamp(start, tz="Australia/Sydney"), periods=len(values), freq="30min")
    local_date = (ts - pd.Timedelta(minutes=30)).date
    return pd.DataFrame({"site_id": site, "channel": channel, "ts": ts, "kwh": values,
                         "local_date": local_date, "slot": "x", "flag": None})


def test_rejects_negative_missing_and_duplicate_rows_with_reasons():
    df = _long([1.0, -0.2, None, 0.5])
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    valid, rejected = check_rows(df)
    assert list(valid["kwh"]) == [1.0, 0.5]
    assert sorted(rejected["reason"]) == ["duplicate", "missing_value", "negative_value"]


def test_flags_site_generating_at_night_for_a_week():
    days = []
    for d in range(8):
        values = [0.2] * 48                                  # output around the clock
        days.append(_long(values, start=f"2012-08-{d + 1:02d} 00:30"))
    flags = check_sites(pd.concat(days), pd.DataFrame({"site_id": [1], "capacity_kw": [2.0]}))
    assert bool(flags.loc[0, "night_generation"])


def test_flags_output_above_rated_capacity():
    values = [0.0] * 20 + [1.5] + [0.0] * 27                # 1.5 kWh in 30 min = 3 kW from a 2 kW system
    flags = check_sites(_long(values), pd.DataFrame({"site_id": [1], "capacity_kw": [2.0]}))
    assert bool(flags.loc[0, "over_capacity"])


def test_normal_site_is_not_flagged():
    values = [0.0] * 14 + [0.5] * 20 + [0.0] * 14
    flags = check_sites(_long(values), pd.DataFrame({"site_id": [1], "capacity_kw": [2.0]}))
    assert flags.empty
