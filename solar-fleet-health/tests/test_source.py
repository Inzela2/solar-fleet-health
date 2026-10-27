import datetime as dt
import pandas as pd
from fleethealth.source import to_long

SLOTS = [f"{h}:{m:02d}" for h in range(24) for m in (0, 30)][1:] + ["0:00"]


def _wide(day):
    row = {"site_id": 1, "channel": "GC", "local_date": day, **{s: 0.2 for s in SLOTS}}
    return pd.DataFrame([row])


def test_normal_day_has_48_readings():
    long, rejected = to_long(_wide(dt.date(2012, 8, 1)))
    assert len(long) == 48 and rejected.empty


def test_clocks_forward_day_drops_the_hour_that_never_happened():
    long, rejected = to_long(_wide(dt.date(2012, 10, 7)))
    assert len(long) == 46
    assert set(rejected["slot"]) == {"2:30", "3:00"}
    assert set(rejected["reason"]) == {"nonexistent_local_time"}


def test_clocks_back_day_flags_merged_slots_and_keeps_timestamps_unique():
    long, _ = to_long(_wide(dt.date(2013, 4, 7)))
    assert set(long.loc[long["flag"] == "merged_dst_hour", "slot"]) == {"2:30", "3:00"}
    assert long["ts"].is_unique and long["ts"].is_monotonic_increasing


def test_midnight_slot_belongs_to_end_of_day():
    long, _ = to_long(_wide(dt.date(2012, 8, 1)))
    last = long[long["slot"] == "0:00"]["ts"].iloc[0]
    assert last.tz_convert("Australia/Sydney").date() == dt.date(2012, 8, 2)
