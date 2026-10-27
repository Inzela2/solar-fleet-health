"""Read the Ausgrid Solar Home file and turn its wide layout into timestamped readings.

The file has one row per customer, channel and day, with 48 half-hour columns labelled by the
END of each interval ("0:30" ... "23:30", "0:00" = midnight at the end of the day).

The meters record local clock time, not standard time: the centre of solar generation jumps by
an hour on the daylight-saving change dates. Every day still has 48 columns, so:
* the day clocks go forward has two columns for an hour that never happened (all zero), and
* the day clocks go back has two columns that each hold two real half-hours.
`to_long` turns the first case into quarantined rows and flags the second.
"""
from pathlib import Path
import io
import urllib.request
import zipfile
import numpy as np
import pandas as pd
from .config import TZ

COMMIT = "05d20c157c6d76925e8bce2c132f0d38d37a3d21"
SOURCE_URL = f"https://raw.githubusercontent.com/rockey-abhishek/Ausgrid-Dataset/{COMMIT}/Data.zip"
MEMBER = "RawData/2012-2013 Solar home electricity data v2.csv"
DEFAULT_PATH = Path("data/solar_home_2012_13.csv")
CHANNELS = ("GG", "GC")
MERGED_ON_FALL_BACK = ("2:30", "3:00")   # slots that hold the repeated hour (checked in the data)


def download(path: Path = DEFAULT_PATH) -> Path:
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(SOURCE_URL) as r:
        z = zipfile.ZipFile(io.BytesIO(r.read()))
    path.write_bytes(z.read(MEMBER))
    return path


def read_wide(path: Path = DEFAULT_PATH) -> pd.DataFrame:
    df = pd.read_csv(path, skiprows=1)
    df = df.rename(columns={"Customer": "site_id", "Generator Capacity": "capacity_kw",
                            "Postcode": "postcode", "Consumption Category": "channel"})
    df["local_date"] = pd.to_datetime(df["date"], dayfirst=True).dt.date
    return df.drop(columns=["date", "Row Quality"], errors="ignore")


def slot_columns(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if ":" in c]


def _naive_start(local_date: pd.Series, slot: pd.Series) -> pd.Series:
    """Clock time each half-hour STARTS. Labels are interval ends: "0:30" starts at 0:00,
    "0:00" is the last interval of the day and starts at 23:30."""
    hm = slot.str.split(":", expand=True).astype(int)
    end_minutes = hm[0] * 60 + hm[1]
    end_minutes = end_minutes.where(end_minutes > 0, 24 * 60)
    return pd.to_datetime(local_date) + pd.to_timedelta(end_minutes - 30, unit="m")


def to_long(wide: pd.DataFrame, tz: str = TZ) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (readings, rejected). Readings have a tz-aware `ts` marking the END of each interval.

    Whether an interval existed depends on when it STARTED: on the clocks-forward day the
    intervals starting at 2:00 and 2:30 never happened (slots "2:30" and "3:00"), while the one
    ending at 2:00 did. So the start is localised, then 30 minutes added.
    """
    wide = wide[wide["channel"].isin(CHANNELS)]
    cols = ["site_id", "channel", "local_date", "slot", "kwh", "ts", "flag"]
    if wide.empty:
        return pd.DataFrame(columns=cols), pd.DataFrame(columns=cols + ["reason"])
    slots = slot_columns(wide)
    long = wide.melt(id_vars=["site_id", "channel", "local_date"], value_vars=slots,
                     var_name="slot", value_name="kwh")
    start = _naive_start(long["local_date"], long["slot"])
    # ambiguous starts (clocks going back) resolve to the first, daylight-time occurrence
    local_start = start.dt.tz_localize(tz, ambiguous=np.ones(len(start), dtype=bool),
                                       nonexistent="NaT")
    long["ts"] = local_start + pd.Timedelta(minutes=30)
    long["flag"] = None

    nonexistent = long["ts"].isna()
    rejected = long[nonexistent].assign(reason="nonexistent_local_time")
    long = long[~nonexistent]

    fall_back = _fall_back_dates(long["local_date"].unique(), tz)
    merged = long["local_date"].isin(fall_back) & long["slot"].isin(MERGED_ON_FALL_BACK)
    long.loc[merged, "flag"] = "merged_dst_hour"
    return long.reset_index(drop=True), rejected.reset_index(drop=True)


def _fall_back_dates(dates, tz: str) -> set:
    out = set()
    for d in dates:
        start = pd.Timestamp(d).tz_localize(tz)
        end = (pd.Timestamp(d) + pd.Timedelta(days=1)).tz_localize(tz)
        if (end - start) > pd.Timedelta(hours=24):
            out.add(d)
    return out


def sites(wide: pd.DataFrame) -> pd.DataFrame:
    return wide.groupby("site_id", as_index=False)[["capacity_kw", "postcode"]].first()
