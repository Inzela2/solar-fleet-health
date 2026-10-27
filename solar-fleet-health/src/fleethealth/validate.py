"""Validation rules, kept as plain functions so each one can be tested on its own.

Row rules decide what is loaded. Site rules flag metadata that looks wrong but should not block
loading: a dashboard can still show the data, with a warning attached.
"""
import pandas as pd

NIGHT_HOURS = {22, 23, 0, 1, 2, 3}       # well outside daylight in Sydney all year
NIGHT_KWH_TOLERANCE = 0.05               # kWh per site-day, allows for meter noise
CAPACITY_HEADROOM = 1.1                  # half-hour output may not exceed 110% of rated kW × 0.5 h


def check_rows(long: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split readings into (valid, rejected-with-reason)."""
    reasons = pd.Series(None, index=long.index, dtype=object)
    reasons[long["kwh"].isna()] = "missing_value"
    reasons[reasons.isna() & (long["kwh"] < 0)] = "negative_value"
    reasons[reasons.isna() & long.duplicated(["site_id", "channel", "ts"], keep="first")] = "duplicate"
    bad = reasons.notna()
    return long[~bad], long[bad].assign(reason=reasons[bad])


def check_sites(long: pd.DataFrame, sites: pd.DataFrame) -> pd.DataFrame:
    """One row per site with metadata problems: night generation or output above rated capacity."""
    gen = long[long["channel"] == "GG"]
    local_hour = (gen["ts"] - pd.Timedelta(minutes=30)).dt.tz_convert("Australia/Sydney").dt.hour
    night = gen[local_hour.isin(NIGHT_HOURS)].groupby(["site_id", "local_date"])["kwh"].sum()
    night_days = (night > NIGHT_KWH_TOLERANCE).groupby("site_id").sum()

    peak = gen.groupby("site_id")["kwh"].max()
    cap = sites.set_index("site_id")["capacity_kw"]
    over = peak / (cap * 0.5)

    out = pd.DataFrame({"night_generation_days": night_days, "peak_to_rated": over}).fillna(0)
    out["night_generation"] = out["night_generation_days"] >= 7
    out["over_capacity"] = out["peak_to_rated"] > CAPACITY_HEADROOM
    return out[out["night_generation"] | out["over_capacity"]].reset_index()
