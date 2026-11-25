"""ClickHouse store: same answers as Postgres, and re-runnable loads.

Integration tests need both databases (docker compose up -d). They use an isolated Postgres
schema and ClickHouse database, so they never touch pipeline data, and skip if either database
is unreachable. Run only the quick tests with: pytest -m "not integration".
"""
import datetime as dt
import os
import numpy as np
import pandas as pd
import pytest
from fleethealth import ch, db, load, metrics

SLOTS = [f"{h}:{m:02d}" for h in range(24) for m in (0, 30)][1:] + ["0:00"]
JAN = (dt.date(2013, 1, 1), dt.date(2013, 1, 31))
TEST_SCHEMA = "fleet_test"


def test_months_split_partial_ranges_into_calendar_months():
    assert ch._months(dt.date(2012, 12, 20), dt.date(2013, 2, 3)) == [
        (201212, dt.date(2012, 12, 1), dt.date(2012, 12, 31)),
        (201301, dt.date(2013, 1, 1), dt.date(2013, 1, 31)),
        (201302, dt.date(2013, 2, 1), dt.date(2013, 2, 28))]


def _fleet(days, n_sites=8, seed=1) -> pd.DataFrame:
    """Synthetic wide file: a daylight bell curve per site, with the problems the real data has
    (a site that is dead for some days, a missing value, a site-day with no GG rows)."""
    rng = np.random.default_rng(seed)
    hours = np.array([(i + 0.5) / 2 for i in range(48)])
    bell = np.clip(np.sin((hours - 6) / 13 * np.pi), 0, None)
    rows = []
    for site in range(1, n_sites + 1):
        cap = round(float(rng.uniform(1.0, 3.0)), 2)
        for d in days:
            weather = rng.uniform(0.3, 1.0)
            gen = np.round(bell * cap * 0.5 * weather * rng.uniform(0.8, 1.0), 3)
            if site == 3 and d.day in (10, 11, 12):
                gen[:] = 0.0                                   # dead system
            use = np.round(rng.uniform(0.05, 0.6, 48), 3)
            common = {"site_id": site, "capacity_kw": cap, "postcode": 2000 + site % 3, "local_date": d}
            g = {**common, "channel": "GG", **dict(zip(SLOTS, gen))}
            if site == 5 and d.day == 15:
                g["12:30"] = np.nan                            # quarantined: missing_value
            if not (site == 7 and d.day == 20):                # site-day with no GG rows at all
                rows.append(g)
            rows.append({**common, "channel": "GC", **dict(zip(SLOTS, use))})
    return pd.DataFrame(rows)


def _days(a, b):
    return [a + dt.timedelta(days=i) for i in range((b - a).days + 1)]


@pytest.fixture
def stores():
    try:
        conn = db.connect(os.environ.get("FLEET_TEST_DSN", db.DSN))
        admin = ch.connect(os.environ.get("FLEET_TEST_CH_DSN", ch.CLICKHOUSE_DSN))
    except Exception as e:                                     # pragma: no cover
        if os.environ.get("FLEET_REQUIRE_DB"):                 # CI: a missing database is a failure
            raise
        pytest.skip(f"databases not reachable: {e}")
    conn.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    conn.execute(f"CREATE SCHEMA {TEST_SCHEMA}")
    conn.execute(f"SET search_path TO {TEST_SCHEMA}")
    db.init_schema(conn)
    admin.command(f"DROP DATABASE IF EXISTS {TEST_SCHEMA}")
    admin.command(f"CREATE DATABASE {TEST_SCHEMA}")
    client = ch.connect(os.environ.get("FLEET_TEST_CH_DSN", ch.CLICKHOUSE_DSN), database=TEST_SCHEMA)
    ch.init_schema(client)
    yield conn, client
    conn.rollback()
    conn.execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE")
    conn.commit()
    conn.close()
    admin.command(f"DROP DATABASE IF EXISTS {TEST_SCHEMA}")


def _load_both(conn, client, wide, a, b):
    batch = load.prepare(wide, a, b)
    load.ingest(conn, wide, a, b, batch=batch)
    ch.ingest(client, batch)
    metrics.rebuild(conn, a, b)
    ch.rebuild(client, a, b)


def _totals(client):
    return client.query("SELECT count(), round(sum(kwh), 3) FROM readings").result_rows[0]


@pytest.mark.integration
def test_daily_metrics_match_postgres(stores):
    conn, client = stores
    wide = _fleet(_days(*JAN))
    _load_both(conn, client, wide, *JAN)

    p, c = metrics.fetch(conn), ch.fetch(client)
    assert len(p) == len(c) > 0
    m = p.merge(c, on=["site_id", "day"], suffixes=("_pg", "_ch"), validate="one_to_one")
    for col in ["gen_kwh", "cons_kwh", "gen_intervals", "yield_kwh_per_kw", "fleet_yield", "perf_ratio"]:
        x, y = m[f"{col}_pg"].astype(float), m[f"{col}_ch"].astype(float)
        assert (x.isna() == y.isna()).all(), f"{col}: NULLs differ"
        # Postgres stores and sums `real` (float32), so agreement is to float32 rounding
        np.testing.assert_allclose(y[x.notna()], x[x.notna()], rtol=1e-5, err_msg=col)

    # the edge cases came through identically
    no_gg = m[(m.site_id == 7) & (m.day == "2013-01-20")].iloc[0]
    assert pd.isna(no_gg.gen_kwh_ch) and no_gg.gen_intervals_ch == 0
    pg_q = conn.execute("SELECT reason, count(*) FROM quarantine GROUP BY 1").fetchall()
    ch_q = client.query("SELECT reason, count() FROM quarantine GROUP BY reason").result_rows
    assert sorted(pg_q) == sorted(ch_q) == [("missing_value", 1)]


@pytest.mark.integration
def test_reloading_a_month_does_not_duplicate(stores):
    conn, client = stores
    wide = _fleet(_days(*JAN))
    batch = load.prepare(wide, *JAN)
    ch.ingest(client, batch)
    ch.rebuild(client, *JAN)
    before = _totals(client), ch.fetch(client)
    ch.ingest(client, batch)
    ch.rebuild(client, *JAN)
    assert _totals(client) == before[0]
    pd.testing.assert_frame_equal(ch.fetch(client), before[1])
    assert client.command("SELECT count() FROM quarantine") == 1
    assert not client.command("SELECT count() FROM system.tables WHERE database = currentDatabase() "
                              "AND name LIKE '%_stage_%'")


@pytest.mark.integration
def test_partial_month_reload_changes_only_its_days(stores):
    conn, client = stores
    wide = _fleet(_days(*JAN))
    ch.ingest(client, load.prepare(wide, *JAN))
    n, _ = _totals(client)
    daily = lambda: dict(client.query("SELECT day, sum(kwh) FROM readings GROUP BY day").result_rows)
    before = daily()

    corrected = wide.copy()                                    # a corrected file for 10-12 Jan
    corrected[SLOTS] = corrected[SLOTS] * 2
    ch.ingest(client, load.prepare(corrected, dt.date(2013, 1, 10), dt.date(2013, 1, 12)))

    after = daily()
    assert _totals(client)[0] == n
    for d, v in before.items():
        if dt.date(2013, 1, 10) <= d <= dt.date(2013, 1, 12):
            assert after[d] == pytest.approx(2 * v, rel=1e-5)
        else:
            assert after[d] == pytest.approx(v, rel=1e-6)


@pytest.mark.integration
def test_metrics_rebuild_for_one_day_matches_full_rebuild(stores):
    """ClickHouse recomputes whole months; that must equal recomputing just the day asked for."""
    conn, client = stores
    wide = _fleet(_days(*JAN))
    _load_both(conn, client, wide, *JAN)
    day = dt.date(2013, 1, 15)
    metrics.rebuild(conn, day, day)
    ch.rebuild(client, day, day)
    p, c = metrics.fetch(conn, day, day), ch.fetch(client, day, day)
    np.testing.assert_allclose(c["perf_ratio"], p["perf_ratio"], rtol=1e-5)
    assert len(ch.fetch(client)) == len(metrics.fetch(conn))
