# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Temporal values cross the binding as tagged ISO 8601 dicts."""

import datetime

import pytest


def test_tagged_temporal_parameters_round_trip(db):
    values = {
        "d": {"$date": "2026-10-30"},
        "t": {"$localtime": "12:30:00.000000001"},
        "ldt": {"$localdatetime": "2026-10-30T12:30:00"},
        "dt": {"$datetime": "2026-10-30T12:00:00+02:00"},
        "dur": {"$duration": "P1M2DT3.5S"},
    }
    db.execute(
        "CREATE (:E {d: $d, t: $t, ldt: $ldt, dt: $dt, dur: $dur})", values
    )
    row = db.execute(
        "MATCH (e:E) RETURN e.d AS d, e.t AS t, e.ldt AS ldt, e.dt AS dt, "
        "e.dur AS dur, e.d.year AS year"
    ).fetchone()
    assert row == {**values, "year": 2026}


def test_native_datetime_parameters_are_accepted(db):
    zone = datetime.timezone(datetime.timedelta(hours=2))
    db.execute(
        "CREATE (:E {d: $d, t: $t, ldt: $ldt, dt: $dt, dur: $dur})",
        {
            "d": datetime.date(2026, 10, 30),
            "t": datetime.time(12, 30, 0, 500),
            "ldt": datetime.datetime(2026, 10, 30, 12, 30),
            "dt": datetime.datetime(2026, 10, 30, 12, 0, tzinfo=zone),
            "dur": datetime.timedelta(days=1, seconds=90),
        },
    )
    row = db.execute(
        "MATCH (e:E) RETURN e.d AS d, e.t AS t, e.ldt AS ldt, e.dt AS dt, e.dur AS dur"
    ).fetchone()
    assert row == {
        "d": {"$date": "2026-10-30"},
        "t": {"$localtime": "12:30:00.000500"},
        "ldt": {"$localdatetime": "2026-10-30T12:30:00"},
        "dt": {"$datetime": "2026-10-30T12:00:00+02:00"},
        "dur": {"$duration": "P1DT1M30S"},
    }


def test_invalid_temporal_parameters_are_rejected(db):
    with pytest.raises(ValueError):
        db.execute("CREATE (:E {d: $d})", {"d": {"$date": "2026-02-30"}})
    with pytest.raises(TypeError):
        db.execute("CREATE (:E {d: $d})", {"d": {"$date": 20261030}})
    zone = datetime.timezone.utc
    with pytest.raises(TypeError):
        db.execute("CREATE (:E {t: $t})", {"t": datetime.time(12, tzinfo=zone)})


UTC = datetime.timezone.utc
PLUS_0530 = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
MINUS_08 = datetime.timezone(datetime.timedelta(hours=-8))


def _create_events(db):
    db.execute_sql(
        "CREATE TABLE events (id BIGINT PRIMARY KEY, at TIMESTAMP NOT NULL,"
        " seen TIMESTAMPTZ)"
    )
    db.execute_sql("CREATE INDEX events_seen ON events (seen)")


def test_sql_datetime_parameters_round_trip_as_tagged_dicts(db):
    _create_events(db)
    insert = "INSERT INTO events (id, at, seen) VALUES ($1, $2, $3)"
    db.execute_sql(insert, [
        1,
        datetime.datetime(2026, 11, 5, 12, 0, 0, 123456),
        datetime.datetime(2026, 11, 5, 17, 30, tzinfo=PLUS_0530),
    ])
    db.execute_sql(insert, [
        2,
        datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
        datetime.datetime(2026, 11, 5, 4, 0, tzinfo=MINUS_08),
    ])
    db.execute_sql(insert, [3, datetime.datetime(2026, 11, 6), None])
    rows = db.execute_sql("SELECT id, at, seen FROM events ORDER BY id").fetchall()
    # Aware datetimes in any zone are stored as their UTC instant.
    assert rows == [
        {
            "id": 1,
            "at": {"$localdatetime": "2026-11-05T12:00:00.123456"},
            "seen": {"$datetime": "2026-11-05T12:00:00Z"},
        },
        {
            "id": 2,
            "at": {"$localdatetime": "1969-12-31T23:59:59.999999"},
            "seen": {"$datetime": "2026-11-05T12:00:00Z"},
        },
        {"id": 3, "at": {"$localdatetime": "2026-11-06T00:00:00"}, "seen": None},
    ]
    # Cypher results keep the same tagged form.
    assert db.execute(
        "UNWIND [1] AS x RETURN $at AS at", {"at": datetime.datetime(2026, 11, 5, 12, tzinfo=UTC)}
    ).fetchone() == {"at": {"$datetime": "2026-11-05T12:00:00Z"}}


def test_sql_datetime_parameters_compare_and_seek_indexes(db):
    _create_events(db)
    insert = "INSERT INTO events (id, at, seen) VALUES ($1, $2, $3)"
    for ident, hour in enumerate([1, 6, 11, 16, 21], start=1):
        db.execute_sql(insert, [
            ident,
            datetime.datetime(2026, 11, 5, hour),
            datetime.datetime(2026, 11, 5, hour, tzinfo=UTC),
        ])
    # 03:00 at -08:00 is 11:00 UTC; 18:30 at +05:30 is 13:00 UTC.
    query = "SELECT id FROM events WHERE seen >= $1 AND seen < $2 ORDER BY id"
    bounds = [
        datetime.datetime(2026, 11, 5, 3, tzinfo=MINUS_08),
        datetime.datetime(2026, 11, 5, 18, 30, tzinfo=PLUS_0530),
    ]
    assert db.execute_sql(query, bounds).fetchall() == [{"id": 3}]
    plan = db.execute_sql(f"EXPLAIN {query}", bounds).fetchall()
    assert any("IndexRangeScanExec" in str(row.get("id")) for row in plan), plan
    assert db.execute_sql(
        "SELECT id FROM events WHERE at > $1 ORDER BY id",
        [datetime.datetime(2026, 11, 5, 16)],
    ).fetchall() == [{"id": 5}]
    # Naive datetimes are TIMESTAMP, aware ones TIMESTAMPTZ; mixing needs a cast.
    with pytest.raises(Exception, match="incompatible"):
        db.execute_sql(
            "SELECT id FROM events WHERE at > $1",
            [datetime.datetime(2026, 11, 5, tzinfo=UTC)],
        )
    with pytest.raises(Exception, match="DATE, TIME and DURATION"):
        db.execute_sql(
            "INSERT INTO events (id, at) VALUES (9, $1)", [datetime.date(2026, 11, 5)]
        )
