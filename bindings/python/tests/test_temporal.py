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
