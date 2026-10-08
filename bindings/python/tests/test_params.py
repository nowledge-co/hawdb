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

"""Parameter conversion stores numbers exactly or rejects them."""

import decimal
import enum
import fractions

import pytest

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)


class Index:
    """An integer-like object that only implements `__index__`."""

    def __init__(self, value):
        self.value = value

    def __index__(self):
        return self.value


class OnlyFloat:
    """A number that only implements `__float__`."""

    def __float__(self):
        return 0.5


class Score(float):
    pass


class Level(enum.IntEnum):
    HIGH = 3


@pytest.fixture
def table(db):
    db.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
    return db


def _rejects(db, error, value, match):
    """Both entry points reject `value`, also nested, and nothing is written."""
    for params in ({"v": value}, {"v": [1, {"nested": value}]}):
        with pytest.raises(error, match=match):
            db.execute("CREATE (:N {v: $v})", params)
    with pytest.raises(error, match=match):
        db.execute_sql("INSERT INTO t (id, v) VALUES ($1, $2)", [1, value])
    assert db.execute("MATCH (n:N) RETURN count(n) AS n").fetchall() == [{"n": 0}]
    assert db.execute_sql("SELECT id FROM t").fetchall() == []


@pytest.mark.parametrize(
    "value",
    [INT64_MAX + 1, INT64_MIN - 1, 2**70, Index(2**63)],
    ids=["max+1", "min-1", "2**70", "__index__"],
)
def test_out_of_range_integers_raise_overflow(table, value):
    _rejects(table, OverflowError, value, "outside the signed 64-bit range")


@pytest.mark.parametrize(
    "value",
    [decimal.Decimal("1.10"), fractions.Fraction(1, 3), OnlyFloat()],
    ids=["Decimal", "Fraction", "__float__"],
)
def test_inexact_float_conversions_raise_type_error(table, value):
    _rejects(table, TypeError, value, f"unsupported parameter type: {type(value).__name__}")


def test_int64_bounds_round_trip(table):
    table.execute("CREATE (:N {hi: $hi, lo: $lo})", {"hi": INT64_MAX, "lo": INT64_MIN})
    assert table.execute("MATCH (n:N) RETURN n.hi, n.lo").fetchall() == [
        {"n.hi": INT64_MAX, "n.lo": INT64_MIN}
    ]
    table.execute_sql("INSERT INTO t (id, v) VALUES ($1, $2)", [1, INT64_MAX])
    table.execute_sql("INSERT INTO t (id, v) VALUES ($1, $2)", [2, INT64_MIN])
    assert table.execute_sql("SELECT id, v FROM t ORDER BY id").fetchall() == [
        {"id": 1, "v": INT64_MAX},
        {"id": 2, "v": INT64_MIN},
    ]


def test_exact_numbers_keep_their_types(db):
    db.execute(
        "CREATE (:N {b: $b, i: $i, e: $e, f: $f})",
        {"b": True, "i": Index(7), "e": Level.HIGH, "f": Score(1.5)},
    )
    row = db.execute("MATCH (n:N) RETURN n.b, n.i, n.e, n.f").fetchone()
    assert row == {"n.b": True, "n.i": 7, "n.e": 3, "n.f": 1.5}
    assert [type(row[key]) for key in ("n.b", "n.i", "n.e", "n.f")] == [bool, int, int, float]
