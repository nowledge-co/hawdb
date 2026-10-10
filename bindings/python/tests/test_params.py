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

"""Parameter conversion stores values exactly or rejects them by path."""

import decimal
import enum
import fractions
import re

import pytest

INT64_MAX = 2**63 - 1
INT64_MIN = -(2**63)

# A statement that reads no parameters, for tests that only convert them.
COUNT = "MATCH (n:N) RETURN count(n) AS n"


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


def _numpy_like(name, value):
    """A stand-in for a NumPy scalar, which the binding recognizes by type."""
    return type(name, (), {"__module__": "numpy", "__float__": lambda self: value})()


@pytest.fixture
def table(db):
    db.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
    return db


def _at(path, message):
    """Match exactly the error `message` for the value at `path`."""
    return f"^{re.escape(f'{path}: {message}')}$"


def _rejects(db, error, value, message):
    """Both entry points reject `value`, also nested, by path and write nothing."""
    for params, path in (({"v": value}, "$v"), ({"v": [1, {"nested": value}]}, "$v[1].nested")):
        with pytest.raises(error, match=_at(path, message)):
            db.execute("CREATE (:N {v: $v})", params)
    with pytest.raises(error, match=_at("$2", message)):
        db.execute_sql("INSERT INTO t (id, v) VALUES ($1, $2)", [1, value])
    assert db.execute(COUNT).fetchall() == [{"n": 0}]
    assert db.execute_sql("SELECT id FROM t").fetchall() == []


@pytest.mark.parametrize(
    "value",
    [INT64_MAX + 1, INT64_MIN - 1, 2**70, Index(2**63)],
    ids=["max+1", "min-1", "2**70", "__index__"],
)
def test_out_of_range_integers_raise_overflow(table, value):
    _rejects(table, OverflowError, value, "integer parameter is outside the signed 64-bit range")


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


def test_numpy_narrow_floats_bind_as_floats(table):
    # float16 and float32 widen to f64 exactly; longdouble may not.
    table.execute(
        "CREATE (:N {h: $h, s: $s})",
        {"h": _numpy_like("float16", 0.5), "s": [_numpy_like("float32", 1.25)]},
    )
    assert table.execute("MATCH (n:N) RETURN n.h, n.s").fetchall() == [
        {"n.h": 0.5, "n.s": [1.25]}
    ]
    with pytest.raises(TypeError, match="unsupported parameter type: longdouble"):
        table.execute("CREATE (:N {v: $v})", {"v": _numpy_like("longdouble", 0.5)})


def test_numpy_scalars(db):
    np = pytest.importorskip("numpy")
    embedding = np.array([1.1, 2.25], dtype=np.float32)
    db.execute(
        "CREATE (:N {f16: $f16, f64: $f64, i: $i, b: $b, xs: $xs})",
        {
            "f16": np.float16(0.5),
            "f64": np.float64(2.5),
            "i": np.int64(7),
            "b": np.bool_(True),
            "xs": list(embedding),
        },
    )
    assert db.execute("MATCH (n:N) RETURN n.f16, n.f64, n.i, n.b, n.xs").fetchone() == {
        "n.f16": 0.5,
        "n.f64": 2.5,
        "n.i": 7,
        "n.b": True,
        "n.xs": embedding.tolist(),
    }
    with pytest.raises(TypeError, match="unsupported parameter type: longdouble"):
        db.execute("CREATE (:N {v: $v})", {"v": np.longdouble(0.5)})


def test_errors_name_the_path(table):
    # Conversion fails before the statement runs, so one handle serves every case.
    unsupported = "unsupported parameter type: Decimal"
    bad = decimal.Decimal("1")
    fake_uuid = type("UUID", (), {"__str__": lambda self: "not-a-uuid"})()
    odd_module = type("Odd", (), {"__module__": None})()
    cases = [
        ({"rows": [{"v": 1}, {"v": bad}]}, "$rows[1].v", unsupported),
        ({"t": (1, (2, bad))}, "$t[1][1]", unsupported),
        ({"m": {"first name": bad}}, '$m["first name"]', unsupported),
        ({"m": {'say "hi"': bad}}, '$m["say \\"hi\\""]', unsupported),
        ({"m": {"价格": bad}}, '$m["价格"]', unsupported),
        ({"first name": bad}, '$["first name"]', unsupported),
        ({"a.b": bad}, '$["a.b"]', unsupported),
        ({"v": [fake_uuid]}, "$v[0]", "invalid uuid.UUID value: not-a-uuid"),
        ({"v": [odd_module]}, "$v[0]", "unsupported parameter type: Odd"),
        ({"m": [{1: "x"}]}, "$m[0]", "parameter maps require string keys, got int"),
    ]
    for params, path, message in cases:
        with pytest.raises(TypeError, match=_at(path, message)):
            table.execute(COUNT, params)
    with pytest.raises(TypeError, match="^query parameter names must be strings, got int$"):
        table.execute(COUNT, {1: "x"})
    with pytest.raises(TypeError, match=_at("$2[0]", unsupported)):
        table.execute_sql("INSERT INTO t (id, v) VALUES ($1, $2)", [1, [bad]])


def test_strings_must_be_valid_utf8(db):
    for params in ({"\ud800": 1}, {"m": {"\ud800": 1}}, {"v": ["\ud800"]}):
        with pytest.raises(UnicodeEncodeError):
            db.execute(COUNT, params)


def _nested(levels, leaf):
    for _ in range(levels):
        leaf = [leaf]
    return leaf


def test_nesting_is_bounded(db):
    assert db.execute(COUNT, {"v": _nested(64, 1)}).fetchall() == [{"n": 0}]
    message = "parameter nests deeper than 64 levels"
    with pytest.raises(ValueError, match=_at("$v" + "[0]" * 64, message)):
        db.execute(COUNT, {"v": _nested(65, 1)})
    # A value that contains itself stops at the same limit instead of
    # recursing until the stack overflows.
    looped = []
    looped.append(looped)
    with pytest.raises(ValueError, match=_at("$v" + "[0]" * 64, message)):
        db.execute(COUNT, {"v": looped})
    cycle = {}
    cycle["next"] = cycle
    with pytest.raises(ValueError, match=_at("$v" + ".next" * 64, message)):
        db.execute(COUNT, {"v": cycle})


def test_transactions_name_the_path(table):
    for begin in (table.transaction, table.read_transaction):
        with begin() as tx:
            _rejects(tx, TypeError, decimal.Decimal("1"), "unsupported parameter type: Decimal")
