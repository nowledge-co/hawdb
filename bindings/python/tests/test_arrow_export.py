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

"""``QueryResult.to_arrow()``, ``to_pandas()`` and ``to_polars()``.

PyArrow, pandas and Polars are optional. Tests that need one skip without
it; the others prove that the methods import them only when called and name
the extra that installs them.
"""

import calendar
import datetime
import decimal
import os
import struct
import subprocess
import sys
import uuid

import pytest

import hawdb


@pytest.fixture
def pa():
    return pytest.importorskip("pyarrow")


def _run_fresh(code):
    # A fresh interpreter, because other tests may have imported the packages.
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    subprocess.run([sys.executable, "-c", code], check=True, env=env)


def _epoch_nanos(*fields, nanos=0):
    return calendar.timegm(fields) * 10**9 + nanos


def test_import_and_queries_do_not_load_arrow_packages():
    _run_fresh(
        "import sys, hawdb\n"
        "db = hawdb.open()\n"
        "result = db.execute('UNWIND [1, 2] AS x RETURN x')\n"
        "assert result.fetchall() == [{'x': 1}, {'x': 2}]\n"
        "for name in ('pyarrow', 'pandas', 'polars'):\n"
        "    assert name not in sys.modules, name\n"
    )


@pytest.mark.parametrize(
    "method,missing,extra,imported_first",
    [
        ("to_arrow", "pyarrow", "arrow", None),
        ("to_pandas", "pandas", "pandas", None),
        ("to_pandas", "pyarrow", "pandas", "pandas"),
        ("to_polars", "polars", "polars", None),
        ("to_polars", "pyarrow", "polars", "polars"),
    ],
)
def test_a_missing_package_names_the_extra(db, monkeypatch, method, missing, extra, imported_first):
    result = db.execute("UNWIND [1] AS x RETURN x")
    if imported_first:
        # The method imports this package before `missing`.
        pytest.importorskip(imported_first)
    # A None entry makes `import` raise ModuleNotFoundError, as when the
    # package is not installed.
    monkeypatch.setitem(sys.modules, missing, None)
    with pytest.raises(ImportError) as error:
        getattr(result, method)()
    assert str(error.value) == (
        f"QueryResult.{method}() requires {missing}; "
        f"install it with \"pip install 'hawdb[{extra}]'\""
    )
    assert isinstance(error.value.__cause__, ImportError)
    assert result.fetchall() == [{"x": 1}]


def test_scalars_keep_result_order_and_exact_values(pa, db):
    result = db.execute(
        "UNWIND [0] AS z RETURN $i AS zi, $f AS f, $b AS b, $s AS s, $n AS n, "
        "$bytes AS a_bytes, $u AS u, $d AS d",
        {
            "i": -(2**63),
            "f": 0.1,
            "b": True,
            "s": "héllo",
            "n": None,
            "bytes": b"\x00\xff",
            # Hex letters, so the text must be the canonical lowercase form.
            "u": uuid.UUID("0a1b2c3d-4e5f-4061-8a9b-0c1d2e3f4a5b"),
            "d": decimal.Decimal("12.3400"),
        },
    )
    table = result.to_arrow()
    assert isinstance(table, pa.Table)
    assert table.column_names == ["zi", "f", "b", "s", "n", "a_bytes", "u", "d"]
    assert table.schema.types == [
        pa.int64(),
        pa.float64(),
        pa.bool_(),
        pa.string(),
        pa.null(),
        pa.binary(),
        pa.string(),
        pa.decimal128(6, 4),
    ]
    assert table.to_pylist() == [
        {
            "zi": -(2**63),
            "f": 0.1,
            "b": True,
            "s": "héllo",
            "n": None,
            "a_bytes": b"\x00\xff",
            "u": "0a1b2c3d-4e5f-4061-8a9b-0c1d2e3f4a5b",
            "d": decimal.Decimal("12.3400"),
        }
    ]


def test_null_and_list_columns(pa, db):
    result = db.execute("UNWIND [[1, 2], null, [], [null, 3]] AS l RETURN l, null AS n")
    table = result.to_arrow()
    assert table.schema.field("l").type == pa.list_(pa.int64())
    assert table.column("l").to_pylist() == [[1, 2], None, [], [None, 3]]
    assert table.schema.field("n").type == pa.null()
    assert table.column("n").null_count == 4
    empty = db.execute("UNWIND [[], []] AS l RETURN l").to_arrow()
    assert empty.schema.field("l").type == pa.list_(pa.null())


def test_results_without_rows_or_columns(pa, db):
    table = db.execute("MATCH (n:Missing) RETURN n.name AS name, n AS n").to_arrow()
    assert table.num_rows == 0
    assert table.column_names == ["name", "n"]
    assert table.schema.types == [pa.null(), pa.null()]
    created = db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
    assert created.columns == []
    assert created.to_arrow().shape == (0, 0)


def test_nodes_and_relationships_stay_nested_structs(pa, db):
    db.execute("CREATE (:P {name: 'a', age: 3})-[:K {w: 1.5}]->(:P {name: 'b'})")
    query = "MATCH (a:P)-[r:K]->(b:P) RETURN b, r, a"
    rows = db.execute(query).fetchall()
    table = db.execute(query).to_arrow()
    assert table.column_names == ["b", "r", "a"]
    assert table.to_pylist() == rows
    labels = ("labels", pa.list_(pa.string()))
    assert table.schema.field("a").type == pa.struct(
        [("_id", pa.int64()), ("age", pa.int64()), labels, ("name", pa.string())]
    )
    assert table.schema.field("b").type == pa.struct(
        [("_id", pa.int64()), labels, ("name", pa.string())]
    )
    # Across rows, a node without a property gets a null field, not a column.
    nodes = db.execute("MATCH (n:P) RETURN n ORDER BY n.name").to_arrow()
    assert nodes.schema.field("n").type == table.schema.field("a").type
    assert [node["age"] for node in nodes.column("n").to_pylist()] == [3, None]
    assert table.schema.field("r").type == pa.struct(
        [
            ("_id", pa.int64()),
            ("source_id", pa.int64()),
            ("target_id", pa.int64()),
            ("type", pa.string()),
            ("w", pa.float64()),
        ]
    )


def test_maps_union_their_keys_across_rows(pa, db):
    table = db.execute(
        "UNWIND [{a: 1}, {b: 'x'}, null, {a: 2, c: [true]}] AS m RETURN m"
    ).to_arrow()
    assert table.schema.field("m").type == pa.struct(
        [("a", pa.int64()), ("b", pa.string()), ("c", pa.list_(pa.bool_()))]
    )
    assert table.column("m").to_pylist() == [
        {"a": 1, "b": None, "c": None},
        {"a": None, "b": "x", "c": None},
        None,
        {"a": 2, "b": None, "c": [True]},
    ]


def test_temporal_values_are_exact(pa, db):
    result = db.execute(
        "UNWIND [0] AS z RETURN date('2026-10-30') AS d, "
        "localtime('12:34:56.123456789') AS t, "
        "localdatetime('2026-10-30T12:34:56.123456789') AS ldt, "
        "datetime('2026-10-30T12:34:56.5+02:00') AS dt, "
        "duration('P1Y2M3DT4H5M6.789S') AS dur, "
        "date('+10000-01-01') AS far, date('-0001-01-01') AS bc"
    )
    table = result.to_arrow()
    assert table.schema.types == [
        pa.date32(),
        pa.time64("ns"),
        pa.timestamp("ns"),
        pa.timestamp("ns", tz="+02:00"),
        pa.month_day_nano_interval(),
        pa.date32(),
        pa.date32(),
    ]
    epoch = datetime.date(1970, 1, 1)
    # Days since 1970: -0001-01-01 is 731 days (years -1 and leap year 0)
    # before 0001-01-01, and +10000-01-01 the day after 9999-12-31.
    assert [table.column(name).cast(pa.int32())[0].as_py() for name in ("d", "far", "bc")] == [
        (datetime.date(2026, 10, 30) - epoch).days,
        (datetime.date(9999, 12, 31) - epoch).days + 1,
        (datetime.date(1, 1, 1) - epoch).days - 731,
    ]
    assert table.column("d").to_pylist() == [datetime.date(2026, 10, 30)]
    assert table.column("t").cast(pa.int64()).to_pylist() == [45296123456789]
    assert table.column("ldt").cast(pa.int64()).to_pylist() == [
        _epoch_nanos(2026, 10, 30, 12, 34, 56, nanos=123456789)
    ]
    # Arrow stores the UTC instant; the column's time zone keeps the offset.
    assert table.column("dt").cast(pa.int64()).to_pylist() == [
        _epoch_nanos(2026, 10, 30, 10, 34, 56, nanos=500_000_000)
    ]
    time_part = ((4 * 60 + 5) * 60 + 6) * 10**9 + 789_000_000
    assert table.column("dur").to_pylist() == [(14, 3, time_part)]
    negative = db.execute(
        "UNWIND [0] AS z RETURN duration('P-1M') AS a, duration('PT-1.5S') AS b"
    ).to_arrow()
    assert negative.to_pylist() == [{"a": (-1, 0, 0), "b": (0, 0, -1_500_000_000)}]


@pytest.mark.parametrize(
    "texts,zone",
    [
        (["2026-01-01T00:00+02:00", "2026-06-01T12:00+02:00"], "+02:00"),
        (["2026-01-01T00:00-05:30"], "-05:30"),
        (["2026-01-01T00:00Z", "2026-01-01T00:00+00:00"], "UTC"),
        (["2026-01-01T00:00+02:00", "2026-01-01T00:00Z"], "UTC"),
    ],
)
def test_zoned_datetimes_share_an_offset_or_use_utc(pa, db, texts, zone):
    table = db.execute(
        "UNWIND $texts AS text RETURN datetime(text) AS dt", {"texts": texts}
    ).to_arrow()
    assert table.schema.field("dt").type == pa.timestamp("ns", tz=zone)
    expected = [
        int(datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
        * 10**9
        for text in texts
    ]
    assert table.column("dt").cast(pa.int64()).to_pylist() == expected


def test_sql_timestamps(pa, db):
    table = db.execute_sql(
        "SELECT CAST('2026-10-30 12:34:56.123456' AS TIMESTAMP) AS ts, "
        "CURRENT_TIMESTAMP AS now"
    ).to_arrow()
    assert table.schema.types == [pa.timestamp("ns"), pa.timestamp("ns", tz="UTC")]
    assert table.column("ts").cast(pa.int64()).to_pylist() == [
        _epoch_nanos(2026, 10, 30, 12, 34, 56, nanos=123_456_000)
    ]


def test_sql_table_rows(pa, db):
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, price DECIMAL(10, 2))")
    db.executemany_sql(
        "INSERT INTO items (id, name, price) VALUES ($1, $2, $3)",
        [(1, "pen", decimal.Decimal("1.50")), (2, None, decimal.Decimal("120.00"))],
    )
    table = db.execute_sql("SELECT id, name, price FROM items ORDER BY id").to_arrow()
    # The type comes from the values, not the declared DECIMAL(10, 2).
    assert table.schema.types == [pa.int64(), pa.string(), pa.decimal128(5, 2)]
    assert table.to_pylist() == [
        {"id": 1, "name": "pen", "price": decimal.Decimal("1.50")},
        {"id": 2, "name": None, "price": decimal.Decimal("120.00")},
    ]


def test_decimals_widen_to_the_largest_digits_and_scale(pa, db):
    # Neither the largest scale nor the most integer digits come last.
    values = [decimal.Decimal("-0.001"), decimal.Decimal("123.25"), decimal.Decimal("1.5")]
    table = db.execute("UNWIND $d AS d RETURN d", {"d": values}).to_arrow()
    assert table.schema.field("d").type == pa.decimal128(6, 3)
    assert table.column("d").to_pylist() == values
    wide = [decimal.Decimal("9" * 38), decimal.Decimal("0." + "1" * 38)]
    table = db.execute("UNWIND $d AS d RETURN d", {"d": wide}).to_arrow()
    assert table.schema.field("d").type == pa.decimal256(76, 38)
    assert table.column("d").to_pylist() == wide


def test_vectors_are_float32_lists(pa, db):
    def column(vectors):
        params = {"v": [None if v is None else {"$vector": v} for v in vectors]}
        return db.execute("UNWIND $v AS v RETURN v", params).to_arrow().column("v")

    same = column([[1.0, 2.5], None, [0.1, -3.0]])
    assert same.type == pa.list_(pa.float32(), 2)
    tenth = struct.unpack("f", struct.pack("f", 0.1))[0]
    assert same.to_pylist() == [[1.0, 2.5], None, [tenth, -3.0]]
    mixed = column([[1.0, 2.0], [3.0]])
    assert mixed.type == pa.list_(pa.float32())
    assert mixed.to_pylist() == [[1.0, 2.0], [3.0]]


def test_uuids_and_strings_share_a_string_column(pa, db):
    identifier = uuid.UUID("a1b2c3d4-e5f6-4789-8abc-def012345678")
    table = db.execute(
        "UNWIND $x AS x RETURN x", {"x": [identifier, "plain", None]}
    ).to_arrow()
    assert table.schema.field("x").type == pa.string()
    assert table.column("x").to_pylist() == [str(identifier), "plain", None]


@pytest.mark.parametrize(
    "statement,message",
    [
        ("UNWIND [1, 'a'] AS x RETURN x", 'column "x", row 1: string value after integer'),
        ("UNWIND [1, 2.5] AS x RETURN x", 'column "x", row 1: float value after integer'),
        ("UNWIND [[1], 'a'] AS x RETURN x", 'column "x", row 1: string value after list'),
        (
            "UNWIND [{a: 1}, {a: 'x'}] AS m RETURN m",
            'column "m", row 1, at $.a: string value after integer',
        ),
        (
            "UNWIND [0] AS z RETURN [1, 'a'] AS l",
            'column "l", row 0, at $[*]: string value after integer',
        ),
        (
            "UNWIND [{tags: [1]}, {tags: [true]}] AS m RETURN m",
            'column "m", row 1, at $.tags[*]: boolean value after integer',
        ),
    ],
)
def test_mixed_kinds_raise_type_error_with_their_place(pa, db, statement, message):
    result = db.execute(statement)
    with pytest.raises(TypeError) as error:
        result.to_arrow()
    assert str(error.value).startswith(f"QueryResult.to_arrow(): {message} values;")
    assert str(error.value).endswith("so cast the values in the query")


def test_conflict_paths_quote_keys_that_are_not_identifiers(pa, db):
    # Cypher map literals cannot spell these keys, so a parameter holds them.
    rows = [{"first name": {"1st": [1]}}, {"first name": {"1st": ["x"]}}]
    result = db.execute("UNWIND $rows AS m RETURN m", {"rows": rows})
    with pytest.raises(TypeError) as error:
        result.to_arrow()
    assert str(error.value).startswith(
        'QueryResult.to_arrow(): column "m", row 1, at $["first name"]["1st"][*]: '
        "string value after integer values;"
    )


@pytest.mark.parametrize(
    "fits,outside,target",
    [
        (
            "localdatetime('2262-04-11T23:47:16.854775807')",
            "localdatetime('+10000-01-01T00:00')",
            "timestamp[ns]",
        ),
        (
            "datetime('1677-09-21T00:12:43.145224192Z')",
            "datetime('1600-01-01T00:00Z')",
            "timestamp[ns]",
        ),
        ("duration('P1M')", "duration('P250000000Y')", "month_day_nano_interval"),
        ("duration('P1D')", "duration('P3000000000D')", "month_day_nano_interval"),
        ("duration('PT1S')", "duration('PT9999999999999S')", "month_day_nano_interval"),
    ],
)
def test_values_outside_their_arrow_type_raise_value_error(pa, db, fits, outside, target):
    assert db.execute(f"UNWIND [{fits}] AS v RETURN v").to_arrow().num_rows == 1
    result = db.execute(f"UNWIND [{fits}, {outside}] AS v RETURN v")
    with pytest.raises(ValueError) as error:
        result.to_arrow()
    assert str(error.value).startswith('QueryResult.to_arrow(): column "v", row 1: ')
    assert f"does not fit Arrow's {target}" in str(error.value)


def test_conversion_ignores_and_keeps_the_fetch_cursor(pa, db):
    result = db.execute("UNWIND [1, 2, 3] AS x RETURN x")
    assert result.fetchone() == {"x": 1}
    assert result.to_arrow().column("x").to_pylist() == [1, 2, 3]
    assert result.fetchall() == [{"x": 2}, {"x": 3}]
    assert result.to_arrow().num_rows == 3


def test_transaction_results_convert(pa, db):
    with db.transaction() as tx:
        tx.execute("CREATE (:Item {id: 1})")
        table = tx.execute("MATCH (i:Item) RETURN i.id AS id").to_arrow()
    assert table.to_pylist() == [{"id": 1}]


def test_to_pandas(pa, db):
    pandas = pytest.importorskip("pandas")
    db.execute("CREATE (:P {name: 'a', age: 3})")
    db.execute("CREATE (:P {name: 'b'})")
    result = db.execute(
        "MATCH (n:P) RETURN n.name AS name, n.age AS age, n AS node ORDER BY name"
    )
    assert result.fetchone()["name"] == "a"
    frame = result.to_pandas()
    assert result.fetchone()["name"] == "b"
    assert isinstance(frame, pandas.DataFrame)
    assert list(frame.columns) == ["name", "age", "node"]
    assert frame["name"].tolist() == ["a", "b"]
    # pandas' default conversion: a nullable integer column becomes float.
    assert frame["age"].tolist()[0] == 3
    assert pandas.isna(frame["age"].tolist()[1])
    assert list(frame["node"].tolist()[0]["labels"]) == ["P"]


def test_to_polars(pa, db):
    polars = pytest.importorskip("polars")
    db.execute("CREATE (:P {name: 'a', age: 3})")
    db.execute("CREATE (:P {name: 'b'})")
    result = db.execute(
        "MATCH (n:P) RETURN n.name AS name, n.age AS age, n AS node, "
        "datetime('2026-01-01T00:00Z') AS at ORDER BY name"
    )
    assert result.fetchone()["name"] == "a"
    frame = result.to_polars()
    assert result.fetchone()["name"] == "b"
    assert isinstance(frame, polars.DataFrame)
    assert frame.columns == ["name", "age", "node", "at"]
    assert frame.schema["name"] == polars.String
    assert frame.schema["age"] == polars.Int64
    assert frame.schema["at"] == polars.Datetime("ns", "UTC")
    assert frame["age"].to_list() == [3, None]
    assert frame["node"].to_list()[1]["name"] == "b"
