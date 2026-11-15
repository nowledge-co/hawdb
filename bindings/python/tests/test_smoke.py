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

"""Statement-level behavior that holds on every database backend.

The `db` fixture parametrizes these tests over a file-backed project and
the in-memory database from `hawdb.open()`. File-only behaviors live in
test_file.py; in-memory specifics in test_memory.py.
"""

import sys

import pytest

import hawdb
from hawdb import exceptions


def test_open_execute_match(db):
    db.execute(
        "CREATE (s:Stock {code: $code, name: $name})", {"code": "603122", "name": "hefu"}
    )
    result = db.execute("MATCH (s:Stock) RETURN s.code, s.name")
    assert set(result.columns) == {"s.code", "s.name"}
    rows = result.fetchall()
    assert rows == [{"s.code": "603122", "s.name": "hefu"}]


def test_parameter_types_round_trip(db):
    db.execute(
        "CREATE (n:Node {i: $i, f: $f, s: $s, b: $b, xs: $xs, m: $m})",
        {
            "i": 7,
            "f": 1.5,
            "s": "text",
            "b": True,
            "xs": [1, "two", None],
            "m": {"k": "v"},
        },
    )
    row = db.execute("MATCH (n:Node) RETURN n.i, n.f, n.s, n.b, n.xs, n.m").fetchone()
    assert row["n.i"] == 7
    assert row["n.f"] == 1.5
    assert row["n.s"] == "text"
    assert row["n.b"] is True
    assert row["n.xs"] == [1, "two", None]
    assert row["n.m"] == {"k": "v"}


def test_fetch_protocols(db):
    db.execute(
        "UNWIND $rows AS row CREATE (:N {x: row.v})",
        {"rows": [{"v": 1}, {"v": 2}, {"v": 3}]},
    )
    result = db.execute("MATCH (n:N) RETURN n.x ORDER BY n.x")
    assert len(result) == 3
    assert result.fetchone() == {"n.x": 1}
    assert result.fetchmany(2) == [{"n.x": 2}, {"n.x": 3}]
    assert result.fetchone() is None
    result = db.execute("MATCH (n:N) RETURN n.x ORDER BY n.x")
    assert [row["n.x"] for row in result] == [1, 2, 3]


def test_fetch_tuples_share_the_cursor(db):
    db.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, label TEXT)")
    db.executemany_sql(
        "INSERT INTO t (id, label) VALUES ($1, $2)", [(i, f"r{i}") for i in range(5)]
    )
    result = db.execute_sql("SELECT label, id FROM t ORDER BY id")
    assert result.fetchone() == {"label": "r0", "id": 0}
    assert result.fetchmany_tuples(2) == [("r1", 1), ("r2", 2)]
    assert result.fetchmany_tuples() == [("r3", 3)]
    assert result.fetchall() == [{"label": "r4", "id": 4}]
    assert result.fetchmany_tuples(10) == []
    assert result.row_count == len(result) == 5
    assert db.execute_sql("SELECT id FROM t WHERE id < 0").fetchmany_tuples(3) == []


def test_fetched_rows_are_new_objects_the_result_does_not_keep(db):
    """Rows are converted per fetch, so the result holds no Python copy."""
    db.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, label TEXT)")
    db.execute_sql("INSERT INTO t (id, label) VALUES (1, 'one')")
    result = db.execute_sql("SELECT id, label FROM t")
    row = result.fetchone()
    # Only `row` and getrefcount's argument refer to the dict.
    assert sys.getrefcount(row) == 2
    row["label"] = "changed"
    assert result._display_rows() == [{"id": 1, "label": "one"}]
    assert result._display_rows()[0] is not result._display_rows()[0]
    (values,) = db.execute_sql("SELECT id, label FROM t").fetchmany_tuples()
    assert sys.getrefcount(values) == 2


def test_first_non_null_scans_the_whole_result(db):
    db.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, late TEXT, never TEXT)")
    db.executemany_sql(
        "INSERT INTO t (id, late, never) VALUES ($1, $2, $3)",
        [(i, "found" if i == 2999 else None, None) for i in range(3000)],
    )
    result = db.execute_sql("SELECT id, late, never FROM t ORDER BY id")
    assert result.fetchmany_tuples(3000)[-1] == (2999, "found", None)
    # Independent of the cursor, which is now at the end.
    assert [result._first_non_null(i) for i in range(3)] == [0, "found", None]
    with pytest.raises(IndexError):
        result._first_non_null(3)


def test_parse_error_maps_to_exception(db):
    with pytest.raises(exceptions.ParseError):
        db.execute("THIS IS NOT CYPHER")


def test_closed_database_rejects_execute(db):
    db.close()
    assert not db.is_open
    with pytest.raises(Exception):
        db.execute("RETURN 1")


def test_context_manager_closes(open_db):
    with open_db() as db:
        db.execute("CREATE (:N)")
    assert not db.is_open


def test_owned_nested_result_survives_database_close(db):
    import math
    import struct
    import uuid

    nan = struct.unpack("<d", struct.pack("<Q", 0x7FF800000000DEAD))[0]
    identity = uuid.UUID("00000000-0000-0000-0000-000000000001")
    payload = {
        "text": "知识\x00🙂" * 4096,
        "binary": b"\x00\xff" * 4096,
        "empty_bytes": b"",
        "empty_string": "",
        "items": [None, True, 2**63 - 1, -0.0, nan, {"uuid": identity}],
    }
    db.execute("CREATE (:Owned {payload: $payload})", {"payload": payload})
    result = db.execute("MATCH (n:Owned) RETURN n.payload AS payload")
    db.close()
    # Results own Python objects even when conversion borrows native values.
    payload["items"].clear()
    output = result.fetchone()["payload"]
    assert output["text"] == "知识\x00🙂" * 4096
    assert output["binary"] == b"\x00\xff" * 4096
    assert type(output["binary"]) is bytes
    assert output["empty_bytes"] == b""
    assert output["empty_string"] == ""
    assert output["items"][:3] == [None, True, 2**63 - 1]
    assert [type(value) for value in output["items"][:3]] == [type(None), bool, int]
    assert struct.pack("<d", output["items"][3]) == struct.pack("<d", -0.0)
    assert math.isnan(output["items"][4])
    assert struct.pack("<d", output["items"][4]) == struct.pack("<d", nan)
    assert output["items"][5] == {"uuid": str(identity)}
    assert result.fetchone() is None


@pytest.mark.parametrize("suffix", ["", " LIMIT 0"])
def test_cypher_return_declaration_order_and_empty_schema(db, suffix):
    db.execute("CREATE (:Stock {code: 'a', price: 1.5})")
    for code in ["a", "missing", "a"]:
        result = db.execute(
            "MATCH (s:Stock) WHERE s.code = $code "
            "RETURN s.price AS price, s.code AS code" + suffix,
            {"code": code},
        )
        assert result.columns == ["price", "code"]
        rows = result.fetchall()
        assert rows == ([{"price": 1.5, "code": "a"}] if code == "a" and not suffix else [])
        if rows:
            assert list(rows[0].values()) == [1.5, "a"]
