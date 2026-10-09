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
