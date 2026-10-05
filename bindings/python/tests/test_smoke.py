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

import uuid

import pytest

import hawdb
from hawdb import exceptions


def test_open_execute_match(tmp_path):
    db = hawdb.open(tmp_path / "graph")
    db.execute("CREATE (s:Stock {code: $code, name: $name})", {"code": "603122", "name": "hefu"})
    result = db.execute("MATCH (s:Stock) RETURN s.code, s.name")
    assert set(result.columns) == {"s.code", "s.name"}
    rows = result.fetchall()
    assert rows == [{"s.code": "603122", "s.name": "hefu"}]
    db.close()


def test_parameter_types_round_trip(tmp_path):
    db = hawdb.open(tmp_path / "params")
    ident = uuid.uuid4()
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
    db.close()


def test_fetch_protocols(tmp_path):
    db = hawdb.open(tmp_path / "rows")
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
    db.close()


def test_parse_error_maps_to_exception(tmp_path):
    db = hawdb.open(tmp_path / "errors")
    with pytest.raises(exceptions.ParseError):
        db.execute("THIS IS NOT CYPHER")
    db.close()


def test_closed_database_rejects_execute(tmp_path):
    db = hawdb.open(tmp_path / "closed")
    db.close()
    assert not db.is_open
    with pytest.raises(Exception):
        db.execute("RETURN 1")


def test_context_manager_closes(tmp_path):
    with hawdb.open(tmp_path / "ctx") as db:
        db.execute("CREATE (:N)")
    assert not db.is_open


def test_open_rejects_missing_parent(tmp_path):
    with pytest.raises(exceptions.Error):
        hawdb.open(tmp_path / "missing" / "deep" / "db")


def test_deferred_admission_keeps_metadata_and_retries_after_busy_writer(tmp_path):
    path = tmp_path / "deferred"
    writer = hawdb.open(path)
    writer.execute("CREATE (:Memory {id: 'retained'})")
    reader = hawdb.open(path)
    catalog = reader.execute_sql("SHOW BRANCHES LIMIT 1").fetchall()
    try:
        with pytest.raises(exceptions.BranchError):
            reader.execute("MATCH (m:Memory) RETURN m.id AS id")
        assert reader.execute_sql("SHOW BRANCHES LIMIT 1").fetchall() == catalog
        # CURRENT includes the live commit epoch, so it must report failed
        # admission rather than inventing an epoch from the catalog birth row.
        with pytest.raises(exceptions.BranchError):
            reader.execute_sql("SHOW CURRENT BRANCH")
        writer.close()
        assert reader.execute("MATCH (m:Memory) RETURN m.id AS id").fetchall() == [
            {"id": "retained"}
        ]
    finally:
        reader.close()
        writer.close()


def test_sql_nested_branch_schema_data_and_reopen(tmp_path):
    path = tmp_path / "branches"
    create = "CREATE BRANCH NAME $1 FROM ID $2 AT REVISION $3 REQUEST KEY $4"
    with hawdb.open(path) as db:
        db.execute_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, value TEXT)")
        db.execute_sql("INSERT INTO records (id, value) VALUES (1, 'main')")
        main = db.execute_sql("SHOW CURRENT BRANCH").fetchone()
        request = ["child", main["branch_id"], main["commit_epoch"], "python-child"]
        child = db.execute_sql(create, request).fetchall()
        assert db.execute_sql(create, request).fetchall() == child
        db.execute_sql("USE BRANCH child")
        db.execute_sql("ALTER TABLE records ADD COLUMN tag TEXT")
        db.execute_sql(
            "INSERT INTO records (id, value, tag) VALUES (2, 'child', 'private')"
        )
        current = db.execute_sql("SHOW CURRENT BRANCH").fetchone()
        db.execute_sql(
            create,
            ["grandchild", current["branch_id"], current["commit_epoch"], "python-nested"],
        )
        db.execute_sql("USE BRANCH main")
        assert db.execute_sql("SELECT id, value FROM records ORDER BY id").fetchall() == [
            {"id": 1, "value": "main"}
        ]
        with pytest.raises(exceptions.Error):
            db.execute_sql("SELECT tag FROM records")

    with hawdb.open(path) as reopened:
        reopened.execute_sql("USE BRANCH grandchild")
        assert reopened.execute_sql(
            "SELECT id, value, tag FROM records ORDER BY id"
        ).fetchall() == [
            {"id": 1, "value": "main", "tag": None},
            {"id": 2, "value": "child", "tag": "private"},
        ]
