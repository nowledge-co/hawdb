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

"""Behaviors tied to a durable project path: reopen persistence, read-only
open, and the parent-directory guard."""

import pytest

import hawdb
from hawdb import exceptions


def test_file_open_reports_path_and_persists(tmp_path):
    path = tmp_path / "graph"
    with hawdb.open(path) as db:
        assert db.path == str(path)
        db.execute("CREATE (:Persisted {id: 1})")

    with hawdb.open(path) as reopened:
        assert reopened.execute("MATCH (p:Persisted) RETURN p.id AS id").fetchall() == [
            {"id": 1}
        ]


def test_open_rejects_missing_parent(tmp_path):
    with pytest.raises(exceptions.Error):
        hawdb.open(tmp_path / "missing" / "deep" / "db")


def test_open_rejects_read_only_without_existing_path(tmp_path):
    with pytest.raises(Exception):
        hawdb.open(tmp_path / "absent", read_only=True)


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
