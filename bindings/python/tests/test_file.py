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

"""Durable path validation, stable handle paths, and reopen behavior."""

import os
from pathlib import Path

import pytest

import hawdb
from hawdb import exceptions


OPENERS = [hawdb.open, hawdb.Database, hawdb.connect]
OPENER_IDS = ["open", "Database", "connect"]


@pytest.mark.parametrize("opener", OPENERS[:2], ids=OPENER_IDS[:2])
def test_file_open_reports_path_and_persists(tmp_path, opener):
    path = tmp_path / "graph"
    with opener(path) as db:
        assert db.path == str(path)
        db.execute("CREATE (:Persisted {id: 1})")

    with hawdb.open(path) as reopened:
        assert reopened.execute("MATCH (p:Persisted) RETURN p.id AS id").fetchall() == [
            {"id": 1}
        ]


@pytest.mark.parametrize("opener", OPENERS, ids=OPENER_IDS)
@pytest.mark.parametrize("read_only", [False, True])
def test_open_rejects_missing_parent(tmp_path, opener, read_only):
    with pytest.raises(exceptions.StorageError):
        with opener(tmp_path / "missing" / "deep" / "db", read_only=read_only):
            pass
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("opener", OPENERS[:2], ids=OPENER_IDS[:2])
def test_open_accepts_bare_relative_path(tmp_path, monkeypatch, opener):
    # Like sqlite3.connect("app.db"), a bare name opens or creates the
    # database in the current directory.
    monkeypatch.chdir(tmp_path)
    with opener("app.db") as db:
        assert db.path == str(tmp_path / "app.db")
        db.execute("CREATE (:Persisted {id: 1})")

    assert (tmp_path / "app.db").is_dir()
    with hawdb.open("app.db") as reopened:
        assert reopened.execute("MATCH (p:Persisted) RETURN p.id AS id").fetchall() == [
            {"id": 1}
        ]


@pytest.mark.parametrize("opener", OPENERS, ids=OPENER_IDS)
def test_open_rejects_missing_relative_parent(tmp_path, monkeypatch, opener):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(exceptions.StorageError) as caught:
        with opener("missing/app.db"):
            pass
    assert str(caught.value) == (
        f"parent directory does not exist: {tmp_path / 'missing'}"
    )
    assert not (tmp_path / "missing").exists()


@pytest.mark.parametrize("opener", OPENERS, ids=OPENER_IDS)
def test_open_rejects_read_only_without_existing_path(tmp_path, opener):
    with pytest.raises(exceptions.StorageError):
        with opener(tmp_path / "absent", read_only=True):
            pass
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("opener", OPENERS, ids=OPENER_IDS)
@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("as_parent", [False, True])
@pytest.mark.parametrize("relative", [False, True])
def test_open_rejects_regular_files_without_changing_them(
    tmp_path, monkeypatch, opener, read_only, as_parent, relative
):
    existing = tmp_path / "app.db"
    content = b"SQLite format 3\x00preserve this file"
    existing.write_bytes(content)
    path = existing / "child" if as_parent else existing
    if relative:
        monkeypatch.chdir(tmp_path)
    requested = path.relative_to(tmp_path) if relative else path
    with pytest.raises(exceptions.StorageError) as caught:
        with opener(requested, read_only=read_only):
            pass
    assert str(path) in str(caught.value)
    if not as_parent:
        assert "not a directory" in str(caught.value)
    assert existing.read_bytes() == content
    assert list(tmp_path.iterdir()) == [existing]


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory permissions")
@pytest.mark.parametrize("opener", OPENERS, ids=OPENER_IDS)
@pytest.mark.parametrize("read_only", [False, True])
def test_open_preserves_permission_errors(tmp_path, opener, read_only):
    locked = tmp_path / "locked"
    parent = locked / "sub"
    parent.mkdir(parents=True)
    path = parent / "app.db"
    mode = locked.stat().st_mode
    locked.chmod(0)
    try:
        try:
            parent.stat()
        except PermissionError as error:
            cause = os.strerror(error.errno)
        else:
            pytest.skip("the current user can bypass directory permissions")
        with pytest.raises(exceptions.StorageError) as caught:
            with opener(path, read_only=read_only):
                pass
        assert cause in str(caught.value)
        assert str(path) in str(caught.value)
        assert "parent directory does not exist" not in str(caught.value)
    finally:
        locked.chmod(mode)
    assert list(parent.iterdir()) == []


@pytest.mark.parametrize("opener", OPENERS[:2], ids=OPENER_IDS[:2])
def test_handle_path_reopens_the_same_database_after_chdir(tmp_path, monkeypatch, opener):
    original = tmp_path / "original"
    elsewhere = tmp_path / "elsewhere"
    original.mkdir()
    elsewhere.mkdir()
    monkeypatch.chdir(original)
    with opener("app.db") as db:
        path = db.path
        assert path == str(original / "app.db")
        assert original.name in repr(db)
        monkeypatch.chdir(elsewhere)
        db.execute("CREATE (:Persisted {id: 7})")
    with hawdb.open(path, read_only=True) as reopened:
        assert reopened.path == path
        assert reopened.execute("MATCH (p:Persisted) RETURN p.id AS id").fetchall() == [
            {"id": 7}
        ]
    assert list(elsewhere.iterdir()) == []


@pytest.mark.skipif(os.name != "posix", reason="requires directory symlinks")
@pytest.mark.parametrize("opener", OPENERS[:2], ids=OPENER_IDS[:2])
def test_handle_path_preserves_symlink_spelling(tmp_path, monkeypatch, opener):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(target, target_is_directory=True)
    monkeypatch.chdir(tmp_path)
    with opener(Path("alias") / "app.db") as db:
        assert db.path == str(alias / "app.db")
    assert (target / "app.db").is_dir()


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
