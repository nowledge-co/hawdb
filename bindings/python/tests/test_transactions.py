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

"""Explicit multi-statement transactions on both backends."""

import pytest

import hawdb


def test_transaction_commit_publishes_all_statements(db):
    with db.transaction() as tx:
        tx.execute("CREATE (:Item {id: 1})")
        tx.execute("CREATE (:Item {id: 2})")

    result = db.execute("MATCH (i:Item) RETURN i.id AS id ORDER BY id")
    assert result.fetchall() == [{"id": 1}, {"id": 2}]


def test_transaction_sees_own_writes(db):
    with db.transaction() as tx:
        tx.execute("CREATE (:Item {id: 1})")
        inside = tx.execute("MATCH (i:Item) RETURN i.id AS id")
        assert inside.fetchall() == [{"id": 1}]


def test_transaction_exception_rolls_back_both_statements(db):
    with pytest.raises(hawdb.exceptions.Error):
        with db.transaction() as tx:
            tx.execute("CREATE (:Item {id: 1})")
            tx.execute("CREATE (:Item {id: 2})")
            tx.execute("THIS IS NOT CYPHER")

    assert db.execute("MATCH (i:Item) RETURN i.id AS id").fetchall() == []


def test_transaction_rollback_discards(db):
    db.execute("CREATE (:Item {id: 1})")
    with db.transaction() as tx:
        tx.execute("CREATE (:Item {id: 2})")
        tx.rollback()

    assert db.execute("MATCH (i:Item) RETURN i.id AS id").fetchall() == [
        {"id": 1}
    ]


def test_transaction_sql_statements(db):
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
    with db.transaction() as tx:
        tx.execute_sql("INSERT INTO items (id) VALUES ($1)", [1])
        tx.execute_sql("INSERT INTO items (id) VALUES ($1)", [2])

    result = db.execute_sql("SELECT id FROM items ORDER BY id")
    assert result.fetchall() == [{"id": 1}, {"id": 2}]


def test_nested_transaction_fails(db):
    with db.transaction():
        with pytest.raises(hawdb.exceptions.Error):
            db.transaction()


def test_autocommit_blocked_while_transaction_open(db):
    with db.transaction():
        with pytest.raises(Exception, match="transaction"):
            db.execute("MATCH (i:Item) RETURN i.id AS id")
        with pytest.raises(Exception, match="transaction"):
            db.execute_sql("SELECT 1")


def test_transaction_closed_after_block(db):
    with db.transaction() as tx:
        tx.execute("CREATE (:Item {id: 1})")

    with pytest.raises(Exception, match="closed"):
        tx.execute("CREATE (:Item {id: 2})")
    with pytest.raises(Exception, match="closed"):
        tx.commit()
    with pytest.raises(Exception, match="closed"):
        tx.execute_sql("SELECT 1")


def test_dropped_transaction_rolls_back(db):
    tx = db.transaction()
    tx.execute("CREATE (:Item {id: 1})")
    del tx

    assert db.execute("MATCH (i:Item) RETURN i.id AS id").fetchall() == []


def test_autocommit_unchanged_outside_block(db):
    db.execute("CREATE (:Item {id: 1})")
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
    db.execute_sql("INSERT INTO items (id) VALUES ($1)", [7])

    assert db.execute("MATCH (i:Item) RETURN i.id AS id").fetchall() == [
        {"id": 1}
    ]
    assert db.execute_sql("SELECT id FROM items").fetchall() == [{"id": 7}]


def test_read_transaction_rejects_write(db):
    db.execute("CREATE (:Item {id: 1})")
    with db.read_transaction() as tx:
        assert tx.execute("MATCH (i:Item) RETURN i.id AS id").fetchall() == [
            {"id": 1}
        ]
        with pytest.raises(hawdb.exceptions.Error):
            tx.execute("CREATE (:Item {id: 2})")
        with pytest.raises(hawdb.exceptions.Error):
            tx.execute_sql(
                "CREATE TABLE items (id BIGINT PRIMARY KEY)"
            )


def test_read_transaction_stable_snapshot(db):
    db.execute("CREATE (:Item {id: 1})")
    with db.read_transaction() as tx:
        db.execute("CREATE (:Item {id: 2})")
        snapshot = tx.execute("MATCH (i:Item) RETURN i.id AS id")
        assert snapshot.fetchall() == [{"id": 1}]

    assert db.execute("MATCH (i:Item) RETURN i.id AS id").fetchall() == [
        {"id": 1},
        {"id": 2},
    ]


def test_read_transaction_closed_after_block(db):
    with db.read_transaction() as tx:
        pass
    with pytest.raises(Exception, match="closed"):
        tx.execute("MATCH (i:Item) RETURN i.id AS id")
