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


def statement_summary(db, query_text):
    return db.execute_sql(
        "SELECT statement_kind, execution_count, success_count, error_count "
        "FROM system.statement_summary WHERE query_language = 'sql' AND query_text = $1",
        [query_text],
    ).fetchall()


def test_transaction_sql_is_recorded_in_the_statement_summary(db):
    insert = "INSERT INTO items (id) VALUES ($1)"
    select = "SELECT id FROM items WHERE id > $1"
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
    with db.transaction() as tx:
        assert tx.execute_sql(insert, [1]).affected_rows == 1
        # A refused statement counts as an error and leaves the transaction open.
        with pytest.raises(hawdb.exceptions.ExecutionError):
            tx.execute_sql(insert, [1])
        assert tx.execute_sql(select, [0]).fetchall() == [{"id": 1}]
    # The admitted autocommit read is recorded under the same text.
    assert db.execute_sql(select, [0]).fetchall() == [{"id": 1}]
    assert db.execute_sql(insert, [2]).affected_rows == 1
    assert statement_summary(db, insert) == [{
        "statement_kind": "insert", "execution_count": 3,
        "success_count": 2, "error_count": 1,
    }]
    assert statement_summary(db, select) == [{
        "statement_kind": "select", "execution_count": 2,
        "success_count": 2, "error_count": 0,
    }]


# A value per refusal: a duplicate key, a NULL in a NOT NULL column, a wrong type.
RELATIONAL_REFUSALS = [([1, "again"], "constraint"), ([2, None], "constraint"),
                       ([3, 3], "schema")]


@pytest.mark.parametrize("parameters,kind", RELATIONAL_REFUSALS,
                         ids=["duplicate key", "not null", "wrong type"])
def test_relational_refusals_carry_their_kind_on_both_sql_paths(db, parameters, kind):
    insert = "INSERT INTO items (id, name) VALUES ($1, $2)"
    db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
    db.execute_sql(insert, [1, "one"])
    # Autocommit reports through storage, a transaction statement through execution.
    with pytest.raises(hawdb.exceptions.StorageError) as autocommitted:
        db.execute_sql(insert, parameters)
    with db.transaction() as tx:
        with pytest.raises(hawdb.exceptions.ExecutionError) as staged:
            tx.execute_sql(insert, parameters)
        tx.rollback()
    for caught, prefix in [(autocommitted, "storage error: "), (staged, "execution error: ")]:
        error = caught.value
        assert (error.kind, error.retryable) == (kind, False)
        assert str(error).startswith(f"{prefix}relational {kind} ")
        # Not the engine's IntegrityError, which reports damaged storage.
        assert not isinstance(error, hawdb.exceptions.IntegrityError)
    assert db.execute_sql("SELECT id FROM items").fetchall() == [{"id": 1}]


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
