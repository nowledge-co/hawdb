# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Atomic host-row batches through the existing transaction object."""

from collections.abc import Sequence
import uuid

import pytest

import hawdb


CYPHER = "CREATE (:Item {id: $id})"
SQL = "INSERT INTO items (id) VALUES ($1)"


def prepare(db, dialect):
    if dialect == "sql":
        db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
    else:
        # Keep schema setup outside the transaction whose publication is checked.
        db.execute("CREATE (:Item {id: 0})")


def rows_for(dialect, *ids):
    if dialect == "sql":
        return [(value,) for value in ids]
    return [{"id": value} for value in ids]


def batch(target, dialect, rows):
    if dialect == "sql":
        return target.executemany_sql(SQL, rows)
    return target.executemany(CYPHER, rows)


def ids(target, dialect):
    if dialect == "sql":
        return target.execute_sql("SELECT id FROM items ORDER BY id").fetchall()
    return target.execute("MATCH (i:Item) RETURN i.id AS id ORDER BY id").fetchall()


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_batch_publishes_all_rows_once(db, dialect):
    prepare(db, dialect)
    before = ids(db, dialect)
    observations = []

    class ObservedRows(Sequence):
        def __len__(self):
            return 3

        def __getitem__(self, index):
            if index >= 3:
                raise IndexError(index)
            if index == 1:
                with db.read_transaction() as read:
                    observations.append(ids(read, dialect))
            return rows_for(dialect, index + 1)[0]

    assert batch(db, dialect, ObservedRows()) is None
    assert observations == [before]
    assert ids(db, dialect) == before + [{"id": 1}, {"id": 2}, {"id": 3}]


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
@pytest.mark.parametrize("failure", ["value", "row_shape", "statement"])
def test_later_row_failure_rolls_back_the_whole_batch(db, dialect, failure):
    prepare(db, dialect)
    before = ids(db, dialect)
    rows = rows_for(dialect, 1, 2)
    error_type = TypeError
    if failure == "value":
        rows[1] = rows_for(dialect, object())[0]
    elif failure == "row_shape":
        rows[1] = "not a parameter binding"
    else:
        error_type = hawdb.exceptions.Error
        rows[1] = (1,) if dialect == "sql" else {}

    with pytest.raises(error_type):
        batch(db, dialect, rows)
    assert ids(db, dialect) == before
    assert batch(db, dialect, rows_for(dialect, 3)) is None
    assert ids(db, dialect) == before + [{"id": 3}]


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_transaction_batch_stays_staged_until_outer_commit(db, dialect):
    prepare(db, dialect)
    before = ids(db, dialect)
    with db.transaction() as tx:
        assert batch(tx, dialect, tuple(rows_for(dialect, 1, 2))) is None
        assert ids(tx, dialect) == before + [{"id": 1}, {"id": 2}]
        with db.read_transaction() as read:
            assert ids(read, dialect) == before
    assert ids(db, dialect) == before + [{"id": 1}, {"id": 2}]


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_caught_batch_failure_aborts_prior_outer_statements(db, dialect):
    prepare(db, dialect)
    before = ids(db, dialect)
    with db.transaction() as tx:
        if dialect == "sql":
            tx.execute_sql(SQL, [1])
        else:
            tx.execute(CYPHER, {"id": 1})
        with pytest.raises(TypeError):
            batch(tx, dialect, rows_for(dialect, 2, object()))
        with pytest.raises(RuntimeError, match="closed"):
            tx.commit()
    assert ids(db, dialect) == before


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_empty_batch_does_not_open_or_commit_a_transaction(db, dialect):
    prepare(db, dialect)
    before = ids(db, dialect)
    with db.transaction() as tx:
        for target in [db, tx]:
            method = target.executemany_sql if dialect == "sql" else target.executemany
            assert method("not a valid statement", []) is None
            assert method("not a valid statement", ()) is None
        batch(tx, dialect, rows_for(dialect, 1))
        tx.rollback()
    assert ids(db, dialect) == before


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_iteration_failure_rolls_back_already_executed_rows(db, dialect):
    prepare(db, dialect)
    before = ids(db, dialect)

    class BrokenRows(Sequence):
        def __len__(self):
            return 2

        def __getitem__(self, index):
            if index == 0:
                return rows_for(dialect, 1)[0]
            raise ValueError("host row source failed")

    with pytest.raises(ValueError, match="host row source failed"):
        batch(db, dialect, BrokenRows())
    assert ids(db, dialect) == before


def test_sql_batch_accepts_mixed_positional_sequences(db):
    prepare(db, "sql")
    assert db.executemany_sql(SQL, [[1], (2,), range(3, 4)]) is None
    assert ids(db, "sql") == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_batch_preserves_existing_parameter_values(db):
    identifier = uuid.UUID("00000000-0000-0000-0000-000000000017")
    values = [
        None, True, 7, 1.25, "text", b"binary", identifier,
        [1, None], {"key": [False]},
    ]
    db.executemany(
        "CREATE (:Item {id: $id, value: $value})",
        [{"id": index, "value": value} for index, value in enumerate(values)],
    )
    expected = values.copy()
    expected[6] = str(identifier)
    output = db.execute(
        "MATCH (i:Item) RETURN i.id AS id, i.value AS value ORDER BY id"
    )
    assert output.fetchall() == [
        {"id": index, "value": value} for index, value in enumerate(expected)
    ]


@pytest.mark.parametrize("rows", [None, "", b"", bytearray(), {}, {"id": 1}])
@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_batch_rejects_non_sequence_containers(db, dialect, rows):
    prepare(db, dialect)
    before = ids(db, dialect)
    with pytest.raises(TypeError):
        batch(db, dialect, rows)
    assert ids(db, dialect) == before


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_database_batch_does_not_join_an_existing_transaction(db, dialect):
    prepare(db, dialect)
    with db.transaction() as tx:
        with pytest.raises(hawdb.exceptions.Error, match="transaction"):
            batch(db, dialect, rows_for(dialect, 1))
        batch(tx, dialect, rows_for(dialect, 2))
    assert {"id": 1} not in ids(db, dialect)
    assert {"id": 2} in ids(db, dialect)


@pytest.mark.parametrize("dialect", ["cypher", "sql"])
def test_closed_handles_reject_batches(db, dialect):
    tx = db.transaction()
    tx.rollback()
    for rows in [[], rows_for(dialect, 1)]:
        with pytest.raises(RuntimeError, match="closed"):
            batch(tx, dialect, rows)
    db.close()
    for rows in [[], rows_for(dialect, 1)]:
        with pytest.raises(RuntimeError, match="closed"):
            batch(db, dialect, rows)


def test_persistent_batch_survives_reopen_without_a_failed_prefix(tmp_path):
    path = tmp_path / "batch-db"
    with hawdb.open(path) as db:
        db.executemany(CYPHER, [{"id": 1}, {"id": 2}])
        with pytest.raises(TypeError):
            db.executemany(CYPHER, [{"id": 3}, {"id": object()}])
    with hawdb.open(path) as db:
        assert ids(db, "cypher") == [{"id": 1}, {"id": 2}]
