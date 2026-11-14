# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Engine read ceilings and plan-only execution through the Python facade."""

import pytest

import hawdb
from hawdb import exceptions


@pytest.fixture(params=[hawdb.open, hawdb.Database], ids=["open", "constructor"])
def opener(request):
    return request.param


@pytest.fixture(params=["memory", "file"])
def database_path(request, tmp_path):
    return None if request.param == "memory" else tmp_path / "graph"


def seed(db, count=3):
    for value in range(1, count + 1):
        db.execute("CREATE (:Item {id: $id})", {"id": value})


PAYLOAD_COLUMN = "c" * 2048
PAYLOAD_LIMIT = 4 * (len(PAYLOAD_COLUMN) + 8) - 1
PAYLOAD_QUERY = (
    f"MATCH (n:Item) RETURN n.id AS {PAYLOAD_COLUMN} ORDER BY {PAYLOAD_COLUMN}"
)


def test_row_ceiling_refuses_without_poisoning_handle(opener, database_path):
    with opener(database_path, max_read_result_rows=1) as db:
        seed(db)
        with pytest.raises(exceptions.ExecutionError, match="max_read_result_rows"):
            db.execute("MATCH (n:Item) RETURN n.id AS id ORDER BY id")
        assert db.execute(
            "MATCH (n:Item) RETURN n.id AS id ORDER BY id LIMIT 1"
        ).fetchall() == [{"id": 1}]


def test_payload_ceiling_refuses_then_accepts_small_result(opener, database_path):
    # Repeated names reach the logical payload cap before physical ownership does.
    with opener(database_path, max_read_result_payload_bytes=PAYLOAD_LIMIT) as db:
        seed(db, count=4)
        with pytest.raises(
            exceptions.ExecutionError, match="max_read_result_payload_bytes"
        ):
            db.execute(PAYLOAD_QUERY)
        assert db.execute(
            "MATCH (n:Item) WHERE n.id = $id RETURN n.id AS id", {"id": 1}
        ).fetchall() == [{"id": 1}]


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"max_read_result_rows": None, "max_read_result_payload_bytes": None},
    ],
    ids=["omitted", "none"],
)
def test_omitted_and_none_budgets_preserve_defaults(opener, database_path, options):
    with opener(database_path, **options) as db:
        seed(db)
        assert db.execute(
            "MATCH (n:Item) RETURN n.id AS id ORDER BY id"
        ).fetchall() == [{"id": 1}, {"id": 2}, {"id": 3}]
        db.execute("CREATE (:Payload {text: $text})", {"text": "x" * 1024})
        result = db.execute("MATCH (n:Payload) RETURN n.text AS text")
        assert result.fetchall() == [{"text": "x" * 1024}]


def test_read_only_open_applies_each_ceiling(opener, tmp_path):
    path = tmp_path / "read-only"
    with hawdb.open(path) as writer:
        seed(writer, count=4)
    with opener(path, read_only=True, max_read_result_rows=1) as db:
        with pytest.raises(exceptions.ExecutionError, match="max_read_result_rows"):
            db.execute("MATCH (n:Item) RETURN n.id AS id")
        result = db.execute("MATCH (n:Item) RETURN n.id AS id ORDER BY id LIMIT 1")
        assert result.fetchone() == {"id": 1}
    with opener(
        path, read_only=True, max_read_result_payload_bytes=PAYLOAD_LIMIT
    ) as db:
        with pytest.raises(
            exceptions.ExecutionError, match="max_read_result_payload_bytes"
        ):
            db.execute(PAYLOAD_QUERY)
        result = db.execute("MATCH (n:Item) RETURN n.id AS id ORDER BY id LIMIT 1")
        assert result.fetchone() == {"id": 1}


def test_open_time_budget_is_not_persisted(tmp_path):
    path = tmp_path / "reopen"
    with hawdb.open(path, max_read_result_rows=1) as db:
        seed(db)
    with hawdb.open(path) as db:
        assert len(db.execute("MATCH (n:Item) RETURN n.id")) == 3


def test_zero_ceilings_follow_engine_semantics():
    with hawdb.open(max_read_result_rows=0) as db:
        seed(db)
        assert len(db.execute("MATCH (n:Item) RETURN n.id")) == 3
    with hawdb.open(max_read_result_payload_bytes=0) as db:
        seed(db, count=1)
        with pytest.raises(exceptions.ExecutionError):
            db.execute("MATCH (n:Item) RETURN n.id")


@pytest.mark.parametrize(
    "option", ["max_read_result_rows", "max_read_result_payload_bytes"]
)
@pytest.mark.parametrize(
    "value,error", [(-1, OverflowError), (2**100, OverflowError), ("1", TypeError)]
)
def test_invalid_budget_is_rejected_before_open(opener, tmp_path, option, value, error):
    path = tmp_path / "invalid"
    with pytest.raises(error):
        opener(path, **{option: value})
    assert not path.exists()


def test_sql_reads_share_the_open_time_ceiling(database_path):
    with hawdb.open(database_path, max_read_result_rows=1) as db:
        db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        db.execute_sql("INSERT INTO items (id) VALUES (1), (2)")
        with pytest.raises(
            exceptions.ResourceBudgetError, match="max_intermediate_rows 1"
        ) as refusal:
            db.execute_sql("SELECT id FROM items")
        assert refusal.value.resource == "work_units"
        assert refusal.value.scope == "query"
        assert refusal.value.owner == "relational SQL"
        assert refusal.value.budget_option == "max_intermediate_rows"
        assert refusal.value.budget == 1
        result = db.execute_sql("SELECT id FROM items WHERE id = 1")
        assert result.fetchall() == [{"id": 1}]


def test_sql_explain_still_uses_execute_sql(database_path):
    with hawdb.open(database_path) as db:
        db.execute_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        assert db.execute_sql("EXPLAIN SELECT id FROM items").fetchall()


def test_explain_binds_parameters_without_executing(database_path):
    with hawdb.open(database_path, max_read_result_rows=1) as db:
        seed(db)
        plan = db.explain("MATCH (n:Item) WHERE n.id = $id RETURN n.id", {"id": 2})
        assert isinstance(plan, str) and plan.strip()
        assert db.explain("CREATE (:PlannedOnly {id: $id})", {"id": 9}).strip()
        assert db.execute("MATCH (n:PlannedOnly) RETURN n.id").fetchall() == []
        assert db.execute("MATCH (n:Item) RETURN count(n) AS count").fetchall() == [
            {"count": 3}
        ]


def test_explain_preserves_error_and_handle_contracts():
    db = hawdb.open()
    with pytest.raises(TypeError):
        db.explain("MATCH (n) RETURN $value", {"value": object()})
    with pytest.raises(exceptions.ParseError):
        db.explain("MATCH (")
    with db.transaction():
        with pytest.raises(RuntimeError, match="transaction is open"):
            db.explain("MATCH (n) RETURN n")
    db.close()
    with pytest.raises(RuntimeError, match="database is closed"):
        db.explain("MATCH (n) RETURN n")
