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

"""PEP 249 connections and cursors over ``execute_sql``.

The layer has no backend-specific code, so most cases run in memory. Engine
transaction semantics also run on a file database, and durability, read-only
and branch cases use one directly.
"""

import gc
import inspect
import pickle
import threading
from collections import deque

import pytest

import hawdb
from hawdb import dbapi

CREATE = "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)"
INSERT = "INSERT INTO items (id, name) VALUES ($1, $2)"

PEP249_ERRORS = [
    dbapi.InterfaceError, dbapi.DatabaseError, dbapi.DataError,
    dbapi.OperationalError, dbapi.IntegrityError, dbapi.InternalError,
    dbapi.ProgrammingError, dbapi.NotSupportedError,
]

# The PEP 249 class each error from the binding gains.
PEP249_CLASS = {
    hawdb.exceptions.Error: dbapi.DatabaseError,
    hawdb.exceptions.ParseError: dbapi.ProgrammingError,
    hawdb.exceptions.SemanticError: dbapi.ProgrammingError,
    hawdb.exceptions.StorageError: dbapi.DatabaseError,
    hawdb.exceptions.IntegrityError: dbapi.InternalError,
    hawdb.exceptions.DescriptorError: dbapi.OperationalError,
    hawdb.exceptions.ExecutionError: dbapi.DatabaseError,
    hawdb.exceptions.ResourceBudgetError: dbapi.OperationalError,
    hawdb.exceptions.ConflictError: dbapi.OperationalError,
    hawdb.exceptions.CapabilityError: dbapi.NotSupportedError,
    hawdb.exceptions.BranchError: dbapi.OperationalError,
    hawdb.exceptions.AdmissionError: dbapi.OperationalError,
    hawdb.exceptions.TaskStoppedError: dbapi.OperationalError,
    hawdb.exceptions.RetainedError: dbapi.DatabaseError,
    hawdb.exceptions.BackpressureError: dbapi.OperationalError,
    RuntimeError: dbapi.InternalError,
}


def pep249_classes(error):
    """The PEP 249 classes ``error`` is an instance of, in a fixed order."""
    return [pep for pep in PEP249_ERRORS if isinstance(error, pep)]


@pytest.fixture(params=["file", "memory"])
def connect(request, tmp_path):
    """``hawdb.connect`` on each backend; every connection closes at teardown."""
    connections = []

    def factory(**kwargs):
        path = None if request.param == "memory" else tmp_path / "db"
        connection = hawdb.connect(path, **kwargs)
        connections.append(connection)
        return connection

    yield factory
    for connection in connections:
        connection.close()


@pytest.fixture
def connection():
    """An in-memory connection, for checks that do not depend on the backend."""
    connection = hawdb.connect()
    yield connection
    connection.close()


def ids(connection):
    cursor = connection.cursor()
    return [row[0] for row in cursor.execute("SELECT id FROM items ORDER BY id")]


def accepts(executemany, container):
    try:
        executemany("SELECT 1", container)
    except TypeError:
        return False
    return True


def test_module_interface():
    assert (dbapi.apilevel, dbapi.threadsafety, dbapi.paramstyle) == (
        "2.0", 1, "numeric_dollar",
    )
    assert hawdb.connect is dbapi.connect is dbapi.Connection
    assert dbapi.Error is hawdb.exceptions.Error
    # InterfaceError and DatabaseError are siblings; the rest are database errors.
    assert not issubclass(dbapi.InterfaceError, dbapi.DatabaseError)
    for pep in PEP249_ERRORS[2:]:
        assert issubclass(pep, dbapi.DatabaseError)
    assert issubclass(dbapi.Warning, Exception)
    assert not issubclass(dbapi.Warning, dbapi.Error)
    names = [
        "Warning", "Error", "InterfaceError", "DatabaseError", "DataError",
        "OperationalError", "IntegrityError", "InternalError",
        "ProgrammingError", "NotSupportedError",
    ]
    for name in names:
        assert getattr(dbapi.Connection, name) is getattr(dbapi, name)
        if name != "Warning":
            assert issubclass(getattr(dbapi, name), dbapi.Error)
    # The engine's IntegrityError reports damaged storage, not a constraint.
    assert dbapi.IntegrityError is not hawdb.exceptions.IntegrityError


def test_connect_takes_the_options_of_open():
    def shape(callable_, skip=()):
        return [
            (parameter.name, parameter.kind, parameter.default)
            for parameter in inspect.signature(callable_).parameters.values()
            if parameter.name not in skip
        ]

    assert shape(hawdb.connect, skip=("autocommit",)) == shape(hawdb.open)


def test_insert_with_parameters_and_fetch_tuples(connection):
    cursor = connection.cursor()
    assert cursor.connection is connection
    cursor.execute(CREATE)
    assert cursor.execute(INSERT, [1, "one"]) is cursor
    cursor.execute(INSERT, (2, None))
    cursor.execute("SELECT name, id FROM items")
    assert [column[0] for column in cursor.description] == ["name", "id"]
    assert cursor.rowcount == 2
    assert sorted(cursor.fetchall(), key=repr) == [("one", 1), (None, 2)]

    cursor.execute("SELECT id FROM items WHERE id >= $1 ORDER BY id", [1])
    assert cursor.arraysize == 1
    assert cursor.fetchmany() == [(1,)]
    assert cursor.fetchone() == (2,)
    assert cursor.fetchone() is None
    assert cursor.fetchmany(5) == []
    assert cursor.fetchall() == []

    cursor.execute("SELECT id FROM items ORDER BY id")
    for size, error in [(-1, ValueError), (1.0, TypeError)]:
        with pytest.raises(error):
            cursor.fetchmany(size)
    cursor.arraysize = 2
    assert cursor.fetchmany() == [(1,), (2,)]
    cursor.execute("SELECT id FROM items ORDER BY id")
    assert list(cursor) == [(1,), (2,)]
    cursor.setinputsizes([None, None])
    cursor.setoutputsize(10, 0)


def test_description_type_codes_come_from_python_values(connection):
    cursor = connection.cursor()
    cursor.execute(
        "CREATE TABLE kinds (id BIGINT PRIMARY KEY, label TEXT,"
        " score DOUBLE PRECISION, data BYTEA, flag BOOLEAN, missing TEXT)"
    )
    insert = (
        "INSERT INTO kinds (id, label, score, data, flag, missing)"
        " VALUES ($1, $2, $3, $4, $5, $6)"
    )
    cursor.executemany(insert, [
        (1, None, 1.5, b"x", True, None),
        (2, "two", None, None, False, None),
    ])
    cursor.execute("SELECT id, label, score, data, flag, missing FROM kinds")
    description = cursor.description
    assert [column[0] for column in description] == [
        "id", "label", "score", "data", "flag", "missing",
    ]
    assert [column[1] for column in description] == [int, str, float, bytes, bool, None]
    assert all(column[2:] == (None,) * 5 for column in description)

    id_code, label_code, score_code, data_code, flag_code, _ = (
        column[1] for column in description
    )
    assert id_code == dbapi.NUMBER and score_code == dbapi.NUMBER
    assert label_code == dbapi.STRING and data_code == dbapi.BINARY
    assert flag_code != dbapi.NUMBER and label_code != dbapi.NUMBER
    assert label_code != dbapi.DATETIME and id_code != dbapi.ROWID
    assert dbapi.Binary(bytearray(b"x")) == b"x"


@pytest.mark.parametrize("autocommit", [False, True])
def test_catalog_description_and_tuple_order(connection, autocommit):
    connection.autocommit = autocommit
    cursor = connection.cursor()
    cursor.execute(CREATE)
    for statement, columns, expected in [
        (
            "SELECT data_type, column_name FROM information_schema.columns"
            " WHERE table_name = $1 ORDER BY ordinal_position",
            ["data_type", "column_name"],
            [("bigint", "id"), ("text", "name")],
        ),
        (
            "SELECT tablename, schemaname FROM pg_catalog.pg_tables"
            " WHERE tablename = $1",
            ["tablename", "schemaname"],
            [("items", "public")],
        ),
    ]:
        for name, rows in [("items", expected), ("missing", [])]:
            cursor.execute(statement, (name,))
            assert [column[0] for column in cursor.description] == columns
            assert cursor.fetchall() == rows
    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'items'")
    assert [column[0] for column in cursor.description] == [
        "table_catalog", "table_schema", "table_name", "table_type",
        "self_referencing_column_name", "reference_generation",
        "user_defined_type_catalog", "user_defined_type_schema", "user_defined_type_name",
        "is_insertable_into", "is_typed", "commit_action",
    ]
    assert cursor.fetchone()[1:4] == ("public", "items", "BASE TABLE")


@pytest.mark.parametrize("analyze", [False, True])
def test_sql_explain_description_and_tuple_order(connection, analyze):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute(INSERT, (1, "one"))
    keyword = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
    cursor.execute(f"{keyword} SELECT name, id FROM items WHERE id = 1")
    columns = (
        ["id", "estRows", "actRows", "task", "access object",
         "execution info", "operator info", "memory", "disk"]
        if analyze else ["id", "estRows", "task", "access object", "operator info"]
    )
    assert [column[0] for column in cursor.description] == columns
    rows = cursor.fetchall()
    assert rows
    assert all(isinstance(row[0], str) for row in rows)
    assert all(row[columns.index("task")] == "root" for row in rows)


def test_type_objects_hash_by_identity():
    assert {dbapi.NUMBER: "number", dbapi.STRING: "string"}[dbapi.NUMBER] == "number"
    assert hash(dbapi.ROWID) == hash(dbapi.ROWID)
    # A set holds the type objects themselves, so compare a type code with ==.
    assert int == dbapi.NUMBER and int not in {dbapi.NUMBER}


def test_statements_without_result_columns(connection):
    cursor = connection.cursor()
    for statement, parameters in [(CREATE, None), (INSERT, [1, "one"])]:
        cursor.execute(statement, parameters)
        assert (cursor.description, cursor.rowcount) == (None, -1)
        assert cursor.fetchone() is None
        assert cursor.fetchall() == []


def test_empty_key_lookup_still_describes_its_columns(connection):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute("SELECT id, name FROM items WHERE id = $1", [1])
    assert cursor.fetchall() == []
    assert cursor.description is not None
    assert [column[0] for column in cursor.description] == ["id", "name"]


def test_rows_follow_the_select_list(connection):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute(INSERT, [1, "one"])
    cursor.execute("SELECT name, id FROM items WHERE id = $1", [1])
    assert cursor.fetchone() == ("one", 1)


def test_fetch_without_a_result_raises_programming_error(connection):
    cursor = connection.cursor()
    with pytest.raises(dbapi.ProgrammingError):
        cursor.fetchone()
    cursor.execute(CREATE)
    cursor.execute(INSERT, [1, "one"])
    # A failed execute discards the previous result.
    for statement, parameters, error in [
        ("SELECT id FROM missing", None, hawdb.exceptions.SemanticError),
        ("SELECT id FROM items WHERE id = $1", {"id": 1}, TypeError),
    ]:
        cursor.execute("SELECT id FROM items")
        with pytest.raises(error):
            cursor.execute(statement, parameters)
        with pytest.raises(dbapi.ProgrammingError):
            cursor.fetchall()


@pytest.mark.parametrize("parameters", ["12", b"12", bytearray(b"12"), {"1": 1}, 7])
def test_parameters_must_be_a_positional_sequence(connection, parameters):
    cursor = connection.cursor()
    with pytest.raises(TypeError, match="by position") as caught:
        cursor.execute(CREATE, parameters)
    assert isinstance(caught.value, dbapi.ProgrammingError)


@pytest.mark.parametrize(
    "container",
    [None, "", b"", bytearray(), {}, [], (), range(0), memoryview(b""), deque()],
    ids=repr,
)
def test_batch_containers_follow_the_binding_rule(connection, container):
    native = hawdb.open()
    try:
        expected = accepts(native.executemany_sql, container)
    finally:
        native.close()
    assert accepts(connection.cursor().executemany, container) == expected
    # A rejected container never reached the transaction, so it is not aborted.
    connection.commit()


@pytest.mark.parametrize("placeholder", ["?", "%s", ":id"])
def test_other_placeholders_are_not_rewritten(connection, placeholder):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    with pytest.raises(connection.Error):
        cursor.execute(f"SELECT id FROM items WHERE id = {placeholder}", [1])


@pytest.mark.parametrize(
    "statement,parameters,native,pep",
    [
        ("SELEC 1", None, hawdb.exceptions.ParseError, dbapi.ProgrammingError),
        ("SELECT id FROM missing", None, hawdb.exceptions.SemanticError,
         dbapi.ProgrammingError),
        ("SELECT $1, $2", [1], hawdb.exceptions.SemanticError, dbapi.ProgrammingError),
        ("SELECT $1", [object()], TypeError, dbapi.ProgrammingError),
        ("SELECT $1", [2**70], OverflowError, dbapi.DataError),
        ("SELECT $1", ["\ud800"], UnicodeEncodeError, dbapi.DataError),
    ],
    ids=["syntax", "unknown table", "missing parameter", "unsupported type",
         "integer overflow", "invalid text"],
)
def test_errors_keep_their_types_and_gain_pep249_classes(
    connection, statement, parameters, native, pep
):
    with pytest.raises(native) as caught:
        connection.cursor().execute(statement, parameters)
    assert pep249_classes(caught.value) == [connection.DatabaseError, pep]


def test_constraint_violations_are_plain_database_errors(connection):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute(INSERT, [1, "one"])
    connection.commit()
    connection.autocommit = True
    with pytest.raises(hawdb.exceptions.StorageError, match="duplicate") as autocommitted:
        cursor.execute(INSERT, [1, "again"])
    connection.autocommit = False
    with pytest.raises(hawdb.exceptions.ExecutionError, match="duplicate") as staged:
        cursor.execute(INSERT, [1, "again"])
    # Not IntegrityError, which the engine cannot tell apart yet, nor an
    # OperationalError that a retry loop would repeat.
    for caught in (autocommitted, staged):
        assert pep249_classes(caught.value) == [dbapi.DatabaseError]


def test_a_busy_path_is_an_operational_error(tmp_path):
    path = tmp_path / "db"
    with hawdb.connect(path) as holder:
        holder.cursor().execute(CREATE)
        with hawdb.connect(path, autocommit=True) as other:
            with pytest.raises(hawdb.exceptions.BranchError, match="busy") as caught:
                other.cursor().execute("SELECT id FROM items")
    assert pep249_classes(caught.value) == [dbapi.DatabaseError, dbapi.OperationalError]


def test_translated_errors_keep_attributes_and_pickle():
    with hawdb.connect() as connection:
        cursor = connection.cursor()
        cursor.execute(CREATE)
        cursor.executemany(INSERT, [(1, "a"), (2, "b")])
        with pytest.raises(hawdb.exceptions.ExecutionError) as caught:
            cursor.execute(INSERT, [1, "duplicate"])
    error = caught.value
    assert (error.kind, error.retryable) == ("execution", False)
    assert pep249_classes(error) == [dbapi.DatabaseError]
    # The binding's own error stays reachable for debugging.
    assert type(error.__context__) is hawdb.exceptions.ExecutionError
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is type(error)
    assert (restored.args, restored.kind) == (error.args, "execution")


def test_translated_work_budget_errors_keep_attributes_and_pickle():
    with hawdb.connect(max_read_result_rows=1) as connection:
        cursor = connection.cursor()
        cursor.execute(CREATE)
        cursor.executemany(INSERT, [(1, "a"), (2, "b")])
        with pytest.raises(hawdb.exceptions.ResourceBudgetError) as caught:
            cursor.execute("SELECT id FROM items")
    error = caught.value
    assert (error.resource, error.scope, error.owner) == (
        "work_units", "query", "relational SQL"
    )
    assert (error.budget_option, error.budget) == ("max_intermediate_rows", 1)
    assert pep249_classes(error) == [dbapi.DatabaseError, dbapi.OperationalError]
    assert type(error.__context__) is hawdb.exceptions.ResourceBudgetError
    restored = pickle.loads(pickle.dumps(error))
    assert type(restored) is type(error)
    assert restored.args == error.args
    assert vars(restored) == vars(error)


@pytest.mark.parametrize("native", list(PEP249_CLASS), ids=lambda native: native.__name__)
def test_every_binding_error_maps_to_its_pep249_class(native):
    error = native("message")
    error.detail = "kept"
    translated = dbapi._pep249(error, statement=False)
    assert isinstance(translated, native)
    expected = PEP249_CLASS[native]
    assert pep249_classes(translated) == [
        pep for pep in PEP249_ERRORS if issubclass(expected, pep)
    ]
    assert (translated.args, translated.detail) == (("message",), "kept")


def test_the_mapping_covers_every_engine_error():
    engine = {
        value for value in vars(hawdb.exceptions).values()
        if isinstance(value, type) and issubclass(value, BaseException)
    }
    assert engine == set(PEP249_CLASS) - {RuntimeError}


def test_a_retryable_execution_error_is_operational():
    error = hawdb.exceptions.ExecutionError("projection is busy")
    error.kind, error.retryable = "projection_busy", True
    translated = dbapi._pep249(error, statement=False)
    assert pep249_classes(translated) == [dbapi.DatabaseError, dbapi.OperationalError]
    assert translated.retryable


def test_built_in_errors_gain_classes_only_from_statements():
    for error, pep in [
        (TypeError("type"), dbapi.ProgrammingError),
        (ValueError("value"), dbapi.DataError),
        (OverflowError("range"), dbapi.DataError),
    ]:
        assert dbapi._pep249(error, statement=False) is error
        translated = dbapi._pep249(error, statement=True)
        assert isinstance(translated, type(error)) and isinstance(translated, pep)
    # Only exact types map, so a subclass such as RecursionError passes through.
    recursion = RecursionError("deep")
    assert dbapi._pep249(recursion, statement=True) is recursion
    with pytest.raises(ValueError, match="requires a database path") as caught:
        hawdb.connect(None, True)
    assert not isinstance(caught.value, dbapi.Error)


def test_errors_leave_no_reference_cycle():
    # A cycle through a traceback would leave the unsendable native objects in
    # it to the garbage collector, which may run on another thread.
    connection = hawdb.connect()
    cursor = connection.cursor()
    cursor.execute(CREATE)
    closed = hawdb.connect()
    closed.close()

    def fail():
        for call in [
            lambda: cursor.execute("SELEC 1"),
            lambda: cursor.executemany(INSERT, [(1, object())]),
            closed.commit,  # an error the connection raises unchanged
        ]:
            try:
                call()
            except dbapi.Error:
                pass

    gc.collect()
    gc.disable()
    try:
        fail()
        unreachable = gc.collect()
    finally:
        gc.enable()
        connection.close()
    assert unreachable == 0


def test_an_error_that_cannot_be_rebuilt_is_raised_unchanged(monkeypatch):
    # As if a binding error gained a constructor that refuses its own args.
    class Refusing(hawdb.exceptions.ParseError, dbapi.ProgrammingError):
        def __init__(self, message, position):
            super().__init__(message, position)

    monkeypatch.setitem(dbapi._ENGINE_ERRORS, hawdb.exceptions.ParseError, Refusing)
    connection = hawdb.connect()
    cursor = connection.cursor()

    def fail():
        try:
            cursor.execute("SELEC 1")
        except dbapi.Error as error:
            return error

    gc.collect()
    gc.disable()
    try:
        error = fail()
        unreachable = gc.collect()
    finally:
        gc.enable()
        connection.close()
    assert type(error) is hawdb.exceptions.ParseError
    # The failed construction is neither chained to it nor left in a cycle.
    assert error.__context__ is None
    assert unreachable == 0


def test_commit_publishes_and_rollback_discards(connect):
    connection = connect()
    assert not connection.autocommit
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute(INSERT, [1, "kept"])
    connection.commit()
    cursor.execute(INSERT, [2, "dropped"])
    cursor.execute("CREATE TABLE scratch (id BIGINT PRIMARY KEY)")
    assert ids(connection) == [1, 2]
    connection.rollback()
    assert ids(connection) == [1]
    with pytest.raises(hawdb.exceptions.SemanticError):
        cursor.execute("SELECT id FROM scratch")
    connection.rollback()
    connection.commit()  # nothing is open, so nothing happens
    assert ids(connection) == [1]


def test_failed_execute_keeps_earlier_statements_staged(connect):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute(INSERT, [1, "staged"])
    with pytest.raises(hawdb.exceptions.SemanticError):
        cursor.execute("SELECT id FROM missing")
    with pytest.raises(hawdb.exceptions.ExecutionError, match="duplicate primary key"):
        cursor.execute("INSERT INTO items (id, name) VALUES (2, 'b'), (1, 'dup')")
    connection.commit()
    assert ids(connection) == [1]


def test_failed_executemany_aborts_until_rollback(connect):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(CREATE)
    connection.commit()
    refused = [
        lambda: cursor.execute(INSERT, [4, "d"]),
        lambda: cursor.executemany(INSERT, [(4, "d")]),
        connection.commit,
    ]
    # Engine, value and row-shape failures inside the batch.
    for rows, error, pep in [
        ([(2, "b"), (2, "duplicate")], hawdb.exceptions.ExecutionError,
         dbapi.DatabaseError),
        ([(2, "b"), (3, object())], TypeError, dbapi.ProgrammingError),
        ([(2, "b"), "not a row"], TypeError, dbapi.ProgrammingError),
    ]:
        cursor.execute(INSERT, [1, "before the batch"])
        with pytest.raises(error) as caught:
            cursor.executemany(INSERT, rows)
        assert isinstance(caught.value, pep)
        for call in refused:
            with pytest.raises(dbapi.OperationalError, match="rollback"):
                call()
        connection.rollback()
        # Nothing staged before the failed batch survives.
        assert ids(connection) == []
    cursor.execute(INSERT, [5, "after"])
    connection.commit()
    assert ids(connection) == [5]


def test_close_rolls_back_and_commit_survives_reopen(tmp_path):
    path = tmp_path / "db"
    connection = hawdb.connect(path)
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute(INSERT, [1, "committed"])
    connection.commit()
    cursor.execute(INSERT, [2, "uncommitted"])
    connection.close()
    connection.close()
    with hawdb.connect(path) as connection:
        assert ids(connection) == [1]


def test_context_manager_commits_or_rolls_back_then_closes(tmp_path):
    path = tmp_path / "db"
    with hawdb.connect(path) as connection:
        connection.cursor().execute(CREATE)
        connection.cursor().execute(INSERT, [1, "kept"])
    with pytest.raises(dbapi.InterfaceError):
        connection.cursor()
    with pytest.raises(ValueError, match="boom"):
        with hawdb.connect(path) as connection:
            connection.cursor().execute(INSERT, [2, "lost"])
            raise ValueError("boom")
    # An error leaving an aborted block propagates unchanged.
    with pytest.raises(TypeError):
        with hawdb.connect(path) as connection:
            connection.cursor().executemany(INSERT, [(3, object())])
    # A clean exit refuses to commit the work a failed batch discarded.
    with pytest.raises(dbapi.OperationalError):
        with hawdb.connect(path) as connection:
            cursor = connection.cursor()
            cursor.execute(INSERT, [4, "lost"])
            try:
                cursor.executemany(INSERT, [(5, object())])
            except TypeError:
                pass
    with pytest.raises(dbapi.InterfaceError):
        connection.cursor()
    with hawdb.connect(path) as connection:
        assert ids(connection) == [1]


def test_autocommit_runs_each_statement_on_its_own():
    with hawdb.connect(autocommit=True) as connection:
        assert connection.autocommit
        cursor = connection.cursor()
        cursor.execute(CREATE)
        cursor.execute(INSERT, [1, "one"])
        connection.rollback()
        assert ids(connection) == [1]
        with pytest.raises(hawdb.exceptions.ExecutionError):
            cursor.executemany(INSERT, [(2, "b"), (3, "c"), (1, "duplicate")])
        # One engine transaction, not a loop of autocommits: no row survived.
        assert ids(connection) == [1]
        assert cursor.executemany(INSERT, [(2, "b"), [3, "c"]]) is cursor
        assert (cursor.description, cursor.rowcount, cursor.fetchall()) == (None, -1, [])
        connection.commit()
        assert ids(connection) == [1, 2, 3]


def test_autocommit_changes_between_transactions(connection):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    with pytest.raises(dbapi.ProgrammingError, match="between transactions"):
        connection.autocommit = True
    connection.commit()
    connection.autocommit = True
    cursor.execute(INSERT, [1, "one"])
    connection.rollback()  # nothing is open in autocommit
    connection.autocommit = False
    cursor.execute(INSERT, [2, "two"])
    connection.rollback()
    assert ids(connection) == [1]
    with pytest.raises(TypeError):
        cursor.executemany(INSERT, [(3, object())])
    # An aborted connection is still in its transaction until rollback().
    with pytest.raises(dbapi.ProgrammingError, match="between transactions"):
        connection.autocommit = True
    connection.rollback()
    connection.autocommit = True
    assert connection.autocommit


def test_executemany_stages_in_the_connection_transaction(connection):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    connection.commit()
    cursor.executemany(INSERT, [(1, "a"), [2, "b"]])
    cursor.executemany(INSERT, [])
    assert ids(connection) == [1, 2]
    connection.rollback()
    assert ids(connection) == []
    cursor.executemany(INSERT, ((3, "c"),))
    connection.commit()
    assert ids(connection) == [3]


@pytest.mark.parametrize(
    "operation,rows",
    [
        (INSERT, "12"),
        (INSERT, bytearray(b"12")),
        (INSERT, {"id": 1}),
        (INSERT, None),
        (INSERT.encode(), [(2, "b")]),
        (None, [(2, "b")]),
    ],
)
def test_wrong_batch_arguments_leave_the_transaction_open(connection, operation, rows):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.execute(INSERT, [1, "staged"])
    with pytest.raises(TypeError) as caught:
        cursor.executemany(operation, rows)
    assert isinstance(caught.value, dbapi.ProgrammingError)
    connection.commit()
    assert ids(connection) == [1]


def test_aborted_connection_still_closes(connection):
    cursor = connection.cursor()
    cursor.execute(CREATE)
    with pytest.raises(TypeError):
        cursor.executemany(INSERT, [(1, object())])
    connection.close()
    with pytest.raises(dbapi.InterfaceError):
        connection.rollback()


def test_read_only_connection_runs_in_autocommit(tmp_path):
    path = tmp_path / "db"
    with hawdb.connect(path) as connection:
        connection.cursor().execute(CREATE)
        connection.cursor().execute(INSERT, [1, "one"])
    with hawdb.connect(path, True) as connection:
        assert connection.autocommit
        connection.autocommit = True
        with pytest.raises(dbapi.NotSupportedError, match="read-only"):
            connection.autocommit = False
        assert ids(connection) == [1]
        connection.commit()
        with pytest.raises(hawdb.exceptions.ExecutionError, match="read-only"):
            connection.cursor().execute(INSERT, [2, "two"])
        connection.rollback()


def test_branch_commands_need_an_autocommit_connection(tmp_path):
    path = tmp_path / "db"
    with hawdb.connect(path) as connection:
        with pytest.raises(hawdb.exceptions.BranchError, match="explicit transaction"):
            connection.cursor().execute("USE BRANCH main")
    with hawdb.connect(path, autocommit=True) as connection:
        connection.cursor().execute("USE BRANCH main")


def test_closed_connection_and_cursor_raise_interface_error(connection):
    with connection.cursor() as cursor:
        cursor.execute(CREATE)
    cursor.close()
    for call in [lambda: cursor.execute(CREATE), cursor.fetchone]:
        with pytest.raises(dbapi.InterfaceError, match="cursor is closed"):
            call()
    other = connection.cursor()
    connection.close()
    connection.close()
    calls = [
        connection.cursor, connection.commit, connection.rollback,
        lambda: setattr(connection, "autocommit", True),
        lambda: other.execute(CREATE), lambda: other.executemany(INSERT, []),
        other.fetchall,
    ]
    for call in calls:
        with pytest.raises(dbapi.InterfaceError, match="connection is closed"):
            call()


def test_connection_stays_on_its_thread(connection):
    cursor = connection.cursor()
    calls = [
        connection.cursor, connection.commit, connection.rollback,
        connection.close, lambda: cursor.execute(CREATE),
        lambda: setattr(connection, "autocommit", True),
    ]
    errors = []

    def use_elsewhere():
        for call in calls:
            try:
                call()
            except dbapi.ProgrammingError as error:
                errors.append(error)

    thread = threading.Thread(target=use_elsewhere)
    thread.start()
    thread.join()
    assert len(errors) == len(calls)
    cursor.execute(CREATE)


def test_a_thread_that_reuses_the_owner_ident_is_refused():
    opened = []

    def open_and_close():
        connection = hawdb.connect()
        connection.close()
        opened.append(connection)

    owner = threading.Thread(target=open_and_close)
    owner.start()
    owner.join()
    connection = opened.pop()
    errors = []

    def use():
        try:
            connection.cursor()
        except dbapi.Error as error:
            errors.append(error)

    # Idents are reused once their thread ends; try until one is.
    for _ in range(20):
        user = threading.Thread(target=use)
        user.start()
        user.join()
        if user.ident == owner.ident:
            break
    else:
        pytest.skip("no new thread reused the owner's ident")
    # A guard keyed on the ident would let it through to "connection is closed".
    assert all(isinstance(error, dbapi.ProgrammingError) for error in errors)


@pytest.mark.filterwarnings("ignore:pandas only supports SQLAlchemy:UserWarning")
def test_pandas_read_sql(connection):
    pandas = pytest.importorskip("pandas")
    cursor = connection.cursor()
    cursor.execute(CREATE)
    cursor.executemany(INSERT, [(1, "a"), (2, "b")])
    frame = pandas.read_sql(
        "SELECT id, name FROM items WHERE id >= $1", connection, params=[2]
    )
    assert frame.to_dict("records") == [{"id": 2, "name": "b"}]
