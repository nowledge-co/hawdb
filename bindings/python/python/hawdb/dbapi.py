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

"""PEP 249 (DB-API 2.0) connections over HawDB's SQL surface.

``hawdb.connect(path=None, read_only=False, *, autocommit=False, ...)`` opens
a database like ``hawdb.open``, with the same options, and returns a
``Connection`` that owns it. ``connect`` is another name for ``Connection``.
Cursor statements run through ``execute_sql``; Cypher, named parameters and
graph values stay on ``Database.execute``. Like any handle, the connection
keeps other handles off its path until it closes, so close it before opening
the same path with ``hawdb.open``.

Parameters are a sequence bound to ``$1``, ``$2``, ... in order
(``paramstyle = "numeric_dollar"``). Statements are sent unchanged: ``?``,
``%s`` and ``:name`` are not rewritten and fail like any other invalid SQL.
Parameter values follow the binding's rules.

Autocommit is off, as PEP 249 requires. The first ``execute`` or
``executemany`` after ``connect``, ``commit()`` or ``rollback()`` begins a
transaction, even for a query; ``commit()`` publishes it, ``rollback()``
discards it, and ``close()`` rolls back whatever was not committed. With
autocommit on, through ``connect(..., autocommit=True)`` or by setting
``autocommit`` between transactions, each ``execute`` is its own transaction
and each ``executemany`` one atomic batch, and ``commit()`` and
``rollback()`` do nothing. Setting ``autocommit`` while a transaction is open,
or after a failed ``executemany``, raises ``ProgrammingError``. Statements the
engine refuses inside a transaction, such as ``CREATE BRANCH`` and
``USE BRANCH``, need autocommit. A ``read_only=True`` connection always
autocommits, because a read-only database cannot commit even an empty
transaction, and turning its autocommit off raises ``NotSupportedError``.

A transaction that has only run queries plans them as autocommit does, under
the same result budgets, but reads through its own workspace. Until it ends,
it pins the snapshot it began with, which holds back checkpoint reclamation,
and its statements stay out of the slow-query log and statement summary.
Neither mode applies query admission to SQL. End read-only transactions
promptly, or run long-lived read-only work with autocommit on.

A failed ``execute`` leaves the transaction open with the earlier statements
still staged. A failed ``executemany`` does not: there are no savepoints, so
the batch takes the whole transaction with it, including the statements
before it. The connection is then aborted: ``execute``, ``executemany`` and
``commit()`` raise ``OperationalError`` until ``rollback()`` acknowledges the
lost work. ``close()`` still succeeds.

Rows are tuples in ``description`` order, which is the order of the SELECT
list. ``description`` holds one seven-item tuple per column, also when no
row matches: the name, a type code and five ``None``. The type code is the
Python type of the column's first non-null value in the whole result, or
``None`` when every value is null; it is known at ``execute``, before any
row is fetched. The engine's result schema carries column names but no
types, so a column whose values are all null has no type code. ``STRING``
equals ``str``, ``BINARY`` equals ``bytes`` and ``NUMBER`` equals ``int``
and ``float`` and ``DATETIME`` equals ``datetime.datetime``
(and ``date``/``time``); booleans and vectors match none of them. DDL
and every INSERT, UPDATE and DELETE without ``RETURNING`` leave
``description`` as ``None``, and fetching from them returns no rows.

``execute`` holds the engine's whole result, as ``Database.execute_sql``
does, but no Python copy of it: rows become tuples as they are fetched,
1,024 at a time, and the cursor keeps none of the tuples it returns.
``fetchall()`` of a large result therefore needs Python memory for the
returned list and one such chunk. The engine's rows are released once
every row has been fetched or the cursor runs another statement or closes.

``rowcount`` is the number of rows a SELECT returned, or the number an
INSERT, UPDATE or DELETE inserted, updated or deleted, in autocommit and in a
transaction alike. Inside a transaction the count is of the rows the
statement staged, which ``commit()`` publishes. Rows that ``ON CONFLICT DO
NOTHING`` skipped are not counted. ``rowcount`` is -1 for DDL and other
statements without a result or a count, for writes to strict append tables,
which report no row count, and after ``executemany``.

``TIMESTAMP`` values arrive as naive ``datetime.datetime`` objects and
``TIMESTAMPTZ`` values as aware ones in UTC. Both are microsecond-exact, so the
conversion is lossless. Only cursors convert: ``Database.execute_sql`` and
Cypher keep the binding's tagged dicts (``{"$localdatetime": ...}``,
``{"$datetime": ...}``). A value Python cannot represent, such as a year
outside 1..9999, stays a tagged dict.

Catalog queries (``information_schema``, ``pg_catalog`` and ``system``)
also preserve SELECT-list order and empty-result descriptions; wildcards
expand in the catalog's declared column order. SQL ``EXPLAIN`` reports
``id, estRows, task, access object, operator info``. ``EXPLAIN ANALYZE``
reports ``id, estRows, actRows, task, access object, execution info,
operator info, memory, disk``.

Errors follow the PEP 249 hierarchy. ``Error`` is ``hawdb.exceptions.Error``;
``InterfaceError`` and ``DatabaseError`` are its subclasses, and the other
classes are subclasses of ``DatabaseError``. An engine error is raised as a
subclass of both its ``hawdb.exceptions`` type and a PEP 249 class, with its
attributes, so ``except hawdb.exceptions.ParseError`` and
``except ProgrammingError`` both catch a syntax error:

- ``ProgrammingError``: ``ParseError`` and ``SemanticError``.
- ``OperationalError``: ``ConflictError``, ``BranchError``, ``AdmissionError``,
  ``TaskStoppedError``, ``DescriptorError``, ``ResourceBudgetError``,
  ``BackpressureError``, and an ``ExecutionError`` that is ``retryable``.
- ``NotSupportedError``: ``CapabilityError``.
- ``IntegrityError``: a ``StorageError`` or ``ExecutionError`` whose ``kind``
  is ``"constraint"``, such as a duplicate primary key or a NULL in a NOT NULL
  column.
- ``DataError``: a ``StorageError`` or ``ExecutionError`` whose ``kind`` is
  ``"schema"``, such as a value of the wrong type for its column.
- ``InternalError``: ``hawdb.exceptions.IntegrityError``, which reports
  damaged storage rather than a violated constraint.
- ``DatabaseError``: every other engine error, including the remaining
  ``StorageError`` and ``ExecutionError``.

A constraint or schema refusal keeps the engine's type for the path it took:
a ``StorageError`` in autocommit and an ``ExecutionError`` in a transaction.
Either way it has ``retryable`` set to ``False``, so neither class is an
``OperationalError`` that a retry loop would repeat. ``IntegrityError`` is not
``hawdb.exceptions.IntegrityError``, so ``except IntegrityError`` around an
insert cannot swallow damaged storage. A parameter value the binding cannot
convert is a ``DataError`` too, such as an integer outside 64 bits or text
that is not valid UTF-8, or a ``ProgrammingError`` for a type it does not
support. That error is also the binding's ``OverflowError``,
``UnicodeEncodeError``, ``ValueError`` or ``TypeError``, and a parameter
container this module refuses is likewise a ``ProgrammingError`` and a
``TypeError``. A ``RuntimeError`` from the binding, which would mean that the
connection is out of step with its database, is an ``InternalError``. Using a
closed connection or cursor raises ``InterfaceError``; fetching before a
successful ``execute`` raises ``ProgrammingError``.

``threadsafety`` is 1: threads may share the module but not a connection. A
connection must stay on the thread that opened it, and other threads get
``ProgrammingError``. Close it before that thread ends, because no other
thread can release the database, which then keeps its path busy until the
process exits. There is no pool.

Deliberate departures from PEP 249: ``paramstyle`` is ``"numeric_dollar"``,
SQLAlchemy's name for ``$1`` placeholders, which the PEP does not list; and
fetching after a statement without a result set returns no rows instead of
raising, because a query that matches nothing can look the same. Not
provided: ``callproc``, ``nextset``, scrollable cursors, and the ``Date``,
``Time``, ``Timestamp`` and ``*FromTicks`` constructors, because the binding
takes no temporal parameter values.
"""

from __future__ import annotations

import datetime
import functools
import operator
import os
import sys
import threading
from collections.abc import Callable, Iterator, Sequence
from itertools import islice
from types import TracebackType
from typing import Any, ParamSpec, TypeVar

from hawdb._hawdb import Database, QueryResult, Transaction, exceptions
from hawdb._hawdb import open as _open

__all__ = [
    "BINARY",
    "Binary",
    "Connection",
    "Cursor",
    "DATETIME",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NUMBER",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "ROWID",
    "STRING",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

apilevel = "2.0"
threadsafety = 1
paramstyle = "numeric_dollar"

_ABORTED = (
    "a failed executemany rolled back the whole transaction; "
    "call rollback() before using the connection again"
)


class Warning(Exception):
    """Declared by PEP 249; HawDB does not raise it."""


Error = exceptions.Error


class InterfaceError(Error):
    """A closed connection or cursor was used."""


class DatabaseError(Error):
    """An error from the database; engine errors arrive as its subclasses."""


class DataError(DatabaseError):
    """A value that does not fit its column, or a parameter the binding cannot convert."""


class OperationalError(DatabaseError):
    """A conflict, busy branch or refused resource, or an aborted connection."""


class IntegrityError(DatabaseError):
    """A violated constraint, such as a duplicate key or a NULL in a NOT NULL column."""


class InternalError(DatabaseError):
    """Damaged storage, or a connection out of step with its database."""


class ProgrammingError(DatabaseError):
    """An invalid statement or parameter, or a misused connection or cursor."""


class NotSupportedError(DatabaseError):
    """An unavailable capability, or autocommit off on a read-only connection."""


# An error from the binding is raised as a subclass of both its own type and
# its PEP 249 class, so code written against either keeps working. They are
# defined here, under the names they are bound to, so that they pickle.
class _ParseError(exceptions.ParseError, ProgrammingError):
    pass


class _SemanticError(exceptions.SemanticError, ProgrammingError):
    pass


class _StorageError(exceptions.StorageError, DatabaseError):
    pass


class _StorageConstraintError(exceptions.StorageError, IntegrityError):
    pass


class _StorageSchemaError(exceptions.StorageError, DataError):
    pass


class _StorageIntegrityError(exceptions.IntegrityError, InternalError):
    pass


class _DescriptorError(exceptions.DescriptorError, OperationalError):
    pass


class _ExecutionError(exceptions.ExecutionError, DatabaseError):
    pass


class _ExecutionConstraintError(exceptions.ExecutionError, IntegrityError):
    pass


class _ExecutionSchemaError(exceptions.ExecutionError, DataError):
    pass


class _RetryableExecutionError(exceptions.ExecutionError, OperationalError):
    pass


class _ResourceBudgetError(exceptions.ResourceBudgetError, OperationalError):
    pass


class _ConflictError(exceptions.ConflictError, OperationalError):
    pass


class _CapabilityError(exceptions.CapabilityError, NotSupportedError):
    pass


class _BranchError(exceptions.BranchError, OperationalError):
    pass


class _AdmissionError(exceptions.AdmissionError, OperationalError):
    pass


class _TaskStoppedError(exceptions.TaskStoppedError, OperationalError):
    pass


class _RetainedError(exceptions.RetainedError, DatabaseError):
    pass


class _BackpressureError(exceptions.BackpressureError, OperationalError):
    pass


class _InternalRuntimeError(RuntimeError, InternalError):
    pass


class _ProgrammingTypeError(TypeError, ProgrammingError):
    pass


class _DataValueError(ValueError, DataError):
    pass


class _DataOverflowError(OverflowError, DataError):
    pass


class _DataEncodingError(UnicodeEncodeError, DataError):
    pass


# Keyed by exact type, so that a subclass such as RecursionError passes through.
_ENGINE_ERRORS: dict[type[Exception], type[Exception]] = {
    # DatabaseError already derives from hawdb.exceptions.Error.
    exceptions.Error: DatabaseError,
    exceptions.ParseError: _ParseError,
    exceptions.SemanticError: _SemanticError,
    exceptions.StorageError: _StorageError,
    exceptions.IntegrityError: _StorageIntegrityError,
    exceptions.DescriptorError: _DescriptorError,
    exceptions.ExecutionError: _ExecutionError,
    exceptions.ResourceBudgetError: _ResourceBudgetError,
    exceptions.ConflictError: _ConflictError,
    exceptions.CapabilityError: _CapabilityError,
    exceptions.BranchError: _BranchError,
    exceptions.AdmissionError: _AdmissionError,
    exceptions.TaskStoppedError: _TaskStoppedError,
    exceptions.RetainedError: _RetainedError,
    exceptions.BackpressureError: _BackpressureError,
    RuntimeError: _InternalRuntimeError,
}
# A relational refusal keeps its engine type, and its ``kind`` picks the class.
_RELATIONAL_ERRORS: dict[tuple[type[Exception], str], type[Exception]] = {
    (exceptions.StorageError, "constraint"): _StorageConstraintError,
    (exceptions.StorageError, "schema"): _StorageSchemaError,
    (exceptions.ExecutionError, "constraint"): _ExecutionConstraintError,
    (exceptions.ExecutionError, "schema"): _ExecutionSchemaError,
}
# Only statements convert parameters, so only there do these mean a bad value.
_PARAMETER_ERRORS: dict[type[Exception], type[Exception]] = {
    TypeError: _ProgrammingTypeError,
    ValueError: _DataValueError,
    OverflowError: _DataOverflowError,
    UnicodeEncodeError: _DataEncodingError,
}


def _pep249(error: Exception, statement: bool) -> Exception:
    """Returns ``error`` rebuilt as its PEP 249 subclass, or else unchanged."""
    target = _ENGINE_ERRORS.get(type(error))
    if target is None and statement:
        target = _PARAMETER_ERRORS.get(type(error))
    if target is None:
        return error
    kind = getattr(error, "kind", None)
    if isinstance(kind, str):
        target = _RELATIONAL_ERRORS.get((type(error), kind), target)
    if target is _ExecutionError and getattr(error, "retryable", False) is True:
        target = _RetryableExecutionError
    try:
        translated = target(*error.args)
        vars(translated).update(vars(error))
    except Exception:
        # A failure here, such as a constructor that refuses the error's own
        # args, must not replace the error: it loses its PEP 249 class instead.
        return error
    return translated


_P = ParamSpec("_P")
_R = TypeVar("_R")


def _raises_pep249(
    statement: bool = False,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Raises the binding's errors from the decorated method as PEP 249 errors.

    ``statement`` marks a method that converts statement parameters.
    """

    def decorate(method: Callable[_P, _R]) -> Callable[_P, _R]:
        @functools.wraps(method)
        def call(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            try:
                return method(*args, **kwargs)
            except Exception as error:
                translated = _pep249(error, statement)
                try:
                    if translated is error:
                        raise
                    # The binding's error stays reachable as __context__.
                    translated.__traceback__ = error.__traceback__
                    raise translated from error.__cause__
                finally:
                    # The traceback holds this frame, so a local naming the
                    # error would form a cycle that only the garbage collector
                    # frees, possibly on another thread, where the unsendable
                    # native objects in the traceback cannot be dropped.
                    del translated

        return call

    return decorate


class _TypeObject:
    """A PEP 249 type object, equal to each Python type it groups.

    It hashes by identity, so it can key a dict, but a set of type objects
    finds only the objects themselves: compare a type code with ``==``.
    """

    __hash__ = object.__hash__

    def __init__(self, name: str, *types: type) -> None:
        self._name = name
        self._types = frozenset(types)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, type):
            return NotImplemented
        return other in self._types

    def __repr__(self) -> str:
        return f"hawdb.dbapi.{self._name}"


STRING = _TypeObject("STRING", str)
BINARY = _TypeObject("BINARY", bytes)
NUMBER = _TypeObject("NUMBER", int, float)
# Cursors return TIMESTAMP/TIMESTAMPTZ columns as datetime.datetime.
DATETIME = _TypeObject("DATETIME", datetime.date, datetime.datetime, datetime.time)
# HawDB exposes no row ids; ROWID never matches.
ROWID = _TypeObject("ROWID")

Binary = bytes

_Column = tuple[str, "type | None", None, None, None, None, None]


class Connection:
    """A DB-API connection that owns one ``hawdb.Database``.

    ``Connection(path=None, read_only=False, *, autocommit=False, ...)``, also
    spelled ``connect``, opens the database like ``hawdb.open``, with the same
    options. Closing the connection closes the database. As a context manager
    it commits on a clean exit, rolls back on an exception, and closes either
    way.
    """

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    @_raises_pep249()
    def __init__(
        self,
        path: str | os.PathLike[str] | None = None,
        read_only: bool = False,
        *,
        autocommit: bool = False,
        max_read_result_rows: int | None = None,
        max_read_result_payload_bytes: int | None = None,
        namespace_durability_anchor: str | os.PathLike[str] | None = None,
    ) -> None:
        self._database: Database | None = _open(
            path,
            read_only,
            max_read_result_rows=max_read_result_rows,
            max_read_result_payload_bytes=max_read_result_payload_bytes,
            namespace_durability_anchor=namespace_durability_anchor,
        )
        # A read-only database cannot commit even an empty transaction.
        self._read_only = bool(read_only)
        self._autocommit = bool(autocommit or read_only)
        # Thread-local, so a later thread that reuses this thread's ident
        # does not pass for the owner.
        self._owner = threading.local()
        self._owner.opened_here = True
        self._transaction: Transaction | None = None
        self._aborted = False  # a failed executemany ended the transaction

    @property
    def autocommit(self) -> bool:
        """Whether each statement commits on its own.

        It changes only between transactions, and a read-only connection
        keeps it on.
        """
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._open()
        if self._transaction is not None or self._aborted:
            raise ProgrammingError(
                "autocommit changes only between transactions; "
                "call commit() or rollback() first"
            )
        if self._read_only and not value:
            raise NotSupportedError("a read-only connection always autocommits")
        self._autocommit = bool(value)

    def cursor(self) -> Cursor:
        self._open()
        return Cursor(self)

    @_raises_pep249()
    def commit(self) -> None:
        """Publish the open transaction; without one, do nothing."""
        self._usable()
        transaction, self._transaction = self._transaction, None
        if transaction is not None:
            transaction.commit()

    @_raises_pep249()
    def rollback(self) -> None:
        """Discard the open transaction and clear an aborted state."""
        self._open()
        self._aborted = False
        transaction, self._transaction = self._transaction, None
        if transaction is not None:
            transaction.rollback()

    @_raises_pep249()
    def close(self) -> None:
        """Roll back uncommitted work and close the database; repeats do nothing."""
        if self._database is None:
            return
        database, self._database = self._open(), None
        transaction, self._transaction = self._transaction, None
        try:
            if transaction is not None:
                transaction.rollback()
        finally:
            database.close()

    def __enter__(self) -> Connection:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None and self._database is not None:
                self.commit()
        finally:
            self.close()

    def _open(self) -> Database:
        # The database is unsendable: another thread would hit a Rust panic.
        if not getattr(self._owner, "opened_here", False):
            raise ProgrammingError("a connection must stay on the thread that opened it")
        if self._database is None:
            raise InterfaceError("connection is closed")
        return self._database

    def _usable(self) -> Database:
        database = self._open()
        if self._aborted:
            raise OperationalError(_ABORTED)
        return database

    def _begin(self) -> Transaction:
        database = self._usable()
        if self._transaction is None:
            # Holding a transaction for queries blocks no other reader today:
            # the first handle to run a statement keeps others off its path
            # until it closes. Revisit this if handles ever share a path.
            self._transaction = database.transaction()
        return self._transaction

    @_raises_pep249(statement=True)
    def _execute(self, operation: str, parameters: list[Any] | None) -> QueryResult:
        if self._autocommit:
            return self._open().execute_sql(operation, parameters)
        return self._begin().execute_sql(operation, parameters)

    @_raises_pep249(statement=True)
    def _executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> None:
        if self._autocommit:
            self._open().executemany_sql(operation, seq_of_parameters)
            return
        transaction = self._begin()
        try:
            transaction.executemany_sql(operation, seq_of_parameters)
        except BaseException:
            # A failed batch has already rolled back the whole transaction.
            # Roll back here too, for a failure such as an interrupt that
            # left it open.
            self._transaction, self._aborted = None, True
            transaction.rollback()
            raise


connect = Connection


class Cursor:
    """A DB-API cursor; rows are tuples in ``description`` order.

    ``execute`` materializes the engine's whole result, as
    ``Database.execute_sql`` does. The fetch methods convert rows to Python
    tuples as they return them, and the result is released once every row
    has been fetched or the cursor runs another statement or closes.
    """

    _rows: Iterator[tuple[Any, ...]] | None  # None: nothing to fetch
    _description: tuple[_Column, ...] | None
    _rowcount: int

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.arraysize = 1
        self._closed = False
        self._reset()

    @property
    def description(self) -> tuple[_Column, ...] | None:
        return self._description

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> Cursor:
        """Run one SQL statement with ``parameters`` bound to ``$1``, ``$2``, ..."""
        self._check_open()
        self._reset()
        bound = None if parameters is None else list(_sequence(parameters, "parameters"))
        result = self.connection._execute(operation, bound)
        columns = result.columns
        if columns:
            self._description = tuple(
                (name, _type_code(result, position), None, None, None, None, None)
                for position, name in enumerate(columns)
            )
            self._rowcount = result.row_count
        affected = result.affected_rows
        if affected is not None:
            self._rowcount = affected
        self._rows = _tuples(result)
        return self

    def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> Cursor:
        """Run one SQL statement for each parameter sequence as one batch.

        The batch delegates to ``executemany_sql`` and produces no result set.
        In autocommit it is its own transaction; otherwise it joins the
        connection's transaction, which a failed batch aborts.
        """
        self._check_open()
        self._reset()
        # Wrong arguments must fail here: a failed batch aborts the transaction.
        if not isinstance(operation, str):
            raise _ProgrammingTypeError(
                f"operation must be a str, not {type(operation).__name__}"
            )
        _sequence(seq_of_parameters, "seq_of_parameters")
        self.connection._executemany(operation, seq_of_parameters)
        self._rows = iter(())
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        return next(self._result(), None)

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to ``size`` rows; ``size`` defaults to ``arraysize``."""
        rows = self._result()
        size = operator.index(self.arraysize if size is None else size)
        if size < 0:
            raise ValueError("fetchmany size must not be negative")
        return list(islice(rows, min(size, sys.maxsize)))

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._result())

    def close(self) -> None:
        """Release the result; repeats do nothing."""
        self._closed = True
        self._reset()

    def setinputsizes(self, sizes: object) -> None:
        """Accepted and ignored, as PEP 249 allows."""

    def setoutputsize(self, size: object, column: object = None) -> None:
        """Accepted and ignored, as PEP 249 allows."""

    def __iter__(self) -> Cursor:
        return self

    def __next__(self) -> tuple[Any, ...]:
        return next(self._result())

    def __enter__(self) -> Cursor:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError("cursor is closed")
        self.connection._open()

    def _reset(self) -> None:
        self._rows = None
        self._description = None
        self._rowcount = -1

    def _result(self) -> Iterator[tuple[Any, ...]]:
        self._check_open()
        if self._rows is None:
            raise ProgrammingError("no result to fetch; execute a statement first")
        return self._rows


def _sequence(value: Any, name: str) -> Sequence[Any]:
    # The binding's rule for parameter containers, checked here so that a
    # wrong container fails before it reaches the connection's transaction.
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise _ProgrammingTypeError(
            f"{name} must be a sequence such as a list or tuple, not "
            f"{type(value).__name__}; statements bind $1, $2, ... by position"
        )
    return value


def _sql_value(value: Any) -> Any:
    """Converts a SQL timestamp's tagged dict into ``datetime.datetime``.

    ``$localdatetime`` (TIMESTAMP) becomes a naive datetime and ``$datetime``
    (TIMESTAMPTZ, always UTC) an aware UTC datetime. Text that Python cannot
    represent exactly (more than six fractional digits or an out-of-range
    year) keeps the tagged dict.
    """
    if type(value) is not dict or len(value) != 1:
        return value
    ((key, text),) = value.items()
    if key not in ("$localdatetime", "$datetime") or type(text) is not str:
        return value
    _, dot, fraction = text.partition(".")
    if dot and len(fraction) - len(fraction.lstrip("0123456789")) > 6:
        return value
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        return value
    if (key == "$datetime") != (parsed.tzinfo is not None):
        return value
    return parsed if parsed.tzinfo is None else parsed.astimezone(datetime.timezone.utc)


# Rows the binding converts per call: bounds the tuples of a fetch that are
# alive twice, once as the binding returns them and once converted.
_FETCH_CHUNK = 1024


def _tuples(result: QueryResult) -> Iterator[tuple[Any, ...]]:
    """Yields the rows of ``result`` as tuples, converting them as they are fetched.

    The binding builds Python objects only for the rows it returns, and keeps
    none of them, so the cursor holds the engine's rows plus at most one chunk
    beyond what its caller kept. ``result`` is released when the rows run out
    or the cursor resets.
    """
    while chunk := result.fetchmany_tuples(_FETCH_CHUNK):
        for row in chunk:
            yield tuple(map(_sql_value, row))


def _type_code(result: QueryResult, position: int) -> type | None:
    """The type of the column's first non-null value in the whole result.

    The binding finds it among the engine's values without converting the
    rows, so the code does not depend on how many rows have been fetched.
    """
    value = result._first_non_null(position)
    return None if value is None else type(_sql_value(value))
