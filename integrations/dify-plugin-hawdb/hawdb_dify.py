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

"""Code shared by the plugin's tools, provider and retrieval endpoint.

Every call opens the configured database, runs one statement, fetches all of
its rows and closes the database before anything is returned to Dify. HawDB
lets one handle at a time use a database, in this process or any other, so
calls on the same path take turns and the plugin holds no handle between
calls.
"""

from __future__ import annotations

import base64
import binascii
import decimal
import json
import math
import os
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import gevent
import hawdb

# Dify's plugin daemon reads each plugin message as one line of at most
# PLUGIN_RUNTIME_MAX_BUFFER_SIZE bytes (5 MiB by default) and kills the plugin
# process on a longer line. A tool result is one JSON message, so the tools
# refuse a result whose JSON is larger than this, leaving room for the
# message envelope.
MAX_MESSAGE_BYTES = 4 * 1024 * 1024

# Budgets are unsigned 64-bit engine options.
_MAX_BUDGET = 2**64 - 1

# Single-key objects for values JSON cannot represent. The binding itself
# returns and accepts the temporal tags ($date, $localtime, $localdatetime,
# $datetime, $duration) and $vector; these three are the plugin's own.
_BYTES_TAG = "$bytes"
_DECIMAL_TAG = "$decimal"
_FLOAT_TAG = "$float"
_NON_FINITE = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}


class PluginConfigurationError(ValueError):
    """A provider credential or endpoint setting is missing or malformed."""


class ParameterError(ValueError):
    """A statement or its parameters cannot be sent to HawDB."""


class ResultTooLargeError(Exception):
    """A result whose JSON is larger than one Dify plugin message."""


@dataclass(frozen=True)
class DatabaseConfig:
    """The arguments of `hawdb.open` taken from plugin configuration."""

    path: str
    read_only: bool
    max_read_result_rows: int | None
    max_read_result_payload_bytes: int | None


@dataclass(frozen=True)
class QueryOutput:
    """The fetched rows of one statement, as the binding returned them."""

    columns: list[str]
    rows: list[dict[str, Any]]
    affected_rows: int | None


def database_config(
    settings: Mapping[str, Any], *, read_only: bool | None = None
) -> DatabaseConfig:
    """Read the database options from provider credentials or endpoint settings.

    `read_only` overrides the `access_mode` setting; the retrieval endpoint
    always opens the database read-only.
    """
    path = settings.get("database_path")
    if not isinstance(path, str) or not path.strip():
        raise PluginConfigurationError("database_path is required")
    path = path.strip()
    if not os.path.isabs(path):
        raise PluginConfigurationError(
            f"database_path must be an absolute path on the plugin host, got {path!r}"
        )
    if read_only is None:
        mode = settings.get("access_mode") or "read_write"
        if mode not in ("read_write", "read_only"):
            raise PluginConfigurationError(
                f"access_mode must be read_write or read_only, got {mode!r}"
            )
        read_only = mode == "read_only"
    return DatabaseConfig(
        path=path,
        read_only=read_only,
        # A row budget of 0 sets no ceiling, but a payload budget of 0 would
        # refuse every result, so the payload budget starts at 1.
        max_read_result_rows=_budget(settings, "max_read_result_rows", 0),
        max_read_result_payload_bytes=_budget(
            settings, "max_read_result_payload_bytes", 1
        ),
    )


def _budget(settings: Mapping[str, Any], name: str, minimum: int) -> int | None:
    # Dify credential and setting forms have no number field, so budgets
    # arrive as text; an empty field keeps the engine default.
    value = settings.get(name)
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        number = int(value) if value.isascii() and value.isdigit() else None
    elif isinstance(value, int) and not isinstance(value, bool):
        number = value
    else:
        number = None
    if number is None or not minimum <= number <= _MAX_BUDGET:
        raise PluginConfigurationError(
            f"{name} must be empty or a whole number from {minimum} to "
            f"{_MAX_BUDGET}, got {value!r}"
        )
    return number


_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _lock_for(path: str) -> threading.Lock:
    # Created on first use, after dify_plugin has patched `threading` for
    # gevent, so a call that waits yields to the plugin's other requests.
    key = os.path.realpath(path)
    with _locks_guard:
        lock = _locks.get(key)
        if lock is None:
            lock = _locks[key] = threading.Lock()
        return lock


def with_database[T](config: DatabaseConfig, use: Callable[[hawdb.Database], T]) -> T:
    """Open the configured database, call `use` with it and close it.

    dify_plugin runs every request as a greenlet on one OS thread, so a
    statement running on that thread would stall the plugin's other requests
    and its heartbeat. The whole open, `use` and close therefore run on one
    thread of gevent's thread pool, which keeps the handle on the thread that
    created it; the binding releases the GIL while HawDB works. Calls on the
    same path wait for one another, since only one handle may use a database.
    """
    with _lock_for(config.path):
        return gevent.get_hub().threadpool.apply(_open_and_use, (config, use))


def _open_and_use[T](config: DatabaseConfig, use: Callable[[hawdb.Database], T]) -> T:
    database = hawdb.open(
        config.path,
        read_only=config.read_only,
        max_read_result_rows=config.max_read_result_rows,
        max_read_result_payload_bytes=config.max_read_result_payload_bytes,
    )
    try:
        return use(database)
    except BaseException as error:
        # The traceback's frames hold the handle, and the error is re-raised
        # on the hub's thread, where the binding refuses to free a handle.
        _drop_tracebacks(error)
        raise
    finally:
        database.close()
        del database


def _drop_tracebacks(error: BaseException) -> None:
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        current.__traceback__ = None
        pending.extend(
            chained
            for chained in (current.__cause__, current.__context__)
            if chained is not None
        )


def check_database(config: DatabaseConfig) -> None:
    """Open the database and run a statement that reads nothing.

    A read-write path that does not exist yet is created, as `hawdb.open`
    does; a read-only path must already hold a database.
    """
    with_database(
        config, lambda db: db.execute("UNWIND [0] AS ready RETURN ready").fetchall()
    )


def run_cypher(
    config: DatabaseConfig, statement: str, parameters: dict[str, Any]
) -> QueryOutput:
    """Run one Cypher statement with named parameters and fetch every row."""
    return with_database(config, lambda db: _fetch(db.execute(statement, parameters)))


def run_sql(
    config: DatabaseConfig, statement: str, parameters: list[Any]
) -> QueryOutput:
    """Run one SQL statement with `$1`, `$2`, ... parameters and fetch every row."""
    return with_database(
        config, lambda db: _fetch(db.execute_sql(statement, parameters))
    )


def run_tool(
    credentials: Mapping[str, Any], tool_parameters: Mapping[str, Any], *, sql: bool
) -> dict[str, Any]:
    """Run one tool call and return its JSON message."""
    config = database_config(credentials)
    statement = statement_text(tool_parameters.get("statement"))
    parameters = parse_parameters(tool_parameters.get("parameters"), positional=sql)
    if sql:
        output = run_sql(config, statement, parameters)
    else:
        output = run_cypher(config, statement, parameters)
    return tool_payload(output, affected_rows=sql)


def _fetch(result: hawdb.QueryResult) -> QueryOutput:
    return QueryOutput(list(result.columns), result.fetchall(), result.affected_rows)


def statement_text(value: Any, name: str = "statement") -> str:
    """Return the statement unchanged, refusing a missing or blank one."""
    if not isinstance(value, str) or not value.strip():
        raise ParameterError(f"{name} is required")
    return value


def parse_parameters(value: Any, *, positional: bool) -> dict[str, Any] | list[Any]:
    """Turn the tool's JSON parameter text into statement parameters.

    Cypher takes a JSON object of `$name` values and SQL a JSON array of the
    `$1`, `$2`, ... values. Empty input means no parameters. Single-key
    `$bytes`, `$decimal` and `$float` objects become bytes, `decimal.Decimal`
    and non-finite floats; the binding reads the temporal and `$vector` tags.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return [] if positional else {}
    if isinstance(value, str):
        try:
            value = json.loads(value, parse_constant=_refuse_constant)
        except ValueError as error:
            raise ParameterError(f"parameters must be JSON: {error}") from error
    if positional and not isinstance(value, list):
        raise ParameterError(
            "parameters must be a JSON array of the $1, $2, ... values"
        )
    if not positional and not isinstance(value, dict):
        raise ParameterError("parameters must be a JSON object of the $name values")
    if positional:
        return [_decode(item, f"${index}") for index, item in enumerate(value, start=1)]
    return {key: _decode(item, _member("$", key)) for key, item in value.items()}


def _refuse_constant(name: str) -> Any:
    raise ValueError(
        f"{name} is not a JSON value; write {json.dumps({_FLOAT_TAG: name})}"
    )


def _member(path: str, key: str) -> str:
    if key.isidentifier():
        return f"{path}.{key}" if path != "$" else f"${key}"
    return f"{path}[{json.dumps(key, ensure_ascii=False)}]"


def _decode(value: Any, path: str) -> Any:
    if isinstance(value, list):
        return [_decode(item, f"{path}[{index}]") for index, item in enumerate(value)]
    if not isinstance(value, dict):
        return value
    if len(value) == 1:
        ((tag, text),) = value.items()
        if tag in (_BYTES_TAG, _DECIMAL_TAG, _FLOAT_TAG):
            return _decode_tag(tag, text, path)
    return {key: _decode(item, _member(path, key)) for key, item in value.items()}


def _decode_tag(tag: str, text: Any, path: str) -> Any:
    if not isinstance(text, str):
        raise ParameterError(f"{path}: {tag} takes a string")
    if tag == _BYTES_TAG:
        try:
            return base64.b64decode(text, validate=True)
        except binascii.Error as error:
            raise ParameterError(f"{path}: {tag} must be base64: {error}") from error
    if tag == _DECIMAL_TAG:
        try:
            number = decimal.Decimal(text)
        except decimal.InvalidOperation as error:
            raise ParameterError(
                f"{path}: {tag} must be a decimal number, got {text!r}"
            ) from error
        if not number.is_finite():
            raise ParameterError(f"{path}: {tag} must be finite, got {text!r}")
        return number
    if text not in _NON_FINITE:
        raise ParameterError(
            f"{path}: {tag} must be NaN, Infinity or -Infinity, got {text!r}"
        )
    return _NON_FINITE[text]


def json_value(value: Any) -> Any:
    """Return a result value with bytes, decimals and non-finite floats tagged."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {
            _FLOAT_TAG: "NaN"
            if math.isnan(value)
            else "Infinity"
            if value > 0
            else "-Infinity"
        }
    if isinstance(value, decimal.Decimal):
        return {_DECIMAL_TAG: str(value)}
    if isinstance(value, bytes):
        return {_BYTES_TAG: base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_value(item) for item in value]
    raise TypeError(f"cannot encode a {type(value).__name__} result value as JSON")


def json_size(payload: Any) -> int:
    """The UTF-8 size of `payload` as compact JSON, as Dify messages carry it."""
    text = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    return len(text.encode("utf-8"))


def tool_payload(output: QueryOutput, *, affected_rows: bool) -> dict[str, Any]:
    """The tool's JSON message: the column names and every fetched row."""
    payload: dict[str, Any] = {
        "columns": output.columns,
        "rows": [json_value(row) for row in output.rows],
    }
    if affected_rows:
        payload["affected_rows"] = output.affected_rows
    size = json_size(payload)
    if size > MAX_MESSAGE_BYTES:
        raise ResultTooLargeError(
            f"the result is {size} bytes of JSON in {len(output.rows)} rows, more than the "
            f"{MAX_MESSAGE_BYTES} bytes one Dify plugin message carries; return fewer rows or "
            "columns, or lower max_read_result_rows or max_read_result_payload_bytes so that "
            "HawDB refuses such a read"
        )
    return payload
