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

"""The HawDB Dify plugin against real embedded databases, with no network.

The plugin object is built by main.py, as the Dify plugin daemon starts it,
and each request goes through the SDK's own request handler with a writer
that keeps the lines the daemon would read from the plugin's stdout. One test
also runs main.py as a process and talks to it over stdin and stdout. No Dify
server or plugin daemon is involved.
"""

import base64
import binascii
import importlib.metadata
import importlib.util
import json
import os
import select
import subprocess
import sys
import threading
import time
import tomllib
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Importing dify_plugin patches the standard library for gevent, as it does
# first thing in the plugin process; the plugin's locks are created after it.
import dify_plugin  # noqa: F401
import gevent.monkey
import hawdb
import pytest
from dify_plugin.core.server.__base.response_writer import ResponseWriter

PLUGIN_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PLUGIN_DIR))

import hawdb_dify

API_KEY = "test-key"
# The plugin daemon's default PLUGIN_RUNTIME_MAX_BUFFER_SIZE.
DAEMON_LINE_LIMIT = 5 * 1024 * 1024

RETRIEVAL = (
    "MATCH (c:Chunk) WHERE c.kb = $knowledge_id AND c.text CONTAINS $query "
    "RETURN c.text AS content, c.score AS score, c.title AS title, "
    "{source: c.source} AS metadata ORDER BY c.rank LIMIT $top_k"
)
CHUNKS = [
    {
        "kb": "docs",
        "text": "HawDB is an embedded graph database",
        "title": "Intro",
        "source": "intro.md",
        "rank": 1,
        "score": {"$decimal": "0.9"},
    },
    {
        "kb": "docs",
        "text": "Dify calls the retrieval endpoint",
        "title": None,
        "source": "dify.md",
        "rank": 2,
        "score": 0.2,
    },
    {
        "kb": "docs",
        "text": "Budgets bound every read",
        "title": "Budgets",
        "source": "budgets.md",
        "rank": 3,
        "score": 0.8,
    },
    {
        "kb": "other",
        "text": "HawDB in another knowledge base",
        "title": "Other",
        "source": "other.md",
        "rank": 4,
        "score": 1.0,
    },
]


class RecordingWriter(ResponseWriter):
    """Keeps the lines a plugin process writes to the daemon."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def write(self, data: str) -> None:
        self.lines.append(data)

    def done(self) -> None:
        pass


@dataclass
class Reply:
    lines: list[str]
    stream: list[dict[str, Any]]
    error: dict[str, Any] | None


@dataclass
class HttpReply:
    status: int
    headers: dict[str, str]
    body: bytes
    chunks: int
    lines: list[str]

    def json(self) -> Any:
        return json.loads(self.body)


@pytest.fixture(scope="session")
def plugin():
    """The `plugin` main.py builds, loaded from the plugin directory."""
    previous_dir = os.getcwd()
    previous_method = os.environ.get("INSTALL_METHOD")
    # A remote-debugging .env in the plugin directory must not redirect the
    # plugin to a Dify instance.
    os.environ["INSTALL_METHOD"] = "local"
    os.chdir(PLUGIN_DIR)
    try:
        spec = importlib.util.spec_from_file_location(
            "dify_plugin_hawdb_main", PLUGIN_DIR / "main.py"
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        os.chdir(previous_dir)
        if previous_method is None:
            del os.environ["INSTALL_METHOD"]
        else:
            os.environ["INSTALL_METHOD"] = previous_method
    return module.plugin


def handle(plugin, request: dict[str, Any]) -> Reply:
    """Run one daemon request through the handler the SDK's stdin reader uses."""
    writer = RecordingWriter()
    session_id = uuid.uuid4().hex
    plugin._execute_request_in_thread(
        session_id, request, plugin.request_reader, writer
    )
    return parse_session(writer.lines, session_id)


def parse_session(lines: list[str], session_id: str) -> Reply:
    stream, errors, ended = [], [], False
    for line in lines:
        message = json.loads(line)
        assert message["event"] == "session"
        assert message["session_id"] == session_id
        assert not ended, "nothing follows the end of a session"
        kind, data = message["data"]["type"], message["data"]["data"]
        if kind == "stream":
            stream.append(data)
        elif kind == "error":
            errors.append(data)
        else:
            assert kind == "end"
            ended = True
    assert ended
    assert len(errors) <= 1
    return Reply(lines, stream, errors[0] if errors else None)


def run_tool(
    plugin, tool: str, credentials: dict[str, Any], **parameters: Any
) -> Reply:
    return handle(
        plugin,
        {
            "type": "tool",
            "action": "invoke_tool",
            "user_id": "test-user",
            "provider": "hawdb",
            "tool": tool,
            "credentials": credentials,
            "tool_parameters": parameters,
        },
    )


def cypher(plugin, credentials, statement, parameters=None) -> Reply:
    if parameters is not None and not isinstance(parameters, str):
        parameters = json.dumps(parameters)
    return run_tool(
        plugin, "cypher_query", credentials, statement=statement, parameters=parameters
    )


def sql(plugin, credentials, statement, parameters=None) -> Reply:
    if parameters is not None and not isinstance(parameters, str):
        parameters = json.dumps(parameters)
    return run_tool(
        plugin, "sql_query", credentials, statement=statement, parameters=parameters
    )


def tool_json(reply: Reply) -> dict[str, Any]:
    """The single JSON message of a successful call."""
    assert reply.error is None, reply.error
    assert len(reply.stream) == 1, reply.stream
    (message,) = reply.stream
    assert message["type"] == "json"
    return message["message"]["json_object"]


def tool_error(reply: Reply) -> dict[str, Any]:
    """The error of a refused call, which sent no message before it."""
    assert reply.stream == []
    assert reply.error is not None
    return reply.error


def validate(plugin, credentials: dict[str, Any]) -> Reply:
    return handle(
        plugin,
        {
            "type": "tool",
            "action": "validate_tool_credentials",
            "user_id": "test-user",
            "provider": "hawdb",
            "credentials": credentials,
        },
    )


def call_endpoint(
    plugin,
    settings: dict[str, Any],
    body: Any,
    *,
    authorization: str | None = f"Bearer {API_KEY}",
    path: str = "/retrieval",
) -> HttpReply:
    """Send one HTTP request to the plugin as the daemon relays it."""
    payload = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    head = [
        f"POST {path} HTTP/1.1",
        "Host: localhost",
        "Content-Type: application/json",
        f"Content-Length: {len(payload)}",
    ]
    if authorization is not None:
        head.append(f"Authorization: {authorization}")
    raw = "\r\n".join([*head, "", ""]).encode("ascii") + payload
    reply = handle(
        plugin,
        {
            "type": "endpoint",
            "action": "invoke_endpoint",
            "settings": settings,
            "raw_http_request": raw.hex(),
        },
    )
    assert reply.error is None, reply.error
    parts = [message["result"] for message in reply.stream if "result" in message]
    return HttpReply(
        status=reply.stream[0]["status"],
        headers=reply.stream[0]["headers"],
        body=b"".join(binascii.unhexlify(part) for part in parts),
        chunks=len(parts),
        lines=reply.lines,
    )


def credentials_for(path: Path, **extra: Any) -> dict[str, Any]:
    return {"database_path": str(path), **extra}


def settings_for(
    path: Path, statement: str = RETRIEVAL, **extra: Any
) -> dict[str, Any]:
    return {
        "database_path": str(path),
        "api_key": API_KEY,
        "retrieval_statement": statement,
        **extra,
    }


def retrieval(
    query: str = "",
    knowledge_id: str = "docs",
    top_k: int = 10,
    score_threshold: float = 0.0,
) -> dict[str, Any]:
    return {
        "knowledge_id": knowledge_id,
        "query": query,
        "retrieval_setting": {"top_k": top_k, "score_threshold": score_threshold},
    }


def longest_line(lines: list[str]) -> int:
    return max(len(line.rstrip("\n").encode("utf-8")) for line in lines)


def assert_closed(path: Path) -> None:
    """The plugin holds no handle: another handle can run a statement."""
    with hawdb.open(str(path)) as db:
        db.execute("UNWIND [0] AS ready RETURN ready").fetchall()


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return tmp_path / "graph"


@pytest.fixture(scope="module")
def chunks(plugin, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A database holding the CHUNKS nodes, written through the Cypher tool.

    The tests that use it only read it, so they share one database.
    """
    database = tmp_path_factory.mktemp("chunks") / "graph"
    created = cypher(
        plugin,
        credentials_for(database),
        "UNWIND $rows AS row CREATE (:Chunk {kb: row.kb, text: row.text, title: row.title, "
        "source: row.source, rank: row.rank, score: row.score})",
        {"rows": CHUNKS},
    )
    tool_json(created)
    return database


def test_declarations_load_through_the_sdk(plugin) -> None:
    registration = plugin.registration
    configuration = registration.configuration
    assert configuration.name == "hawdb"
    assert configuration.version == "0.1.0"
    # The plugin API version `dify plugin init` writes for daemon 0.6.x.
    assert configuration.meta.version == "0.0.1"
    assert configuration.meta.runner.version == "3.12"
    assert configuration.meta.runner.entrypoint == "main"
    assert [
        (provider.identity.name, [tool.identity.name for tool in provider.tools])
        for provider in registration.tools_configuration
    ] == [("hawdb", ["cypher_query", "sql_query"])]
    assert [
        [(endpoint.path, endpoint.method) for endpoint in group.endpoints]
        for group in registration.endpoints_configuration
    ] == [[("/retrieval", "POST")]]
    assert [asset.filename for asset in registration.files] == ["icon.svg"]
    version = importlib.metadata.version("dify_plugin")
    assert version.split(".")[:2] == ["0", "11"], version


def test_daemon_installs_the_same_dependencies_either_way() -> None:
    pyproject = tomllib.loads((PLUGIN_DIR / "pyproject.toml").read_text("utf-8"))
    requirements = (PLUGIN_DIR / "requirements.txt").read_text("utf-8").split()
    assert pyproject["project"]["dependencies"] == requirements


def test_cypher_creates_with_a_parameter_and_reads_it_back(
    plugin, database: Path
) -> None:
    credentials = credentials_for(database)
    tool_json(
        cypher(
            plugin,
            credentials,
            "CREATE (:Person {name: $name, born: $born})",
            {"name": "Ada", "born": 1815},
        )
    )
    found = tool_json(
        cypher(
            plugin,
            credentials,
            "MATCH (p:Person) WHERE p.born < $year RETURN p.name AS name, p.born AS born",
            {"year": 1900},
        )
    )
    assert found == {
        "columns": ["name", "born"],
        "rows": [{"name": "Ada", "born": 1815}],
    }
    assert_closed(database)


def test_sql_runs_with_positional_parameters(plugin, database: Path) -> None:
    credentials = credentials_for(database)
    created = tool_json(
        sql(
            plugin,
            credentials,
            "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)",
        )
    )
    assert created["columns"] == []
    assert created["rows"] == []
    assert "affected_rows" in created
    inserted = tool_json(
        sql(
            plugin,
            credentials,
            "INSERT INTO notes (id, body) VALUES ($1, $2), ($3, $4)",
            [1, "first", 2, "second"],
        )
    )
    assert inserted["affected_rows"] == 2
    selected = tool_json(
        sql(
            plugin,
            credentials,
            "SELECT id, body FROM notes WHERE id > $1 ORDER BY id",
            [0],
        )
    )
    assert selected == {
        "columns": ["id", "body"],
        "rows": [{"id": 1, "body": "first"}, {"id": 2, "body": "second"}],
        "affected_rows": None,
    }
    missing = tool_error(sql(plugin, credentials, "SELECT id FROM notes WHERE id = $1"))
    assert missing["error_type"] == "SemanticError"
    assert "requires 1 parameters" in missing["message"]


def test_retrieval_returns_one_record(plugin, chunks: Path) -> None:
    reply = call_endpoint(
        plugin,
        settings_for(chunks),
        retrieval("embedded", top_k=3, score_threshold=0.5),
    )
    assert reply.status == 200
    assert reply.headers["Content-Type"] == "application/json"
    assert reply.json() == {
        "records": [
            {
                "content": "HawDB is an embedded graph database",
                "score": 0.9,
                "title": "Intro",
                "metadata": {"source": "intro.md"},
            }
        ]
    }
    assert_closed(chunks)


@pytest.mark.parametrize(
    ("setting", "value", "texts"),
    [
        ("max_read_result_rows", "1", ("exceeding max_read_result_rows 1",)),
        (
            "max_read_result_payload_bytes",
            "8",
            ("8-byte budget", "max_read_result_payload_bytes 8"),
        ),
    ],
)
def test_a_read_over_budget_is_a_tool_error_without_rows(
    plugin, chunks: Path, setting: str, value: str, texts: tuple[str, ...]
) -> None:
    budgeted = credentials_for(chunks, **{setting: value})
    error = tool_error(
        cypher(plugin, budgeted, "MATCH (c:Chunk) RETURN c.text AS text")
    )
    # hawdb 0.31 raises ExecutionError for the row budget; later bindings
    # raise its subclass ResourceBudgetError for both budgets.
    refusal = getattr(hawdb.exceptions, error["error_type"])
    assert issubclass(refusal, hawdb.exceptions.ExecutionError)
    assert any(text in error["message"] for text in texts), error["message"]
    assert_closed(chunks)


def test_a_read_within_the_row_budget_succeeds(plugin, chunks: Path) -> None:
    budgeted = credentials_for(chunks, max_read_result_rows="1")
    found = tool_json(
        cypher(
            plugin,
            budgeted,
            "MATCH (c:Chunk {rank: $rank}) RETURN c.title AS title",
            {"rank": 1},
        )
    )
    assert found["rows"] == [{"title": "Intro"}]


def test_a_retrieval_over_budget_returns_no_records(plugin, chunks: Path) -> None:
    reply = call_endpoint(
        plugin, settings_for(chunks, max_read_result_rows="1"), retrieval()
    )
    assert reply.status == 500
    body = reply.json()
    assert "records" not in body
    assert "exceeding max_read_result_rows 1" in body["error_msg"]


def test_read_only_access_mode_refuses_writes(plugin, chunks: Path) -> None:
    read_only = credentials_for(chunks, access_mode="read_only")
    error = tool_error(cypher(plugin, read_only, "CREATE (:Person {name: 'Eve'})"))
    assert error["error_type"] == "ExecutionError"
    assert "read-only" in error["message"]
    counted = tool_json(
        cypher(plugin, read_only, "MATCH (c:Chunk) RETURN count(c) AS n")
    )
    assert counted["rows"] == [{"n": 4}]


def test_values_json_cannot_hold_travel_as_tags(plugin, database: Path) -> None:
    parameters = {
        "blob": {"$bytes": base64.b64encode(b"\x00\xffhaw").decode("ascii")},
        "price": {"$decimal": "1.50"},
        "low": {"$float": "-Infinity"},
        "missing": {"$float": "NaN"},
        "day": {"$date": "1815-12-10"},
        "embedding": {"$vector": [1.0, 2.5]},
        "nested": {"list": [{"$bytes": "AA=="}, {"$decimal": "-7"}]},
    }
    statement = "UNWIND [0] AS z RETURN " + ", ".join(
        f"${name} AS {name}" for name in parameters
    )
    found = tool_json(cypher(plugin, credentials_for(database), statement, parameters))
    assert found == {"columns": list(parameters), "rows": [parameters]}


def test_nodes_and_relationships_come_back_as_maps(plugin, database: Path) -> None:
    credentials = credentials_for(database)
    tool_json(
        cypher(
            plugin,
            credentials,
            "CREATE (:Person {name: $a, photo: $photo})-[:KNOWS {since: 2020}]->"
            "(:Person {name: $b})",
            {"a": "Ada", "b": "Bob", "photo": {"$bytes": "AAE="}},
        )
    )
    found = tool_json(
        cypher(
            plugin,
            credentials,
            "MATCH (a:Person {name: 'Ada'})-[k:KNOWS]->(b) RETURN a, k, b.name AS b",
        )
    )
    (row,) = found["rows"]
    assert row["a"]["labels"] == ["Person"]
    assert row["a"]["name"] == "Ada"
    assert row["a"]["photo"] == {"$bytes": "AAE="}
    assert row["k"]["type"] == "KNOWS"
    assert row["k"]["since"] == 2020
    assert row["k"]["source_id"] == row["a"]["_id"]
    assert row["b"] == "Bob"


@pytest.mark.parametrize(
    ("tool", "statement", "parameters", "text"),
    [
        ("cypher_query", "RETURN 1", "[1]", "parameters must be a JSON object"),
        ("sql_query", "SELECT 1", '{"a": 1}', "parameters must be a JSON array"),
        ("cypher_query", "RETURN 1", "{not json", "parameters must be JSON"),
        ("cypher_query", "RETURN 1", '{"x": NaN}', 'write {"$float": "NaN"}'),
        (
            "cypher_query",
            "RETURN 1",
            '{"x": {"$bytes": "no base64!"}}',
            "$x: $bytes must be base64",
        ),
        (
            "cypher_query",
            "RETURN 1",
            '{"m": {"k": {"$decimal": "NaN"}}}',
            "$m.k: $decimal must be finite",
        ),
        (
            "cypher_query",
            "RETURN 1",
            '{"a b": [{"$bytes": 7}]}',
            '$["a b"][0]: $bytes takes a string',
        ),
        (
            "sql_query",
            "SELECT $1",
            '[{"$float": "nan"}]',
            "$1: $float must be NaN, Infinity or -Infinity",
        ),
        ("cypher_query", "   ", None, "statement is required"),
    ],
)
def test_parameter_errors_refuse_before_opening_the_database(
    plugin, database: Path, tool: str, statement: str, parameters: str | None, text: str
) -> None:
    error = tool_error(
        run_tool(
            plugin,
            tool,
            credentials_for(database),
            statement=statement,
            parameters=parameters,
        )
    )
    assert error["error_type"] == "ParameterError"
    assert text in error["message"]
    assert not database.exists()


def test_valid_credentials_create_a_read_write_database(plugin, database: Path) -> None:
    reply = validate(plugin, credentials_for(database, max_read_result_rows=" 100 "))
    assert reply.error is None, reply.error
    assert reply.stream == [{"result": True}]
    assert database.exists()
    assert_closed(database)


@pytest.mark.parametrize(
    ("credentials", "text"),
    [
        ({"database_path": ""}, "database_path is required"),
        ({"database_path": "graph"}, "must be an absolute path"),
        ({"access_mode": "admin"}, "access_mode must be read_write or read_only"),
        (
            {"max_read_result_rows": "ten"},
            "max_read_result_rows must be empty or a whole number from 0",
        ),
        (
            {"max_read_result_payload_bytes": "0"},
            "max_read_result_payload_bytes must be empty or a whole number from 1",
        ),
        (
            {"max_read_result_payload_bytes": "-1"},
            "max_read_result_payload_bytes must be empty",
        ),
        (
            {"max_read_result_rows": str(2**64)},
            "from 0 to 18446744073709551615",
        ),
        ({"access_mode": "read_only"}, "read-only database path does not exist"),
    ],
)
def test_invalid_credentials_are_refused(
    plugin, database: Path, credentials: dict[str, Any], text: str
) -> None:
    error = validate(plugin, {"database_path": str(database), **credentials}).error
    assert error is not None
    assert error["error_type"] == "ToolProviderCredentialValidationError"
    assert text in error["message"]
    assert not database.exists()


def test_a_result_larger_than_one_message_is_a_tool_error(
    plugin, database: Path
) -> None:
    credentials = credentials_for(database)
    statement = "UNWIND range(1, $n) AS i RETURN i, $pad AS pad"
    pad = "x" * 1_000_000
    error = tool_error(cypher(plugin, credentials, statement, {"n": 5, "pad": pad}))
    assert error["error_type"] == "ResultTooLargeError"
    assert str(hawdb_dify.MAX_MESSAGE_BYTES) in error["message"]
    assert_closed(database)
    largest = cypher(plugin, credentials, statement, {"n": 4, "pad": pad})
    assert len(tool_json(largest)["rows"]) == 4
    assert longest_line(largest.lines) < DAEMON_LINE_LIMIT


@pytest.mark.parametrize(
    ("authorization", "status", "code"),
    [
        (None, 401, 1001),
        ("Basic dGVzdA==", 401, 1001),
        ("Bearer", 401, 1001),
        (f"Bearer {API_KEY} extra", 401, 1001),
        ("Bearer wrong-key", 403, 1002),
    ],
)
def test_retrieval_refuses_a_bad_api_key(
    plugin, chunks: Path, authorization: str | None, status: int, code: int
) -> None:
    reply = call_endpoint(
        plugin, settings_for(chunks), retrieval(), authorization=authorization
    )
    assert reply.status == status
    assert reply.json()["error_code"] == code


def test_retrieval_accepts_any_case_of_bearer(plugin, chunks: Path) -> None:
    reply = call_endpoint(
        plugin,
        settings_for(chunks),
        retrieval(top_k=1),
        authorization=f"bearer {API_KEY}",
    )
    assert reply.status == 200
    assert len(reply.json()["records"]) == 1


def test_retrieval_without_an_api_key_setting_refuses_every_request(
    plugin, chunks: Path
) -> None:
    reply = call_endpoint(
        plugin, settings_for(chunks, api_key=" "), retrieval(), authorization="Bearer  "
    )
    assert reply.status == 500
    assert reply.json() == {"error_msg": "the endpoint setting api_key is empty"}


def test_retrieval_refuses_an_api_key_setting_with_whitespace(
    plugin, chunks: Path
) -> None:
    reply = call_endpoint(
        plugin,
        settings_for(chunks, api_key="two words"),
        retrieval(),
        authorization="Bearer two words",
    )
    assert reply.status == 500
    assert reply.json() == {
        "error_msg": "the endpoint setting api_key contains whitespace"
    }


@pytest.mark.parametrize(
    ("body", "text"),
    [
        (b"not json", "the request body must be a JSON object"),
        ([], "the request body must be a JSON object"),
        (
            {
                "knowledge_id": "docs",
                "retrieval_setting": {"top_k": 1, "score_threshold": 0},
            },
            "query must be a string",
        ),
        (
            {
                "query": "x",
                "knowledge_id": 7,
                "retrieval_setting": {"top_k": 1, "score_threshold": 0},
            },
            "knowledge_id must be a string",
        ),
        ({"query": "x", "knowledge_id": "docs"}, "retrieval_setting must be an object"),
        (
            {**retrieval(), "retrieval_setting": {"top_k": -1, "score_threshold": 0}},
            "top_k",
        ),
        (
            {**retrieval(), "retrieval_setting": {"top_k": True, "score_threshold": 0}},
            "top_k",
        ),
        (
            {**retrieval(), "retrieval_setting": {"top_k": 1.5, "score_threshold": 0}},
            "top_k",
        ),
        (
            {
                **retrieval(),
                "retrieval_setting": {"top_k": 2**63, "score_threshold": 0},
            },
            "top_k",
        ),
        (
            {
                **retrieval(),
                "retrieval_setting": {"top_k": 1, "score_threshold": "0.5"},
            },
            "score_threshold must be a number",
        ),
        (
            {
                **retrieval(),
                "retrieval_setting": {"top_k": 1, "score_threshold": 10**400},
            },
            "score_threshold must be finite",
        ),
    ],
)
def test_retrieval_refuses_a_malformed_request(
    plugin, chunks: Path, body: Any, text: str
) -> None:
    reply = call_endpoint(plugin, settings_for(chunks), body)
    assert reply.status == 400
    error = reply.json()
    assert "error_code" not in error
    assert text in error["error_msg"]


def test_retrieval_answers_difys_validation_request(plugin, chunks: Path) -> None:
    # The body Dify posts to check an external knowledge API when it is added.
    probe = {
        "knowledge_id": "",
        "query": "",
        "retrieval_setting": {"top_k": 1, "score_threshold": 0.0},
    }
    reply = call_endpoint(plugin, settings_for(chunks), probe)
    assert reply.status == 200
    assert reply.json() == {"records": []}


def test_retrieval_ignores_the_metadata_condition(plugin, chunks: Path) -> None:
    condition = {
        "logical_operator": "and",
        "conditions": [
            {"name": "source", "comparison_operator": "is", "value": "dify.md"}
        ],
    }
    body = {**retrieval(top_k=1), "metadata_condition": condition}
    reply = call_endpoint(plugin, settings_for(chunks), body)
    assert reply.status == 200
    assert [record["title"] for record in reply.json()["records"]] == ["Intro"]


def test_retrieval_applies_the_threshold_then_top_k_in_statement_order(
    plugin, chunks: Path
) -> None:
    statement = (
        "MATCH (c:Chunk) WHERE c.kb = $knowledge_id RETURN c.text AS content, "
        "c.score AS score, c.title AS title ORDER BY c.rank"
    )
    settings = settings_for(chunks, statement)

    def titles(top_k: int, score_threshold: float) -> list[tuple[str, float]]:
        reply = call_endpoint(
            plugin, settings, retrieval(top_k=top_k, score_threshold=score_threshold)
        )
        assert reply.status == 200
        return [
            (record["title"], record["score"]) for record in reply.json()["records"]
        ]

    assert titles(10, 0.5) == [("Intro", 0.9), ("Budgets", 0.8)]
    assert titles(1, 0.5) == [("Intro", 0.9)]
    assert titles(10, 0.0) == [("Intro", 0.9), ("", 0.2), ("Budgets", 0.8)]
    assert titles(0, 0.0) == []


def test_retrieval_records_default_title_and_metadata(plugin, chunks: Path) -> None:
    statement = (
        "MATCH (c:Chunk {rank: 2}) RETURN c.text AS content, c.score AS score, "
        "c.title AS title, null AS metadata"
    )
    reply = call_endpoint(plugin, settings_for(chunks, statement), retrieval())
    assert reply.json() == {
        "records": [
            {
                "content": "Dify calls the retrieval endpoint",
                "score": 0.2,
                "title": "",
                "metadata": {},
            }
        ]
    }
    bare = call_endpoint(
        plugin,
        settings_for(
            chunks,
            "MATCH (c:Chunk {rank: 1}) RETURN c.text AS content, "
            "c.score AS score, {exact: c.score} AS metadata",
        ),
        retrieval(),
    )
    assert bare.json()["records"] == [
        {
            "content": "HawDB is an embedded graph database",
            "score": 0.9,
            "title": "",
            "metadata": {"exact": {"$decimal": "0.9"}},
        }
    ]


@pytest.mark.parametrize(
    ("statement", "text"),
    [
        (
            "UNWIND [0] AS z RETURN 'a' AS text, 1.0 AS score",
            "RecordError: the retrieval statement must return the columns content and score",
        ),
        (
            "UNWIND [0] AS z RETURN 'a' AS content, 1.0 AS score, 2 AS rank",
            "it returned ['content', 'score', 'rank']",
        ),
        (
            "UNWIND [0] AS z RETURN 7 AS content, 1.0 AS score",
            "row 1: content must be a string, got int",
        ),
        (
            "UNWIND [0] AS z RETURN 'a' AS content, true AS score",
            "row 1: score must be a number, got bool",
        ),
        (
            "UNWIND [1.0, null] AS s RETURN 'a' AS content, s AS score",
            "row 2: score must be a number, got null",
        ),
        (
            "UNWIND [0] AS z RETURN 'a' AS content, 1.0 AS score, 5 AS title",
            "row 1: title must be a string or null, got int",
        ),
        (
            "UNWIND [0] AS z RETURN 'a' AS content, 1.0 AS score, [1] AS metadata",
            "row 1: metadata must be a map or null, got list",
        ),
    ],
)
def test_retrieval_refuses_rows_that_are_not_records(
    plugin, chunks: Path, statement: str, text: str
) -> None:
    reply = call_endpoint(plugin, settings_for(chunks, statement), retrieval())
    assert reply.status == 500
    body = reply.json()
    assert "records" not in body
    assert text in body["error_msg"]


@pytest.mark.parametrize(
    ("settings", "text"),
    [
        (
            {"retrieval_statement": " "},
            "endpoint settings: retrieval_statement is required",
        ),
        (
            {"database_path": "graph"},
            "endpoint settings: database_path must be an absolute path",
        ),
        (
            {"max_read_result_rows": "many"},
            "endpoint settings: max_read_result_rows must be",
        ),
        ({"retrieval_statement": "MATCH (c RETURN c"}, "ParseError: "),
        (
            {
                "retrieval_statement": "MATCH (c:Chunk) SET c.text = $query "
                "RETURN c.text AS content, 1.0 AS score"
            },
            "ExecutionError: execution error: database is opened in read-only mode",
        ),
    ],
)
def test_retrieval_reports_setting_and_statement_errors(
    plugin, chunks: Path, settings: dict[str, Any], text: str
) -> None:
    reply = call_endpoint(plugin, {**settings_for(chunks), **settings}, retrieval("x"))
    assert reply.status == 500
    assert text in reply.json()["error_msg"]
    counted = tool_json(
        cypher(plugin, credentials_for(chunks), "MATCH (c:Chunk) RETURN count(c) AS n")
    )
    assert counted["rows"] == [{"n": 4}]


def test_retrieval_does_not_create_a_missing_database(plugin, database: Path) -> None:
    reply = call_endpoint(plugin, settings_for(database), retrieval())
    assert reply.status == 500
    assert "StorageError: " in reply.json()["error_msg"]
    assert not database.exists()


def test_retrieval_sends_a_large_body_in_chunks(plugin, chunks: Path) -> None:
    query = "q" * 300_000
    settings = settings_for(
        chunks, "UNWIND [0] AS z RETURN $query AS content, 1.0 AS score"
    )
    reply = call_endpoint(plugin, settings, retrieval(query))
    assert reply.status == 200
    assert reply.json()["records"][0]["content"] == query
    assert reply.chunks == -(-len(reply.body) // (64 * 1024))
    assert reply.chunks > 1
    assert longest_line(reply.lines) < DAEMON_LINE_LIMIT


def test_unknown_endpoint_path_is_not_found(plugin, chunks: Path) -> None:
    reply = call_endpoint(plugin, settings_for(chunks), retrieval(), path="/search")
    assert reply.status == 404


def test_a_database_another_handle_is_using_is_a_tool_error(
    plugin, chunks: Path
) -> None:
    credentials = credentials_for(chunks)
    statement = "MATCH (c:Chunk) RETURN count(c) AS n"
    with hawdb.open(str(chunks)) as other:
        other.execute(statement).fetchall()
        error = tool_error(cypher(plugin, credentials, statement))
        assert error["error_type"] == "BranchError"
        assert "busy" in error["message"]
    assert tool_json(cypher(plugin, credentials, statement))["rows"] == [{"n": 4}]


def concurrent_calls(plugin, chunks: Path, count: int) -> list[Any]:
    """Run tool and retrieval calls on one database at the same time."""
    results: list[Any] = [None] * count

    def call(index: int) -> None:
        if index % 2:
            results[index] = call_endpoint(
                plugin, settings_for(chunks), retrieval(top_k=1)
            )
        else:
            results[index] = cypher(
                plugin, credentials_for(chunks), "MATCH (c:Chunk) RETURN count(c) AS n"
            )

    threads = [threading.Thread(target=call, args=(index,)) for index in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return results


def test_a_running_statement_does_not_stall_other_requests(
    plugin, chunks: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Block the call's thread for real, as a slow statement does, and count
    # how often another greenlet runs meanwhile.
    blocking_sleep = gevent.monkey.get_original("time", "sleep")
    fetch = hawdb_dify._fetch

    def slow_fetch(result):
        blocking_sleep(0.3)
        return fetch(result)

    monkeypatch.setattr(hawdb_dify, "_fetch", slow_fetch)
    replies: list[Reply] = []
    call = threading.Thread(
        target=lambda: replies.append(
            cypher(
                plugin, credentials_for(chunks), "MATCH (c:Chunk) RETURN count(c) AS n"
            )
        )
    )
    call.start()
    ticks = 0
    while call.is_alive():
        time.sleep(0.01)
        ticks += 1
    call.join()
    assert tool_json(replies[0])["rows"] == [{"n": 4}]
    assert ticks >= 10


def test_concurrent_calls_on_one_database_take_turns(
    plugin, chunks: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Requests are greenlets in the plugin process. Make each one yield while
    # it holds an open handle, as a slow read would.
    assert gevent.monkey.is_module_patched("threading")
    fetch = hawdb_dify._fetch

    def yielding_fetch(result):
        time.sleep(0.05)
        return fetch(result)

    monkeypatch.setattr(hawdb_dify, "_fetch", yielding_fetch)
    for result in concurrent_calls(plugin, chunks, 6):
        if isinstance(result, HttpReply):
            assert result.status == 200, result.body
        else:
            assert tool_json(result)["rows"] == [{"n": 4}]

    # Without the per-path lock, the same calls run into each other's handle.
    monkeypatch.setattr(hawdb_dify, "_lock_for", lambda path: threading.Lock())
    collided = [
        result
        for result in concurrent_calls(plugin, chunks, 6)
        if isinstance(result, Reply)
        and result.error
        and result.error["error_type"] == "BranchError"
    ]
    assert collided
    assert_closed(chunks)


class LineReader:
    """Reads lines from a pipe with a deadline."""

    def __init__(self, fd: int) -> None:
        self.fd = fd
        self.buffer = b""

    def line(self, deadline: float) -> bytes:
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            assert remaining > 0, "the plugin process did not answer in time"
            ready, _, _ = select.select([self.fd], [], [], remaining)
            if ready:
                chunk = os.read(self.fd, 1 << 20)
                assert chunk, "the plugin process closed its stdout"
                self.buffer += chunk
        line, _, self.buffer = self.buffer.partition(b"\n")
        return line


def exchange(process, reader: LineReader, request: dict[str, Any]) -> Reply:
    """Write one request line to the plugin and read its session's lines."""
    session_id = uuid.uuid4().hex
    line = json.dumps({"session_id": session_id, "event": "request", "data": request})
    process.stdin.write(line.encode("utf-8") + b"\n")
    process.stdin.flush()
    deadline = time.monotonic() + 60
    lines = []
    while True:
        raw = reader.line(deadline).strip()
        if not raw:
            continue
        message = json.loads(raw)
        if message.get("event") != "session" or message.get("session_id") != session_id:
            continue
        lines.append(raw.decode("utf-8"))
        if message["data"]["type"] == "end":
            return parse_session(lines, session_id)


@pytest.mark.skipif(sys.platform == "win32", reason="select() on pipes needs POSIX")
# gevent's thread pool threads exist by now, and Popen forks.
@pytest.mark.filterwarnings(
    "ignore:This process .* is multi-threaded:DeprecationWarning"
)
def test_main_answers_the_daemon_over_stdin_and_stdout(tmp_path: Path) -> None:
    database = tmp_path / "graph"
    environment = {**os.environ, "INSTALL_METHOD": "local"}
    with (tmp_path / "plugin-stderr.txt").open("wb") as stderr:
        process = subprocess.Popen(
            [sys.executable, "-m", "main"],
            cwd=PLUGIN_DIR,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
        )
        try:
            reader = LineReader(process.stdout.fileno())
            created = exchange(
                process,
                reader,
                {
                    "type": "tool",
                    "action": "invoke_tool",
                    "user_id": "test-user",
                    "provider": "hawdb",
                    "tool": "cypher_query",
                    "credentials": credentials_for(database),
                    "tool_parameters": {
                        "statement": "CREATE (:Chunk {kb: $kb, text: $text, rank: 1, score: 1.0})",
                        "parameters": json.dumps({"kb": "docs", "text": "over stdio"}),
                    },
                },
            )
            tool_json(created)
            raw = (
                "POST /retrieval HTTP/1.1\r\nHost: localhost\r\n"
                f"Authorization: Bearer {API_KEY}\r\nContent-Type: application/json\r\n\r\n"
            ).encode("ascii") + json.dumps(retrieval("stdio")).encode("utf-8")
            answered = exchange(
                process,
                reader,
                {
                    "type": "endpoint",
                    "action": "invoke_endpoint",
                    "settings": settings_for(database),
                    "raw_http_request": raw.hex(),
                },
            )
        finally:
            process.kill()
            process.wait()
    assert answered.error is None, answered.error
    assert answered.stream[0]["status"] == 200
    body = b"".join(
        binascii.unhexlify(m["result"]) for m in answered.stream if "result" in m
    )
    assert json.loads(body)["records"] == [
        {
            "content": "over stdio",
            "score": 1.0,
            "title": "",
            "metadata": {"source": None},
        }
    ]
