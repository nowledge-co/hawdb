# HawDB plugin for Dify

A Dify tool plugin that runs parameterized Cypher and SQL against an embedded
HawDB database, with an optional external knowledge API endpoint that answers
Dify's retrieval requests from one Cypher statement. The plugin opens the
database in its own process with the `hawdb` Python binding; there is no
HawDB server or network protocol.

The plugin is not published on the Dify Marketplace. Package it from a
checkout of this repository. HawDB does not yet promise on-disk compatibility
between releases, so keep a backup of any database you cannot recreate.

## Versions

- The manifest declares plugin `meta.version` `0.0.1`, the plugin API version
  that `dify plugin init` writes for plugin daemon 0.6.x, and sets no
  `minimum_dify_version`. The limits below are plugin daemon 0.6.15's
  defaults.
- The plugin runs on Python 3.12 with `dify_plugin` 0.11 (`>=0.11.0,<0.12.0`;
  CI tests 0.11.0) and `hawdb` 0.31.0 or later.
- The plugin daemon installs those dependencies itself, from
  `pyproject.toml` with uv, or from `requirements.txt` when it is configured
  with a pip mirror, so it needs to reach PyPI or that mirror. `hawdb` has
  wheels for Linux (manylinux 2.28, x86_64 and aarch64), macOS and Windows.
  A reinstall can pick up a newer `hawdb`; to stay on one release, pin it in
  both files before packaging.

## Install

Package the plugin with the Dify plugin CLI from the
[dify-plugin-daemon](https://github.com/langgenius/dify-plugin-daemon)
releases:

```bash
dify plugin package ./integrations/dify-plugin-hawdb
```

`.difyignore` keeps the tests and local caches out of the package. Dify
installs unsigned packages only when the plugin daemon runs with
`FORCE_VERIFYING_SIGNATURE=false`; in the Docker deployment, set it in
`docker/.env` and restart. Then install the `.difypkg` file from Plugins >
Install plugin > Local package file.

The database path is a path on the host where the plugin daemon runs the
plugin. In the Docker deployment that is the `plugin_daemon` container, so
mount a directory into it, for example under its `volumes` in
`docker/docker-compose.yaml`:

```yaml
- ./volumes/hawdb:/data/hawdb
```

and use `/data/hawdb/graph` as the database path.

## Tools

Authorize the plugin in Dify with these fields:

| Field | Meaning |
| --- | --- |
| Database path | Absolute path of the database directory. In read-write mode a missing database is created; its parent directory must exist. |
| Access mode | `Read and write`, or `Read only`, which opens the database read-only so that HawDB refuses writes. A read-only database must exist. |
| Row budget | HawDB's `max_read_result_rows`. Empty or `0` sets no row ceiling. |
| Payload budget (bytes) | HawDB's `max_read_result_payload_bytes`, at least 1. Empty keeps the 64 MiB default. |

Saving the fields opens the database and runs a statement that reads nothing,
so a wrong path or budget is reported then.

| Tool | Inputs | Result |
| --- | --- | --- |
| Run Cypher (`cypher_query`) | `statement`, one Cypher statement; `parameters`, a JSON object of its `$name` values | `{"columns": [...], "rows": [...]}` |
| Run SQL (`sql_query`) | `statement`, one SQL statement; `parameters`, a JSON array of its `$1`, `$2`, ... values | `{"columns": [...], "rows": [...], "affected_rows": n}` |

For example, `MATCH (p:Person {name: $name}) RETURN p.born AS born` with
parameters `{"name": "Ada"}`, or `SELECT body FROM notes WHERE id = $1` with
`[7]`. Cypher ignores parameters the statement does not use; SQL needs exactly
as many as the statement uses. Leave `parameters` empty when there are none.

Each tool returns one JSON message, the `json` output of the tool node in a
workflow. A row is an object keyed by column name. A node comes back as a map
of its properties plus `_id` and `labels`, and a relationship as a map of its
properties plus `_id`, `type`, `source_id` and `target_id`.

A statement runs in its own autocommit transaction, so it commits completely
or not at all, and no transaction spans two calls. Every error is a tool
error, and a failed call returns no rows: HawDB errors keep their class name,
such as `ParseError`, `ResourceBudgetError` or `BranchError`; malformed
parameters are a `ParameterError`.

### Values JSON cannot represent

Parameters and results write these values as objects with one key:

| Value | JSON |
| --- | --- |
| bytes | `{"$bytes": "AAE="}` (base64) |
| decimal | `{"$decimal": "1.50"}` |
| NaN and infinities | `{"$float": "NaN"}`, `{"$float": "Infinity"}`, `{"$float": "-Infinity"}` |
| temporal values | `{"$date": "1815-12-10"}`, `$localtime`, `$localdatetime`, `$datetime`, `$duration` |
| vector | `{"$vector": [1.0, 2.5]}` |

The first three are the plugin's; the binding reads and writes the others as
described in the [Python binding README](../../bindings/python/README.md). The
JSON literals `NaN` and `Infinity` are refused. A map whose only key is one of
these tags is read as that value.

## Retrieval endpoint

The plugin's endpoint implements Dify's external knowledge API. Add an
endpoint in Plugins > HawDB > Endpoints with these settings:

| Setting | Meaning |
| --- | --- |
| Database path | Absolute path of an existing database, always opened read-only. |
| API key | The key Dify sends as `Authorization: Bearer <key>`, without whitespace. The endpoint refuses every request while it is empty or holds whitespace. |
| Retrieval statement | One Cypher statement, described below. |
| Row budget, Payload budget (bytes) | As for the tools. |

Dify lists the endpoint's URL, which ends in `/retrieval`. In Knowledge >
External Knowledge API, add an API whose endpoint is that URL without
`/retrieval`, since Dify appends it to every request, with the same API key.
Then connect an external knowledge base to that API; its External Knowledge
ID reaches the statement as `$knowledge_id`.

Each request runs the statement with these parameters:

| Parameter | Value |
| --- | --- |
| `$query` | The user's query. |
| `$knowledge_id` | The External Knowledge ID. |
| `$top_k` | Top K, a whole number. |
| `$score_threshold` | Score threshold, a float; 0.0 when Dify's threshold is off. |

The statement returns a `content` string and a `score` number, and may return
a `title` string and a `metadata` map; a null title becomes `""` and null
metadata `{}`. Any other column, or a row with a value of another type, fails
the request. For example:

```cypher
MATCH (c:Chunk)
WHERE c.kb = $knowledge_id AND c.text CONTAINS $query
RETURN c.text AS content, 1.0 AS score, c.title AS title,
       {source: c.source} AS metadata
ORDER BY c.position
LIMIT $top_k
```

Dify applies neither Top K nor the score threshold to external records, so the
endpoint keeps the rows whose score is at least `$score_threshold` and returns
the first `$top_k` of them, in the statement's order. A statement that
computes scores and ends in `LIMIT $top_k` should filter on
`$score_threshold` before the limit, or the limit can drop rows that pass.
When the API is added, Dify checks it with an empty query and knowledge ID
and Top K 1. The check fails on a 401, 403, 404 or 502 answer and passes on
any other, so it catches a wrong URL or API key but not a broken statement.

| Status | Body | When |
| --- | --- | --- |
| 200 | `{"records": [{"content", "score", "title", "metadata"}]}` | The statement ran and every row is a record. |
| 400 | `{"error_msg": ...}` | The body is not a retrieval request. |
| 401 | `{"error_code": 1001, "error_msg": ...}` | No `Authorization: Bearer <key>` header. |
| 403 | `{"error_code": 1002, "error_msg": ...}` | The key is wrong. |
| 500 | `{"error_msg": ...}` | The API key setting is empty, a setting is malformed, HawDB refused the statement, or a row is not a record. |

An unknown knowledge ID is not an error: the statement returns no records.
The endpoint ignores Dify's `metadata_condition`.

## Limits

- HawDB lets one handle at a time use a database, across processes. Each call
  opens the database, runs one statement, fetches every row and closes the
  database before it answers, and the plugin's calls on the same path wait for
  one another. While another process has the database open, a call fails with
  `BranchError: branch is busy` instead of waiting. Each call pays for the
  open, a few tens of milliseconds for a small database on a local disk.
  Statements run on a worker thread, so a slow one holds up only the calls on
  its own database, not the plugin's other requests or its heartbeat.
- The plugin daemon reads each plugin message as one line, and stops the
  plugin process on a line over `PLUGIN_RUNTIME_MAX_BUFFER_SIZE`, 5 MiB by
  default. A tool therefore refuses a result whose JSON exceeds 4 MiB with
  `ResultTooLargeError`. The statement has run by then, so a write that
  returns such a result has committed. The budgets make HawDB refuse a large
  read itself. The endpoint sends its response in 64 KiB pieces.
- Nothing interrupts a running statement. If the plugin daemon stops waiting
  for a long call, the statement still finishes, and the plugin's other calls
  on that database wait for it.
- The tools run whatever statement the agent or workflow passes, with the
  access the fields give. Use `Read only` and budgets for agents that only
  need to read. Anyone with the endpoint URL and API key can run the
  retrieval statement.

## Tests

From the repository root, with Python 3.12 or later:

```bash
pip install -r integrations/dify-plugin-hawdb/requirements.txt pytest
```

```bash
pytest integrations/dify-plugin-hawdb/tests
```

The tests load the plugin the way the plugin daemon does and send requests
through the SDK's request handler, and one test runs `main.py` over stdin and
stdout. They use file databases in temporary directories and need no Dify
server or network access. CI runs them against the `hawdb` wheel built from
the checkout.
