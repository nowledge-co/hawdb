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

"""Dify's external knowledge `POST /retrieval` contract over one statement.

The operator configures one Cypher statement. Each request runs it on a
read-only handle with `$query`, `$knowledge_id`, `$top_k` and
`$score_threshold` bound, and maps its rows onto Dify records.
"""

import decimal
import hmac
import json
import logging
import math
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from typing import Any

import hawdb
from dify_plugin import Endpoint
from werkzeug import Request, Response

import hawdb_dify

# The plugin daemon relays a response body as hex text, one stdout line per
# chunk, so a large body is sent in chunks well below its line limit.
_CHUNK_BYTES = 64 * 1024
_REQUIRED_COLUMNS = ("content", "score")
_RECORD_COLUMNS = (*_REQUIRED_COLUMNS, "title", "metadata")
_MAX_TOP_K = 2**63 - 1

logger = logging.getLogger(__name__)


class RecordError(Exception):
    """A row of the configured statement that does not map onto a record."""


@dataclass(frozen=True)
class RetrievalRequest:
    query: str
    knowledge_id: str
    top_k: int
    score_threshold: float

    def parameters(self) -> dict[str, Any]:
        return asdict(self)


class RetrievalEndpoint(Endpoint):
    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        del values
        api_key = settings.get("api_key")
        if not isinstance(api_key, str) or not api_key.strip():
            return _error(500, "the endpoint setting api_key is empty")
        if any(character.isspace() for character in api_key.strip()):
            # A Bearer token cannot carry whitespace, so no request could match.
            return _error(500, "the endpoint setting api_key contains whitespace")
        refusal = _authorize(r.headers.get("Authorization"), api_key.strip())
        if refusal is not None:
            return refusal
        try:
            config = hawdb_dify.database_config(settings, read_only=True)
            statement = hawdb_dify.statement_text(
                settings.get("retrieval_statement"), "retrieval_statement"
            )
        except ValueError as error:
            return _error(500, f"endpoint settings: {error}")
        try:
            request = _retrieval_request(r.get_json(force=True, silent=True))
        except hawdb_dify.ParameterError as error:
            return _error(400, str(error))
        try:
            output = hawdb_dify.run_cypher(config, statement, request.parameters())
            records = _records(output, request)
        except (hawdb.exceptions.Error, RecordError) as error:
            return _error(500, f"{type(error).__name__}: {error}")
        except Exception as error:
            # Anything else still answers with Dify's JSON error body.
            logger.exception("retrieval failed")
            return _error(500, f"{type(error).__name__}: {error}")
        return _json_response(200, {"records": records})


def _authorize(header: str | None, api_key: str) -> Response | None:
    parts = (header or "").split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return _error(
            401,
            "Invalid Authorization header format. Expected 'Bearer <api-key>' format.",
            code=1001,
        )
    if not hmac.compare_digest(parts[1].encode("utf-8"), api_key.encode("utf-8")):
        return _error(403, "Authorization failed.", code=1002)
    return None


def _retrieval_request(body: Any) -> RetrievalRequest:
    if not isinstance(body, dict):
        raise hawdb_dify.ParameterError("the request body must be a JSON object")
    query = body.get("query")
    knowledge_id = body.get("knowledge_id")
    setting = body.get("retrieval_setting")
    if not isinstance(query, str):
        raise hawdb_dify.ParameterError("query must be a string")
    if not isinstance(knowledge_id, str):
        raise hawdb_dify.ParameterError("knowledge_id must be a string")
    if not isinstance(setting, dict):
        raise hawdb_dify.ParameterError("retrieval_setting must be an object")
    top_k = setting.get("top_k")
    if (
        isinstance(top_k, bool)
        or not isinstance(top_k, int)
        or not 0 <= top_k <= _MAX_TOP_K
    ):
        raise hawdb_dify.ParameterError(
            "retrieval_setting.top_k must be a whole number of 0 or more"
        )
    threshold = setting.get("score_threshold")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise hawdb_dify.ParameterError(
            "retrieval_setting.score_threshold must be a number"
        )
    try:
        threshold = float(threshold)
    except OverflowError:
        threshold = math.inf
    if not math.isfinite(threshold):
        raise hawdb_dify.ParameterError(
            "retrieval_setting.score_threshold must be finite"
        )
    return RetrievalRequest(query, knowledge_id, top_k, threshold)


def _records(
    output: hawdb_dify.QueryOutput, request: RetrievalRequest
) -> list[dict[str, Any]]:
    missing = [name for name in _REQUIRED_COLUMNS if name not in output.columns]
    unknown = [name for name in output.columns if name not in _RECORD_COLUMNS]
    if missing or unknown:
        raise RecordError(
            "the retrieval statement must return the columns content and score, and may "
            f"return title and metadata; it returned {output.columns}"
        )
    # Dify applies neither setting to external records, so the endpoint drops
    # records below the threshold and keeps the first top_k in statement order.
    records = [_record(row, number) for number, row in enumerate(output.rows, start=1)]
    kept = [record for record in records if record["score"] >= request.score_threshold]
    return kept[: request.top_k]


def _record(row: dict[str, Any], number: int) -> dict[str, Any]:
    content = row["content"]
    if not isinstance(content, str):
        raise RecordError(
            f"row {number}: content must be a string, got {_kind(content)}"
        )
    score = row["score"]
    if isinstance(score, bool) or not isinstance(score, (int, float, decimal.Decimal)):
        raise RecordError(f"row {number}: score must be a number, got {_kind(score)}")
    score = float(score)
    if not math.isfinite(score):
        raise RecordError(f"row {number}: score must be finite")
    title = row.get("title")
    if title is not None and not isinstance(title, str):
        raise RecordError(
            f"row {number}: title must be a string or null, got {_kind(title)}"
        )
    metadata = row.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise RecordError(
            f"row {number}: metadata must be a map or null, got {_kind(metadata)}"
        )
    # Dify's retrieval pipeline needs an object, not null, for metadata.
    return {
        "content": content,
        "score": score,
        "title": title or "",
        "metadata": hawdb_dify.json_value(metadata or {}),
    }


def _kind(value: Any) -> str:
    return "null" if value is None else type(value).__name__


def _error(status: int, message: str, *, code: int | None = None) -> Response:
    payload: dict[str, Any] = {} if code is None else {"error_code": code}
    payload["error_msg"] = message
    return _json_response(status, payload)


def _json_response(status: int, payload: dict[str, Any]) -> Response:
    body = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    )
    return Response(
        _chunks(body.encode("utf-8")), status=status, content_type="application/json"
    )


def _chunks(body: bytes) -> Iterator[bytes]:
    for start in range(0, len(body), _CHUNK_BYTES):
        yield body[start : start + _CHUNK_BYTES]
