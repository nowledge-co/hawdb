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

"""A LangChain ``GraphStore`` over an embedded HawDB database.

``query`` runs the caller's Cypher with ``Database.execute``. ``refresh_schema``
reads the schema in one read snapshot: labels, relationship types, relationship
patterns and property statistics from ``system.graph_statistics``, constraints
and indexes from ``system.constraints`` and ``system.indexes``, and one node of
each label and one relationship of each type. The statistics leave out list,
map, vector and other non-scalar properties, and drop properties with many
distinct values when they outgrow their memory budget, so the sampled node and
relationship add the properties the statistics miss. ``get_schema`` and
``get_structured_schema`` return what the last refresh read, in the layout of
LangChain's ``Neo4jGraph``.

``add_graph_documents`` writes one call in one transaction, with one ``UNWIND``
statement per label and property set. Each write statement after the first
write in a transaction takes time that grows with the stored graph, whatever
its row count, so the store writes a call's rows with as few statements as it
can rather than one statement per row. It creates an equality index on ``id``
for each label it writes, so each ``MERGE`` finds its node through the index.

HawDB Cypher takes labels, relationship types and property names only as
statement text, spelled as ASCII letters, digits and underscores that do not
start with a digit, and has no backtick quoting. The store refuses any other
name, so a name cannot change a statement; ids and property values bind as
parameters.
"""

from __future__ import annotations

import hashlib
import re
import threading
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import hawdb
from langchain_community.graphs.graph_document import GraphDocument
from langchain_community.graphs.graph_store import GraphStore

if TYPE_CHECKING:
    from langchain_community.graphs.graph_document import Node, Relationship
    from langchain_core.documents import Document

# The label of a stored source document, and the type of its relationships to
# the nodes extracted from it, as in Neo4jGraph.
DOCUMENT_LABEL = "Document"
MENTIONS_TYPE = "MENTIONS"

# HawDB refuses an intermediate row of more than 16 MiB. A statement holds a
# node it reads with every property the node stores, and ``RETURN n`` holds
# the node a second time, so ``refresh_schema`` could not sample a label whose
# first node took more than 8 MiB. So the store refuses a node, source
# documents included, that would take more than MAX_NODE_BYTES, and
# relationship properties of more than MAX_RELATIONSHIP_BYTES. It counts each
# string by its UTF-8 bytes, four bytes per vector coordinate and 32 bytes for
# each value, list element and map entry, about what HawDB holds for each.
MAX_NODE_BYTES = 4 << 20
MAX_RELATIONSHIP_BYTES = 1 << 20
_VALUE_BYTES = 32

# A label, relationship type or property name HawDB Cypher can spell.
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# ``RETURN n`` adds these keys to a node's properties, and ``RETURN r`` these to
# a relationship's, hiding a stored property of the same name.
_NODE_KEYS = frozenset({"_id", "labels"})
_RELATIONSHIP_KEYS = frozenset({"_id", "source_id", "target_id", "type"})
# A node's ``id`` is its LangChain id, so its properties cannot set another.
_NODE_RESERVED = _NODE_KEYS | {"id"}

_STATISTICS = (
    "SELECT statistic_kind, label_name, relationship_type_name, "
    "source_label_name, target_label_name, property_name, histogram "
    "FROM system.graph_statistics WHERE statistic_kind IN ("
    "'label_count', 'relationship_type_count', 'path_count', "
    "'node_property_histogram', 'relationship_property_histogram')"
)
_CONSTRAINTS = (
    "SELECT subject_kind, subject_name, property_name, constraint_kind "
    "FROM system.constraints"
)
_INDEXES = (
    "SELECT subject_kind, subject_name, property_names, index_kind FROM system.indexes"
)
# Read per label, so that the rows stay within a host's max_read_result_rows
# however many other labels the database indexes.
_LABEL_INDEXES = (
    "SELECT property_names FROM system.indexes WHERE subject_kind = 'node' "
    "AND index_kind = 'equality' AND subject_name = $1"
)

# The binding returns temporal values and vectors as a dict with one of these
# keys.
_TAGGED_TYPES = {
    "$date": "DATE",
    "$datetime": "DATE_TIME",
    "$localdatetime": "LOCAL_DATE_TIME",
    "$localtime": "LOCAL_TIME",
    "$duration": "DURATION",
    "$vector": "VECTOR",
}

_WRONG_THREAD = (
    "HawDBGraph must be used on the thread that created it, because a "
    "hawdb.Database stays on the thread that opened it. LangChain runs ainvoke "
    "and abatch, and batch over several inputs, on worker threads; call invoke "
    "on that thread instead."
)


class WrongThreadError(RuntimeError):
    """A thread other than the store's creator called it."""


class HawDBGraph(GraphStore):
    """LangChain graph store over an open ``hawdb.Database``.

    The caller opens and closes the database, with any read budgets or
    ``read_only=True``; the store only runs statements on it. Creating the
    store reads the schema unless ``refresh_schema`` is false, and writes
    nothing, so a read-only database works for ``query`` and the schema.

    A ``hawdb.Database`` stays on the thread that opened it. Create the store on
    that thread and use it only there: another thread that calls the store gets
    ``WrongThreadError``.

    ``add_graph_documents`` writes like ``Neo4jGraph`` without
    ``baseEntityLabel``. It merges each node by its type and ``id`` and each
    relationship by its endpoints and type. A node or relationship that does
    not exist yet is created with the properties of its first occurrence in the
    call; one that exists keeps its stored properties. A relationship endpoint
    that no document lists among its nodes is created with only its ``id``.
    With ``include_source=True`` each source document becomes a ``Document``
    node with its ``page_content`` as ``text``, its metadata as properties and
    a ``MENTIONS`` relationship to each of its nodes; a stored ``Document``
    also keeps its properties. Its id is ``metadata["id"]``, or else the MD5
    digest of its text.

    The whole call is one transaction, so a call that fails writes nothing.
    HawDB refuses a transaction of more than 100,000 version entries, about one
    per node and three per relationship, and on a file database one whose WAL
    record exceeds 16 MiB; split a larger import into several calls.

    Node types, relationship types and property names must be HawDB Cypher
    names: ASCII letters, digits and underscores, not starting with a digit.
    Node properties cannot be named ``id``, ``_id`` or ``labels``, and
    relationship properties cannot be named ``_id``, ``source_id``,
    ``target_id`` or ``type``, because ``RETURN n`` and ``RETURN r`` use those
    keys. A property whose value is ``None`` is not stored. Ids must be strings
    or integers. So that the schema refresh can read every node back, a node,
    source documents included, may take at most ``MAX_NODE_BYTES`` (4 MiB) and
    a relationship's properties ``MAX_RELATIONSHIP_BYTES`` (1 MiB). The store
    checks a whole call before it writes.
    """

    def __init__(
        self, database: hawdb.Database, *, refresh_schema: bool = True
    ) -> None:
        if not isinstance(database, hawdb.Database):
            raise TypeError(
                f"database must be a hawdb.Database, not {type(database).__name__}"
            )
        self._db = database
        # Thread-local, so a later thread that reuses this thread's ident does
        # not pass for the owner.
        self._owner = threading.local()
        self._owner.created_here = True
        self.schema = ""
        self.structured_schema: dict[str, Any] = {}
        if refresh_schema:
            self.refresh_schema()

    @property
    def get_schema(self) -> str:
        """The schema text of the last ``refresh_schema``."""
        return self.schema

    @property
    def get_structured_schema(self) -> dict[str, Any]:
        """The structured schema of the last ``refresh_schema``."""
        return self.structured_schema

    def query(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Run the caller's Cypher with ``Database.execute`` and return its rows."""
        return self._database().execute(query, params or {}).fetchall()

    def refresh_schema(self) -> None:
        """Read the schema again, in one read snapshot."""
        with self._database().read_transaction() as snapshot:
            structured = _read_schema(snapshot)
        self.structured_schema = structured
        self.schema = _schema_text(structured)

    def add_graph_documents(
        self, graph_documents: list[GraphDocument], include_source: bool = False
    ) -> None:
        """Write the documents' nodes and relationships in one transaction.

        The schema is not read again; call ``refresh_schema`` afterwards.
        """
        database = self._database()
        batch = _Batch()
        for document in graph_documents:
            if not isinstance(document, GraphDocument):
                raise TypeError(
                    "graph_documents must hold GraphDocument objects, not "
                    f"{type(document).__name__}"
                )
            source_id = batch.add_source(document.source) if include_source else None
            for node in document.nodes:
                batch.add_node(node, source_id)
            for relationship in document.relationships:
                batch.add_relationship(relationship)
        statements = batch.statements()
        if not statements:
            return
        with database.transaction() as tx:
            # Read every index before the first write: each statement after a
            # write costs time that grows with the stored graph.
            missing = [
                label
                for label in sorted(batch.labels())
                if not any(
                    list(row["property_names"]) == ["id"]
                    for row in tx.execute_sql(_LABEL_INDEXES, [label])
                )
            ]
            for label in missing:
                tx.execute(f"CREATE INDEX ON :{label}(id)")
            for statement, rows in statements:
                tx.execute(statement, {"rows": rows})

    def _database(self) -> hawdb.Database:
        # The database is unsendable: another thread would hit a Rust panic.
        if not getattr(self._owner, "created_here", False):
            raise WrongThreadError(_WRONG_THREAD)
        return self._db


class _Batch:
    """The rows of one ``add_graph_documents`` call, checked and deduplicated."""

    def __init__(self) -> None:
        self.sources: dict[str | int, dict[str, Any]] = {}
        self.nodes: dict[tuple[str, str | int], dict[str, Any]] = {}
        # Dicts with ``None`` values keep their keys in insertion order.
        self.endpoints: dict[tuple[str, str | int], None] = {}
        self.relationships: dict[
            tuple[str, str | int, str, str, str | int], dict[str, Any]
        ] = {}
        self.mentions: dict[tuple[str | int, str, str | int], None] = {}

    def add_source(self, source: Document) -> str | int:
        metadata = dict(source.metadata)
        # Neo4jGraph replaces a missing or empty id the same way, but also
        # writes it back into the document's metadata.
        source_id = (
            metadata.pop("id", None)
            or hashlib.md5(
                source.page_content.encode("utf-8"), usedforsecurity=False
            ).hexdigest()
        )
        _check_id(source_id, "a source document's metadata id")
        owner = f"source document {_brief(source_id)}"
        # Metadata named "text" replaces the page content, as in Neo4jGraph.
        properties = _properties(
            {"text": source.page_content, **metadata}, _NODE_RESERVED, owner
        )
        _check_size(_size(source_id) + _size(properties), MAX_NODE_BYTES, owner)
        self.sources.setdefault(source_id, properties)
        return source_id

    def add_node(self, node: Node, source_id: str | int | None) -> None:
        """Add a node, mentioned by the source document ``source_id`` if set."""
        key = _endpoint(node)
        owner = f"node {_brief(key)}"
        properties = _properties(node.properties, _NODE_RESERVED, owner)
        _check_size(_size(key[1]) + _size(properties), MAX_NODE_BYTES, owner)
        self.nodes.setdefault(key, properties)
        if source_id is not None:
            self.mentions[(source_id, *key)] = None

    def add_relationship(self, relationship: Relationship) -> None:
        source = _endpoint(relationship.source)
        target = _endpoint(relationship.target)
        relationship_type = _check_name(relationship.type, "relationship type")
        key = (*source, relationship_type, *target)
        owner = f"relationship {_brief(key)}"
        properties = _properties(relationship.properties, _RELATIONSHIP_KEYS, owner)
        _check_size(
            _size(properties), MAX_RELATIONSHIP_BYTES, f"the properties of {owner}"
        )
        self.endpoints[source] = None
        self.endpoints[target] = None
        self.relationships.setdefault(key, properties)

    def labels(self) -> set[str]:
        """The labels of every node the call writes or matches."""
        labels = {label for label, _ in self.nodes}
        labels.update(label for label, _ in self.endpoints)
        if self.sources:
            labels.add(DOCUMENT_LABEL)
        return labels

    def statements(self) -> list[tuple[str, list[list[Any]]]]:
        """Group the rows by statement: sources, nodes, relationships, mentions."""
        groups: dict[str, list[list[Any]]] = {}
        for source_id, properties in self.sources.items():
            statement = _merge_nodes(DOCUMENT_LABEL, tuple(properties))
            groups.setdefault(statement, []).append([source_id, *properties.values()])
        for (label, node_id), properties in self.nodes.items():
            statement = _merge_nodes(label, tuple(properties))
            groups.setdefault(statement, []).append([node_id, *properties.values()])
        for label, node_id in self.endpoints:
            if (label, node_id) not in self.nodes:
                groups.setdefault(_merge_nodes(label, ()), []).append([node_id])
        for key, properties in self.relationships.items():
            source_label, source_id, relationship_type, target_label, target_id = key
            statement = _merge_relationships(
                source_label, relationship_type, target_label, tuple(properties)
            )
            groups.setdefault(statement, []).append(
                [source_id, target_id, *properties.values()]
            )
        for source_id, label, node_id in self.mentions:
            groups.setdefault(_merge_mentions(label), []).append([source_id, node_id])
        return list(groups.items())


def _endpoint(node: Node) -> tuple[str, str | int]:
    label = _check_name(node.type, "node type")
    what = f"the id of a {label} node"
    _check_id(node.id, what)
    _check_size(_size(node.id), MAX_NODE_BYTES, what)
    return label, node.id


def _check_name(name: object, what: str) -> str:
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise ValueError(
            f"{what} {name!r} is not a HawDB Cypher name: use ASCII letters, "
            "digits and underscores, not starting with a digit"
        )
    return name


def _check_id(value: object, what: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError(f"{what} must be a str or int, not {type(value).__name__}")


def _check_size(size: int, limit: int, what: str) -> None:
    if size > limit:
        raise ValueError(
            f"{what} would take about {size:,} bytes; the store writes at most "
            f"{limit:,} so that HawDB can read it back"
        )


def _size(value: Any) -> int:
    """Estimate the bytes HawDB holds for a value in an intermediate row."""
    if isinstance(value, str):
        return _VALUE_BYTES + len(value.encode())
    if isinstance(value, (bytes, bytearray)):
        return _VALUE_BYTES + len(value)
    if isinstance(value, dict):
        vector = value.get("$vector") if len(value) == 1 else None
        if isinstance(vector, (list, tuple)):
            return _VALUE_BYTES + 4 * len(vector)
        return _VALUE_BYTES + sum(_size(k) + _size(v) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return _VALUE_BYTES + sum(_size(item) for item in value)
    return _VALUE_BYTES


def _brief(value: object) -> str:
    text = repr(value)
    return text if len(text) <= 80 else f"{text[:77]}..."


def _properties(
    properties: dict[str, Any], reserved: frozenset[str], owner: str
) -> dict[str, Any]:
    """Check the property names and drop ``None`` values, sorted by name."""
    for name in properties:
        _check_name(name, f"property name of {owner}")
        if name in reserved:
            raise ValueError(f"{owner} cannot store a property named {name!r}")
    return {
        name: properties[name]
        for name in sorted(properties)
        if properties[name] is not None
    }


def _merge_nodes(label: str, names: tuple[str, ...]) -> str:
    statement = f"UNWIND $rows AS row MERGE (n:{label} {{id: row[0]}})"
    if not names:
        return statement
    assignments = ", ".join(
        f"n.{name} = row[{position}]" for position, name in enumerate(names, 1)
    )
    return f"{statement} ON CREATE SET {assignments}"


def _merge_relationships(
    source_label: str, relationship_type: str, target_label: str, names: tuple[str, ...]
) -> str:
    statement = (
        f"UNWIND $rows AS row MATCH (s:{source_label} {{id: row[0]}}), "
        f"(t:{target_label} {{id: row[1]}}) MERGE (s)-[r:{relationship_type}]->(t)"
    )
    if not names:
        return statement
    assignments = ", ".join(
        f"r.{name} = row[{position}]" for position, name in enumerate(names, 2)
    )
    return f"{statement} ON CREATE SET {assignments}"


def _merge_mentions(label: str) -> str:
    return (
        f"UNWIND $rows AS row MATCH (d:{DOCUMENT_LABEL} {{id: row[0]}}), "
        f"(n:{label} {{id: row[1]}}) MERGE (d)-[:{MENTIONS_TYPE}]->(n)"
    )


def _read_schema(snapshot: Any) -> dict[str, Any]:
    labels: set[str] = set()
    types: set[str] = set()
    patterns: set[tuple[str, str, str]] = set()
    node_properties: dict[str, dict[str, str]] = {}
    relationship_properties: dict[str, dict[str, str]] = {}
    for row in snapshot.execute_sql(_STATISTICS):
        kind = row["statistic_kind"]
        label = row["label_name"]
        relationship_type = row["relationship_type_name"]
        # A node without a label has no name to match it by.
        if kind == "label_count" and label is not None:
            labels.add(label)
        elif kind == "relationship_type_count" and relationship_type is not None:
            types.add(relationship_type)
        elif kind == "path_count":
            pattern = (
                row["source_label_name"],
                relationship_type,
                row["target_label_name"],
            )
            if None not in pattern:
                patterns.add(pattern)
        elif kind == "node_property_histogram" and label is not None:
            _add_histogram(node_properties.setdefault(label, {}), row)
        elif (
            kind == "relationship_property_histogram" and relationship_type is not None
        ):
            _add_histogram(
                relationship_properties.setdefault(relationship_type, {}), row
            )
    constraints = snapshot.execute_sql(_CONSTRAINTS).fetchall()
    indexes = snapshot.execute_sql(_INDEXES).fetchall()

    labels.update(node_properties)
    labels.update(start for start, _, _ in patterns)
    labels.update(end for _, _, end in patterns)
    types.update(relationship_properties)
    types.update(relationship_type for _, relationship_type, _ in patterns)
    # HawDB names are already spellable; the check keeps any other name out of
    # the statement text.
    for label in sorted(labels):
        if _NAME.fullmatch(label):
            for row in snapshot.execute(f"MATCH (n:{label}) RETURN n LIMIT 1"):
                _add_sampled(
                    node_properties.setdefault(label, {}), row["n"], _NODE_KEYS
                )
    for relationship_type in sorted(types):
        if _NAME.fullmatch(relationship_type):
            for row in snapshot.execute(
                f"MATCH ()-[r:{relationship_type}]->() RETURN r LIMIT 1"
            ):
                _add_sampled(
                    relationship_properties.setdefault(relationship_type, {}),
                    row["r"],
                    _RELATIONSHIP_KEYS,
                )

    return {
        "node_props": _property_lists(node_properties),
        "rel_props": _property_lists(relationship_properties),
        "relationships": [
            {"start": start, "type": relationship_type, "end": end}
            for start, relationship_type, end in sorted(patterns)
        ],
        "metadata": {"constraint": constraints, "index": indexes},
    }


def _add_histogram(owner: dict[str, str], row: dict[str, Any]) -> None:
    # The statistics leave out properties whose values differ in type, so the
    # first value other than an explicit null, which sorts first, names the
    # type of them all.
    for value in row["histogram"] or ():
        if value is not None:
            owner[row["property_name"]] = _type_name(value)
            return


def _add_sampled(
    owner: dict[str, str], element: dict[str, Any], added: frozenset[str]
) -> None:
    for name, value in element.items():
        if name not in added and value is not None:
            owner.setdefault(name, _type_name(value))


def _property_lists(
    owners: dict[str, dict[str, str]],
) -> dict[str, list[dict[str, str]]]:
    # Like Neo4jGraph, list only labels and types that have properties.
    return {
        owner: [
            {"property": name, "type": properties[name]} for name in sorted(properties)
        ]
        for owner, properties in sorted(owners.items())
        if properties
    }


def _type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, str):
        return "STRING"
    if isinstance(value, bytes):
        return "BYTES"
    if isinstance(value, Decimal):
        return "DECIMAL"
    if isinstance(value, list):
        return "LIST"
    if isinstance(value, dict):
        if len(value) == 1:
            [key] = value
            if key in _TAGGED_TYPES:
                return _TAGGED_TYPES[key]
        return "MAP"
    return type(value).__name__.upper()


def _schema_text(structured: dict[str, Any]) -> str:
    """Format the schema as Neo4jGraph and ``construct_schema`` do."""

    def properties(items: list[dict[str, str]]) -> str:
        return ", ".join(f"{item['property']}: {item['type']}" for item in items)

    return "\n".join(
        [
            "Node properties are the following:",
            ",".join(
                f"{label} {{{properties(items)}}}"
                for label, items in structured["node_props"].items()
            ),
            "Relationship properties are the following:",
            ",".join(
                f"{relationship_type} {{{properties(items)}}}"
                for relationship_type, items in structured["rel_props"].items()
            ),
            "The relationships are the following:",
            ",".join(
                f"(:{item['start']})-[:{item['type']}]->(:{item['end']})"
                for item in structured["relationships"]
            ),
        ]
    )
