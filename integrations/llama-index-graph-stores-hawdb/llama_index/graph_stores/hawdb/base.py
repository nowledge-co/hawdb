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

"""A LlamaIndex ``PropertyGraphStore`` over an embedded HawDB database.

HawDB Cypher takes labels and relationship types only as statement text, so
the store keeps every LlamaIndex node under one label and every relation under
one type, and stores the LlamaIndex label as a property::

    (:__Node__ {id, kind, label, name, text, properties,
                triplet_source_id, ref_doc_id, embedding, digest})
    -[:__REL__ {label, properties}]->(:__Node__)

``kind`` is ``"entity"`` or ``"chunk"``. ``properties`` is the LlamaIndex
property dict as one JSON object. Filters on its members use ``json_value``,
except ``triplet_source_id`` and ``ref_doc_id``, which LlamaIndex uses to find
the nodes of a chunk or document; the store copies those two into indexed
properties. ``embedding`` is a HawDB vector. ``digest`` hashes the other
properties, so an upsert finds the nodes that changed without reading them
back.

The store reads over read snapshots, where HawDB refuses a statement whose
result exceeds 64 MiB, so it reads nodes in pages. HawDB also refuses a
transaction whose WAL record would exceed 16 MiB, or that would take more than
100,000 mutation operations or version entries, so a large write or delete
commits in several transactions. Each write statement the store runs takes
time that grows with the stored graph, so each write transaction reads first
and then writes its rows with as few statements as it can. The store builds
its statements only from its own label, type and property names; LlamaIndex
labels, ids, names, filter keys and values bind as parameters.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
import threading
from collections.abc import Iterator, Sequence
from typing import Any

import hawdb
from llama_index.core.graph_stores.types import (
    TRIPLET_SOURCE_KEY,
    ChunkNode,
    EntityNode,
    LabelledNode,
    PropertyGraphStore,
    Relation,
    Triplet,
)
from llama_index.core.vector_stores.types import VectorStoreQuery

NODE_LABEL = "__Node__"
RELATION_TYPE = "__REL__"
ENTITY_KIND = "entity"
CHUNK_KIND = "chunk"

# ``json_value`` reads at most 1 MiB of JSON per document.
MAX_PROPERTIES_BYTES = 1 << 20

# ``get`` sizes each node with one expression over its id, kind, label, name,
# text and properties, and HawDB refuses an expression whose string functions
# read more than 10,000,000 bytes. So a chunk's text holds at most 8 MiB of
# UTF-8, and a node's id and label, or a relation's endpoint ids, at most
# 64 KiB each; an entity's name is its id, and an endpoint becomes an entity
# named by its id. With 1 MiB of properties, a chunk then sizes at most
# 9,568,261 bytes.
MAX_TEXT_BYTES = 8 << 20
MAX_FIELD_BYTES = 64 << 10

# A read by id takes at most this many ids per statement.
_PAGE_SIZE = 500

# A page of nodes read back holds at most this many characters of ids, kinds,
# labels, names, text and properties, at most 8 MiB of UTF-8, inside the 64 MiB
# a statement in a read snapshot may return. A larger node takes a page of its
# own.
_PAGE_CHARACTERS = 1 << 21

# HawDB logs a transaction as one WAL record of at most 16 MiB. A write that
# would need more commits in several transactions of at most this many
# estimated bytes. Half the record leaves room for new relation endpoints,
# which store each id twice, as their id and their name, where the estimate of
# a relation counts it once.
_TRANSACTION_BYTES = 8 << 20

# HawDB also refuses a transaction that would take more than 100,000 mutation
# operations or version entries. Rewriting a node takes nine operations, and a
# relation with two new endpoints five entries, so a write transaction holds at
# most this many rows.
_TRANSACTION_ROWS = 10_000

# Deleting a node takes one operation and up to three entries, and so does each
# relation deleted with it or by label, so a delete commits in transactions of
# at most this many nodes and relations.
_DELETE_ROWS = 30_000

# The binding passes a dict with this one key as a HawDB vector.
_VECTOR = "$vector"

_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1

# A ``json_value`` path is ``$.`` and one field name of at most 254 bytes.
_PROPERTY_KEY = re.compile(r"[A-Za-z0-9_-]{1,254}")

# LlamaIndex finds the nodes of a chunk or a document through these members, so
# the store also keeps them as indexed node properties of the same name.
_INDEXED_PROPERTIES = (TRIPLET_SOURCE_KEY, "ref_doc_id")

_REQUIRED_INDEXES = ("id", *_INDEXED_PROPERTIES)

_CATALOG_UNIQUE = (
    "SELECT property_name FROM system.constraints "
    "WHERE subject_kind = 'node' AND subject_name = $1 "
    "AND constraint_kind = 'node_property_unique'"
)
_CATALOG_INDEXES = (
    "SELECT property_names FROM system.indexes "
    "WHERE subject_kind = 'node' AND subject_name = $1 AND index_kind = 'equality'"
)
_CATALOG_COUNTS = (
    "SELECT statistic_kind, count FROM system.graph_statistics "
    "WHERE (statistic_kind = 'label_count' AND label_name = $1) "
    "OR (statistic_kind = 'relationship_type_count' "
    "AND relationship_type_name = $2)"
)
_CREATE_ID_CONSTRAINT = "CREATE CONSTRAINT ON :__Node__(id) ASSERT UNIQUE"
_CREATE_INDEX = {
    "id": "CREATE INDEX ON :__Node__(id)",
    "triplet_source_id": "CREATE INDEX ON :__Node__(triplet_source_id)",
    "ref_doc_id": "CREATE INDEX ON :__Node__(ref_doc_id)",
}

_NODE_COLUMNS = (
    "n.id AS id, n.kind AS kind, n.label AS label, n.name AS name, "
    "n.text AS text, n.properties AS properties"
)
# The characters of every column a node read returns, by which ``get`` pages.
_NODE_SIZE = (
    "coalesce(size(n.id), 0) + coalesce(size(n.kind), 0) + "
    "coalesce(size(n.label), 0) + coalesce(size(n.name), 0) + "
    "coalesce(size(n.text), 0) + coalesce(size(n.properties), 0)"
)
_NODES_BY_ID = f"MATCH (n:__Node__) WHERE n.id IN $ids RETURN {_NODE_COLUMNS}"
_STORED_DIGESTS = (
    "MATCH (n:__Node__) WHERE n.id IN $ids RETURN n.id AS id, n.digest AS digest"
)
_CREATE_NODES = (
    "UNWIND $rows AS row MERGE (n:__Node__ {id: row.id}) "
    "ON CREATE SET n.kind = row.kind, n.label = row.label, n.name = row.name, "
    "n.text = row.text, n.properties = row.properties, "
    "n.triplet_source_id = row.triplet_source_id, n.ref_doc_id = row.ref_doc_id, "
    "n.embedding = row.embedding, n.digest = row.digest"
)
# HawDB has no batched form of this rewrite, and it finds the node by scanning
# the stored nodes, so each changed node costs one such scan.
_UPDATE_NODE = (
    "MATCH (n:__Node__ {id: $id}) "
    "SET n.kind = $kind, n.label = $label, n.name = $name, n.text = $text, "
    "n.properties = $properties, n.triplet_source_id = $triplet_source_id, "
    "n.ref_doc_id = $ref_doc_id, n.embedding = $embedding, n.digest = $digest"
)
# A relation endpoint that is not stored yet becomes an entity named by its id,
# as in SimplePropertyGraphStore.
_CREATE_ENDPOINTS = (
    "UNWIND $rows AS row MERGE (n:__Node__ {id: row.id}) "
    "ON CREATE SET n.kind = 'entity', n.label = 'entity', n.name = row.id, "
    "n.properties = '{}'"
)
_CREATE_RELATIONS = (
    "UNWIND $rows AS row "
    "MATCH (s:__Node__ {id: row.source}) MATCH (t:__Node__ {id: row.target}) "
    "MERGE (s)-[r:__REL__ {label: row.label}]->(t) "
    "ON CREATE SET r.properties = row.properties"
)

_TRIPLET_COLUMNS = (
    "s.id AS s_id, s.kind AS s_kind, s.label AS s_label, s.name AS s_name, "
    "s.text AS s_text, s.properties AS s_properties, "
    "r.label AS r_label, r.properties AS r_properties, "
    "t.id AS t_id, t.kind AS t_kind, t.label AS t_label, t.name AS t_name, "
    "t.text AS t_text, t.properties AS t_properties"
)
# One breadth-first hop. Relationships to nodes expanded by an earlier hop were
# returned by that hop, so only the ones leading elsewhere come back.
_REL_MAP_HOP = (
    "MATCH (a:__Node__)-[r:__REL__]-(o:__Node__) "
    "WHERE a.id IN $frontier AND NOT o.id IN $expanded AND NOT r.label IN $ignore "
    "RETURN startNode(r).id AS s_id, startNode(r).kind AS s_kind, "
    "startNode(r).label AS s_label, startNode(r).name AS s_name, "
    "startNode(r).text AS s_text, startNode(r).properties AS s_properties, "
    "r.label AS r_label, r.properties AS r_properties, "
    "endNode(r).id AS t_id, endNode(r).kind AS t_kind, "
    "endNode(r).label AS t_label, endNode(r).name AS t_name, "
    "endNode(r).text AS t_text, endNode(r).properties AS t_properties, "
    "o.id AS next_id LIMIT $limit"
)

# The nodes a delete filter selects, each with the number of its relationships
# of any type, which DETACH DELETE removes with it.
_NODE_DEGREES = (
    "MATCH (n:__Node__) WHERE {where} OPTIONAL MATCH (n)-[r]-() "
    "RETURN n.id AS id, count(r) AS count"
)
# The store's nodes at the other end of a node's relationships, each with the
# number of relationships to it. A delete counts those it also deletes against
# its own set of ids: HawDB checks ``IN $list`` element by element, which took
# 19 s for a node with 30,000 relationships to selected nodes, and this 0.1 s.
_NEIGHBOURS = (
    "MATCH (n:__Node__ {id: $id})-[r]-(m:__Node__) RETURN m.id AS id, count(r) AS count"
)
_DELETE_NODES = "MATCH (n:__Node__) WHERE n.id IN $ids DETACH DELETE n"
# The sources of the relations a label filter selects, each with their number.
_RELATION_SOURCES = (
    "MATCH (s:__Node__)-[r:__REL__]->(:__Node__) WHERE r.label IN $labels "
    "RETURN s.id AS id, count(r) AS count"
)
_DELETE_RELATIONS = (
    "MATCH (s:__Node__)-[r:__REL__]->(:__Node__) "
    "WHERE s.id IN $ids AND r.label IN $labels DELETE r"
)

_SCHEMA_LABELS = "MATCH (n:__Node__) RETURN DISTINCT n.kind AS kind, n.label AS label"
_SCHEMA_RELATIONS = (
    "MATCH (s:__Node__)-[r:__REL__]->(t:__Node__) "
    "RETURN DISTINCT s.label AS source, r.label AS label, t.label AS target"
)

_WRONG_THREAD = (
    "HawDBPropertyGraphStore must be used on the thread that created it, because "
    "a hawdb.Database stays on the thread that opened it. LlamaIndex runs async "
    "retrieval on a worker thread when an event loop is already running, for "
    "example in Jupyter; pass use_async=False to PropertyGraphIndex or "
    "as_retriever() there."
)


class WrongThreadError(Exception):
    """A thread other than the store's creator called it.

    It is not a ``RuntimeError``: LlamaIndex's ``asyncio_run`` replaces a
    ``RuntimeError`` from its worker thread with an unrelated message.
    """


class HawDBPropertyGraphStore(PropertyGraphStore):
    """LlamaIndex property graph store over an open ``hawdb.Database``.

    The caller opens and closes the database; the store only runs statements on
    it. Opening the store creates the unique ``id`` constraint and the indexes
    it needs, and skips any the catalog already lists, so a read-only database
    the store has initialized before can be opened again.

    A ``hawdb.Database`` stays on the thread that opened it. Create the store on
    that thread and use it only there: another thread that calls the store gets
    ``WrongThreadError``. HawDB itself raises ``PanicException`` when a
    database is used on another thread, which is what creating the store, or
    using ``client``, on another thread raises.

    ``upsert_nodes`` replaces a stored node with the same id. It refuses a node
    whose id or label exceeds ``MAX_FIELD_BYTES`` of UTF-8, a chunk whose text
    exceeds ``MAX_TEXT_BYTES`` or a node whose properties exceed
    ``MAX_PROPERTIES_BYTES`` of JSON, so that ``get`` can read every node back.
    ``upsert_relations`` creates missing endpoints as entities named by their id,
    and refuses an endpoint id over ``MAX_FIELD_BYTES``. It keeps a relation
    that already exists with the same source, label and target, as
    ``SimplePropertyGraphStore`` does. A write call is one
    transaction unless it would exceed about 8 MiB of HawDB's 16 MiB WAL record
    or 10,000 nodes or relations; then it commits in several transactions, each
    holding whole nodes or relations, and a failure can leave the earlier ones
    written.

    Property filters compare the member with ``=``, so a key must name a JSON
    field (ASCII letters, digits, ``_`` or ``-``) and a value must be a string,
    number or boolean. HawDB compares JSON values by type, so ``3`` does not
    match ``3.0`` and ``True`` does not match ``1``. ``None`` for ``ids``,
    ``entity_names`` or ``relation_names`` means no filter; an empty list
    matches nothing. ``get_triplets`` returns relations between entities, like
    the Neo4j store.

    Vector queries are not supported, so ``PropertyGraphIndex`` keeps entity
    embeddings in its own vector store and passes entities to the store without
    them. The store saves the ``embedding`` of each node it is given, such as a
    chunk's, as a vector of 32-bit floats.
    """

    supports_structured_queries: bool = True
    supports_vector_queries: bool = False

    def __init__(self, database: hawdb.Database) -> None:
        if not isinstance(database, hawdb.Database):
            raise TypeError(
                f"database must be a hawdb.Database, not {type(database).__name__}"
            )
        self._db = database
        # Thread-local, so a later thread that reuses this thread's ident does
        # not pass for the owner.
        self._owner = threading.local()
        self._owner.created_here = True
        self._schema: dict[str, Any] | None = None
        self._create_indexes()

    @property
    def client(self) -> hawdb.Database:
        """The ``hawdb.Database`` the store runs statements on."""
        return self._db

    def get(
        self,
        properties: dict | None = None,
        ids: list[str] | None = None,
    ) -> list[LabelledNode]:
        """Get the nodes whose id is in ``ids`` and whose properties match.

        Over one snapshot, the store first selects the matching ids and their
        sizes, then reads the nodes in pages, so a large result, such as every
        chunk of a document, never comes back from one statement.
        """
        if ids is not None and len(ids) == 0:
            return []
        params: dict[str, Any] = {}
        predicates = [] if ids is None else ["n.id IN $ids"]
        if properties:
            predicates.extend(_property_predicates(properties, "n", params))
        where = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        select = f"MATCH (n:__Node__){where} RETURN n.id AS id, {_NODE_SIZE} AS size"
        nodes: list[LabelledNode] = []
        with self._database().read_transaction() as snapshot:
            if ids is None:
                matches = snapshot.execute(select, params).fetchall()
            else:
                matches = [
                    match
                    for page in _pages(list(dict.fromkeys(ids)))
                    for match in snapshot.execute(select, {**params, "ids": page})
                ]
            for page in _node_pages(matches):
                rows = snapshot.execute(_NODES_BY_ID, {"ids": page}).fetchall()
                nodes.extend(_node(row, "") for row in rows)
        return nodes

    def get_triplets(
        self,
        entity_names: list[str] | None = None,
        relation_names: list[str] | None = None,
        properties: dict | None = None,
        ids: list[str] | None = None,
    ) -> list[Triplet]:
        """Get the relations between entities that match every given filter.

        ``entity_names`` and ``ids`` match either end, and ``properties``
        matches when every member matches on the same end. The relations come
        back from one statement over a read snapshot.
        """
        if any(
            given is not None and len(given) == 0
            for given in (entity_names, relation_names, ids)
        ):
            return []
        params: dict[str, Any] = {}
        predicates = ["s.kind = 'entity'", "t.kind = 'entity'"]
        if entity_names:
            predicates.append("(s.name IN $names OR t.name IN $names)")
            params["names"] = list(entity_names)
        if relation_names:
            predicates.append("r.label IN $relations")
            params["relations"] = list(relation_names)
        if ids:
            predicates.append("(s.id IN $ids OR t.id IN $ids)")
            params["ids"] = list(ids)
        if properties:
            source = " AND ".join(_property_predicates(properties, "s", params))
            target = " AND ".join(_property_predicates(properties, "t", params))
            predicates.append(f"(({source}) OR ({target}))")
        statement = (
            "MATCH (s:__Node__)-[r:__REL__]->(t:__Node__) "
            f"WHERE {' AND '.join(predicates)} RETURN {_TRIPLET_COLUMNS}"
        )
        with self._database().read_transaction() as snapshot:
            rows = snapshot.execute(statement, params).fetchall()
        return [_triplet(row) for row in rows]

    def get_rel_map(
        self,
        graph_nodes: list[LabelledNode],
        depth: int = 2,
        limit: int = 30,
        ignore_rels: list[str] | None = None,
    ) -> list[Triplet]:
        """Get up to ``limit`` relations within ``depth`` hops of the nodes.

        The search runs one statement per hop over one read snapshot, follows
        relations in both directions and skips the labels in ``ignore_rels``.
        """
        frontier = list(dict.fromkeys(node.id for node in graph_nodes))
        ignore = list(ignore_rels or [])
        triplets: list[Triplet] = []
        seen: set[tuple[str, str, str]] = set()
        expanded: set[str] = set()
        database = self._database()
        with database.read_transaction() as snapshot:
            for _ in range(depth):
                remaining = limit - len(triplets)
                if not frontier or remaining <= 0:
                    break
                # Each relationship comes back at most twice, once from each
                # end, so twice the remainder holds enough distinct ones.
                rows = snapshot.execute(
                    _REL_MAP_HOP,
                    {
                        "frontier": frontier,
                        "expanded": sorted(expanded),
                        "ignore": ignore,
                        "limit": 2 * remaining,
                    },
                ).fetchall()
                expanded.update(frontier)
                next_frontier: dict[str, None] = {}
                for row in rows:
                    key = (row["s_id"], row["r_label"], row["t_id"])
                    if key not in seen and len(triplets) < limit:
                        seen.add(key)
                        triplets.append(_triplet(row))
                    if row["next_id"] not in expanded:
                        next_frontier[row["next_id"]] = None
                frontier = list(next_frontier)
        return triplets

    def upsert_nodes(self, nodes: Sequence[LabelledNode]) -> None:
        """Store the nodes, replacing a stored node with the same id.

        Each transaction reads the stored digests first and creates its new
        nodes in one statement. A stored node is rewritten only if its digest
        differs, with one statement per node, and each of those statements
        scans the stored nodes.
        """
        rows: dict[str, dict[str, Any]] = {}
        for node in nodes:
            row = _node_row(node)
            # The last node with an id wins, as in SimplePropertyGraphStore.
            rows.pop(row["id"], None)
            rows[row["id"]] = row
        database = self._database()
        for group in _transactions(list(rows.values())):
            with database.transaction() as tx:
                stored = {}
                for page in _pages([row["id"] for row in group]):
                    for record in tx.execute(_STORED_DIGESTS, {"ids": page}):
                        stored[record["id"]] = record["digest"]
                created = [row for row in group if row["id"] not in stored]
                changed = [
                    row
                    for row in group
                    if row["id"] in stored and stored[row["id"]] != row["digest"]
                ]
                if created:
                    tx.execute(_CREATE_NODES, {"rows": created})
                if changed:
                    tx.executemany(_UPDATE_NODE, changed)

    def upsert_relations(self, relations: list[Relation]) -> None:
        """Store the relations that do not exist yet, and their endpoints.

        Each transaction writes its endpoints in one statement and its relations
        in another.
        """
        rows: dict[tuple[str, str, str], dict[str, Any]] = {}
        for relation in relations:
            key = (relation.source_id, relation.label, relation.target_id)
            if key not in rows:
                for end, endpoint in (
                    ("source", relation.source_id),
                    ("target", relation.target_id),
                ):
                    _check_field(endpoint, f"{end} id of relation {_brief(key)}")
                rows[key] = {
                    "source": relation.source_id,
                    "label": relation.label,
                    "target": relation.target_id,
                    "properties": _dump(relation.properties, "relation", key),
                }
        database = self._database()
        for group in _transactions(list(rows.values())):
            endpoints = dict.fromkeys(
                endpoint for row in group for endpoint in (row["source"], row["target"])
            )
            with database.transaction() as tx:
                tx.execute(
                    _CREATE_ENDPOINTS,
                    {"rows": [{"id": endpoint} for endpoint in endpoints]},
                )
                tx.execute(_CREATE_RELATIONS, {"rows": group})

    def delete(
        self,
        entity_names: list[str] | None = None,
        relation_names: list[str] | None = None,
        properties: dict | None = None,
        ids: list[str] | None = None,
    ) -> None:
        """Delete the nodes and relations each given filter selects.

        Each filter deletes on its own: entities by name, relations by label,
        nodes by properties and nodes by id. A deleted node takes its relations
        with it. Over one snapshot, the store first selects the nodes, with the
        number of their relations, and the sources of the relations to delete
        by label. A delete is one transaction unless it would remove more than
        30,000 nodes and relations; then it commits in several transactions, and
        a failure can leave the earlier ones deleted.

        A node with more relations than one transaction removes goes last,
        after the other selected nodes have taken their relations to it with
        them, such as the chunks of a document whose placeholder entity has a
        ``SOURCE`` relation from each. If it would still take 30,000 or more
        relations with it, or a node is the source of more than 30,000
        relations to delete by label, the store refuses the call before
        anything is deleted.
        """
        selections: list[tuple[str, dict[str, Any]]] = [
            ("n.name IN $names", {"names": page})
            for page in _pages(list(dict.fromkeys(entity_names or [])))
        ]
        if properties:
            params: dict[str, Any] = {}
            predicates = _property_predicates(properties, "n", params)
            selections.append((" AND ".join(predicates), params))
        selections.extend(
            ("n.id IN $ids", {"ids": page})
            for page in _pages(list(dict.fromkeys(ids or [])))
        )
        labels = list(dict.fromkeys(relation_names or []))
        if not selections and not labels:
            return
        database = self._database()
        costs: dict[tuple[str, str], int] = {}
        with database.read_transaction() as snapshot:
            for where, params in selections:
                statement = _NODE_DEGREES.format(where=where)
                for row in snapshot.execute(statement, params):
                    costs["node", row["id"]] = 1 + row["count"]
            # A node with too many relations to delete in one transaction can
            # still go last: the selected nodes at the other end of its
            # relations take those relations with them first.
            hubs = [key for (_, key), cost in costs.items() if cost > _DELETE_ROWS]
            others = {key for _, key in costs}.difference(hubs)
            for hub in hubs:
                shared = sum(
                    row["count"]
                    for row in snapshot.execute(_NEIGHBOURS, {"id": hub})
                    if row["id"] in others
                )
                costs["node", hub] = costs.pop(("node", hub)) - shared
            if labels:
                for row in snapshot.execute(_RELATION_SOURCES, {"labels": labels}):
                    costs["source", row["id"]] = row["count"]
        for group in _delete_groups(costs):
            nodes = [key for kind, key in group if kind == "node"]
            sources = [key for kind, key in group if kind == "source"]
            with database.transaction() as tx:
                if nodes:
                    tx.execute(_DELETE_NODES, {"ids": nodes})
                if sources:
                    tx.execute(_DELETE_RELATIONS, {"ids": sources, "labels": labels})

    def structured_query(
        self, query: str, param_map: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Run the caller's Cypher with ``Database.execute`` and return its rows."""
        return self._database().execute(query, param_map or {}).fetchall()

    def vector_query(
        self, query: VectorStoreQuery, **kwargs: Any
    ) -> tuple[list[LabelledNode], list[float]]:
        """Not supported; ``PropertyGraphIndex`` uses its vector store instead."""
        raise NotImplementedError("HawDBPropertyGraphStore does not run vector queries")

    def get_schema(self, refresh: bool = False) -> dict[str, Any]:
        """Describe the stored graph, reading it again when ``refresh`` is true.

        The catalog lists only the store's own label and relationship type, so
        the LlamaIndex labels come from ``DISTINCT`` reads over the stored
        ``label`` properties, and the counts of the store's nodes and relations
        from ``system.graph_statistics``.
        """
        if refresh or self._schema is None:
            self._schema = self._read_schema()
        return self._schema

    def get_schema_str(self, refresh: bool = False) -> str:
        """Describe the stored graph for a text-to-Cypher prompt."""
        schema = self.get_schema(refresh=refresh)
        relations = ", ".join(
            f"({item['source']})-[{item['label']}]->({item['target']})"
            for item in schema["relations"]
        )
        return "\n".join(
            [
                (
                    "Every node has the label __Node__ and the properties id, kind "
                    "('entity' or 'chunk'), label, name (entities), text (chunks) "
                    "and properties (a JSON object; read a member with "
                    "json_value(n.properties, '$.key'))."
                ),
                (
                    "Every relationship has the type __REL__ and the properties "
                    "label and properties (a JSON object)."
                ),
                (
                    "Match a label as a property value, for example MATCH "
                    "(a:__Node__ {label: 'PERSON'})-[r:__REL__ {label: 'KNOWS'}]->"
                    "(b:__Node__) RETURN a.name, b.name."
                ),
                f"Entity labels: {', '.join(schema['entity_labels']) or 'none'}",
                f"Chunk labels: {', '.join(schema['chunk_labels']) or 'none'}",
                f"Relations: {relations or 'none'}",
            ]
        )

    async def aget_schema_str(self, refresh: bool = False) -> str:
        """Describe the stored graph for a text-to-Cypher prompt."""
        # The base class returns str() of the schema dict instead.
        return self.get_schema_str(refresh=refresh)

    def _database(self) -> hawdb.Database:
        # The database is unsendable: another thread would hit a Rust panic.
        if not getattr(self._owner, "created_here", False):
            raise WrongThreadError(_WRONG_THREAD)
        return self._db

    def _create_indexes(self) -> None:
        database = self._database()
        unique = {
            row["property_name"]
            for row in database.execute_sql(_CATALOG_UNIQUE, [NODE_LABEL])
        }
        indexed = {
            tuple(row["property_names"])
            for row in database.execute_sql(_CATALOG_INDEXES, [NODE_LABEL])
        }
        if "id" not in unique:
            database.execute(_CREATE_ID_CONSTRAINT)
        for name in _REQUIRED_INDEXES:
            if (name,) not in indexed:
                database.execute(_CREATE_INDEX[name])

    def _read_schema(self) -> dict[str, Any]:
        with self._database().read_transaction() as snapshot:
            labels = snapshot.execute(_SCHEMA_LABELS).fetchall()
            relations = snapshot.execute(_SCHEMA_RELATIONS).fetchall()
            counts = {
                row["statistic_kind"]: row["count"]
                for row in snapshot.execute_sql(
                    _CATALOG_COUNTS, [NODE_LABEL, RELATION_TYPE]
                )
            }
        return {
            "node_label": NODE_LABEL,
            "relation_type": RELATION_TYPE,
            "entity_labels": _sorted_labels(labels, ENTITY_KIND),
            "chunk_labels": _sorted_labels(labels, CHUNK_KIND),
            "relations": sorted(
                (dict(row) for row in relations),
                key=lambda item: (
                    str(item["source"]),
                    str(item["label"]),
                    str(item["target"]),
                ),
            ),
            "node_count": counts.get("label_count", 0),
            "relation_count": counts.get("relationship_type_count", 0),
        }


def _pages(items: list[Any]) -> Iterator[list[Any]]:
    for start in range(0, len(items), _PAGE_SIZE):
        yield items[start : start + _PAGE_SIZE]


def _transactions(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Group write rows into transactions by their estimated WAL bytes.

    The groups are built before anything is written, so a string that cannot
    be encoded fails the call before its first transaction.
    """
    groups: list[list[dict[str, Any]]] = []
    group: list[dict[str, Any]] = []
    size = 0
    for row in rows:
        row_size = _row_bytes(row)
        if group and (
            len(group) == _TRANSACTION_ROWS or size + row_size > _TRANSACTION_BYTES
        ):
            groups.append(group)
            group, size = [], 0
        group.append(row)
        size += row_size
    if group:
        groups.append(group)
    return groups


def _row_bytes(row: dict[str, Any]) -> int:
    # Measured: about one WAL byte per UTF-8 byte and four per vector
    # coordinate. The constant covers property names and record framing,
    # including that of a relation's new endpoints.
    size = 512
    for value in row.values():
        if isinstance(value, str):
            size += len(value.encode())
        elif isinstance(value, dict):
            size += 4 * len(value[_VECTOR])
    return size


def _delete_groups(costs: dict[tuple[str, str], int]) -> list[list[tuple[str, str]]]:
    """Group deleted nodes and relation sources by the rows they remove."""
    for (kind, key), cost in costs.items():
        if cost > _DELETE_ROWS:
            if kind == "node":
                what = (
                    f"would take {cost - 1} relations with it that no other "
                    "deleted node takes, which with the node exceed"
                )
            else:
                what = f"is the source of {cost} relations to delete, more than"
            raise ValueError(
                f"node {key!r} {what} the {_DELETE_ROWS} nodes and relations the "
                "store deletes in one HawDB transaction; delete some of those "
                "relations with structured_query first"
            )
    groups: list[list[tuple[str, str]]] = []
    group: list[tuple[str, str]] = []
    size = 0
    for item, cost in costs.items():
        if group and size + cost > _DELETE_ROWS:
            groups.append(group)
            group, size = [], 0
        group.append(item)
        size += cost
    if group:
        groups.append(group)
    return groups


def _node_pages(matches: list[dict[str, Any]]) -> Iterator[list[str]]:
    """Group matched ids by count and by the size of the nodes they name."""
    page: list[str] = []
    characters = 0
    for match in matches:
        if page and (
            len(page) == _PAGE_SIZE or characters + match["size"] > _PAGE_CHARACTERS
        ):
            yield page
            page, characters = [], 0
        page.append(match["id"])
        characters += match["size"]
    if page:
        yield page


def _property_predicates(
    properties: dict, variable: str, params: dict[str, Any]
) -> list[str]:
    """Return one predicate per member; keys and values become parameters."""
    predicates = []
    for index, (key, value) in enumerate(properties.items()):
        if not isinstance(key, str) or not _PROPERTY_KEY.fullmatch(key):
            raise ValueError(
                f"property filter key {key!r} must be 1 to 254 ASCII letters, "
                "digits, '_' or '-'"
            )
        if value is None or not isinstance(value, (str, int, float)):
            raise ValueError(
                f"property filter {key!r} must compare a string, number or "
                f"boolean, not {type(value).__name__}"
            )
        if isinstance(value, int) and not _INT64_MIN <= value <= _INT64_MAX:
            raise ValueError(
                f"property filter {key!r} must compare an integer within the "
                "signed 64-bit range"
            )
        params[f"value{index}"] = value
        if key in _INDEXED_PROPERTIES and isinstance(value, str):
            # The indexed copy holds string members only.
            predicates.append(f"{variable}.{key} = $value{index}")
        else:
            params[f"path{index}"] = f"$.{key}"
            predicates.append(
                f"json_value({variable}.properties, $path{index}) = $value{index}"
            )
    return predicates


def _dump(properties: dict, what: str, key: object) -> str:
    try:
        return json.dumps(
            properties, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"properties of {what} {key!r} must be JSON without NaN or infinity: "
            f"{error}"
        ) from error


def _check_field(value: str | None, what: str) -> None:
    if value is not None and len(value.encode()) > MAX_FIELD_BYTES:
        raise ValueError(
            f"{what} exceeds the {MAX_FIELD_BYTES} bytes of UTF-8 the store reads back"
        )


def _brief(value: object) -> str:
    """Return the repr of an id or key for a message, cut to 80 characters."""
    text = repr(value)
    return text if len(text) <= 80 else f"{text[:77]}..."


def _embedding(node: LabelledNode) -> tuple[dict[str, list[float]] | None, bytes]:
    """Return the node's embedding as a HawDB vector and its 32-bit bytes."""
    if not node.embedding:
        return None, b""
    coordinates = list(map(float, node.embedding))
    try:
        # Packing refuses a value that rounds past the 32-bit range, as HawDB does.
        stored = struct.pack(f"<{len(coordinates)}f", *coordinates)
    except OverflowError:
        stored = b""
    if not stored or not all(map(math.isfinite, coordinates)):
        raise ValueError(
            f"embedding of node {node.id!r} must hold finite 32-bit floats"
        )
    return {_VECTOR: coordinates}, stored


def _node_row(node: LabelledNode) -> dict[str, Any]:
    if isinstance(node, EntityNode):
        kind, name, text = ENTITY_KIND, node.name, None
    elif isinstance(node, ChunkNode):
        kind, name, text = CHUNK_KIND, None, node.text
        if len(text.encode()) > MAX_TEXT_BYTES:
            raise ValueError(
                f"text of chunk {node.id!r} exceeds the {MAX_TEXT_BYTES} bytes "
                "the store reads back"
            )
    else:
        raise TypeError(
            "HawDBPropertyGraphStore stores EntityNode and ChunkNode, "
            f"not {type(node).__name__}"
        )
    # An entity's id is its name, so this also bounds the name.
    _check_field(node.id, f"id of node {_brief(node.id)}")
    _check_field(node.label, f"label of node {_brief(node.id)}")
    properties = _dump(node.properties, "node", node.id)
    if len(properties.encode()) > MAX_PROPERTIES_BYTES:
        raise ValueError(
            f"properties of node {node.id!r} exceed the {MAX_PROPERTIES_BYTES} "
            "bytes json_value reads"
        )
    embedding, stored_embedding = _embedding(node)
    row = {
        "id": node.id,
        "kind": kind,
        "label": node.label,
        "name": name,
        "text": text,
        "properties": properties,
        "embedding": embedding,
    }
    for key in _INDEXED_PROPERTIES:
        value = node.properties.get(key)
        row[key] = value if isinstance(value, str) else None
    # The indexed copies derive from the properties, so the digest leaves them
    # out. It hashes the embedding as HawDB stores it, in 32-bit floats.
    content = [kind, node.label, name, text, properties]
    digest = hashlib.sha256(
        json.dumps(content, ensure_ascii=False, separators=(",", ":")).encode()
    )
    digest.update(stored_embedding)
    row["digest"] = digest.hexdigest()
    return row


def _load(document: str | None) -> dict[str, Any]:
    if document is None:
        return {}
    value = json.loads(document)
    if not isinstance(value, dict):
        raise ValueError(f"stored properties are not a JSON object: {document[:80]!r}")
    return value


def _node(row: dict[str, Any], prefix: str) -> LabelledNode:
    id_ = row[f"{prefix}id"]
    properties = _load(row[f"{prefix}properties"])
    label = row[f"{prefix}label"]
    if row[f"{prefix}kind"] == CHUNK_KIND:
        return ChunkNode(
            id_=id_,
            text=row[f"{prefix}text"] or "",
            label="text_chunk" if label is None else label,
            properties=properties,
        )
    name = row[f"{prefix}name"]
    return EntityNode(
        name=id_ if name is None else name,
        label="entity" if label is None else label,
        properties=properties,
    )


def _triplet(row: dict[str, Any]) -> Triplet:
    relation = Relation(
        label=row["r_label"],
        source_id=row["s_id"],
        target_id=row["t_id"],
        properties=_load(row["r_properties"]),
    )
    return (_node(row, "s_"), relation, _node(row, "t_"))


def _sorted_labels(rows: list[dict[str, Any]], kind: str) -> list[str]:
    # An empty label gives a text-to-Cypher prompt nothing to match on.
    return sorted(
        {row["label"] for row in rows if row["kind"] == kind and row["label"]}
    )
