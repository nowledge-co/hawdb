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

"""The knowledge graph of one RAGFlow dataset, in an embedded HawDB database.

RAGFlow's GraphRAG (``rag/graphrag`` in RAGFlow v0.27.3) extracts one
NetworkX subgraph per document, merges it into the dataset's graph, and keeps
the whole graph as one JSON chunk in its document engine. This store keeps that
graph in HawDB instead::

    (:Entity {name, seq, entity_type, description, source_id, attributes,
              stored_bytes})
    -[:RELATION {seq, weight, description, keywords, source_id, attributes,
                 stored_bytes}]->(:Entity)
    (:Document {doc_id, seq})
    (:RAGFlowGraph {key, version, next_seq, written, clearing})

``name`` is the NetworkX node, which RAGFlow keys by entity name. A graph edge
is undirected, so the store keeps one relationship per pair of entities,
directed from the name that sorts first, as RAGFlow's ``get_from_to`` orders
the pair. ``source_id`` is the list of documents RAGFlow records on each node
and edge. ``attributes`` holds every other node or edge attribute as one JSON
object, so a graph reads back with the attributes it was written with, except
``rank`` and ``pagerank``: RAGFlow derives those from the whole graph on every
merge, so the store keeps neither, and ``get_graph`` sets ``rank`` to the
degree as RAGFlow's merge does. ``Document`` nodes record which documents the
graph holds, in merge order, as RAGFlow's ``graph.graph["source_id"]``.
``seq`` keeps the order entities, relations and documents were added in.
``stored_bytes`` estimates a row's stored text, for paging. ``written``
estimates the bytes written since the last checkpoint.

Chunk text, chunk embeddings and the entity and relation chunks RAGFlow
retrieves stay in RAGFlow's document engine.

Every statement is parameterized: names, ids and values bind as parameters,
and statement text comes only from the constants below.

``merge_subgraph`` writes one extracted subgraph in one transaction, with the
merge rules of RAGFlow's ``graph_merge``. ``n_hop_paths`` replaces RAGFlow's
NetworkX ``n_neighbor`` walk with a bounded Cypher traversal.
``document_change`` names what documents contributed, so that RAGFlow can
write the chunks of a merge that committed here before RAGFlow failed.
``remove_document`` removes one document in one transaction; ``clear``
deletes the graph and ``drop`` the whole database.

HawDB refuses an intermediate row of more than 16 MiB, and a two-hop
traversal holds three entities and two relations in one, so the store refuses
an entity above ``MAX_ENTITY_BYTES`` and a relation above
``MAX_RELATION_BYTES``. HawDB also refuses a transaction whose WAL record
exceeds 16 MiB or that takes more than 100,000 version entries; a merge, a
document removal, or a single graph edit that needs more fails without writing
anything.

A store opened on a path opens the database for each call, on the calling
thread, and closes it before returning, so RAGFlow's thread pool and its API
and task executor processes can share one database. A process that has run a
statement on a HawDB database holds it until it closes it, so a call that
finds the database held by another process retries until ``busy_timeout``.
Each open loads the graph and replays the WAL written since the last
checkpoint, so such a store runs ``CHECKPOINT`` after it has written about
``_CHECKPOINT_BYTES`` since the last one, and after ``clear``.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import math
import os
import shutil
import threading
import time
import traceback
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import Any, TypeVar

import hawdb
import networkx as nx
from hawdb.exceptions import BranchError
from hawdb.exceptions import Error as HawDBError

logger = logging.getLogger(__name__)

# RAGFlow's GRAPH_FIELD_SEP, between the descriptions graph_merge joins.
GRAPH_FIELD_SEP = "<SEP>"

ENTITY_LABEL = "Entity"
RELATION_TYPE = "RELATION"
DOCUMENT_LABEL = "Document"
META_LABEL = "RAGFlowGraph"

# The layout version this module writes and reads.
FORMAT_VERSION = 1

# An entity may store at most this many bytes of UTF-8, and a relation at most
# MAX_RELATION_BYTES. A two-hop traversal holds three entities and two
# relations in one intermediate row: 3 * 4 MiB + 2 * 1 MiB stays inside the
# 16 MiB HawDB allows.
MAX_ENTITY_BYTES = 4 << 20
MAX_RELATION_BYTES = 1 << 20

# n_hop_paths returns at most this many paths for an entity by default, the
# heaviest first.
DEFAULT_MAX_PATHS = 1000

# A call on a path waits at most this many seconds for another process to
# close the database.
DEFAULT_BUSY_TIMEOUT = 60.0

# RAGFlow recomputes these node attributes from the whole graph.
DERIVED_NODE_ATTRIBUTES = frozenset({"rank", "pagerank"})

# The order in which RAGFlow's extractors write node and edge attributes.
_NODE_ORDER = ("entity_name", "entity_type", "description", "source_id")
_EDGE_ORDER = ("src_id", "tgt_id", "description", "keywords", "weight", "source_id")

# A statement that looks up entities or relations by name takes at most this
# many names or pairs.
_NAME_PAGE = 1000

# A page of entities or relations read back by name holds at most this many
# rows and estimated bytes, inside the 64 MiB a statement in a transaction
# returns.
_READ_PAGE_ROWS = 500
_READ_PAGE_BYTES = 8 << 20

# get_graph reads entities and relations in ranges of seq. Each range is one
# scan, so a range holds many rows: at most this many, and this many
# estimated bytes.
_RANGE_ROWS = 50_000
_RANGE_BYTES = 16 << 20

# A write statement takes at most this many rows and estimated bytes.
_STATEMENT_ROWS = 2000
_STATEMENT_BYTES = 4 << 20

# An edit that may span transactions, apply_change or import_graph, writes at
# most this many rows and estimated bytes per transaction, inside the 16 MiB
# WAL record and the 100,000 version entries of a HawDB transaction.
_TRANSACTION_ROWS = 5000
_TRANSACTION_BYTES = 8 << 20

# Deleting an entity, a relation or a document takes about three version
# entries, so clear() deletes at most this many per transaction.
_DELETE_ROWS = 25_000

# Every open replays the WAL written since the last checkpoint, and HawDB
# checkpoints by itself only when the WAL nears 70% of 1 GiB. A store on a
# path therefore runs CHECKPOINT once it has written about this many bytes
# since the last one, counting each row's stored bytes and _ROW_WRITE_BYTES.
_CHECKPOINT_BYTES = 16 << 20
_ROW_WRITE_BYTES = 256

# A file every HawDB database directory holds; drop() deletes only a directory
# that holds it.
_DATABASE_FILE = "manifest.hawdb"

_CONSTRAINTS = ((ENTITY_LABEL, "name"), (DOCUMENT_LABEL, "doc_id"), (META_LABEL, "key"))
_SQL_UNIQUE = (
    "SELECT property_name FROM system.constraints WHERE subject_kind = 'node' "
    "AND subject_name = $1 AND constraint_kind = 'node_property_unique'"
)
_SQL_INDEXES = (
    "SELECT property_names FROM system.indexes WHERE subject_kind = 'node' "
    "AND subject_name = $1 AND index_kind = 'equality'"
)

_META = (
    "MATCH (g:RAGFlowGraph {key: 'graph'}) "
    "RETURN g.version AS version, g.next_seq AS next_seq, g.written AS written, "
    "g.clearing AS clearing"
)
_CREATE_META = (
    "CREATE (:RAGFlowGraph {key: 'graph', version: $version, "
    "next_seq: $next_seq, written: $written, clearing: $clearing})"
)
_SET_META = (
    "MATCH (g:RAGFlowGraph {key: 'graph'}) "
    "SET g.next_seq = $next_seq, g.written = $written"
)
_SET_CLEARING = "MATCH (g:RAGFlowGraph {key: 'graph'}) SET g.clearing = $clearing"
_RESET_WRITTEN = "MATCH (g:RAGFlowGraph {key: 'graph'}) SET g.written = 0"
_DELETE_META = "MATCH (g:RAGFlowGraph) DELETE g"
_CHECKPOINT = "CHECKPOINT"

_DOCUMENTS = "MATCH (d:Document) RETURN d.doc_id AS doc_id, d.seq AS seq"
_DOCUMENTS_IN = (
    "MATCH (d:Document) WHERE d.doc_id IN $doc_ids RETURN d.doc_id AS doc_id"
)
_CREATE_DOCUMENTS = (
    "UNWIND $rows AS row CREATE (:Document {doc_id: row.doc_id, seq: row.seq})"
)
_DELETE_DOCUMENT = "MATCH (d:Document {doc_id: $doc_id}) DELETE d"
_DELETE_DOCUMENT_RANGE = (
    "MATCH (d:Document) WHERE d.seq >= $low AND d.seq <= $high DELETE d"
)

_ENTITY_COLUMNS = (
    "n.name AS name, n.seq AS seq, n.entity_type AS entity_type, "
    "n.description AS description, n.source_id AS source_id, "
    "n.attributes AS attributes, n.stored_bytes AS stored_bytes"
)
_ENTITY_LISTING = "MATCH (n:Entity) RETURN n.seq AS seq, n.stored_bytes AS stored_bytes"
_ENTITY_RANGE = (
    f"MATCH (n:Entity) WHERE n.seq >= $low AND n.seq <= $high RETURN {_ENTITY_COLUMNS}"
)
_ENTITY_SIZES_IN = (
    "MATCH (n:Entity) WHERE n.name IN $names "
    "RETURN n.name AS name, n.seq AS seq, n.stored_bytes AS stored_bytes"
)
_ENTITIES_IN = f"MATCH (n:Entity) WHERE n.name IN $names RETURN {_ENTITY_COLUMNS}"
_ENTITY_DEGREES_IN = (
    "MATCH (n:Entity) WHERE n.name IN $names OPTIONAL MATCH (n)-[r:RELATION]-(:Entity) "
    "RETURN n.name AS name, count(r) AS degree"
)
_CREATE_ENTITIES = (
    "UNWIND $rows AS row CREATE (:Entity {name: row.name, seq: row.seq, "
    "entity_type: row.entity_type, description: row.description, "
    "source_id: row.source_id, attributes: row.attributes, "
    "stored_bytes: row.stored_bytes})"
)
_UPDATE_ENTITIES = (
    "UNWIND $rows AS row MATCH (n:Entity {name: row.name}) "
    "SET n.entity_type = row.entity_type, n.description = row.description, "
    "n.source_id = row.source_id, n.attributes = row.attributes, "
    "n.stored_bytes = row.stored_bytes"
)
# HawDB checks every relationship of the graph for each node it deletes, so
# deleting entities costs time in proportion to the relations the graph still
# holds; clear() deletes every relation before any entity.
_DELETE_ENTITIES = "MATCH (n:Entity) WHERE n.name IN $names DETACH DELETE n"
_DELETE_ENTITY_RANGE = (
    "MATCH (n:Entity) WHERE n.seq >= $low AND n.seq <= $high DETACH DELETE n"
)

_RELATION_COLUMNS = (
    "s.name AS source, t.name AS target, r.seq AS seq, r.weight AS weight, "
    "r.description AS description, r.keywords AS keywords, "
    "r.source_id AS source_id, r.attributes AS attributes, "
    "r.stored_bytes AS stored_bytes"
)
_RELATION_LISTING = (
    "MATCH (:Entity)-[r:RELATION]->(:Entity) "
    "RETURN r.seq AS seq, r.stored_bytes AS stored_bytes"
)
# HawDB 0.36.0 evaluates a lower bound on a relationship property, such as
# r.seq >= $low, in time that grows with the relations it rejects and the size
# of the graph: on a graph of 60,000 entities and 120,000 relations, a range
# that starts halfway took 26 seconds. NOT r.seq < $low selects the same
# relations in a fraction of a second, so relation ranges use it.
_RELATION_RANGE = (
    "MATCH (s:Entity)-[r:RELATION]->(t:Entity) "
    "WHERE NOT r.seq < $low AND r.seq <= $high "
    f"RETURN {_RELATION_COLUMNS}"
)
_RELATION_SIZES_FROM = (
    "MATCH (s:Entity)-[r:RELATION]->(t:Entity) WHERE s.name IN $names "
    "RETURN s.name AS source, t.name AS target, r.seq AS seq, "
    "r.stored_bytes AS stored_bytes"
)
_RELATION_SIZES_TO = (
    "MATCH (s:Entity)-[r:RELATION]->(t:Entity) WHERE t.name IN $names "
    "RETURN s.name AS source, t.name AS target, r.seq AS seq, "
    "r.stored_bytes AS stored_bytes"
)
_RELATION_SIZES_BETWEEN = (
    "UNWIND $pairs AS pair MATCH (s:Entity {name: pair.source}) "
    "MATCH (s)-[r:RELATION]->(t:Entity) WHERE t.name = pair.target "
    "RETURN s.name AS source, t.name AS target, r.seq AS seq, "
    "r.stored_bytes AS stored_bytes"
)
_RELATIONS_BETWEEN = (
    "UNWIND $pairs AS pair MATCH (s:Entity {name: pair.source}) "
    "MATCH (s)-[r:RELATION]->(t:Entity) WHERE t.name = pair.target "
    f"RETURN {_RELATION_COLUMNS}"
)
_CREATE_RELATIONS = (
    "UNWIND $rows AS row MATCH (s:Entity {name: row.source}) "
    "MATCH (t:Entity {name: row.target}) "
    "CREATE (s)-[:RELATION {seq: row.seq, weight: row.weight, "
    "description: row.description, keywords: row.keywords, "
    "source_id: row.source_id, attributes: row.attributes, "
    "stored_bytes: row.stored_bytes}]->(t)"
)
_UPDATE_RELATIONS = (
    "UNWIND $rows AS row MATCH (s:Entity {name: row.source}) "
    "MATCH (s)-[r:RELATION]->(t:Entity) WHERE t.name = row.target "
    "SET r.weight = row.weight, r.description = row.description, "
    "r.keywords = row.keywords, r.source_id = row.source_id, "
    "r.attributes = row.attributes, r.stored_bytes = row.stored_bytes"
)
# HawDB deletes a relationship only through a pattern filtered by property
# equality or comparison, not by a list of pairs, so the store marks the pairs
# it deletes and then deletes the marked relations, in one transaction.
_MARK_RELATIONS = (
    "UNWIND $rows AS row MATCH (s:Entity {name: row.source}) "
    "MATCH (s)-[r:RELATION]->(t:Entity) WHERE t.name = row.target SET r.doomed = true"
)
_DELETE_MARKED_RELATIONS = (
    "MATCH (:Entity)-[r:RELATION]->(:Entity) WHERE r.doomed = true DELETE r"
)
_DELETE_RELATION_RANGE = (
    "MATCH (:Entity)-[r:RELATION]->(:Entity) "
    "WHERE NOT r.seq < $low AND r.seq <= $high DELETE r"
)

_ENTITIES_OF_DOCUMENT = (
    "MATCH (n:Entity) WHERE $doc_id IN n.source_id "
    "RETURN n.name AS name, size([x IN n.source_id WHERE x <> $doc_id]) AS remaining"
)
_RELATIONS_OF_DOCUMENT = (
    "MATCH (s:Entity)-[r:RELATION]->(t:Entity) WHERE $doc_id IN r.source_id "
    "RETURN s.name AS source, t.name AS target, "
    "size([x IN r.source_id WHERE x <> $doc_id]) AS remaining"
)
_STRIP_ENTITIES = (
    "MATCH (n:Entity) WHERE $doc_id IN n.source_id "
    "SET n.source_id = [x IN n.source_id WHERE x <> $doc_id]"
)
_STRIP_RELATIONS = (
    "MATCH (:Entity)-[r:RELATION]->(:Entity) WHERE $doc_id IN r.source_id "
    "SET r.source_id = [x IN r.source_id WHERE x <> $doc_id]"
)
# The store never writes an empty source_id, so after a document leaves every
# source_id, the relations it leaves empty are those this deletes.
_DELETE_EMPTY_RELATIONS = (
    "MATCH (:Entity)-[r:RELATION]->(:Entity) WHERE r.source_id = $empty DELETE r"
)

# The paths of n_hop_paths, as RAGFlow's n_neighbor walks them: a path never
# steps straight back over the node it came from, and stops at a node it has
# already visited. The heaviest paths come first, then by name.
_PATHS = {
    1: (
        "MATCH (a:Entity {name: $name})-[r1:RELATION]-(b:Entity) "
        "RETURN b.name AS b, r1.weight AS w1 "
        "ORDER BY coalesce(w1, 0) DESC, b "
        "LIMIT $limit"
    ),
    2: (
        "MATCH (a:Entity {name: $name})-[r1:RELATION]-(b:Entity) "
        "OPTIONAL MATCH (b)-[r2:RELATION]-(c:Entity) "
        "WHERE b.name <> $name AND c.name <> $name "
        "RETURN b.name AS b, r1.weight AS w1, c.name AS c, r2.weight AS w2 "
        "ORDER BY coalesce(w1, 0) + coalesce(w2, 0) DESC, b, c "
        "LIMIT $limit"
    ),
}

_WRONG_THREAD = (
    "a HawDBGraphStore over an open hawdb.Database runs on the thread that "
    "opened the database; open the store on a path to call it from any thread"
)

_T = TypeVar("_T")


class WrongThreadError(RuntimeError):
    """A store over an open database was called from another thread."""


class StoreBusyError(TimeoutError):
    """Another process held the database for longer than ``busy_timeout``."""


class StoreFormatError(RuntimeError):
    """The database holds a layout this store does not read, or an unfinished clear."""


@dataclasses.dataclass
class GraphChange:
    """What a call changed, with the fields of RAGFlow's ``GraphChange``.

    Edges are ``(lesser name, greater name)`` pairs, as RAGFlow's
    ``get_from_to`` orders them.
    """

    removed_nodes: set[str] = dataclasses.field(default_factory=set)
    added_updated_nodes: set[str] = dataclasses.field(default_factory=set)
    removed_edges: set[tuple[str, str]] = dataclasses.field(default_factory=set)
    added_updated_edges: set[tuple[str, str]] = dataclasses.field(default_factory=set)


class HawDBGraphStore:
    """The knowledge graph of one RAGFlow dataset in one HawDB database.

    ``database`` is a path, or an open ``hawdb.Database`` the store uses as it
    is and never closes. A store on a path opens the database for each call
    and creates it on the first write. A store on an open database runs only
    on the thread that opened it, since a ``hawdb.Database`` is bound to that
    thread.

    ``max_paths`` bounds the paths ``n_hop_paths`` returns for one entity.
    ``busy_timeout`` is how long a call on a path waits for another process to
    close the database.
    """

    def __init__(
        self,
        database: hawdb.Database | str | os.PathLike[str],
        *,
        max_paths: int = DEFAULT_MAX_PATHS,
        busy_timeout: float = DEFAULT_BUSY_TIMEOUT,
    ) -> None:
        _check_positive_int(max_paths, "max_paths")
        if isinstance(busy_timeout, bool) or not isinstance(busy_timeout, (int, float)):
            raise TypeError("busy_timeout must be a number of seconds")
        if not busy_timeout >= 0:
            raise ValueError("busy_timeout must not be negative")
        self._max_paths = max_paths
        self._busy_timeout = float(busy_timeout)
        if isinstance(database, hawdb.Database):
            self._database: hawdb.Database | None = database
            self._path: str | None = None
            self._owner = threading.local()
            self._owner.created_here = True
            self._schema_ready = False
        elif isinstance(database, (str, os.PathLike)):
            path = os.fspath(database)
            if not isinstance(path, str) or not path:
                raise ValueError("the database path must be a non-empty string path")
            self._database = None
            self._path = os.path.abspath(path)
        else:
            raise TypeError("database must be a path or a hawdb.Database")

    def __repr__(self) -> str:
        target = "an open database" if self._path is None else repr(self._path)
        return f"HawDBGraphStore({target})"

    @property
    def path(self) -> str | None:
        """The database path, or None for a store over an open database."""
        return self._path

    def merge_subgraph(self, subgraph: nx.Graph) -> GraphChange:
        """Merge one extracted subgraph in one transaction.

        ``subgraph`` is what RAGFlow's ``generate_subgraph`` returns: nodes
        named by entity, each with a ``description`` string and a
        ``source_id`` list, edges with the same two attributes and usually a
        ``weight`` and ``keywords``, and the extracted documents in
        ``subgraph.graph["source_id"]``. The merge follows RAGFlow's
        ``graph_merge``: a new entity or relation is added as it is; an
        existing entity appends the new description after ``<SEP>`` and the
        new ``source_id`` entries, and keeps its other attributes; an existing
        relation also adds the new weight and appends the new keywords.

        A subgraph whose documents the graph already holds is not merged
        again, and the call returns an empty change. Otherwise it returns
        every node and edge of the subgraph as added or updated, as
        ``graph_merge`` reports them.
        """
        documents = _documents_of(subgraph)
        entities = _subgraph_entities(subgraph)
        relations = _subgraph_relations(subgraph, entities)
        change = GraphChange(
            added_updated_nodes=set(entities),
            added_updated_edges=set(relations),
        )

        def write(database: hawdb.Database) -> tuple[GraphChange, int]:
            with database.transaction() as tx:
                meta = _meta(tx)
                _check_meta(meta)
                present = [
                    row["doc_id"]
                    for row in tx.execute(
                        _DOCUMENTS_IN, {"doc_ids": documents}
                    ).fetchall()
                ]
                if present:
                    if set(present) == set(documents):
                        return GraphChange(), 0
                    raise ValueError(
                        "the graph already holds some documents of this subgraph: "
                        + ", ".join(sorted(_brief(doc) for doc in present))
                    )
                next_seq = meta["next_seq"] if meta else 0
                stored = _read_entities(tx, list(entities))
                entity_creates, entity_updates = [], []
                for name, attrs in entities.items():
                    old = stored.get(name)
                    if old is None:
                        row = _entity_row(name, attrs)
                        row["seq"] = next_seq
                        next_seq += 1
                        entity_creates.append(row)
                        continue
                    merged = _node_attributes(old)
                    merged["description"] += GRAPH_FIELD_SEP + attrs["description"]
                    merged["source_id"] = merged["source_id"] + list(attrs["source_id"])
                    row = _entity_row(name, merged)
                    row["seq"] = old["seq"]
                    entity_updates.append(row)
                existing = _read_relations(tx, list(relations))
                relation_creates, relation_updates = [], []
                for pair, attrs in relations.items():
                    old = existing.get(pair)
                    if old is None:
                        row = _relation_row(pair, attrs)
                        row["seq"] = next_seq
                        next_seq += 1
                        relation_creates.append(row)
                        continue
                    merged = _edge_attributes(old)
                    weight = merged.get("weight", 0)
                    keywords = merged.get("keywords", [])
                    try:
                        merged["weight"] = weight + attrs.get("weight", 0)
                        merged["keywords"] = keywords + attrs.get("keywords", [])
                    except TypeError as error:
                        raise ValueError(
                            f"cannot merge relation {_brief(pair)}: {error}"
                        ) from None
                    merged["description"] += GRAPH_FIELD_SEP + attrs["description"]
                    merged["source_id"] = merged["source_id"] + list(attrs["source_id"])
                    row = _relation_row(pair, merged)
                    row["seq"] = old["seq"]
                    relation_updates.append(row)
                document_rows = []
                for doc in documents:
                    document_rows.append({"doc_id": doc, "seq": next_seq})
                    next_seq += 1
                rows = (
                    entity_creates
                    + entity_updates
                    + relation_creates
                    + relation_updates
                    + document_rows
                )
                _execute_rows(tx, _CREATE_ENTITIES, entity_creates)
                _execute_rows(tx, _UPDATE_ENTITIES, entity_updates)
                _execute_rows(tx, _CREATE_RELATIONS, relation_creates)
                _execute_rows(tx, _UPDATE_RELATIONS, relation_updates)
                _execute_rows(tx, _CREATE_DOCUMENTS, document_rows)
                written = _update_meta(tx, meta, next_seq, _written(rows))
            return change, written

        return self._write(write)

    def apply_change(self, graph: nx.Graph, change: Any) -> None:
        """Write a change RAGFlow made to the whole graph, such as entity resolution.

        ``change`` has the fields of RAGFlow's ``GraphChange``. Every node and
        edge it names is written as ``graph`` now has it, or deleted if
        ``graph`` no longer has it, and so is every edge of a node it names,
        including edges RAGFlow moved onto a merged node without naming them.
        The graph's documents stay as they are.

        An edit that fits commits in one transaction. A larger one commits in
        several, writing entities and relations before deleting any, and
        calling again with the same graph and change completes it.
        """
        removed_nodes = _names(change.removed_nodes, "removed_nodes")
        updated_nodes = _names(change.added_updated_nodes, "added_updated_nodes")
        removed_edges = _pairs(change.removed_edges, "removed_edges")
        updated_edges = _pairs(change.added_updated_edges, "added_updated_edges")
        self._apply(
            graph,
            removed_nodes | updated_nodes,
            removed_edges | updated_edges,
            documents=None,
        )

    def import_graph(self, graph: nx.Graph) -> None:
        """Write a whole RAGFlow graph, such as one kept in RAGFlow's document engine.

        The store must not hold any document yet. Nodes and edges without a
        ``description`` or a ``source_id`` are skipped, as RAGFlow's
        ``tidy_graph`` purges them before a merge, and so are those whose
        ``source_id`` is empty, as RAGFlow deletes their chunks when it next
        removes a document. The documents in
        ``graph.graph["source_id"]`` are written last, so an import that
        stops early holds no document, and importing again completes it.
        """
        documents = list(
            dict.fromkeys(
                _string_list(
                    graph.graph.get("source_id", []), "graph.graph['source_id']"
                )
            )
        )
        tidy = nx.Graph()
        tidy.graph.update(graph.graph)
        skipped = 0
        for name, attrs in graph.nodes(data=True):
            if "description" in attrs and attrs.get("source_id"):
                tidy.add_node(name, **attrs)
            else:
                skipped += 1
        for source, target, attrs in graph.edges(data=True):
            if (
                tidy.has_node(source)
                and tidy.has_node(target)
                and "description" in attrs
                and attrs.get("source_id")
            ):
                tidy.add_edge(source, target, **{"keywords": [], **attrs})
            else:
                skipped += 1
        if skipped:
            logger.warning(
                "import_graph skipped %d nodes and edges without a description "
                "or a document",
                skipped,
            )
        self._apply(
            tidy,
            set(_names(tidy.nodes, "graph nodes")),
            {_pair(source, target) for source, target in tidy.edges},
            documents=documents,
        )

    def n_hop_paths(
        self,
        names: Iterable[str],
        *,
        n_hop: int = 2,
        max_paths: int | None = None,
    ) -> dict[str, list[dict[str, list[Any]]]]:
        """The paths of up to ``n_hop`` edges from each entity, like ``n_neighbor``.

        Each path is ``{"path": [name, ...], "weights": [...]}``, with one
        weight per edge, 0 for an edge without one: the structure RAGFlow
        stores as an entity chunk's ``n_hop_with_weight``. A path never steps
        straight back over the node it came from, ends where it reaches a node
        it has already visited, and ends early at a node with no other edge.

        One bounded statement per entity returns at most ``max_paths`` paths,
        the store's default if None, the heaviest first, then by name.
        RAGFlow's walk keeps every path. An entity the graph does not hold has
        no paths. ``n_hop`` is 1 or 2; RAGFlow walks 2.
        """
        if isinstance(n_hop, bool) or n_hop not in _PATHS:
            raise ValueError("n_hop must be 1 or 2")
        limit = self._max_paths if max_paths is None else max_paths
        _check_positive_int(limit, "max_paths")
        wanted = list(dict.fromkeys(_names(list(names), "names", ordered=True)))
        statement = _PATHS[n_hop]

        def read(snapshot: Any) -> dict[str, list[dict[str, list[Any]]]]:
            _check_meta(_meta(snapshot))
            paths: dict[str, list[dict[str, list[Any]]]] = {}
            for name in wanted:
                rows = snapshot.execute(
                    statement, {"name": name, "limit": limit + 1}
                ).fetchall()
                if len(rows) > limit:
                    logger.warning(
                        "entity %s has more than %d paths of %d hops; "
                        "kept the %d heaviest",
                        _brief(name),
                        limit,
                        n_hop,
                        limit,
                    )
                    rows = rows[:limit]
                paths[name] = [_path(name, row) for row in rows]
            return paths

        return self._read(read, empty=lambda: {name: [] for name in wanted})

    def get_graph(self) -> nx.Graph | None:
        """The whole graph, as RAGFlow's ``get_graph`` returns it, or None if empty.

        Nodes and edges come in the order they were added, with the
        attributes they were written with, and ``rank`` set to each node's
        degree. ``graph.graph["source_id"]`` lists the documents in merge
        order. The graph is read in ranges of ``seq``, inside one read
        snapshot.
        """

        def read(snapshot: Any) -> nx.Graph | None:
            _check_meta(_meta(snapshot))
            documents = _document_ids(snapshot)
            entities = sorted(snapshot.execute(_ENTITY_LISTING).fetchall(), key=_seq)
            if not documents and not entities:
                return None
            graph = nx.Graph()
            graph.graph["source_id"] = documents
            for low, high in _seq_ranges(entities, _RANGE_ROWS, _RANGE_BYTES):
                rows = snapshot.execute(
                    _ENTITY_RANGE, {"low": low, "high": high}
                ).fetchall()
                for row in sorted(rows, key=_seq):
                    graph.add_node(row["name"], **_node_attributes(row))
            relations = sorted(snapshot.execute(_RELATION_LISTING).fetchall(), key=_seq)
            for low, high in _seq_ranges(relations, _RANGE_ROWS, _RANGE_BYTES):
                rows = snapshot.execute(
                    _RELATION_RANGE, {"low": low, "high": high}
                ).fetchall()
                for row in sorted(rows, key=_seq):
                    graph.add_edge(
                        row["source"], row["target"], **_edge_attributes(row)
                    )
            for name, degree in graph.degree:
                graph.nodes[name]["rank"] = int(degree)
            return graph

        return self._read(read, empty=lambda: None)

    def document_ids(self) -> list[str]:
        """The documents the graph holds, in merge order."""

        def read(snapshot: Any) -> list[str]:
            _check_meta(_meta(snapshot))
            return _document_ids(snapshot)

        return self._read(read, empty=list)

    def contains_document(self, doc_id: str) -> bool:
        """Whether the graph holds ``doc_id``, as RAGFlow's ``does_graph_contains``."""
        _check_text(doc_id, "doc_id")

        def read(snapshot: Any) -> bool:
            _check_meta(_meta(snapshot))
            return bool(
                snapshot.execute(_DOCUMENTS_IN, {"doc_ids": [doc_id]}).fetchall()
            )

        return self._read(read, empty=lambda: False)

    def document_change(self, doc_ids: Iterable[str]) -> GraphChange:
        """Every entity and relation whose ``source_id`` names one of ``doc_ids``.

        The change names them as added or updated. RAGFlow writes the entity
        and relation chunks of what a change names: if a merge committed here
        and RAGFlow failed before it wrote those chunks, this change names
        them again, so they can be written.
        """
        wanted = list(dict.fromkeys(_names(list(doc_ids), "doc_ids", ordered=True)))

        def read(snapshot: Any) -> GraphChange:
            _check_meta(_meta(snapshot))
            change = GraphChange()
            for doc_id in wanted:
                for row in snapshot.execute(
                    _ENTITIES_OF_DOCUMENT, {"doc_id": doc_id}
                ).fetchall():
                    change.added_updated_nodes.add(row["name"])
                for row in snapshot.execute(
                    _RELATIONS_OF_DOCUMENT, {"doc_id": doc_id}
                ).fetchall():
                    change.added_updated_edges.add((row["source"], row["target"]))
            return change

        return self._read(read, empty=GraphChange)

    def remove_document(self, doc_id: str) -> GraphChange:
        """Remove one document from the graph in one transaction.

        As RAGFlow removes a document from its entity and relation chunks,
        ``doc_id`` leaves every ``source_id``, and an entity or relation left
        without one is deleted, with the relations of a deleted entity.
        Descriptions and weights keep what the document contributed, as they
        do in the graph RAGFlow rebuilds after a removal.
        """
        _check_text(doc_id, "doc_id")

        def write(database: hawdb.Database) -> tuple[GraphChange, int]:
            with database.transaction() as tx:
                meta = _meta(tx)
                _check_meta(meta)
                entities = tx.execute(
                    _ENTITIES_OF_DOCUMENT, {"doc_id": doc_id}
                ).fetchall()
                relations = tx.execute(
                    _RELATIONS_OF_DOCUMENT, {"doc_id": doc_id}
                ).fetchall()
                present = bool(
                    tx.execute(_DOCUMENTS_IN, {"doc_ids": [doc_id]}).fetchall()
                )
                if not entities and not relations and not present:
                    return GraphChange(), 0
                emptied = sorted(
                    row["name"] for row in entities if row["remaining"] == 0
                )
                change = GraphChange(
                    removed_nodes=set(emptied),
                    added_updated_nodes={row["name"] for row in entities}
                    - set(emptied),
                )
                # A deleted entity takes its relations with it, including
                # any the document did not name.
                change.removed_edges = _incident_relations(tx, emptied)
                for row in relations:
                    pair = (row["source"], row["target"])
                    if row["remaining"] == 0:
                        change.removed_edges.add(pair)
                    elif pair not in change.removed_edges:
                        change.added_updated_edges.add(pair)
                if entities:
                    tx.execute(_STRIP_ENTITIES, {"doc_id": doc_id})
                if relations:
                    tx.execute(_STRIP_RELATIONS, {"doc_id": doc_id})
                if any(row["remaining"] == 0 for row in relations):
                    tx.execute(_DELETE_EMPTY_RELATIONS, {"empty": []})
                for names in _pages(emptied, _NAME_PAGE):
                    tx.execute(_DELETE_ENTITIES, {"names": names})
                if present:
                    tx.execute(_DELETE_DOCUMENT, {"doc_id": doc_id})
                touched = len(entities) + len(relations) + len(change.removed_edges) + 1
                written = _update_meta(
                    tx,
                    meta,
                    meta["next_seq"] if meta else 0,
                    _ROW_WRITE_BYTES * touched,
                )
            return change, written

        if self._path is not None and not os.path.exists(self._path):
            return GraphChange()
        return self._write(write)

    def clear(self) -> None:
        """Delete the whole graph.

        A large graph is deleted over several transactions: relations first,
        then entities, then documents. The store first marks the graph as
        being cleared, so until a later call to ``clear`` finishes, every other
        call raises ``StoreFormatError`` instead of reading or extending a
        partly deleted graph.
        """

        def write(database: hawdb.Database) -> tuple[None, int]:
            with database.transaction() as tx:
                meta = _meta(tx)
                if meta is not None and meta["version"] != FORMAT_VERSION:
                    raise StoreFormatError(_version_message(meta["version"]))
                if meta is None:
                    tx.execute(
                        _CREATE_META,
                        {
                            "version": FORMAT_VERSION,
                            "next_seq": 0,
                            "written": 0,
                            "clearing": True,
                        },
                    )
                elif not meta["clearing"]:
                    tx.execute(_SET_CLEARING, {"clearing": True})
                relations = sorted(tx.execute(_RELATION_LISTING).fetchall(), key=_seq)
                entities = sorted(tx.execute(_ENTITY_LISTING).fetchall(), key=_seq)
                documents = sorted(tx.execute(_DOCUMENTS).fetchall(), key=_seq)
            for listing, statement in (
                (relations, _DELETE_RELATION_RANGE),
                (entities, _DELETE_ENTITY_RANGE),
                (documents, _DELETE_DOCUMENT_RANGE),
            ):
                for low, high in _seq_ranges(listing, _DELETE_ROWS, None):
                    with database.transaction() as tx:
                        tx.execute(statement, {"low": low, "high": high})
            with database.transaction() as tx:
                tx.execute(_DELETE_META)
            # Checkpoint after deleting a graph, so that later opens do not
            # replay the deletes.
            deleted = len(relations) + len(entities) + len(documents)
            return None, _CHECKPOINT_BYTES if deleted else 0

        if self._path is not None and not os.path.exists(self._path):
            return
        self._write(write)

    def drop(self) -> None:
        """Delete the database on the store's path, with every file in it.

        RAGFlow drops a dataset's store when it deletes the dataset, whereas
        ``clear`` deletes the graph and keeps the database. A process that
        still has the database open fails on its next call. The store deletes
        only a directory that holds the ``manifest.hawdb`` file of a HawDB
        database.
        """
        if self._path is None:
            raise TypeError("only a store opened on a path can drop its database")
        with _path_lock(self._path):
            if not os.path.lexists(self._path):
                return
            if os.path.islink(self._path) or not os.path.isfile(
                os.path.join(self._path, _DATABASE_FILE)
            ):
                raise StoreFormatError(
                    f"{self._path} is not a HawDB database, "
                    "so the store does not delete it"
                )
            shutil.rmtree(self._path)

    def _apply(
        self,
        graph: nx.Graph,
        names: set[str],
        pairs: set[tuple[str, str]],
        *,
        documents: list[str] | None,
    ) -> None:
        upsert_names = sorted(name for name in names if graph.has_node(name))
        delete_names = sorted(name for name in names if not graph.has_node(name))
        incident = {
            _pair(name, other)
            for name in upsert_names
            for other in graph.neighbors(name)
        }
        upsert_pairs = sorted(
            {pair for pair in pairs if graph.has_edge(*pair)} | incident
        )
        entity_rows = {
            name: _entity_row(name, graph.nodes[name]) for name in upsert_names
        }
        relation_rows = {
            pair: _relation_row(pair, graph.edges[pair]) for pair in upsert_pairs
        }

        def write(database: hawdb.Database) -> tuple[None, int]:
            with database.transaction() as tx:
                meta = _meta(tx)
                _check_meta(meta)
                if documents is not None and _document_ids(tx):
                    raise ValueError(
                        "import_graph needs a store that holds no document"
                    )
                endpoints = sorted(
                    {name for pair in upsert_pairs for name in pair} - set(entity_rows)
                )
                stored = {
                    row["name"]: row
                    for row in _entity_sizes(
                        tx, upsert_names + delete_names + endpoints
                    )
                }
                for name in endpoints:
                    if name not in stored:
                        if not graph.has_node(name):
                            raise ValueError(
                                f"edge endpoint {_brief(name)} is in neither "
                                "the graph nor the store"
                            )
                        entity_rows[name] = _entity_row(name, graph.nodes[name])
                existing = {
                    (row["source"], row["target"]): row
                    for row in _relation_sizes(tx, sorted(set(upsert_pairs) | pairs))
                }
                doomed = {name: 0 for name in delete_names if name in stored}
                for row in _entity_degrees(tx, list(doomed)):
                    doomed[row["name"]] = row["degree"]
                delete_pairs = sorted(
                    pair
                    for pair in (
                        {pair for pair in pairs if pair in existing}
                        | _incident_relations(tx, list(entity_rows))
                    )
                    if pair not in relation_rows and not doomed.keys() & set(pair)
                )
                next_seq = meta["next_seq"] if meta else 0
                steps: list[tuple[str, list[dict[str, Any]]]] = []
                creates, updates = [], []
                for name, row in entity_rows.items():
                    if name in stored:
                        row["seq"] = stored[name]["seq"]
                        updates.append(row)
                    else:
                        row["seq"] = next_seq
                        next_seq += 1
                        creates.append(row)
                steps += [(_CREATE_ENTITIES, creates), (_UPDATE_ENTITIES, updates)]
                creates, updates = [], []
                for pair, row in relation_rows.items():
                    if pair in existing:
                        row["seq"] = existing[pair]["seq"]
                        updates.append(row)
                    else:
                        row["seq"] = next_seq
                        next_seq += 1
                        creates.append(row)
                steps += [(_CREATE_RELATIONS, creates), (_UPDATE_RELATIONS, updates)]
                # Marking and deleting a relation writes it twice.
                steps.append(
                    (
                        _MARK_RELATIONS,
                        [
                            {"source": s, "target": t, "cost": 2}
                            for s, t in delete_pairs
                        ],
                    )
                )
                # Deleting an entity also deletes its relations, so it costs
                # one row for itself and one for each of them.
                steps.append(
                    (
                        _DELETE_ENTITIES,
                        [
                            {"name": name, "cost": 1 + degree}
                            for name, degree in doomed.items()
                        ],
                    )
                )
                if documents is not None:
                    rows = []
                    for doc in documents:
                        rows.append({"doc_id": doc, "seq": next_seq})
                        next_seq += 1
                    steps.append((_CREATE_DOCUMENTS, rows))
                written = _update_meta(
                    tx, meta, next_seq, sum(_written(items) for _, items in steps)
                )
                batches = _transaction_batches(steps)
                _run_steps(tx, batches[0])
            for batch in batches[1:]:
                with database.transaction() as tx:
                    _run_steps(tx, batch)
            return None, written

        self._write(write)

    def _write(self, operation: Callable[[hawdb.Database], tuple[_T, int]]) -> _T:
        # operation returns its result and the bytes written since the last
        # checkpoint. A store over an open database leaves checkpoints to
        # HawDB, which checkpoints a database that stays open by itself.
        if self._database is not None:
            self._check_thread()
            if not self._schema_ready:
                _ensure_schema(self._database)
                self._schema_ready = True
            return operation(self._database)[0]

        def write_then_checkpoint(database: hawdb.Database) -> _T:
            result, written = operation(database)
            if written >= _CHECKPOINT_BYTES:
                try:
                    database.execute(_CHECKPOINT)
                    with database.transaction() as tx:
                        tx.execute(_RESET_WRITTEN)
                except HawDBError as error:
                    # The write committed; the next one checkpoints again.
                    logger.warning("CHECKPOINT of %s failed: %s", self._path, error)
            return result

        return self._with_file(write_then_checkpoint, read_only=False, empty=None)

    def _read(self, operation: Callable[[Any], _T], *, empty: Callable[[], _T]) -> _T:
        def in_snapshot(database: hawdb.Database) -> _T:
            with database.read_transaction() as snapshot:
                return operation(snapshot)

        if self._database is not None:
            self._check_thread()
            return in_snapshot(self._database)
        return self._with_file(in_snapshot, read_only=True, empty=empty)

    def _with_file(
        self,
        operation: Callable[[hawdb.Database], _T],
        *,
        read_only: bool,
        empty: Callable[[], _T] | None,
    ) -> _T:
        assert self._path is not None
        deadline = time.monotonic() + self._busy_timeout
        delay = 0.01
        with _path_lock(self._path):
            while True:
                if read_only and not os.path.exists(self._path):
                    assert empty is not None
                    return empty()
                if not read_only:
                    if not os.path.exists(self._path):
                        # Another process may have dropped the database, so
                        # the one this call creates needs its schema.
                        _SCHEMA_READY.discard(self._path)
                    os.makedirs(os.path.dirname(self._path), exist_ok=True)
                try:
                    return _call_on(self._path, read_only, operation)
                except BranchError as error:
                    if not str(error).startswith("branch is busy"):
                        raise
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise StoreBusyError(
                            f"another process held {self._path} for more than "
                            f"{self._busy_timeout:g} seconds"
                        ) from error
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 1.0)

    def _check_thread(self) -> None:
        if not getattr(self._owner, "created_here", False):
            raise WrongThreadError(_WRONG_THREAD)


def _call_on(
    path: str, read_only: bool, operation: Callable[[hawdb.Database], _T]
) -> _T:
    # A hawdb.Database, open or closed, must be freed on the thread that opened
    # it. An exception escaping this call can be freed on any thread, for
    # instance by RAGFlow's event loop after its thread pool raised it, so drop
    # the references its frames hold before it leaves.
    database = hawdb.open(path, read_only=read_only)
    try:
        if not read_only and path not in _SCHEMA_READY:
            _ensure_schema(database)
            _SCHEMA_READY.add(path)
        return operation(database)
    except BaseException as error:
        traceback.clear_frames(error.__traceback__)
        raise
    finally:
        database.close()
        del database


_PATH_LOCKS: dict[str, threading.Lock] = {}
_PATH_LOCKS_GUARD = threading.Lock()
_SCHEMA_READY: set[str] = set()


def _path_lock(path: str) -> threading.Lock:
    with _PATH_LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(path, threading.Lock())


def _ensure_schema(database: hawdb.Database) -> None:
    for label, key in _CONSTRAINTS:
        unique = {
            row["property_name"]
            for row in database.execute_sql(_SQL_UNIQUE, [label]).fetchall()
        }
        if key not in unique:
            database.execute(f"CREATE CONSTRAINT ON :{label}({key}) ASSERT UNIQUE")
        indexed = {
            tuple(row["property_names"])
            for row in database.execute_sql(_SQL_INDEXES, [label]).fetchall()
        }
        if (key,) not in indexed:
            database.execute(f"CREATE INDEX ON :{label}({key})")


def _meta(tx: Any) -> dict[str, Any] | None:
    rows = tx.execute(_META).fetchall()
    return rows[0] if rows else None


def _check_meta(meta: dict[str, Any] | None) -> None:
    if meta is None:
        return
    if meta["version"] != FORMAT_VERSION:
        raise StoreFormatError(_version_message(meta["version"]))
    if meta["clearing"]:
        raise StoreFormatError(
            "an earlier clear() of this graph did not finish; call clear() again"
        )


def _version_message(version: Any) -> str:
    return (
        f"the database holds RAGFlow graph layout {version!r}; "
        f"this store reads layout {FORMAT_VERSION}"
    )


def _update_meta(
    tx: Any, meta: dict[str, Any] | None, next_seq: int, written: int
) -> int:
    # Records the next seq and the bytes written since the last checkpoint,
    # and returns the latter.
    _check_meta(meta)
    total = written + ((meta["written"] or 0) if meta else 0)
    if meta is None:
        tx.execute(
            _CREATE_META,
            {
                "version": FORMAT_VERSION,
                "next_seq": next_seq,
                "written": total,
                "clearing": False,
            },
        )
    else:
        tx.execute(_SET_META, {"next_seq": next_seq, "written": total})
    return total


def _written(rows: list[dict[str, Any]]) -> int:
    return sum(row.get("stored_bytes", 0) + _ROW_WRITE_BYTES for row in rows)


def _document_ids(tx: Any) -> list[str]:
    return [
        row["doc_id"] for row in sorted(tx.execute(_DOCUMENTS).fetchall(), key=_seq)
    ]


def _entity_sizes(tx: Any, names: list[str]) -> list[dict[str, Any]]:
    rows = []
    for page in _pages(list(dict.fromkeys(names)), _NAME_PAGE):
        rows.extend(tx.execute(_ENTITY_SIZES_IN, {"names": page}).fetchall())
    return rows


def _read_entities(tx: Any, names: list[str]) -> dict[str, dict[str, Any]]:
    stored = {}
    for page in _byte_pages(_entity_sizes(tx, names)):
        for row in tx.execute(
            _ENTITIES_IN, {"names": [item["name"] for item in page]}
        ).fetchall():
            stored[row["name"]] = row
    return stored


def _relation_sizes(tx: Any, pairs: Sequence[tuple[str, str]]) -> list[dict[str, Any]]:
    rows = []
    for page in _pages(list(pairs), _NAME_PAGE):
        rows.extend(
            tx.execute(
                _RELATION_SIZES_BETWEEN,
                {
                    "pairs": [
                        {"source": source, "target": target} for source, target in page
                    ]
                },
            ).fetchall()
        )
    return rows


def _read_relations(
    tx: Any, pairs: list[tuple[str, str]]
) -> dict[tuple[str, str], dict[str, Any]]:
    existing = {}
    for page in _byte_pages(_relation_sizes(tx, pairs)):
        bound = [{"source": item["source"], "target": item["target"]} for item in page]
        for row in tx.execute(_RELATIONS_BETWEEN, {"pairs": bound}).fetchall():
            existing[(row["source"], row["target"])] = row
    return existing


def _entity_degrees(tx: Any, names: list[str]) -> list[dict[str, Any]]:
    rows = []
    for page in _pages(names, _NAME_PAGE):
        rows.extend(tx.execute(_ENTITY_DEGREES_IN, {"names": page}).fetchall())
    return rows


def _incident_relations(tx: Any, names: list[str]) -> set[tuple[str, str]]:
    pairs = set()
    for page in _pages(names, _NAME_PAGE):
        for statement in (_RELATION_SIZES_FROM, _RELATION_SIZES_TO):
            for row in tx.execute(statement, {"names": page}).fetchall():
                pairs.add((row["source"], row["target"]))
    return pairs


def _seq_ranges(
    rows: list[dict[str, Any]], max_rows: int, max_bytes: int | None
) -> list[tuple[int, int]]:
    # rows come sorted by seq. Each range spans at most max_rows of them and,
    # unless max_bytes is None, at most max_bytes estimated bytes.
    ranges: list[tuple[int, int]] = []
    count = size = 0
    for row in rows:
        row_size = (row.get("stored_bytes") or 0) if max_bytes is not None else 0
        if count and (
            count == max_rows or (max_bytes is not None and size + row_size > max_bytes)
        ):
            count = size = 0
        if count == 0:
            ranges.append((row["seq"], row["seq"]))
        else:
            ranges[-1] = (ranges[-1][0], row["seq"])
        count += 1
        size += row_size
    return ranges


def _transaction_batches(
    steps: list[tuple[str, list[dict[str, Any]]]],
) -> list[list[tuple[str, list[dict[str, Any]]]]]:
    batches: list[list[tuple[str, list[dict[str, Any]]]]] = [[]]
    rows = size = 0
    for statement, items in steps:
        for item in items:
            item_rows = item.get("cost", 1)
            item_size = item.get("stored_bytes", 64)
            if rows and (
                rows + item_rows > _TRANSACTION_ROWS
                or size + item_size > _TRANSACTION_BYTES
            ):
                batches.append([])
                rows = size = 0
            batch = batches[-1]
            if not batch or batch[-1][0] != statement:
                batch.append((statement, []))
            batch[-1][1].append(item)
            rows += item_rows
            size += item_size
    return batches


def _run_steps(tx: Any, steps: list[tuple[str, list[dict[str, Any]]]]) -> None:
    for statement, items in steps:
        if statement == _MARK_RELATIONS:
            _execute_rows(tx, statement, items)
            tx.execute(_DELETE_MARKED_RELATIONS)
        elif statement == _DELETE_ENTITIES:
            for page in _pages([item["name"] for item in items], _NAME_PAGE):
                tx.execute(statement, {"names": page})
        else:
            _execute_rows(tx, statement, items)


def _execute_rows(tx: Any, statement: str, rows: list[dict[str, Any]]) -> None:
    chunk: list[dict[str, Any]] = []
    size = 0
    for row in rows:
        row_size = row.get("stored_bytes", 64)
        if chunk and (
            len(chunk) == _STATEMENT_ROWS or size + row_size > _STATEMENT_BYTES
        ):
            tx.execute(statement, {"rows": chunk})
            chunk, size = [], 0
        chunk.append(row)
        size += row_size
    if chunk:
        tx.execute(statement, {"rows": chunk})


def _pages(items: list[_T], size: int) -> Iterator[list[_T]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _byte_pages(rows: list[dict[str, Any]]) -> Iterator[list[dict[str, Any]]]:
    page: list[dict[str, Any]] = []
    size = 0
    for row in rows:
        row_size = row["stored_bytes"] or 0
        if page and (
            len(page) == _READ_PAGE_ROWS or size + row_size > _READ_PAGE_BYTES
        ):
            yield page
            page, size = [], 0
        page.append(row)
        size += row_size
    if page:
        yield page


def _seq(row: dict[str, Any]) -> int:
    return row["seq"]


def _path(name: str, row: dict[str, Any]) -> dict[str, list[Any]]:
    path = [name, row["b"]]
    weights = [_weight(row["w1"])]
    if row.get("c") is not None:
        path.append(row["c"])
        weights.append(_weight(row["w2"]))
    return {"path": path, "weights": weights}


def _weight(value: Any) -> Any:
    return 0 if value is None else value


def _documents_of(subgraph: nx.Graph) -> list[str]:
    documents = subgraph.graph.get("source_id")
    if not documents:
        raise ValueError(
            "a subgraph names its documents in subgraph.graph['source_id']"
        )
    return list(dict.fromkeys(_string_list(documents, "subgraph.graph['source_id']")))


def _subgraph_entities(subgraph: nx.Graph) -> dict[str, dict[str, Any]]:
    if subgraph.is_directed() or subgraph.is_multigraph():
        raise ValueError("a RAGFlow subgraph is an undirected networkx.Graph")
    entities = {}
    for name, attrs in subgraph.nodes(data=True):
        _check_text(name, "an entity name")
        _entity_row(name, attrs)
        entities[name] = attrs
    return entities


def _subgraph_relations(
    subgraph: nx.Graph,
    entities: dict[str, dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    relations = {}
    for source, target, attrs in subgraph.edges(data=True):
        pair = _pair(source, target)
        _relation_row(pair, attrs)
        relations[pair] = attrs
    return relations


def _entity_row(name: str, attrs: dict[str, Any]) -> dict[str, Any]:
    what = f"entity {_brief(name)}"
    description = attrs.get("description")
    if not isinstance(description, str):
        raise ValueError(f"{what} needs a description string")
    source_id = _source_ids(attrs.get("source_id"), what)
    entity_type = attrs.get("entity_type")
    typed = {"description", "source_id"}
    if isinstance(entity_type, str):
        typed.add("entity_type")
    else:
        entity_type = None
    attributes = _attributes(
        {
            key: value
            for key, value in attrs.items()
            if key not in typed and key not in DERIVED_NODE_ATTRIBUTES
        },
        what,
    )
    stored_bytes = (
        _size(name)
        + _size(entity_type or "")
        + _size(description)
        + _list_size(source_id)
        + _size(attributes or "")
    )
    if stored_bytes > MAX_ENTITY_BYTES:
        raise ValueError(
            f"{what} would store {stored_bytes} bytes, more than the "
            f"{MAX_ENTITY_BYTES} a two-hop traversal can hold"
        )
    return {
        "name": name,
        "entity_type": entity_type,
        "description": description,
        "source_id": source_id,
        "attributes": attributes,
        "stored_bytes": stored_bytes,
    }


def _relation_row(pair: tuple[str, str], attrs: dict[str, Any]) -> dict[str, Any]:
    what = f"relation {_brief(pair)}"
    description = attrs.get("description")
    if not isinstance(description, str):
        raise ValueError(f"{what} needs a description string")
    source_id = _source_ids(attrs.get("source_id"), what)
    typed = {"description", "source_id"}
    weight = attrs.get("weight")
    if _is_number(weight):
        typed.add("weight")
    else:
        weight = None
    keywords = attrs.get("keywords")
    if isinstance(keywords, list) and all(
        isinstance(keyword, str) for keyword in keywords
    ):
        typed.add("keywords")
    else:
        keywords = None
    attributes = _attributes(
        {key: value for key, value in attrs.items() if key not in typed}, what
    )
    stored_bytes = (
        _size(pair[0])
        + _size(pair[1])
        + _size(description)
        + _list_size(keywords or [])
        + _list_size(source_id)
        + _size(attributes or "")
    )
    if stored_bytes > MAX_RELATION_BYTES:
        raise ValueError(
            f"{what} would store {stored_bytes} bytes, more than the "
            f"{MAX_RELATION_BYTES} allowed"
        )
    return {
        "source": pair[0],
        "target": pair[1],
        "weight": weight,
        "description": description,
        "keywords": keywords,
        "source_id": source_id,
        "attributes": attributes,
        "stored_bytes": stored_bytes,
    }


def _node_attributes(row: dict[str, Any]) -> dict[str, Any]:
    typed = {"description": row["description"], "source_id": list(row["source_id"])}
    if row["entity_type"] is not None:
        typed["entity_type"] = row["entity_type"]
    return _ordered(_NODE_ORDER, typed, _load(row["attributes"]))


def _edge_attributes(row: dict[str, Any]) -> dict[str, Any]:
    typed = {"description": row["description"], "source_id": list(row["source_id"])}
    if row["weight"] is not None:
        typed["weight"] = row["weight"]
    if row["keywords"] is not None:
        typed["keywords"] = list(row["keywords"])
    return _ordered(_EDGE_ORDER, typed, _load(row["attributes"]))


def _ordered(
    order: tuple[str, ...], typed: dict[str, Any], extra: dict[str, Any]
) -> dict[str, Any]:
    attrs = {}
    for key in order:
        if key in typed:
            attrs[key] = typed.pop(key)
        elif key in extra:
            attrs[key] = extra.pop(key)
    attrs.update(typed)
    attrs.update(extra)
    return attrs


def _attributes(extra: dict[str, Any], what: str) -> str | None:
    if not extra:
        return None
    try:
        return json.dumps(extra, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ValueError(f"the attributes of {what} are not JSON: {error}") from None


def _load(attributes: str | None) -> dict[str, Any]:
    return json.loads(attributes) if attributes else {}


def _string_list(value: Any, what: str) -> list[str]:
    if not isinstance(value, (list, tuple)) or not all(
        isinstance(item, str) for item in value
    ):
        raise ValueError(f"{what} must be a list of strings")
    return list(value)


def _source_ids(value: Any, what: str) -> list[str]:
    # RAGFlow deletes a graph chunk left without documents, and so does
    # remove_document, so an entity or relation always names one.
    source_id = _string_list(value, f"the source_id of {what}")
    if not source_id:
        raise ValueError(f"the source_id of {what} must name at least one document")
    return source_id


def _names(values: Iterable[Any], what: str, *, ordered: bool = False) -> Any:
    names = list(values) if ordered else set(values)
    for name in names:
        _check_text(name, f"every name in {what}")
    return names


def _pairs(values: Iterable[Any], what: str) -> set[tuple[str, str]]:
    pairs = set()
    for value in values:
        if not isinstance(value, tuple) or len(value) != 2:
            raise ValueError(f"every edge in {what} must be a pair of names")
        _check_text(value[0], f"every name in {what}")
        _check_text(value[1], f"every name in {what}")
        pairs.add(_pair(*value))
    return pairs


def _pair(source: str, target: str) -> tuple[str, str]:
    return (source, target) if source < target else (target, source)


def _check_text(value: Any, what: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{what} must be a non-empty string")


def _check_positive_int(value: Any, what: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{what} must be a positive integer")


def _is_number(value: Any) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _size(text: str) -> int:
    return len(text.encode("utf-8"))


def _list_size(items: list[str]) -> int:
    return sum(_size(item) + 8 for item in items)


def _brief(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 80 else text[:77] + "..."
