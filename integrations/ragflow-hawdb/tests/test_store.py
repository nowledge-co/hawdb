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

"""HawDBGraphStore on real embedded databases, without RAGFlow or a document engine.

The references below restate RAGFlow v0.27.3's ``graph_merge`` and
``n_neighbor`` in ``rag/graphrag/utils.py``. ``patches/verify_patch.py``
compares the store with RAGFlow's own functions instead.
"""

import contextlib
import copy
import errno
import gc
import glob
import json
import logging
import os
import random
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
from itertools import pairwise
from pathlib import Path

import hawdb
import networkx as nx
import pytest
from hawdb.exceptions import Error as HawDBError

from ragflow_hawdb import (
    GRAPH_FIELD_SEP,
    MAX_ENTITY_BYTES,
    MAX_RELATION_BYTES,
    GraphChange,
    HawDBGraphStore,
    StaleImportError,
    StoreBusyError,
    StoreFormatError,
    WrongThreadError,
)
from ragflow_hawdb import store as store_module
from ragflow_hawdb.ragflow import (
    VIEW_EDGES,
    VIEW_MARKER,
    VIEW_MARKER_VALUE,
    VIEW_NODES,
    is_view_graph,
    store_for,
    view_graph,
)

PATCH = Path(__file__).resolve().parent.parent / "patches" / "ragflow-v0.27.3.patch"
RAGFLOW_COMMIT = "8090ffd8ba6098813d7f7ec2c4b8774f36c8423a"

HOSTILE = [
    "Robert'); MATCH (n) DETACH DELETE n; //",
    "x}) DETACH DELETE n //",
    "`backtick`",
    "$name",
    "名前 with spaces",
    "line\nbreak",
    GRAPH_FIELD_SEP,
]


@pytest.fixture(autouse=True)
def collect_garbage_on_this_thread():
    # A hawdb.Database refuses to be freed on another thread, and the cyclic
    # garbage collector runs on whichever thread allocates. Free any database
    # a test left in reference cycles before a later test starts a thread.
    yield
    gc.collect()


@pytest.fixture(params=["path", "open database"])
def store(request, tmp_path):
    if request.param == "path":
        yield HawDBGraphStore(tmp_path / "tenant" / "kb")
        return
    database = hawdb.open()
    yield HawDBGraphStore(database)
    database.close()


# For behavior only a store on a path has: opening per call, checkpoints,
# drop, and other processes.
path_only = pytest.mark.parametrize("store", ["path"], indirect=True)


def subgraph(doc, entities, relations=()):
    """A subgraph as RAGFlow's extractors make one from ``doc``.

    ``entities`` is a list of names, or a dict of names to entity types.
    ``relations`` holds ``(source, target, weight)``; a weight of None
    leaves the relation without one.
    """
    types = (
        entities if isinstance(entities, dict) else dict.fromkeys(entities, "PERSON")
    )
    graph = nx.Graph()
    for name, entity_type in types.items():
        graph.add_node(
            name,
            entity_name=name,
            entity_type=entity_type,
            description=f"{name} in {doc}",
            source_id=[doc],
        )
    for source, target, weight in relations:
        attrs = {
            "src_id": source,
            "tgt_id": target,
            "description": f"{source} and {target} in {doc}",
            "keywords": [f"{doc} keyword"],
            "source_id": [doc],
        }
        if weight is not None:
            attrs["weight"] = weight
        graph.add_edge(source, target, **attrs)
    graph.graph["source_id"] = [doc]
    return graph


def random_subgraph(rng, doc, names):
    # A few entities, relations between them, and sometimes a relation of an
    # entity with itself.
    chosen = rng.sample(names, rng.randint(1, min(8, len(names))))
    relations = []
    for _ in range(rng.randint(0, 12)):
        source, target = rng.choice(chosen), rng.choice(chosen)
        if source != target or rng.random() < 0.3:
            relations.append((source, target, rng.choice([0.5, 1.0, 2.0, 3.25])))
    graph = subgraph(doc, chosen, relations)
    for name in chosen:
        graph.nodes[name]["description"] += f" #{rng.randint(0, 99)}"
    return graph


def merged(graph, sub):
    """What RAGFlow's graph_merge makes of ``graph`` and ``sub``, on copies.

    A new entity or relation is added as it is. An entity already there gets
    the new description after GRAPH_FIELD_SEP and the new documents, and keeps
    its other attributes; a relation also adds the new weight and appends the
    new keywords. The graph's documents gain the subgraph's.
    """
    graph = nx.Graph() if graph is None else copy.deepcopy(graph)
    for name, attrs in sub.nodes(data=True):
        if name not in graph:
            graph.add_node(name, **copy.deepcopy(attrs))
            continue
        node = graph.nodes[name]
        node["description"] = (
            node["description"] + GRAPH_FIELD_SEP + attrs["description"]
        )
        node["source_id"] = node["source_id"] + attrs["source_id"]
    for source, target, attrs in sub.edges(data=True):
        if not graph.has_edge(source, target):
            graph.add_edge(source, target, **copy.deepcopy(attrs))
            continue
        edge = graph.edges[source, target]
        edge["weight"] = edge["weight"] + attrs.get("weight", 0)
        edge["description"] = (
            edge["description"] + GRAPH_FIELD_SEP + attrs["description"]
        )
        edge["keywords"] = edge["keywords"] + attrs["keywords"]
        edge["source_id"] = edge["source_id"] + attrs["source_id"]
    graph.graph["source_id"] = graph.graph.get("source_id", []) + sub.graph["source_id"]
    return graph


def without_document(graph, doc):
    """The store's rule for removing ``doc``, on a copy.

    ``doc`` leaves every source_id; an entity or relation left without one is
    deleted, and so are the relations of a deleted entity.
    """
    graph = copy.deepcopy(graph)
    for name in graph.nodes:
        graph.nodes[name]["source_id"] = [
            other for other in graph.nodes[name]["source_id"] if other != doc
        ]
    for source, target in list(graph.edges):
        edge = graph.edges[source, target]
        edge["source_id"] = [other for other in edge["source_id"] if other != doc]
        if not edge["source_id"]:
            graph.remove_edge(source, target)
    graph.remove_nodes_from(
        [name for name in graph.nodes if not graph.nodes[name]["source_id"]]
    )
    graph.graph["source_id"] = [
        other for other in graph.graph["source_id"] if other != doc
    ]
    return graph


def walk(graph, name, n_hop):
    """The paths RAGFlow's n_neighbor walks from ``name``, in its order.

    The walk follows each edge of ``name``. On a second hop it follows every
    edge of the neighbor except the one straight back, unless the neighbor is
    ``name`` itself; a neighbor without such an edge ends its path after one
    hop. A missing weight counts as 0.
    """

    def weight(source, target):
        value = graph.edges[source, target].get("weight")
        return 0 if value is None else value

    paths = []
    for neighbor in graph.adj[name]:
        onward = (
            []
            if n_hop == 1 or neighbor == name
            else [other for other in graph.adj[neighbor] if other != name]
        )
        if not onward:
            paths.append(
                {"path": [name, neighbor], "weights": [weight(name, neighbor)]}
            )
        for other in onward:
            paths.append(
                {
                    "path": [name, neighbor, other],
                    "weights": [weight(name, neighbor), weight(neighbor, other)],
                }
            )
    return paths


def sorted_paths(paths):
    return sorted((tuple(path["path"]), tuple(path["weights"])) for path in paths)


def snapshot(graph):
    """The entities, relations and documents of ``graph``, less rank and pagerank."""
    if graph is None:
        return {}, {}, []
    nodes = {
        name: {
            key: value
            for key, value in attrs.items()
            if key not in ("rank", "pagerank")
        }
        for name, attrs in graph.nodes(data=True)
    }
    edges = {
        tuple(sorted((source, target))): dict(attrs)
        for source, target, attrs in graph.edges(data=True)
    }
    return nodes, edges, list(graph.graph.get("source_id", []))


def check_store(store, expected):
    graph = store.get_graph()
    if expected is None or expected.number_of_nodes() == 0:
        assert graph is None or graph.number_of_nodes() == 0
        return
    assert snapshot(graph) == snapshot(expected)
    assert list(graph.nodes) == list(expected.nodes)
    assert all(graph.nodes[name]["rank"] == degree for name, degree in graph.degree)
    for n_hop in (1, 2):
        found = store.n_hop_paths(list(expected.nodes), n_hop=n_hop, max_paths=10_000)
        for name in expected.nodes:
            assert sorted_paths(found[name]) == sorted_paths(
                walk(expected, name, n_hop)
            ), (name, n_hop)


def on_database(store, run, *, read_only):
    # Runs run(database) on the database the store uses.
    if store.path is None:
        return run(store._database)
    database = hawdb.open(store.path, read_only=read_only)
    try:
        return run(database)
    finally:
        database.close()
        del database


def cypher(store, statement, parameters=None):
    """The rows of a parameterized read on the store's database."""

    def run(database):
        with database.read_transaction() as snapshot_tx:
            return snapshot_tx.execute(statement, parameters or {}).fetchall()

    return on_database(store, run, read_only=True)


def write_cypher(store, statement, parameters=None):
    def run(database):
        with database.transaction() as tx:
            tx.execute(statement, parameters or {})

    on_database(store, run, read_only=False)


def meta(store):
    rows = cypher(store, store_module._META)
    return rows[0] if rows else None


def exists(store):
    return store.path is None or os.path.exists(store.path)


def reopened(store, **settings):
    # Another store over the same database, with other settings.
    return HawDBGraphStore(
        store._database if store.path is None else store.path, **settings
    )


def module_statements():
    """Every statement text the store module can run."""
    statements = set()
    for name, value in vars(store_module).items():
        if not re.fullmatch(r"_[A-Z][A-Z0-9_]*", name):
            continue
        if isinstance(value, str):
            statements.add(value)
        elif isinstance(value, dict):
            statements.update(item for item in value.values() if isinstance(item, str))
    for label, key in store_module._CONSTRAINTS:
        statements.add(f"CREATE CONSTRAINT ON :{label}({key}) ASSERT UNIQUE")
        statements.add(f"CREATE INDEX ON :{label}({key})")
    return statements


class Recorder:
    """Records every statement the stores run, with its scope, by wrapping hawdb.open.

    A scope is "autocommit" or "sql" for a statement outside a transaction,
    "write" or "read" for one inside, where BEGIN and COMMIT or ROLLBACK mark
    the transaction. ``fail``, a predicate on statement text, makes the
    matching statements raise before they run.
    """

    def __init__(self, monkeypatch):
        self.events = []
        self.fail = None
        real_open = hawdb.open

        def recording_open(*args, **kwargs):
            return _RecordingDatabase(real_open(*args, **kwargs), self)

        monkeypatch.setattr(hawdb, "open", recording_open)

    def record(self, scope, statement, parameters):
        if self.fail is not None and self.fail(statement):
            raise HawDBError(f"injected failure before {statement[:40]!r}")
        self.events.append((scope, statement, parameters))

    def statements(self, *scopes):
        return [statement for scope, statement, _ in self.events if scope in scopes]

    def transactions(self, scope="write"):
        return self.statements(scope).count("BEGIN")


class _RecordingDatabase:
    def __init__(self, database, recorder):
        self._database = database
        self._recorder = recorder

    def execute(self, statement, *args):
        self._recorder.record("autocommit", statement, args[0] if args else None)
        return self._database.execute(statement, *args)

    def execute_sql(self, statement, *args):
        self._recorder.record("sql", statement, args[0] if args else None)
        return self._database.execute_sql(statement, *args)

    def transaction(self):
        return _RecordingTransaction(
            self._database.transaction(), self._recorder, "write"
        )

    def read_transaction(self):
        return _RecordingTransaction(
            self._database.read_transaction(), self._recorder, "read"
        )

    def close(self):
        self._database.close()


class _RecordingTransaction:
    def __init__(self, transaction, recorder, scope):
        self._transaction = transaction
        self._recorder = recorder
        self._scope = scope

    def __enter__(self):
        self._transaction.__enter__()
        self._recorder.events.append((self._scope, "BEGIN", None))
        return self

    def __exit__(self, kind, error, trace):
        self._recorder.events.append(
            (self._scope, "COMMIT" if kind is None else "ROLLBACK", None)
        )
        return self._transaction.__exit__(kind, error, trace)

    def execute(self, statement, *args):
        self._recorder.record(self._scope, statement, args[0] if args else None)
        return self._transaction.execute(statement, *args)


def test_a_merged_subgraph_keeps_source_id_through_a_one_hop_traversal(store):
    sub = subgraph(
        "doc-1", {"ALICE": "PERSON", "ACME": "ORGANIZATION"}, [("ALICE", "ACME", 2.0)]
    )

    change = store.merge_subgraph(sub)

    assert change == GraphChange(
        added_updated_nodes={"ALICE", "ACME"}, added_updated_edges={("ACME", "ALICE")}
    )
    assert store.n_hop_paths(["ALICE"], n_hop=1) == {
        "ALICE": [{"path": ["ALICE", "ACME"], "weights": [2.0]}]
    }
    graph = store.get_graph()
    assert graph.nodes["ACME"]["source_id"] == ["doc-1"]
    assert graph.edges["ALICE", "ACME"]["source_id"] == ["doc-1"]
    # The same hop, run directly on the database.
    rows = cypher(
        store,
        "MATCH (:Entity {name: $name})-[r:RELATION]-(b:Entity) "
        "RETURN b.name AS name, b.source_id AS entity_source, "
        "r.source_id AS relation_source",
        {"name": "ALICE"},
    )
    assert rows == [
        {"name": "ACME", "entity_source": ["doc-1"], "relation_source": ["doc-1"]}
    ]


def test_merges_follow_graph_merge(store):
    first = subgraph(
        "doc1",
        {"ALICE": "PERSON", "BOB": "PERSON", "ACME": "ORGANIZATION"},
        [("ALICE", "BOB", 2.0), ("BOB", "ACME", 1.0)],
    )
    first.nodes["ALICE"]["importance"] = {"score": 3, "tags": ["founder"]}
    # ALICE comes back with another type, which graph_merge does not take,
    # and BOB-ALICE is the relation ALICE-BOB seen from the other side.
    second = subgraph(
        "doc2",
        {"ALICE": "ORGANIZATION", "BOB": "PERSON", "CAROL": "PERSON"},
        [("BOB", "ALICE", 1.5), ("CAROL", "BOB", 0.5)],
    )
    third = subgraph(
        "doc3", {"ACME": "ORGANIZATION", "DAVE": "PERSON"}, [("DAVE", "ACME", 4.0)]
    )

    expected = None
    for sub in (first, second, third):
        change = store.merge_subgraph(copy.deepcopy(sub))
        expected = merged(expected, sub)
        assert change.added_updated_nodes == set(sub.nodes)
        assert change.added_updated_edges == {tuple(sorted(pair)) for pair in sub.edges}
        check_store(store, expected)

    graph = store.get_graph()
    assert graph.graph["source_id"] == ["doc1", "doc2", "doc3"]
    assert graph.nodes["ALICE"]["entity_type"] == "PERSON"
    assert graph.nodes["ALICE"]["importance"] == {"score": 3, "tags": ["founder"]}
    relation = graph.edges["ALICE", "BOB"]
    assert relation["weight"] == 3.5
    assert relation["keywords"] == ["doc1 keyword", "doc2 keyword"]
    assert (
        relation["description"]
        == f"ALICE and BOB in doc1{GRAPH_FIELD_SEP}BOB and ALICE in doc2"
    )
    assert relation["source_id"] == ["doc1", "doc2"]


@pytest.mark.parametrize("seed", range(8))
def test_random_merges_and_removals_match_the_references(seed, tmp_path):
    rng = random.Random(seed)
    names = [f"E{i}" for i in range(rng.randint(3, 12))] + HOSTILE[:3]
    database = hawdb.open() if seed % 2 else None
    store = HawDBGraphStore(tmp_path / "kb" if database is None else database)
    expected = None
    for number in range(rng.randint(2, 5)):
        sub = random_subgraph(rng, f"doc{number}", names)
        change = store.merge_subgraph(copy.deepcopy(sub))
        expected = merged(expected, sub)
        assert change.added_updated_nodes == set(sub.nodes)
        assert store.merge_subgraph(copy.deepcopy(sub)) == GraphChange()
        check_store(store, expected)
    for doc in rng.sample(
        expected.graph["source_id"], rng.randint(1, len(expected.graph["source_id"]))
    ):
        store.remove_document(doc)
        expected = without_document(expected, doc)
        assert not store.contains_document(doc)
        check_store(store, expected)
    if database is not None:
        database.close()
        del store, database


def test_n_hop_paths_walk_as_n_neighbor_does(store):
    # HUB has a relation with itself and one without a weight; LEAF has no
    # other relation, LONE none at all.
    sub = subgraph(
        "doc1",
        ["HUB", "A", "B", "LEAF", "LONE"],
        [
            ("HUB", "A", 3.0),
            ("HUB", "B", 1.0),
            ("A", "B", 2.0),
            ("HUB", "LEAF", None),
            ("HUB", "HUB", 0.5),
        ],
    )
    store.merge_subgraph(copy.deepcopy(sub))

    paths = store.n_hop_paths(["HUB", "LEAF", "LONE", "NOBODY"])

    assert paths["HUB"] == [
        {"path": ["HUB", "A", "B"], "weights": [3.0, 2.0]},
        {"path": ["HUB", "B", "A"], "weights": [1.0, 2.0]},
        {"path": ["HUB", "HUB"], "weights": [0.5]},
        {"path": ["HUB", "LEAF"], "weights": [0]},
    ]
    assert paths["LEAF"] == [
        {"path": ["LEAF", "HUB", "A"], "weights": [0, 3.0]},
        {"path": ["LEAF", "HUB", "B"], "weights": [0, 1.0]},
        {"path": ["LEAF", "HUB", "HUB"], "weights": [0, 0.5]},
    ]
    assert paths["LONE"] == [] and paths["NOBODY"] == []
    for n_hop in (1, 2):
        found = store.n_hop_paths(list(sub.nodes), n_hop=n_hop)
        for name in sub.nodes:
            assert sorted_paths(found[name]) == sorted_paths(walk(sub, name, n_hop))


def test_n_hop_paths_keep_the_heaviest_paths_first(store, caplog):
    store.merge_subgraph(
        subgraph(
            "doc1",
            ["HUB", "A", "B", "C", "D"],
            [
                ("HUB", "A", 1.0),
                ("HUB", "C", 4.0),
                ("HUB", "B", 4.0),
                ("HUB", "D", 2.0),
            ],
        )
    )

    def neighbors(found):
        return [path["path"][1] for path in found["HUB"]]

    assert neighbors(store.n_hop_paths(["HUB"], n_hop=1)) == ["B", "C", "D", "A"]
    with caplog.at_level(logging.WARNING, logger=store_module.__name__):
        assert neighbors(store.n_hop_paths(["HUB"], n_hop=1, max_paths=2)) == ["B", "C"]
    assert "more than 2 paths of 1 hops" in caplog.text
    assert neighbors(reopened(store, max_paths=3).n_hop_paths(["HUB"], n_hop=1)) == [
        "B",
        "C",
        "D",
    ]


@pytest.mark.parametrize(
    "names, settings",
    [
        (["A"], {"n_hop": 3}),
        (["A"], {"n_hop": 0}),
        (["A"], {"n_hop": True}),
        (["A"], {"max_paths": 0}),
        (["A"], {"max_paths": 1.5}),
        ([""], {}),
        ([7], {}),
    ],
)
def test_n_hop_paths_refuses_bad_arguments(store, names, settings):
    with pytest.raises(ValueError):
        store.n_hop_paths(names, **settings)


@path_only
def test_a_call_runs_module_statements_in_one_transaction(store, monkeypatch):
    recorder = Recorder(monkeypatch)
    store.merge_subgraph(subgraph("doc0", ["SEED"]))
    # The first write on a path in a process creates the schema, and only
    # that runs outside a transaction.
    autocommit = recorder.statements("autocommit")
    assert autocommit
    assert all(
        statement.startswith(("CREATE CONSTRAINT ON :", "CREATE INDEX ON :"))
        for statement in autocommit
    )

    recorder.events.clear()
    sub = subgraph(
        "doc1", HOSTILE, [(HOSTILE[0], HOSTILE[1], 1.0), (HOSTILE[2], HOSTILE[3], 2.0)]
    )
    store.merge_subgraph(sub)
    assert recorder.statements("autocommit", "sql") == []
    assert [
        statement
        for statement in recorder.statements("write")
        if statement in ("BEGIN", "COMMIT")
    ] == [
        "BEGIN",
        "COMMIT",
    ]
    created = [
        parameters
        for _, statement, parameters in recorder.events
        if statement == store_module._CREATE_ENTITIES
    ]
    assert {row["name"] for parameters in created for row in parameters["rows"]} == set(
        HOSTILE
    )

    store.get_graph()
    store.n_hop_paths(HOSTILE)
    store.remove_document("doc1")
    assert recorder.transactions("write") == 2
    assert recorder.transactions("read") == 2

    allowed = module_statements()
    ran = {statement for _, statement, _ in recorder.events} - {
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
    }
    assert ran <= allowed
    assert set(store.get_graph().nodes) == {"SEED"}


@path_only
def test_a_failed_merge_writes_nothing(store, monkeypatch):
    store.merge_subgraph(subgraph("doc1", ["A", "B"], [("A", "B", 1.0)]))
    before = snapshot(store.get_graph())
    recorder = Recorder(monkeypatch)
    recorder.fail = lambda statement: statement == store_module._CREATE_RELATIONS

    with pytest.raises(HawDBError, match="injected"):
        store.merge_subgraph(subgraph("doc2", ["B", "C"], [("B", "C", 1.0)]))

    assert recorder.statements("write")[-1] == "ROLLBACK"
    recorder.fail = None
    assert snapshot(store.get_graph()) == before


def test_a_subgraph_sharing_some_documents_is_refused(store):
    store.merge_subgraph(subgraph("doc1", ["A"]))
    sub = subgraph("doc2", ["B"])
    sub.graph["source_id"] = ["doc1", "doc2"]

    with pytest.raises(ValueError, match="doc1"):
        store.merge_subgraph(sub)

    assert store.document_ids() == ["doc1"]
    assert list(store.get_graph().nodes) == ["A"]


def test_merging_a_document_again_changes_nothing(store):
    sub = subgraph("doc1", ["A", "B"], [("A", "B", 1.0)])
    store.merge_subgraph(copy.deepcopy(sub))
    before = snapshot(store.get_graph())

    assert store.merge_subgraph(copy.deepcopy(sub)) == GraphChange()
    assert snapshot(store.get_graph()) == before


@pytest.mark.parametrize(
    "spoil, message",
    [
        (lambda g: g.graph.pop("source_id"), "names its documents"),
        (lambda g: g.graph.update(source_id=[]), "names its documents"),
        (lambda g: g.graph.update(source_id="doc1"), "list of strings"),
        (lambda g: g.nodes["A"].update(source_id=[]), "at least one document"),
        (lambda g: g.nodes["A"].update(source_id=[1]), "list of strings"),
        (lambda g: g.nodes["A"].pop("description"), "description string"),
        (lambda g: g.edges["A", "B"].update(source_id=[]), "at least one document"),
        (lambda g: g.edges["A", "B"].update(weight=float("nan")), "not JSON"),
        (lambda g: g.nodes["A"].update(extra={1, 2}), "not JSON"),
        (
            lambda g: g.add_node(7, description="x", source_id=["doc1"]),
            "non-empty string",
        ),
        (
            lambda g: g.add_node("", description="x", source_id=["doc1"]),
            "non-empty string",
        ),
    ],
)
def test_invalid_subgraphs_are_refused_before_anything_is_written(
    store, spoil, message
):
    sub = subgraph("doc1", ["A", "B"], [("A", "B", 1.0)])
    spoil(sub)

    with pytest.raises(ValueError, match=message):
        store.merge_subgraph(sub)

    assert store.get_graph() is None
    assert store.path is None or not os.path.exists(store.path)


def test_a_subgraph_must_be_an_undirected_graph(store):
    sub = nx.DiGraph(subgraph("doc1", ["A", "B"], [("A", "B", 1.0)]))
    sub.graph["source_id"] = ["doc1"]
    with pytest.raises(ValueError, match="undirected"):
        store.merge_subgraph(sub)


def test_documents_are_listed_in_merge_order(store):
    for doc in ("doc-b", "doc-a", "doc-c"):
        store.merge_subgraph(subgraph(doc, ["SHARED"]))

    assert store.document_ids() == ["doc-b", "doc-a", "doc-c"]
    assert store.get_graph().graph["source_id"] == ["doc-b", "doc-a", "doc-c"]
    assert store.contains_document("doc-a")
    assert not store.contains_document("doc-z")
    with pytest.raises(ValueError):
        store.contains_document("")


def test_a_store_on_a_missing_path_reads_empty_and_creates_nothing(tmp_path):
    path = tmp_path / "tenant" / "kb"
    store = HawDBGraphStore(path)

    assert store.get_graph() is None
    assert store.document_ids() == []
    assert store.contains_document("doc1") is False
    assert store.n_hop_paths(["A"]) == {"A": []}
    assert store.document_change(["doc1"]) == GraphChange()
    assert store.remove_document("doc1") == GraphChange()
    assert store.holds_graph() is False
    assert store.generation() == ""
    store.import_graph(nx.Graph())
    store.import_graph(nx.Graph(), generation="")
    store.drop()

    # clear() is the one call that creates a database there; see
    # test_clear_on_a_missing_path_creates_the_database_to_record_its_generation.
    assert not path.parent.exists()


def test_removing_a_document_strips_it_and_deletes_what_only_it_named(store):
    store.merge_subgraph(
        subgraph(
            "doc1",
            ["A", "B", "C", "E"],
            [("A", "B", 1.0), ("B", "C", 2.0), ("B", "E", 1.0)],
        )
    )
    store.merge_subgraph(
        subgraph("doc2", ["B", "C", "D", "E"], [("B", "C", 3.0), ("C", "D", 1.0)])
    )

    change = store.remove_document("doc1")

    # B-E names only doc1, so it goes although B and E stay.
    assert change == GraphChange(
        removed_nodes={"A"},
        added_updated_nodes={"B", "C", "E"},
        removed_edges={("A", "B"), ("B", "E")},
        added_updated_edges={("B", "C")},
    )
    graph = store.get_graph()
    assert list(graph.nodes) == ["B", "C", "E", "D"]
    assert {tuple(sorted(pair)) for pair in graph.edges} == {("B", "C"), ("C", "D")}
    assert graph.nodes["B"]["source_id"] == ["doc2"]
    # Descriptions and weights keep what doc1 contributed.
    assert graph.nodes["B"]["description"] == f"B in doc1{GRAPH_FIELD_SEP}B in doc2"
    assert graph.edges["B", "C"]["weight"] == 5.0
    assert graph.edges["B", "C"]["source_id"] == ["doc2"]
    assert store.document_ids() == ["doc2"]
    assert store.remove_document("doc1") == GraphChange()
    assert store.remove_document("never merged") == GraphChange()


def test_a_deleted_entity_takes_relations_the_document_did_not_name(store):
    graph = nx.Graph()
    graph.add_node("A", description="a", source_id=["doc1"])
    graph.add_node("F", description="f", source_id=["doc2"])
    graph.add_edge(
        "A", "F", description="a and f", keywords=[], weight=1.0, source_id=["doc2"]
    )
    graph.graph["source_id"] = ["doc1", "doc2"]
    store.import_graph(graph)

    assert store.remove_document("doc1") == GraphChange(
        removed_nodes={"A"}, removed_edges={("A", "F")}
    )
    stored = store.get_graph()
    assert list(stored.nodes) == ["F"]
    assert stored.number_of_edges() == 0


def test_document_change_names_what_documents_contributed(store):
    store.merge_subgraph(subgraph("doc1", ["A", "B"], [("A", "B", 1.0)]))
    store.merge_subgraph(subgraph("doc2", ["B", "C"], [("B", "C", 1.0)]))

    assert store.document_change(["doc2"]) == GraphChange(
        added_updated_nodes={"B", "C"}, added_updated_edges={("B", "C")}
    )
    assert store.document_change(["doc1", "doc2", "doc1"]) == GraphChange(
        added_updated_nodes={"A", "B", "C"},
        added_updated_edges={("A", "B"), ("B", "C")},
    )
    assert store.document_change(["missing"]) == GraphChange()
    with pytest.raises(ValueError):
        store.document_change([""])


def test_apply_change_writes_an_entity_resolution(store):
    store.merge_subgraph(
        subgraph(
            "doc1",
            ["ALICE", "ALICIA", "BOB", "CAROL"],
            [("ALICE", "BOB", 1.0), ("ALICIA", "BOB", 2.0), ("ALICIA", "CAROL", 3.0)],
        )
    )
    graph = store.get_graph()
    # Merge ALICIA into ALICE as RAGFlow's EntityResolution does.
    graph.nodes["ALICE"]["description"] += (
        GRAPH_FIELD_SEP + graph.nodes["ALICIA"]["description"]
    )
    graph.edges["ALICE", "BOB"]["weight"] += graph.edges["ALICIA", "BOB"]["weight"]
    graph.add_edge("ALICE", "CAROL", **copy.deepcopy(graph.edges["ALICIA", "CAROL"]))
    graph.remove_node("ALICIA")
    for name in graph.nodes:
        graph.nodes[name]["pagerank"] = 0.25
    # The change leaves out ALICE-CAROL, which the resolution moved onto ALICE.
    change = GraphChange(
        removed_nodes={"ALICIA"},
        added_updated_nodes={"ALICE"},
        removed_edges={("ALICIA", "BOB"), ("ALICIA", "CAROL")},
        added_updated_edges={("ALICE", "BOB")},
    )

    store.apply_change(graph, change)

    stored = store.get_graph()
    assert snapshot(stored) == snapshot(graph)
    assert "pagerank" not in stored.nodes["ALICE"]
    assert stored.nodes["ALICE"]["rank"] == 2
    assert store.document_ids() == ["doc1"]


def test_apply_change_deletes_an_entity_with_its_relations(store):
    store.merge_subgraph(
        subgraph("doc1", ["A", "B", "C"], [("A", "B", 1.0), ("B", "C", 1.0)])
    )
    graph = store.get_graph()
    graph.remove_node("C")

    store.apply_change(graph, GraphChange(removed_nodes={"C"}))

    assert snapshot(store.get_graph()) == snapshot(graph)


@pytest.mark.parametrize(
    "change",
    [
        GraphChange(removed_edges={("A",)}),
        GraphChange(added_updated_edges={("A", 1)}),
        GraphChange(removed_nodes={""}),
    ],
)
def test_apply_change_refuses_malformed_changes(store, change):
    with pytest.raises(ValueError):
        store.apply_change(nx.Graph(), change)


@path_only
def test_a_large_change_spans_transactions_and_completes_on_retry(store, monkeypatch):
    names = [f"N{i:02}" for i in range(12)]
    store.merge_subgraph(
        subgraph("doc1", names, [(a, b, 1.0) for a, b in pairwise(names)])
    )
    graph = store.get_graph()
    for name in names:
        graph.nodes[name]["description"] += " (resolved)"
    graph.remove_node("N00")
    change = GraphChange(removed_nodes={"N00"}, added_updated_nodes=set(names[1:]))
    monkeypatch.setattr(store_module, "_TRANSACTION_ROWS", 3)
    recorder = Recorder(monkeypatch)
    # Entities and relations are written before any is deleted, so failing
    # the delete leaves the updates and the entity it was to delete.
    recorder.fail = lambda statement: statement == store_module._DELETE_ENTITIES

    with pytest.raises(HawDBError, match="injected"):
        store.apply_change(graph, change)

    recorder.fail = None
    partial = store.get_graph()
    assert "N00" in partial and partial.nodes["N01"]["description"].endswith(
        "(resolved)"
    )
    recorder.events.clear()
    store.apply_change(graph, change)
    assert recorder.transactions() > 1
    assert snapshot(store.get_graph()) == snapshot(graph)


def test_import_graph_writes_a_whole_graph_and_skips_what_tidy_graph_purges(
    store, caplog
):
    graph = nx.Graph()
    graph.add_node(
        "A",
        entity_type="PERSON",
        description="a",
        source_id=["doc1"],
        rank=5,
        pagerank=0.3,
    )
    graph.add_node(
        "B", entity_type="PERSON", description="b", source_id=["doc1", "doc2"]
    )
    graph.add_node("NO DESCRIPTION", source_id=["doc1"])
    graph.add_node("NO DOCUMENT", description="x", source_id=[])
    graph.add_edge("A", "B", description="a and b", weight=2.0, source_id=["doc1"])
    graph.add_edge(
        "A", "NO DESCRIPTION", description="gone", weight=1.0, source_id=["doc1"]
    )
    graph.add_edge(
        "B", "NO DOCUMENT", description="gone", weight=1.0, source_id=["doc2"]
    )
    graph.add_edge("B", "B", weight=1.0, source_id=["doc2"])
    graph.graph["source_id"] = ["doc1", "doc2", "doc1"]

    with caplog.at_level(logging.WARNING, logger=store_module.__name__):
        store.import_graph(graph)

    assert "skipped 5 nodes and edges" in caplog.text
    stored = store.get_graph()
    assert list(stored.nodes) == ["A", "B"]
    assert stored.nodes["A"] == {
        "entity_type": "PERSON",
        "description": "a",
        "source_id": ["doc1"],
        "rank": 1,
    }
    # An edge without keywords gets an empty list, as tidy_graph gives it.
    assert stored.edges["A", "B"] == {
        "description": "a and b",
        "keywords": [],
        "weight": 2.0,
        "source_id": ["doc1"],
    }
    assert stored.graph["source_id"] == ["doc1", "doc2"]


def test_import_graph_needs_a_store_without_documents(store):
    store.merge_subgraph(subgraph("doc1", ["A"]))
    with pytest.raises(ValueError, match="holds no document"):
        store.import_graph(subgraph("doc2", ["B"]))
    assert store.document_ids() == ["doc1"]


@contextlib.contextmanager
def failing_before_transaction(number):
    # Makes an edit over several transactions fail before it runs the steps of
    # its transaction number, after the earlier ones committed.
    run_steps = store_module._run_steps
    calls = 0

    def failing(tx, steps):
        nonlocal calls
        calls += 1
        if calls == number:
            raise HawDBError(f"injected failure before transaction {number}")
        run_steps(tx, steps)

    store_module._run_steps = failing
    try:
        yield
    finally:
        store_module._run_steps = run_steps


UNFINISHED_IMPORT = r"import_graph\(\) into this store did not finish"


def test_an_import_stopped_after_some_documents_starts_over(store):
    # With the default limits, 5,001 documents take two transactions, and the
    # second one fails after the first committed 4,997 of them.
    documents = [f"doc{i:05}" for i in range(5001)]
    graph = nx.Graph()
    graph.add_node("HUB", entity_type="T", description="hub", source_id=documents)
    graph.add_node(
        "LEAF", entity_type="T", description="leaf", source_id=documents[-1:]
    )
    graph.add_edge(
        "HUB",
        "LEAF",
        description="d",
        keywords=[],
        weight=1.0,
        source_id=documents[-1:],
    )
    graph.graph["source_id"] = documents

    with failing_before_transaction(2), pytest.raises(HawDBError, match="injected"):
        store.import_graph(graph)

    committed = [row["doc_id"] for row in cypher(store, store_module._DOCUMENTS)]
    assert 0 < len(committed) < len(documents)
    assert meta(store)["importing"] is True
    assert store.holds_graph() is False
    # Nothing reads the partial graph or adds to it, so a document whose record
    # the import has not written yet cannot be merged into what it wrote.
    calls = [
        store.get_graph,
        store.document_ids,
        lambda: store.contains_document(documents[0]),
        lambda: store.n_hop_paths(["HUB"]),
        lambda: store.document_change(documents[:1]),
        lambda: store.merge_subgraph(subgraph(documents[-1], ["HUB"])),
        lambda: store.remove_document(documents[0]),
        lambda: store.apply_change(graph, GraphChange(added_updated_nodes={"HUB"})),
    ]
    for call in calls:
        with pytest.raises(StoreFormatError, match=UNFINISHED_IMPORT):
            call()

    store.import_graph(graph)

    assert store.holds_graph() is True
    assert meta(store)["importing"] is False
    assert store.document_ids() == documents
    assert snapshot(store.get_graph()) == snapshot(graph)
    # Every document was merged once: merging one of them again is skipped.
    assert store.merge_subgraph(subgraph(documents[-1], ["HUB"])) == GraphChange()
    assert store.get_graph().nodes["HUB"]["description"] == "hub"


@path_only
def test_an_import_that_stops_while_starting_over_finishes_on_retry(store, monkeypatch):
    names = [f"N{i:02}" for i in range(15)]
    graph = subgraph("doc1", names, [(a, b, 1.0) for a, b in pairwise(names)])
    graph.graph["source_id"] = ["doc1", "doc2"]
    monkeypatch.setattr(store_module, "_TRANSACTION_ROWS", 4)
    with failing_before_transaction(3), pytest.raises(HawDBError, match="injected"):
        store.import_graph(graph)
    assert cypher(store, store_module._DOCUMENTS) == []
    assert cypher(store, "MATCH (n:Entity) RETURN n.name AS name")

    # Starting over deletes what the import wrote, and that stops too.
    monkeypatch.setattr(store_module, "_DELETE_ROWS", 2)
    recorder = Recorder(monkeypatch)
    recorder.fail = lambda statement: statement == store_module._DELETE_ENTITY_RANGE
    with pytest.raises(HawDBError, match="injected"):
        store.import_graph(graph)
    recorder.fail = None
    assert meta(store)["importing"] is True and meta(store)["clearing"] is True
    assert store.holds_graph() is False
    with pytest.raises(StoreFormatError, match=UNFINISHED_IMPORT):
        store.get_graph()

    recorder.events.clear()
    store.import_graph(graph)

    assert recorder.transactions() > 1
    assert store.document_ids() == ["doc1", "doc2"]
    assert snapshot(store.get_graph()) == snapshot(graph)
    # Starting over deleted rows, so the call checkpointed.
    assert "CHECKPOINT" in recorder.statements("autocommit")


def test_importing_an_empty_graph_into_an_empty_database_writes_nothing(store):
    # A database that holds nothing yet, as one opened with hawdb.open, gets
    # no bookkeeping node, so its generation stays "".
    if store.path is not None:
        os.makedirs(os.path.dirname(store.path), exist_ok=True)
    write_cypher(store, "MATCH (n) RETURN count(n) AS nodes")
    store.import_graph(nx.Graph())

    assert meta(store) is None
    assert store.generation() == ""
    assert store.holds_graph() is False


def test_importing_an_empty_graph_discards_an_unfinished_import(store, monkeypatch):
    graph = subgraph("doc1", ["A", "B", "C"], [("A", "B", 1.0), ("B", "C", 1.0)])
    graph.graph["source_id"] = ["doc1", "doc2"]
    monkeypatch.setattr(store_module, "_TRANSACTION_ROWS", 4)
    with failing_before_transaction(2), pytest.raises(HawDBError, match="injected"):
        store.import_graph(graph)
    assert store.holds_graph() is False

    generation = store.generation()
    store.import_graph(nx.Graph())

    after = meta(store)
    assert (after["importing"], after["clearing"], after["next_seq"]) == (
        False,
        False,
        0,
    )
    # Discarding the import is no clear: the generation stays.
    assert store.generation() == generation
    assert store.get_graph() is None
    assert store.holds_graph() is False
    store.merge_subgraph(subgraph("doc3", ["X"]))
    assert store.document_ids() == ["doc3"]


@path_only
def test_an_import_that_fits_one_transaction_never_marks_the_graph(store, monkeypatch):
    recorder = Recorder(monkeypatch)

    store.import_graph(subgraph("doc1", ["A", "B"], [("A", "B", 1.0)]))

    assert recorder.transactions() == 1
    assert store_module._SET_IMPORTING not in recorder.statements("write")
    assert meta(store)["importing"] is False
    assert store.holds_graph() is True


def test_holds_graph_once_a_document_is_merged_or_imported(store):
    assert store.holds_graph() is False
    store.merge_subgraph(subgraph("doc1", ["A"]))
    assert store.holds_graph() is True
    store.remove_document("doc1")
    assert store.holds_graph() is False
    store.import_graph(subgraph("doc2", ["B"]))
    assert store.holds_graph() is True
    store.clear()
    assert store.holds_graph() is False


def test_clear_deletes_the_graph_and_the_store_starts_over(store):
    store.merge_subgraph(subgraph("doc1", ["A", "B"], [("A", "B", 1.0)]))
    before = store.generation()

    store.clear()

    assert store.get_graph() is None
    assert store.document_ids() == []
    assert cypher(store, "MATCH (n) RETURN count(n) AS nodes") == [{"nodes": 1}]
    after = meta(store)
    assert (after["next_seq"], after["clearing"], after["importing"]) == (
        0,
        False,
        False,
    )
    assert before and store.generation() not in ("", before)
    store.merge_subgraph(subgraph("doc1", ["A"]))
    assert store.document_ids() == ["doc1"]
    assert store.get_graph().nodes["A"]["description"] == "A in doc1"


def test_a_graph_read_before_a_clear_is_not_imported_after_it(store):
    # RAGFlow's first use of a dataset in a process, interleaved with a wipe of
    # the dataset's graph. The first use reads the store's generation, finds no
    # graph in the store, reads the graph the document engine holds, and
    # imports it; the wipe clears the store, deletes the chunks and clears the
    # store again.
    old = subgraph("doc1", ["A", "B"], [("A", "B", 1.0)])

    # The first use read the old graph before the wipe, and imports after it.
    generation = store.generation()
    assert store.holds_graph() is False
    store.clear()
    store.clear()
    with pytest.raises(StaleImportError, match="cleared, dropped or created"):
        store.import_graph(old, generation=generation)
    assert store.get_graph() is None
    assert store.holds_graph() is False

    # It read the generation after the first clear and imported before the
    # second, which deletes the graph.
    generation = store.generation()
    store.import_graph(old, generation=generation)
    assert store.document_ids() == ["doc1"]
    store.clear()
    assert store.get_graph() is None
    with pytest.raises(StaleImportError):
        store.import_graph(old, generation=generation)

    # One that starts after the wipe finds no graph in the document engine,
    # and an import read in the current generation is written.
    store.import_graph(nx.Graph(), generation=store.generation())
    assert store.holds_graph() is False
    store.import_graph(old, generation=store.generation())
    assert store.document_ids() == ["doc1"]
    with pytest.raises(TypeError):
        store.import_graph(nx.Graph(), generation=7)


@path_only
def test_clear_on_a_missing_path_creates_the_database_to_record_its_generation(store):
    assert store.generation() == ""

    store.clear()

    assert os.path.isfile(os.path.join(store.path, "manifest.hawdb"))
    assert store.generation() != ""
    assert store.get_graph() is None
    # An import of a graph read before the clear, while there was no database.
    with pytest.raises(StaleImportError):
        store.import_graph(subgraph("doc1", ["A"]), generation="")
    assert store.document_ids() == []


@path_only
def test_an_import_after_a_drop_is_refused_and_creates_nothing(store):
    store.merge_subgraph(subgraph("doc1", ["A"]))
    generation = store.generation()
    store.drop()

    for graph in (subgraph("doc2", ["B"]), nx.Graph()):
        with pytest.raises(StaleImportError):
            store.import_graph(graph, generation=generation)

    assert os.listdir(os.path.dirname(store.path)) == []


def test_an_import_is_refused_once_another_call_created_the_store(store):
    # RAGFlow's first use of a new dataset reads the generation of a store
    # that holds nothing yet, and another process merges the first document
    # before the import.
    generation = store.generation()
    assert generation == ""
    store.merge_subgraph(subgraph("doc1", ["A"]))
    assert store.generation() != ""

    for graph in (subgraph("doc2", ["B"]), nx.Graph()):
        with pytest.raises(StaleImportError, match="created"):
            store.import_graph(graph, generation=generation)

    assert store.document_ids() == ["doc1"]


def test_an_import_that_starts_over_keeps_the_generation(store, monkeypatch):
    graph = subgraph("doc1", ["A", "B", "C"], [("A", "B", 1.0), ("B", "C", 1.0)])
    graph.graph["source_id"] = ["doc1", "doc2"]
    monkeypatch.setattr(store_module, "_TRANSACTION_ROWS", 4)
    with failing_before_transaction(2), pytest.raises(HawDBError, match="injected"):
        store.import_graph(graph, generation=store.generation())
    # The stopped import created the store's metadata, with a generation.
    generation = store.generation()
    assert generation != "" and store.holds_graph() is False

    # A stale generation is refused before the unfinished import is deleted.
    with pytest.raises(StaleImportError):
        store.import_graph(graph, generation="")
    assert meta(store)["importing"] is True
    assert cypher(store, "MATCH (n:Entity) RETURN n.name AS name")

    store.import_graph(graph, generation=generation)

    assert store.generation() == generation
    assert store.document_ids() == ["doc1", "doc2"]
    assert snapshot(store.get_graph()) == snapshot(graph)


@path_only
def test_an_interrupted_clear_fails_every_call_until_clear_finishes(store, monkeypatch):
    names = [f"N{i:02}" for i in range(10)]
    store.merge_subgraph(
        subgraph("doc1", names, [(a, b, 1.0) for a, b in pairwise(names)])
    )
    monkeypatch.setattr(store_module, "_DELETE_ROWS", 2)
    recorder = Recorder(monkeypatch)
    recorder.fail = lambda statement: statement == store_module._DELETE_ENTITY_RANGE

    with pytest.raises(HawDBError, match="injected"):
        store.clear()

    recorder.fail = None
    calls = [
        store.get_graph,
        store.document_ids,
        store.holds_graph,
        lambda: store.contains_document("doc1"),
        lambda: store.n_hop_paths(["N01"]),
        lambda: store.document_change(["doc1"]),
        lambda: store.merge_subgraph(subgraph("doc2", ["X"])),
        lambda: store.remove_document("doc1"),
        lambda: store.apply_change(nx.Graph(), GraphChange()),
        lambda: store.import_graph(subgraph("doc3", ["Y"])),
    ]
    for call in calls:
        with pytest.raises(StoreFormatError, match="did not finish"):
            call()
    store.clear()
    assert store.get_graph() is None
    store.merge_subgraph(subgraph("doc2", ["X"]))
    assert store.document_ids() == ["doc2"]


def test_a_database_with_another_layout_is_refused(store):
    store.merge_subgraph(subgraph("doc1", ["A"]))
    write_cypher(
        store,
        "MATCH (g:RAGFlowGraph {key: 'graph'}) SET g.version = $version",
        {"version": 99},
    )

    calls = [
        store.get_graph,
        store.document_ids,
        store.holds_graph,
        lambda: store.contains_document("doc1"),
        lambda: store.n_hop_paths(["A"]),
        lambda: store.document_change(["doc1"]),
        lambda: store.merge_subgraph(subgraph("doc2", ["B"])),
        lambda: store.remove_document("doc1"),
        lambda: store.import_graph(subgraph("doc3", ["C"])),
        lambda: store.import_graph(subgraph("doc3", ["C"]), generation=""),
        store.generation,
        store.clear,
    ]
    for call in calls:
        with pytest.raises(StoreFormatError, match="layout 99"):
            call()


@path_only
def test_a_store_on_a_path_checkpoints_after_writing_enough(store, monkeypatch, caplog):
    recorder = Recorder(monkeypatch)
    store.merge_subgraph(subgraph("doc1", ["A", "B"], [("A", "B", 1.0)]))
    first = meta(store)["written"]
    store.merge_subgraph(subgraph("doc2", ["B", "C"], [("B", "C", 1.0)]))
    assert meta(store)["written"] > first > 0
    assert "CHECKPOINT" not in recorder.statements("autocommit")

    monkeypatch.setattr(store_module, "_CHECKPOINT_BYTES", 1)
    recorder.events.clear()
    store.merge_subgraph(subgraph("doc3", ["C", "D"], [("C", "D", 1.0)]))
    ran = [(scope, statement) for scope, statement, _ in recorder.events]
    checkpoint = ran.index(("autocommit", "CHECKPOINT"))
    assert ran[checkpoint - 1] == ("write", "COMMIT")
    assert ran[checkpoint + 1 :] == [
        ("write", "BEGIN"),
        ("write", store_module._RESET_WRITTEN),
        ("write", "COMMIT"),
    ]
    assert meta(store)["written"] == 0

    # A failed checkpoint leaves the write committed, and the next one tries again.
    recorder.fail = lambda statement: statement == "CHECKPOINT"
    with caplog.at_level(logging.WARNING, logger=store_module.__name__):
        change = store.merge_subgraph(subgraph("doc4", ["D", "E"], [("D", "E", 1.0)]))
    recorder.fail = None
    assert change.added_updated_nodes == {"D", "E"}
    assert "CHECKPOINT of" in caplog.text
    assert meta(store)["written"] > 0
    assert store.document_ids() == ["doc1", "doc2", "doc3", "doc4"]

    # Deleting a graph checkpoints too.
    monkeypatch.setattr(store_module, "_CHECKPOINT_BYTES", 16 << 20)
    recorder.events.clear()
    store.clear()
    assert "CHECKPOINT" in recorder.statements("autocommit")


@path_only
def test_drop_deletes_the_database_and_a_later_write_creates_it_again(store):
    store.merge_subgraph(subgraph("doc1", ["A"]))

    store.drop()

    # Neither the database, its lock file nor the renamed directory is left.
    assert os.listdir(os.path.dirname(store.path)) == []
    assert store.get_graph() is None
    store.drop()
    store.merge_subgraph(subgraph("doc2", ["A"]))
    assert store.document_ids() == ["doc2"]

    # The new database has the store's constraints again.
    def constraints(database):
        return {
            row["property_name"]
            for row in database.execute_sql(
                store_module._SQL_UNIQUE, ["Entity"]
            ).fetchall()
        }

    assert on_database(store, constraints, read_only=True) == {"name"}


@path_only
def test_a_database_another_process_deleted_gets_its_schema_again(store):
    store.merge_subgraph(subgraph("doc1", ["A"]))
    # Another process drops the database, so this one's record of the schema
    # it created is stale.
    shutil.rmtree(store.path)
    assert store.path in store_module._SCHEMA_READY

    store.merge_subgraph(subgraph("doc2", ["A"]))

    def constraints(database):
        return {
            label: {
                row["property_name"]
                for row in database.execute_sql(
                    store_module._SQL_UNIQUE, [label]
                ).fetchall()
            }
            for label, _ in store_module._CONSTRAINTS
        }

    assert on_database(store, constraints, read_only=True) == {
        label: {key} for label, key in store_module._CONSTRAINTS
    }


def test_drop_deletes_only_a_hawdb_database(tmp_path):
    other = tmp_path / "tenant" / "kb"
    other.mkdir(parents=True)
    (other / "notes.txt").write_text("keep")
    with pytest.raises(StoreFormatError, match="not a HawDB database"):
        HawDBGraphStore(other).drop()
    assert (other / "notes.txt").read_text() == "keep"
    assert os.listdir(other.parent) == ["kb"]

    real = tmp_path / "real"
    HawDBGraphStore(real).merge_subgraph(subgraph("doc1", ["A"]))
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(StoreFormatError, match="not a HawDB database"):
        HawDBGraphStore(link).drop()
    assert HawDBGraphStore(real).document_ids() == ["doc1"]
    assert not (tmp_path / "link.lock").exists()


def renamed_by_drops(store):
    return glob.glob(glob.escape(store.path) + ".dropping-*")


def lock_file_text(store):
    return Path(store.path + ".lock").read_text(encoding="utf-8")


@contextlib.contextmanager
def drops_stopping_after_the_rename(monkeypatch, *, delete_manifest=False):
    # Makes a drop stop after it renamed the database, as a process killed
    # while it deletes the files would, optionally after deleting one.
    def rmtree_partly(path, *args, **kwargs):
        if delete_manifest:
            os.remove(os.path.join(path, "manifest.hawdb"))
        raise OSError("injected failure while deleting")

    with monkeypatch.context() as patch:
        patch.setattr(shutil, "rmtree", rmtree_partly)
        yield


@path_only
def test_a_drop_that_stops_partway_leaves_nothing_at_the_path(store, monkeypatch):
    store.merge_subgraph(subgraph("doc1", ["A"]))

    with (
        drops_stopping_after_the_rename(monkeypatch, delete_manifest=True),
        pytest.raises(OSError, match="injected"),
    ):
        store.drop()

    assert not os.path.exists(store.path)
    [renamed] = renamed_by_drops(store)
    assert not os.path.exists(os.path.join(renamed, "manifest.hawdb"))
    # The lock file records the directory the drop renamed.
    record = json.loads(lock_file_text(store))
    assert record["renamed_to"] == os.path.basename(renamed)
    identity = os.lstat(renamed)
    assert (record["device"], record["inode"]) == (identity.st_dev, identity.st_ino)
    assert store.get_graph() is None
    store.merge_subgraph(subgraph("doc2", ["B"]))
    assert store.document_ids() == ["doc2"]

    # The next drop deletes what the stopped one left, although it no longer
    # holds a manifest, and then the new database.
    store.drop()

    assert os.listdir(os.path.dirname(store.path)) == []


@path_only
def test_drop_deletes_no_directory_it_did_not_rename(store):
    parent = Path(store.path).parent
    store.merge_subgraph(subgraph("doc1", ["A"]))
    # Directories named like the ones drops rename, which no drop renamed.
    others = sorted(["kb.dropping", "kb.dropping-" + "0" * 32, "kb.dropping-x"])
    for name in others:
        (parent / name / "keep").mkdir(parents=True)
        (parent / name / "keep" / "notes.txt").write_text("keep")

    store.drop()

    assert sorted(os.listdir(parent)) == others
    for name in others:
        assert (parent / name / "keep" / "notes.txt").read_text() == "keep"


@path_only
def test_drop_deletes_no_other_directory_at_a_recorded_name(store, monkeypatch):
    parent = Path(store.path).parent
    store.merge_subgraph(subgraph("doc1", ["A"]))
    with (
        drops_stopping_after_the_rename(monkeypatch),
        pytest.raises(OSError, match="injected"),
    ):
        store.drop()
    [renamed] = renamed_by_drops(store)
    # The renamed database moves away, which keeps its identity in use, and
    # another directory, and then a link to the database, takes the name.
    moved = renamed + "-moved"
    os.rename(renamed, moved)
    os.makedirs(os.path.join(renamed, "keep"))
    store.merge_subgraph(subgraph("doc2", ["B"]))
    for replace in (None, lambda: os.symlink(moved, renamed, target_is_directory=True)):
        if replace is not None:
            shutil.rmtree(renamed)
            replace()
        before = files_under(parent)
        with pytest.raises(StoreFormatError, match="is not the directory a drop"):
            store.drop()
        assert files_under(parent) == before
        assert store.document_ids() == ["doc2"]


@path_only
def test_drop_stops_when_the_new_name_is_taken(store, monkeypatch):
    parent = Path(store.path).parent
    store.merge_subgraph(subgraph("doc1", ["A"]))
    monkeypatch.setattr(store_module, "_new_token", lambda: "0" * 32)
    # An empty directory, which a rename would replace on POSIX systems.
    taken = parent / ("kb.dropping-" + "0" * 32)
    taken.mkdir()

    with pytest.raises(StoreFormatError, match="exists already"):
        store.drop()

    # The drop's statement may add files to the database, as any read does,
    # but nothing was renamed or deleted.
    assert sorted(os.listdir(parent)) == ["kb", "kb.dropping-" + "0" * 32, "kb.lock"]
    assert os.listdir(taken) == []
    assert lock_file_text(store) == ""
    assert store.document_ids() == ["doc1"]


@path_only
def test_drop_refuses_a_lock_file_that_holds_something_else(store):
    store.merge_subgraph(subgraph("doc1", ["A"]))
    others = [
        "the lock of another program",
        # A record that would name a directory no drop of this store renames.
        json.dumps({"renamed_to": "../elsewhere", "device": 1, "inode": 2}),
        json.dumps({"renamed_to": "kb.dropping-" + "0" * 31, "device": 1, "inode": 2}),
        json.dumps(
            {"renamed_to": "kb.dropping-" + "0" * 32, "device": True, "inode": 2}
        ),
        # Nested deeper than the JSON decoder of Python 3.11 recurses, though
        # 3.13's parses it, and a record longer than the store reads.
        "[" * 2000 + "]" * 2000,
        json.dumps({"renamed_to": "kb.dropping-" + "0" * 32, "device": 1, "inode": 2})
        + " " * 5000,
    ]
    for text in others:
        Path(store.path + ".lock").write_text(text, encoding="utf-8")
        with pytest.raises(StoreFormatError, match="holds something other"):
            store.drop()
        assert lock_file_text(store) == text
        # Other calls only lock the file.
        assert store.document_ids() == ["doc1"]


@path_only
def test_a_drop_the_system_refuses_to_rename_deletes_nothing(store, monkeypatch):
    parent = Path(store.path).parent
    store.merge_subgraph(subgraph("doc1", ["A"]))

    def refuse(source, target):
        raise PermissionError(f"injected refusal to rename {source}")

    # As Windows may refuse to rename the directory of an open database.
    with monkeypatch.context() as patch:
        patch.setattr(os, "rename", refuse)
        with pytest.raises(PermissionError, match="injected"):
            store.drop()

    assert sorted(os.listdir(parent)) == ["kb", "kb.lock"]
    assert lock_file_text(store) == ""
    assert store.document_ids() == ["doc1"]


@path_only
@pytest.mark.skipif(os.name == "nt", reason="Windows does not sync directories")
def test_a_drop_whose_directory_sync_fails_keeps_its_record(store, monkeypatch):
    parent = os.path.dirname(store.path)
    store.merge_subgraph(subgraph("doc1", ["A"]))
    real_fsync = os.fsync
    directory_syncs = 0

    def fsync(fd):
        nonlocal directory_syncs
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            directory_syncs += 1
            # The barrier after the deletion. The one after the rename stands.
            if directory_syncs >= 2:
                raise OSError(errno.EIO, "injected directory sync failure")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)
    with pytest.raises(OSError, match="injected directory sync failure") as raised:
        store.drop()
    assert raised.value.errno == errno.EIO

    # The renamed directory is already gone, and the record is still there.
    assert not os.path.exists(store.path)
    assert renamed_by_drops(store) == []
    record = json.loads(lock_file_text(store))
    assert record["renamed_to"].startswith(os.path.basename(store.path) + ".dropping-")

    synced_before_clear = False
    real_sync = store_module._sync_directory
    real_record = store_module._LifecycleLock.record_drop

    def sync_directory(directory):
        nonlocal synced_before_clear
        assert directory == parent
        real_sync(directory)
        synced_before_clear = True

    def record_drop(self, value):
        if value is None:
            assert synced_before_clear
        real_record(self, value)

    monkeypatch.setattr(os, "fsync", real_fsync)
    monkeypatch.setattr(store_module, "_sync_directory", sync_directory)
    monkeypatch.setattr(store_module._LifecycleLock, "record_drop", record_drop)

    store.drop()

    assert synced_before_clear
    assert os.listdir(parent) == []


@path_only
@pytest.mark.skipif(os.name == "nt", reason="Windows does not sync directories")
def test_a_directory_sync_a_file_system_does_not_support_still_drops(
    store, monkeypatch
):
    parent = os.path.dirname(store.path)
    store.merge_subgraph(subgraph("doc1", ["A"]))
    real_fsync = os.fsync

    def fsync(fd):
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError(errno.EINVAL, "directory sync is not supported")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fsync)

    store.drop()

    assert os.listdir(parent) == []


def test_only_a_store_on_a_path_drops_its_database():
    database = hawdb.open()
    with pytest.raises(TypeError):
        HawDBGraphStore(database).drop()
    database.close()


@pytest.mark.parametrize("element", ["entity", "relation"])
def test_entities_and_relations_above_the_size_limits_are_refused(store, element):
    sub = subgraph("doc1", ["A", "B"], [("A", "B", 1.0)])
    if element == "entity":
        sub.nodes["A"]["description"] = "x" * MAX_ENTITY_BYTES
    else:
        sub.edges["A", "B"]["description"] = "x" * MAX_RELATION_BYTES

    with pytest.raises(ValueError, match=f"{element} .* bytes"):
        store.merge_subgraph(sub)

    assert store.get_graph() is None


def test_a_merge_that_would_grow_an_entity_past_the_limit_writes_nothing(store):
    first = subgraph("doc1", ["A"])
    first.nodes["A"]["description"] = "x" * (MAX_ENTITY_BYTES - 64)
    store.merge_subgraph(first)
    second = subgraph("doc2", ["A", "B"], [("A", "B", 1.0)])
    second.nodes["A"]["description"] = "y" * 100

    with pytest.raises(ValueError, match="entity 'A'"):
        store.merge_subgraph(second)

    assert store.document_ids() == ["doc1"]
    assert list(store.get_graph().nodes) == ["A"]


def test_entities_and_relations_near_the_limits_read_back_and_traverse_two_hops(store):
    entity = "e" * (MAX_ENTITY_BYTES - 4096)
    relation = "r" * (MAX_RELATION_BYTES - 4096)
    first = subgraph("doc1", ["A"])
    first.nodes["A"]["description"] = entity
    store.merge_subgraph(first)
    for doc, (old, new) in (("doc2", ("A", "B")), ("doc3", ("B", "C"))):
        sub = subgraph(doc, [old, new], [(old, new, 1.0)])
        sub.nodes[new]["description"] = entity
        sub.edges[old, new]["description"] = relation
        store.merge_subgraph(sub)

    assert store.n_hop_paths(["A"]) == {
        "A": [{"path": ["A", "B", "C"], "weights": [1.0, 1.0]}]
    }
    graph = store.get_graph()
    assert graph.nodes["C"]["description"] == entity
    assert graph.edges["A", "B"]["description"] == relation


@path_only
def test_a_merge_too_large_for_one_wal_record_writes_nothing(store):
    sub = subgraph("doc1", [f"N{i}" for i in range(5)])
    for name in sub.nodes:
        sub.nodes[name]["description"] = "x" * (MAX_ENTITY_BYTES - 4096)

    with pytest.raises(HawDBError, match="WAL record"):
        store.merge_subgraph(sub)

    assert store.get_graph() is None
    assert store.document_ids() == []


def test_names_with_cypher_syntax_are_plain_values(store):
    sub = subgraph(
        "doc'1", HOSTILE, [(HOSTILE[0], HOSTILE[1], 1.0), (HOSTILE[2], HOSTILE[3], 2.0)]
    )

    store.merge_subgraph(sub)

    assert list(store.get_graph().nodes) == HOSTILE
    assert store.contains_document("doc'1")
    assert store.n_hop_paths([HOSTILE[0]], n_hop=1) == {
        HOSTILE[0]: [{"path": HOSTILE[:2], "weights": [1.0]}]
    }
    assert store.remove_document("doc'1").removed_nodes == set(HOSTILE)
    assert store.get_graph() is None


def test_attributes_read_back_as_written_except_those_ragflow_derives(store):
    sub = subgraph("doc1", ["A", "B"], [("A", "B", 2)])
    sub.nodes["A"].update(
        entity_type=7,
        nested={"list": [1, 2.5, None, True], "text": "名前"},
        flag=False,
        rank=99,
        pagerank=0.5,
    )
    sub.edges["A", "B"].update(keywords="not a list", extra=[{"x": 1}])

    store.merge_subgraph(copy.deepcopy(sub))

    graph = store.get_graph()
    expected = {
        key: value
        for key, value in sub.nodes["A"].items()
        if key not in ("rank", "pagerank")
    }
    assert {
        key: value for key, value in graph.nodes["A"].items() if key != "rank"
    } == expected
    assert graph.nodes["A"]["rank"] == 1
    assert graph.edges["A", "B"] == sub.edges["A", "B"]


@pytest.mark.parametrize(
    "limits",
    [
        {
            "_NAME_PAGE": 2,
            "_READ_PAGE_ROWS": 1,
            "_RANGE_ROWS": 2,
            "_STATEMENT_ROWS": 2,
            "_TRANSACTION_ROWS": 3,
        },
        {
            "_READ_PAGE_BYTES": 1,
            "_RANGE_BYTES": 1,
            "_STATEMENT_BYTES": 1,
            "_TRANSACTION_BYTES": 1,
        },
    ],
    ids=["rows", "bytes"],
)
def test_small_pages_and_ranges_give_the_same_graph(store, monkeypatch, limits):
    def history(target):
        target.merge_subgraph(
            subgraph("doc1", ["A", "B", "C", "D"], [("A", "B", 1.0), ("B", "C", 2.0)])
        )
        target.merge_subgraph(
            subgraph(
                "doc2",
                ["B", "C", "E", "F"],
                [("B", "C", 1.0), ("E", "F", 3.0), ("C", "E", 1.0)],
            )
        )
        target.merge_subgraph(
            subgraph("doc3", ["A", "F", "G"], [("A", "F", 2.0), ("F", "G", 1.0)])
        )
        graph = target.get_graph()
        graph.nodes["B"]["description"] += (
            GRAPH_FIELD_SEP + graph.nodes["D"]["description"]
        )
        graph.remove_node("D")
        target.apply_change(
            graph, GraphChange(removed_nodes={"D"}, added_updated_nodes={"B"})
        )
        target.remove_document("doc2")
        graph = target.get_graph()
        return (
            snapshot(graph),
            list(graph.nodes),
            target.n_hop_paths(list(graph.nodes)),
            target.document_ids(),
        )

    database = hawdb.open()
    expected = history(HawDBGraphStore(database))
    database.close()
    for name, value in limits.items():
        monkeypatch.setattr(store_module, name, value)

    assert history(store) == expected


def test_a_store_over_an_open_database_runs_only_on_its_thread():
    database = hawdb.open()
    store = HawDBGraphStore(database)
    store.merge_subgraph(subgraph("doc1", ["A"]))
    outcomes = []

    def call_from_another_thread():
        for call in (
            store.get_graph,
            store.document_ids,
            lambda: store.n_hop_paths(["A"]),
            lambda: store.merge_subgraph(subgraph("doc2", ["B"])),
        ):
            try:
                call()
                outcomes.append("ran")
            except WrongThreadError:
                outcomes.append("refused")

    thread = threading.Thread(target=call_from_another_thread)
    thread.start()
    thread.join()

    assert outcomes == ["refused"] * 4
    assert store.document_ids() == ["doc1"]
    database.close()


def test_stores_on_one_path_serve_concurrent_threads(tmp_path):
    path = tmp_path / "kb"
    errors = []

    def merge(doc):
        try:
            HawDBGraphStore(path).merge_subgraph(
                subgraph(doc, ["HUB", f"{doc} ONLY"], [("HUB", f"{doc} ONLY", 1.0)])
            )
        except Exception as error:
            errors.append(repr(error))

    threads = [threading.Thread(target=merge, args=(f"doc{i}",)) for i in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    store = HawDBGraphStore(path)
    assert sorted(store.document_ids()) == [f"doc{i}" for i in range(6)]
    hub = store.get_graph().nodes["HUB"]
    assert hub["source_id"] == store.document_ids()
    assert hub["description"].count(GRAPH_FIELD_SEP) == 5


# Holds a database the way another RAGFlow process does: a process that has
# run a statement on a HawDB database holds it until it closes it.
HOLDER = """
import sys
import hawdb
database = hawdb.open(sys.argv[1], read_only=True)
database.execute("MATCH (n:Entity) RETURN count(n) AS entities").fetchall()
print("holding", flush=True)
sys.stdin.read()
database.close()
"""


# Holds the lock file of a database the way a store in another process does
# while it uses the database, including before it has run a statement on it.
LOCKER = """
import sys
import time
from ragflow_hawdb import store
lock = store._LifecycleLock(sys.argv[1])
assert lock.acquire(time.monotonic() + 60, create=True)
print("holding", flush=True)
sys.stdin.read()
lock.release()
"""

# The directory this module imported ragflow_hawdb from, for other processes.
PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(store_module.__file__)))


@contextlib.contextmanager
def in_another_process(script, path):
    paths = [PACKAGE_ROOT, os.environ.get("PYTHONPATH", "")]
    holder = subprocess.Popen(
        [sys.executable, "-c", script, os.fspath(path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(filter(None, paths))},
    )
    try:
        assert holder.stdout.readline().strip() == "holding"
        yield holder
    finally:
        with contextlib.suppress(OSError, ValueError):
            holder.stdin.close()
        holder.wait(timeout=60)
        holder.stdout.close()


def held(path):
    return in_another_process(HOLDER, path)


# Opens a database with hawdb.open, as a program that uses no store does, and
# reports whether a statement took it.
GRAB = """
import sys
import hawdb
try:
    database = hawdb.open(sys.argv[1], read_only=True)
    database.execute("MATCH (n:Entity) RETURN count(n) AS entities").fetchall()
except Exception as error:
    print(f"refused: {error}")
else:
    print("took it")
    database.close()
"""


def grab(path):
    return subprocess.run(
        [sys.executable, "-c", GRAB, os.fspath(path)],
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    ).stdout.strip()


def locked(path):
    return in_another_process(LOCKER, path)


def files_under(path):
    return sorted(
        os.path.relpath(os.path.join(root, name), path)
        for root, directories, files in os.walk(path)
        for name in directories + files
    )


def test_a_call_gives_up_on_a_database_another_process_holds(tmp_path):
    store = HawDBGraphStore(tmp_path / "kb", busy_timeout=0.3)
    store.merge_subgraph(subgraph("doc1", ["A"]))

    with held(store.path):
        start = time.monotonic()
        with pytest.raises(StoreBusyError):
            store.get_graph()
        with pytest.raises(StoreBusyError):
            store.merge_subgraph(subgraph("doc2", ["B"]))
        assert time.monotonic() - start >= 0.6

    assert issubclass(StoreBusyError, TimeoutError)
    assert store.document_ids() == ["doc1"]


def test_a_call_proceeds_once_the_other_process_closes_the_database(tmp_path):
    store = HawDBGraphStore(tmp_path / "kb", busy_timeout=60)
    store.merge_subgraph(subgraph("doc1", ["A"]))

    with held(store.path) as holder:
        release = threading.Timer(0.5, holder.stdin.close)
        release.start()
        start = time.monotonic()
        store.merge_subgraph(subgraph("doc2", ["B"]))
        waited = time.monotonic() - start
        release.join()

    assert waited >= 0.4
    assert store.document_ids() == ["doc1", "doc2"]


def test_drop_waits_for_another_process_to_close_the_database(tmp_path):
    store = HawDBGraphStore(tmp_path / "kb", busy_timeout=0.3)
    store.merge_subgraph(subgraph("doc1", ["A"]))

    with held(store.path) as holder:
        before = files_under(tmp_path)
        start = time.monotonic()
        with pytest.raises(StoreBusyError):
            store.drop()
        assert time.monotonic() - start >= 0.3
        # Nothing was deleted, renamed or created.
        assert files_under(tmp_path) == before

        # A drop that waits deletes the database once the other process
        # closes it.
        release = threading.Timer(0.5, holder.stdin.close)
        release.start()
        start = time.monotonic()
        reopened(store, busy_timeout=60).drop()
        waited = time.monotonic() - start
        release.join()

    assert waited >= 0.4
    assert os.listdir(tmp_path) == []


def test_drop_waits_while_a_handle_in_this_process_holds_the_database(tmp_path):
    store = HawDBGraphStore(tmp_path / "kb", busy_timeout=0.3)
    store.merge_subgraph(subgraph("doc1", ["A"]))
    holder = hawdb.open(store.path, read_only=True)
    holder.execute("MATCH (n:Entity) RETURN count(n) AS entities").fetchall()
    before = files_under(tmp_path)

    with pytest.raises(StoreBusyError):
        store.drop()

    assert files_under(tmp_path) == before
    holder.close()
    del holder
    store.drop()
    assert os.listdir(tmp_path) == []


def test_drop_renames_and_deletes_the_database_while_it_holds_it(tmp_path, monkeypatch):
    store = HawDBGraphStore(tmp_path / "kb", busy_timeout=0.3)
    store.merge_subgraph(subgraph("doc1", ["A"]))
    # Opened with hawdb.open before the drop, but not yet taken, since a
    # handle takes a database at its first statement.
    early = hawdb.open(store.path)
    rename = os.rename
    rmtree = shutil.rmtree
    seen = []

    def rename_while_others_try(source, target):
        # Another process tries to take the database at the moment the drop
        # retires the path: before the rename, after it at the path, and
        # after it at the new name.
        seen.append(grab(source))
        rename(source, target)
        seen.append(grab(source))
        seen.append(grab(target))

    def delete_while_others_try(target, *args, **kwargs):
        # And once more at the new name when the drop starts deleting it.
        seen.append(grab(target))
        rmtree(target, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, "rename", rename_while_others_try)
        patch.setattr(shutil, "rmtree", delete_while_others_try)
        store.drop()

    assert len(seen) == 4, seen
    assert seen[0].startswith("refused: branch is busy"), seen
    assert seen[1].startswith("refused:") and "does not exist" in seen[1], seen
    assert seen[2].startswith("refused: branch is busy"), seen
    assert seen[3].startswith("refused: branch is busy"), seen
    # The handle opened before the drop finds no database to take.
    with pytest.raises(HawDBError, match="No such file or directory"):
        early.execute("MATCH (n:Entity) RETURN count(n) AS entities").fetchall()
    early.close()
    del early
    assert os.listdir(tmp_path) == []


def test_a_store_in_another_process_keeps_drop_and_other_calls_waiting(tmp_path):
    store = HawDBGraphStore(tmp_path / "kb", busy_timeout=0.3)
    store.merge_subgraph(subgraph("doc1", ["A"]))

    # The other process holds the lock file, as a store does from before it
    # opens the database until after it closes it.
    with locked(store.path) as locker:
        before = files_under(tmp_path)
        calls = [
            store.drop,
            store.get_graph,
            lambda: store.merge_subgraph(subgraph("doc2", ["B"])),
        ]
        for call in calls:
            with pytest.raises(StoreBusyError):
                call()
        assert files_under(tmp_path) == before

        release = threading.Timer(0.5, locker.stdin.close)
        release.start()
        start = time.monotonic()
        reopened(store, busy_timeout=60).drop()
        waited = time.monotonic() - start
        release.join()

    assert waited >= 0.4
    assert os.listdir(tmp_path) == []


def test_the_lock_file_is_unlocked_even_if_deleting_it_fails(tmp_path, monkeypatch):
    path = os.fspath(tmp_path / "kb")
    lock = store_module._LifecycleLock(path)
    assert lock.acquire(time.monotonic(), create=True)
    lock.delete_on_release = True

    def refuse(target, *args, **kwargs):
        raise PermissionError(f"injected refusal to delete {target}")

    monkeypatch.setattr(os, "unlink", refuse)
    with contextlib.suppress(PermissionError):
        lock.release()
    monkeypatch.undo()

    # The file is still there, and nothing holds it: another lock on it, which
    # gives up at once if it is held, takes it.
    again = store_module._LifecycleLock(path)
    assert again.acquire(time.monotonic(), create=False)
    again.release()


@pytest.mark.skipif(os.name == "nt", reason="Windows deletes no file that is open")
def test_a_lock_on_a_lock_file_a_drop_deleted_is_taken_again(tmp_path, monkeypatch):
    path = os.fspath(tmp_path / "kb")
    dropping = store_module._LifecycleLock(path)
    assert dropping.acquire(time.monotonic(), create=True)
    dropping.delete_on_release = True
    try_lock = store_module._try_lock
    tried = []

    def released_meanwhile(fd):
        # The drop deletes the lock file and unlocks it after this call opened
        # the file and before it locks it.
        if not tried:
            dropping.release()
        tried.append(fd)
        return try_lock(fd)

    monkeypatch.setattr(store_module, "_try_lock", released_meanwhile)
    waiter = store_module._LifecycleLock(path)
    assert waiter.acquire(time.monotonic() + 10, create=True)

    # It locked the file it had opened, found that the drop had deleted it, and
    # locked the file now at the path. (Inode numbers are not compared: a file
    # system may give the new file the number of the deleted one.)
    assert len(tried) == 2
    assert store_module._is_file_at(waiter._fd, path + ".lock")
    waiter.release()


def test_view_graph_keeps_what_the_knowledge_graph_view_shows():
    graph = nx.Graph()
    for i in range(300):
        graph.add_node(
            f"N{i:03}", description="d", source_id=["doc1"], pagerank=i / 1000
        )
    for i in range(299):
        graph.add_edge(f"N{i:03}", f"N{i + 1:03}", weight=float(i), source_id=["doc1"])
    graph.add_edge("N299", "N299", weight=1000.0, source_id=["doc1"])
    graph.graph["source_id"] = ["doc1"]

    view = view_graph(graph)

    assert set(view.nodes) == {f"N{i:03}" for i in range(300 - VIEW_NODES, 300)}
    weights = sorted(
        (attrs["weight"] for _, _, attrs in view.edges(data=True)), reverse=True
    )
    assert weights == [float(i) for i in range(298, 298 - VIEW_EDGES, -1)]
    assert all(source != target for source, target in view.edges)
    assert view.nodes["N299"] == graph.nodes["N299"]
    assert view.graph["source_id"] == ["doc1"]
    assert is_view_graph(view)
    assert not is_view_graph(graph)
    assert VIEW_MARKER not in graph.graph


def test_store_for_keeps_each_dataset_in_its_own_database(tmp_path):
    store = store_for(tmp_path, "tenant-1", "kb_2", max_paths=1, busy_timeout=5)
    assert store.path == os.path.join(os.path.abspath(tmp_path), "tenant-1", "kb_2")
    store.merge_subgraph(
        subgraph("doc1", ["A", "B", "C"], [("A", "B", 1.0), ("A", "C", 2.0)])
    )

    assert store.n_hop_paths(["A"], n_hop=1) == {
        "A": [{"path": ["A", "C"], "weights": [2.0]}]
    }
    assert store_for(tmp_path, "tenant-1", "kb_3").document_ids() == []


@pytest.mark.parametrize(
    "tenant_id, kb_id",
    [
        ("..", "kb"),
        ("tenant", "../kb"),
        ("a/b", "kb"),
        ("tenant", ""),
        ("tenant", "k b"),
        ("tenant", "x" * 129),
        (7, "kb"),
    ],
)
def test_store_for_refuses_ids_that_are_not_ragflow_ids(tmp_path, tenant_id, kb_id):
    with pytest.raises(ValueError):
        store_for(tmp_path, tenant_id, kb_id)


@pytest.mark.parametrize(
    "settings, error",
    [
        ({"max_paths": 0}, ValueError),
        ({"max_paths": True}, ValueError),
        ({"busy_timeout": -1}, ValueError),
        ({"busy_timeout": float("nan")}, ValueError),
        ({"busy_timeout": "1"}, TypeError),
    ],
)
def test_store_settings_are_checked(tmp_path, settings, error):
    with pytest.raises(error):
        HawDBGraphStore(tmp_path / "kb", **settings)


def test_a_store_needs_a_path_or_an_open_database():
    with pytest.raises(TypeError):
        HawDBGraphStore(42)
    with pytest.raises(ValueError):
        HawDBGraphStore("")


def test_the_patch_records_its_ragflow_revision_and_calls_this_package():
    patch = PATCH.read_text(encoding="utf-8")
    added = "\n".join(
        line[1:]
        for line in patch.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    )

    assert RAGFLOW_COMMIT in patch
    assert f'_VIEW_MARKER = ("{VIEW_MARKER}", "{VIEW_MARKER_VALUE}")' in added
    assert "from ragflow_hawdb.ragflow import store_for" in added
    assert "from ragflow_hawdb.ragflow import view_graph" in added
    called = set(re.findall(r"\bstore\.(\w+)", added))
    assert {
        "merge_subgraph",
        "apply_change",
        "generation",
        "holds_graph",
        "import_graph",
        "n_hop_paths",
        "remove_document",
        "clear",
        "drop",
    } <= called
    assert called <= set(dir(HawDBGraphStore))
    # The first use imports in the generation it read before the chunks, and
    # both graph wipes clear the store before and after deleting the chunks.
    assert "store.import_graph, nx.Graph() if graph is None else graph, " in added
    assert "generation=generation)" in added
    assert added.count("wipe_graph(kb.tenant_id, dataset_id, lambda: ") == 2
    assert added.count("store.clear()") == 2
