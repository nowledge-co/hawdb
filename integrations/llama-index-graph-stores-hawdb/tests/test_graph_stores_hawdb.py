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

"""HawDBPropertyGraphStore against real embedded databases, with no network."""

import asyncio
import gc
import threading
from collections.abc import Sequence
from typing import Any

import hawdb
import pytest
from llama_index.core import Document, MockEmbedding, PropertyGraphIndex
from llama_index.core.graph_stores.types import (
    KG_NODES_KEY,
    KG_RELATIONS_KEY,
    TRIPLET_SOURCE_KEY,
    ChunkNode,
    EntityNode,
    LabelledNode,
    Relation,
)
from llama_index.core.indices.property_graph import VectorContextRetriever
from llama_index.core.llms import MockLLM
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import (
    BaseNode,
    NodeRelationship,
    RelatedNodeInfo,
    TextNode,
    TransformComponent,
)
from llama_index.graph_stores.hawdb import (
    HawDBPropertyGraphStore,
    WrongThreadError,
    base,
)
from llama_index.graph_stores.hawdb.base import MAX_PROPERTIES_BYTES, MAX_TEXT_BYTES


@pytest.fixture(autouse=True)
def collect_garbage_on_this_thread():
    # A hawdb.Database refuses to be freed on another thread, and the cyclic
    # garbage collector runs on whichever thread allocates. A PropertyGraphIndex
    # holds its store, and so its database, in reference cycles; free them here
    # before a later test starts LlamaIndex's worker thread.
    yield
    gc.collect()


@pytest.fixture(params=["memory", "file"])
def database(request, tmp_path):
    db = hawdb.open() if request.param == "memory" else hawdb.open(tmp_path / "graph")
    yield db
    db.close()


@pytest.fixture
def store(database):
    return HawDBPropertyGraphStore(database)


def keys(triplets):
    return sorted(
        (source.id, relation.label, target.id) for source, relation, target in triplets
    )


def ids(nodes):
    return sorted(node.id for node in nodes)


def test_two_entities_and_a_relation_round_trip(store):
    store.upsert_nodes(
        [
            EntityNode(name="Alice", label="PERSON", properties={"age": 30}),
            EntityNode(name="Acme", label="ORG"),
        ]
    )
    store.upsert_relations(
        [
            Relation(
                label="WORKS_AT",
                source_id="Alice",
                target_id="Acme",
                properties={"since": 2020},
            )
        ]
    )

    [(source, relation, target)] = store.get_triplets(entity_names=["Alice"])
    assert (source.name, source.label, source.properties) == (
        "Alice",
        "PERSON",
        {"age": 30},
    )
    assert (relation.label, relation.source_id, relation.target_id) == (
        "WORKS_AT",
        "Alice",
        "Acme",
    )
    assert relation.properties == {"since": 2020}
    assert (target.name, target.label, target.properties) == ("Acme", "ORG", {})

    store.delete(entity_names=["Acme"])
    assert store.get_triplets(entity_names=["Alice"]) == []
    assert ids(store.get()) == ["Alice"]

    rows = store.structured_query(
        "MATCH (n:__Node__ {label: $label}) RETURN n.name AS name", {"label": "PERSON"}
    )
    assert rows == [{"name": "Alice"}]


def test_upsert_nodes_replaces_the_stored_node(store):
    store.upsert_nodes(
        [
            EntityNode(
                name="Alice", label="PERSON", properties={"age": 30, "city": "Paris"}
            )
        ]
    )
    store.upsert_nodes([EntityNode(name="Alice", label="CEO", properties={"age": 31})])
    [alice] = store.get(ids=["Alice"])
    assert (alice.label, alice.properties) == ("CEO", {"age": 31})
    # A change to the properties alone rewrites the node.
    store.upsert_nodes([EntityNode(name="Alice", label="CEO", properties={"age": 32})])
    [alice] = store.get(ids=["Alice"])
    assert alice.properties == {"age": 32}

    # The last node with an id wins, and an unchanged node stays as it is.
    store.upsert_nodes(
        [
            EntityNode(name="Bob", properties={"v": 1}),
            EntityNode(name="Bob", properties={"v": 2}),
        ]
    )
    store.upsert_nodes([EntityNode(name="Alice", label="CEO", properties={"age": 32})])
    nodes = {node.id: node for node in store.get()}
    assert set(nodes) == {"Alice", "Bob"}
    assert nodes["Bob"].properties == {"v": 2}
    assert (nodes["Alice"].label, nodes["Alice"].properties) == ("CEO", {"age": 32})


def test_chunks_keep_text_label_properties_and_embedding(store, database):
    def chunk(text, embedding=None):
        return ChunkNode(
            text=text,
            id_="c1",
            properties={"ref_doc_id": "doc-1", "page": 2},
            embedding=embedding,
        )

    def stored_embedding():
        return database.execute(
            "MATCH (n:__Node__ {id: 'c1'}) RETURN n.embedding AS embedding"
        ).fetchall()

    store.upsert_nodes([chunk("Alice works at Acme.", [0.25, 0.5])])
    [restored] = store.get(ids=["c1"])
    assert isinstance(restored, ChunkNode)
    assert (restored.text, restored.label, restored.properties) == (
        "Alice works at Acme.",
        "text_chunk",
        {"ref_doc_id": "doc-1", "page": 2},
    )
    assert stored_embedding() == [{"embedding": {"$vector": [0.25, 0.5]}}]

    # A new embedding alone rewrites the node.
    store.upsert_nodes([chunk("Alice works at Acme.", [0.75, 1.0])])
    assert stored_embedding() == [{"embedding": {"$vector": [0.75, 1.0]}}]

    store.upsert_nodes([chunk("Alice left Acme.")])
    [restored] = store.get(ids=["c1"])
    assert restored.text == "Alice left Acme."
    assert stored_embedding() == [{"embedding": None}]

    # A rewrite moves the indexed copy of ref_doc_id with the properties.
    store.upsert_nodes(
        [ChunkNode(text="Alice left Acme.", id_="c1", properties={"ref_doc_id": "d2"})]
    )
    assert ids(store.get(properties={"ref_doc_id": "d2"})) == ["c1"]
    assert store.get(properties={"ref_doc_id": "doc-1"}) == []

    # An empty label stays empty.
    store.upsert_nodes([ChunkNode(text="t", id_="c2", label="")])
    store.upsert_nodes([EntityNode(name="e", label="")])
    assert {node.id: node.label for node in store.get(ids=["c2", "e"])} == {
        "c2": "",
        "e": "",
    }


def test_llama_nodes_round_trip_and_delete_by_chunk_and_document(store):
    node = TextNode(
        id_="chunk-1", text="Alice works at Acme.", metadata={"file": "a.txt"}
    )
    node.relationships[NodeRelationship.SOURCE] = RelatedNodeInfo(node_id="doc-1")
    store.upsert_llama_nodes([node])
    store.upsert_nodes(
        [EntityNode(name="Alice", properties={TRIPLET_SOURCE_KEY: "chunk-1"})]
    )

    [restored] = store.get_llama_nodes(["chunk-1"])
    assert (restored.id_, restored.text, restored.metadata) == (
        "chunk-1",
        node.text,
        {"file": "a.txt"},
    )
    assert restored.ref_doc_id == "doc-1"
    assert ids(store.get(properties={TRIPLET_SOURCE_KEY: "chunk-1"})) == ["Alice"]

    store.delete_llama_nodes(ref_doc_ids=["doc-1"])
    assert ids(store.get()) == ["Alice"]
    store.delete_llama_nodes(node_ids=["chunk-1"])
    assert store.get() == []


def test_get_filters_by_ids_and_properties(store):
    store.upsert_nodes(
        [
            EntityNode(
                name="Alice",
                properties={
                    "team": "red",
                    "level": 3,
                    "active": True,
                    TRIPLET_SOURCE_KEY: "c1",
                },
            ),
            EntityNode(
                name="Bob",
                properties={"team": "red", "level": 2, TRIPLET_SOURCE_KEY: "c2"},
            ),
            EntityNode(name="Carol", properties={"team": "blue", "level": 2.5}),
        ]
    )
    assert ids(store.get(properties={"team": "red"})) == ["Alice", "Bob"]
    assert ids(store.get(properties={"team": "red", "level": 3})) == ["Alice"]
    assert ids(store.get(properties={"level": 2.5})) == ["Carol"]
    assert ids(store.get(properties={"active": True})) == ["Alice"]
    assert ids(store.get(properties={TRIPLET_SOURCE_KEY: "c2"})) == ["Bob"]
    assert ids(store.get(properties={"team": "red"}, ids=["Bob", "Carol"])) == ["Bob"]
    assert ids(store.get(ids=["Carol", "nobody"])) == ["Carol"]
    assert ids(store.get()) == ["Alice", "Bob", "Carol"]
    assert store.get(ids=[]) == []
    # HawDB compares JSON values by type, unlike SimplePropertyGraphStore.
    assert store.get(properties={"level": 3.0}) == []
    assert store.get(properties={"active": 1}) == []

    for key in ("file name", "a.b", "", "x" * 255):
        with pytest.raises(ValueError, match="key"):
            store.get(properties={key: "x"})
    for value in (["x"], {"a": 1}, None):
        with pytest.raises(ValueError, match="string, number or boolean"):
            store.get(properties={"team": value})
    for value in (1 << 63, -(1 << 63) - 1):
        with pytest.raises(ValueError, match="signed 64-bit"):
            store.get(properties={"level": value})


def test_labels_ids_names_and_values_bind_as_parameters(store):
    hostile = "x'}) DETACH DELETE n //"
    store.upsert_nodes(
        [
            EntityNode(
                name=hostile,
                label="L') MATCH (m) DETACH DELETE m //",
                properties={"k": hostile},
            ),
            EntityNode(name="other'", label="`OTHER`"),
        ]
    )
    store.upsert_relations(
        [Relation(label="R]->() DELETE r //", source_id=hostile, target_id="other'")]
    )

    [(source, relation, target)] = store.get_triplets(ids=[hostile])
    assert (source.id, source.label, source.properties) == (
        hostile,
        "L') MATCH (m) DETACH DELETE m //",
        {"k": hostile},
    )
    assert relation.label == "R]->() DELETE r //"
    assert (target.id, target.label) == ("other'", "`OTHER`")
    assert ids(store.get(properties={"k": hostile})) == [hostile]


def test_entity_names_match_names_rather_than_ids(store):
    # EntityNode replaces '"' in a name with a space in its id.
    quoted = EntityNode(name='say "hi"')
    assert quoted.id != quoted.name
    store.upsert_nodes([quoted, EntityNode(name="Bob")])
    store.upsert_relations([Relation(label="R", source_id=quoted.id, target_id="Bob")])
    assert keys(store.get_triplets(entity_names=['say "hi"'])) == [
        (quoted.id, "R", "Bob")
    ]
    store.delete(entity_names=['say "hi"'])
    assert ids(store.get()) == ["Bob"]


def test_upsert_relations_creates_endpoints_and_keeps_existing_relations(store):
    store.upsert_nodes([EntityNode(name="Alice", label="PERSON")])
    store.upsert_relations(
        [
            Relation(
                label="KNOWS",
                source_id="Alice",
                target_id="Bob",
                properties={"since": 2020},
            )
        ]
    )
    store.upsert_relations(
        [
            Relation(
                label="KNOWS",
                source_id="Alice",
                target_id="Bob",
                properties={"since": 2024},
            ),
            Relation(label="LIKES", source_id="Alice", target_id="Bob"),
            Relation(
                label="LIKES",
                source_id="Alice",
                target_id="Bob",
                properties={"dropped": True},
            ),
        ]
    )
    nodes = {node.id: node for node in store.get()}
    assert set(nodes) == {"Alice", "Bob"}
    assert nodes["Alice"].label == "PERSON"
    assert isinstance(nodes["Bob"], EntityNode)
    assert (nodes["Bob"].name, nodes["Bob"].label, nodes["Bob"].properties) == (
        "Bob",
        "entity",
        {},
    )
    relations = sorted(
        (relation.label, relation.properties)
        for _, relation, _ in store.get_triplets(ids=["Alice"])
    )
    assert relations == [("KNOWS", {"since": 2020}), ("LIKES", {})]

    # A later node upsert replaces the placeholder endpoint, and its chunk then
    # finds it.
    store.upsert_nodes(
        [
            EntityNode(
                name="Bob",
                label="PERSON",
                properties={"age": 40, TRIPLET_SOURCE_KEY: "c9"},
            )
        ]
    )
    [bob] = store.get(ids=["Bob"])
    assert (bob.label, bob.properties) == (
        "PERSON",
        {"age": 40, TRIPLET_SOURCE_KEY: "c9"},
    )
    assert ids(store.get(properties={TRIPLET_SOURCE_KEY: "c9"})) == ["Bob"]
    assert keys(store.get_triplets(entity_names=["Bob"])) == [
        ("Alice", "KNOWS", "Bob"),
        ("Alice", "LIKES", "Bob"),
    ]


def test_get_triplets_filters_relations_between_entities(store):
    store.upsert_nodes(
        [
            EntityNode(name="Alice", label="PERSON", properties={"team": "red"}),
            EntityNode(name="Acme", label="ORG", properties={"level": 1}),
            EntityNode(name="Bob", label="PERSON", properties={"team": "blue"}),
            ChunkNode(text="Alice works at Acme.", id_="c1"),
        ]
    )
    store.upsert_relations(
        [
            Relation(label="WORKS_AT", source_id="Alice", target_id="Acme"),
            Relation(label="KNOWS", source_id="Bob", target_id="Alice"),
            Relation(label="MENTIONS", source_id="c1", target_id="Alice"),
        ]
    )
    both = [("Alice", "WORKS_AT", "Acme"), ("Bob", "KNOWS", "Alice")]
    assert keys(store.get_triplets(entity_names=["Alice"])) == both
    assert keys(store.get_triplets(entity_names=["Acme"])) == [
        ("Alice", "WORKS_AT", "Acme")
    ]
    assert keys(store.get_triplets(relation_names=["KNOWS"])) == [
        ("Bob", "KNOWS", "Alice")
    ]
    assert keys(store.get_triplets(ids=["Bob"])) == [("Bob", "KNOWS", "Alice")]
    assert keys(store.get_triplets(properties={"team": "red"})) == both
    assert keys(store.get_triplets(properties={"team": "blue"})) == [
        ("Bob", "KNOWS", "Alice")
    ]
    # Every member must match on the same end.
    assert store.get_triplets(properties={"team": "red", "level": 1}) == []
    assert keys(
        store.get_triplets(entity_names=["Alice"], relation_names=["WORKS_AT"])
    ) == [("Alice", "WORKS_AT", "Acme")]
    assert keys(store.get_triplets()) == both
    assert store.get_triplets(entity_names=[]) == []
    assert store.get_triplets(relation_names=["MENTIONS"]) == []


def test_get_rel_map_follows_both_directions_up_to_depth_and_limit(store):
    store.upsert_relations(
        [
            Relation(label="R1", source_id="A", target_id="B"),
            Relation(label="R5", source_id="B", target_id="A"),
            Relation(label="R2", source_id="C", target_id="B"),
            Relation(label="R3", source_id="B", target_id="D"),
            Relation(label="R4", source_id="D", target_id="E"),
            Relation(label="SOURCE", source_id="A", target_id="X"),
            Relation(label="R6", source_id="X", target_id="Y"),
        ]
    )
    [a] = store.get(ids=["A"])
    first = [("A", "R1", "B"), ("B", "R5", "A")]
    second = [*first, ("B", "R3", "D"), ("C", "R2", "B")]
    assert keys(store.get_rel_map([a], depth=1, ignore_rels=["SOURCE"])) == first
    assert keys(store.get_rel_map([a], depth=2, ignore_rels=["SOURCE"])) == sorted(
        second
    )
    assert keys(store.get_rel_map([a], depth=3, ignore_rels=["SOURCE"])) == sorted(
        [*second, ("D", "R4", "E")]
    )
    assert keys(store.get_rel_map([a], depth=2)) == sorted(
        [*second, ("A", "SOURCE", "X"), ("X", "R6", "Y")]
    )

    limited = store.get_rel_map([a], depth=3, limit=3, ignore_rels=["SOURCE"])
    assert len(limited) == 3
    assert set(keys(limited)) <= {*second, ("D", "R4", "E")}
    assert store.get_rel_map([], depth=2) == []
    assert store.get_rel_map([a], depth=0) == []


def test_get_rel_map_limit_counts_each_relation_once(store):
    # The second hop from A skips the relations back to A, which the first hop
    # returned, so they do not use up the limit.
    store.upsert_relations(
        [
            Relation(label="R1", source_id="A", target_id="B"),
            *(
                Relation(label=f"BACK{i}", source_id="B", target_id="A")
                for i in range(3)
            ),
            Relation(label="R2", source_id="B", target_id="C"),
        ]
    )
    [a] = store.get(ids=["A"])
    assert keys(store.get_rel_map([a], depth=2, limit=5)) == [
        ("A", "R1", "B"),
        ("B", "BACK0", "A"),
        ("B", "BACK1", "A"),
        ("B", "BACK2", "A"),
        ("B", "R2", "C"),
    ]

    # A relation between two nodes of a hop comes back from both ends.
    store.upsert_relations(
        [
            Relation(label="X", source_id="P", target_id="Q"),
            Relation(label="W", source_id="Z", target_id="Q"),
        ]
    )
    p_and_q = store.get(ids=["P", "Q"])
    assert keys(store.get_rel_map(p_and_q, depth=1, limit=2)) == [
        ("P", "X", "Q"),
        ("Z", "W", "Q"),
    ]


def test_delete_runs_each_filter(store):
    store.upsert_nodes(
        [
            EntityNode(name=name, properties={"team": team})
            for name, team in [("A", "red"), ("B", "red"), ("C", "blue"), ("D", "blue")]
        ]
    )
    store.upsert_relations(
        [
            Relation(label="KNOWS", source_id="C", target_id="D"),
            Relation(label="LIKES", source_id="C", target_id="D"),
            Relation(label="KNOWS", source_id="A", target_id="C"),
        ]
    )
    store.delete(relation_names=["KNOWS"])
    assert keys(store.get_triplets()) == [("C", "LIKES", "D")]

    store.delete(properties={"team": "red"})
    assert ids(store.get()) == ["C", "D"]

    store.delete(ids=["C"])
    assert ids(store.get()) == ["D"]
    assert store.get_triplets() == []

    store.delete()
    store.delete(entity_names=[], relation_names=[], properties={}, ids=[])
    assert ids(store.get()) == ["D"]


def test_deletes_split_into_transactions_and_refuse_nodes_over_the_limit(
    store, monkeypatch
):
    # HawDB refuses a transaction of more than 100,000 mutation operations or
    # version entries, and deleting a node or relation takes one operation and
    # up to three entries.
    monkeypatch.setattr(base, "_DELETE_ROWS", 4)
    store.upsert_relations(
        [
            *(
                Relation(label="NEXT", source_id=f"e{i}", target_id=f"e{i + 1}")
                for i in range(5)
            ),
            *(
                Relation(label="TAG", source_id="e0", target_id=f"t{i}")
                for i in range(3)
            ),
            *(
                Relation(label="TAG", source_id="e5", target_id=f"t{i}")
                for i in range(2)
            ),
        ]
    )
    recording = _RecordingDatabase(store.client)
    monkeypatch.setattr(store, "_db", recording)

    # e1, e2 and e3 each take themselves and two relations.
    store.delete(ids=["e1", "e2", "e3", "missing"])
    assert recording.reads == [base._NODE_DEGREES.format(where="n.id IN $ids")]
    assert recording.transactions == [[base._DELETE_NODES]] * 3
    assert ids(store.get()) == ["e0", "e4", "e5", "t0", "t1", "t2"]

    # e0 is the source of three TAG relations and e5 of two, so each source
    # takes a transaction, which deletes that source's relations only.
    recording.clear()
    recording.failing_transaction = 2
    with pytest.raises(RuntimeError, match="injected failure"):
        store.delete(relation_names=["TAG"])
    assert recording.reads == [base._RELATION_SOURCES]
    assert recording.transactions == [[base._DELETE_RELATIONS], []]
    assert keys(store.get_triplets(relation_names=["TAG"])) in (
        [("e0", "TAG", f"t{i}") for i in range(3)],
        [("e5", "TAG", f"t{i}") for i in range(2)],
    )
    recording.failing_transaction = None
    store.delete(relation_names=["TAG"])
    assert keys(store.get_triplets()) == [("e4", "NEXT", "e5")]

    # A delete within the limit is one transaction, whatever its filters.
    recording.clear()
    store.delete(ids=["e4"], relation_names=["NEXT"])
    assert recording.transactions == [[base._DELETE_NODES, base._DELETE_RELATIONS]]
    assert ids(store.get()) == ["e0", "e5", "t0", "t1", "t2"]

    # A node that would take more relations with it than one transaction
    # deletes fails before anything is deleted, whatever their type.
    store.upsert_relations(
        [Relation(label="HUB", source_id="hub", target_id=f"h{i}") for i in range(5)]
    )
    store.structured_query(
        "UNWIND $rows AS row CREATE (:Other {i: row.i})",
        {"rows": [{"i": i} for i in range(4)]},
    )
    store.structured_query(
        "MATCH (n:__Node__ {id: 'e5'}) MATCH (o:Other) CREATE (n)-[:MENTIONS]->(o)"
    )
    recording.clear()
    with pytest.raises(ValueError, match="'e5' would take 4 relations with it"):
        store.delete(ids=["e5"])
    # h0 takes its own relation to the hub with it, which leaves four.
    with pytest.raises(ValueError, match="'hub' would take 4 relations with it"):
        store.delete(ids=["hub", "h0"])
    with pytest.raises(ValueError, match="'hub' is the source of 5 relations"):
        store.delete(relation_names=["HUB"])
    assert recording.transactions == []
    assert {"e5", "hub", "h0"} <= set(ids(store.get()))
    assert len(store.get_triplets(relation_names=["HUB"])) == 5

    # When the other ends of its relations are deleted too, they take those
    # relations with them first, and the node goes last.
    recording.clear()
    hub_and_ends = ["hub", *(f"h{i}" for i in range(5))]
    store.delete(ids=hub_and_ends)
    assert recording.reads == [
        base._NODE_DEGREES.format(where="n.id IN $ids"),
        base._NEIGHBOURS,
    ]
    groups = [parameters["ids"] for parameters in recording.written]
    assert [len(group) for group in groups] == [2, 2, 2]
    assert groups[-1][-1] == "hub"
    assert not set(hub_and_ends) & set(ids(store.get()))


class _Recording:
    """Forwards a transaction or snapshot and records the statements it runs."""

    def __init__(self, inner, statements, parameters, fail=False):
        self._inner = inner
        self._statements = statements
        self._parameters = parameters
        self._fail = fail

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, *exception):
        return self._inner.__exit__(*exception)

    def execute(self, statement, *params):
        if self._fail:
            raise RuntimeError("injected failure")
        self._statements.append(statement)
        self._parameters.append(params[0] if params else None)
        return self._inner.execute(statement, *params)

    def executemany(self, statement, rows):
        if self._fail:
            raise RuntimeError("injected failure")
        self._statements.extend([statement] * len(rows))
        self._parameters.extend(rows)
        return self._inner.executemany(statement, rows)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _RecordingDatabase:
    """Records the statements of each write transaction, and the reads.

    ``written`` holds the parameters of each write statement in order. The
    transaction numbered ``failing_transaction``, counting from one, fails on
    its first statement.
    """

    def __init__(self, database):
        self._database = database
        self.transactions = []
        self.written = []
        self.reads = []
        self.autocommit = []
        self.failing_transaction = None

    def transaction(self):
        self.transactions.append([])
        fail = len(self.transactions) == self.failing_transaction
        return _Recording(
            self._database.transaction(), self.transactions[-1], self.written, fail
        )

    def read_transaction(self):
        return _Recording(self._database.read_transaction(), self.reads, [])

    def execute(self, statement, *params):
        self.autocommit.append(statement)
        return self._database.execute(statement, *params)

    def clear(self):
        self.transactions.clear()
        self.written.clear()
        self.reads.clear()
        self.autocommit.clear()

    def __getattr__(self, name):
        return getattr(self._database, name)


def test_reads_split_into_pages_and_writes_into_transactions(store, monkeypatch):
    monkeypatch.setattr(base, "_PAGE_SIZE", 2)
    # Each chunk below is larger than a page and is read on its own.
    monkeypatch.setattr(base, "_PAGE_CHARACTERS", 40)
    # Two of these rows per transaction.
    monkeypatch.setattr(base, "_TRANSACTION_BYTES", 1300)
    recording = _RecordingDatabase(store.client)
    monkeypatch.setattr(store, "_db", recording)
    chunks = [
        ChunkNode(text="v1" * 15, id_=f"c{i}", properties={"ref_doc_id": "doc"})
        for i in range(5)
    ]
    store.upsert_nodes([*chunks, *(EntityNode(name=f"e{i}") for i in range(5))])
    assert recording.transactions == [[base._STORED_DIGESTS, base._CREATE_NODES]] * 5
    recording.clear()
    store.upsert_relations(
        [
            Relation(label="R", source_id=f"e{i}", target_id=f"e{i + 1}")
            for i in range(4)
        ]
        + [Relation(label="R", source_id="e4", target_id="new")]
    )
    assert (
        recording.transactions == [[base._CREATE_ENDPOINTS, base._CREATE_RELATIONS]] * 3
    )

    chunk_ids = [f"c{i}" for i in range(5)]
    recording.clear()
    assert ids(store.get(properties={"ref_doc_id": "doc"})) == chunk_ids
    # One snapshot selects the chunks through the indexed copy, then reads them
    # one per page.
    [select, *pages] = recording.reads
    assert "n.ref_doc_id = $value0" in select
    assert pages == [base._NODES_BY_ID] * 5
    assert (recording.transactions, recording.autocommit) == ([], [])

    assert ids(store.get()) == sorted([*chunk_ids, *(f"e{i}" for i in range(5)), "new"])
    assert ids(store.get(ids=["c4", "missing", "c0", "c4", "e1"])) == ["c0", "c4", "e1"]
    assert ids(store.get(properties={"ref_doc_id": "doc"}, ids=["c3", "e3", "c1"])) == [
        "c1",
        "c3",
    ]
    recording.clear()
    assert len(store.get_triplets()) == 5
    assert (len(recording.reads), recording.autocommit) == (1, [])

    store.upsert_nodes(
        [
            ChunkNode(text="v2" * 15, id_=f"c{i}", properties={"ref_doc_id": "doc"})
            for i in range(5)
        ]
    )
    assert {node.text for node in store.get(ids=chunk_ids)} == {"v2" * 15}


def test_writes_read_first_and_take_one_statement_per_transaction(store, monkeypatch):
    # Each write statement the store runs takes time that grows with the stored
    # graph, so a transaction reads first and writes its rows in one statement
    # where HawDB allows it.
    recording = _RecordingDatabase(store.client)
    monkeypatch.setattr(store, "_db", recording)
    nodes = [EntityNode(name=f"e{i}") for i in range(1200)]
    store.upsert_nodes(nodes)
    assert recording.transactions == [[base._STORED_DIGESTS] * 3 + [base._CREATE_NODES]]

    # Unchanged nodes are not written again.
    recording.clear()
    store.upsert_nodes(nodes)
    assert recording.transactions == [[base._STORED_DIGESTS] * 3]

    recording.clear()
    store.upsert_nodes(
        [EntityNode(name="e0", label="PERSON"), EntityNode(name="e1", label="PERSON")]
    )
    assert recording.transactions == [[base._STORED_DIGESTS] + [base._UPDATE_NODE] * 2]

    recording.clear()
    store.upsert_relations(
        [
            Relation(label="R", source_id=f"e{i}", target_id=f"e{i + 1}")
            for i in range(1199)
        ]
    )
    assert recording.transactions == [[base._CREATE_ENDPOINTS, base._CREATE_RELATIONS]]
    assert len(store.get_triplets()) == 1199
    assert {node.label for node in store.get(ids=["e0", "e1", "e2"])} == {
        "PERSON",
        "entity",
    }


def test_write_transactions_hold_at_most_the_row_limit(store, monkeypatch):
    # HawDB refuses a transaction of more than 100,000 mutation operations, and
    # rewriting a node takes nine.
    monkeypatch.setattr(base, "_TRANSACTION_ROWS", 3)
    recording = _RecordingDatabase(store.client)
    monkeypatch.setattr(store, "_db", recording)
    store.upsert_nodes([EntityNode(name=f"e{i}") for i in range(7)])
    assert recording.transactions == [[base._STORED_DIGESTS, base._CREATE_NODES]] * 3

    recording.clear()
    store.upsert_nodes([EntityNode(name=f"e{i}", label="PERSON") for i in range(7)])
    assert recording.transactions == [
        [base._STORED_DIGESTS] + [base._UPDATE_NODE] * 3,
        [base._STORED_DIGESTS] + [base._UPDATE_NODE] * 3,
        [base._STORED_DIGESTS, base._UPDATE_NODE],
    ]
    assert {node.label for node in store.get()} == {"PERSON"}


def test_embeddings_split_writes_by_their_wal_bytes(store):
    # 1,500 embeddings of 3,072 dimensions take about 18 MiB of WAL as 32-bit
    # vectors, more than one transaction's 16 MiB WAL record holds.
    embedding = [0.5] * 3072
    store.upsert_nodes(
        [ChunkNode(text=f"c{i}", id_=f"c{i}", embedding=embedding) for i in range(1500)]
    )
    assert len(store.get()) == 1500
    rows = store.structured_query(
        "MATCH (n:__Node__ {id: 'c1499'}) RETURN n.embedding AS embedding"
    )
    assert rows == [{"embedding": {"$vector": embedding}}]


def test_large_results_come_back_in_pages(store):
    # 3,000 chunks of 25,000 characters are more than the 64 MiB one statement
    # in a read snapshot returns, as the chunks of a large document can be.
    text = "word " * 5000
    store.upsert_nodes(
        [
            ChunkNode(text=text, id_=f"c{i}", properties={"ref_doc_id": "doc"})
            for i in range(3000)
        ]
    )
    every = [f"c{i}" for i in range(3000)]
    assert ids(store.get(ids=every)) == sorted(every)
    assert ids(store.get(properties={"ref_doc_id": "doc"})) == sorted(every)
    store.delete_llama_nodes(ref_doc_ids=["doc"])
    assert store.get() == []


def test_chunk_text_holds_at_most_what_get_can_size(store):
    # get sizes each node with size(text) + size(properties), and HawDB refuses
    # an expression whose string functions read more than 10,000,000 bytes.
    blob = "x" * (MAX_PROPERTIES_BYTES - len('{"blob":""}'))
    store.upsert_nodes(
        [
            ChunkNode(
                text="a" * MAX_TEXT_BYTES, id_="ascii", properties={"blob": blob}
            ),
            ChunkNode(text="漢" * (MAX_TEXT_BYTES // 3), id_="cjk"),
        ]
    )
    assert {node.id: len(node.text) for node in store.get()} == {
        "ascii": MAX_TEXT_BYTES,
        "cjk": MAX_TEXT_BYTES // 3,
    }
    # The limit counts UTF-8 bytes, and a refused chunk fails the whole call.
    for text in ("a" * (MAX_TEXT_BYTES + 1), "漢" * (MAX_TEXT_BYTES // 3 + 1)):
        with pytest.raises(ValueError, match="text of chunk 'big'"):
            store.upsert_nodes([EntityNode(name="ok"), ChunkNode(text=text, id_="big")])
    assert ids(store.get()) == ["ascii", "cjk"]


def test_rejected_nodes_write_nothing(store, monkeypatch):
    # Every row below gets its own transaction, so a row refused only when its
    # transaction runs would leave the earlier rows written.
    monkeypatch.setattr(base, "_TRANSACTION_BYTES", 1)
    for embedding in ([float("nan")], [float("inf")], [1e39]):
        with pytest.raises(ValueError, match="finite 32-bit floats"):
            store.upsert_nodes(
                [
                    EntityNode(name="ok"),
                    ChunkNode(text="t", id_="v", embedding=embedding),
                ]
            )
    with pytest.raises(UnicodeEncodeError):
        store.upsert_nodes(
            [ChunkNode(text="t", id_="ok"), ChunkNode(text="t", id_="\ud800")]
        )
    with pytest.raises(UnicodeEncodeError):
        store.upsert_relations(
            [
                Relation(label="R", source_id="a", target_id="b"),
                Relation(label="R\udc00", source_id="c", target_id="d"),
            ]
        )
    with pytest.raises(ValueError, match="NaN"):
        store.upsert_nodes(
            [
                EntityNode(name="ok"),
                EntityNode(name="bad", properties={"score": float("nan")}),
            ]
        )
    with pytest.raises(ValueError, match="exceed"):
        store.upsert_nodes(
            [
                ChunkNode(
                    text="t", id_="big", properties={"blob": "x" * MAX_PROPERTIES_BYTES}
                )
            ]
        )
    with pytest.raises(ValueError, match="JSON"):
        store.upsert_relations(
            [
                Relation(
                    label="R",
                    source_id="a",
                    target_id="b",
                    properties={"when": object()},
                )
            ]
        )

    class Other(LabelledNode):
        def __str__(self) -> str:
            return "other"

        @property
        def id(self) -> str:
            return "other"

    with pytest.raises(TypeError, match="EntityNode and ChunkNode"):
        store.upsert_nodes([EntityNode(name="ok"), Other()])
    assert store.get() == []


def test_schema_lists_stored_labels_and_relations(store):
    store.upsert_nodes(
        [
            EntityNode(name="Alice", label="PERSON"),
            EntityNode(name="Acme", label="ORG"),
            EntityNode(name="Blank", label=""),
            ChunkNode(text="Alice works at Acme.", id_="c1"),
        ]
    )
    store.upsert_relations(
        [Relation(label="WORKS_AT", source_id="Alice", target_id="Acme")]
    )
    # The counts leave out graphs of other labels in the same database.
    store.client.execute("CREATE (:Other {x: 1})-[:OTHER_REL]->(:Other {x: 2})")
    schema = store.get_schema(refresh=True)
    # An empty label gives a text-to-Cypher prompt nothing to match on.
    assert schema["entity_labels"] == ["ORG", "PERSON"]
    assert schema["chunk_labels"] == ["text_chunk"]
    assert schema["relations"] == [
        {"source": "PERSON", "label": "WORKS_AT", "target": "ORG"}
    ]
    assert (schema["node_count"], schema["relation_count"]) == (4, 1)
    text = store.get_schema_str()
    assert "Entity labels: ORG, PERSON" in text
    assert "Relations: (PERSON)-[WORKS_AT]->(ORG)" in text
    # TextToCypherRetriever reads the schema through aget_schema_str when async.
    assert asyncio.run(store.aget_schema_str()) == text

    store.upsert_nodes([EntityNode(name="Bob", label="CEO")])
    assert "CEO" not in store.get_schema()["entity_labels"]
    assert "CEO" in store.get_schema(refresh=True)["entity_labels"]


def test_reopened_and_read_only_databases_keep_the_graph(tmp_path):
    path = tmp_path / "graph"
    with hawdb.open(path) as db:
        store = HawDBPropertyGraphStore(db)
        store.upsert_nodes(
            [
                EntityNode(name="Alice", label="PERSON"),
                EntityNode(name="Acme", label="ORG"),
            ]
        )
        store.upsert_relations(
            [Relation(label="WORKS_AT", source_id="Alice", target_id="Acme")]
        )
    with hawdb.open(path) as db:
        HawDBPropertyGraphStore(db)
        constraints = db.execute_sql(
            "SELECT property_name FROM system.constraints"
        ).fetchall()
        indexes = db.execute_sql("SELECT property_names FROM system.indexes").fetchall()
        assert constraints == [{"property_name": "id"}]
        assert sorted(row["property_names"] for row in indexes) == [
            ["id"],
            ["ref_doc_id"],
            ["triplet_source_id"],
        ]
    with hawdb.open(path, read_only=True) as db:
        store = HawDBPropertyGraphStore(db)
        assert keys(store.get_triplets(entity_names=["Alice"])) == [
            ("Alice", "WORKS_AT", "Acme")
        ]


def test_other_threads_get_wrong_thread_error(store):
    entity = EntityNode(name="a")
    calls = {
        "get": lambda: store.get(),
        "get_triplets": lambda: store.get_triplets(),
        "get_rel_map": lambda: store.get_rel_map([entity]),
        "upsert_nodes": lambda: store.upsert_nodes([entity]),
        "upsert_relations": lambda: store.upsert_relations(
            [Relation(label="R", source_id="a", target_id="b")]
        ),
        "delete": lambda: store.delete(ids=["a"]),
        "structured_query": lambda: store.structured_query("RETURN 1 AS one"),
        "get_schema": lambda: store.get_schema(refresh=True),
    }
    errors = {}

    def use_store():
        for name, call in calls.items():
            try:
                call()
            except BaseException as error:  # a Rust panic would not be an Exception
                errors[name] = error

    worker = threading.Thread(target=use_store)
    worker.start()
    worker.join()
    assert {name: type(error) for name, error in errors.items()} == dict.fromkeys(
        calls, WrongThreadError
    )
    assert "use_async=False" in str(errors["get"])
    assert store.get() == []


def test_rejects_other_clients():
    with pytest.raises(TypeError, match=r"hawdb\.Database"):
        HawDBPropertyGraphStore("graph.db")


class FixedExtractor(TransformComponent):
    """Stands in for an LLM extractor: every chunk yields the same triplet."""

    def __call__(self, nodes: Sequence[BaseNode], **kwargs: Any) -> Sequence[BaseNode]:
        for node in nodes:
            node.metadata[KG_NODES_KEY] = [
                EntityNode(name="Alice", label="PERSON"),
                EntityNode(name="Acme", label="ORG"),
            ]
            node.metadata[KG_RELATIONS_KEY] = [
                Relation(label="WORKS_AT", source_id="Alice", target_id="Acme")
            ]
        return nodes


def test_property_graph_index_inserts_retrieves_and_deletes(store):
    document = Document(id_="doc-1", text="Alice works at Acme.")
    embed_model = MockEmbedding(embed_dim=8)
    index = PropertyGraphIndex.from_documents(
        [document],
        property_graph_store=store,
        # A whitespace tokenizer keeps the default tiktoken download out of the test.
        transformations=[
            SentenceSplitter(chunk_size=64, chunk_overlap=0, tokenizer=str.split)
        ],
        kg_extractors=[FixedExtractor()],
        embed_model=embed_model,
        llm=MockLLM(),
        use_async=False,
    )
    [chunk] = [node for node in store.get() if isinstance(node, ChunkNode)]
    assert (chunk.text, chunk.properties["ref_doc_id"]) == (
        "Alice works at Acme.",
        "doc-1",
    )
    entities = {node.id: node for node in store.get() if isinstance(node, EntityNode)}
    assert {
        key: node.properties[TRIPLET_SOURCE_KEY] for key, node in entities.items()
    } == {
        "Alice": chunk.id,
        "Acme": chunk.id,
    }
    assert "WORKS_AT" in store.get_schema_str()

    def retriever(use_async):
        sub_retriever = VectorContextRetriever(
            store,
            vector_store=index.vector_store,
            embed_model=embed_model,
            include_text=True,
        )
        return index.as_retriever(sub_retrievers=[sub_retriever], use_async=use_async)

    [result] = retriever(use_async=False).retrieve("Where does Alice work?")
    assert "Alice -> WORKS_AT -> Acme" in result.node.get_content()
    assert "Alice works at Acme." in result.node.get_content()

    # In a running event loop LlamaIndex moves async retrieval to a worker thread.
    async def inside_event_loop(use_async):
        return retriever(use_async).retrieve("Where does Alice work?")

    with pytest.raises(WrongThreadError, match="use_async=False"):
        asyncio.run(inside_event_loop(True))
    assert len(asyncio.run(inside_event_loop(False))) == 1

    index.delete_ref_doc("doc-1")
    assert ids(store.get()) == ["Acme", "Alice"]
    store.delete_llama_nodes(node_ids=[chunk.id])
    assert store.get() == []
