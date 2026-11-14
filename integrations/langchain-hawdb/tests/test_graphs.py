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

"""HawDBGraph against real embedded databases, with no network."""

import asyncio
import gc
import hashlib
import threading
import uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import hawdb
import pytest
from langchain_community.chains.graph_qa.cypher import (
    GraphCypherQAChain,
    construct_schema,
)
from langchain_community.graphs.graph_document import (
    GraphDocument,
    Node,
    Relationship,
)
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.documents import Document
from langchain_core.language_models.fake import FakeListLLM

from langchain_hawdb import DOCUMENT_LABEL, MENTIONS_TYPE, HawDBGraph, WrongThreadError
from langchain_hawdb.graphs import MAX_NODE_BYTES, MAX_RELATIONSHIP_BYTES

EMPTY_SCHEMA = (
    "Node properties are the following:\n\n"
    "Relationship properties are the following:\n\n"
    "The relationships are the following:\n"
)

ALICE = Node(id="Alice", type="Person", properties={"age": 30})
ACME = Node(id="Acme", type="Company", properties={"city": "Paris"})


@pytest.fixture(autouse=True)
def collect_garbage_on_this_thread():
    # A hawdb.Database refuses to be freed on another thread, and the cyclic
    # garbage collector runs on whichever thread allocates. Free any database
    # a test left in reference cycles before a later test starts a worker
    # thread.
    yield
    gc.collect()


@pytest.fixture(params=["memory", "file"])
def database(request, tmp_path):
    db = hawdb.open() if request.param == "memory" else hawdb.open(tmp_path / "graph")
    yield db
    db.close()


@pytest.fixture
def graph(database):
    return HawDBGraph(database)


# For checks that do not depend on how the database stores its data.
memory_only = pytest.mark.parametrize("database", ["memory"], indirect=True)


def graph_document(
    nodes=(), relationships=(), page_content="Alice works at Acme.", **metadata
):
    return GraphDocument(
        nodes=list(nodes),
        relationships=list(relationships),
        source=Document(page_content=page_content, metadata=metadata),
    )


def works_at(properties=None):
    return Relationship(
        source=ALICE, target=ACME, type="WORKS_AT", properties=properties or {}
    )


def count(graph, pattern):
    return graph.query(f"MATCH {pattern} RETURN count(*) AS count")[0]["count"]


def stored_nodes(graph):
    """Every stored node as (labels, id, other properties), in a fixed order."""
    nodes = []
    for row in graph.query("MATCH (n) RETURN n"):
        node = dict(row["n"])
        del node["_id"]
        nodes.append((node.pop("labels"), node.pop("id"), node))
    return sorted(nodes, key=repr)


def stored_relationships(graph):
    """Every stored relationship as (source id, type, target id, properties)."""
    relationships = []
    for row in graph.query(
        "MATCH (s)-[r]->(t) RETURN s.id AS source, r, t.id AS target"
    ):
        relationship = dict(row["r"])
        for key in ("_id", "source_id", "target_id"):
            del relationship[key]
        relationships.append(
            (row["source"], relationship.pop("type"), row["target"], relationship)
        )
    return sorted(relationships, key=repr)


def test_parameterized_cypher_round_trip_and_schema_refresh(graph, database):
    graph.query(
        "CREATE (:Person {id: $id, name: $name})", {"id": "p1", "name": "Alice"}
    )
    graph.query(
        "CREATE (:Company {id: $id, name: $name})", {"id": "c1", "name": "Acme"}
    )
    graph.query(
        "MATCH (p:Person {id: $person}), (c:Company {id: $company}) "
        "CREATE (p)-[:WORKS_AT {since: $since}]->(c)",
        {"person": "p1", "company": "c1", "since": 2020},
    )

    statement = (
        "MATCH (p:Person)-[r:WORKS_AT]->(c:Company) WHERE p.name = $name "
        "RETURN p.name AS person, r.since AS since, c.name AS company"
    )
    rows = graph.query(statement, {"name": "Alice"})
    assert rows == [{"person": "Alice", "since": 2020, "company": "Acme"}]
    assert rows == database.execute(statement, {"name": "Alice"}).fetchall()

    # The schema the store read when it was created stays until a refresh.
    assert graph.get_schema == EMPTY_SCHEMA
    graph.refresh_schema()
    schema = graph.get_structured_schema
    assert schema["node_props"] == {
        "Company": [
            {"property": "id", "type": "STRING"},
            {"property": "name", "type": "STRING"},
        ],
        "Person": [
            {"property": "id", "type": "STRING"},
            {"property": "name", "type": "STRING"},
        ],
    }
    assert schema["rel_props"] == {
        "WORKS_AT": [{"property": "since", "type": "INTEGER"}]
    }
    assert schema["relationships"] == [
        {"start": "Person", "type": "WORKS_AT", "end": "Company"}
    ]
    assert graph.get_schema == (
        "Node properties are the following:\n"
        "Company {id: STRING, name: STRING},Person {id: STRING, name: STRING}\n"
        "Relationship properties are the following:\n"
        "WORKS_AT {since: INTEGER}\n"
        "The relationships are the following:\n"
        "(:Person)-[:WORKS_AT]->(:Company)"
    )


@memory_only
def test_query_runs_the_statement_as_written(graph):
    assert graph.query("MATCH (n) RETURN count(n) AS count") == [{"count": 0}]
    assert graph.query(
        "UNWIND $values AS value RETURN value", {"values": [1, "a"]}
    ) == [
        {"value": 1},
        {"value": "a"},
    ]
    with pytest.raises(hawdb.exceptions.Error):
        graph.query("MATCH (n RETURN n")


def test_add_graph_documents_writes_nodes_and_relationships(graph):
    bob = Node(id="Bob", type="Person", properties={"age": 40})
    knows = Relationship(source=ALICE, target=bob, type="KNOWS", properties={})
    graph.add_graph_documents(
        [graph_document([ALICE, ACME], [works_at({"since": 2020}), knows])]
    )

    assert stored_nodes(graph) == [
        (["Company"], "Acme", {"city": "Paris"}),
        (["Person"], "Alice", {"age": 30}),
        # An endpoint that no document lists as a node gets only its id.
        (["Person"], "Bob", {}),
    ]
    assert stored_relationships(graph) == [
        ("Alice", "KNOWS", "Bob", {}),
        ("Alice", "WORKS_AT", "Acme", {"since": 2020}),
    ]
    # Nothing about the source document is stored without include_source.
    assert count(graph, f"(:{DOCUMENT_LABEL})") == 0


def test_existing_nodes_and_relationships_keep_their_properties(graph):
    graph.add_graph_documents(
        [graph_document([ALICE, ACME], [works_at({"since": 2020})])]
    )
    changed_alice = Node(id="Alice", type="Person", properties={"age": 31, "x": 1})
    changed = Relationship(
        source=changed_alice, target=ACME, type="WORKS_AT", properties={"since": 1999}
    )
    graph.add_graph_documents([graph_document([changed_alice], [changed])])

    assert graph.query("MATCH (n:Person) RETURN n.age AS age, n.x AS x") == [
        {"age": 30, "x": None}
    ]
    assert graph.query("MATCH ()-[r:WORKS_AT]->() RETURN r.since AS since") == [
        {"since": 2020}
    ]


def test_first_occurrence_in_a_call_wins(graph):
    first = Node(id="Alice", type="Person", properties={"age": 30})
    second = Node(id="Alice", type="Person", properties={"age": 99})
    # An endpoint mention before the node's own entry does not create it bare.
    bare_first = Relationship(
        source=Node(id="Alice", type="Person"), target=ACME, type="WORKS_AT"
    )
    graph.add_graph_documents(
        [
            graph_document([ACME], [bare_first]),
            graph_document([first, second], [works_at({"since": 1})]),
            graph_document([second], [works_at({"since": 2})]),
        ]
    )
    assert graph.query("MATCH (n:Person) RETURN n.id AS id, n.age AS age") == [
        {"id": "Alice", "age": 30}
    ]
    assert graph.query("MATCH ()-[r:WORKS_AT]->() RETURN r.since AS since") == [
        {"since": None}
    ]


def test_labels_are_part_of_node_identity(graph, database):
    human = Node(id="Alice", type="Human")
    graph.add_graph_documents(
        [
            graph_document(
                [ALICE], [Relationship(source=human, target=ACME, type="LIKES")]
            )
        ]
    )
    assert stored_nodes(graph) == [
        (["Company"], "Acme", {}),
        (["Human"], "Alice", {}),
        (["Person"], "Alice", {"age": 30}),
    ]
    # Labels that only endpoints have get an id index too.
    assert database.execute_sql(
        "SELECT subject_name FROM system.indexes ORDER BY subject_name"
    ).fetchall() == [
        {"subject_name": "Company"},
        {"subject_name": "Human"},
        {"subject_name": "Person"},
    ]


def test_integer_ids_and_self_relationships(graph):
    one = Node(id=1, type="Item", properties={"name": "one"})
    loop = Relationship(source=one, target=one, type="SAME")
    graph.add_graph_documents(
        [graph_document([one, Node(id="1", type="Item")], [loop])]
    )
    assert count(graph, "(:Item)") == 2
    assert graph.query("MATCH (n:Item {id: 1}) RETURN n.name AS name") == [
        {"name": "one"}
    ]
    assert graph.query("MATCH (n:Item {id: '1'}) RETURN n.name AS name") == [
        {"name": None}
    ]
    assert graph.query("MATCH (a)-[:SAME]->(b) RETURN a.id AS a, b.id AS b") == [
        {"a": 1, "b": 1}
    ]


def test_include_source_writes_documents_and_mentions(graph):
    text = "Alice works at Acme."
    metadata = {"source": "a.txt", "page": 3}
    bob = Node(id="Bob", type="Person")
    with_nodes = graph_document(
        [ALICE, ACME, ALICE],
        [works_at(), Relationship(source=ALICE, target=bob, type="KNOWS")],
        text,
        **metadata,
    )
    with_id = graph_document([ACME], [], "Acme is in Paris.", id="doc-2", text="kept")
    without_nodes = graph_document([], [], "Nothing here.")
    graph.add_graph_documents([with_nodes, with_id, without_nodes], include_source=True)

    digest = hashlib.md5(text.encode("utf-8")).hexdigest()
    assert graph.query(
        f"MATCH (d:{DOCUMENT_LABEL}) RETURN d.id AS id, d.text AS text, "
        "d.source AS source, d.page AS page ORDER BY d.text"
    ) == [
        {"id": digest, "text": "Alice works at Acme.", "source": "a.txt", "page": 3},
        {
            "id": hashlib.md5(b"Nothing here.").hexdigest(),
            "text": "Nothing here.",
            "source": None,
            "page": None,
        },
        # Metadata named "text" replaces the page content, as in Neo4jGraph.
        {"id": "doc-2", "text": "kept", "source": None, "page": None},
    ]
    # Each listed node is mentioned once; a bare endpoint is not mentioned.
    assert graph.query(
        f"MATCH (d:{DOCUMENT_LABEL})-[:{MENTIONS_TYPE}]->(n) "
        "RETURN d.id AS document, n.id AS node ORDER BY document, node"
    ) == [
        {"document": "doc-2", "node": "Acme"},
        {"document": digest, "node": "Acme"},
        {"document": digest, "node": "Alice"},
    ]
    # Unlike Neo4jGraph, the store leaves the caller's metadata as it was.
    assert with_nodes.source.metadata == metadata


def test_stored_documents_keep_their_properties(graph):
    graph.add_graph_documents(
        [
            graph_document([ALICE], page_content="first", id="d", source="a"),
            # Within a call the first document with an id wins, as for nodes.
            graph_document([], page_content="again", id="d", source="c"),
        ],
        include_source=True,
    )
    graph.add_graph_documents(
        [graph_document([ACME], page_content="second", id="d", source="b")],
        include_source=True,
    )
    assert graph.query(
        f"MATCH (d:{DOCUMENT_LABEL})-[:{MENTIONS_TYPE}]->(n) "
        "RETURN d.text AS text, d.source AS source, n.id AS node ORDER BY node"
    ) == [
        {"text": "first", "source": "a", "node": "Acme"},
        {"text": "first", "source": "a", "node": "Alice"},
    ]


def test_property_values_round_trip_with_their_schema_types(graph):
    values = {
        "text": "x",
        "number": 7,
        "ratio": 1.5,
        "flag": True,
        "items": [1, 2],
        "attributes": {"k": "v"},
        "raw": b"\x00\x01",
        "amount": Decimal("1.25"),
        "day": date(2020, 1, 2),
        "at": datetime(2020, 1, 2, 3, 4, 5, tzinfo=UTC),
        "local": datetime(2020, 1, 2, 3, 4, 5),
        "clock": time(3, 4, 5),
        "span": timedelta(days=1),
        "embedding": {"$vector": [0.5, 1.0]},
        "missing": None,
    }
    thing = Node(id="x", type="Thing", properties=values)
    graph.add_graph_documents([graph_document([thing])])

    [row] = graph.query("MATCH (n:Thing) RETURN n")
    stored = {name: value for name, value in row["n"].items() if name != "_id"}
    assert stored == {
        "id": "x",
        "labels": ["Thing"],
        "text": "x",
        "number": 7,
        "ratio": 1.5,
        "flag": True,
        "items": [1, 2],
        "attributes": {"k": "v"},
        "raw": b"\x00\x01",
        "amount": Decimal("1.25"),
        "day": {"$date": "2020-01-02"},
        "at": {"$datetime": "2020-01-02T03:04:05Z"},
        "local": {"$localdatetime": "2020-01-02T03:04:05"},
        "clock": {"$localtime": "03:04:05"},
        "span": {"$duration": "P1D"},
        "embedding": {"$vector": [0.5, 1.0]},
    }

    graph.refresh_schema()
    assert {
        item["property"]: item["type"]
        for item in graph.get_structured_schema["node_props"]["Thing"]
    } == {
        "amount": "DECIMAL",
        "at": "DATE_TIME",
        "attributes": "MAP",
        "clock": "LOCAL_TIME",
        "day": "DATE",
        "embedding": "VECTOR",
        "flag": "BOOLEAN",
        "id": "STRING",
        "items": "LIST",
        "local": "LOCAL_DATE_TIME",
        "number": "INTEGER",
        "ratio": "FLOAT",
        "raw": "BYTES",
        "span": "DURATION",
        "text": "STRING",
    }


@pytest.mark.parametrize(
    ("document", "message"),
    [
        (graph_document([Node(id="a", type="Programming language")]), "node type"),
        (graph_document([Node(id="a", type="Café")]), "node type"),
        (graph_document([Node(id="a", type="1st")]), "node type"),
        (graph_document([Node(id="a", type="")]), "node type"),
        (graph_document([Node(id="a", type="A`B")]), "node type"),
        (
            graph_document(
                [], [Relationship(source=ALICE, target=ACME, type="works at")]
            ),
            "relationship type",
        ),
        (
            graph_document(
                [],
                [
                    Relationship(
                        source=Node(id="a", type="Bad-Label"), target=ACME, type="R"
                    )
                ],
            ),
            "node type",
        ),
        (
            graph_document([Node(id="a", type="A", properties={"first name": "x"})]),
            "property name",
        ),
        (
            graph_document([], [works_at({"valid-from": 2020})]),
            "property name",
        ),
        (graph_document([ALICE], **{"Content-Type": "text/plain"}), "property name"),
    ],
)
@memory_only
def test_names_must_be_hawdb_cypher_names(database, document, message):
    graph = HawDBGraph(database)
    recording = _RecordingDatabase(database)
    graph._db = recording
    with pytest.raises(ValueError, match=message):
        graph.add_graph_documents(
            [graph_document([ALICE]), document], include_source=True
        )
    assert recording.transactions == []
    assert count(graph, "(n)") == 0


@pytest.mark.parametrize(
    "document",
    [
        graph_document([Node(id="a", type="A", properties={"id": "b"})]),
        graph_document([Node(id="a", type="A", properties={"labels": ["B"]})]),
        graph_document([Node(id="a", type="A", properties={"_id": 1})]),
        graph_document([], [works_at({"type": "employment"})]),
        graph_document([], [works_at({"source_id": 1})]),
        graph_document([], [works_at({"target_id": 1})]),
        graph_document([], [works_at({"_id": 1})]),
        graph_document([ALICE], labels=["A"]),
    ],
)
@memory_only
def test_property_names_that_reads_hide_are_refused(graph, document):
    with pytest.raises(ValueError, match="cannot store a property named"):
        graph.add_graph_documents([document], include_source=True)
    assert count(graph, "(n)") == 0


@pytest.mark.parametrize(
    ("document", "message"),
    [
        (
            graph_document([Node.model_construct(id=1.5, type="A", properties={})]),
            "id of a A node must be a str or int, not float",
        ),
        (
            graph_document([Node.model_construct(id=True, type="A", properties={})]),
            "not bool",
        ),
        (
            graph_document([ALICE], id=uuid.UUID(int=1)),
            "metadata id must be a str or int",
        ),
    ],
)
@memory_only
def test_ids_must_be_strings_or_integers(graph, document, message):
    with pytest.raises(TypeError, match=message):
        graph.add_graph_documents([document], include_source=True)
    assert count(graph, "(n)") == 0


@memory_only
def test_rejects_other_documents(graph):
    with pytest.raises(TypeError, match="GraphDocument"):
        graph.add_graph_documents([Document(page_content="text")])


def test_a_call_is_one_transaction_of_grouped_statements(graph, database):
    recording = _RecordingDatabase(database)
    graph._db = recording
    bob = Node(id="Bob", type="Person", properties={"age": 40})
    carol = Node(id="Carol", type="Person", properties={"age": 50, "city": "Rome"})
    graph.add_graph_documents(
        [
            graph_document(
                [ALICE, ACME, bob, carol],
                [
                    works_at(),
                    Relationship(source=bob, target=ACME, type="WORKS_AT"),
                    Relationship(
                        source=ALICE, target=Node(id="Dan", type="Person"), type="KNOWS"
                    ),
                ],
                id="doc",
            )
        ],
        include_source=True,
    )

    assert recording.autocommit == []
    [statements] = recording.transactions
    assert statements == [
        "CREATE INDEX ON :Company(id)",
        "CREATE INDEX ON :Document(id)",
        "CREATE INDEX ON :Person(id)",
        (
            "UNWIND $rows AS row MERGE (n:Document {id: row[0]}) "
            "ON CREATE SET n.text = row[1]"
        ),
        (
            "UNWIND $rows AS row MERGE (n:Person {id: row[0]}) "
            "ON CREATE SET n.age = row[1]"
        ),
        (
            "UNWIND $rows AS row MERGE (n:Company {id: row[0]}) "
            "ON CREATE SET n.city = row[1]"
        ),
        (
            "UNWIND $rows AS row MERGE (n:Person {id: row[0]}) "
            "ON CREATE SET n.age = row[1], n.city = row[2]"
        ),
        "UNWIND $rows AS row MERGE (n:Person {id: row[0]})",
        (
            "UNWIND $rows AS row MATCH (s:Person {id: row[0]}), "
            "(t:Company {id: row[1]}) MERGE (s)-[r:WORKS_AT]->(t)"
        ),
        (
            "UNWIND $rows AS row MATCH (s:Person {id: row[0]}), "
            "(t:Person {id: row[1]}) MERGE (s)-[r:KNOWS]->(t)"
        ),
        (
            "UNWIND $rows AS row MATCH (d:Document {id: row[0]}), "
            "(n:Person {id: row[1]}) MERGE (d)-[:MENTIONS]->(n)"
        ),
        (
            "UNWIND $rows AS row MATCH (d:Document {id: row[0]}), "
            "(n:Company {id: row[1]}) MERGE (d)-[:MENTIONS]->(n)"
        ),
    ]
    assert recording.parameters[3:] == [
        {"rows": [["doc", "Alice works at Acme."]]},
        {"rows": [["Alice", 30], ["Bob", 40]]},
        {"rows": [["Acme", "Paris"]]},
        {"rows": [["Carol", 50, "Rome"]]},
        {"rows": [["Dan"]]},
        {"rows": [["Alice", "Acme"], ["Bob", "Acme"]]},
        {"rows": [["Alice", "Dan"]]},
        {"rows": [["doc", "Alice"], ["doc", "Bob"], ["doc", "Carol"]]},
        {"rows": [["doc", "Acme"]]},
    ]

    # The indexes exist now, so a later call does not create them again.
    recording.clear()
    graph.add_graph_documents([graph_document([ALICE])])
    assert recording.transactions == [
        [
            (
                "UNWIND $rows AS row MERGE (n:Person {id: row[0]}) "
                "ON CREATE SET n.age = row[1]"
            )
        ]
    ]


def test_a_failed_call_writes_nothing(graph, database):
    graph.add_graph_documents([graph_document([ACME])])
    recording = _RecordingDatabase(database, fail_on="MENTIONS")
    graph._db = recording
    with pytest.raises(RuntimeError, match="injected failure"):
        graph.add_graph_documents(
            [graph_document([ALICE, ACME], [works_at()])], include_source=True
        )
    graph._db = database
    assert len(recording.transactions[0]) > 3
    assert stored_nodes(graph) == [(["Company"], "Acme", {"city": "Paris"})]
    assert stored_relationships(graph) == []
    assert database.execute_sql(
        "SELECT subject_name FROM system.indexes ORDER BY subject_name"
    ).fetchall() == [{"subject_name": "Company"}]


# The store counts 32 bytes for each value, list element and map entry, the
# UTF-8 bytes of each string and four bytes per vector coordinate. A node "x"
# with one property "text" so takes 33 + 32 + 36 + 32 bytes besides its text.
def text_node(label, extra=0):
    text = "t" * (MAX_NODE_BYTES - 133 + extra)
    return Node(id="x", type=label, properties={"text": text})


def items_node(extra=0):
    items = [7] * ((MAX_NODE_BYTES - 134) // 32 + extra)
    return Node(id="x", type="Items", properties={"items": items})


def vector_node(extra=0):
    vector = [0.5] * ((MAX_NODE_BYTES - 130) // 4 + extra)
    return Node(id="x", type="Vector", properties={"v": {"$vector": vector}})


def linked(extra=0):
    note = "n" * (MAX_RELATIONSHIP_BYTES - 100 + extra)
    return Relationship(
        source=text_node("Text"),
        target=items_node(),
        type="LINKS",
        properties={"note": note},
    )


@memory_only
@pytest.mark.parametrize(
    ("document", "message"),
    [
        (lambda: graph_document([text_node("Text", 1)]), "node ('Text', 'x')"),
        (lambda: graph_document([items_node(1)]), "node ('Items', 'x')"),
        (lambda: graph_document([vector_node(1)]), "node ('Vector', 'x')"),
        (
            lambda: graph_document([], [], "d" * (MAX_NODE_BYTES - 132), id="d"),
            "source document 'd'",
        ),
        (lambda: graph_document([], [linked(1)]), "the properties of relationship"),
        (
            lambda: graph_document(
                [],
                [
                    Relationship(
                        source=Node(id="i" * (MAX_NODE_BYTES - 31), type="Long"),
                        target=ACME,
                        type="R",
                    )
                ],
            ),
            "the id of a Long node",
        ),
    ],
)
def test_nodes_too_large_to_read_back_are_refused(graph, document, message):
    with pytest.raises(ValueError, match=r"so that HawDB can read it back") as error:
        graph.add_graph_documents([document()], include_source=True)
    assert str(error.value).startswith(message)
    assert len(str(error.value)) < 300
    assert count(graph, "(n)") == 0


def test_nodes_at_the_size_limits_read_back(graph, database):
    graph.add_graph_documents(
        [graph_document([text_node("Text"), items_node()], [linked()])]
    )
    graph.add_graph_documents(
        [graph_document([vector_node()], [], "d" * (MAX_NODE_BYTES - 133), id="d")],
        include_source=True,
    )

    graph.refresh_schema()
    assert graph.get_schema.splitlines()[1:4] == [
        (
            "Document {id: STRING, text: STRING},Items {id: STRING, items: LIST},"
            "Text {id: STRING, text: STRING},Vector {id: STRING, v: VECTOR}"
        ),
        "Relationship properties are the following:",
        "LINKS {note: STRING}",
    ]
    with database.read_transaction() as snapshot:
        for label in (DOCUMENT_LABEL, "Items", "Text", "Vector"):
            [row] = snapshot.execute(f"MATCH (n:{label}) RETURN n").fetchall()
            assert row["n"]["id"] in ("d", "x")
        [row] = snapshot.execute("MATCH ()-[r:LINKS]->() RETURN r").fetchall()
        assert len(row["r"]["note"]) == MAX_RELATIONSHIP_BYTES - 100


def test_a_value_the_binding_refuses_writes_nothing(graph):
    unsupported = Node(id="b", type="Other", properties={"tags": {"a", "b"}})
    with pytest.raises(TypeError, match="unsupported parameter type: set"):
        graph.add_graph_documents(
            [graph_document([ALICE, ACME], [works_at()]), graph_document([unsupported])]
        )
    assert count(graph, "(n)") == 0


@memory_only
def test_calls_with_nothing_to_write_open_no_transaction(graph, database):
    recording = _RecordingDatabase(database)
    graph._db = recording
    graph.add_graph_documents([])
    graph.add_graph_documents([graph_document()])
    assert recording.transactions == []


def test_schema_lists_properties_the_statistics_leave_out(graph):
    tagged = Node(
        id="t",
        type="Tagged",
        properties={"tags": ["a"], "extra": {"k": 1}, "embedding": {"$vector": [1.0]}},
    )
    link = Relationship(
        source=tagged, target=tagged, type="LINKS", properties={"path": [1, 2]}
    )
    graph.add_graph_documents([graph_document([tagged], [link])])
    graph.refresh_schema()
    schema = graph.get_structured_schema
    assert schema["node_props"]["Tagged"] == [
        {"property": "embedding", "type": "VECTOR"},
        {"property": "extra", "type": "MAP"},
        {"property": "id", "type": "STRING"},
        {"property": "tags", "type": "LIST"},
    ]
    assert schema["rel_props"] == {"LINKS": [{"property": "path", "type": "LIST"}]}


def test_schema_lists_properties_the_sampled_elements_lack(graph):
    # The refresh reads one node per label and one relationship per type; the
    # statistics add what the others have, whichever ones it reads.
    bob = Node(id="Bob", type="Person", properties={"nickname": "B"})
    leads = Relationship(
        source=bob, target=ACME, type="WORKS_AT", properties={"role": "CEO"}
    )
    graph.add_graph_documents(
        [graph_document([ALICE, bob, ACME], [works_at({"since": 2020}), leads])]
    )
    graph.refresh_schema()
    schema = graph.get_structured_schema
    assert schema["node_props"]["Person"] == [
        {"property": "age", "type": "INTEGER"},
        {"property": "id", "type": "STRING"},
        {"property": "nickname", "type": "STRING"},
    ]
    assert schema["rel_props"]["WORKS_AT"] == [
        {"property": "role", "type": "STRING"},
        {"property": "since", "type": "INTEGER"},
    ]


def test_schema_text_matches_construct_schema(graph):
    graph.add_graph_documents(
        [graph_document([ALICE, ACME], [works_at({"since": 2020})])],
        include_source=True,
    )
    graph.refresh_schema()
    assert graph.get_schema == construct_schema(graph.get_structured_schema, [], [])
    assert graph.get_schema.splitlines()[-1] == (
        "(:Document)-[:MENTIONS]->(:Company),(:Document)-[:MENTIONS]->(:Person),"
        "(:Person)-[:WORKS_AT]->(:Company)"
    )
    # A label or type without properties is left out, as in Neo4jGraph.
    assert "MENTIONS" not in graph.get_structured_schema["rel_props"]


def test_schema_metadata_lists_constraints_and_indexes(graph):
    graph.query("CREATE CONSTRAINT ON :Person(email) ASSERT UNIQUE")
    graph.add_graph_documents([graph_document([ALICE])])
    graph.refresh_schema()
    metadata = graph.get_structured_schema["metadata"]
    assert {
        (row["subject_name"], row["property_name"], row["constraint_kind"])
        for row in metadata["constraint"]
    } == {("Person", "email", "node_property_unique")}
    assert ("Person", ["id"], "equality") in [
        (row["subject_name"], row["property_names"], row["index_kind"])
        for row in metadata["index"]
    ]


def test_refresh_schema_false_reads_nothing(database):
    database.execute("CREATE (:Person {id: 'p'})")
    graph = HawDBGraph(database, refresh_schema=False)
    assert graph.get_schema == ""
    assert graph.get_structured_schema == {}
    graph.refresh_schema()
    assert "Person {id: STRING}" in graph.get_schema


def test_a_read_only_database_serves_queries_and_the_schema(tmp_path):
    with hawdb.open(tmp_path / "graph") as db:
        HawDBGraph(db).add_graph_documents(
            [graph_document([ALICE, ACME], [works_at()])]
        )

    with hawdb.open(tmp_path / "graph", read_only=True) as db:
        graph = HawDBGraph(db)
        assert graph.get_structured_schema["relationships"] == [
            {"start": "Person", "type": "WORKS_AT", "end": "Company"}
        ]
        assert graph.query("MATCH (n:Person) RETURN n.id AS id") == [{"id": "Alice"}]
        with pytest.raises(hawdb.exceptions.Error, match="read-only"):
            graph.add_graph_documents([graph_document([Node(id="Bob", type="Person")])])
        assert count(graph, "(n:Person)") == 1


def test_read_budgets_set_on_open_apply_to_queries():
    with hawdb.open(max_read_result_rows=1) as db:
        graph = HawDBGraph(db)
        graph.add_graph_documents([graph_document([ALICE, ACME])])
        assert graph.query("MATCH (n:Person) RETURN n.id AS id") == [{"id": "Alice"}]
        with pytest.raises(hawdb.exceptions.Error, match="max_read_result_rows"):
            graph.query("MATCH (n) RETURN n.id AS id")


@memory_only
def test_other_threads_get_wrong_thread_error(graph):
    calls = {
        "query": lambda: graph.query("MATCH (n) RETURN count(n) AS count"),
        "refresh_schema": graph.refresh_schema,
        "add_graph_documents": lambda: graph.add_graph_documents(
            [graph_document([ALICE])]
        ),
    }
    errors = {}

    def use_graph():
        for name, call in calls.items():
            # Anything else, such as a Rust panic, ends the thread and the test
            # finds the call missing.
            try:
                call()
            except WrongThreadError as error:
                errors[name] = error

    worker = threading.Thread(target=use_graph)
    worker.start()
    worker.join()
    assert {name: type(error) for name, error in errors.items()} == dict.fromkeys(
        calls, WrongThreadError
    )
    assert "call invoke" in str(errors["query"])
    assert graph.query("MATCH (n) RETURN count(n) AS count") == [{"count": 0}]


def test_rejects_other_clients():
    with pytest.raises(TypeError, match=r"hawdb\.Database"):
        HawDBGraph("graph.db")


CYPHER = (
    "MATCH (p:Person {id: 'Alice'})-[:WORKS_AT]->(c:Company) RETURN c.id AS company"
)


@memory_only
def test_graph_cypher_qa_chain_runs_on_the_store(graph):
    graph.add_graph_documents([graph_document([ALICE, ACME], [works_at()])])
    graph.refresh_schema()
    prompts = _Prompts()
    chain = GraphCypherQAChain.from_llm(
        llm=FakeListLLM(responses=[CYPHER, "Alice works at Acme."]),
        graph=graph,
        allow_dangerous_requests=True,
        return_intermediate_steps=True,
    )

    result = chain.invoke(
        {"query": "Where does Alice work?"}, config={"callbacks": [prompts]}
    )
    assert result["result"] == "Alice works at Acme."
    assert result["intermediate_steps"] == [
        {"query": CYPHER},
        {"context": [{"company": "Acme"}]},
    ]
    assert graph.get_schema in prompts.prompts[0]


@memory_only
def test_async_chains_run_on_a_worker_thread(graph):
    chain = GraphCypherQAChain.from_llm(
        llm=FakeListLLM(responses=[CYPHER, "unused"]),
        graph=graph,
        allow_dangerous_requests=True,
    )
    with pytest.raises(WrongThreadError):
        asyncio.run(chain.ainvoke({"query": "Where does Alice work?"}))


class _Prompts(BaseCallbackHandler):
    def __init__(self):
        self.prompts = []

    def on_llm_start(self, serialized, prompts, **kwargs):
        self.prompts.extend(prompts)


class _Recorder:
    """Forwards a transaction or snapshot and records the statements it runs."""

    def __init__(self, inner, statements, parameters, fail_on=None):
        self._inner = inner
        self._statements = statements
        self._parameters = parameters
        self._fail_on = fail_on

    def __enter__(self):
        self._inner.__enter__()
        return self

    def __exit__(self, *exception):
        return self._inner.__exit__(*exception)

    def execute(self, statement, *params):
        if self._fail_on is not None and self._fail_on in statement:
            raise RuntimeError("injected failure")
        self._statements.append(statement)
        self._parameters.append(params[0] if params else None)
        return self._inner.execute(statement, *params)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _RecordingDatabase:
    """Records the Cypher statements of each write transaction, and the reads.

    A write statement containing ``fail_on`` fails before it runs.
    """

    def __init__(self, database, fail_on=None):
        self._database = database
        self._fail_on = fail_on
        self.transactions = []
        self.parameters = []
        self.reads = []
        self.autocommit = []

    def transaction(self):
        self.transactions.append([])
        return _Recorder(
            self._database.transaction(),
            self.transactions[-1],
            self.parameters,
            self._fail_on,
        )

    def read_transaction(self):
        return _Recorder(self._database.read_transaction(), self.reads, [])

    def execute(self, statement, *params):
        self.autocommit.append(statement)
        return self._database.execute(statement, *params)

    def clear(self):
        self.transactions.clear()
        self.parameters.clear()
        self.reads.clear()
        self.autocommit.clear()

    def __getattr__(self, name):
        return getattr(self._database, name)
