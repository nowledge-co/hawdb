# LangChain graph store for HawDB

`langchain-hawdb` is a LangChain `GraphStore` over an embedded HawDB database.
`HawDBGraph` stores the `GraphDocument`s that `LLMGraphTransformer` extracts,
runs parameterized Cypher, and gives `GraphCypherQAChain` the schema of the
stored graph.

The package is not published on PyPI. Install it from a checkout of this
repository; pip takes `hawdb` and `langchain-community` from PyPI:

```bash
pip install ./integrations/langchain-hawdb
```

It needs Python 3.11 or later, `hawdb` 0.28.0 or later and a
`langchain-community` 0.4 release from 0.4.2. LangChain's `GraphStore` and
`GraphDocument` live in `langchain-community`, which warns on import that it is
being sunset.

HawDB's on-disk format carries no compatibility promise until HawDB declares
one, so a database may need to be recreated after an upgrade.

## Usage

```python
import hawdb
from langchain_community.graphs.graph_document import (
    GraphDocument,
    Node,
    Relationship,
)
from langchain_core.documents import Document
from langchain_hawdb import HawDBGraph

alice = Node(id="Alice", type="Person", properties={"age": 30})
acme = Node(id="Acme", type="Company")
document = GraphDocument(
    nodes=[alice, acme],
    relationships=[Relationship(source=alice, target=acme, type="WORKS_AT")],
    source=Document(page_content="Alice works at Acme."),
)

with hawdb.open("./graph") as db:
    graph = HawDBGraph(db)
    graph.add_graph_documents([document], include_source=True)
    graph.refresh_schema()
    print(graph.get_schema)
    print(
        graph.query(
            "MATCH (p:Person {id: $name})-[:WORKS_AT]->(c:Company) "
            "RETURN c.id AS company",
            {"name": "Alice"},
        )
    )
```

```text
Node properties are the following:
Company {id: STRING},Document {id: STRING, text: STRING},Person {age: INTEGER, id: STRING}
Relationship properties are the following:

The relationships are the following:
(:Document)-[:MENTIONS]->(:Company),(:Document)-[:MENTIONS]->(:Person),(:Person)-[:WORKS_AT]->(:Company)
[{'company': 'Acme'}]
```

`query` runs your statement as written with `Database.execute` and returns its
rows as dicts; bind values as `$name` parameters. The store runs statements on
the database you pass and never opens or closes it, so pass read budgets such
as `max_read_result_rows`, or `read_only=True`, to `hawdb.open`.

### Graphs extracted by an LLM

`LLMGraphTransformer` from `langchain-experimental` turns documents into graph
documents:

```python
from langchain_experimental.graph_transformers import LLMGraphTransformer

transformer = LLMGraphTransformer(
    llm=llm,
    allowed_nodes=["Person", "Company", "City"],
    allowed_relationships=[
        ("Person", "WORKS_AT", "Company"),
        ("Company", "LOCATED_IN", "City"),
    ],
)
graph_documents = transformer.convert_to_graph_documents(documents)

with hawdb.open("./graph") as db:
    HawDBGraph(db).add_graph_documents(graph_documents, include_source=True)
```

`llm` is any LangChain chat model and `documents` a list of `Document`s. The
allowed lists keep the names to ones HawDB Cypher can spell and keep a call's
statements few (see [Limits](#limits)). Without them the LLM names node and
relationship types freely, and the store refuses a call with a name such as
`Programming language`. Replace such names before you add the documents:

```python
import re


def hawdb_name(name):
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)
    return f"_{name}" if name[:1].isdigit() else name


def with_hawdb_names(document):
    def node(n):
        return Node(id=n.id, type=hawdb_name(n.type), properties=n.properties)

    return GraphDocument(
        nodes=[node(n) for n in document.nodes],
        relationships=[
            Relationship(
                source=node(r.source),
                target=node(r.target),
                type=hawdb_name(r.type),
                properties=r.properties,
            )
            for r in document.relationships
        ],
        source=document.source,
    )


with hawdb.open("./graph") as db:
    HawDBGraph(db).add_graph_documents(
        [with_hawdb_names(d) for d in graph_documents], include_source=True
    )
```

`hawdb_name` turns every other character into an underscore, so different
names can become the same one, such as two names in another script.

### Question answering

```python
from langchain_community.chains.graph_qa.cypher import GraphCypherQAChain

with hawdb.open("./graph", read_only=True) as db:
    chain = GraphCypherQAChain.from_llm(
        llm=llm, graph=HawDBGraph(db), allow_dangerous_requests=True
    )
    print(chain.invoke({"query": "Where does Alice work?"})["result"])
```

The chain runs whatever Cypher the LLM writes, which is why it requires
`allow_dangerous_requests=True`. Open the database with `read_only=True`, as
here, and HawDB refuses any statement that would change it.

The chain's default prompt asks for Neo4j Cypher. HawDB Cypher is a subset: it
has no backtick-quoted names, `OPTIONAL MATCH` or `toLower`, among others, so
a generated statement can fail with a `hawdb.exceptions.Error` from `invoke`.
Pass a `cypher_prompt` that describes the dialect if that happens often.

## Schema

Creating the store reads the schema unless you pass `refresh_schema=False`.
`add_graph_documents` does not read it again, so call `refresh_schema()` after
writing. `get_schema` returns the schema as text, in the layout of
`Neo4jGraph`, and `get_structured_schema` as the dict `Neo4jGraph` uses, with
`node_props`, `rel_props`, `relationships` and `metadata`. Like `Neo4jGraph`,
it lists only labels and relationship types that have properties under
`node_props` and `rel_props`.

`refresh_schema` reads in one read snapshot, with the catalog statements HawDB
already has:

- labels, relationship types, `(start)-[type]->(end)` patterns and property
  statistics from `system.graph_statistics`;
- constraints from `system.constraints` and indexes from `system.indexes`,
  which `metadata` lists as HawDB returns them;
- one node of each label and one relationship of each type.

The statistics leave out list, map and vector properties, long strings,
properties whose values differ in type, and properties with many distinct
values once their histograms outgrow their memory budget. So such a property
appears only if the one node or relationship the refresh reads has it. A
property's type is the type of the first value the statistics or that node or
relationship show, such as `STRING`, `INTEGER`, `LIST` or `DATE`.

## Writing graph documents

- `add_graph_documents` merges each node by its type and `id`, and each
  relationship by its source, type and target. A node or relationship that
  does not exist yet is created with the properties of its first occurrence in
  the call; one that exists keeps its stored properties. Change stored
  properties with `query`.
- A relationship endpoint that no document in the call lists among its nodes is
  created with only its `id`.
- With `include_source=True`, each source document becomes a `Document` node
  with its `page_content` as `text`, its metadata as properties, and a
  `MENTIONS` relationship to each node the document lists. Its id is
  `metadata["id"]`, or else the MD5 digest of its text, and metadata named
  `text` replaces the page content. Like other nodes, a `Document` is created
  from its first occurrence in the call, and a stored one keeps its properties.
- The whole call is one transaction, so a call that fails writes nothing. The
  first call that writes a label also creates an equality index on its `id`,
  in the same transaction.
- The store checks the whole call before it writes. Node types, relationship
  types and property names must be ASCII letters, digits and underscores, not
  starting with a digit. Node properties cannot be named `id`, `_id` or
  `labels`, and relationship properties cannot be named `_id`, `source_id`,
  `target_id` or `type`, because `RETURN n` and `RETURN r` use those keys. Ids
  must be strings or integers. The store raises `ValueError` or `TypeError` for
  anything else.
- A property whose value is `None` is not stored. Other values are stored as
  the `hawdb` binding converts them, including dates, times, `Decimal`, bytes,
  lists, dicts and `{"$vector": [...]}` vectors; a value it cannot convert,
  such as a set, raises `TypeError` and the call writes nothing.

### Differences from `Neo4jGraph`

- There is no `baseEntityLabel` argument; nodes are merged by type and `id`.
- Names are not changed. `Neo4jGraph` replaces spaces in relationship types with
  underscores and uppercases them, and quotes other names with backticks. HawDB
  Cypher has no quoting, so the store refuses names it cannot spell.
- A stored `Document` keeps its text and metadata; `Neo4jGraph` overwrites them
  each time the document is added.
- The store does not write the generated id into `document.source.metadata`.
- A node listed in a call is created with its properties even when an earlier
  document in the call names it only as a relationship endpoint.
- The whole call is one transaction. `Neo4jGraph` runs two statements per
  document, so a failure leaves the earlier documents written.
- `get_structured_schema["metadata"]` holds HawDB's constraint and index rows,
  and there is no enhanced schema with example values.

## Threads

A `hawdb.Database` stays on the thread that opened it. Create the store on that
thread and use it only there: another thread that calls the store gets
`WrongThreadError`. LangChain runs `ainvoke` and `abatch`, and `batch` over
several inputs, on worker threads, so call `invoke` instead.

Python's garbage collector frees objects in reference cycles on whichever
thread it runs, and HawDB does not free a database on another thread: it
reports that the database "is unsendable, but is being dropped on another
thread" and leaks the object. Close the database when you finish, as the
examples do; a closed database that leaks this way keeps no files open.

## Limits

- Each write statement after the first write in a transaction takes time that
  grows with the stored graph. So a call writes its rows with one `UNWIND`
  statement per label and set of property names, one per source label,
  relationship type, target label and set of property names, and one
  `MENTIONS` statement per label, whatever the number of documents. Add many
  documents per call, and keep the labels and relationship patterns few. On an
  Apple M5 Pro, a call of 100 chunks with 8 labels and 25 relationship patterns
  ran about 42 statements and took 0.16 s on an empty graph, 1.2 s with 10,000
  nodes and 20,000 relationships stored, and 11 s with 100,000 nodes and
  200,000 relationships. Choosing among the same 8 labels and 15 relationship
  types at random made about 640 statements a call, and a call took 20 s with
  10,000 nodes stored. `refresh_schema` took 0.12 s and 1.1 s at those two
  sizes.
- HawDB refuses a transaction of more than 100,000 version entries, about one
  per node and three per relationship, `MENTIONS` included, and on a file
  database one whose WAL record exceeds 16 MiB, about the UTF-8 bytes and four
  bytes per vector coordinate written, plus 100 to 400 bytes per row. Split a
  larger import into several calls.
- A statement that reads a node holds it with every property it stores, and
  `RETURN n` holds it twice, while HawDB refuses an intermediate row above
  16 MiB. So that `refresh_schema` and your own queries can read every node
  back, the store refuses a node, source documents included, above 4 MiB, and
  relationship properties above 1 MiB. It counts the UTF-8 bytes of each
  string, four bytes per vector coordinate and 32 bytes for each value, list
  element and map entry. Nodes and relationships your own Cypher writes are
  not checked.
- `query` runs in autocommit, where HawDB charges the result against its query
  memory, 16 MiB, and refuses a larger one with `ResourceBudgetError`; about
  8 MiB of returned text already exceeds it. The Python binding does not expose
  that setting. Run a large read in `db.read_transaction()` instead, where
  `max_read_result_payload_bytes`, 64 MiB unless you pass another value to
  `hawdb.open`, limits the result.
- The read budgets you pass to `hawdb.open` also apply to the store's own
  reads. `refresh_schema` reads one row for each label, relationship type,
  pattern and property statistic, and `add_graph_documents` reads one row for
  each node index, so a lower `max_read_result_rows` makes them fail.

## Tests

From the repository root:

```bash
pip install ./integrations/langchain-hawdb pytest
```

```bash
pytest integrations/langchain-hawdb/tests
```

The tests use in-memory and file-backed databases and need no network access
or API keys.
