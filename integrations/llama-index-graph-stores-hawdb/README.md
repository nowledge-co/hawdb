# LlamaIndex property graph store for HawDB

`llama-index-graph-stores-hawdb` is a LlamaIndex `PropertyGraphStore` over an
embedded HawDB database. `PropertyGraphIndex` keeps its entities, relations and
text chunks in a HawDB file, and `structured_query` runs parameterized Cypher
on them.

The package is not published on PyPI. Install it from a checkout of this
repository; pip takes `hawdb` and `llama-index-core` from PyPI:

```bash
pip install ./integrations/llama-index-graph-stores-hawdb
```

It needs Python 3.11 or later, `hawdb` 0.28.0 or later and a
`llama-index-core` 0.14 release from 0.14.25.

## Usage

```python
import hawdb
from llama_index.core import PropertyGraphIndex, SimpleDirectoryReader
from llama_index.graph_stores.hawdb import HawDBPropertyGraphStore

documents = SimpleDirectoryReader("./data").load_data()

with hawdb.open("./graph") as db:
    graph_store = HawDBPropertyGraphStore(db)
    index = PropertyGraphIndex.from_documents(
        documents,
        property_graph_store=graph_store,
        llm=llm,
        embed_model=embed_model,
    )
    retriever = index.as_retriever(include_text=True)
    for result in retriever.retrieve("Where does Alice work?"):
        print(result.text)
```

`llm` and `embed_model` are any LlamaIndex LLM and embedding model; leave them
out to use `Settings.llm` and `Settings.embed_model`. The store runs statements
on the database you pass and never opens or closes it.

The store does not run vector queries, so `PropertyGraphIndex` keeps entity
embeddings in its own vector store, which lives in memory unless you pass one.
To keep similarity retrieval after a restart, persist that vector store and
pass it back with the graph:

```python
index.storage_context.vector_store.persist(persist_path="./vectors.json")
```

```python
from llama_index.core.vector_stores import SimpleVectorStore

with hawdb.open("./graph") as db:
    index = PropertyGraphIndex.from_existing(
        property_graph_store=HawDBPropertyGraphStore(db),
        vector_store=SimpleVectorStore.from_persist_path("./vectors.json"),
        llm=llm,
        embed_model=embed_model,
    )
```

Without the vector store, a reopened index finds entities only through
`LLMSynonymRetriever`, which looks them up by the names the LLM suggests.

A reopened index also starts with an empty docstore, so `delete_ref_doc` logs
that the document was not found and deletes nothing. Delete a document's
chunks from the graph store instead:

```python
index.property_graph_store.delete_llama_nodes(ref_doc_ids=["doc-1"])
```

## Storage layout

HawDB Cypher takes labels and relationship types only as statement text, so the
store does not turn LlamaIndex labels into HawDB labels. Every node has the label
`__Node__`, every relationship has the type `__REL__`, and the LlamaIndex label
is the `label` property.

| Node property | Value |
|---|---|
| `id` | The LlamaIndex node id; unique. |
| `kind` | `entity` or `chunk`. |
| `label` | The LlamaIndex label, such as `PERSON` or `text_chunk`. |
| `name` | The entity name. |
| `text` | The chunk text. |
| `properties` | The LlamaIndex properties as one JSON object. |
| `triplet_source_id`, `ref_doc_id` | Indexed copies of those string properties, which LlamaIndex uses to find the nodes of a chunk or a document. |
| `embedding` | The node embedding as a HawDB vector of 32-bit floats, which the Python binding returns as `{"$vector": [...]}`. |
| `digest` | A hash of the other properties, so an upsert finds changed nodes without reading them back. |

A relationship has a `label` and a `properties` JSON object. Opening the store
creates a unique constraint on `id` and equality indexes on `id`,
`triplet_source_id` and `ref_doc_id`, and skips any the catalog already lists,
so a database the store has initialized also opens with `read_only=True`.

`structured_query` runs your Cypher as written. Match labels as property values
and read JSON members with `json_value`:

```python
rows = graph_store.structured_query(
    "MATCH (a:__Node__ {label: $label})-[r:__REL__]->(b:__Node__) "
    "RETURN a.name AS source, r.label AS relation, b.name AS target, "
    "json_value(r.properties, '$.triplet_source_id') AS chunk",
    {"label": "PERSON"},
)
```

The store reads every `__Node__` and `__REL__` as its own. If your Cypher
creates any, give each node a string `id`, each relationship a string `label`,
and any `properties` a JSON object, and keep at most one relationship of a
label from one node to another, as `upsert_relations` does. Otherwise reads
can fail, or `get_rel_map` can return fewer than `limit` relations.

`get_schema_str()` and `aget_schema_str()` describe this layout and the stored
labels for a text-to-Cypher prompt.

## Behavior

- `upsert_nodes` replaces a stored node with the same id. It compares digests,
  so it rewrites only the nodes that changed and keeps any other properties
  your own Cypher added. If your Cypher changes one of the store's properties,
  also set the node's `digest` to null, or a later upsert of the original
  content skips the node.
- `upsert_relations` keeps a relation that already exists with the same source,
  label and target, as `SimplePropertyGraphStore` does, and creates missing
  endpoints as entities named by their id.
- A file database logs each transaction as one WAL record of at most 16 MiB,
  and HawDB refuses a transaction that would take more than 100,000 mutation
  operations or version entries. An upsert is one transaction unless it would
  exceed about 8 MiB or
  10,000 nodes or relations; then it commits in several transactions, each
  holding whole nodes or relations, and a failure can leave the earlier ones
  written. `PropertyGraphIndex.from_documents` passes every chunk in one call,
  so large inserts take this path.
- `get_triplets` returns relations between entities, like the Neo4j store; a
  relation with a chunk at either end, such as the `SOURCE`, `NEXT` and
  `PREVIOUS` relations of LlamaIndex's `ImplicitPathExtractor`, is not a
  triplet.
- `delete` removes what each filter selects on its own, and a deleted node takes
  its relations with it. A delete is one transaction unless it would remove
  more than 30,000 nodes and relations; then it commits in several
  transactions, and a failure can leave the earlier ones deleted. A node with
  too many relations for one transaction goes last, after the other selected
  nodes have taken their relations to it with them. So
  `delete_llama_nodes(ref_doc_ids=[...])` also deletes the document's entity,
  which `ImplicitPathExtractor` links to each chunk with a `SOURCE` relation,
  however many chunks it has. A node that would still take 30,000 or more
  relations with it, or the source of more than 30,000 relations to delete by
  label, is refused before anything is deleted; delete some of those relations
  with `structured_query` first.
- `None` for `ids`, `entity_names` or `relation_names` means no filter; an empty
  list matches nothing.
- A property filter compares each member with `=`. A key has 1 to 254 ASCII
  letters, digits, `_` or `-`, and a value is a string, number or boolean; an
  integer must fit in 64 signed bits. HawDB compares JSON values by type, so
  `3` does not match `3.0` and `True` does not match `1`, unlike
  `SimplePropertyGraphStore`.

## Threads and notebooks

A `hawdb.Database` stays on the thread that opened it. Create the store on that
thread and use it only there. Another thread that calls the store gets
`WrongThreadError`; HawDB itself raises `PanicException` when a database is
used on another thread, which is what creating the store, or using its `client`,
on another thread raises. When an event loop is already running, as in Jupyter,
LlamaIndex runs async retrieval on a worker thread, so pass `use_async=False`
there:

```python
retriever = index.as_retriever(include_text=True, use_async=False)
```

A `PropertyGraphIndex` holds its store, and so the database, in reference
cycles, and Python's garbage collector frees those on whichever thread it runs.
HawDB does not free a database on another thread: it reports that the database
"is unsendable, but is being dropped on another thread" and leaks the object.
Close the database when you finish, as the examples do; a closed database that
leaks this way keeps no files open.

## Limits

- Each write statement the store runs takes time that grows with the stored
  graph, so an upsert writes its new nodes, or its relations and their
  endpoints, with one or two statements per transaction and skips unchanged
  nodes. HawDB has no batched form of rewriting a changed node, and that
  statement scans the stored nodes, so each changed node costs about 90 ms with
  50,000 entities stored. `PropertyGraphIndex` skips the entities it has
  stored and the chunks whose content is unchanged, so it rewrites nodes only
  when a stored chunk changes.
- Merging a relation reads the existing relations of its source, so adding
  relations to a node with many of them is slow: about 2 ms per relation from
  a node with 16,000.
- Deleting a node reads every relationship in the database, so deleting many
  nodes from a large graph is slow: about 11 ms per node with 50,000
  relationships stored.
- `PropertyGraphIndex` refreshes the schema after every insert, and that reads
  every node and relationship.
- On a file database every write transaction waits for the disk.
  `delete_ref_doc` deletes a document's chunks one at a time, each in its own
  transaction; `delete_llama_nodes(ref_doc_ids=[...])` deletes them together.
- The store reads over read snapshots, where HawDB refuses a statement whose
  result exceeds `max_read_result_payload_bytes`, 64 MiB unless you pass
  another value to `hawdb.open`. `get` without ids, and `delete` with only a
  property filter, select the matching ids with one statement, about 50 bytes
  per node with the 36-character ids LlamaIndex gives chunks, so such a call
  fails once it matches about 1.3 million nodes. Given ids or names, they
  select 500 at a time. `get` reads the nodes in pages of at most 500 nodes
  and 2 million characters of their ids, labels, names, text and properties.
  A page holds at most 8 MiB of UTF-8, or one node of at most about 9.6 MB,
  so a limit of 10 MiB or more holds every page. `get_triplets` returns its
  result from one statement: within 64 MiB, an unfiltered `get_triplets()`
  returns about 440,000 relations with short names and empty properties.
  `get_rel_map` runs one statement per hop, each returning at most twice
  `limit` rows.
- `structured_query` runs in autocommit, where HawDB refuses a result that
  needs more than its query memory, 16 MiB by default, with
  `ResourceBudgetError`; the Python binding does not expose that setting. For
  a large read, run the query in `graph_store.client.read_transaction()`.
- `get` sizes each node with one `size()` expression over its id, label,
  name, text and properties, and HawDB refuses an expression whose string
  functions read more than 10,000,000 bytes. So the store refuses a chunk
  whose text exceeds 8 MiB of UTF-8, and a node whose id or label, or a
  relation whose endpoint id, exceeds 64 KiB; an entity's id is its name.
- Filters on `triplet_source_id` and `ref_doc_id` use their indexes. Filters on
  other keys read the JSON with `json_value`, so the store refuses node
  properties above 1 MiB of JSON, the most `json_value` reads. Such a filter
  fails if a node it reads has an object or array under the filtered key, or
  an integer from 2^63 to 2^64 − 1; HawDB reads larger integers, and those
  below −2^63, as floats.
- In an autocommit `Database.execute`, which `structured_query` uses, HawDB
  currently charges `json_value` over a single-node scan 64 bytes of query
  memory per document byte, so with the default query memory such a query
  fails on documents above about 252 KiB. The store's own filters run in read
  snapshots and are not affected.
- Vector queries are not supported; see [Usage](#usage).

## Tests

From the repository root:

```bash
pip install ./integrations/llama-index-graph-stores-hawdb pytest
```

```bash
pytest integrations/llama-index-graph-stores-hawdb/tests
```

The tests use in-memory and file-backed databases and need no network access
or API keys.
