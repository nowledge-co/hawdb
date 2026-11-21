# RAGFlow graph store for HawDB

`ragflow-hawdb` keeps the knowledge graph that RAGFlow's GraphRAG builds for a
dataset in an embedded HawDB database, one database per dataset. It has two
parts: `HawDBGraphStore`, a library that writes and reads RAGFlow's NetworkX
graphs with parameterized Cypher, and a patch for RAGFlow v0.27.3 that calls it
from `rag/graphrag` and from the document and dataset services.

The package is not published on PyPI. Install it from a checkout of this
repository; pip takes `hawdb` and `networkx` from PyPI:

```bash
pip install ./integrations/ragflow-hawdb
```

It needs Python 3.11 or later, `hawdb` 0.36.0 or later and NetworkX 3. RAGFlow
v0.27.3 itself runs on Python 3.13 and locks NetworkX 3.6.1, which CI tests.

HawDB's on-disk format carries no compatibility promise until HawDB declares
one, so a database may need to be recreated after an upgrade; see
[Moving in and out](#moving-in-and-out) for how RAGFlow rebuilds it.

## RAGFlow

The patch is made against RAGFlow v0.27.3, commit
`8090ffd8ba6098813d7f7ec2c4b8774f36c8423a`, and the files it changes are the
same on the v0.27.x branch at commit `27847b3b9424d79140486e2142284319f4f71247`.
RAGFlow 1.0 replaces the Python server with one written in Go that has no
`rag/graphrag` package, so the patch is for the 0.27 line. From the root of a
RAGFlow checkout:

```bash
git apply /path/to/hawdb/integrations/ragflow-hawdb/patches/ragflow-v0.27.3.patch
```

Install `ragflow-hawdb` into RAGFlow's Python environment and add a section to
`conf/service_conf.yaml`:

```yaml
hawdb_graph:
  path: /ragflow/hawdb_graph
  max_paths_per_entity: 1000
  busy_timeout: 60
```

Each dataset then keeps its graph at `<path>/<tenant id>/<dataset id>`.
`max_paths_per_entity` bounds the paths stored with each entity, and
`busy_timeout` is how many seconds a call waits for another process to close a
database. Every RAGFlow process that builds, retrieves or deletes graphs, the
API server and each task executor, must see the same directory. In RAGFlow's
Docker setup, `conf/service_conf.yaml` is rendered from
`docker/service_conf.yaml.template` when the container starts, so add the
section there and mount a volume at the path. Without `hawdb_graph.path`, the
patched RAGFlow keeps the graph in its document engine as before.

The patch changes five files:

- `rag/graphrag/hawdb_store.py` (new) reads `hawdb_graph` and opens a
  dataset's store.
- `rag/graphrag/general/index.py` merges each document's extracted subgraph
  into the store in one transaction, and writes the change of an entity
  resolution to it.
- `rag/graphrag/utils.py` reads the graph from the store, imports a graph the
  document engine holds on first use, and takes each entity's n-hop paths from
  the store when it writes the entity and relation chunks.
- `api/db/services/document_service.py` removes a deleted document from the
  store.
- `api/apps/services/dataset_api_service.py` drops the store of a deleted
  dataset, and clears it when the knowledge graph or the graph index is
  deleted.

Chunk text, embeddings, extraction, the entity and relation chunks, community
reports and retrieval stay as they are. RAGFlow's `KGSearch` still reads
entity and relation chunks from the document engine, including the
`n_hop_with_weight` paths of each entity, which the store now computes when the
chunks are written. The `graph` chunk, which the knowledge-graph view and
document removal read, holds what the view shows: the 256 entities of highest
PageRank and the 128 heaviest relations between them, marked so that RAGFlow
does not mistake it for a whole graph.

## How a graph is built

For each document, RAGFlow extracts a subgraph and keeps it as a `subgraph`
chunk, as before. `merge_subgraph` then merges it into the store with the
rules of RAGFlow's `graph_merge`, in one transaction: a new entity or relation
is added as it is; an entity already there gets the new description after
`<SEP>` and the new `source_id` entries; a relation also adds the new weight
and appends the new keywords. RAGFlow then writes the chunks of every entity
and relation the merge touched, the new `graph` chunk, and only then deletes the
old `graph` chunk.

The `graph` chunk's `source_id` is the record that a document's chunks were
written. If RAGFlow fails after the store committed a merge, for instance when
an embedding call fails, the retry finds the document in the store but in no
`graph` chunk, and writes the chunks of that document's entities and
relations; any later merge does the same for every such document. A document
that the store and a `graph` chunk both name is skipped.

PageRank, entity resolution and Leiden communities still run in NetworkX over
`get_graph()`. Entity resolution writes its result back with `apply_change`
before RAGFlow writes the chunks.

## Using the store directly

```python
import networkx as nx
from ragflow_hawdb import HawDBGraphStore

store = HawDBGraphStore("./graphs/tenant/dataset")

subgraph = nx.Graph()
subgraph.add_node(
    "ALICE",
    entity_type="PERSON",
    description="Alice founded Acme.",
    source_id=["doc-1"],
)
subgraph.add_node(
    "ACME",
    entity_type="ORGANIZATION",
    description="Acme makes rockets.",
    source_id=["doc-1"],
)
subgraph.add_edge(
    "ALICE",
    "ACME",
    description="Alice founded Acme.",
    keywords=["founder"],
    weight=2.0,
    source_id=["doc-1"],
)
subgraph.graph["source_id"] = ["doc-1"]

change = store.merge_subgraph(subgraph)
paths = store.n_hop_paths(["ALICE"])
print(paths)  # {'ALICE': [{'path': ['ALICE', 'ACME'], 'weights': [2.0]}]}
graph = store.get_graph()  # a networkx.Graph, as RAGFlow's get_graph returns it
print(graph.nodes["ACME"]["source_id"])
```

| Method | What it does |
| --- | --- |
| `merge_subgraph(subgraph)` | Merges one extracted subgraph in one transaction and returns RAGFlow's `GraphChange`; a subgraph whose documents the store holds is skipped. |
| `apply_change(graph, change)` | Writes a change made to the whole graph, such as entity resolution. |
| `import_graph(graph)` | Writes a whole graph into a store that holds no document. |
| `n_hop_paths(names, n_hop=2, max_paths=None)` | The paths RAGFlow's `n_neighbor` walks, with a bounded Cypher traversal. |
| `get_graph()` | The whole graph, or None. |
| `document_ids()`, `contains_document(doc_id)` | The documents the graph holds, in merge order. |
| `document_change(doc_ids)` | Every entity and relation that names the documents. |
| `remove_document(doc_id)` | Removes one document in one transaction. |
| `clear()`, `drop()` | Deletes the graph, or the whole database. |

`ragflow_hawdb.ragflow` holds what the patch calls besides the store:
`store_for(root, tenant_id, kb_id)`, which checks the ids and opens the store
of one dataset, and `view_graph(graph)`.

## Data model

```text
(:Entity {name, seq, entity_type, description, source_id, attributes, stored_bytes})
  -[:RELATION {seq, weight, description, keywords, source_id, attributes, stored_bytes}]->(:Entity)
(:Document {doc_id, seq})
(:RAGFlowGraph {key, version, next_seq, written, clearing})
```

`name` is the NetworkX node, which RAGFlow keys by entity name, and is unique.
A graph edge is undirected, so the store keeps one relationship per pair of
entities, directed from the name that sorts first, as RAGFlow's `get_from_to`
orders the pair. `source_id` lists the documents of each entity and relation.
`attributes` holds every other attribute as one JSON object, so a graph reads
back with the attributes it was written with, except `rank` and `pagerank`:
RAGFlow derives them from the whole graph, so the store keeps neither and
`get_graph` sets `rank` to the degree as `graph_merge` does. `Document` nodes
record the graph's documents in merge order. `seq` keeps the order in which
entities, relations and documents were added. `RAGFlowGraph` records the
layout version and the store's own bookkeeping.

Every statement is parameterized: names, ids and values are bound as
parameters, and statement text comes only from constants in
`ragflow_hawdb/store.py`.

## Processes and threads

A store on a path opens the database for each call, on the calling thread, and
closes it before returning, so RAGFlow's thread pool and its processes can
share a database. Calls on one path in a process take turns. Across processes,
a process that has run a statement on a HawDB database holds it until it closes
it, so a call that finds the database held retries until `busy_timeout`, then
raises `StoreBusyError`, a `TimeoutError`.

A store can also wrap an open `hawdb.Database`, as in the tests. It then runs
only on the thread that opened the database and raises `WrongThreadError`
elsewhere.

Each open loads the graph and replays the WAL written since the last
checkpoint, and HawDB checkpoints by itself only when the WAL nears 717 MiB. A
store on a path therefore runs `CHECKPOINT` once it has written about 16 MiB
since the last one, and after `clear`. On an Apple M5 Pro, a checkpoint took
1.3 to 1.5 s at 20,000 entities and 40,000 relations and 3.3 s at 60,000 and
120,000. HawDB keeps the last two checkpoints: counting hard-linked files
once, a database of 20,000 entities and 40,000 relations, whose import wrote
19 MiB of WAL, took 100 MiB on disk between checkpoints and up to 152 MiB
right after one.

## Moving in and out

The first time a process uses a dataset's store, a store without documents
imports the graph the document engine holds, if there is one, so an existing
dataset moves into HawDB on first use without re-extraction.

The `subgraph` chunks stay in the document engine as extraction wrote them. If
`hawdb_graph.path` is removed again, RAGFlow finds a `graph` chunk that holds a
view and rebuilds the graph from the `subgraph` chunks, as it does when its
`graph` chunk is gone. Likewise, when a RAGFlow process first uses a dataset
whose database is missing or empty, it rebuilds the graph that way and imports
it. A rebuilt graph is not the merged one: the rebuild composes the
subgraphs, so an entity keeps the description of the last subgraph that named
it, and entity resolution is lost until it runs again.

## Limits

- HawDB refuses an intermediate row of more than 16 MiB, and a two-hop
  traversal holds three entities and two relations in one, so the store
  refuses an entity that would store more than 4 MiB (`MAX_ENTITY_BYTES`) and
  a relation above 1 MiB (`MAX_RELATION_BYTES`), counting UTF-8 bytes.
- HawDB refuses a transaction whose WAL record exceeds 16 MiB or that takes
  more than 100,000 version entries. A merge, a document removal, or a single
  entity or relation edit that needs more fails without writing anything.
  `apply_change`, which writes entity resolution, `import_graph` and `clear`
  split a larger edit over several transactions: calling `apply_change` or
  `import_graph` again with the same arguments completes it, and until a
  `clear` finishes, every other call raises `StoreFormatError`.
- An entity or relation must name at least one document: the store refuses an
  empty `source_id`, and `import_graph` skips such entities and relations and
  those without a description, as RAGFlow's `tidy_graph` purges them.
- `n_hop_paths` returns at most `max_paths` paths per entity, 1,000 by
  default, the heaviest first and then by name, and walks at most two hops.
  RAGFlow's walk keeps every path, in adjacency order.
- Statements after the first write in a transaction, and relationship scans,
  take time that grows with the graph. On an Apple M5 Pro with `hawdb` 0.36.0:

  | Call | 20,000 entities, 40,000 relations | 60,000 entities, 120,000 relations |
  | --- | --- | --- |
  | `import_graph`, with its checkpoint | 6.3 s | 22 to 25 s |
  | `get_graph` | 1.1 s | 4.0 s |
  | `merge_subgraph` of 50 entities and about 78 relations | 0.75 to 0.86 s | 2.3 to 2.8 s |
  | `n_hop_paths` of 50 entities | 0.5 s | 1.6 to 1.8 s |
  | `contains_document` | 0.44 s | 1.3 s |
  | `apply_change` of a resolution that merges 200 pairs of entities | 6.2 s | 18.7 s |
  | `remove_document` | 1.8 s | 7.2 to 8.4 s |
  | `clear` | 2.1 s | 6.2 s |

  Opening the database and running its first statement took 0.33 s and 1.0 s
  of these. In `remove_document` at the larger size, deleting 120 entities took
  4.1 s, because HawDB 0.36.0 checks every relationship of the graph for each
  node it deletes. HawDB 0.36.0 also evaluates a lower bound on a relationship
  property, such as `r.seq >= $low`, in time that grows with the graph, so the
  store writes `NOT r.seq < $low` instead.

## Differences from RAGFlow's own graph

- RAGFlow keys an edge by either orientation; the store keeps the pair in
  `get_from_to` order, and `GraphChange` names edges that way.
- `n_hop_paths` keeps the heaviest `max_paths` paths of an entity, where
  RAGFlow keeps every path.
- Removing a document deletes a relation that names only that document, even
  when both of its entities stay; RAGFlow's rebuild keeps such a relation.
  Descriptions and weights keep what the removed document contributed, as they
  do in the graph RAGFlow rebuilds.
- RAGFlow's `set_graph` rewrites the `subgraph` chunk of every document from
  the merged graph on each merge; the patch leaves them as extraction wrote
  them, so a graph rebuilt from them lacks merged descriptions and entity
  resolution.
- Repeated writes of one statement go through `UNWIND` over a list of rows, in
  statements of up to 2,000 rows inside the call's transaction, rather than
  `executemany`, which runs the statement once per row. On the machine above,
  with 20,000 entities and 40,000 relations, writing 50 entities and 78
  relations in one transaction took 0.15 to 0.22 s as two `UNWIND` statements
  and 15.7 to 20.6 s through `executemany`.

## Tests

From the repository root:

```bash
pip install ./integrations/ragflow-hawdb pytest
```

```bash
pytest integrations/ragflow-hawdb/tests
```

The tests use in-memory and file-backed databases, start a second Python
process to hold a database, and need no network access, RAGFlow or document
engine.

`patches/verify_patch.py` is a manual check of the patch against a RAGFlow
checkout. It loads the patched `rag/graphrag` modules with in-memory stand-ins
for the document engine, Redis and the models, builds graphs as RAGFlow's
GraphRAG task does, including failed and retried merges, entity resolution, a
dataset moving into HawDB and HawDB turned off again, and compares random
merges with RAGFlow's own `graph_merge` and `n_neighbor`. From the repository
root, with a RAGFlow checkout next to it:

```bash
git clone https://github.com/infiniflow/ragflow.git ../ragflow
```

```bash
git -C ../ragflow checkout 8090ffd8ba6098813d7f7ec2c4b8774f36c8423a
```

```bash
git -C ../ragflow apply "$PWD/integrations/ragflow-hawdb/patches/ragflow-v0.27.3.patch"
```

```bash
python integrations/ragflow-hawdb/patches/verify_patch.py ../ragflow
```

It needs Python 3.13, with `hawdb` and `networkx` installed, and takes about
30 seconds. It is not run in CI.
