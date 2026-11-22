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

"""Run the patched RAGFlow GraphRAG code over real HawDB stores.

This is a manual check of ``ragflow-v0.27.3.patch``, not part of the test
suite, because it needs a RAGFlow checkout. Apply the patch to RAGFlow v0.27.3
and pass the checkout::

    git clone https://github.com/infiniflow/ragflow.git
    git -C ragflow checkout 8090ffd8ba6098813d7f7ec2c4b8774f36c8423a
    git -C ragflow apply /path/to/ragflow-v0.27.3.patch
    python /path/to/verify_patch.py ragflow

It needs Python 3.13, as RAGFlow v0.27.3 does, with hawdb and networkx
installed; SciPy is optional. It imports ``ragflow_hawdb`` from the directory
above this one and loads three patched files from the checkout:
``rag/graphrag/hawdb_store.py``, ``rag/graphrag/utils.py`` and
``rag/graphrag/general/index.py``. What they import from the rest of RAGFlow
(the document engine, Redis, the configuration, the database services, the
extractors and the models) is replaced by in-memory stand-ins, so no other
RAGFlow code runs. The other two files the patch changes are only compiled.

It then builds graphs as RAGFlow's GraphRAG task does and checks that:

- a dataset keeps its graph in HawDB, merges match RAGFlow's ``graph_merge``,
  the entity and relation chunks match the store, the graph chunk holds a
  view, entity chunks carry the store's n-hop paths, and the subgraph
  checkpoints stay;
- after a merge whose chunks were not written, because the embedding, the
  entity and relation chunks or the graph chunk failed, the retry writes them;
- a document already merged is skipped;
- entity resolution reaches the store and the chunks;
- a dataset built without HawDB moves into it on first use;
- an import that stops after committing some of its documents holds no graph:
  the next task starts it over, and once the dataset has no graph left in the
  document engine, the next task discards it;
- when another process merges a document into a new dataset's store after the
  check that it holds no graph, the refused import is ignored and the
  document stays;
- with HawDB turned off again, RAGFlow rebuilds the graph from the subgraph
  chunks instead of reading the view as the whole graph;
- on random subgraphs, the store merges as RAGFlow's own ``graph_merge`` does
  and returns the paths RAGFlow's ``n_neighbor`` walks, also after random
  documents are removed by the store's rule (``--seeds`` sets how many
  graphs).
"""

import argparse
import asyncio
import concurrent.futures
import contextlib
import hashlib
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import types
import uuid

import networkx as nx

PACKAGE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The files ragflow-v0.27.3.patch changes or adds.
PATCHED = (
    "rag/graphrag/hawdb_store.py",
    "rag/graphrag/utils.py",
    "rag/graphrag/general/index.py",
    "api/db/services/document_service.py",
    "api/apps/services/dataset_api_service.py",
)

TENANT = "tenant1"

# The configuration get_base_config reads, and the messages of the GraphRAG task.
CONFIG = {}
LOG = []


def check(condition, message):
    # Unlike assert, this also checks under python -O.
    if not condition:
        raise AssertionError(message)


def module(name, **attrs):
    mod = sys.modules.get(name)
    if mod is None:
        mod = types.ModuleType(name)
        mod.__path__ = []
        sys.modules[name] = mod
        parent, _, child = name.rpartition(".")
        if parent:
            setattr(module(parent), child, mod)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


async def thread_pool_exec(func, *args, **kwargs):
    # As RAGFlow's: a new executor with one thread for each call.
    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


def timeout(*_args, **_kwargs):
    return lambda func: func


class Semaphore:
    def __init__(self, _count):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


class Redis:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, _ttl=None):
        self.data[key] = value
        return True

    def mget(self, keys):
        return [self.data.get(key) for key in keys]


class Lock:
    def __init__(self, *_args, **_kwargs):
        pass

    def acquire(self):
        return True

    def release(self):
        return True


class XXH64:
    # Only the chunk ids RAGFlow derives from it depend on the hash.
    def __init__(self, data=b""):
        self._hash = hashlib.sha1(data)

    def update(self, data):
        self._hash.update(data)

    def hexdigest(self):
        return self._hash.hexdigest()[:16]


class Result:
    def __init__(self, hits, fields):
        self.ids = [hit["id"] for hit in hits]
        self.field = {
            hit["id"]: {field: hit.get(field) for field in fields} for hit in hits
        }
        self.total = len(hits)


class DocStore:
    """The document engine: chunks by id, searched as RAGFlow's graph code does."""

    def __init__(self):
        self.chunks = {}
        # A predicate on a batch of chunks, and how many inserts it fails.
        # insert_chunks_bounded tries each batch three times.
        self.fail_insert = None
        self.insert_failures = 0

    @staticmethod
    def matches(chunk, condition):
        for key, want in condition.items():
            if key == "must_not":
                if chunk.get(want["exists"]):
                    return False
                continue
            wants = want if isinstance(want, list) else [want]
            have = chunk.get(key)
            if isinstance(have, list):
                if not any(item in have for item in wants):
                    return False
            elif have not in wants:
                return False
        return True

    def select(self, condition):
        return [
            chunk for chunk in self.chunks.values() if self.matches(chunk, condition)
        ]

    def insert(self, batch, _index, _kb_id):
        if self.insert_failures and self.fail_insert(batch):
            self.insert_failures -= 1
            raise RuntimeError("injected insert failure")
        for chunk in batch:
            self.chunks[chunk["id"]] = json.loads(json.dumps(chunk))
        return []

    def delete(self, condition, _index, kb_id):
        doomed = [chunk["id"] for chunk in self.select({**condition, "kb_id": kb_id})]
        for chunk_id in doomed:
            del self.chunks[chunk_id]
        return len(doomed)

    def search(
        self,
        _fields,
        _highlight,
        condition,
        _match,
        _order,
        offset,
        limit,
        _index,
        kb_ids,
    ):
        hits = self.select({**condition, "kb_id": kb_ids})
        return hits[offset : offset + limit]

    def get_fields(self, res, fields):
        return {hit["id"]: {field: hit.get(field) for field in fields} for hit in res}


class Retriever:
    def __init__(self, store):
        self.store = store

    async def search(self, req, _index, kb_ids):
        condition = {
            key: value
            for key, value in req.items()
            if key not in ("fields", "size", "page")
        }
        hits = self.store.select({**condition, "kb_id": kb_ids})[
            : int(req.get("size", 30))
        ]
        return Result(hits, req.get("fields", []))


class Embedder:
    llm_name = "fake-embedding"

    def __init__(self):
        self.failures = 0

    def encode(self, texts):
        if self.failures:
            self.failures -= 1
            raise RuntimeError("injected embedding failure")
        return [[float(len(text)), 1.0] for text in texts], 0


class Resolution:
    """Stands in for RAGFlow's EntityResolution.

    It merges the second entity of each pair in ``PAIRS`` into the first.
    """

    PAIRS = ()

    def __init__(self, _llm):
        pass

    async def __call__(self, graph, _subgraph_nodes, **_kwargs):
        from rag.graphrag.utils import GRAPH_FIELD_SEP, GraphChange, get_from_to

        change = GraphChange()
        for keep, gone in self.PAIRS:
            node = graph.nodes[keep]
            node["description"] += GRAPH_FIELD_SEP + graph.nodes[gone]["description"]
            node["source_id"] = list(
                dict.fromkeys(node["source_id"] + graph.nodes[gone]["source_id"])
            )
            for neighbor in list(graph.neighbors(gone)):
                change.removed_edges.add(get_from_to(gone, neighbor))
                if neighbor == keep:
                    continue
                attrs = dict(graph.edges[gone, neighbor])
                if graph.has_edge(keep, neighbor):
                    graph.edges[keep, neighbor]["weight"] += attrs["weight"]
                else:
                    graph.add_edge(keep, neighbor, **attrs)
                change.added_updated_edges.add(get_from_to(keep, neighbor))
            graph.remove_node(gone)
            change.removed_nodes.add(gone)
            change.added_updated_nodes.add(keep)
        for name, rank in nx.pagerank(graph).items():
            graph.nodes[name]["pagerank"] = rank
        return types.SimpleNamespace(graph=graph, change=change)


DOCS = DocStore()
EMBED = Embedder()


async def _no_checkpoint(*_args, **_kwargs):
    return {}


def install_stand_ins():
    if importlib.util.find_spec("scipy") is None:
        # nx.pagerank needs SciPy; NetworkX's pure-Python PageRank stands in.
        from networkx.algorithms.link_analysis.pagerank_alg import _pagerank_python

        nx.pagerank = _pagerank_python
    if importlib.util.find_spec("xxhash") is None:
        module("xxhash", xxh64=XXH64)
    if importlib.util.find_spec("numpy") is None:
        module("numpy", array=list, ndarray=type("ndarray", (), {}))
    module(
        "common.config_utils",
        get_base_config=lambda key, default=None: CONFIG.get(key, default),
    )
    module(
        "common.misc_utils",
        env_flag=lambda _name, default=False: default,
        thread_pool_exec=thread_pool_exec,
        get_uuid=lambda: uuid.uuid4().hex,
    )
    module("common.connection_utils", timeout=timeout)
    module("common.asyncio_utils", LoopLocalSemaphore=Semaphore)
    module("common.settings", retriever=Retriever(DOCS), docStoreConn=DOCS)
    module("common.doc_store.doc_store_base", OrderByExpr=type("OrderByExpr", (), {}))
    module(
        "common.exceptions",
        TaskCanceledException=type("TaskCanceledException", (Exception,), {}),
    )
    module(
        "rag.nlp",
        rag_tokenizer=types.SimpleNamespace(
            tokenize=str.lower, fine_grained_tokenize=str
        ),
        search=types.SimpleNamespace(index_name=lambda tenant: f"ragflow_{tenant}"),
    )
    module("rag.utils.redis_conn", REDIS_CONN=Redis(), RedisDistributedLock=Lock)
    module("api.db.services.document_service", DocumentService=object)
    module("api.db.services.task_service", has_canceled=lambda _task_id: False)
    module(
        "rag.graphrag.checkpoints",
        COMMUNITY_CHECKPOINT="community",
        RESOLUTION_CHECKPOINT="resolution",
        cleanup_checkpoints=_no_checkpoint,
        load_checkpoints=_no_checkpoint,
        save_checkpoint=_no_checkpoint,
    )
    module(
        "rag.graphrag.general.community_reports_extractor",
        CommunityReportsExtractor=object,
    )
    module("rag.graphrag.general.extractor", Extractor=object)
    for package in ("general", "light", "ner"):
        module(f"rag.graphrag.{package}.graph_extractor", GraphExtractor=object)
    module(
        "rag.graphrag.phase_markers",
        PHASE_COMMUNITY="community",
        PHASE_RESOLUTION="resolution",
        clear_phase_markers=lambda _kb: None,
        has_phase_marker=lambda _kb, _phase: False,
        set_phase_marker=lambda _kb, _phase: None,
    )
    module("rag.graphrag.entity_resolution", EntityResolution=Resolution)


def load(ragflow, name, relative):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ragflow, relative))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    parent, _, child = name.rpartition(".")
    setattr(module(parent), child, mod)
    spec.loader.exec_module(mod)
    return mod


def use_hawdb(root):
    # root None turns the HawDB graph store off.
    if root is None:
        CONFIG.pop("hawdb_graph", None)
    else:
        CONFIG["hawdb_graph"] = {"path": root, "max_paths_per_entity": 50}


def subgraph(doc, entities, relations):
    # What RAGFlow's generate_subgraph returns for one document.
    graph = nx.Graph()
    for name in entities:
        graph.add_node(
            name,
            entity_name=name,
            entity_type="PERSON",
            description=f"{name} in {doc}",
            source_id=[doc],
        )
    for source, target, weight in relations:
        graph.add_edge(
            source,
            target,
            src_id=source,
            tgt_id=target,
            description=f"{source} and {target} in {doc}",
            keywords=[doc],
            weight=weight,
            source_id=[doc],
        )
    graph.graph["source_id"] = [doc]
    return graph


def write_checkpoint(kb, graph):
    # The subgraph chunk generate_subgraph writes before a merge.
    doc = graph.graph["source_id"][0]
    chunk_id = uuid.uuid4().hex
    DOCS.chunks[chunk_id] = {
        "id": chunk_id,
        "content_with_weight": json.dumps(
            nx.node_link_data(graph, edges="edges"), ensure_ascii=False
        ),
        "knowledge_graph_kwd": "subgraph",
        "kb_id": kb,
        "source_id": [doc],
        "available_int": 0,
        "removed_kwd": "N",
    }


def kind(kb, what):
    return DOCS.select({"kb_id": kb, "knowledge_graph_kwd": what})


def graph_chunk(kb):
    chunks = kind(kb, "graph")
    check(len(chunks) == 1, f"{len(chunks)} graph chunks")
    chunk = chunks[0]
    return chunk, nx.node_link_graph(
        json.loads(chunk["content_with_weight"]), edges="edges"
    )


@contextlib.contextmanager
def import_stopped_after(rows):
    # Makes an import commit a first transaction of at most rows entities,
    # relations and documents, and then fail before the next one.
    from ragflow_hawdb import store as store_module

    limit, run_steps = store_module._TRANSACTION_ROWS, store_module._run_steps
    calls = []

    def fail_second(tx, steps):
        calls.append(steps)
        if len(calls) == 2:
            raise RuntimeError("injected failure before an import's second transaction")
        run_steps(tx, steps)

    store_module._TRANSACTION_ROWS, store_module._run_steps = rows, fail_second
    try:
        yield
    finally:
        store_module._TRANSACTION_ROWS, store_module._run_steps = limit, run_steps


async def stop_import(utils, kb, graph):
    # Starts moving the dataset into HawDB, stopping after all but its last
    # document, and returns the documents the store then holds.
    import hawdb

    elements = graph.number_of_nodes() + graph.number_of_edges()
    with import_stopped_after(elements + len(graph.graph["source_id"]) - 1):
        try:
            await utils.hawdb_graph_store(TENANT, kb)
        except RuntimeError:
            pass
        else:
            check(False, "the import did not stop")
    path = os.path.join(CONFIG["hawdb_graph"]["path"], TENANT, kb)
    database = hawdb.open(path, read_only=True)
    try:
        rows = database.execute("MATCH (d:Document) RETURN d.doc_id AS doc").fetchall()
    finally:
        database.close()
        del database
    return sorted(row["doc"] for row in rows)


def log(msg=None, **_kwargs):
    LOG.append(msg or "")


async def build(index, kb, graphs):
    for graph in graphs:
        write_checkpoint(kb, graph)
    return await index.run_graphrag_for_kb(
        {"tenant_id": TENANT, "kb_id": kb, "id": f"task-{kb}"},
        [graph.graph["source_id"][0] for graph in graphs],
        "English",
        {"graphrag": {"merge_retry_attempts": 2, "retry_backoff_seconds": 0}},
        None,
        EMBED,
        log,
        with_resolution=False,
        with_community=False,
    )


def comparable(graph):
    nodes = {
        name: {
            key: value
            for key, value in attrs.items()
            if key not in ("rank", "pagerank")
        }
        for name, attrs in graph.nodes(data=True)
    }
    edges = {tuple(sorted(pair)): attrs for *pair, attrs in graph.edges(data=True)}
    return nodes, edges, graph.graph.get("source_id")


def check_chunks(kb, store):
    from ragflow_hawdb.ragflow import is_view_graph

    graph = store.get_graph()
    entities = {chunk["entity_kwd"] for chunk in kind(kb, "entity")}
    relations = {
        (chunk["from_entity_kwd"], chunk["to_entity_kwd"])
        for chunk in kind(kb, "relation")
    }
    check(
        entities == set(graph.nodes),
        f"entity chunks differ from the store: {sorted(entities ^ set(graph.nodes))}",
    )
    check(
        relations == {tuple(sorted(pair)) for pair in graph.edges},
        "relation chunks differ from the store",
    )
    chunk, view = graph_chunk(kb)
    check(is_view_graph(view), "the graph chunk is not a view")
    check(
        chunk["source_id"] == store.document_ids() == graph.graph["source_id"],
        "the graph chunk names other documents",
    )
    return graph


async def scenarios(root, utils, index):
    from ragflow_hawdb import HawDBGraphStore
    from ragflow_hawdb.ragflow import is_view_graph

    def store_of(kb):
        return HawDBGraphStore(os.path.join(root, TENANT, kb))

    # A dataset that keeps its graph in HawDB from the start.
    use_hawdb(root)
    kb = "kb-hawdb"
    first = subgraph(
        "doc1",
        ["ALICE", "BOB", "CAROL"],
        [("ALICE", "BOB", 2.0), ("BOB", "CAROL", 1.0)],
    )
    second = subgraph(
        "doc2", ["ALICE", "BOB", "DAVE"], [("ALICE", "BOB", 1.0), ("BOB", "DAVE", 3.0)]
    )
    result = await build(index, kb, [first, second])
    check(result["ok_docs"] == ["doc1", "doc2"], result)
    store = store_of(kb)
    graph = check_chunks(kb, store)
    expected = utils.graph_merge(first.copy(), second, utils.GraphChange())
    check(
        comparable(graph) == comparable(expected),
        "the store's merge differs from graph_merge",
    )
    check(len(kind(kb, "subgraph")) == 2, "the subgraph checkpoints were not kept")
    bob = [chunk for chunk in kind(kb, "entity") if chunk["entity_kwd"] == "BOB"]
    latest = json.loads(bob[-1]["n_hop_with_weight"])
    check(
        latest == store.n_hop_paths(["BOB"])["BOB"],
        "the entity chunk holds other n-hop paths than the store",
    )
    print(
        "fresh dataset: 2 merges match graph_merge, the chunks match the store, "
        "n-hop paths come from it"
    )

    # set_graph fails after the store committed the merge: at the embedding,
    # at the insert of the entity and relation chunks, or at the insert of the
    # graph chunk. The retry writes the chunks of the document the store holds.
    def is_graph(batch):
        return any(chunk["knowledge_graph_kwd"] == "graph" for chunk in batch)

    failures = (
        ("doc3", "embedding", None),
        (
            "doc4",
            "insert of the entity and relation chunks",
            lambda batch: not is_graph(batch),
        ),
        ("doc5", "insert of the graph chunk", is_graph),
    )
    for doc, failure, fail_insert in failures:
        names = [f"{doc.upper()} ONE", f"{doc.upper()} TWO"]
        graph = subgraph(
            doc, [*names, "BOB"], [(names[0], names[1], 1.0), (names[1], "BOB", 1.0)]
        )
        if fail_insert is None:
            EMBED.failures = 1
        else:
            DOCS.fail_insert = fail_insert
            DOCS.insert_failures = 3
        LOG.clear()
        result = await build(index, kb, [graph])
        check(result["ok_docs"] == [doc], result)
        check(
            any("failed attempt 1/2" in msg for msg in LOG),
            f"{failure}: no attempt failed",
        )
        check(
            any("an earlier attempt merged" in msg for msg in LOG),
            f"{failure}: the retry did not write the chunks",
        )
        check_chunks(kb, store)
        print(f"retry after the {failure} failed: the chunks of {doc} were written")

    # A document whose merge and chunks were written is skipped.
    LOG.clear()
    await build(index, kb, [subgraph("doc5", ["DOC5 ONE"], [])])
    check(
        any("already merged" in msg for msg in LOG),
        "a merged document was merged again",
    )
    print("a merged document is skipped")

    # Entity resolution writes its change to the store before the chunks.
    Resolution.PAIRS = (("DOC3 ONE", "DOC3 TWO"),)
    graph = store.get_graph()
    for name, rank in nx.pagerank(graph).items():
        graph.nodes[name]["pagerank"] = rank
    await index.resolve_entities(
        graph, set(graph.nodes), TENANT, kb, None, None, EMBED, log
    )
    resolved = check_chunks(kb, store)
    check(
        "DOC3 TWO" not in resolved and resolved.has_edge("DOC3 ONE", "BOB"),
        "the resolution did not reach the store",
    )
    print(
        "entity resolution: the store and the chunks follow the merge of two entities"
    )

    # A dataset built in the document engine moves into HawDB on first use.
    use_hawdb(None)
    legacy = "kb-legacy"
    first = subgraph("a", ["ALICE", "BOB"], [("ALICE", "BOB", 1.0)])
    second = subgraph("b", ["BOB", "CAROL"], [("BOB", "CAROL", 2.0)])
    await build(index, legacy, [first, second])
    _, before = graph_chunk(legacy)
    check(
        not is_view_graph(before) and len(kind(legacy, "subgraph")) == 2,
        "the document engine holds no whole graph",
    )
    use_hawdb(root)
    third = subgraph("c", ["CAROL", "DAVE"], [("CAROL", "DAVE", 1.0)])
    await build(index, legacy, [third])
    moved = check_chunks(legacy, store_of(legacy))
    expected = utils.graph_merge(before.copy(), third, utils.GraphChange())
    check(
        comparable(moved) == comparable(expected),
        "the imported and merged graph differs from graph_merge",
    )
    print("dataset built without HawDB: imported into HawDB, then merged")

    # An import that stops after committing some of its documents holds no
    # graph, so the next task starts it over.
    use_hawdb(None)
    partial = "kb-partial"
    first = subgraph("p1", ["ALICE", "BOB"], [("ALICE", "BOB", 1.0)])
    second = subgraph("p2", ["BOB", "CAROL"], [("BOB", "CAROL", 2.0)])
    await build(index, partial, [first, second])
    _, before = graph_chunk(partial)
    use_hawdb(root)
    held = await stop_import(utils, partial, before)
    check(
        held == ["p1"] and not store_of(partial).holds_graph(),
        f"the stopped import holds {held} and counts as a graph",
    )
    third = subgraph("p3", ["CAROL", "DAVE"], [("CAROL", "DAVE", 1.0)])
    await build(index, partial, [third])
    resumed = check_chunks(partial, store_of(partial))
    expected = utils.graph_merge(before.copy(), third, utils.GraphChange())
    check(
        comparable(resumed) == comparable(expected),
        "the import started over and merged differs from graph_merge",
    )
    print("an import stopped after some documents: started over by the next task")

    # Once the dataset has no graph left in the document engine, as after
    # RAGFlow removed its last document, the next task discards such an import.
    use_hawdb(None)
    gone = "kb-gone"
    first = subgraph("g1", ["ALICE", "BOB"], [("ALICE", "BOB", 1.0)])
    second = subgraph("g2", ["BOB", "CAROL"], [("BOB", "CAROL", 2.0)])
    await build(index, gone, [first, second])
    _, before = graph_chunk(gone)
    use_hawdb(root)
    held = await stop_import(utils, gone, before)
    check(held == ["g1"], f"the stopped import holds {held}")
    DOCS.delete(
        {"knowledge_graph_kwd": ["graph", "subgraph", "entity", "relation"]},
        None,
        gone,
    )
    store = await utils.hawdb_graph_store(TENANT, gone)
    check(
        not store.holds_graph() and store.get_graph() is None,
        "the stopped import was not discarded",
    )
    print("an import stopped after some documents: discarded once the graph is gone")

    # Another process merges a document into a new dataset's store after this
    # process found that it holds no graph: the import of the empty graph is
    # refused, and the store keeps the document.
    race = "kb-race"
    holds_graph = HawDBGraphStore.holds_graph
    raced = []

    def merged_meanwhile(self):
        held = holds_graph(self)
        if not raced and self.path and os.path.basename(self.path) == race:
            raced.append(held)
            store_of(race).merge_subgraph(subgraph("r1", ["ALICE"], []))
        return held

    HawDBGraphStore.holds_graph = merged_meanwhile
    try:
        store = await utils.hawdb_graph_store(TENANT, race)
    finally:
        HawDBGraphStore.holds_graph = holds_graph
    check(
        raced == [False] and store.document_ids() == ["r1"],
        f"the import raced with a merge: {raced}, {store.document_ids()}",
    )
    print("a merge by another process before the first import: the import is skipped")

    # With HawDB turned off, get_graph rebuilds the graph from the subgraph
    # chunks rather than read the view in the graph chunk as the whole graph.
    use_hawdb(None)
    rebuilt = await utils.get_graph(TENANT, legacy)
    check(
        set(rebuilt.nodes) == set(moved.nodes),
        f"the rebuilt graph differs: {sorted(set(rebuilt.nodes) ^ set(moved.nodes))}",
    )
    print("HawDB turned off: get_graph rebuilds the graph from the subgraph chunks")


def random_subgraph(rng, utils, doc, names):
    # Like an extractor's subgraph: a few entities, relations between them, and
    # sometimes a relation of an entity with itself.
    graph = nx.Graph()
    chosen = rng.sample(names, rng.randint(1, min(8, len(names))))
    for name in chosen:
        graph.add_node(
            name,
            entity_name=name,
            entity_type=rng.choice(["PERSON", "ORGANIZATION", "GEO"]),
            description=f"{name} in {doc} #{rng.randint(0, 99)}",
            source_id=[doc],
        )
    for _ in range(rng.randint(0, 12)):
        source, target = rng.choice(chosen), rng.choice(chosen)
        if source == target and rng.random() < 0.7:
            continue
        low, high = utils.get_from_to(source, target)
        graph.add_edge(
            source,
            target,
            src_id=low,
            tgt_id=high,
            description=f"{source} and {target} in {doc}",
            keywords=[f"k{rng.randint(0, 5)}"],
            weight=rng.choice([0.5, 1.0, 2.0, 3.25]),
            source_id=[doc],
        )
    graph.graph["source_id"] = [doc]
    return graph


def without_document(graph, doc):
    # The store's rule for removing a document: the document leaves every
    # source_id, and an entity or relation left without one is deleted, with
    # the relations of a deleted entity.
    graph = graph.copy()
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


def sorted_paths(paths):
    return sorted((tuple(path["path"]), tuple(path["weights"])) for path in paths)


def check_parity(store, expected, utils, where):
    graph = store.get_graph()
    if expected.number_of_nodes() == 0:
        check(
            graph is None or graph.number_of_nodes() == 0,
            f"{where}: the store holds entities",
        )
        return
    check(
        graph.graph["source_id"] == expected.graph["source_id"],
        f"{where}: other documents",
    )
    check(
        list(graph.nodes) == list(expected.nodes),
        f"{where}: other entities or another order",
    )
    for name, attrs in expected.nodes(data=True):
        want = {
            key: value
            for key, value in attrs.items()
            if key not in ("rank", "pagerank")
        }
        have = {key: value for key, value in graph.nodes[name].items() if key != "rank"}
        check(have == want, f"{where}: entity {name!r} is {have}, not {want}")
        check(
            graph.nodes[name]["rank"] == expected.degree(name),
            f"{where}: the rank of {name!r} is not its degree",
        )
    edges = {utils.get_from_to(*pair) for pair in graph.edges}
    check(
        edges == {utils.get_from_to(*pair) for pair in expected.edges},
        f"{where}: other relations",
    )
    for source, target, attrs in expected.edges(data=True):
        check(
            graph.edges[source, target] == attrs,
            f"{where}: relation {(source, target)!r} differs",
        )
    for n_hop in (1, 2):
        found = store.n_hop_paths(list(expected.nodes), n_hop=n_hop, max_paths=10_000)
        for name in expected.nodes:
            want = sorted_paths(utils.n_neighbor(expected, name, n_hop))
            check(
                sorted_paths(found[name]) == want,
                f"{where}: other {n_hop}-hop paths from {name!r}",
            )


def random_parity(root, utils, seeds):
    # Merges random subgraphs and removes random documents, and compares the
    # store with what RAGFlow's own graph_merge and n_neighbor make of them.
    import copy
    import random

    import hawdb
    from ragflow_hawdb import GraphChange, HawDBGraphStore

    merges = removals = 0
    for seed in range(seeds):
        rng = random.Random(seed)
        names = [f"E{i}" for i in range(rng.randint(3, 14))] + [
            "QUOTE'\"`}) DETACH DELETE n //",
            "名前",
        ]
        database = hawdb.open() if seed % 2 else None
        if database is None:
            store = HawDBGraphStore(os.path.join(root, "random", str(seed)))
        else:
            store = HawDBGraphStore(database)
        expected = None
        for number in range(rng.randint(1, 6)):
            doc = f"doc{number}"
            sub = random_subgraph(rng, utils, doc, names)
            change = store.merge_subgraph(copy.deepcopy(sub))
            want = utils.GraphChange()
            if expected is None:
                expected = sub
                want.added_updated_nodes = set(sub.nodes)
                want.added_updated_edges = {
                    utils.get_from_to(*pair) for pair in sub.edges
                }
            else:
                utils.tidy_graph(expected, None)
                expected = utils.graph_merge(expected, copy.deepcopy(sub), want)
            check(
                (change.added_updated_nodes, change.added_updated_edges)
                == (want.added_updated_nodes, want.added_updated_edges),
                f"seed {seed}: the merge of {doc} reports another change "
                "than graph_merge",
            )
            check(
                store.merge_subgraph(copy.deepcopy(sub)) == GraphChange(),
                f"seed {seed}: {doc} merged twice",
            )
            check_parity(store, expected, utils, f"seed {seed}, after {doc}")
            merges += 1
        for doc in rng.sample(
            expected.graph["source_id"],
            rng.randint(0, len(expected.graph["source_id"])),
        ):
            store.remove_document(doc)
            expected = without_document(expected, doc)
            check(
                not store.contains_document(doc), f"seed {seed}: {doc} was not removed"
            )
            check_parity(store, expected, utils, f"seed {seed}, without {doc}")
            removals += 1
        if database is not None:
            database.close()
            del store, database
    print(
        f"random graphs: {merges} merges and {removals} removals match "
        "graph_merge and n_neighbor"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "ragflow", help="a RAGFlow v0.27.3 checkout with ragflow-v0.27.3.patch applied"
    )
    parser.add_argument(
        "--seeds", type=int, default=40, help="random graphs to compare (default 40)"
    )
    args = parser.parse_args()
    ragflow = os.path.abspath(args.ragflow)
    missing = [
        path for path in PATCHED if not os.path.isfile(os.path.join(ragflow, path))
    ]
    if missing:
        parser.error(
            f"{ragflow} is not a patched RAGFlow checkout; "
            f"it lacks {', '.join(missing)}"
        )
    for relative in PATCHED:
        path = os.path.join(ragflow, relative)
        with open(path, encoding="utf-8") as file:
            compile(file.read(), path, "exec")
    print(f"compiled the {len(PATCHED)} files the patch changes")

    # Leave no __pycache__ in the checkout.
    sys.dont_write_bytecode = True
    sys.path.insert(0, PACKAGE)
    install_stand_ins()
    load(ragflow, "rag.graphrag.hawdb_store", "rag/graphrag/hawdb_store.py")
    utils = load(ragflow, "rag.graphrag.utils", "rag/graphrag/utils.py")
    index = load(ragflow, "rag.graphrag.general.index", "rag/graphrag/general/index.py")
    root = tempfile.mkdtemp(prefix="ragflow-hawdb-verify-")
    try:
        asyncio.run(scenarios(root, utils, index))
        random_parity(root, utils, args.seeds)
    finally:
        shutil.rmtree(root, ignore_errors=True)
    print("verify_patch: every check passed")


if __name__ == "__main__":
    main()
