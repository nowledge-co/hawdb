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

"""What the RAGFlow v0.27.3 patch calls besides the store.

RAGFlow keeps a dataset's graph as one ``graph`` chunk in its document engine.
Its knowledge-graph view (``get_knowledge_graph`` in
``api/apps/services/dataset_api_service.py``) reads that chunk and shows the
256 nodes of highest PageRank and the 128 heaviest edges between them, and
document removal reads the chunk's ``source_id``. Once the graph lives in
HawDB, the patch keeps only that view in the chunk.
"""

from __future__ import annotations

import os
import re

import networkx as nx

from ragflow_hawdb.store import DEFAULT_BUSY_TIMEOUT, DEFAULT_MAX_PATHS, HawDBGraphStore

VIEW_NODES = 256
VIEW_EDGES = 128

# A graph attribute that marks a view, so that the chunk is not mistaken for a
# whole graph RAGFlow kept before HawDB held it.
VIEW_MARKER = "graph_store"
VIEW_MARKER_VALUE = "hawdb"

# RAGFlow tenant and dataset ids are UUID hex strings.
_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


def store_for(
    root: str | os.PathLike[str],
    tenant_id: str,
    kb_id: str,
    *,
    max_paths: int = DEFAULT_MAX_PATHS,
    busy_timeout: float = DEFAULT_BUSY_TIMEOUT,
) -> HawDBGraphStore:
    """The store of one RAGFlow dataset: a database at ``root/tenant_id/kb_id``."""
    for value, what in ((tenant_id, "tenant_id"), (kb_id, "kb_id")):
        if not isinstance(value, str) or not _ID.fullmatch(value):
            raise ValueError(
                f"{what} must be a RAGFlow id of letters, digits, '_' and '-'"
            )
    path = os.path.join(os.fspath(root), tenant_id, kb_id)
    return HawDBGraphStore(path, max_paths=max_paths, busy_timeout=busy_timeout)


def view_graph(graph: nx.Graph) -> nx.Graph:
    """The part of ``graph`` RAGFlow's knowledge-graph view shows.

    It selects what the view selects from a whole graph, in the same order:
    the ``VIEW_NODES`` nodes of highest ``pagerank``, and of the edges between
    two different ones, the ``VIEW_EDGES`` of highest ``weight``. Graph
    attributes, including ``source_id``, are kept.
    """
    nodes = sorted(
        graph.nodes(data=True),
        key=lambda item: item[1].get("pagerank", 0),
        reverse=True,
    )[:VIEW_NODES]
    shown = {name for name, _ in nodes}
    edges = [
        (source, target, attrs)
        for source, target, attrs in graph.edges(data=True)
        if source != target and source in shown and target in shown
    ]
    edges = sorted(edges, key=lambda item: item[2].get("weight", 0), reverse=True)[
        :VIEW_EDGES
    ]
    view = nx.Graph()
    view.graph.update(graph.graph)
    view.graph[VIEW_MARKER] = VIEW_MARKER_VALUE
    view.add_nodes_from(nodes)
    view.add_edges_from(edges)
    return view


def is_view_graph(graph: nx.Graph) -> bool:
    """Whether ``graph`` is a view ``view_graph`` made, not a whole graph."""
    return graph.graph.get(VIEW_MARKER) == VIEW_MARKER_VALUE
