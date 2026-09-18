# Knowledge Retrieval Pipeline Specification

## Scope

This specification defines the embedded knowledge-retrieval execution contract
used to combine the rebuildable search projection with canonical graph state.
The search projection remains derived state outside the canonical WAL. The
canonical graph snapshot remains authoritative for identity, scope, entity
properties, and graph context.

## Required stage order

Every successful retrieval executes these logical stages exactly once and in
this order:

1. `search_candidate`
2. `metadata_filter`
3. `authorized_graph_expand`
4. `rerank`
5. `top_k`
6. `canonical_hydration`

The implementation records the completed order in
`KnowledgeRetrievalPipelineReport`. A stage-order violation is an execution
error. Canonical output entities are not constructed until the merged candidate
set has been reranked and truncated by `candidate_limit`.

Search hits are rebound to canonical node IDs against the pinned graph view.
The rebinding repeats authorization-scope predicates against canonical state so
stale or mismatched projection metadata cannot authorize graph traversal. Other
metadata predicates remain relevance filters evaluated by the search projection
and graph-seed retriever.
Search hits without a canonical identity are projection residue and must be
removed before candidate construction. The pipeline report exposes the removed
count as `canonical_identity_filtered_out_count`.
Graph expansion propagates only authorization-scope fields (`space_id`,
`tenant_id`, `workspace_id`, and `visibility`) to adjacent nodes. Relevance
filters such as `kind` do not prevent a scoped Memory candidate from expanding
to an Entity in the same space.

## Resource contract

The pipeline uses one query-rooted `QueryMemoryLedger` for retained identity
maps, graph-seed candidates, bounded graph-context candidates, merged ranking
state, and canonical result materialization. Adjacency collection retains at
most the remaining graph-context budget plus one candidate per direction, and
the graph-seed retriever retains only its bounded top candidates while counting
the exact pre-limit total.

The final response is measured against
`DatabaseConfig::max_read_result_payload_bytes`. Exceeding either the query
memory root or the result payload budget fails the entire request. No partial
candidate, evidence, or graph-context response is returned. Diagnostics expose
the configured budgets, tracked peak, measured result payload, unique canonical
nodes read for output construction, and canonical candidate hydration count.

Search backend working sets remain governed by their backend-specific bounded
candidate and hydration controls before entering this retained pipeline. The
pipeline report deliberately names `peak_tracked_memory_bytes`; it does not
claim that resident search-index pages or the caller-owned projection are query
allocations.

## Snapshot contract

`DatabaseReadTransaction::retrieve_knowledge` binds canonical identity,
authorized graph expansion, reranking inputs, and canonical output hydration to
one immutable graph snapshot and reports its commit epoch. Projection freshness
is reported separately because the search projection is asynchronous derived
state. A projection epoch is never presented as canonical graph identity.

## Formal refinement

`docs/tla/HawDBKnowledgeRetrievalPipeline.tla` models the stage machine,
snapshot binding, authorization gate, TopK-before-hydration rule, query memory
bound, result payload bound, and fail-closed terminal states. The Rust
refinement points are `KnowledgeRetrievalGraphContext`,
`KnowledgeRetrievalPipelineBudget`, `expand_knowledge_context`,
`knowledge_candidates`, and `hydrate_knowledge_output`.
