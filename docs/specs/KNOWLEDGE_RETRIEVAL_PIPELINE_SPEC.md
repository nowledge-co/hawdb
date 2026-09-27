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

## Rerank scoring

The `rerank` stage combines a candidate's retriever scores into its ranking
score. Three policies are supported:

- `Max`: the larger of the search-hit score and the graph-seed score.
- `WeightedSum { search_weight, graph_seed_weight }`: the two retriever scores
  with caller weights.
- `Spec(ScoringSpec)`: a typed, host-injectable specification — a weighted sum
  of features multiplied by exponential decay factors. Features are the search
  score, the graph-seed score, the bounded graph distance from a seed, a
  numeric canonical node property, and a canonical timestamp property aged
  against the request clock.

Semantics:

- **Missing features are reported, not scored.** A feature the engine cannot
  supply contributes nothing and is listed in the evaluation's
  `missing_features`, which the candidate's `score_breakdown` carries. A
  request never fails because a property is absent, and an absent property is
  never silently ranked as zero-valued evidence.
- **Decay multiplies.** `0.5^(age / half_life)` is clamped to
  `[min_factor, 1]`; timestamps in the future are treated as age zero. Only
  `HopDistance` and `TimestampProperty` define an age.
- **Weights are rebindable slots.** `ScoringSpec::shape_fingerprint` identifies
  the feature shape without its weights, so changing weights never invalidates
  a cached plan or a cached scoring decision.
- **Property features read canonical state inside the pinned snapshot.** Node
  records are loaded only when the policy asks for a property feature, bounded
  by the candidate limit; the stage order, snapshot binding, and budgets of
  this specification are unchanged, and the TLA refinement points keep their
  meaning.
- **`HopDistance` is zero for every candidate today** because only search hits
  and graph seeds become candidates; expanded nodes currently appear as graph
  context. Distance decay becomes observable when expanded nodes are ranked as
  candidates, which this stage's feature contract already supports.

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
