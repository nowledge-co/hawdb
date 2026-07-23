# Skein TODO

This file tracks the remaining Nowledge Mem replacement work for Skein. The
scope is intentionally product-driven: implement capabilities required to
replace the local Kuzu/Ladybug graph layer and the LanceDB search projection.
Do not expand into general-purpose database features unless a Nowledge Mem route,
query family, or cutover gate requires them.

## P0: Production Replacement Gates

- [ ] Cover every production graph read route with shadow parity evidence.
  - Current route families already gated include overview, explore, expand,
    live preview, node details, orphans, shortest path, community members,
    community subgraph, and community recent memories.
  - Add the remaining route families only after confirming they are still active
    Nowledge Mem call sites.
- [ ] Move graph read traffic through the query runtime boundary.
  - Keep Kuzu/Ladybug as primary until each route has stable Skein shadow
    evidence.
  - Avoid direct hand-written execution paths in application routes when the
    AST, fast-path detector, optimizer, and executor can own the path.
- [ ] Complete dual-engine cutover readiness.
  - Replacement summary must fail closed when route parity, storage recovery,
    background maintenance, search projection parity, or bounded-read coverage is
    missing.
  - Integration readiness must keep legacy Kuzu/Ladybug and LanceDB data
    side-by-side until cutover is proven.
- [ ] Keep sensitive paths and data out of readiness artifacts.
  - Default reports must redact local paths and raw parse or I/O errors.
  - Expose raw local diagnostics only behind explicit debug flags.

## P0: Graph Kernel Compatibility

- [ ] Finish the Nowledge-used Cypher subset.
  - `MATCH`, one-hop and bounded multi-hop patterns.
  - `WHERE` equality, range, boolean, null, and list membership predicates.
  - `RETURN`, aliases, aggregation, ordering, offset, and limit.
  - `CREATE`, `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`.
  - Nowledge schema DDL and migration statements.
- [ ] Keep parser output syntax-only.
  - Parameter binding, catalog lookup, type checks, and semantic validation stay
    outside the parser.
  - Fast paths should be selected from simple AST shape checks, not string
    matching.
- [ ] Strengthen planner, optimizer, and executor ownership.
  - Use Cascades groups, logical rules, implementation rules, physical
    properties, and deterministic costs for non-trivial graph reads.
  - Keep storage-specific choices in catalog metadata and physical rules, not in
    parser or route handlers.
- [ ] Maintain stable Nowledge API behavior.
  - Preserve node, relationship, metadata, pagination, and ordering contracts.
  - Preserve `include_metadata=false` metadata stripping behavior.
  - Compare row shape and error class before allowing replacement readiness.

## P0: Storage and Recovery

- [ ] Keep WAL and checkpoint recovery as cutover blockers.
  - Mutations must recover as whole committed batches or not at all.
  - Torn WAL tails must be detected and bounded.
  - Checkpoint manifests must include replay boundaries.
- [ ] Add storage-level scan pruning where semantics are exact.
  - Equality, numeric range, date/time range, enum/in-list, and unique-key
    summaries should decide whether a segment needs to be read.
  - Bloom or cuckoo filters should be used only for fields where false positives
    are acceptable and false negatives are impossible.
- [ ] Keep memory use bounded by default.
  - User foreground reads are admitted first.
  - Internal background import, projection, compaction, analytics, and shadow
    compare work must be deferrable under resource pressure.

## P0: Search Projection Replacement

- [ ] Continue replacing LanceDB only as a rebuildable search projection.
  - Canonical facts remain graph/content state, not vector index state.
  - Search projection evidence must prove row count, document identity, embedding
    identity, lifecycle, and incremental watermark parity.
- [ ] Keep FTS and vector projection maintenance incremental.
  - Full rebuild is a repair path, not the steady-state update mechanism.
  - Background projection updates must respect QoS limits.
- [ ] Add retrieval projection options behind advisor gates.
  - Raw float32 or SQ8 remains the safe path.
  - TurboQuant-style compressed projections can be used for cold or constrained
    local segments only after recall and parity evidence is available.

## P1: Operability

- [ ] Add compact readiness dashboards for route, query-family, storage, search,
  and background-maintenance blockers.
- [ ] Add stable counters for plan cache hit, miss, admission, eviction, and
  memory pressure.
- [ ] Add explain output that includes semantic checks, selected fast path,
  optimizer budget, chosen indexes, scan-pruning decisions, and resource class.
- [ ] Add typed preflight or harness commands for all replacement artifacts so
  Python-only validation scripts can be retired from the critical path.

## P1: Performance From Architecture

- [ ] Improve statistics maintenance.
  - Prefer incremental label, relationship, distinct-value, and degree summaries
    once correctness is proven.
  - Keep full rebuild as a validation and repair tool.
- [ ] Improve adjacency and index layout for read-heavy local workloads.
  - Optimize for bounded memory and predictable read amplification.
  - Keep row-oriented canonical records and add projection/index layouts only
    where route evidence proves value.
- [ ] Add workload fixtures based on real Nowledge routes before low-level tuning.
  - Benchmark graph reads, bounded expansions, metadata-filtered search, and
    mixed foreground/background workloads.

## P2: Deferred Capabilities

- [ ] Advanced graph algorithms beyond Nowledge's active routes.
- [ ] Broad openCypher compatibility not exercised by Nowledge Mem.
- [ ] Distributed storage, replication, or cloud-primary execution inside the
  local embedded engine.
- [ ] Aggressive SIMD work unless route-level evidence shows it is needed.
