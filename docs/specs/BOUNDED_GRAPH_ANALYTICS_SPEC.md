# Bounded graph analytics and complete publication

Issue: [#294](https://github.com/nowledge-co/hawdb/issues/294).

## Execution contract

`CALL page_rank(...)` and `CALL louvain(...)` remain read-only. They prefer
the existing resident projection when its complete algorithm estimate fits
`blocking_operator_bytes`. If projection or algorithm-state admission fails,
the same analytics kernels run over repeatable, typed adjacency reads from the
pinned graph source. Storage errors are propagated rather than treated as
memory-admission failures. The execution report identifies the fallback as
`GraphAlgorithmStreaming`, with its budget and peak tracked state.

The fallback keeps sorted node IDs, one neighbor row, algorithm scratch and
complete scalar results in memory. Canonical edges remain in storage; it
neither creates a second edge set nor spills a materialized graph. Louvain
contraction retains original-node memberships and rebuilds each contracted
neighbor row by reading the original adjacency. Parallel endpoints are
deduplicated using generation marks. Sparse rows do not clear or scan the
entire node set. Community representatives use ordered membership sets.

This implementation preserves HawDB's existing simple, unweighted algorithm
semantics, deterministic ordering, self-loop handling and hierarchy outputs.
It does not implement the weighted/filtered Mem compatibility proposal in
[#898](https://github.com/nowledge-co/hawdb/issues/898). Typed legacy algorithm
methods continue to use the same kernels.

The operator conservatively admits 192 bytes per node for PageRank and 576
for Louvain, plus twice the scalar result size for possible vector capacity
growth. Louvain includes every requested level in the result estimate. A
decoded relationship must fit the remaining operator budget and is charged
transiently to the query ledger. Decoding and page retention also depend on
the host's separate storage record and segment-cache limits. Node state,
oversized records, output rows, or query-ledger exhaustion may still make a
request impossible; it fails instead of silently truncating the algorithm.

Cancellation is checked at scan boundaries, every 1,024 visited records or
algorithm steps, and iteration/level boundaries. Computation completes before
any algorithm row is emitted. A downstream consumer can stop or fail normally
once output begins; complete staging below rejects incomplete consumption.

Out-of-core reopen skips optional persisted V1 CSR/CSC cache artifacts. Their
files remain intact, and authoritative projection definitions recover through
the existing WAL/catalog path. Such readers query canonical adjacency rather
than loading a complete old cache before query admission.

Descriptor scans retain a verified page view in the existing bounded segment
cache. Its charge includes the immutable page image, entry offsets and codec
limit maxima. Cache hits bind the selecting page's SHA-256, identity, key range
and current limits before borrowing entries; they do not rehash or allocate
the entire page. Prefix and lower-bound scans seek through sorted entries,
including binary-search comparisons in the leaf-read counters. Cache misses
and full scrub still run the complete checksum and structural codec. Cache
rejection preserves fully verified uncached execution. No persistent descriptor
format or cache-capacity default changes.

## Embedded staging and publication

The facade exposes `GraphAnalyticsRequest`, `GraphAnalyticsAlgorithm`,
`PreparedGraphAnalytics` and `GraphAnalyticsPublicationStatus`:

1. `Database::prepare_graph_analytics` pins the source epoch and projection,
   obtains background projection admission, and executes ordinary parameterized
   Cypher. Explicit row, payload and staged-result budgets cover the complete
   result. The staging reservation is deducted from the configured query
   budget. Iterations and damping are parameters; the projection name is escaped
   structural input because the existing CALL grammar requires a string literal.
2. `Database::publish_graph_analytics` validates the database incarnation,
   branch and source epoch, obtains background admission for the complete
   publication, then applies scalar properties and provenance with
   parameterized Cypher in one existing mixed graph/SQL transaction. For Louvain,
   every computed level is staged and the final level determines the published
   node property. Complete publication must fit mutation/WAL operation limits;
   it is never split across transactions.
3. The transaction also updates `__hawdb_analytics_publications`, keyed by
   `(projection, property)`, with publication identity, source/publication epochs,
   algorithm options, pinned definition and row count. This is HawDB operational
   metadata, not a newly imported Mem canonical dataset. It uses existing SQL
   schema/data durability without a WAL or checkpoint encoding change. Its
   exact schema is validated on read; conflicting user-created schemas fail
   closed. No compatibility migration is added for development databases.

For a property `rank`, publication sets `rank`,
`rank_computed_at_commit_epoch`, `rank_published_at_commit_epoch` and
`rank_publication_id`. Property names must be ASCII identifiers of at most 128
bytes. Prepared results retain the source reader pin until dropped. Existing
pinned readers continue to observe the old complete result.

Cancellation, changed source, admission failure, constraints, or incomplete WAL
append leave the previous complete publication unchanged. A retry using the
same prepared object and property returns its durable publication outcome,
including when a response was lost. Retrying requires retaining that object;
this API does not provide a serialized job-resumption format.

`graph_analytics_publication_status` returns `Unavailable`, `Fresh` or `Stale`
and both epochs. Freshness is conservative: any later commit makes a publication
stale, while its own publication commit does not. Hosts can use this typed
status in readiness/observability without enabling Mem cutover. Larger workloads
may require explicitly configured background QoS and transaction limits; the
library's default admission and power-loss-safe durability remain unchanged.

## Verification and measurement

Executor differential tests compare resident and external execution, including
exact PageRank float results, filtering, unknown selectors, direction, duplicate
edges, self-loops, hierarchy, LIMIT and consumer stop/error behavior. Edge-heavy
fixtures complete both algorithms with a 96 KiB operator budget although just
the unique edge endpoint payload exceeds that budget. Injected storage errors,
mid-scan cancellation and impossible budgets produce no algorithm rows.

Facade tests cover source changes, old readers, idempotent retry, failed
constraints, partial WAL append, WAL/checkpoint reopen under both durability
policies, and preservation of a completed publication across a later torn tail.
The isolated native RSS test opens a checkpointed 128-node/16,256-edge database
in out-of-core mode with a 2 MiB segment cache and 4 MiB query budget. It asserts
an RSS-growth limit from the initialized reader baseline separately from the
operator's tracked-state limit and records peak RSS, bytes read and wall time.
These tests reuse the existing
transaction protocol; they do not constitute a new platform power-loss proof.

Local benchmark entrypoints:

```sh
cargo test --locked -p hawdb --bench graph_analytics
cargo bench --locked -p hawdb --bench graph_analytics
bazel run -c opt //:hawdb_bench_graph_analytics
```

The benchmark is manual and is not added to CI or the routine benchmark
dispatcher. Debug builds use small smoke fixtures. Optimized builds measure
33,000 nodes/45,000 edges, a doubled fixture, and an edge-heavy fixture, all
explicitly synthetic. Each shape runs in its own process, with fixture setup
excluded from the measured read-only query. Fixture preparation uses the existing
typed initial-import library path with a fixture-only WAL batch limit matching
its record count; measurements use a 64-row batch, 200,000-row limit and 64 MiB payload
limit. JSON records source epoch, open
time, algorithm options, budgets, output rows, query peak, operator reports,
adjacency bytes read and process RSS high-water marks. Repeated passes trade
bounded edge memory for CPU and I/O; the receipt makes that cost visible.

For a prepared, sanitized export with an existing projection:

```sh
cargo bench --locked -p hawdb --bench graph_analytics -- \
  --database /path/to/export mem_graph 24576
```

The last argument is the operator budget in KiB. The export is opened read-only;
neither algorithm publishes properties or advances its head. A representative
Mem export and its scaled resource receipt remain a separate qualification
requirement for #294. Synthetic measurements must not be presented as evidence
from a real Mem dataset. HawDB remains excluded from stable Mem artifacts under
the repository release policy.
