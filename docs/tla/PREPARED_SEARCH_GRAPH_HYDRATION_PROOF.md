# Prepared graph Search hydration: conditional argument

`PreparedRead::hydrate_search_projection_nodes` constructs Search rows from the
pinned source using a fixed parameterized query adjacent to the helper in
`src/api/dependency_read/search_projection.rs`. The query runtime selects internal
IDs, expands optional undirected HAS_LABEL edges to Label nodes, groups by the
root and collects distinct business-label nodes. The helper performs the Search
kernel's existing per-row field conversion and business-label string shaping.
It does not perform a graph join, traverse a host graph or scan all relationships.

The return value contains rows and no changefeed cursor, source watermark or
batch-coverage receipt. A caller still needs a complete canonical-issued batch
and host binding before claiming incremental progress below the snapshot epoch.
The existing Database/Mem change-batch builder has not been switched to this API.

## Exact source values and query boundary

`properties(n)` returns the native node/relationship property map, including
user-owned `_id`, `labels`, `type` and other metadata-like names without replacing
them. `labels(n)` independently returns catalog names for native node labels in
their stored ID order. They use native entity bindings, including identity
restoration across WITH; optional null entities return null. `labels` on a
relationship and arbitrary scalar/map arguments are unsupported. These are
local expressions, not new graph sources. The planner keeps the native record
rather than selecting an incomplete property subset for these expressions.

Both functions reserve computed expression ownership before cloning values.
Labels allocate their exact valid-name capacity; a one-name result does not rely
on Vec's growth capacity matching its logical length. Plan fingerprints,
binding/variable analysis and plan-cache determinism include the new expressions.
The parser distinguishes native `properties(nodes)` from the existing path form
`properties(NODES(route), 'name')`, including a native variable named `nodes`.

Root properties use the raw function because `RETURN n` adds `_id` and `labels`
metadata and can replace user keys. Collected business-label nodes may use the
existing entity-value representation: Search reads only their `canonical_name`,
`name` and `id` properties, which that representation preserves. The same Search
formatter supplies nonempty-string precedence, coercion and sorted/deduplicated
business labels. Root kind precedence, external ID fallback, metadata conversion,
space normalization and Memory metadata paths reuse the existing Search shaper.

## Dependency closure

Assume complete existing graph write intents, append-only implicit catalog
creation, explicit schema barriers for destructive catalog operations, retained
newer history and one canonical publication order:

1. The necessary internal-ID bound covers every possible root, including absence.
   Each root's GraphNode key covers its presence, properties and label membership.
   Explicit schema changes cover label-name/kind interpretation. Implicit new
   catalog names preserve existing IDs and do not change their meaning.
2. The complete single-hop prepass records both root adjacency directions and
   every candidate relationship and endpoint before filtering for Label or
   property values. A new edge hits adjacency; edge/neighbor changes hit their
   identity keys. Empty optional scopes retain adjacency dependencies.
3. Null extension and COLLECT/grouping are deterministic functions of that same
   closed input set. No root is silently lost just because it has no business
   label. Root properties and labels do not introduce another data source.
4. If Label or HAS_LABEL is absent at capture, a node-only query suffices for the
   current result, but its footprint must still cover future first edges.
   Implicit CREATE need not emit Schema: missing HAS_LABEL therefore records
   both directed root scopes explicitly. If HAS_LABEL exists but Label does not,
   a bounded prepass retains the existing candidate endpoints as well. A later
   first matching edge cannot validate against an empty point-only footprint.
5. Search conversion reads only these captured row fields and the same captured
   catalog. Thus accepted validation implies unchanged hydrated Search rows at
   publication. The existing source validator holds the exclusive Database
   borrow through actual Search activation, preventing a validation/install gap.

This is a conditional source argument, not mechanized Rust refinement. It
depends on existing canonical adjacency completeness and mutation classification.
It preserves the transaction system's Snapshot Isolation boundary.

## Admission and refusal

The helper admits at most 1024 root IDs before constructing parameters, rejects
IDs outside the query runtime's nonnegative i64 range, and returns one row per
present Search node even for repeated IDs. Missing/non-Search nodes are omitted.
Empty input is a deterministic empty result.

Configured statement row/payload/runtime limits apply to the complete query,
and existing dependency entry/byte limits apply cumulatively. Single-hop metadata
enumeration shares 1024 work units across roots/directions. The separate output
cap counts retained row UTF-8 fields and metadata keys/values; overflow or excess
fails the entire operation. Shaping can own one candidate row plus the bounded
query result and admitted retained rows before this check. This is a logical
payload bound, not a total allocation, parser-work or RSS bound. Canonical block
reads still validate encoded values as described in the adjacency proof.

Every query, metadata, shape or output-budget error makes the preparation sticky
failed. A later successful empty call cannot turn a failed preparation into a
valid source proof. No prefix of returned rows or progress is published on error.

## Root-access performance boundary

The hydration plan can retain `FilterExec(IdIn)` over `SeqNodeScan`; the
physical operator name alone does not establish a full scan. Necessary positive
ID bounds now select admitted point reads in ordinary roots, projected LabelScan
roots and generic MATCH node patterns. The [source-linked refinement and
regressions](INTERNAL_ID_POINT_ACCESS_PROOF.md) cover resident and checkpoint-open
OutOfCore sources, residual/NULL semantics and unrelated large payloads.

This closes the eligible root ownership fallback. Unsupported shapes keep scan
execution, and cold block/cache validation still has its existing I/O and memory
costs. Adjacency and host query/mapping work need separate budgets. The original
1/4/8-writer benchmarks and retained-source RSS qualification remain required;
the helper is not a completed end-to-end performance delivery.

## Evidence and remaining scope

`HawDBAdjacencyProjectionReadValidation` now includes required and optional empty,
node-filtered and edge-filtered results. Its full-history oracle and direct query
outputs cover **5,029,782 generated / 2,143,350 distinct states**, depth 11. All ten
fault controls and the disjoint-progress witness return status 12 with their
exact expected invariant. The [adjacency proof](ADJACENCY_PROJECTION_READ_VALIDATION_PROOF.md)
maps identity, barrier, pruning and sticky-refusal transitions to native source.

Native consumers compare prepared hydration with the existing Search graph-delta
semantics, including raw reserved-name properties, duplicate/incoming labels,
missing and repeated roots, unlabelled roots, hot/checkpointed out-of-core data,
unrelated foreground writes and actual staged Search activation. First-label-edge
regressions distinguish harmless implicit label creation from a relevant new
relationship. Separate row, statement-payload, retained-output, root-count and
ID-range refusal tests require an unusable token after ignored errors. Allocator
observation verifies that denied property/label expression ownership does not
copy the watched large payload, without claiming upstream/total memory bounds.

The synthetic cold-start changefeed omission is now repaired by pre-WAL
canonical metadata capture, with permanent delta-versus-full-rebuild and replay
regressions. See [capture and recovery argument](PROJECTION_CHANGE_CAPTURE_PROOF.md).
This does not certify bounded B < E batches hydrated from the current E source;
valid hydration dependencies alone cannot establish batch coverage.

The finite model abstracts maps, expression parsing/coercion, physical I/O and
string formatting. Native equivalence and admission regressions support those
mappings; they do not prove arbitrary Cypher, all allocations, byte-level power
loss or fairness/performance. Bounded changefeed coverage and exact relational
hydration receipts, host incarnation/binding, Mem owner handoff, recovery/fault
composition, retention/RSS and the full #231/#232 latency gates remain required.

## Admitted internal-ID root refinement

Qualified internal-ID roots now use the [point-access refinement](INTERNAL_ID_POINT_ACCESS_PROOF.md) in ordinary, projected LabelScan and generic MATCH execution. Actual hot/cold hydration with a 1 MiB unrelated node passes a 128 KiB query budget. These tests and the finite access model close the eligible root ownership gap; unsupported shapes and canonical block/cache I/O retain their existing limits. They do not prove batch coverage, complete host mapping, total RSS or publication latency.
