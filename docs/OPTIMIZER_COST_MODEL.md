# Optimizer logical cost contract

`PlanCostBreakdown` stores candidate cardinality, raw logical work components,
and one saturated scalar total. Cardinality remains independent of weights.
The initial policy uses CPU = 1, random access = 2, sequential access = 1, and
output = 1. The constants live in `crates/cascades/src/cost.rs`.

These are planning units, not nanoseconds, bytes, physical pages or a portable
hardware calibration. Random access has a modest locality penalty; this does
not model cache state, row width, page clustering or storage residency. The
[persisted measurements](RELATIONAL_ACCESS_COST_LINUX_CALIBRATION.md) motivate
separate row-fetch accounting and rejection of non-covering paths with broad
estimated fanout.
They also show that sparse first/warm winners can differ. The current structural
context and value-specific counts do not promise every selected path is faster;
first-handle and warm timings still require separate candidate measurements. No runtime configuration or persisted setting is introduced. The
[earlier macOS qualification](RELATIONAL_ACCESS_COST_MACOS_20261007.md) records
its named revision and the cache/locality/skew boundary. Its timings do not
qualify later context or metadata-count changes.

## Composition

The constructor weights the raw components exactly once. Unary/binary and
probe composition add or scale raw components before constructing a new total;
they never add a weighted total into an I/O component. The CPU normalization
of one preserves opaque scalar-to-breakdown conversion without reweighting.
Such a conversion preserves cost, not the original component attribution.
All arithmetic saturates; the existing minimum-one cardinality floor remains.

For `n = max(estimated_rows, 1)`, relational accesses use:

| Access | CPU | Random | Sequential | Output |
| --- | ---: | ---: | ---: | ---: |
| Full scan | n + 4 | 0 | n | n |
| Primary key | n | n | 0 | n |
| Secondary index | n + fetches | 1 + fetches | n, or 0 for a unique point | n |

`fetches` is `n` only when `requires_row_fetch` is true. The scan setup and
index navigation charges occur once per invocation. Ordering/range metadata
affects delivered properties and estimated visits, not an extra invented disk
charge. These are access and join costs; later sorting, deferred output
hydration and other SQL stages are not a full-query latency estimate.

For 100 rows, a full scan costs 304. A non-covering 90-row index costs 542; a
covering one costs 272. A one-row non-covering index costs 8, while a direct
primary-key lookup costs 4. The distinction exists before skyline pruning,
access selection and join enumeration. Resident posting lists supply exact
prefix counts where available. For nonresident indexes, a fixed complete key
uses the selected unchanged checkpoint's metadata count when the provider
supports shared operation admission. Partial keys, unknown outer join keys,
matching live/private changes, recovery backends and unsupported providers
retain fresh prefix NDV average fanout, then table rows when statistics are
unavailable. The average does not capture value-specific skew; stale statistics
remain ineligible. Exact counts retain the minimum-one cardinality floor and
existing table/intermediate-row caps. Coverage is derived from required scan
fields before candidates are costed, then retained in the final physical tree.

Successful count reuse also binds the actual retained checkpoint reader, since
equal schema/root definitions and generation/epoch metadata can belong to
different databases. A context retains one source; switching readers drops
previous counts. Statement metadata, transaction visitors and constraint reads
share one settlement policy: typed descriptor refusal keeps accrued charges and
permits retry, while other admitted errors and unwinds close the owning ledger.

For a selected row snapshot with `r` table rows and `p` table pages, each point
lookup adds raw CPU `2 + floor(log2(p)) + 1` and random access
`3 * (floor(log2(p)) + 1)`. A scan of `n` rows adds both CPU and sequential
access `3 * min(ceil(n*p/r), p)`. Each checked descriptor reads its fixed record
and two nonempty bound keys; point searches jump among descriptors while scans
visit them in order. Those metadata operations remain distinct from the
canonical row accesses already charged by the descriptor. Secondary paths add
point work only when they fetch canonical rows. Covering paths do not pay it.
Projection rows and missing/mismatched roots retain the default.
Contexts are keyed by original binding identity, including after join reorder,
and come from the same reader moved into execution.

The shared descriptor estimator is used by skyline/selection, all join memo
frontends, physical plan construction and profiles. A probe scales right-input
components by outer cardinality; materialized/hash/merge inputs are paid once.
The current Hash adapter retains typed locators and re-fetches both projected
rows for every estimated equality match, including resident execution. Each
snapshot locator fetch adds CPU `3 + search_depth` and random work
`1 + 3*search_depth`; it does not add another output charge. LEFT Hash also
allows a left locator fetch for every outer row as a conservative unmatched
bound: matching-pair counts do not identify distinct matched outer rows. Merge
retains projected rows and has no such resident fetch. Original binding
contexts are used consistently in memo selection and physical/profile costing.
Missing/default contexts preserve the descriptor-only join contract. Spill
validation and repartitioning add work beyond this resident estimate; costs
neither certify cache warmth nor guarantee admission under execution budgets.
The public rows-only access/probe helpers retain their previous CPU-only
contracts for consumers without descriptors; runtime planning uses descriptors.

Graph access ranking and final costing share raw access helpers. A node index
seek charges candidate CPU visits plus random candidate/startup work. Scans
retain sequential work. This changes some scan/index thresholds and reported
costs; cardinality estimators, residual predicates, order requirements and
limit placement are unchanged. The indexed-memory golden keeps the same plan
and changes only its cost line (total 4 to 6, CPU 1 to 2, random work 3 to 2).

## Verification surfaces

Ordinary tests compare component arithmetic, candidate permutations, covering
and non-covering selection, saturation, scalar normalization and join scaling.
The manual access campaign compares the selected cost with an independent
minimum over unpruned candidates. Existing physical-tree and memo campaigns
check independent component arithmetic, legal plans and profiles. Embedded SQL
tests assert complete results and the actual scan/index/point operators.

The persisted benchmark's v2 report names its second arm `cost_selected` and
checks each expected operator. All-rows predicates deliberately choose a scan;
the historical three bucket predicates retained a non-covering index because
fresh NDV averages coincided despite different actual selectivities. That
historical selection does not qualify the current value-specific planner. Historical forced
index v1 observations remain labeled as baseline evidence. First-handle and
warm measurements must remain separate, and neither is a cold-device claim.

## Planning reads and statement ownership

Metadata counting traverses and decodes the root-to-leaf path without visiting
canonical rows or decoding posting chains. A cold mounted handle also validates
the whole immutable object; those physical bytes are admitted and reported.
Preparation and execution move one owned index context through the pinned
planning snapshot. Logical and physical allowance, cancellation, selected-view
identity and accumulated reports survive that move; execution receives no new
allowance. Budget/read failures abort planning, and admitted incomplete reads
close the context. Ordinary `EXPLAIN` retains its existing independent planning
task; SELECT and EXPLAIN ANALYZE retain the caller's task.

Only successful eligible counts enter the statement-local cache. Every reuse
rechecks eligibility, known poison, identity and ledger health. Reuse performs
no page traversal and returns zero read charges with the selected identity.
Cache entry count and retained key payload are bounded by the statement's page
and byte limits. New identities cannot reuse older values. A declined capability
performs no I/O and creates no read attempt or cache entry.

`index_execution_evidence` includes all statement index reads, including planning
metadata and probes of candidates later rejected by selection. Ordinary EXPLAIN
can therefore report metadata I/O while its operator profiles have no actual
execution rows. Plan/execute stage timings remain separate; read counters are
not hardware latency estimates.

The total `lookups` includes metadata counts. `metadata_count_lookups` (rendered
as `metadata_counts` in EXPLAIN) identifies those planning reads separately,
including zero-I/O cached counts. Backend, range and early-stop counters describe
execution probes only. Ordinary EXPLAIN therefore reports metadata I/O and
`runtime_path=not_executed`, without inflating an executed backend counter.
Both read purposes retain the same statement budget and identity accounting.

The [exact-count default smoke qualification](RELATIONAL_ACCESS_COST_EXACT_COUNTS_20261007.md)
records all twenty 256-row comparisons and every first/warm profile. Its complete
results and plan assertions pass, while non-point latency regressions relative
to the paired unindexed scan remain explicit. It does not replace full-scale
release calibration or establish a uniformly faster access policy.
