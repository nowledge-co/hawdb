# Optimizer logical cost contract

`PlanCostBreakdown` stores candidate cardinality, raw logical work components,
and one saturated scalar total. Cardinality remains independent of weights.
The initial policy uses CPU = 1, random access = 2, sequential access = 1, and
output = 1. The constants live in `skein-optimizer/src/cost.rs`.

These are planning units, not nanoseconds, bytes, physical pages or a portable
hardware calibration. Random access has a modest locality penalty; this does
not model cache state, row width, page clustering or storage residency. The
[persisted measurements](RELATIONAL_ACCESS_COST_LINUX_CALIBRATION.md) motivate
separate row-fetch accounting and rejection of non-covering paths with broad
estimated fanout.
They also show that sparse first/warm winners can differ. This initial policy
does not resolve that context-dependent calibration or promise every selected
path is faster. No runtime configuration or persisted setting is introduced.

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
prefix counts where available; nonresident indexes use existing fresh prefix
NDV average fanout, then fall back to table rows when statistics are unavailable.
The average does not capture value-specific skew, and live/delta invalidation
continues to reject stale statistics. Coverage is derived from required scan
fields before candidates are costed, then retained in the final physical tree.

The shared descriptor estimator is used by skyline/selection, all join memo
frontends, physical plan construction and profiles. A probe scales right-input
components by outer cardinality; materialized/hash/merge inputs are paid once.
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
the three bucket predicates retain a non-covering index because their fresh
NDV averages coincide, despite different actual selectivities. Historical forced
index v1 observations remain labeled as baseline evidence. First-handle and
warm measurements must remain separate, and neither is a cold-device claim.
