# Graph width and blocking-work estimates

The graph cost fold carries bounded row-shape metadata with its existing
cardinality/binding metadata. It estimates logical work for choosing existing
implementations; it does not measure elapsed time, allocated Rust heap, device
pages, or power-loss behavior. Public `output_rows` remains a row-work count.
Runtime memory admission, cancellation, spill quotas and failure behavior stay
mandatory and use the unchanged executor configuration.

## Sources and bounds

The optimizer catalog retains the same pinned core schema through its existing
COW containers. A table-qualified descriptor range avoids scanning unrelated
schema objects. A full record inspects at most 64 public properties, and a row
tracks at most 64 projected column names. Additional fields retain positive
opaque payload proxies rather than disappearing from the retained-state estimate.
Pass-through operators move this metadata; they do not clone the column map at
every level. All arithmetic that can multiply row counts or widths saturates.

Declared Boolean/integer/float cells have no separately owned variable payload.
Vectors use their canonical four-byte dimension plus four bytes per component.
Their owned encoded buffer is deep-cloned when the value is cloned. TEXT uses
a 4-KiB type proxy; LIST uses 256 bytes. These types deliberately do not acquire
new value histograms. Missing or unsupported scalar metadata uses a positive
64-byte payload proxy. Unknown full graph records use 4 KiB.
Core value kinds without explicit payload models use the existing unknown-width
fallback for literals and samples.

Existing eligible scalar histograms can supply at most 32 evenly spaced distinct
values. The maximum observed supported payload length is a **distinct-domain
sample proxy**, not a frequency-weighted mean, a maximum over all source rows,
a uniqueness/non-NULL proof or a memory bound. Declared STRING rejects a sample
with an incompatible value type. Stale/missing statistics retain the existing
source validity gates and the documented fallbacks. No statistics collection,
persistence format or background task is introduced.

Traversal steps add a conservative 4-KiB graph payload proxy. Each step is
included in `proxy_fields`, including in downstream Sort reports; this can
overestimate retention/spill for ID-only bindings. It is not a measured per-hop
allocation or a runtime memory bound.

Logical field/container cells follow the runtime's map-cell estimation shape.
Reported `proxy_fields` and `sampled_values` make unavailable/type-proxy and
sample-backed estimates visible; they do not turn the result into an allocation
bound. Ordinary projections preserve graph bindings, whereas a nonempty fused
node projection scan emits values alone. Projection aliases resolve against the
incoming value scope before replacing the output scope.

## Access and blocking work

Supported schema/sample/type information adds payload-copy/decode work to scans,
selected-property scans, full-record index hydration and column lookups. A
selected-property scan charges visited input rows, including rows rejected by its
predicate, and only its required fields. An index declaration does not establish
a covering access path. Unknown decoder metadata preserves the existing opaque
access charge while retained state still has positive width proxies. Existing
per-binding snapshot-row/page costing and storage capability checks remain in
place; `Auto` residency does not establish that an input is warm or resident.

Full sort replaces the previous approximately `2N` local CPU estimate with key
materialization, comparison work and header movement. It uses an `N log2 N`
comparison proxy, key width and the ordinal tie breaker. Binding payloads are
retained in memory, but sorting moves headers/pointers rather than copying their
entire bodies at every comparison. TopN models heap insertion, checking rejected
rows, a bounded replacement heuristic and sorting its retained `OFFSET+LIMIT`
rows for final delivery. When the estimated input fits within `OFFSET+LIMIT`,
TopN uses the same ordering-work proxy as Sort, and equal candidate rankings
prefer TopN. This deliberately avoids choosing unbounded retained state just to
save estimated initial heap work when correlated filters or fanout underestimate
actual input. It is a robustness heuristic, not a claim of identical comparison
counts or a hard source-work bound. Arrival order and adaptive standard-library sorting can
change actual comparisons; these are planning heuristics, not measured counts.
A completely satisfied ordering is still elided by the existing property proof.
A partially satisfied prefix receives no incremental-sort discount: the executor
still performs a full sort for that shape.

Sort/TopN retain bindings and prepared sort keys. Distinct owns both cloned value
keys and representative bindings; hash join retains its right build input; aggregate
uses estimated groups and opaque accumulator payloads. Data-dependent aggregate
states remain approximate. When TopN's retained rows cannot fit, its spill proxy
covers the input stream because spilling loses the heap cutoff.

The existing `OptimizerContext.resource_hints.max_memory_bytes`, derived from
`ExecutionMemoryConfig.blocking_operator_bytes` by the embedded runtime, supplies
the known blocking-state hint. A positive native hint below estimated retained
state adds logical 64-byte read/write payload units and a conservative two-way
merge-depth proxy to `sequential_io`. This is not a forecast of encoded spill
bytes or physical I/O requests. The browser has no disk spill and receives no
disk-spill cost. Unknown/zero hints do not invent a disk forecast. Live soft
pressure, skew, directory availability, quotas and single-item refusal remain
executor decisions.

Bounded sort and ordered-access candidates are compared after the same existing
projection fusion, and the unordered competitor includes TopN when applicable.
This avoids charging an unfused full graph record to only one alternative.
Offline qualification perturbs ranking only after raw width estimates; the
Sort/TopN competitor comparison uses the same qualification controls.
Width/decode/spill work enters the same startup/variable-work profile used by
row goals. Blocking work remains startup work; streaming decode can scale with
consumption. Bounded ordered and unordered candidates retain the caller's memory
hint throughout this comparison.
The selected plan uses the same context/cost fold as ranking. Only that fold
formats `physical width cost:` diagnostics in existing query reports; alternative
costing does not allocate diagnostic strings. Refreshing a cached plan replaces
these diagnostics rather than appending stale estimates.

## Independent delivery and verification

The comparison model can prefer Sort plus LIMIT for very small inputs. A
regression with two output rows and a 2-MiB query budget exposed LIMIT reserving
the full 16-MiB batch ceiling. Its transfer now admits the already-owned selected
payload before allocating the output vector and enforces the pending output
window. This is the identical narrow executor fix also present in the independent
joint-NDV PR; neither PR depends on the other branch. Kernel tests check exact
admission and one-byte-deficit refusal. A property-ordered generic projection can
have its own pre-existing batch reservation; no broad projection rewrite is
part of this change.

See `graph::costing::tests_width`, schema snapshot tests, transform
`limit_admission`, and root `api::tests::width_costing` for bounded arithmetic,
source/type/alias ownership, actual choices, NULL/tie semantics, warm templates,
pinned readers and checkpoint/residency coverage. Release experiments record
complete result parity, real spill counters, neutral/adverse cases and source
provenance separately from estimated work. No performance gate or CI fuzz lane
is added.
