# Joint statistics in graph cardinality estimates

The graph optimizer reuses existing composite-index distinct counts for a
bounded set of filters, grouping keys and inner equijoins. This changes plan
estimates and can change physical choices such as TopN versus Sort plus LIMIT.
It adds no statistics collection, persisted fields or public configuration.

## Source and scope

`OptimizerCatalog` builds one unordered view of declared composite indexes,
limited to two through four properties and 32 key sets per label. Differently
ordered indexes with conflicting samples make that key set unavailable. Original
ordered access statistics remain intact. Existing advanced-statistics completion,
sample validity and staleness gates still apply. A sampled NDV is an estimate,
not the sample's raw observed unique count. Query snapshots retain their own data
and statistics; no catalog facts are shared across a reader's snapshot boundary.

Only positive NDVs bounded by indexed rows and label population are usable.
Statistics never establish uniqueness, non-NULL values or a legal rewrite. In
particular, indexed rows matching the label count at the sample epoch does not
constitute a schema proof.

## Supported estimates

- A pure AND of non-NULL equality literals on one binding can use the full key
  set. Traversal is capped at 16 conjunct/tree nodes and four distinct keys.
  Repeated identical clauses count once. With no covered keys, the estimate is
  `input rows * indexed rows / (label population * joint NDV)`, rounded up.
- An access predicate already applied to the same fixed value is conditioned
  out, rather than counted again. Partial conditioning requires compatible
  complete key populations and a usable covered-key NDV. Range/multi-value
  access and incompatible populations retain the ordinary model. Fully covered
  equalities retain input cardinality. Duplicated input bindings are not capped
  by a source-node frequency estimate.
- Multi-key grouping uses joint NDV plus a bound for source rows excluded from
  the composite index by NULL or missing components, capped by input rows. Pure
  property aliases are resolved in the aggregate's incoming scope, including
  shadowed output names. Computed/ambiguous aliases, expansion populations and
  rebound lookups do not acquire this statistical domain.
- Hash join first estimates its actual single-key intermediate as
  `left rows * right rows / max(left key NDV, right key NDV)` when those domains
  are available. Multiplication uses `u128` before division and clamping. A
  following supported multi-key equality filter can condition this estimate
  using the complete joint domains. The hash operator and residual predicate
  still pay for the single-key intermediate. Missing or sparse joint domains
  retain that intermediate instead of applying an unrelated default selectivity
  to the already-enforced hash key. This marginal hash-key correction applies
  to single-key joins too, independently of a composite index. If either marginal
  NDV is absent or its binding is not a compatible label population, hash costing
  retains the product and the following filter uses the ordinary default model;
  the optimizer must not assume that key selectivity was already estimated.

Statistical metadata travels with the existing cost fold. Selected-plan
`decisions` entries beginning `cardinality statistics:` report operator ID,
operation and the numeric catalog inputs used. Trace refresh replaces earlier
statistics entries. This does not add a second plan traversal or allocate
diagnostic strings for every candidate alternative.

## LIMIT transfer admission

The changed estimates exposed an existing integration problem: a small group
result choosing Sort plus LIMIT could reserve the entire 16-MiB transfer batch
ceiling. LIMIT now sums only the already-owned rows inside its selected window,
caps that reservation at the existing batch ceiling, and admits it before
allocating the output vector. Skipped payload does not require an output
reservation. Pending buffered rows count toward the remaining window.

This is a narrow transfer change: source work, blocking operator limits,
oversized-row refusal, cancellation and downstream stopping remain enforced.
The regression runs a 400-row/20-group query under a 2-MiB query budget, and a
kernel check rejects a transfer budget short by exactly one byte before output.
The admission ledger describes tracked reservations, not measured Rust heap or
process RSS. Other operators may still reserve their configured batch ceiling.

## Validation

See [test ownership](TEST_INDEX.md) for replay commands. The manual
`joint_ndv_planning` benchmark compares small/bulk correlated grouping,
independent grouping, missing-index controls and two-key joins. Every measured
call checks complete rows and ordering, cache hits and released query accounts.
Source-bound performance evidence belongs in its benchmark report; more
accurate estimates alone do not establish lower latency, particularly for skewed
data where NDV does not encode key frequencies.
