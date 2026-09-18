# Optimizer Statistics Eligibility Specification

## Status

This document defines the v1 eligibility boundary for graph property optimizer
statistics. It covers property distinct counts, value histograms, histogram
sampling markers, external refresh spill records, checkpoint recovery, and
optimizer consumption. It does not change property indexes, full-text indexes,
canonical records, basic graph counters, or path-cardinality statistics.

## Eligibility

Property statistics admit scalar values whose declared schema type is eligible:

- `Null`
- `Bool`
- `Int`
- `Float`
- `String`, including `VARCHAR` and `CHARACTER VARYING` declarations

The following declared types or runtime values are ineligible:

- `Text`, regardless of encoded length
- `List`
- `Map`

New variable-width or large value kinds are ineligible until this specification
explicitly admits them. Adding a new `Value` variant MUST require an explicit
eligibility decision in the exhaustive classifier.

Schemaless string properties retain the historical `String` behavior. A caller
that stores unbounded content MUST declare that property as `TEXT` to select the
large-value resource policy. `String` does not enforce a byte-length limit in
v1; `VARCHAR` and `CHARACTER VARYING` are schema aliases rather than physical
length constraints.

Text indexes and text predicates remain supported. This boundary only prevents
general-purpose NDV and histogram collection from cloning, sorting, hashing,
spilling, checkpointing, and retaining declared text payloads. The policy is
hard-coded in v1 so collection, recovery, and optimizer consumption cannot
disagree because of configuration drift.

## Whole-group exclusion

Statistics for one `(label, property)` or `(relationship type, property)` group
MUST be complete for the admitted value domain. If any canonical record in a
group contains an ineligible value, HawDB MUST publish none of the following
for that group:

- distinct count
- histogram values
- exact-versus-sampled marker

This rule applies to mixed-type properties. Publishing statistics for only the
compact subset would undercount NDV and could make the cost model select an
unsafe or systematically poor physical plan. Missing property statistics use
the optimizer's existing conservative fallback.

## Collection paths

Materialized statistics MUST classify a value before cloning it into a distinct
set. Encountering an ineligible value removes any earlier compact candidates
for the same group and permanently excludes that group for the refresh.

External statistics refresh MUST classify a value before cloning, encoding,
sorting, hashing, or writing a spill fact. It retains only the property-group
identity in a bounded exclusion set. Earlier compact facts may already exist in
a bounded spill run when a later record makes the group ineligible; the merge
MUST consult the complete exclusion set and publish no statistics for that
group. The exclusion set and fact buffers share `memory_budget_bytes`.

Index backfill MAY maintain an index over a declared `TEXT` value, but MUST NOT
derive generic optimizer property statistics from that value. Index storage
and optimizer statistics are separate resource domains.

This differs deliberately from PostgreSQL `ANALYZE`, which retains lightweight
null-fraction and average-width information for wide values while excluding
large payloads from its detailed value distribution. HawDB v1 publishes no
generic property group for declared `TEXT` because an embedded PC workload has
a stricter and less predictable memory envelope. It follows Neo4j's narrower
graph-planner shape more closely: exact structural counts remain available,
while property selectivity should increasingly come from explicitly maintained
indexes rather than universal property scans.

## Persistence and recovery

Checkpoints persist only eligible property statistics produced by the current
collector. Recovery treats property statistics as rebuildable derived data. It
MUST remove a recovered property-statistics group when its histogram contains
an ineligible value or when the distinct-count, histogram, and sampling-marker
triple is incomplete or empty. Canonical records, indexes, basic counters, and
path statistics remain intact.

`advanced_statistics_complete` means the bounded refresh completed for all
supported statistics families at one source epoch. It does not mean that every
property type has NDV or histogram data.

## Observability

`OptimizerStatisticsRefreshReport` reports published node and relationship
property group counts separately from excluded group counts. `generated_facts`
does not include ineligible values because they never enter the fact stream.
Peak buffer accounting includes the bounded exclusion set.

## Cardinality floor

Optimizer cardinality is a planning weight, not an assertion that a row exists.
Every graph plan cost, cost breakdown, access-path estimate, and emitted
`estRows` value MUST therefore be at least one. Exact catalog counters may
remain zero, predicates may be proven false, and `LIMIT 0` may produce no rows;
those facts MUST remain visible through exact-empty metadata and measured
`actRows`, not by propagating a zero planning estimate.

The floor is applied after selectivity, offset, and limit arithmetic. It MUST
NOT turn `EmptyExec` into an executable scan, change query results, or add work
to the empty executor. It prevents a zero child estimate from annihilating join,
expand, aggregate, and parent cost comparisons.

## Verification

Required regressions cover:

- text, list, map, and mixed-type node property groups remain absent
- declared text relationship property groups remain absent
- compact numeric and `VARCHAR` groups retain exact NDV and bounded histograms
- a text value larger than the refresh memory budget does not become a fact
- external multi-run merge cannot publish facts from an excluded group
- checkpoint reopen preserves the same eligible statistics
- `HawDBStatisticsEligibility.tla` proves `VARCHAR` remains publishable while
  no text, large, or mixed property is published after a complete scan,
  including the case where a compact fact was observed before an unsupported
  value
