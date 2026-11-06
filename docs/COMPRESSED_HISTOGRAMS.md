# Bounded compressed property histograms

HawDB stores a frequency-bearing row sample alongside the existing distinct
value samples. `CompressedHistogram` splits that sample into high-frequency
singleton values (MCVs) and approximately equal-depth residual buckets. MCV
mass is removed before residual buckets are built. A bucket range may straddle
an MCV, but does not contain its frequency. This follows the
[compressed histogram](http://mysql.taobao.org/monthly/2021/05/03/#compressed-histogram)
and [MCV plus residual histogram](https://www.postgresql.org/docs/18/row-estimation-examples.html)
model. It is not a distinct-value quantile sketch or an incremental TopN index.

## Collection and ownership

The core crate owns the storage-neutral distribution and arithmetic. Storage
owns admission, sampling, eligibility and publication. The embedded `hawdb`
facade re-exports the public histogram types; hosts need no internal crate.

Each admitted `(label, property)` or `(relationship type, property)` observes
every eligible row, including duplicates and explicit NULLs. Missing counts
are the source label/type population minus observed properties. Non-NULL rows
use Algorithm R reservoir sampling, with a fixed SplitMix64 stream and unbiased
bounded draws. Reservoir order is deterministic for a traversal, not identical
across different source traversals. External merge observes every fact before
distinct-value deduplication. Materialized, cooperative checkpoint and external
refresh paths use the same builder.

The builder retains at most 64 singleton/bucket entries. At most 16 high-frequency
singletons are selected when the sample is incomplete or has more than 64
distinct values. A complete low-NDV sample retains every distinct value as a
singleton, up to 64. Counts, sample size, NULLs and missing values remain explicit.
The existing distinct samples remain separate and never supply frequency weights.

Resource admission uses these limits:

| Resource | Limit and policy |
| --- | --- |
| Row reservoir | 256 non-NULL rows per target |
| Target requested-memory charge | 128 KiB, including vector capacity and six times retained sample/payload bytes for construction scratch |
| Materialized distribution collectors | 8 MiB each for nodes and relationships, including conservative key/map charges |
| Legacy materialized distinct sets | One monotone 8 MiB admission budget shared by node/relationship property groups |
| External refresh | Existing caller memory/spill/fact budgets; optional distribution uses remaining admitted state and may be omitted |
| Parameter-sensitive costing bindings | 256 scalar bindings and 64 KiB per parameter map; otherwise use generic estimates |

An oversized selected value or unsupported value excludes the whole distribution
target. New targets are not admitted after collector admission closes: collecting
only a suffix would misrepresent population frequencies. Distinct-set admission
likewise removes a refused target instead of publishing partial NDV. Its charges
are not reclaimed during the capture. Unsupported-type policy remains the
[statistics eligibility contract](specs/OPTIMIZER_STATISTICS_ELIGIBILITY_SPEC.md).

These are conservative requested-memory accounting limits, not allocator or RSS
guarantees. Existing path/joint statistics, canonical snapshot residency, source
scanning and checkpoint text serialization have separate resource costs. Use
external refresh for out-of-core hosts. This change does not make the full
materialized statistics collector constant-memory.

## Cardinality and CBO flow

One catalog adapter resolves bound values and supplies estimates to graph start
selection, access candidate ranking, residual filtering and final physical cost.
The optimizer catalog shares an `Arc<OptimizerCatalogStatistics>` snapshot.
Per-query parameter/type proofs and exact point overrides do not clone its
legacy value samples, NDV or path maps; explicit catalog mutation forks it.
Exact query-local point counts retain precedence. The shared raw CPU/random/
sequential/output cost contract and Cascades candidate search remain unchanged.
No coefficient is fitted from these samples.

- Equality uses an MCV frequency, then residual non-NULL mass divided by known
  population NDV minus MCV count. Sample NDV is never presented as population NDV.
  Without NDV, the conservative residual prior uses the non-NULL population.
- IN deduplicates bound values and sums disjoint equality estimates, capped at
  input rows. NULL and missing counts are excluded from ordinary comparisons.
- Ranges combine accepted MCV mass and interpolated residual bucket mass.
  Integer interpolation subtracts in i128 before converting to f64. Strings
  and other ordered nonnumeric values use a coarse midpoint within a bucket.
  Mixed runtime kinds and unsupported comparisons retain the legacy fallback.
- NULL tests use explicit NULL plus missing counts. Not-equal subtracts equality
  and NULL mass. `<> NULL` retains the planning floor rather than assuming all
  non-NULL rows match.

Histogram counts are scaled from their source population to the current input
cardinality. Correlated predicates still need joint statistics; this marginal
distribution cannot discover their correlation. Existing joint-NDV estimates
keep precedence. Multi-hop expansion and optional-degree models still retain
their existing structural counts and NDV assumptions; this marginal histogram
is not a degree/fanout distribution. All graph planning estimates retain the minimum-one floor.
Neither zero histogram mass nor sample absence proves semantic emptiness,
uniqueness, authorization, or a safe execution-memory bound.

Parameter templates retain executable rebinding. A bounded marker-to-value map
feeds indexed equality planning for a single-node filter under projection,
sorting, limiting or distinct, including equality conjuncts. Only those predicate
slots participate; projection, ordering and unrelated parameters retain their
existing cache contract. Scoring, complex pipelines, IN and range templates
retain generic parameter planning in this first delivery. Literal predicates and
bound qualification/diagnostics still use their available histogram estimates.

The physical cache key includes the admitted bindings' statistical identity.
A node equality groups observed MCV values
by equal sample frequency: their costs are identical at every input cardinality,
so they can share a template while executable values and ACL scopes are rebound.
Estimates at the one-row planning floor also share one class, including across
unobserved values; that does not prove existence or absence. Other unobserved
values and shared multi-column markers retain exact statistical identity.
Hot and cold frequencies use separate physical variants. Oversized
maps use generic estimates; existing entry/byte cache limits
still apply. Statistics publication invalidates plans through the existing epoch
contract. Statistics usage decisions expose source, sample size and population.

## Refresh, deletion and persistence

Writes/deletes use the existing dirty-statistics/source-epoch lifecycle. There is
no signed decrement of a reservoir or MCV table. A fresh capture rebuilds the
sample from live rows; old readers retain their source snapshot. Stale advanced
statistics remain estimates according to the existing freshness policy, not an
exact current distribution. Publication is atomic with the existing refresh
path; refusal must not publish a partial refresh.

Checkpoints add `stat_property_distribution` and
`stat_rel_property_distribution` records with versioned JSON derived metadata.
Decode validates size, mass, ordering, bucket limits and scalar tags. Invalid
optional distributions are omitted; canonical recovery and legacy statistics
remain independent. Older checkpoints fall back without distributions. Older
engines need not understand the new records: this is a development checkpoint
extension, not a forward-compatible persisted production contract.

## Measurement

`query-lab freeze-graph-stats` freezes 20 graph cases: hot/cold equality, numeric
ranges including empty output, missing values, IN duplicates and not-equal, with
indexed and unindexed labels. `ce-run` obtains selected plan, raw costs and
operator actual counts from the same qualification execution on one pinned read
snapshot. Successful whitelisted queries have no LIMIT or existence shortcut.
Missing/uninvoked operator observations remain unobserved. Complete typed results
and independently enumerated result counts are checked separately.

The existing `cost`/`cost-check` rounds can replay this manifest. The graph adapter
bypasses the physical cache for every observation and reports whole-call latency;
it does not invent parse/bind/plan/execute timings or allocator peaks. Plan
fingerprints identify shape, with frozen parameters retained separately. This
qualifies neither alternative-plan regret nor hardware coefficients. Measure a
source-bound release baseline and candidate on the same manifest before claiming
latency improvement or a globally optimal plan.
