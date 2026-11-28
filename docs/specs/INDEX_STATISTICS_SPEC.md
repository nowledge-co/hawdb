# Index Statistics Specification

## Status

This document defines the v1 optimizer-statistics contract for explicit graph
property indexes. It covers equality, range, and composite node indexes,
checkpoint persistence, WAL churn accounting, optimizer admission, and system
catalog observability. Full-text token distributions and relationship-property
indexes remain outside this contract.

## Motivation

Generic property statistics and index selectivity have different resource
boundaries. Generic statistics may collect bounded histograms for compact
scalar and `VARCHAR` values, but declared `TEXT`, list, map, and mixed groups
are excluded. An explicit index already owns the cost of maintaining its keys,
so HawDB may publish payload-free cardinality metadata for that index without
copying indexed values into a second resident statistics structure.

This follows the useful part of Neo4j's split between exact structural counts
and index-local samples. HawDB does not require a universal `ANALYZE` scan to
make an explicit equality or composite index costable.

## Sample contract

Each supported `IndexId` may own one `IndexStatisticsSample` containing:

- `index_size`: indexed row references at the sample epoch
- `unique_values`: distinct scalar or composite keys observed in the sample
- `sample_size`: row references represented by the sample
- `updates_since_sample`: index-key mutations applied after the sample epoch

The resident sample MUST NOT retain `Value`, encoded property payload, posting
lists, histograms, or canonical rows. Memory is therefore proportional to the
number of explicit indexes, not the number or size of indexed values.

An exact sample has `sample_size == index_size`. A sampled implementation MAY
use `sample_size < index_size`, but MUST preserve
`unique_values <= sample_size <= index_size`. Empty indexes retain a zero
sample for observability but do not provide an optimizer selectivity estimate.

Equality and range descriptors over the same `(label, property)` may share one
physical ordered index, but their `IndexId` values and samples remain distinct.
Composite samples count complete composite keys; they MUST NOT multiply
independent per-property NDVs when a joint sample is available. Index IDs are
unique across scalar and composite descriptor kinds.

## Collection and mutation

Materialized stores derive exact samples from already-maintained ordered index
keys and posting cardinalities. Collection MUST NOT clone key payloads. Scalar
range declarations backfill and maintain the same ordered value index used by
equality declarations so the sample and range access path describe complete
data.

Live index DDL against an already out-of-core canonical base does not publish a
sample from the resident WAL delta alone. The descriptor remains usable through
the executor's authoritative fallback, but sample publication waits for a
complete rebuild or the bounded external statistics refresh.

An out-of-core checkpoint publishes exact index samples before releasing the
materialized index payload. WAL replay and live mutations do not rewrite the
sampled size or NDV. They increment `updates_since_sample` once for every
supported index whose indexed key changes. A composite update is counted only
when its complete joint key changes or enters/leaves the index.
Mutation accounting computes only the affected `IndexId` values before applying
the write; it MUST NOT clone the old or new canonical row to compare keys.

External advanced-statistics refresh rebuilds every supported scalar and
composite sample from its pinned canonical scan. Index keys use the same
byte-bounded external sort as other statistics facts, including keys for
declared `TEXT` that remain excluded from generic histograms. Index sampling is
charged to `memory_budget_bytes`, `max_generated_facts`, `max_spill_bytes`, and
`max_spill_runs`; one key that cannot fit the memory budget rejects the refresh
before publication. Resident descriptor lookup state and published samples stay
proportional to the number of explicit indexes.

The refresh publishes only if its source commit epoch is still current. A
budget, I/O, decode, or source-epoch failure leaves the prior sample and churn
counters unchanged. Successful publication atomically replaces every supported
sample with an exact current sample and resets `updates_since_sample` to zero.

## Background maintenance

The embedded facade exposes optimizer-statistics refresh as caller-owned
background work without spawning an internal worker. Candidate detection reads
only catalog and resident sample metadata. A candidate exists only for a
writable, durable, out-of-core store with at least one supported index whose
sample is missing or stale. Full-text descriptors remain excluded.

A writable, durable resident store above `SYNC_STATISTICS_MAX_RECORDS` (1,000
committed nodes plus relationships) is also a candidate when a supported sample
is missing or stale, its distributions are unavailable, or graph mutation
operations committed since the last refresh exceed `max(1000, 10% of records)`
(#2017). Its refresh recomputes from resident records under the same
`max_input_records` bound, publishes without a checkpoint (a resident
checkpoint recomputes statistics itself), and reports
`checkpoint_persisted = false`. A resident store at or below the bound
recomputes exact statistics for every plan and has no candidate. A resident
store without durable storage has no background task: the commit that crosses
the bound, and any commit after which dirty operations exceed the same
threshold, refreshes synchronously. Writes staged in an explicit transaction
count only when it commits. `StorageResidencyReport::optimizer_statistics`
reports the active path, commit lag, dirty operations and full recomputations
by caller site.

The candidate uses the `Projection` QoS lane. Its record allowance is the
current node plus relationship count, at least one so an empty indexed database
still publishes an exact zero sample. A refresh is one whole-graph maintenance
pass bounded by its own allowance (`max_input_records`, the refresh memory
budget), so its QoS charge is that record count clamped to the policy's
per-task cap (`max_background_operations`): the default policy admits a refresh
of any size as one full task slot without host configuration. Background
enablement and the total and per-class budgets still apply (#2017). When the
database has a runtime governor, the refresh also tries, without waiting, to
admit `memory_budget_bytes` as background maintenance memory; if that is
unavailable the refresh returns `None`, increments
`optimizer_statistics.background_refresh_skipped`, and a later pass retries. A
QoS refusal remains an error and increments `background_refresh_refused`. Each
refresh records its database write-guard hold time
(`last_refresh_write_guard_hold`, `max_refresh_write_guard_hold`). Missing sample
count and stale `updates_since_sample` contribute to the recent-delta hint; the
gap between the current commit epoch and published statistics epoch contributes
to the source-commit-lag hint. This estimate does not replace the refresh's hard
input-record, generated-fact, path-expansion, memory, spill-byte, and spill-run
budgets.

`background_maintenance_candidates` includes this work by default. Execution
still requires the caller to provide explicit `OptimizerStatisticsRefreshOptions`,
including a spill directory, and to drive either policy admission or the
scheduler permit lifecycle. A deferred or rejected candidate MUST NOT start a
canonical scan or create spill state. Scheduled execution MUST release its
permit after success or failure. The explicit direct refresh API remains
ungated for foreground administrative use.

## Optimizer admission

The optimizer admits a valid non-empty sample while:

```text
updates_since_sample <= max(1, ceil(index_size * 5 / 100))
```

For a sampled index, estimated NDV is
`ceil(unique_values * index_size / sample_size)`, clamped to
`[1, index_size]`. Equality cardinality uses
`ceil(index_size / estimated_ndv)`. Composite seeks use joint index size and
joint NDV.

Once churn exceeds the threshold, the optimizer MUST ignore the index sample
and use the existing generic-statistics or conservative fallback. Staleness
may change plan quality but never index completeness or query correctness.

## Persistence and recovery

Checkpoint records persist samples as derived metadata. Recovery validates the
numeric bounds and removes samples for missing, full-text, or unsupported index
descriptors. Invalid samples are not canonical corruption: the index remains
queryable and the optimizer falls back conservatively.

Checkpoint loading MUST NOT count checkpoint rows as post-sample updates. WAL
records after the checkpoint do increment churn, so reopen reconstructs the
same admission decision as uninterrupted execution.

## Observability

`system.graph_statistics` exposes one `index_sample` row per supported sample,
including index identity, kind, property names, all four counters, and the
derived `stale` flag. Explain traces continue to report the distinct count used
for access-path costing.

## Verification

Required regressions cover:

- scalar and composite descriptor IDs never collide
- exact and sampled NDV estimates preserve numeric bounds
- declared `TEXT` remains absent from generic property statistics while an
  explicit equality index publishes payload-free selectivity
- range declarations backfill the shared ordered value index
- composite samples use joint-key NDV
- out-of-core checkpoint plus WAL replay reconstructs churn and rejects a
  stale sample
- bounded external refresh creates samples for live out-of-core DDL and resets
  stale sample churn without publishing partial state on failure
- metadata-only background detection surfaces only missing or stale
  out-of-core samples; QoS defer performs no scan, scheduled failure releases
  its permit, and successful refresh removes the candidate
- checkpoint text and `system.graph_statistics` expose the sample fields
- `HawDBIndexStatistics.tla` checks coherent resampling, exact zero-churn
  samples, update-age accounting, the bounded optimizer admission rule, and
  background permit ownership while direct refresh remains ungated
