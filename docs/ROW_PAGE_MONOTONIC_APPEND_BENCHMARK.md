# RowPage Monotonic Append Benchmark

`cargo bench --bench relational_monotonic_append` compares the explicit
RowPage monotonic-append candidate with the default disabled path on the same
embedded Skein runtime. The benchmark is an admission and latency gate, not a
planner-only microbenchmark.

## Activation Boundary

`DatabaseConfig::relational_monotonic_append_fast_path` is `false` by default.
Enabling it does not change the durable format, WAL, or SQL contract. The
candidate is attempted only for multi-row `INSERT ... Error` batches that:

- are strictly increasing within each primary-key prefix partition;
- do not mix other mutation kinds for the same table;
- do not require unique-constraint or foreign-key probes; and
- run against a pinned metadata-only RowPage snapshot.

Recovery deltas, live rows at or after the first candidate key, ambiguous page
boundaries, single-row batches, duplicate keys, and out-of-order batches use
ordinary point hydration. The metadata proof itself is read-only; state and WAL
publication still occur only after complete transaction validation succeeds.

## Workload

- The database is reopened in `OutOfCore` residency with authoritative
  relational indexes.
- A composite primary key provides a generic partition prefix and ordered
  suffix. No application table or route is recognized by the kernel.
- The checkpoint contains lower and upper neighboring partitions around an
  initially empty target partition.
- Candidate and baseline commit identical ascending batches. They differ only
  in the typed fast-path switch.
- `SyncOnCheckpoint` isolates row hydration and authoritative constraint
  staging from per-transaction fsync latency.
- Candidate and baseline operations alternate to reduce ordering bias.
- Release runs collect 31 samples after three warmups for batch sizes 1, 8,
  and 32. The largest batch stays within the disabled baseline's bounded
  authoritative-index read budget, so both sides commit the identical workload
  and latency remains directly comparable.

The release gate requires every batch to commit, candidate p50 to beat the
disabled baseline for multi-row batches, and enabled single-row p95 to remain
within 5% of the disabled baseline. Output includes p50, p95, p99, p50 rows/s,
speedup, and physical-path counters.

## Qualification Result

The 2026-08-20 release run on arm64 macOS 26.6.2 with Rust 1.97.1 used 8,192
seed rows per neighboring partition and passed every gate:

| Batch | Candidate p50/p95/p99 | Baseline p50/p95/p99 | Candidate rows/s | p50 speedup |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 323.6/366.5/367.5 us | 311.6/387.4/406.6 us | 3,090 | 0.96x |
| 8 | 116.5/134.5/145.3 us | 2,197.8/2,254.6/2,259.0 us | 68,694 | 18.87x |
| 32 | 328.2/391.7/396.0 us | 9,312.0/9,531.3/9,702.2 us | 97,499 | 28.37x |

The single-row p95 changed by -5.38%, within the no-regression gate. Every
measured multi-row candidate attempt hit the metadata proof with zero fallback:
34/34 for each batch size, including warmups. The disabled baseline recorded no
attempts.

## Correctness Evidence

Targeted tests prove:

- the feature remains disabled unless the typed configuration enables it;
- enabled and fallback paths produce the same relational state;
- descriptor/live-overlay absence proof performs zero row-page reads;
- duplicate and out-of-order writes preserve normal validation;
- unique and foreign-key tables remain on point hydration; and
- a rejected transaction advances neither commit epoch nor WAL LSN.

## Observability

`RelationalRowStorageResidencyReport` and the resource-profile JSON expose
cumulative:

- `monotonic_append_attempts`
- `monotonic_append_hits`
- `monotonic_append_fallbacks`
- `monotonic_append_proven_absent_primary_keys`

These counters record the physical path decision even when a later constraint
rejects the transaction. Canonical state and WAL still advance only after the
complete transaction succeeds.
