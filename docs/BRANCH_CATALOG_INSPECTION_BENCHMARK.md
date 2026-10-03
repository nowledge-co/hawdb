# Branch catalog inspection consistency and scaling

Related issue: [#817](https://github.com/nowledge-co/hawdb/issues/817).
The consistency contract is in
[BRANCHING_STORAGE_SPEC.md](specs/BRANCHING_STORAGE_SPEC.md#selection-locks-and-transactions).

## Workload and evidence boundary

`benches/branch_catalog_inspection.rs` measures synthetic metadata catalogs with
1, 16, 256, 4,096, and 16,384 records. A star gives every child the same parent;
a chain gives each child the preceding record as its parent. The fixture does
not create a data runtime for every catalog record. It measures catalog
inspection, not branch admission, recovery, production Mem traffic, or power-loss
safety. Fixtures use the default `SyncOnEveryWrite` policy; the timed operations
are read-only inspection and validation.

Each case measures catalog validation, bounded decoding, and three embedded SQL
operations: a one-row page at the midpoint, exact name lookup, and exact UUID
lookup of the final record. Query parameters are bound, and every query asserts
its exact returned UUID and one-row result. The page and lookup paths still read
and validate the entire catalog; a one-row result does not imply one-record I/O.
The largest encoded catalog contains 2,424,812 bytes in both lineage shapes.

The optimized benchmark uses three warmups and 31 measured samples per operation.
Raw sample order and P50/P95/P99 are retained. The debug smoke uses 1 and 16
records, one warmup, and two measured samples. Both Cargo and the manual Bazel
target execute the same source:

```console
cargo test --locked --bench branch_catalog_inspection
cargo bench --locked --bench branch_catalog_inspection
bazel run //:hawdb_bench_branch_catalog_inspection
```

The Bazel target is outside the routine benchmark smoke dispatch. It does not
add a CI job or change fuzz scheduling.

## Comparison protocol

The baseline runtime is `804f1a6139f73470c58167411f6cad871efffdbf`.
The candidate changes catalog validation to sort UUID values instead of cloning
complete records and binary-search that same temporary array for every parent.
It preserves the existing UUID uniqueness, parent closure, checksum, bounds,
publication, and canonical encoding checks. No catalog cache, persistent index,
format change, or new host API is introduced.

Both binaries use pinned Rust 1.97.1, the Cargo bench profile, the same default
features and identical benchmark source. The preserved binary hashes and
candidate runtime patch digest are recorded with the raw evidence. An initial
baseline probe established the lineage-dependent cost before the implementation
change; it is retained separately from the paired comparison.

The full paired order is `B1, C1, C2, B2, B3, C3` on Linux x86_64, after local
builds and tests finish. Each process runs all ten cases. These are warm-cache
measurements without CPU affinity, frequency control, or a cold-device claim.
The comparison validates all sample counts, recomputes percentiles, and checks
that every run has identical case identities, catalog lengths, and result-row
counts. The source assertions check query result identity during execution.

## Results

The [raw comparison](BRANCH_CATALOG_INSPECTION_BENCHMARK.json) retains all six
complete processes (9,300 measured operations), their raw samples, the initial
baseline probe, binary/source digests, and host observations. Every process
completed successfully and all case, sample, percentile, and result-row checks
passed. Values below are the median of three per-process P50s, in milliseconds;
each cell is baseline to candidate. P95/P99 and every paired change remain in
the raw artifact.

| Shape | Records | Validate | Decode | One-row page | Exact name | Exact UUID |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| star | 1 | 8e-05 → 6e-05 | 0.000211 → 0.000191 | 0.01251 → 0.01026 | 0.01449 → 0.009939 | 0.01657 → 0.01005 |
| star | 16 | 0.002204 → 0.001393 | 0.004679 → 0.004027 | 0.01819 → 0.01483 | 0.01731 → 0.01444 | 0.01934 → 0.01475 |
| star | 256 | 0.04516 → 0.03804 | 0.08561 → 0.0803 | 0.1308 → 0.1051 | 0.1296 → 0.1078 | 0.1292 → 0.1074 |
| star | 4,096 | 0.9593 → 0.8456 | 1.571 → 1.568 | 1.834 → 1.78 | 1.832 → 1.753 | 1.808 → 1.749 |
| star | 16,384 | 4.351 → 4.106 | 7.034 → 6.593 | 8.136 → 7.24 | 8.001 → 7.445 | 8.135 → 7.273 |
| chain | 1 | 8e-05 → 6e-05 | 0.00021 → 0.00021 | 0.01048 → 0.01044 | 0.009969 → 0.009988 | 0.009909 → 0.009849 |
| chain | 16 | 0.002234 → 0.001482 | 0.004598 → 0.004068 | 0.01559 → 0.01473 | 0.01508 → 0.01447 | 0.01511 → 0.01474 |
| chain | 256 | 0.06262 → 0.03998 | 0.1024 → 0.08379 | 0.1281 → 0.1071 | 0.1258 → 0.1072 | 0.1271 → 0.1091 |
| chain | 4,096 | 5.586 → 0.8991 | 11.5 → 1.572 | 6.655 → 1.737 | 6.823 → 1.742 | 6.613 → 1.74 |
| chain | 16,384 | 126.6 → 4.084 | 135 → 6.57 | 134.8 → 7.936 | 134 → 8.019 | 134.4 → 7.973 |

At 16,384 chain records, validation fell from 126.618 ms to 4.084 ms;
all three paired changes were between -96.3% and -97.2%. The three SQL paths
fell from roughly 134 ms to roughly 8 ms. The smaller 4,096-record chain also
improved in every pair. This supports removing repeated parent scans.

The host was not isolated. A desktop content indexer resumed during preceding
test verification, and system swap-in/out counters changed during the cohort.
The artifact records CPU ticks, available memory, paging counters and load
averages around every process. No local build or test overlapped measurement,
and no background service was modified. Small-case and star differences must
therefore be treated as noisy observations: the first 16,384-record star pair
regressed by 6.1%-12.8% across the five operations, while the next two pairs
improved. Some 1/16-record SQL pairs also regressed. This is not a universal
latency improvement claim or a passed production performance gate.

## Decision and remaining qualification

Keep complete, freshly validated catalog reads for each inspection statement.
The temporary UUID array removes lineage-dependent quadratic parent scanning
and avoids cloning record payloads solely for validation. It is released with
each validation; it has no cross-statement invalidation or recovery contract.
Decoding and returning metadata still scale with the full catalog, and `LIMIT`
continues to bound output rather than catalog work.

The correctness tests cover unsorted lineage, missing parents and duplicate
UUIDs; an open reader finishing the old catalog after atomic replacement; newer
catalog publications becoming visible after a data snapshot and within explicit
read/write transactions; and target metadata changing after admission recovery.
The last case rejects stale admission, releases target resources, preserves a
usable source, and accepts a retry against the newly observed target revision.
These tests do not establish power-loss behavior.

This synthetic matrix does not establish representative Mem branch counts,
inspection rates, or latency budgets. Those host measurements remain necessary
before declaring catalog inspection production-qualified or deciding that a
long-lived cache or persistent lookup index is warranted. Retained tombstones
still count toward the 100,000-record codec limit. This change does not alter
receipt retention, branch existence, garbage collection, or deferred ordinary
project admission.
