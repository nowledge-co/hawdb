# Branch catalog inspection consistency and scaling

Related issue: [#817](https://github.com/nowledge-co/hawdb/issues/817).
The consistency contract is in
[BRANCHING_STORAGE_SPEC.md](specs/BRANCHING_STORAGE_SPEC.md#selection-locks-and-transactions).

## Workload and evidence boundary

`benches/branch_catalog_inspection.rs` measures synthetic metadata catalogs with
1, 16, 256, 4,096, 16,384, 65,536, and 100,000 records. The two largest counts
also retain deleted records and their successful-create receipts for 50% and
90% of children, rounded down. The main record remains ready. A star gives
every child the same parent; a chain gives each child the preceding record as
its parent. The fixture does
not create a data runtime for every catalog record. It measures catalog
inspection, not branch admission, recovery, production Mem traffic, or power-loss
safety. Fixtures use the default `SyncOnEveryWrite` policy; the timed operations
are read-only inspection and validation.

Each case measures catalog validation, bounded decoding, and three embedded SQL
operations: a one-row page at the midpoint, exact name lookup, and exact UUID
lookup of the final ready record. Tombstone cases also inspect one retained
deleted UUID and assert its deleted state. Query parameters are bound, and
every query asserts its exact returned UUID and one-row result. The page and
lookup paths still read and validate the entire catalog; a one-row result does
not imply one-record I/O.
Each 100,000-record catalog contains 14,800,012 encoded bytes in both lineage
shapes and at all three retained-deletion fractions.

The codec enforces two independent limits: 100,000 records (`MAX_BRANCHES`)
and 16 MiB of encoded catalog bytes (`MAX_CATALOG_BYTES`). This matrix reaches
the record-count limit with short synthetic fields: each child has a 13-byte
name, a 14-byte request key, and no owner. The 100,000-record fixture averages
approximately 148 encoded bytes per record and uses about 88.2% of the byte
budget. Longer names, request keys, or owners can exhaust the byte budget
before the record-count limit. Their respective field limits are 128, 256,
and 256 bytes; a near-byte-limit fixture with longer fields has not been
measured here. These results do not qualify the 16 MiB byte ceiling or
representative host metadata sizes.

The v2 fixture starts from an ordinary-open project's actual catalog. It keeps
the project/main UUIDs and published main root digest, appends synthetic
metadata records, sorts by UUID, and revalidates the published catalog against
the project selector before timing. Child data runtimes and physical child
artifacts are not created. Names and request keys are unique, and deleted
records keep their parent identities and successful-create receipts.

The release benchmark uses three warmups and 31 measured samples per operation.
Raw sample order and P50/P95/P99 are retained. The debug smoke uses 1 and 16
records, including the two retained-deletion fractions at 16 records, one
warmup, and two measured samples. It executes eight cases. Both Cargo and the
manual Bazel target execute the same source:

```console
cargo test --locked --bench branch_catalog_inspection
cargo bench --locked --bench branch_catalog_inspection
bazel run //:hawdb_bench_branch_catalog_inspection
```

The Bazel target is outside the routine benchmark smoke dispatch. It does not
add a CI job or change fuzz scheduling.

## Historical v1 comparison protocol

The preserved v1 comparison uses the earlier 1-through-16,384-record fixture,
ten cases per process, and no deleted records. Its synthetic project/main
identities and bootstrap-root fields differ from v2. Its results below remain
an independent Linux comparison, with the original raw artifact unchanged.

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

## Historical v1 results

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

## Retained catalog qualification (v2)

The [v2 raw cohort](BRANCH_CATALOG_RETENTION_BENCHMARK.json) retains three
complete release processes on macOS arm64, with 22 cases per process and
10,974 measured operations. Every process validates the full case set, actual
ready/deleted/receipt counts, encoded lengths, query identities and states,
sample counts, and recomputed nearest-rank P50/P95/P99. The eight-case Cargo
and manual Bazel debug smokes each retain 88 measured operations as well.

The source is based directly on main
`199c2c11532845e0b3315ff5577469e3b5102fb2`, using pinned Rust 1.97.1, locked
dependencies, the default HawDB features, and the Cargo bench profile. The
artifact records the exact benchmark and release-binary digests. All checks
and the release build in this protocol finish before the three timed
processes; no compiler or test is started by the protocol during the cohort.
Warmups, all raw sample order, per-process CPU/resource reports, and host load,
paging and swap observations are retained.

Values below are the median of three per-process P50s in milliseconds, at
100,000 total retained records. The delete percentages apply to the 99,999
children, giving 49,999 or 89,999 tombstones; the main branch is always ready.

| Shape | Deleted children | Validate | Decode | One-row page | Exact name | Exact ready UUID | Exact deleted UUID |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| star | 0% | 14.917 | 23.722 | 26.440 | 26.392 | 26.176 | n/a |
| star | 50% | 8.917 | 17.927 | 20.218 | 20.419 | 20.148 | 20.146 |
| star | 90% | 5.204 | 14.319 | 16.192 | 16.031 | 15.884 | 16.009 |
| chain | 0% | 16.650 | 25.902 | 28.370 | 28.095 | 28.426 | n/a |
| chain | 50% | 11.953 | 20.905 | 22.984 | 23.513 | 23.418 | 23.580 |
| chain | 90% | 8.082 | 17.275 | 19.663 | 19.721 | 19.669 | 19.817 |

The validator inserts only non-deleted names into its uniqueness set. More
deleted records therefore reduce that part of validation, while the encoded
length and complete-catalog read/decode requirement stay fixed. Tombstones
and receipts continue to consume both the 100,000-record allowance and the
16 MiB encoded-byte budget.

This is a standalone warm-cache scaling observation. The host is not
isolated: its one-minute load falls from 16.25 to 5.79 during the cohort and
swap usage changes in the third process. The full-process maximum resident
set reports are 300,990,464, 363,724,800 and 445,513,728 bytes, including fixture
creation and all 22 cases. These do not establish a per-query memory bound or
a production latency gate. The historical Linux v1 comparison and this macOS
v2 cohort use different hosts and fixture identities; no runtime speedup is
inferred between them.

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
inspection rates, or latency budgets. The v2 matrix adds evidence for retained
receipts at the record-count limit with short synthetic fields. Representative
host measurements, including encoded-byte growth and when the byte cap blocks
new creates, remain necessary before declaring catalog inspection
production-qualified or deciding that a long-lived cache or persistent lookup
index is warranted. Retained tombstones
still consume both catalog budgets. This change does not alter
receipt retention, branch existence, garbage collection, or deferred ordinary
project admission.
