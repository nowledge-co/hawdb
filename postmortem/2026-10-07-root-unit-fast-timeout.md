# Root unit-fast target deadline

Tracking: https://github.com/nowledge-co/hawdb/issues/870.
Containing Cypher change: https://github.com/nowledge-co/hawdb/pull/868.

## Observation

On macOS arm64, Apple M3 Max, 16 logical CPUs and 128 GiB RAM, the root
`//:hawdb_unit_fast_tests` target exceeded its implicit `size = "large"`
900-second deadline in three default-configuration executions: a broad aggregate
and two root-only reruns. The final containing revision was
`171cb621b16efe8596e5fa91e0541b9f0ffc3062`, with pinned Rust 1.97.1.

```console
bazel test //:hawdb_unit_fast_tests --nocache_test_results --test_output=errors
```

Latest terminal evidence:

```text
INFO: Elapsed time: 909.707s, Critical Path: 900.90s
//:hawdb_unit_fast_tests TIMEOUT in 900.1s
Executed 1 out of 1 test: 1 fails locally.
```

No assertion failure was reported before timeout. The binary listed 1,697 tests;
1,689 names completed or were ignored. Five remaining names were intentionally
excluded by the target because the crash matrix and row-page compaction have
separate targets. Three selected cases remained unfinished:

- `api::tests::concurrent_transactions::crash_recovery::subprocess_concurrent_crash_recovers_mixed_transactions_as_serial_prefixes`
- `api::tests::concurrent_transactions::relational_mvcc::relational_mvcc_constrained_inserts_validate_unique_values_without_serializing_foreign_keys`
- `api::tests::concurrent_transactions::relational_mvcc::relational_mvcc_constraint_preserving_updates_keep_disjoint_rows_and_barriers`

An earlier focused selection of the crash-prefix and constraint-preserving
updates cases passed in 559.86 seconds. That does not prove aggregate completion.
All four exact-head Prow build/crate/root/recovery checks passed; artifact logs
returned HTTP 403, so CI case durations remain unverified.

## Cause and mitigation boundary

The expired limit is a Bazel test-target deadline, not a build, job or runner
deadline. Resource contention remains a hypothesis; no complete run above 900
seconds or hang cause has been established. Physical storage and complete
concurrent host workload are unknown. Fixtures belong to tests and no user
database was opened.

The independent mitigation makes only this target's timeout explicit at
`eternal` (3,600 seconds), the next standard finite Bazel deadline above the
exhausted 900 seconds. It preserves test selection, assertions, features,
fixtures, retry policy, aggregate labels, .bazelrc and unrelated deadlines.
This is temporary measurement headroom, not evidence of a performance fix.

## Follow-up and close condition

Owner: hawkingrei. Under the
[Bazel timeout policy](https://github.com/nowledge-co/mem/blob/main/docs/BAZEL_TIMEOUT.md),
open this focused mitigation before decomposing the suite. Then record complete
per-case durations on a containing revision and split/rebalance by cohesive
capability and observed runtime. Preserve all cases and the public label and
verify macOS and available CI separately. Do not raise the deadline again in
place of decomposition. Revisit the temporary timeout once the split lands.

Issue #870 remains open until terminal qualification and the measured
decomposition are verified. Current source/CI/focused evidence does not turn the
recorded local aggregate timeouts into passes.
