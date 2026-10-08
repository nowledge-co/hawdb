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
1,689 names completed or were ignored. Two remaining names were intentionally
excluded: the crash matrix and the compaction convergence case have separate
targets. Six selected cases remained unfinished:

- `api::tests::concurrent_transactions::crash_recovery::subprocess_concurrent_crash_recovers_mixed_transactions_as_serial_prefixes`
- `api::tests::concurrent_transactions::relational_mvcc::relational_mvcc_constrained_inserts_validate_unique_values_without_serializing_foreign_keys`
- `api::tests::concurrent_transactions::relational_mvcc::relational_mvcc_constraint_preserving_updates_keep_disjoint_rows_and_barriers`
- `store_facade_tests::row_page_compaction::row_page_compaction_admits_before_building_and_shares_shadow_capacity`
- `store_facade_tests::row_page_compaction::row_page_compaction_dirty_and_materialized_limits_release_admission`
- `store_facade_tests::row_page_compaction::row_page_compaction_failure_limits_leave_the_generation_retryable`

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

## Complete containing-main measurement and decomposition

At main `51315823800aaaf63d0be36e7a00aab1ec9906de`, the unchanged default binary
lists 1,775 cases. Its existing two isolated exclusions leave 1,773 selected
cases: an uncached default Bazel run completes with 1,767 passes, six existing
ignored campaigns, and no failures. The BEP reports 2,090.708 seconds while
libtest reports 579.34 seconds. Host power logs independently show clamshell
sleep and subsequent maintenance sleeps during that run. Preserve both timing
domains; do not present the epoch duration as active computation or claim that
this run qualifies the old 900-second declared target.

A separate stable-libtest serial diagnostic completes the same 1,773-case
inventory in 1,368.066 seconds. Prefix/completion pipe receipt times approximate
individual active durations; fast cases may share one receipt. Three crash
parents interleave child stdout. Their raw early receipts are retained, their
outcomes are checked against the complete parent output, and their completion
upper bounds use the next parent prefix receipt. The audited parent outcomes
match the terminal libtest summary exactly. No nightly timing flags or test
workload changes are needed.

The measured boundaries are:

| Public child target | Selected cases | Observed serial seconds | Declared seconds |
| --- | ---: | ---: | ---: |
| `hawdb_unit_general_tests` | 1,459 | 441.966 | 900 |
| `hawdb_unit_concurrent_tests` | 113 | 478.438 | 900 |
| `hawdb_unit_analytics_tests` | 58 | 174.570 | 900 |
| `hawdb_unit_recovery_tests` | 140 | 250.040 | 900 |
| `hawdb_unit_row_page_compaction_admission_tests` | 3 | 23.022 | 900 |

The original public `hawdb_unit_fast_tests` label becomes their aggregate. The
complete selected inventory is disjoint and byte-for-byte equal by case name
to the containing revision. The independently registered crash matrix and
full compaction convergence workload remain in `hawdb_unit_tests`. The latter
retains its existing 3,600-second deadline under #889. Retry policy, features,
fixtures and assertions are preserved. Caller filters and `--exact` intersect
with the ownership boundary through complementary complete-name skips; adding
a positive filter cannot select another partition through libtest's OR rule.

Compaction moves unchanged into a native Cargo integration test and independently
compiled Bazel targets with four source inputs and explicit facade/core/QoS/storage
dependencies. The shared path fixture is extracted once, and the optional
test-support failpoint case remains discoverable under its original full name.

The four private unit partitions intentionally share one compiled executable.
The concurrent fixture requires crate-private transaction workspace state and
enqueue/read barriers; exporting those hooks solely for testing would change
the library contract. This split bounds executions and reuses the common binary;
it does not reduce compilation of those private unit owners. Compilation-boundary
follow-up owner: hawkingrei. Next step: factor private fixtures and qualify scoped
in-crate test roots before changing these owners' compilation inputs, preserving
their complete inventories without a new public hook API or facade crate.

Local default Bazel changed from 9.2.0 for the baseline to 9.3.0 during candidate
qualification. Repository configuration and pinned Rust 1.97.1 are unchanged.
No cross-version speedup or unobserved Linux/Windows duration is claimed. The
serial table informs the split; actual uncached child execution and exact-head
CI remain separate qualification requirements. Full #870 stays open until
those delivery receipts are verified.

The candidate's complete uncached macOS run passes all eight owning targets:
the five aggregate children, both existing isolated cases, and the partition
runner regression. The seven Rust owners execute the complete original 1,775
case names exactly once (1,769 passed, six existing ignored). Actual BEP target
durations are 452.312 seconds for general, 670.771 for concurrency, 394.191 for
analytics, 514.039 for recovery, 127.149 for compaction admission, 570.484 for
compaction convergence, and 336.164 for the crash matrix. The largest aggregate
child is 670.771 seconds, below its declared 900 seconds. This is a terminal
local receipt, not exact-head remote CI or main delivery.

Native macOS/Windows CI's existing ACL compaction step now selects the
`row_page_compaction` integration target through `cargo-test-required.sh`.
Keeping its former `--lib row_page_compaction` filter would select no cases
after the move. The same four default cases and feature selection are retained;
the existing guard rejects an empty discovery or execution. No CI lane or job
deadline is added or changed.
