# WAL group-commit benchmark diagnostics

The WAL group-commit smoke retains eleven alternating baseline/candidate pairs
for each of four workload families: concurrent, single-writer, staggered cold
start and staggered steady state. These are 88 independent measurements. Worker
counts, commit counts, warmup, coalescing policies and statistical admission
remain defined in `benches/wal_group_commit.rs`.

## Progress and timing boundaries

The benchmark writes progress to stderr, which the existing smoke harness leaves
visible. Each event identifies the workload family, paired round, policy and
phase. Started/completed events distinguish database open, schema creation,
warmup, measured execution, observation, close, recovery open/order/rows/close
and cleanup. Completed events include elapsed microseconds for that phase.

No progress output runs between the measured execution's `Instant::now()` and
its elapsed-time capture, or within any per-commit timing interval. The separately
reported phase elapsed time is diagnostic; it is not substituted for the measured
elapsed time in qualification. The post-execution counter line reports submitted
and completed grouped commits, shared syncs and fsync microseconds. Disabled
grouping does not populate those group-coordinator counters, so zero there is
not evidence that baseline commits were omitted.

The existing single `wal_group_commit` JSON stdout line, field names, paired
measurements and admission/rejection reporting remain unchanged. A process exit
of zero only means the benchmark completed. Inspect `evidence_admitted` and
`evidence_rejection` separately before making a performance-qualification claim.

An unfinished phase identifies where observation stopped, not why. For example,
an unfinished `measurement` may reflect storage latency, scheduling contention
or a stalled worker/coordinator. Capture the live process's thread stacks and
I/O/scheduling state in that environment before attributing the cause. Successful
local measurements cannot explain an intermittent remote Linux timeout.

## One recovery open, independent proofs

After all writers join and the database closes, one default strict reopen now
supplies both proofs. Before any query, its original recovery report must show
LSN 1, the exact final next LSN, the exact replay count and no torn tail. The same
handle then checks the final epoch and every ordered ID/body, including warmup
rows. This retains the original row-count/epoch proof and strengthens it against
same-count row or payload replacement. No checkpoint or repair is inserted
between the two proofs.

The handle closes before cleanup. A failed open rejects both proofs; a failed
row query rejects the row proof and retains the independently evaluated WAL
proof. A checkpointed database may have correct rows while failing the required
full-WAL replay boundary; one proof cannot substitute for the other.

## Verification

The shared helper is compiled by the real benchmark and an integration-test
consumer. Regressions cover the previous two-open result, warmup rows, wrong and
overflowing epochs, missing/extra/replaced rows, wrong payloads, checkpoint-only
state and a torn WAL. The ordinary regression target participates in the existing
smoke suite; it is not a fuzz campaign.

```sh
cargo test --locked --test wal_group_commit_recovery
cargo clippy --locked --all-features --bench wal_group_commit \
  --test wal_group_commit_recovery -- -D warnings
bazel test //:hawdb_wal_group_commit_recovery_tests \
  //:hawdb_linux_ci_wal_group_commit_smoke_test
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests \
  //:hawdb_linux_ci_fuzz_smoke_test
```

Issue #442 remains an intermittent-timeout investigation until repeated
exact-revision Linux CI and phase/process evidence support its acceptance
criteria. Sharing recovery avoids redundant work; it does not establish that
the redundant work caused the reported timeout.
