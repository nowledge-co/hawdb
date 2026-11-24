# Foreground latency investigation and prepared-WAL follow-up

Issue Number: ref #1960. PR: https://github.com/nowledge-co/hawdb/pull/1961.

## Outcome and acceptance boundary

A new complete operation-array walk was confirmed in the source and removed
from pre-encoded graph append. Phase telemetry and a sampled call graph locate
synchronous persistence as a substantial wall-time component, while bulk
relationship updates also have substantial CPU work. These findings do not
establish the cause of every previous regression or qualify the follow-up as
unchanged in foreground/tail performance. The PR remains Draft.

The previous 43.721 ms maximum has no phase counters or stack capture. It cannot
be retrospectively attributed to the WAL, scheduler or this operation scan.
The original unfavorable sample remains in the acceptance dataset.

## Reproducible source finding and attempted repair

`append_durable_wal_batch_prepared` previously called both `changes_graph` and
`changes_adjacency` over the operation array. For an all-node-property batch,
`changes_graph` stops at its first graph operation, while `changes_adjacency`
must visit every operation to establish that no relationship/topology/schema
change exists. The new full walk also happened when immutable WAL bytes had
already been prepared.

At `6a378114156967f45972b49d0bc29ee25ed7874b`, `PreparedWalBatch::prepare` retains
adjacency impact during its existing encoding loop, short-circuiting further
classification after the first invalidating operation. The serialized append
consumes that cached flag. Classification still scales with operation count
during preparation; no separate late array walk or new allocation is needed.
The checked count now uses its encoded u32 domain, preserving the 32-byte owner
on 64-bit targets. Ordinary/mixed append and WAL replay keep their conservative
classification. Pending invalidation is still installed only after successful
append and consumed at the actual commit boundary. WAL bytes and all
synchronization barriers are unchanged.

Storage regressions cover empty, 128-node-property, mixed-marker and relationship
batches in both orders, plus exact prepared/ordinary WAL byte parity. This is
an algorithmic/correctness repair of unnecessary work, not measured proof that
it explains or removes the earlier 0.8%-2.1% foreground differences.

## Diagnostic protocol and limitations

The manual embedded example's optional seventh argument `cpu-profile` reads
`RUSAGE_SELF` before and after the mutation and checkpoint wall timers, and
installs the existing typed WAL telemetry sink. Counters retain user/system CPU,
faults, context switches, block activity, WAL elapsed/flush time, bytes and
append count. Every mutation had one WAL append; checkpoints had none.
Counters are process-wide, not isolated-thread CPU. Snapshot serialization and
telemetry have observer cost; CPU snapshots include a small amount of code
outside the wall timer. CPU overlaps fsync's wall interval and cannot simply be
added to it. Independently computed medians do not form an additive breakdown.
Wall-minus-CPU is not an I/O-only measurement.

Two complete diagnostic suites used 18 immediate alternating operation pairs
per group, two warmups and all four workload/size groups. The [exact controller](run_profile.py)
and [analyzer](analyze_profile.py) preserve all samples. Values/counts,
carry/rebuild behavior, scrub and reopen passed in every group. Both runs were
explicitly under host compilation/desktop load, including the local fixed
release build during the earlier suite. They are not idle-host acceptance runs.
There is no cross-suite causal speedup claim: even the common baseline's CPU
and wall times changed between windows. [Source and harness identities](profile-source-metadata.json)
record the frozen pre-fix harness and the final harness's portable no-op change;
do not describe their source bytes as identical. The macOS profiling behavior
and workloads are unchanged. The isolated fixed release rebuild took 8m40s;
compilation cost is separate from execution.

## Before the follow-up: source 990f87b56 plus the frozen profiling harness

| Nodes / relationships | Mutation | Baseline wall / CPU / WAL fsync (ms) | Candidate wall / CPU / WAL fsync (ms) |
|---|---|---:|---:|
| 128 / 512 | node_property | 4.220 / 0.838 / 3.513 | 4.218 / 0.847 / 3.446 |
| 128 / 512 | relationship_property | 6.262 / 2.635 / 3.720 | 6.109 / 2.507 / 3.696 |
| 512 / 4096 | node_property | 5.750 / 2.326 / 3.573 | 6.188 / 2.350 / 3.895 |
| 512 / 4096 | relationship_property | 24.175 / 20.540 / 3.703 | 24.896 / 20.970 / 3.784 |

[Raw records, per-group binary hashes and successful exit receipts](profile-prefixed-loaded/run-receipts.json)
and [complete phase distributions](profile-prefixed-loaded/profile-summary.json)
are retained. In this window, candidate node-property checkpoint CPU medians
were 51.113 / 59.642 ms, versus wall medians 404.454 / 408.003 ms.
For 4096 relationship updates, candidate mutation CPU was 20.970 ms, so the
small-fixture fsync-dominance finding does not apply to every workload.

## Follow-up: source 6a3781141

| Nodes / relationships | Mutation | Baseline wall / CPU / WAL fsync (ms) | Candidate wall / CPU / WAL fsync (ms) |
|---|---|---:|---:|
| 128 / 512 | node_property | 4.309 / 0.730 / 3.668 | 4.278 / 0.782 / 3.517 |
| 128 / 512 | relationship_property | 6.005 / 2.232 / 3.884 | 6.006 / 2.253 / 3.840 |
| 512 / 4096 | node_property | 5.637 / 2.084 / 3.857 | 5.621 / 2.100 / 3.679 |
| 512 / 4096 | relationship_property | 18.475 / 15.018 / 3.721 | 18.747 / 14.768 / 4.050 |

[Raw records and receipts](profile-fixed-loaded/run-receipts.json) and
[complete phase distributions](profile-fixed-loaded/profile-summary.json)
retain the follow-up's controls and negatives. Its larger relationship-property
wall median still exceeds the concurrent baseline, while CPU is lower and WAL
fsync is higher. The smaller node-property CPU median is also higher. Do not
hide those results or infer no-regression from checkpoint savings. Candidate
node-property checkpoint CPU medians were 45.099 / 53.981 ms, versus wall medians
451.365 / 392.608 ms. These counters help locate work but do not clear acceptance.

## Checkpoint stack evidence

A separate fixed-binary 512-node/4096-relationship worker was sampled for eight
seconds at a requested 1 ms interval, after fixture preparation. The
[exact script](sample_checkpoint.py) creates and samples only its own child.
[Receipt](checkpoint-stack-6a/receipt.json), [worker records](checkpoint-stack-6a/worker.jsonl),
[sampler output](checkpoint-stack-6a/sample.stderr) and the
[complete compressed call graph](checkpoint-stack-6a/candidate-sample.txt.gz)
are retained. The process and sampler both exited successfully. This is
under-load attribution, includes mutations and checkpoints, and is not a
phase-isolated CPU or latency qualification.

The main-thread call graph contains 5973 samples; 4853 top-of-stack samples
(~81.2%) are in `__fcntl`. Representative ancestor paths are:

```text
Database::checkpoint_internal
  GraphStore::publish_prepared_checkpoint_with_reclamation
    GraphStore::publish_admitted_branch_root
      publish_sealed_root
        CheckpointClosurePlan::publish_owned
          ImmutableObjectStore::install_staged
            sync_directory_with_context -> File::synchronize -> fcntl
        CheckpointClosurePlan::publish_owned
          File::synchronize -> fcntl
```

The first directory-synchronization path accounts for 902 sampled leaves; the
second direct file-synchronization path accounts for 866. These are observed
stack paths, not estimates of syscall counts or percentages specific to the
checkpoint phase. `File::synchronize` delegates native files to `sync_all` or
`sync_data`; directory synchronization calls that storage abstraction. Together
with the low checkpoint CPU/wall ratio, this locates much of the sampled
waiting in durable file/object/directory publication. It does not prove that
any particular synchronization is redundant. No barrier was removed, no
carried-artifact integrity validation was skipped, and no directory-sync
batching or relaxed durability setting supplied this patch.

## Correctness and remaining work

[Source-bound follow-up CI](ci-prepared-impact.json) records all nine Prow jobs
successful at head `6a378114156967f45972b49d0bc29ee25ed7874b`. Root/crates/crash
clone records agree on runner `32f546bfb76ab81432275d6983a215f8766bfa37`.
Complete BEP: root 77 targets (62 executed, 15 cached), crates 106 (69 executed,
37 cached), and an executed crash target. Storage units actually ran in
275.111s. These timings are not a CI speedup comparison. Full strict Clippy,
formatting and mandatory hooks passed before the runtime commit; later
evidence-only heads have separate check status.

## Ordinary follow-up after compilation exited

A separate ordinary run used the frozen fixed executable with profiling
omitted: no telemetry sink and no resource counter output. Four groups each
completed 60 immediately alternating pairs after two warmups (480 measured,
16 warmup operations); every value/count, carry/rebuild, scrub and reopen check
passed. [Raw samples and binary receipts](paired-prepared-impact-idle-suite/run-receipts.json)
and [full distributions/order/half splits](paired-prepared-impact-idle-suite/summary.json)
are retained without filtering. The same workload and profiling code ran on
macOS; the final harness's extra unused binding is recorded in source metadata.
The prior frozen baseline executable is reused rather than falsely claiming it
was rebuilt from the final harness source bytes.

The [time-stamped host observations](host-prepared-impact.json) sample the top
seven CPU processes by command name. They contain no active compiler/test
workload, but include ordinary desktop and browser/Mem startup activity. These
are sampled load observations, not continuous CPU/I/O isolation or proof that
no background activity occurred between observations. The run is a lighter-load
shared-workstation measurement, not a dedicated-host p99 qualification.

Foreground synchronized mutation:

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change |
|---|---|---:|---:|---:|
| 128 / 512 | node_property | 4.602 / 5.195 / 5.418 | 4.513 / 5.084 / 5.215 | -1.24% |
| 128 / 512 | relationship_property | 7.098 / 8.178 / 8.688 | 6.954 / 8.274 / 8.490 | -1.18% |
| 512 / 4096 | node_property | 5.572 / 6.455 / 7.298 | 5.609 / 6.683 / 9.168 | +0.01% |
| 512 / 4096 | relationship_property | 23.592 / 27.565 / 28.473 | 23.208 / 27.453 / 37.821 | +0.11% |

Full embedded checkpoint:

| Nodes / relationships | Mutation | Baseline p50 / p95 / p99 (ms) | Candidate p50 / p95 / p99 (ms) | Median paired change |
|---|---|---:|---:|---:|
| 128 / 512 | node_property | 462.859 / 501.770 / 586.961 | 416.042 / 512.795 / 780.092 | -10.92% |
| 128 / 512 | relationship_property | 463.833 / 489.948 / 502.174 | 464.942 / 488.344 / 505.285 | +0.06% |
| 512 / 4096 | node_property | 448.554 / 499.690 / 689.366 | 393.503 / 426.533 / 591.780 | -12.22% |
| 512 / 4096 | relationship_property | 467.322 / 622.076 / 810.841 | 471.342 / 662.295 / 977.947 | +0.69% |

The earlier approximately 1%-2% foreground median increases did not recur in
this run, but that does not identify their cause or establish a causal repair
speedup across the different measurement windows. Larger-group foreground
paired medians are close to zero (+0.01% / +0.11%). The smaller relationship
control's half medians change sign (-3.16% / +3.75%), so its overall -1.18%
median should not be described as a stable improvement. Node-property
checkpoint paired medians remain -10.92% / -12.22%, with zero new adjacency
bytes and the previously documented graph-file-byte reduction.

**Tail acceptance remains open.** Smaller node-property checkpoint p99/max is
780.092 ms versus 586.961 ms; the larger relationship-property checkpoint
p99/max is 977.947 ms versus 810.841 ms. Larger mutation maxima also increase:
9.168 versus 7.298 ms for nodes, and 37.821 versus 28.473 ms for relationships.
With 60 observations these p99 values are maxima. They remain unfavorable
samples, not noise that can be removed. No phase counters were enabled in this
ordinary run, so the loaded diagnostic's stack cannot be assigned to these
particular maxima. The data does not establish complete no-regression
acceptance or justify merging now. More granular relationship CPU work and
controlled synchronization-tail diagnosis remain separate follow-ups.
