# Optimistic commit admission and shared durability

The `OptimisticCommit` logical lock mode removes mutual exclusion between
optimistic committers while retaining their exclusion against ordinary shared
and exclusive lock holders. The embedded coordinator requests this mode on
`Database`, so it overlaps every ordinary lock target. It is an admission
permit, not permission to modify canonical state concurrently.

## Compatibility and ownership argument

With S = shared, X = exclusive and O = optimistic commit, the complete
compatibility matrix is:

| Held / requested | S | X | O |
| --- | --- | --- | --- |
| S | compatible | conflict | conflict |
| X | conflict | conflict | conflict |
| O | conflict | conflict | compatible |

`LockManager::acquire` checks blockers and grants ownership while holding the
same state mutex. Therefore no conflicting ordinary owner can enter between
checking an O request and granting it, or vice versa. Two O owners may coexist.
They cannot modify a pessimistic workspace: ordinary locks remain incompatible
until every overlapping O owner retires.

X covers all three modes, S covers only S, and O covers only O. S and O are
incomparable; escalation of mixed modes must choose X. Otherwise escalation
could drop exclusion against one class of owner. `covering_mode` implements
this join. Existing entry/byte admission, transaction release, savepoint
restoration and wait-for tracking remain in force; the new permit occupies a
normal accounted lock entry. The embedded API does not request narrow O locks,
but the lower-level mode/coverage rules remain coherent for every target.

`ConcurrentDatabaseTransaction::commit_with_result` obtains the O permit before
queueing its owned workspace and releases it only after `execute_grouped`
returns. The existing transaction destructor releases ownership on unwinding;
lock-acquisition failure occurs before queueing. The grouped coordinator
validates and applies tasks under the database mutex, then performs the shared
WAL durability barrier before delivering successful results. Hence the permit
covers queueing, validation, WAL append, sync, and result delivery. This is the
same ownership interval as the previous X permit.

Concurrent O admission does **not** make validation concurrent. For records
processed in order, each later task checks the latest internal version stamps,
including earlier accepted records in the same not-yet-synced group. Disjoint
writes remain valid; an overlapping stale writer fails before its WAL append.
Checking only the previously durable prefix would be unsound. A group failure
must retain the existing poison/reopen contract; callers cannot treat an
internal pre-fsync root as an acknowledged durable result.

## Model and source mapping

`HawDBOptimisticCommitAdmission.tla` separates admission, batch selection,
per-task validation, sync, and individual retirement. It includes one ordinary
S/X owner, two optimistic transactions, two keys, three record epochs, rollback
before queueing, and a terminal crash that discards handles but retains a
nondeterministic complete WAL prefix. That prefix includes every successfully synced record and
may also include unacknowledged records from a failed sync. Validation uses full
intervening history, independent of the production stamp representation. The [MVCC proof](MVCC_VALIDATION_PROOF.md)
separately covers that representation, barriers and pruning.

| Model transition | Source boundary |
| --- | --- |
| `AcquireRegular`, `Enqueue` | `LockTable::blockers`, `LockManager::acquire`; ordinary versus O compatibility |
| `StartBatch`, `Process` | `CommitSequencer::execute_group_commit_tasks`; one database mutex, ordered per-task validation/publication |
| `Valid` | Abstract first-committer-wins history oracle corresponding to graph `validate_version_writes` |
| `Sync`, `FailSync` | Successful barrier or storage-handle poisoning and storage-integrity errors for accepted tasks |
| `Retire` | Transaction lock release after grouped execution returns |
| `Crash` | Loss of handles and replay of a complete persisted prefix retaining all acknowledgements; torn/corrupt decoding is outside this action |

Induction on transitions preserves absence of mixed O/ordinary holders: only
the two acquisition actions add holders, each tests the opposite class; other
actions retain or remove ownership. `ProtectedUntilRetirement` follows because
queueing adds ownership and only retirement or crash removes it. Ordered
validation against all preceding accepted records preserves first-committer-wins
for each appended record. Sync advances the guaranteed durable prefix before
retirement can acknowledge success. A separate `acknowledged` set records delivered record positions and
survives crash, so `AcknowledgedDurable` remains meaningful after handles are
lost. Earlier versions derived acknowledgement solely from a `done` phase and
reset every phase at crash; their post-crash acknowledgement check was vacuous.

## Failed sync and recovered prefix argument

Let H be accepted records in serialized append order, D the guaranteed durable
prefix length, and A the set of acknowledged positions. Maintain A subseteq
1..D and D <= Len(H). `Process` appends only after first-committer-wins
validation against all of H, including the current unsynced batch. Successful
`Sync` advances D before `Retire` adds acknowledged positions. `FailSync` does
not advance D: it marks accepted tasks uncertain, preserves existing conflict
errors, poisons the handle and permits no subsequent canonical publication.
Failed tasks retire without entering A. Previously successful groups can still
deliver their already durable results after a later group fails.

`Crash` chooses a complete prefix length C with D <= C <= Len(H), preserves A,
and retains exactly H[1..C]. Therefore every acknowledged record remains, and
any surviving uncertain transaction appears in the same serial order. Prefix
restriction preserves first-committer-wins because it removes only suffix
records, never an intervening record used by validation. The independent
append-history ghost `appended` is retained across crash, so
`RecoveredSerialPrefix` checks actual sequence identity, not only length.
`FailedSyncNeverAcknowledged` excludes every uncertain transaction from A;
`PoisonBlocksPublication` checks that H cannot grow on the poisoned live handle.

These are conditional induction arguments. They assume successful sync is
honored by storage and that strict recovery either validates a contiguous
complete-record prefix or fails closed. The model does not treat a torn frame
as a successful open, or infer that an error means the write was rolled back.
Physical torn tails require explicit doctor repair under the existing protocol.
The executable regression tests complete prefixes, incomplete frames, and valid
individual frames reordered to violate LSN order. They simulate persisted bytes
and do not claim hardware power-loss coverage.

Run:

```sh
bazel test //docs/tla:HawDBOptimisticCommitAdmission_check
scripts/check-storage-tla.sh --check-mutants
```

The finite positive model checks types, mixed-holder exclusion, permit lifetime,
first-committer-wins, acknowledged durability across crash, failure poisoning
and recovered serial-prefix identity. The current finite run explores 115,248
generated / 37,594 distinct states, depth 21, with an empty queue.
Eight independent negative controls require their named invariant violations:
allow S/O coexistence, release O during processing, skip validation, validate
against only the previously durable prefix, permit publication after poison,
acknowledge a failed sync, lose a guaranteed prefix, and reorder recovery.
`NoBatchWitness` and `NoConflictWitness` are intentionally false reachability
probes. Copy the positive cfg to a temporary
file, append `INVARIANT` and the probe name, and run TLC with that cfg. Each
must fail on that probe, demonstrating an accepted multi-writer batch or a
successful writer alongside a rejected same-key writer within one batch.
`NoUncertainRecoveryWitness` and `NoPostCrashAcknowledgementWitness` similarly
must fail on reachable recovered uncertain data and a surviving acknowledgement
of an optimistic transaction, respectively. The latter explicitly excludes
ordinary-writer-only acknowledgement so the transaction path cannot be vacuous.

The model abstracts a single ordinary lock holder and one-key write sets. It
does not model exact queue order, arbitrary lock-target overlap, escalation,
resource admission, source-level key completeness, or physical decoding and
checkpoint/apply-failure composition. It models sync-failure poisoning but not
new transaction handles after the terminal crash. It is bounded safety evidence
plus a conditional inductive argument, not an unbounded machine-checked Rust refinement or a proof of composition of
all existing models. This model does not include waiting order and proves no fairness theorem.
The subsequent [lock-wait protocol](LOCK_WAIT_FAIRNESS_PROOF.md) prevents
new conflicting O owners from passing an older ordinary waiter. Its separate
conditional request-progress theorem must not be generalized to whole
transactions or retries.

## Executable checks

- The lock-table truth-table test checks all nine mode pairs against literal
  expected conflict/coverage matrices, database/narrow overlap and release of
  one of multiple O holders. Escalation/savepoint tests check incomparable
  S/O coverage without losing ownership or widening a restored snapshot.
- `optimistic_group_admission_batches_disjoint_writes_and_rejects_overlap`
  holds queued tasks behind a releasable test gate. Both O writers must enter
  before release. Disjoint writes use one sync; overlapping writes yield one
  successful record and one typed retryable conflict. An ordinary pessimistic
  mutation must time out while O owners wait. Exact values and epochs survive
  reopen. The gate is released before assertions so reverting to X fails
  without hanging the test.
- `optimistic_group_admission_waits_for_pessimistic_owner_and_retires_on_timeout`
  checks the opposite acquisition order and successful later commits after a
  bounded timeout and owner retirement.

Temporarily restoring the previous database-X acquisition makes the grouped
regression fail because only one writer reaches the queue. The separate mixed
owner regression still passes in that negative run. The [writer benchmark](../CONCURRENT_WRITER_BENCHMARK.md)
measures scheduling and sync amortization without making them safety invariants.

- `admitted_group_failure_preserves_conflicts_and_recovers_only_complete_serial_prefixes`
  stages two governed optimistic transactions, each updating two nodes, and forces
  both into one failing durability group. It checks both disjoint and overlapping
  key sets, exact storage-error/conflict classification, zero successful group
  acknowledgements, retained admission before execution and complete refunds
  after retirement. Both old and new reads, new writes and checkpoints reject
  the poisoned handle without modifying WAL. A previously acknowledged WAL
  record survives every tested prefix; exact rows are compared with decoded
  transaction order, and incomplete or reordered frames fail closed without
  rewriting the bytes. Restoring the complete fixture and opening a new handle
  permits a subsequent commit. This is explicit failpoint/persisted-byte evidence,
  not a claim that every crash/OS/filesystem boundary has been exercised.


## Mixed-domain subprocess crash qualification

`subprocess_concurrent_crash_recovers_mixed_transactions_as_serial_prefixes`
adds actual child-process exit to the earlier failed-sync/byte-cut evidence.
The parent creates checkpointed schemas and nodes, then acknowledges one mixed
graph/relational/append transaction in WAL before spawning the child. Both child
workspaces start at that recovered baseline. A releasable enqueue gate admits
two O owners and fixes queue order as writer 1 then writer 2 before execution.
Each writer updates one graph node, inserts one explicit relational primary key,
and appends to its own generated-order table. The overlapping variant shares
the graph key while keeping the other domains distinct, so rejecting writer 2
must also prevent its unrelated SQL and append writes from appearing.

For each Materialized/OutOfCore mode and disjoint/overlap variant, the seven
points below terminate with exit code 86 without Rust stack unwinding. Let N be
the number of accepted writer records (2 disjoint, 1 overlapping), and C the
number recovered beyond the previously acknowledged baseline:

| Exit boundary | Required recovered prefix |
| --- | --- |
| Before first WAL append | C = 0 |
| After first complete WAL append, before canonical apply | 0 <= C <= 1 |
| After first grouped task's canonical application | 0 <= C <= 1 |
| After both grouped tasks, before shared sync | 0 <= C <= N |
| After successful shared WAL sync, before result delivery | C = N |
| Checkpoint payload persisted, before manifest publication | C = N |
| After checkpoint manifest publication | C = N |

The two coordinator-specific hooks are compiled only for tests. Existing WAL
and checkpoint process-exit hooks supply the other boundaries. The checkpoint
cases first verify one shared sync, exact submitted/completed/WAL-entry counts,
typed overlap rejection and unchanged old-snapshot graph/SQL results. Only then
do they sync an acknowledgement evidence file outside the database format.
The parent requires that evidence for checkpoint cases and its absence at earlier
exit points. It compares every expected graph row, relational row, append payload
and generated sequence, plus the recovered epoch. Checking the exact first C
writers rejects a reordered surviving suffix; checking every domain rejects
partial transaction application. All earlier baseline data must remain.
A new pair of post-restart writers must again produce first-committer-wins;
the rejected commit leaves WAL bytes unchanged, and another reopen retains the
new winner's epoch and value.

The model's history element abstracts an entire compound transaction record.
For this fixture, define apply(record i) as its graph, relational and append
changes together. Recovery of exactly H[1..C] implies applying that vector prefix
in order. The executable oracle independently checks all three components of
that implication, including generated append order; it does not infer atomicity
from the graph result or epoch alone. The model's independent acknowledgement
set maps to the parent's known baseline plus the child's result evidence.
This extends the source/refinement evidence without changing the abstract
transition system. A fresh direct TLC run on the unchanged model checked
115,248 generated / 37,594 distinct states, depth 21, with an empty queue;
the Bazel model action/test were cached and are reported separately.

All 28 process cases passed. These exits leave the OS alive and do not evict its
page cache: they qualify abrupt process loss at the selected boundaries, not
hardware power loss, every filesystem failure, arbitrary instruction interleavings
or persistence of unsynced bytes. The earlier torn/reordered-frame tests and
negative model controls remain necessary. Successful sync and strict recovery
remain assumptions of the conditional serial-prefix theorem, not facts proven
solely by this subprocess fixture.
