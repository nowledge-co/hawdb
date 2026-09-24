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
before queueing, and a terminal crash that discards the undurable suffix and
all handles. Validation uses full intervening history, independent of the
production stamp representation. The [MVCC proof](MVCC_VALIDATION_PROOF.md)
separately covers that representation, barriers and pruning.

| Model transition | Source boundary |
| --- | --- |
| `AcquireRegular`, `Enqueue` | `LockTable::blockers`, `LockManager::acquire`; ordinary versus O compatibility |
| `StartBatch`, `Process` | `CommitSequencer::execute_group_commit_tasks`; one database mutex, ordered per-task validation/publication |
| `Valid` | Abstract first-committer-wins history oracle corresponding to graph `validate_version_writes` |
| `Sync` | Shared durability barrier before successful completion delivery |
| `Retire` | Transaction lock release after grouped execution returns |
| `Crash` | Abstract loss of handles and replay of the durable prefix; no filesystem decoder model |

Induction on transitions preserves absence of mixed O/ordinary holders: only
the two acquisition actions add holders, each tests the opposite class; other
actions retain or remove ownership. `ProtectedUntilRetirement` follows because
queueing adds ownership and only retirement or crash removes it. Ordered
validation against all preceding accepted records preserves first-committer-wins
for each appended record. Sync advances the durable prefix before retirement
can acknowledge success, proving `AcknowledgedDurable` in this abstraction.

Run:

```sh
bazel test //docs/tla:HawDBOptimisticCommitAdmission_check
scripts/check-storage-tla.sh --check-mutants
```

The finite positive model checks types, mixed-holder exclusion, permit lifetime,
first-committer-wins and acknowledged durability. Four independent negative
controls require their named invariant violations: allow S/O coexistence,
release O during processing, skip validation, and validate against only the
previously durable prefix. `NoBatchWitness` and `NoConflictWitness` are
intentionally false reachability probes. Copy the positive cfg to a temporary
file, append `INVARIANT` and the probe name, and run TLC with that cfg. Each
must fail on that probe, demonstrating an accepted multi-writer batch or a
successful writer alongside a rejected overlapping writer.

The model abstracts a single ordinary lock holder and one-key write sets. It
does not model exact queue order, arbitrary lock-target overlap, escalation,
resource admission, source-level key completeness, or group-sync failure
poisoning. It is bounded safety evidence plus a conditional inductive argument,
not an unbounded machine-checked Rust refinement or a proof of composition of
all existing models. Fairness and starvation remain unproved. In particular,
new O owners can coexist with existing ones while an ordinary waiter waits;
this change must not be described as fair admission.

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
