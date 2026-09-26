# Governed conflict retry with capacity escalation

This qualifies an explicit host retry policy for #232 using the existing
`RuntimeGovernor` and `begin_admitted_transaction` APIs. HawDB does not silently
retry user callbacks, change request weights, or enroll plain transactions in a
governor. The initial attempt keeps the ordinary one-slot reservation, allowing
concurrent work. Only after a typed, pre-publication transaction conflict does
the host discard the failed workspace, retain a foreground admission waiter,
and request the governor's full CPU capacity for a fresh attempt.

## Policy and assumptions

1. Run the first attempt with the ordinary mutation permit.
2. On success, return that result. On an error other than
   `is_retryable_transaction_conflict()`, return the error without automatic
   replay. A failed durability operation can have an uncertain outcome; it is
   not proof of rollback.
3. After the conflicting attempt retires, create one foreground waiter. Retain
   that same waiter across retryable admission refusals. Request all C CPU slots
   from the same governor, where C is the stable effective capacity, while
   retaining sufficient mutation/result budgets. Drop the waiter on cancellation
   or terminal admission error.
4. Only after admission, create a new transaction and rerun the complete logical
   operation with its original request inputs and new snapshot. Do not reuse a
   failed transaction, its observed query results, or stale read-derived writes.
   Keep intermediate results and external side effects private until commit;
   database rollback cannot undo arbitrary host side effects.

This policy trades temporary concurrency for a bounded retry path. Full-capacity
admission can also delay queries sharing the governor; it is a caller-selected
fallback, not a changed default. All competing mutators must participate in the
same governor. The result below assumes the same foreground priority, stable
sufficient CPU/memory/task/I/O limits, finite valid work, eventual retirement of
current owners, fair host scheduling and successful storage. Cancellation,
capacity shrinkage, external writers and failed I/O can terminate the operation
instead of completing it successfully.

## Progress argument

During the shared first attempt, a successful overlapping small write may make
the workspace stale. The finite statement sequence eventually reaches commit.
It either succeeds, or per-key validation rejects before publication. The
[MVCC validation protocol](../MVCC_COMMIT_VALIDATION_PROTOCOL.md) and executable
WAL assertion establish that this rejected attempt leaves no partial mutation.
The [admission lease](TRANSACTION_ADMISSION_LEASE_PROOF.md) retains its original
reservation through retirement, then refunds it once.

The retry waiter has a fixed queue position. There are finitely many older
waiters and active holders. Same-priority FIFO admission prevents newer arrivals
from overtaking it. Under the original governed-progress assumptions, older
requests retire; once the retry reaches the head, later one-slot requests cannot
consume released slots. Occupied capacity drains to zero and the retry becomes
continuously admissible. Fair host retry then obtains all C slots.

Snapshot capture occurs after that admission, so writes committed by the old
owners are included. No other participating writer can be active while the
retry holds C slots, including an owner waiting for durability: the reservation
covers that interval too. Consequently no such writer can invalidate this fresh
attempt. Finite valid work followed by successful commit and retirement completes
the operation. Under these assumptions there is at most one rejected attempt;
the statement is not a guarantee that an arbitrary fixed-weight retry loop ends.

Retaining the original one-slot weight has no corresponding guarantee. An
adversarial but fair schedule can let a small writer commit after every fresh
snapshot and before each large commit. Every individual task is serviced, while
the large logical operation rejects forever. Admission fairness and retry
fairness are distinct obligations.

## Finite temporal model

`HawDBGovernedConflictRetry.tla` starts with a one-slot large attempt and a small
owner sharing two slots. Small actors can finish and rejoin indefinitely. Their
successful commits invalidate an active large attempt. Conflict removes the old
holder and appends its retry waiter; successful grant clears staleness to model
fresh snapshot capture. An escalated retry has weight C. Work, commit and durable
retirement remain separate transitions, and owners stay charged until retirement.

TLC checks types/queue membership, capacity, at most one conflict, no publication
from rejection, exclusive retry ownership and eventual large completion:
**404 generated / 190 distinct states**, depth 20, empty queue, with complete
state-space temporal checking. Weak fairness applies to admission and service,
not to the arrival of new requests. Small commits conservatively model successful
interference; real small conflicts can instead retire without invalidating the
large workspace. This does not reduce the required finite-holder drain premise.

- The registered `PublishRejected = TRUE` control violates
  `RejectedNeverPublished`.
- With `Escalate = FALSE`, remove only the policy-specific `AtMostOneConflict`
  and `RetryOwnsCapacity` assertions. The remaining safety checks stay enabled,
  and `LargeCompletes` has a temporal starvation counterexample.
- False witness invariants `NoConflictWitness` and `NoRetryCommitWitness` reach
  an actual rejected first attempt and a successful escalated retry.

This finite abstraction and the source argument are not a machine-checked Rust
refinement, arbitrary-priority theorem, wall-clock bound, or crash/durability
composition proof. The evidence verifier rejects missing/partial temporal logs.

## Executable qualification

`admitted_conflict_retry_escalates_before_recurring_hot_writers` runs in memory
and durable grouped mode. The first attempt increments one hot row by 100 and
inserts 64 rows privately. A small commit changes that same hot key; the first
attempt must return a typed retryable row conflict, leave WAL bytes unchanged,
publish none of its 64 rows, and leave the exact successful epoch/value intact.

Another older owner remains active. The full-capacity retry must report CPU
saturation; a direct younger request and four queued foreground workers must
report `QueuedAhead`. After the old owner commits, the retry sees its value of 2,
reruns the complete operation and commits once. Four workers then each commit
16 transactions, each incrementing the same hot row and inserting a unique row.
They handle their own typed conflicts and independently verify that every fresh
snapshot contains all 64 large rows. Their bounded test retry deadline is not a
production guarantee for fixed-weight retries.

Each fixture checks exactly 129 rows, hot value 166, 67 successful epoch
increments, no queued waiters or CPU/task/memory reservations, and exact durable
reopen. A double-applied large increment, partial failed insert, stale retry
snapshot, missed successful small increment, lost row or admission bypass changes
these independent expected results.

The policy is demonstrated through existing library APIs; automatic retries,
replay of external effects, unconstrained priority combinations and production
workload/performance qualification remain outside this delivery and keep #232
open.
