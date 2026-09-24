# Governed transaction admission lifetime

`ConcurrentDatabase::begin_admitted_transaction` consumes one host-admitted
`RuntimePermit` for mutation work. Admission occurs before snapshot creation.
The existing `RuntimeGovernor` queue owns waiting policy; statements do not
readmit. The original `begin_transaction` remains caller-managed.

## Ownership invariant and induction

Let C be transactions with a caller-owned permit reference, Q transactions
with a queued-request-owned reference, and L = C union Q. Each transaction
owns one governor reservation regardless of its number of Arc references.
Let W contain active workspaces and queued, executing, syncing or undelivered
commit requests. The safety obligation is W subseteq L.

Initially W and L are empty. Admission reserves governor capacity before it
creates a workspace, adding the same transaction to W and C. Enqueue installs
a second reference in Q before the caller can lose ownership. Consuming the
commit callback removes the callback, not Q; thus the interval between callback
return and shared fsync remains covered. Completion publishes a result while
the request remains owned. Delivery/retirement removes the work before the last
reference can drop. Dropping an unsubmitted transaction discards its workspace
before its last permit field; dropping a submitting caller leaves Q intact.
Rejection, conflict, rollback and cancellation follow the same retirement
rules. Consequently every transition preserves W subseteq L. Arc's final drop
refunds the single reservation once, not once per reference.

`execute_grouped_with_admission` keeps its argument alive in the ungrouped path.
In the grouped path `QueuedCommit::_admission` follows the request through the
queue, coordinator's completed batch and result delivery. Keeping a permit only
inside the callback would be insufficient: the callback is consumed before
shared sync. The coordinator regression explicitly removes the caller owner,
consumes the callback and takes the result before checking final release.

Cancellation is cooperative: checks occur before snapshot creation, after
snapshot creation, at statement boundaries and before the queued commit
callback writes. Existing query checkpoints receive the bound context. There
is no cancellation check after durable success. Logical-lock waiting continues
to use its timeout; cancellation does not immediately wake that condition
variable. Pessimistic workspace refresh preserves the outer context and permit.

## Finite model

`HawDBTransactionAdmissionLease.tla` separates callers, queued requests and
consumed callbacks. Three transactions compete for two admission slots, with
caller disappearance, cancellation before execution, sync, completion/failure
and retirement. TLC checked 13,393 generated / 3,904 distinct states, depth 20,
with an empty queue. Invariants cover lifetime, capacity, callback consumption
and absence of durable writes after a pre-execution cancellation.

The registered `RetainRequest = FALSE` negative control must violate
`ProtectedUntilRetirement`. This checks the missing independent queue owner;
it is not an assertion that the current synchronous API normally abandons a
caller during fsync. The positive model and mutant complement the source-level
induction; finite checking alone is not a Rust refinement proof.

## Scope and related proofs

This is reservation lifetime safety, not a proof of full transaction fairness,
WAL recovery, or bounded process memory. The existing
[optimistic commit model](OPTIMISTIC_COMMIT_ADMISSION_PROOF.md) governs logical
locks and serialized publication; those logical permits are distinct from
RuntimeGovernor reservations. The [lock queue proof](LOCK_WAIT_FAIRNESS_PROOF.md)
establishes conditional per-request progress, not retry fairness. The runtime
admission model's capacity/waiting results remain separate; host scheduling,
capacity changes and full model composition are not proved here.

The permit propagates CPU, memory, I/O and cancellation context to existing
resource-aware execution paths. Graph and SQL reads use the admitted result
limit; SQL intersects it with configured limits, including virtual catalogs and
append reads. Mutation staging and RETURNING retain existing `MutationLimits`.
Not every private COW allocation is charged. Hosts can retain returned results
after the transaction releases its permit, and the API does not cap host-created
threads. Callers must size mutation/result reservations appropriately. Plain
transactions and autocommit are not implicitly admitted by this new entrypoint.

The grouped-failure integration regression additionally verifies that two
admitted transactions both refund their reservations when the shared barrier
fails, including a batch with one validation conflict and one uncertain write.
The separate [optimistic admission model](OPTIMISTIC_COMMIT_ADMISSION_PROOF.md)
now covers that failure's acknowledgement and recovered-prefix obligations;
this does not merge the two models into a full resource/recovery refinement.
