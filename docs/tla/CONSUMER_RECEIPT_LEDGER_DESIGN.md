# Bounded consumer operation receipts: proposed protocol

This is a proposed native contract and bounded model. There is no implemented
receipt ledger, allocator or retirement API in this component. It complements
[installed consumer activation](CONSUMER_INSTALLED_ACTIVATION_DESIGN.md); the
models are not a compositional refinement proof or completed MVCC delivery.

## Problem and required behavior

An operation can commit while its response is lost. Retrying it after a later
operation publishes must still return its original result. Keeping only the
current head's operation receipt loses that evidence. Silently evicting an old
receipt to stay within a quota can turn the next retry into a duplicate operation.
Conversely, retaining every receipt forever does not provide a bounded library
contract. The native API needs explicit admission and retirement semantics.

The proposed ledger belongs to one fixed, non-reused consumer incarnation.
Operation ordinals are reserved durably before preparation and never reused,
including reservations whose preparation is cancelled or abandoned. Reservation
exhaustion fails before accepting another operation. A caller cannot supply an
expired ordinal as the identity of a new operation. New incarnations require a
separate binding protocol, not resetting an allocator in an old namespace.

The current selector chooses a view and an immutable receipt-index root together.
Each successful operation preserves every earlier unretired committed receipt.
Before admitting another operation, check the configured receipt capacity; a full
ledger returns typed deferral, without evicting or beginning a new candidate.
Receipt files and the index root are preinstalled and synchronized before the
selector's successful barrier acknowledges them. This is a native typed-library
contract, not a host-maintained JSON journal or external process control plane.

A caller may explicitly retire a completed prefix after observing its results.
The engine persists a monotone retired-through floor. It may compact that prefix
out of the retained index, but must not delete files still required by the
previous durable selector before the retirement barrier completes. Afterward,
an old retry at/below the floor returns an explicit expired result; absence of
a receipt never authorizes re-executing that old ordinal. The current head's own
receipt remains retained. Cancellation gaps need not occupy receipt entries,
but their allocated ordinals still cannot be recycled.

Lookup distinguishes committed-with-the-original-result, explicitly retired,
and allocated-without-a-committed-result. A damaged/unreadable selector cannot
justify the third outcome: it must fail closed and retain evidence. The bounded
model below assumes a validated selector and leaves uncertain-storage handling
to the activation/cleanup protocol. Native integration must compose these checks.

## State transitions and recovery

1. Reserve a fresh ordinal durably, then own one preparation operation. Failure
   before publishing leaves an unused ordinal, not a reusable operation identity.
2. Preinstall the immutable receipt result and complete its dependency barriers.
3. Select the new view and retained receipt-index root atomically. Only a
   successful root barrier can acknowledge the operation. A lost response does
   not remove its committed receipt.
4. For explicit retirement, write a replacement index/root with the advanced
   floor. Until that root's barrier succeeds, GC retains files referenced by both
   the new root and the previous durable root.
5. Reopen either the last synchronized root or a fully persisted unsynchronized
   replacement. Preserve the durable allocation high-water mark and retired
   floor. Resolve retries from retained receipts/floor before allocating new work.

Reservation and receipt-install barriers are abstract atomic successes here.
The implementation still needs actual WAL/catalog framing, fsync/directory
ordering, uncertain publication handling and lost/torn/reordered-write tests.
Neither a process-kill test nor this finite model proves those physical gates.

## Finite model

[HawDBConsumerReceiptLedger](HawDBConsumerReceiptLedger.tla) has three operation
ordinals, capacity two, cancelled reservations, two selector states, a durable
allocator, explicit retirement, lookup, response loss, GC and one crash/reopen.
It includes operations after reopening to expose ordinal reuse. A ghost
committed/allocated history is an oracle for invariants, not an unbounded native
resident data structure. Physical receipt files are assumed complete and durable
at installation; the installed-activation model separately checks dependency
ordering. No model claims an actual storage implementation correspondence yet.

The positive pinned TLC run checked 46,121 generated / 12,415 distinct states at
complete depth 28. Six isolated faults and two reachability witnesses produced
the expected named invariant counterexamples, nine checks total:

| Fault or witness | Expected invariant counterexample |
| --- | --- |
| Discard earlier receipts when selecting a new view | NoLostReceipt |
| Implicitly evict when the ledger is full | NoLostReceipt |
| Forget the retired-through floor on reopening | NoLostReceipt |
| GC against only the new unsynchronized retirement root | DurableReceiptsRetained |
| Roll back the durable allocator on reopening | NoAllocationRollback |
| Reuse a retired ordinal | NoReexecution |
| Retry a lost operation-1 response after a later head | NoLateRetryWitness |
| Retire, return expired, then admit operation 3 within capacity | NoRetireAndContinueWitness |

Witnesses prove reachability, not a fairness or liveness guarantee. Tool jar
SHA-256: `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
All nine corresponding selected Bazel targets passed. Retained evidence matches
the current module/configuration bytes and pinned tool hash. Native immutable index layout,
byte quotas and bounded lookup work, multi-incarnation composition, source/vector
proofs, registry acknowledgement, real recovery and actual Mem adoption remain
unimplemented. All original resource and 1/4/8-writer/group-fsync gates remain.
