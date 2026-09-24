# Conflict-aware lock waiting: ordering and proof boundary

`LockWaitQueue` prevents a new conflicting lock request from overtaking an
older pending request. Compatible requests can still proceed. This closes the
case where a database-X waiter remains blocked while new compatible optimistic
commit holders continuously replace retiring holders.

## Admission relation

Let `H` be the granted locks and `Q` the ordered sequence of pending requests,
with at most one request per transaction. Define `conflict(a,b)` as overlapping
logical targets and incompatible modes. S/S and O/O are compatible; every other
mode pair conflicts on overlapping targets.

For a request `r` by transaction `t`, define:

- `Owners(t,r)`: other transactions with a conflicting granted lock;
- `Older(t,r)`: transactions with conflicting requests before `t` in `Q`;
  if `t` is not yet queued, every existing entry precedes it;
- `Blockers(t,r) = Owners(t,r) ∪ Older(t,r)`.

The manager grants a new lock exactly when this blocker set is empty. If
blocked, it admits one bounded queue entry and checks for a dependency cycle.
Repeated wakeups reuse the same queue position. Already-owned coverage is not
a new grant and can be reused without entering the queue. Normalized escalation
is checked as the actual broader request before queue admission.

All checks, queue changes and granted-lock changes use the same manager mutex.
Thus checking an older waiter and publishing a new grant cannot race. Grant
removes the pending entry and installs ownership within that critical section.
Timeout, budget rejection and deadlock rejection remove pending priority before
unlocking; the caller then notifies the condition variable. This prevents both
stale queue blockers and a second waiter selecting another victim from a cycle
that has already been rejected. Release and statement-lock restoration also
remove any pending entry and wake waiters.

## Safety and progress argument

No-bypass follows by induction on grants. An initial queue is empty. Enqueue
appends a younger request, repeated waiting does not reorder it, and removals
preserve relative order. Before every grant, `Older(t,r)` must be empty.
Consequently no later conflicting grant can increase the set of owners
blocking an already queued request. Compatible grants do not block that
request and do not invalidate this property.

Conditional progress is narrower than unconditional transaction starvation
freedom. Assume a pending request keeps its target/mode, is not cancelled or
rejected, its existing owners eventually retire, earlier conflicting requests
eventually finish, and continuously enabled admission actions receive service.
The request has finitely many predecessors. New conflicting arrivals cannot
pass it. By induction over that finite prefix, predecessor obligations and
existing conflicting owners disappear; admission becomes continuously enabled
and weak fairness eventually grants it. No fixed wall-clock latency bound is
implied. An abandoned holder or unscheduled thread can still prevent progress.

The condition covers one lock request. A multi-statement transaction can later
request another lock, close a dependency cycle, time out, or fail the existing
snapshot/epoch check. It is not a proof of successful commit or starvation-free
retries for a large transaction under arbitrary concurrent writes. Governed
whole-transaction admission and representative sustained mixed-workload
qualification remain #232 acceptance work.

## Current dependency graph and cycle detection

Each queued transaction has outgoing edges to `Blockers(t,r)` computed from the
current `H` and `Q`. A nonwaiting owner has no outgoing edges. `check_deadlock`
performs breadth-first traversal starting at the current requester and rejects
exactly when an edge returns to it. The requester is the deterministic victim;
sorted blocker sets give a reproducible path for the same state.

The traversal invariant is that every visited queued node is reachable from
the requester, and each parent edge is an actual dependency in the immutable
manager-locked state. A return edge therefore reconstructs a real cycle
(soundness). Each reachable queued node is enqueued once and all its blockers
are examined. A path cannot continue through a nonwaiting owner; thus any path
back to the requester is discovered (completeness). At most `|Q|` queued nodes
are visited, so the traversal terminates. The request lookup, visited/parent
maps and frontier occupy linear space in `|Q|`; only one blocker set is live
at a time, bounded by granted-owner plus waiter counts. No quadratic edge table
is retained. Worst-case traversal and target comparison work can still be
large and is not a latency budget.

Queue-only edges strictly decrease queue position and cannot cycle alone.
Adding a new waiter appends it, so only its new outgoing dependencies can close
a cycle in a previously acyclic state. A grant transfers dependency from queue
priority to a conflicting held lock: older conflicting waiters prevent that
grant, and younger conflicting waiters already depended on its queue entry.
Release, cancellation and restoration to a previously owned lock savepoint
remove or weaken dependencies. Under these caller invariants, rejecting the
requester and removing its pending entry before unlocking preserves an acyclic
wait relation. No cached wait-for edges can survive a timeout or change of
request. The standalone `WaitForGraph` utility remains available, but the live
coordinator now derives this graph from its queue and lock table.

## Resource scope

The queue independently uses the existing lock-metadata defaults: 65,536
entries and 16 MiB of estimated metadata. It checks the prospective count and
estimated bytes before cloning/inserting a new request. A repeated identical
entry does not consume another credit; changing a still-pending request fails.
Removal refunds its exact recorded estimate. Granted locks retain their own
separate admission limits.

These are metadata admission bounds, not process RSS bounds. Allocator capacity,
caller-owned request arrays, transaction state and temporary graph traversal
are outside this queue's byte estimate. The queue does not replace
`RuntimeGovernor` admission or impose a bound on host thread count.

## Machine-checked abstraction

`HawDBLockWaitFairness.tla` models three reusable requesters, two keys, all
nonempty key spans and all three modes. Requesters can repeatedly acquire,
release and rejoin, so the temporal check includes indefinitely recurring new
arrivals rather than only a finite arrival trace. It checks:

- queue uniqueness/capacity, disjoint held/pending ownership and valid modes;
- no conflicting held locks and no conflicting grant past an older waiter;
- `EventualGrant` under weak fairness of grants and owner release.

The model intentionally covers one held request at a time, without upgrades,
partial multi-lock ownership, timeouts or cancellation. The source-level graph
argument and executable cycle tests cover a separate boundary; the temporal
result must not be generalized to all multi-statement Rust transactions.

```sh
bazel test //docs/tla:HawDBLockWaitFairness_check
scripts/check-storage-tla.sh --check-mutants
bash scripts/check-storage-tla.test.sh
```

The final positive model checks 48,810 distinct states (438,523 generated),
depth 16, with an empty queue and completed checks of all three temporal
branches over the full graph. Two registered mutants require `NoBypass` or
`NoConflictingHolders` to fail. A temporary cfg with `FairRelease = FALSE`
must produce a temporal counterexample, demonstrating why an eventual-owner-
release assumption is necessary. `NoCompatiblePassWitness` is a deliberately
false reachability probe recording an actual grant ahead of older compatible
waiters; append it as an invariant to a temporary cfg to exhibit that path.
The evidence verifier requires full-state-space temporal evidence for this
model, including TLC's multi-branch log form, and rejects missing/partial logs.

## Executable evidence

The storage tests check ordering, compatible passage, repeated wakeup position,
atomic count/byte rejection and refund, a cycle containing a queue edge, and
removal of stale dependencies. An independent two-bit target/literal-mode oracle
constructs complete dependency matrices for 4,096 generated three-transaction
states and compares transitive-closure cycle answers against the queue BFS.

Live-manager tests cover 32 rejected overtaking attempts followed by actual
service to the older waiter; reuse of already-owned coverage; timeout removal
waking a successor while an unrelated owner remains held; and requester-victim
selection for a cycle containing a queue dependency. Temporarily omitting
pending-request blockers from admission makes the overtaking regression fail
because a new optimistic holder is incorrectly granted.
