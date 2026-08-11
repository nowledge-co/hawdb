# Skein CRDT Replication Specification

## Implementation Status

This is a design-stage contract with a machine-checked model and **no
implemented surface in the Skein crates yet**. Nothing here describes shipped
behavior, and the single-process embedded scope in `TODO.md` still stands: a
replication layer is a product decision, not an implied backlog item. The
document exists so that decision can be made against a verified design, and
it is the layer the deferred replica-assisted repair task depends on.

## Scope

This specification defines the convergent replication contract that lets a
set of Skein nodes synchronize the canonical property graph.
The design is a delta-state CRDT layered above the existing single-node
transactional engine: local transactions keep their existing semantics, and
replication ships the committed effects of those transactions.

A deployment is one **master** and any number of **slaves**. Every replica
mints operations locally, including slaves that cannot currently reach the
master. Two link types carry them: a session between each slave and the
master, and gossip directly between slaves.

The two links do not carry the same thing. A slave's local operation is
**pending** until the master confirms it, and **gossip carries confirmed
operations only**. Pending work travels one way, to the master, over the
session. This is the constraint the rest of the design is built around: it
means the master has seen every operation that exists anywhere in the mesh,
so the mesh can never hold state the master must later reconcile.

The join, the dot identity, and the conflict matrix are the same on every
link, so an operation learned through gossip and the same operation learned
from the master converge to the same value. The master is not a linearizer:
it assigns confirmation order for distribution, and because the join is
commutative that order changes nothing about the result.

Every normative clause is written so that the same state and join rules
extend to N replicas without a format change. That extension is a design
claim, not a verified one; see *Instance Bound*.

### Non-Goals

- Consensus, leader election, or synchronous quorum commit.
- Cross-replica ACID transactions or global serializability.
- Replicating derived state. Full-text, vector, analytics, and plan-cache
  artifacts remain rebuildable local projections and MUST NOT be shipped.
- Replacing PostgreSQL as the Nowledge Cloud canonical store.
- Tolerating Byzantine or actively malicious peers. Peers are trusted after
  transport authentication; corruption defense is checksum-and-fail-closed.

## Replica and Operation Identity

- Each replica persists a stable `ReplicaId` minted once at replica
  initialization and recorded in the durable manifest. A copied or restored
  database directory MUST NOT reuse the source `ReplicaId` for new writes
  without an explicit re-identity step; two writers sharing a `ReplicaId`
  is a fork and MUST fail closed when detected (see Fail-Closed Conditions).
- Every replicated committed operation mints a **dot** `(ReplicaId, counter)`.
  Counters are contiguous per replica starting at 1 and are assigned at commit
  publication, in commit order, alongside the WAL LSN.
- The master appends every operation it confirms to a **confirmation log**
  and assigns it a contiguous position. The log is a distribution order, not
  a semantic one; the join is commutative, so applying entries in a
  different order reaches the same value.
- A replica's **causal context** is therefore two integers, not a vector:
  the confirmation position `P` it has applied, and the count `k` of its own
  operations. Every dot a replica has seen is either in the log prefix `P`
  or one of its own first `k` dots, because those are the only two ways a
  dot can reach it. Cumulative membership does not enter the context, so a
  replaced device or a restored backup costs nothing in metadata and no
  entry-retirement mechanism is needed.
- **Occurrence identity.** A `CREATE`/first-`MERGE` mints a fresh occurrence
  dot for the created node or relationship. Occurrence identity MUST NOT be
  derived from a payload hash or from the logical key. A delete, recreate,
  and re-delete of the same logical entity therefore produces three distinct,
  durable, totally-ordered-per-origin identities, and stale-sequence
  regressions are detectable by counter comparison.

## Replicated State

The replicated object is the canonical graph only. Its CRDT state per replica
is:

- `context`: the replica's causal context, the pair `(P, k)` above.
- `nodes`: a set of live **node occurrences**
  `[key: (label, primary key), dot, props]` with at most one record per dot.
- `edges`: a set of live **edge occurrences**
  `[dot, type, src: node dot, dst: node dot, props]`.
- `props`: per occurrence, a map from property name to an LWW register
  `(value, ts)` where `ts = (hlc, ReplicaId)`. The clock is specified in
  *Hybrid Logical Clock* below. A raw per-replica commit counter is **not**
  a valid `hlc`: it lets a replica overwrite a register it has already seen
  with a smaller timestamp, and the stale value then wins the join (this
  failure is reproduced by the TLA+ model when the clock merge is removed).


### Hybrid Logical Clock

Nothing about causality uses time. Dots and `context` are contiguous
per-replica counters, and they alone decide what a replica has seen, which
deltas may be applied, and what an observed-remove removes. The clock exists
for one purpose: deciding which of two **concurrent** writes to the same
property wins. A design that confuses the two ends up with convergence that
depends on clock quality, which this one does not.

`hlc` is a 64-bit value, 48 bits of milliseconds since the Unix epoch
followed by a 16-bit logical counter:

```text
hlc = (physical_millis << 16) | logical
```

Write `pt` for the replica's physical clock in milliseconds and `(l, c)` for
the current components. Two transitions maintain it:

- **Local commit.** `l' = max(l, pt)`. If `l' = l` then `c' = c + 1`,
  otherwise `c' = 0`.
- **Applying a delta batch.** For the greatest observed `(lm, cm)` in the
  batch, `l' = max(l, lm, pt)`, and then `c'` is `max(c, cm) + 1` when
  `l' = l = lm`, `c + 1` when `l' = l`, `cm + 1` when `l' = lm`, and `0`
  otherwise.

Registers compare lexicographically on `(physical_millis, logical,
ReplicaId)`. The `ReplicaId` tail makes the order total, so no two writes
tie.

Three consequences are normative rather than incidental:

- **Backward physical jumps are absorbed, not obeyed.** `l` never decreases,
  because every transition takes a maximum. A clock that steps backward
  simply leaves the replica incrementing `logical` until physical time
  catches up. Correctness does not depend on a monotonic host clock.
- **The clock is durable state.** A replica MUST persist its `hlc` alongside
  `context` and `ReplicaId` in the checkpoint manifest, and on open MUST
  resume from `max(persisted, pt)`. Reseeding from `pt` alone would let a
  restart after a backward jump re-issue timestamps the replica had already
  emitted, and a value written before the restart would then beat one
  written after it.
- **Logical overflow spills upward.** If `logical` would exceed its 16-bit
  range within one millisecond, `physical_millis` increments and `logical`
  resets, which keeps the value monotone at the cost of running ahead of
  real time. Sixty-five thousand commits inside one millisecond is not
  reachable while each commit crosses a durability barrier, so this is a
  bound rather than an expected path.

#### Bounded Forward Drift

Receive-max is what makes the clock causal, and it is also the design's one
real exposure to a bad clock. A replica whose physical clock reads years in
the future drags every replica that merges its timestamps to that value, and
because `l` never decreases, **the damage is permanent**: there is no way to
walk a hybrid logical clock back. Every subsequent write on every replica
carries the inflated timestamp, and LWW between two honest replicas stops
tracking real time entirely.

Gossip makes this materially worse than a hub topology would. One
misconfigured slave has to reach only a single peer for the value to spread
through the mesh by ordinary anti-entropy.

Therefore a replica MUST reject a delta batch whose greatest observed
`physical_millis` exceeds its own `pt` by more than a configured
`max_clock_offset`, and MUST NOT merge any timestamp from it. Per
*Fail-Closed Conditions* this isolates the session and drops the peer from
the partial view; local state stays usable, because the batch was refused
before it touched the clock. The bound is a policy value: too tight and
ordinary skew between healthy replicas severs sessions, too loose and it
admits the drift it exists to prevent.

The bound does not reach a replica whose clock is wrong by less than
`max_clock_offset`. That case stays a matter of which concurrent write wins,
never of whether replicas converge.

Removal is represented **without tombstones** (ORSWOT style): a dot that is
covered by `context` but absent from `nodes`/`edges` is removed. Storage for
removed occurrences is reclaimed immediately; only the vector survives.

### Read Projection

Cypher reads see a deterministic projection of CRDT state:

- A logical node `(label, key)` is **visible** iff it has at least one live
  occurrence. Concurrent creates on both replicas may leave multiple live
  occurrences for one logical node; `MATCH` MUST return one logical row, and
  each property MUST resolve to the register with the greatest `ts` across
  the live occurrences.
- An edge occurrence is **visible** iff its own dot is live **and both
  endpoint occurrence dots are live**. An edge whose endpoint was removed is
  *masked*: it is retained in CRDT state for convergence but MUST NOT be
  returned by any read, traversal, index, or projection.

Referential integrity of the visible graph therefore holds by construction:
no visible edge can reference an invisible node.

## Operation Semantics

Each committed local mutation becomes one CRDT delta, identified by its dot:

| Cypher operation | CRDT delta |
| --- | --- |
| `CREATE` node / rel | Add a fresh occurrence dot (with initial property registers). |
| `MERGE` (no visible match) | Same as `CREATE`. |
| `MERGE` (visible match) | Bind to the existing live occurrences; no new occurrence. |
| `SET` / `REMOVE` property | Write LWW registers (fresh `ts`) on every locally live occurrence of the matched entity. |
| `DELETE` edge | Observed-remove of the locally live edge dots. |
| `DELETE` node | Observed-remove of the locally live node occurrence dots; MUST fail locally if a visible incident edge exists (existing Cypher rule). |
| `DETACH DELETE` node | Observed-remove of the locally live node occurrence dots and of every locally live incident edge dot, atomically in one delta. |

"Observed-remove" means the delta removes exactly the dots covered by the
local context at commit time. A remove never affects occurrences the replica
has not yet seen.

### Convergent Conflict Matrix

Concurrent (causally unordered) operations resolve deterministically:

| Conflict | Outcome |
| --- | --- |
| `CREATE` vs `CREATE` (same logical key) | Both occurrences survive; the read projection merges them into one logical entity. **Add-wins.** |
| `CREATE` vs `DELETE` (same logical key) | The delete removes only observed occurrences; the concurrent fresh occurrence survives. **Add-wins** for the logical entity. |
| `SET` vs `DELETE` of the same occurrence | The occurrence dies; the concurrent property write is discarded with it. **Remove-wins per occurrence.** |
| `SET` vs `SET` on the same property | Greater `(hlc, ReplicaId)` wins. Unique because `hlc` values are per-replica monotone and ties break on `ReplicaId`. |
| `DETACH DELETE` node vs concurrent incident edge `CREATE` | The edge occurrence survives in state but is **masked** (endpoint dot removed). The visible graph shows neither the node nor the edge. |
| `DELETE` vs `DELETE` of the same occurrence | Idempotent; the occurrence is removed once. |

A recreated logical entity starts from fresh occurrence dots with fresh
property registers; property values of removed occurrences MUST NOT
resurrect through recreation.

## Anti-Entropy Synchronization Protocol

Synchronization runs over an authenticated transport session. The exchange
below is the topology-independent core; *Delivery Topologies* then defines
who talks to whom and how often.

1. **Digest.** The requester sends its confirmation position, and on a
   master session its pending count as well.
2. **Delta response.** The responder replies with the contiguous run of
   confirmation-log entries above the requester's position. The responder
   MUST NOT ship a run with holes; if the requested entries have been
   compacted away it MUST ship its full joined state instead (state transfer
   degenerate case).
3. **Join.** The receiver applies the batch as one local transaction through
   the normal WAL and group-commit path, using the join rules:
   - A local live record whose dot is covered by the sender's context and
     absent from the sender's shipped state is removed.
   - A shipped record whose dot is covered by the receiver's context and
     locally absent is discarded (it was removed here).
   - A record present on both sides merges property registers per-key by
     greatest `ts`.
   - the confirmation position advances to the end of the applied run.
4. **Acknowledgment.** Only after the WAL sync boundary makes the joined
   batch durable does the receiver acknowledge its new `context` to the peer.
   The responder MAY use acknowledged peer contexts for delta retention and
   masked-edge GC, and MUST NOT use unacknowledged ones.

Delivery obligations:

- **Contiguous log prefixes.** A responder ships from the position the
  requester declared, in order. Together with join closure this guarantees
  causal delivery: a shipped edge occurrence's endpoint dots are always
  covered once the batch is applied, because the master confirmed those
  endpoints at earlier positions. A batch that would leave a hole MUST be
  rejected before apply.
- **Idempotent retry.** The join is idempotent, commutative, and
  associative. After a crash, a lost acknowledgment, or a duplicated
  response, the receiver simply re-runs a round from its persisted context.
- **No epoch skew.** Both replicas MUST run the same schema fingerprint; a
  delta batch carries the fingerprint and MUST fail closed on mismatch.

## Roles and Delivery

Two link types carry operations, and they differ in what they may carry.

**Master session.** A slave pushes its pending operations for confirmation
and pulls the confirmation log above its position. The master appends each
accepted operation to the log, joins it into its own state, and records the
slave's new position. A pending operation becomes visible to the rest of the
deployment at exactly this point and not before.

**Slave gossip.** A slave exchanges the confirmation log with another slave
and nothing else. It MUST NOT ship pending operations, its own or anyone
else's, and MUST NOT accept them.

The master does not gossip. It has a session with every slave, so gossiping
would add paths without adding reachability.

### Confirmed-Log Catch-Up

Because gossip carries only the confirmation log, everything on that path
comes from one source and is totally ordered. A gossip digest is therefore a
single integer — the requester's confirmation position — and the response is
the contiguous run of entries above it.

This is why no reorder buffer exists in this design. A responder ships from
the position the requester declared, in order, so a hole cannot appear. Two
concurrent gossip rounds can only overlap, never gap, and overlap is
absorbed by the idempotent join. The one case that cannot be served
contiguously is a requester so far behind that the responder has compacted
the entries it needs, which falls back to the state-transfer form already
defined above.

Duplicates remain normal — a slave hears the same entries from several peers
— and remain free, because the join is idempotent and digest-before-delta
keeps the cost at one round trip rather than one payload.

### Stability and Membership

Retention and masked-edge reclamation need to know an operation is stable.
The master answers this and nothing else does: it records each slave's
confirmed position, so the **stable position** is the minimum across the
members of the current membership epoch. An operation at or below it is on
every replica.

A slave MUST NOT derive stability from gossip. A partial view cannot
distinguish "no other slave exists" from "a slave I have never heard of
exists", and the second case is what a premature reclamation corrupts.
Reclamation MAY proceed only against a stable position carrying a current
membership epoch from the master.

### Losing the Master

A slave that cannot reach the master keeps working, with a deliberate
asymmetry:

- It **keeps accepting local writes**, which accumulate as pending.
- It **keeps catching up on the confirmation log** from other slaves, so a
  partitioned group still converges on everything the master had confirmed
  before the partition.
- Its **new writes stay invisible to its peers**, because gossip carries
  only confirmed work. Two partitioned slaves converge on the past, not on
  each other's present.
- It **stops reclaiming**, because the stable position it would need is
  stale.

The third point is the price of the confirmation rule, and it is worth
stating plainly rather than discovering later: a group of slaves on the same
network cannot exchange new writes while the master is unreachable, however
well connected they are to each other. What gossip buys is faster
distribution of confirmed work and relief from the master's fan-out
bandwidth — not collaboration during a master outage. A deployment that
needs the latter would have to let gossip carry pending operations, which
would give up the property that the master has seen everything that exists.

Nothing about correctness depends on the master being reachable. A partition
costs pending-write latency and unreclaimed space, both bounded by its
duration. Promoting a slave to master is a membership decision and MUST
publish a new epoch; a replica MUST reject a stable position whose epoch it
knows to be superseded, so a demoted master cannot authorize reclamation.

### Masked-Edge Reclamation

A live-but-masked edge occurrence (endpoint removed) MAY be physically
dropped once the removing operation's dot is covered by the master's stable
watermark, and MUST NOT be dropped before then.
Dropping it earlier could resurrect the edge as visible if a replica
independently re-delivered it alongside a surviving endpoint — which gossip
makes more likely, since redelivery by an alternate path is its normal mode
rather than a retry.

## Durability and Recovery Integration

- Remote deltas are ordinary WAL batches: they receive contiguous local
  LSNs, participate in group commit, and obey the whole-batch-or-nothing
  replay rule of the storage durability contract.
- The replica's own `context`, its `hlc`, the peer's last acknowledged
  `context`, and the `ReplicaId` are persisted in the checkpoint manifest
  and recovered
  before the first post-restart round.
- Recovery replays WAL, reconstructs `context`, and resumes anti-entropy
  from persisted state. An ambiguous crash between apply and acknowledgment
  is resolved by retry; idempotent join makes re-application safe.

### Fail-Closed Conditions

The session MUST terminate and refuse further replication, surfacing a typed
error, when it observes:

- a peer dot counter regression or a re-used dot with different content
  (fork / restored-backup detection);
- a delta segment with a per-origin gap that the responder did not declare
  as a state transfer;
- a schema fingerprint mismatch;
- a checksum failure in a delta frame (quarantine per the storage spec);
- a gossip peer offering pending, unconfirmed operations, or requesting
  them;
- a stable watermark presented without a membership epoch, or carrying an
  epoch the replica knows to be superseded;
- a delta batch whose greatest observed `physical_millis` exceeds local
  physical time by more than `max_clock_offset` (refused before the clock
  merges anything from it).

Under gossip these failures MUST isolate the offending **session** and leave
local state usable. A replica that poisoned itself on a bad peer would let
one misconfigured node take down every replica that happened to gossip with
it, which in a mesh is eventually all of them. The isolation is per-peer:
the session terminates, the peer is dropped from the partial view, and other
sessions continue. This is a deliberate departure from the single-session
case, where terminating the session and failing closed are the same act.
Conditions that indicate damaged local state, such as a checksum failure in
an already-applied frame, still fail the replica closed.

## Consistency Boundary

- Per replica, transactions keep their existing strict local guarantees;
  replication never makes uncommitted or non-durable remote state visible.
- Across replicas the guarantee is **strong eventual consistency**: replicas
  that have applied the same set of deltas have identical CRDT state, and
  under continued anti-entropy all replicas converge. There is no global
  total order and no cross-replica uniqueness enforcement; DDL constraints
  that require global uniqueness beyond logical-key merge semantics MUST be
  rejected while replication is enabled.
- DDL replication is out of scope for this revision: schema changes deploy
  out-of-band to both replicas and are guarded by the fingerprint check.

## Verification

`docs/tla/SkeinCrdtReplication.tla` is the executable model of this
contract, checked by `scripts/check-storage-tla.sh`. The model abstracts
delta segments as state joins (their semantic foundation) and checks:

| Contract obligation | Model invariant / property |
| --- | --- |
| Equal delivered contexts imply identical state | `ConvergedWhenContextsEqual` |
| Continued anti-entropy converges both replicas | `EventualConvergence` under sync fairness |
| Every live record and property timestamp is causally covered | `LiveRecordsAreCovered` |
| A received edge never references an unknown occurrence | `EdgeEndpointsCovered` |
| Observed-removed dots never resurrect | `RemovedDotsStayRemoved` |
| Occurrence dots are unique and contexts never exceed minted ops | `DotsAreUnique`, `ContextBoundsMintedDots` |
| Visible-graph referential integrity | By construction via `VisibleEdges`; masking is exercised by the `DETACH DELETE` vs concurrent edge-create interleavings |
| An acknowledged peer context never runs ahead of what that peer applied | `AcknowledgedContextNeverExceedsPeer` |
| A replica's confirmation position never runs past the log | `ConfirmedWithinLog` in `SkeinGossipDelivery.tla` |
| Gossip never carries pending, unconfirmed work | `HeldIsConfirmedOrOwn` |
| Fair rounds deliver every confirmed operation to every slave | `EventualDelivery` |
| Slaves converge on the confirmed prefix while the master is unreachable | `SlavesAgreeWithoutMaster` under `SlaveFairSpec` |
| A crash between a durable apply and its acknowledgement is safe to retry | `Crash` drops only the owed acknowledgement; the idempotent join keeps `ConvergedWhenContextsEqual` |

The model abstracts the hybrid logical clock as a Lamport clock that ticks
on every mint and merges on every sync round, which is exactly the
causality obligation stated above. The encoding, the durability of the
clock across restarts, and the `max_clock_offset` bound are outside the
model: the first two are representation, and the third is a quantitative
drift property that a state-machine model expresses poorly. They rest on
review and implementation tests. It does not check transport security, delta-segment encoding, or
the WAL durability boundary; the last is covered by
`SkeinStorageDurability.tla`.

### Why Masked-Edge Reclamation Is Not Verified Here

Masked-edge GC is deliberately absent from the model rather than merely
unchecked, because adding it at this abstraction would produce false
confidence.

The model represents anti-entropy as a **state join**: every round carries
the sender's full state and context. Under that abstraction, physically
dropping a masked edge is indistinguishable from an ordinary
observed-remove — the receiver sees a dot covered by the sender's context
with no accompanying record and removes it, so convergence holds whether
or not the drop waited for peer acknowledgment. A GC action added here
would therefore pass with the stability guard *and* without it, proving
nothing about the guard.

The hazard the guard exists for lives one level down, in the delta-segment
refinement. A real delta segment carries only the per-origin counter
ranges the peer has not seen. A replica that forgets a masked edge without
retaining a removal the segment can carry has no way to tell a lagging
peer that the edge is gone, and that peer keeps it live. Verifying the
guard therefore requires modeling segments as segments, with their own
retention state — a separate model, not an action bolted onto this one.

Until that model exists, the `MUST NOT` in *Masked-Edge Reclamation* rests
on review and implementation tests, not on machine checking.

### Why Verification Is Layered

Two models cover this contract because one cannot. `SkeinCrdtReplication.tla`
checks what the join computes once a batch arrives and is bounded to two
replicas. `SkeinGossipDelivery.tla` checks what arrives, and by abstracting
the payload to per-origin counters it reaches three nodes and two origins.

Their composition — that fair delivery of causally-ordered prefixes into a
convergent join yields a convergent system — is argued from the join's
commutativity, associativity, and idempotence. It is **not** machine-checked,
because the composed model is exactly the three-replica instance that does
not terminate. A reader should treat gossip convergence as resting on that
argument plus two separately checked halves, not on one end-to-end proof.

### Instance Bound

The checked instance is two replicas, one key, two values, and two
operations per replica, sized by mutation testing rather than by state
count. Three-replica instances were
attempted and are not exhaustively checkable at this shape: tracking each
replica's view of every peer's acknowledged context adds a version vector
per ordered pair, and the smallest three-replica configuration still
exceeded seven million distinct states without terminating. That bound
applies to the join model; the delivery model reaches three nodes because it
abstracts the payload away. Transitive
delivery — where a fault would show as `a` and `c` diverging while each
agrees with `b` — is consequently argued from the join's commutativity and
associativity, not machine-checked. The spec's claim that the rules extend
to N replicas without a format change is a design claim, not a verified
one.
