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
master. Two link types carry deltas: a session between each slave and the
master, and gossip directly between slaves.

The split that makes this work is that only delivery differs. The replicated
state, the dot identity, the join, and the conflict matrix are the same on
every link, so an operation learned through gossip and the same operation
learned from the master converge to the same value. What the master adds is
authority — over membership, and therefore over reclamation — not a
different merge rule and not a linearization of writes.

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
- A **causal context** is a version vector `VV: ReplicaId -> counter`. Because
  per-replica counters are contiguous and deltas are delivered as per-origin
  gap-free prefixes, the context stays a compact vector; no dot cloud is
  required.
- **Occurrence identity.** A `CREATE`/first-`MERGE` mints a fresh occurrence
  dot for the created node or relationship. Occurrence identity MUST NOT be
  derived from a payload hash or from the logical key. A delete, recreate,
  and re-delete of the same logical entity therefore produces three distinct,
  durable, totally-ordered-per-origin identities, and stale-sequence
  regressions are detectable by counter comparison.

## Replicated State

The replicated object is the canonical graph only. Its CRDT state per replica
is:

- `context`: the replica's causal context (a version vector).
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

1. **Digest.** The requester sends its persisted `context`.
2. **Delta response.** The responder replies with its own `context` plus, for
   each origin replica, the gap-free segment of op deltas whose counters lie
   above the requester's context entry, in counter order. The responder MUST
   NOT ship a segment with holes; if requested history has been compacted
   away it MUST ship its full joined state for the affected origin instead
   (state transfer degenerate case).
3. **Join.** The receiver applies the batch as one local transaction through
   the normal WAL and group-commit path, using the join rules:
   - A local live record whose dot is covered by the sender's context and
     absent from the sender's shipped state is removed.
   - A shipped record whose dot is covered by the receiver's context and
     locally absent is discarded (it was removed here).
   - A record present on both sides merges property registers per-key by
     greatest `ts`.
   - `context := pointwise-max(context, sender context)`.
4. **Acknowledgment.** Only after the WAL sync boundary makes the joined
   batch durable does the receiver acknowledge its new `context` to the peer.
   The responder MAY use acknowledged peer contexts for delta retention and
   masked-edge GC, and MUST NOT use unacknowledged ones.

Delivery obligations:

- **Per-origin FIFO prefixes.** Together with join closure this guarantees
  causal delivery: a shipped edge occurrence's endpoint dots are always
  covered by the receiver's context once the batch is applied. A batch that
  would violate this MUST be rejected before apply.
- **Idempotent retry.** The join is idempotent, commutative, and
  associative. After a crash, a lost acknowledgment, or a duplicated
  response, the receiver simply re-runs a round from its persisted context.
- **No epoch skew.** Both replicas MUST run the same schema fingerprint; a
  delta batch carries the fingerprint and MUST fail closed on mismatch.

## Roles and Delivery

A deployment is one **master** and any number of **slaves**. Every replica,
master and slave alike, mints operations locally; a slave that loses its
master keeps accepting writes. That is what makes this a CRDT rather than
replication: the conflict matrix above only earns its keep when more than
one replica writes.

The master is **not** a linearizer. Its operations carry dots like any
other replica's, and the join is commutative, associative, and idempotent,
so a slave that learns an operation through gossip and a slave that learns
the same operation from the master reach the same value. What distinguishes
the master is authority over membership and reclamation, plus being the
durable hub that any two slaves can always reach each other through.

Two link types carry the exchange defined above, and they differ only in
who talks to whom:

- **Master session.** Each slave runs the digest, delta, join, acknowledge
  exchange with the master. The master records each slave's acknowledged
  context.
- **Slave gossip.** Slaves run the same exchange directly with each other,
  selecting up to `fanout` peers from a partial view each round, in
  push-pull form.

The master does not gossip. It has a session with every slave, so gossiping
would add paths without adding reachability, and keeping it out of the mesh
keeps its acknowledged-context bookkeeping the single authoritative view.

Gossip changes three properties of delivery, and each has a consequence
below: an operation reaches a slave by more than one path, so **duplicates**
are normal; paths have different lengths, so segments for one origin arrive
**out of order**; and a partial view means **no slave knows the membership**.

Duplicates are already handled: the join is idempotent, so a redelivered
delta changes nothing, and digest-before-delta keeps the cost of a duplicate
at one round trip rather than one payload.

### Causal Prefix Under Out-of-Order Arrival

`context` remains a version vector, and a version vector can only express a
contiguous per-origin prefix. Under gossip a replica may hold origin `R`'s
counters 1..5 from one path and 8..10 from another; the vector cannot say
that.

A replica MUST NOT apply a delta whose counter exceeds its context entry for
that origin by more than one. Deltas above the contiguous frontier are held
in a bounded per-origin **reorder buffer** and are not applied, not counted
in `context`, and not visible to reads. When the gap fills, the buffer
drains in counter order.

Advancing the contiguous frontier MUST evict every buffered counter the
advance swallowed, whether the frontier moved by draining or by an ordinary
delta arriving. A counter that remains buffered after being applied would be
applied a second time when the buffer next drains. Duplicate delivery makes
this reachable rather than theoretical: the same counter routinely arrives
both as a held out-of-order delta and, later, as an in-order one.

If the reorder buffer for an origin would exceed its bound, the replica MUST
NOT apply with a gap. It requests the state-transfer form of the exchange
for that origin, which is the degenerate case already defined above. That
keeps three properties the compact context depends on: `context` always
denotes an applied contiguous prefix, causal delivery still holds because an
edge's endpoints precede it on its origin, and an overflow fails toward a
more expensive exchange rather than toward a silent hole.

Dot clouds or interval sets would remove the buffer, at the cost of an
unbounded context and a join that no longer reduces to pointwise maximum.
This design keeps the compact context and pays with a bounded buffer.

### Stability and Membership

Retention and masked-edge reclamation both require knowing that an operation
is stable — that every replica has it. The master answers that question and
nothing else does: it holds a session with every slave, records each slave's
acknowledged context, and publishes a membership epoch naming the current
replica set. The **stable watermark** is the pointwise minimum of the
acknowledged contexts of every member of the current epoch.

A slave MUST NOT derive stability from gossip. A partial view cannot
distinguish "no other slave exists" from "a slave I have never heard of
exists", and the second case is exactly what a premature reclamation
corrupts. Concretely:

- A replica MAY reclaim only against a stable watermark carrying a current
  membership epoch from the master.
- A replica MUST NOT reclaim on the basis of contexts it observed through
  gossip, however many peers agreed.

### Losing the Master

A slave that cannot reach the master keeps working, and the degradation is
deliberately asymmetric:

- It **keeps accepting local writes**, minting dots as usual.
- It **keeps converging with other slaves** over gossip, so a partitioned
  group of slaves still agrees among itself.
- It **stops reclaiming**, because the watermark it would need is stale.
  Masked edges and delta history accumulate until the master returns.

Space is therefore the only thing a master outage costs, and it is bounded
by the outage. Nothing about correctness depends on the master being
reachable, which is what keeps a cloud outage from making local writes
unsafe. Promoting a slave to master is a membership decision and MUST
publish a new membership epoch; a replica MUST reject a watermark whose
epoch it knows to be superseded, so a demoted master cannot authorize
reclamation.

### Metadata Growth

A version vector carries one entry per replica that has ever minted an
operation, so its size grows with cumulative membership rather than live
membership. Retiring an entry requires knowing that its replica will never
mint again, which is a membership decision and therefore the master's;
absent a current epoch, entries are retained.

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
- a reorder buffer that would exceed its bound without the peer offering the
  state-transfer form;
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
| A delta is never applied across a per-origin hole | `AppliedPrefixWasReceived` in `SkeinGossipDelivery.tla` |
| The reorder buffer stays above the frontier, bounded, and disjoint from the applied prefix | `BufferIsAboveFrontier`, `BufferRespectsBound`, `BufferAndPrefixAreDisjoint` |
| Fair rounds deliver every operation to every replica | `EventualDelivery` |
| Slaves converge with each other while the master is unreachable | `SlavesAgreeWithoutMaster` under `SlaveFairSpec` |
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
exceeded seven million distinct states without terminating. Transitive
delivery — where a fault would show as `a` and `c` diverging while each
agrees with `b` — is consequently argued from the join's commutativity and
associativity, not machine-checked. The spec's claim that the rules extend
to N replicas without a format change is a design claim, not a verified
one.
