------------------------- MODULE HawDBCrdtReplication -------------------------
(***************************************************************************)
(* Delta-state CRDT replication of the canonical property graph between   *)
(* HawDB replicas, per docs/specs/HAWDB_CRDT_REPLICATION_SPEC.md.         *)
(*                                                                         *)
(* Node and edge occurrences form ORSWOT-style observed-remove sets:       *)
(* every committed mutation mints a dot <<replica, counter>> with          *)
(* contiguous per-replica counters, the causal context is a version        *)
(* vector, and a dot covered by the context but absent from the live sets  *)
(* is removed without a tombstone. Properties are per-occurrence LWW       *)
(* registers ordered by <<lamport, replica>>, where the Lamport clock      *)
(* ticks on every local mint and merges on every sync — the abstraction    *)
(* of the specification's hybrid logical clock. Edges bind endpoint        *)
(* occurrence dots; an edge whose endpoint occurrence was removed stays in *)
(* CRDT state but is masked from the visible graph, so DETACH DELETE      *)
(* dominates a concurrent incident edge create. Anti-entropy is modeled   *)
(* as the state join that per-origin gap-free delta segments refine.       *)
(*                                                                         *)
(* Replicas are modeled as integers so the LWW replica tie-break is a     *)
(* total order, matching the ReplicaId tie-break in the specification.    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Replicas, Keys, Values, MaxOpsPerReplica

ASSUME Replicas \subseteq Nat /\ Replicas # {}
ASSUME MaxOpsPerReplica \in Nat \ {0}

VARIABLES
    opCount,  \* [Replicas -> Nat]: dots minted locally, contiguous from 1
    acked,    \* [Replicas -> [Replicas -> vector]]: peer contexts acknowledged as durable
    owedAck,  \* [Replicas -> [Replicas -> BOOLEAN]]: applied batches not yet acknowledged
    clock,    \* [Replicas -> Nat]: Lamport clock for LWW timestamps
    context,  \* [Replicas -> [Replicas -> Nat]]: causal context vector
    nodes,    \* [Replicas -> set of node occurrence records]
    edges,    \* [Replicas -> set of edge occurrence records]
    removed   \* ghost history: dots ever observed-removed at each replica

vars == <<opCount, clock, acked, owedAck, context, nodes, edges, removed>>

ClockBound == Cardinality(Replicas) * MaxOpsPerReplica
Dots == Replicas \X (1..MaxOpsPerReplica)
Timestamps == (1..ClockBound) \X Replicas
NodeRecords == [key: Keys, dot: Dots, val: Values, ts: Timestamps]
EdgeRecords == [dot: Dots, src: Dots, dst: Dots]

Covered(ctx, d) == d[2] <= ctx[d[1]]
TsLeq(s, t) == s[1] < t[1] \/ (s[1] = t[1] /\ s[2] <= t[2])
Max(a, b) == IF a >= b THEN a ELSE b

NodeDots(r) == {n.dot : n \in nodes[r]}
EdgeDots(r) == {e.dot : e \in edges[r]}
LiveOccurrences(r, k) == {n \in nodes[r] : n.key = k}

\* Visible graph: an edge is readable only while both endpoint occurrences
\* are live. Referential integrity holds by construction of this projection.
VisibleEdges(r) ==
    {e \in edges[r] : e.src \in NodeDots(r) /\ e.dst \in NodeDots(r)}

Init ==
    /\ opCount = [r \in Replicas |-> 0]
    /\ clock = [r \in Replicas |-> 0]
    /\ acked = [r \in Replicas |-> [s \in Replicas |-> [t \in Replicas |-> 0]]]
    /\ owedAck = [r \in Replicas |-> [s \in Replicas |-> FALSE]]
    /\ context = [r \in Replicas |-> [s \in Replicas |-> 0]]
    /\ nodes = [r \in Replicas |-> {}]
    /\ edges = [r \in Replicas |-> {}]
    /\ removed = [r \in Replicas |-> {}]

CanMint(r) == opCount[r] < MaxOpsPerReplica
MintedDot(r) == <<r, opCount[r] + 1>>
MintedTs(r) == <<clock[r] + 1, r>>

\* Every committed mutation, including a delete, advances the local
\* component of the causal context, so the context is a complete op
\* counter and delta segments stay gap-free per origin. The Lamport
\* clock ticks with every mint so a causally-later property write always
\* carries a greater timestamp than any register it has observed.
Advance(r) ==
    /\ opCount' = [opCount EXCEPT ![r] = @ + 1]
    /\ clock' = [clock EXCEPT ![r] = @ + 1]
    /\ context' = [context EXCEPT ![r][r] = @ + 1]

\* CREATE / first MERGE: mint a fresh occurrence dot. The visible-match
\* guard is MERGE semantics and bounds the local occurrence count;
\* concurrent creates on distinct replicas still yield multiple
\* occurrences of one logical key after synchronization.
CreateNode(r, k, v) ==
    /\ CanMint(r)
    /\ LiveOccurrences(r, k) = {}
    /\ Advance(r)
    /\ nodes' = [nodes EXCEPT ![r] = @ \cup
           {[key |-> k, dot |-> MintedDot(r), val |-> v, ts |-> MintedTs(r)]}]
    /\ UNCHANGED <<edges, removed, acked, owedAck>>

\* SET on a matched logical entity writes the LWW register of every
\* locally live occurrence of the key with one fresh timestamp.
SetProperty(r, k, v) ==
    /\ CanMint(r)
    /\ LiveOccurrences(r, k) # {}
    /\ Advance(r)
    /\ nodes' = [nodes EXCEPT ![r] =
           {IF n.key = k THEN [n EXCEPT !.val = v, !.ts = MintedTs(r)] ELSE n
              : n \in @}]
    /\ UNCHANGED <<edges, removed, acked, owedAck>>

\* DETACH DELETE: observed-remove of the locally live occurrences of the
\* key and of every locally live incident edge dot, in one delta.
DetachDeleteNode(r, k) ==
    /\ CanMint(r)
    /\ LiveOccurrences(r, k) # {}
    /\ Advance(r)
    /\ LET victims == {n.dot : n \in LiveOccurrences(r, k)}
           deadEdges == {e \in edges[r] : e.src \in victims \/ e.dst \in victims}
       IN /\ nodes' = [nodes EXCEPT ![r] = @ \ LiveOccurrences(r, k)]
          /\ edges' = [edges EXCEPT ![r] = @ \ deadEdges]
          /\ removed' = [removed EXCEPT ![r] =
                 @ \cup victims \cup {e.dot : e \in deadEdges}]
    /\ UNCHANGED <<acked, owedAck>>

CreateEdge(r) ==
    /\ CanMint(r)
    /\ \E s \in nodes[r], t \in nodes[r] :
        /\ s.dot # t.dot
        /\ ~\E e \in edges[r] : e.src = s.dot /\ e.dst = t.dot
        /\ Advance(r)
        /\ edges' = [edges EXCEPT ![r] = @ \cup
               {[dot |-> MintedDot(r), src |-> s.dot, dst |-> t.dot]}]
    /\ UNCHANGED <<nodes, removed, acked, owedAck>>

DeleteEdge(r) ==
    /\ CanMint(r)
    /\ \E e \in VisibleEdges(r) :
        /\ Advance(r)
        /\ edges' = [edges EXCEPT ![r] = @ \ {e}]
        /\ removed' = [removed EXCEPT ![r] = @ \cup {e.dot}]
    /\ UNCHANGED <<nodes, acked, owedAck>>

\* ORSWOT join for node occurrences. A record present on one side only
\* survives iff the other side's context has not covered (removed) it.
\* A record present on both sides merges its LWW register by timestamp.
JoinNodes(sn, sctx, rn, rctx) ==
    LET senderDots == {n.dot : n \in sn}
        receiverDots == {n.dot : n \in rn}
        At(s, d) == CHOOSE n \in s : n.dot = d
        Newest(m, n) == IF TsLeq(m.ts, n.ts) THEN n ELSE m
    IN {n \in rn : n.dot \notin senderDots /\ ~Covered(sctx, n.dot)}
       \cup {n \in sn : n.dot \notin receiverDots /\ ~Covered(rctx, n.dot)}
       \cup {Newest(At(sn, d), At(rn, d)) : d \in senderDots \cap receiverDots}

JoinEdges(se, sctx, re, rctx) ==
    LET senderDots == {e.dot : e \in se}
        receiverDots == {e.dot : e \in re}
    IN {e \in re : e.dot \in senderDots \/ ~Covered(sctx, e.dot)}
       \cup {e \in se : e.dot \in receiverDots \/ ~Covered(rctx, e.dot)}

\* One anti-entropy round: receiver b pulls from sender a and joins. The
\* implementation ships per-origin gap-free delta segments above b's
\* context; the join of those segments equals this state join, and
\* idempotence of the join is what makes crash-and-retry delivery safe.
Sync(a, b) ==
    /\ a # b
    /\ LET newNodes == JoinNodes(nodes[a], context[a], nodes[b], context[b])
           newEdges == JoinEdges(edges[a], context[a], edges[b], context[b])
           keptDots == {n.dot : n \in newNodes} \cup {e.dot : e \in newEdges}
           dropped == (NodeDots(b) \cup EdgeDots(b)) \ keptDots
       IN /\ nodes' = [nodes EXCEPT ![b] = newNodes]
          /\ edges' = [edges EXCEPT ![b] = newEdges]
          /\ context' = [context EXCEPT ![b] =
                 [s \in Replicas |-> Max(context[b][s], context[a][s])]]
          /\ clock' = [clock EXCEPT ![b] = Max(@, clock[a])]
          /\ removed' = [removed EXCEPT ![b] = @ \cup dropped]
          /\ owedAck' = [owedAck EXCEPT ![b][a] = TRUE]
    /\ UNCHANGED <<opCount, acked>>

\* A receiver acknowledges only after its joined batch is durable, so the
\* sender's view of a peer never runs ahead of what that peer has applied.
\* Retention and masked-edge reclamation may read acknowledged contexts;
\* they must never read an unacknowledged one.
Ack(b, a) ==
    /\ a # b
    /\ owedAck[b][a]
    /\ acked' = [acked EXCEPT ![a][b] = context[b]]
    /\ owedAck' = [owedAck EXCEPT ![b][a] = FALSE]
    /\ UNCHANGED <<opCount, clock, context, nodes, edges, removed>>

\* A crash between a durable apply and its acknowledgement loses only the
\* acknowledgement. Durable CRDT state survives, and the round is retried.
\* The join is idempotent, so re-applying the same batch changes nothing.
Crash(r) ==
    /\ \E s \in Replicas : owedAck[r][s]
    /\ owedAck' = [owedAck EXCEPT ![r] = [s \in Replicas |-> FALSE]]
    /\ UNCHANGED <<opCount, clock, acked, context, nodes, edges, removed>>

Next ==
    \/ \E r \in Replicas, k \in Keys, v \in Values : CreateNode(r, k, v)
    \/ \E r \in Replicas, k \in Keys, v \in Values : SetProperty(r, k, v)
    \/ \E r \in Replicas, k \in Keys : DetachDeleteNode(r, k)
    \/ \E r \in Replicas : CreateEdge(r)
    \/ \E r \in Replicas : DeleteEdge(r)
    \/ \E a \in Replicas, b \in Replicas : Sync(a, b)
    \/ \E a \in Replicas, b \in Replicas : Ack(b, a)
    \/ \E r \in Replicas : Crash(r)

Spec == Init /\ [][Next]_vars

\* Anti-entropy rounds keep running; local mutations stay optional.
FairSpec ==
    Spec
    /\ \A a \in Replicas, b \in Replicas : WF_vars(Sync(a, b))
    /\ \A p \in Replicas, q \in Replicas : WF_vars(Ack(q, p))

-----------------------------------------------------------------------------

TypeOK ==
    /\ opCount \in [Replicas -> 0..MaxOpsPerReplica]
    /\ clock \in [Replicas -> 0..ClockBound]
    /\ acked \in [Replicas -> [Replicas -> [Replicas -> 0..MaxOpsPerReplica]]]
    /\ owedAck \in [Replicas -> [Replicas -> BOOLEAN]]
    /\ context \in [Replicas -> [Replicas -> 0..MaxOpsPerReplica]]
    /\ \A r \in Replicas :
        /\ nodes[r] \subseteq NodeRecords
        /\ edges[r] \subseteq EdgeRecords
        /\ removed[r] \subseteq Dots

\* An occurrence dot identifies exactly one record.
DotsAreUnique ==
    \A r \in Replicas :
        /\ \A m \in nodes[r], n \in nodes[r] : m.dot = n.dot => m = n
        /\ \A d \in edges[r], e \in edges[r] : d.dot = e.dot => d = e
        /\ NodeDots(r) \cap EdgeDots(r) = {}

\* A context never claims coverage of ops its origin has not minted, and
\* the local component counts exactly the locally minted ops.
ContextBoundsMintedDots ==
    \A r \in Replicas :
        /\ context[r][r] = opCount[r]
        /\ \A s \in Replicas : context[r][s] <= opCount[s]

\* Causal closure: every live record is covered by the local context, and
\* the local Lamport clock dominates every observed register timestamp,
\* so the next local property write always wins over what it overwrote.
LiveRecordsAreCovered ==
    \A r \in Replicas :
        /\ \A n \in nodes[r] :
            /\ Covered(context[r], n.dot)
            /\ n.ts[1] <= clock[r]
        /\ \A e \in edges[r] : Covered(context[r], e.dot)

\* A shipped edge never references an occurrence outside the receiver's
\* causal context: the per-origin prefix delivery obligation.
EdgeEndpointsCovered ==
    \A r \in Replicas :
        \A e \in edges[r] :
            Covered(context[r], e.src) /\ Covered(context[r], e.dst)

\* Observed-remove is final: a dot a replica dropped never becomes live
\* again there, whether by local ops, retry, or re-delivery.
RemovedDotsStayRemoved ==
    \A r \in Replicas :
        removed[r] \cap (NodeDots(r) \cup EdgeDots(r)) = {}

\* An acknowledged context is always a prefix of what its owner has really
\* applied. Retention and masked-edge reclamation are gated on these values,
\* so believing a peer is further ahead than it is would discard state the
\* peer still needs.
AcknowledgedContextNeverExceedsPeer ==
    \A r \in Replicas, s \in Replicas :
        \A t \in Replicas : acked[r][s][t] <= context[s][t]

\* Strong eventual consistency, safety half: replicas whose contexts are
\* equal have applied the same deltas and hold identical CRDT state.
ConvergedWhenContextsEqual ==
    \A a \in Replicas, b \in Replicas :
        context[a] = context[b] =>
            /\ nodes[a] = nodes[b]
            /\ edges[a] = edges[b]
            /\ clock[a] = clock[b]

\* Strong eventual consistency, liveness half under fair anti-entropy.
EventualConvergence ==
    <>[](\A a \in Replicas, b \in Replicas :
            /\ context[a] = context[b]
            /\ nodes[a] = nodes[b]
            /\ edges[a] = edges[b])

=============================================================================
