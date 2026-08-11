--------------------------- MODULE SkeinGossipDelivery ---------------------------
(***************************************************************************)
(* Gossip delivery of CRDT deltas, per the Delivery Topologies section of  *)
(* docs/specs/SKEIN_CRDT_REPLICATION_SPEC.md.                              *)
(*                                                                         *)
(* This model is deliberately layered against SkeinCrdtReplication.tla     *)
(* rather than merged into it. That model checks what the join computes    *)
(* once a batch arrives; this one checks what arrives. Keeping them apart  *)
(* is what makes three or more replicas checkable: the payload is          *)
(* abstracted to per-origin counters, so nodes, edges, properties, and     *)
(* clocks are all absent here.                                            *)
(*                                                                         *)
(* Under gossip a delta reaches a node by more than one path, so arrival   *)
(* is duplicated and out of order. `applied` is the contiguous per-origin  *)
(* prefix a node has applied, which is exactly what a version-vector       *)
(* context can express. Anything above that frontier waits in a bounded    *)
(* reorder buffer. An overflow must fall back to state transfer rather     *)
(* than apply across a hole.                                              *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Naturals

CONSTANTS Nodes, Master, Origins, MaxOps, BufferBound

ASSUME Master \in Nodes
ASSUME Origins \subseteq Nodes /\ Origins # {}
ASSUME MaxOps \in Nat \ {0}
ASSUME BufferBound \in Nat

VARIABLES
    minted,   \* [Origins -> Nat]: operations an origin has minted
    applied,  \* [Nodes -> [Origins -> Nat]]: contiguous applied prefix
    buffer,   \* [Nodes -> [Origins -> SUBSET counters]]: held above the frontier
    received  \* ghost: counters actually delivered to a node, applied or not

vars == <<minted, applied, buffer, received>>

Counters == 1..MaxOps
Slaves == Nodes \ {Master}

\* The master holds a session with every slave; slaves gossip with each
\* other. The master does not gossip: it already reaches every slave, so
\* adding it to the mesh would add paths without adding reachability.
Linked(a, b) == a # b /\ (a = Master \/ b = Master \/ {a, b} \subseteq Slaves)

\* A partitioned master is unreachable in both directions while slaves keep
\* gossiping among themselves.
SlaveLinked(a, b) == a \in Slaves /\ b \in Slaves /\ a # b

Init ==
    /\ minted = [o \in Origins |-> 0]
    /\ applied = [n \in Nodes |-> [o \in Origins |-> 0]]
    /\ buffer = [n \in Nodes |-> [o \in Origins |-> {}]]
    /\ received = [n \in Nodes |-> [o \in Origins |-> {}]]

\* An origin mints locally and has applied its own operation by construction.
Mint(o) ==
    /\ minted[o] < MaxOps
    /\ minted' = [minted EXCEPT ![o] = @ + 1]
    /\ applied' = [applied EXCEPT ![o][o] = @ + 1]
    /\ received' = [received EXCEPT ![o][o] = @ \cup {minted[o] + 1}]
    /\ UNCHANGED buffer

\* Everything a node can pass on: its applied prefix plus what it is holding.
Holds(n, o) == (1..applied[n][o]) \cup buffer[n][o]

\* One gossip delivery of a single counter from a to b. Selecting an
\* arbitrary held counter models both alternate-path reordering and the
\* partial views that make delivery order unpredictable; redelivery of an
\* already-applied counter models the duplicates gossip produces normally.
Deliver(a, b, o, c) ==
    /\ Linked(a, b)
    /\ c \in Holds(a, o)
    /\ IF c <= applied[b][o] + 1
         THEN \* At or below the frontier: applying is idempotent, and a
              \* counter exactly at the frontier extends it. Advancing the
              \* frontier must also evict what it swallowed, or the same
              \* counter would sit in both the prefix and the buffer.
              LET frontier == IF c = applied[b][o] + 1
                                THEN applied[b][o] + 1
                                ELSE applied[b][o]
              IN /\ applied' = [applied EXCEPT ![b][o] = frontier]
                 /\ buffer' = [buffer EXCEPT ![b][o] = {d \in @ : d > frontier}]
         ELSE \* Above the frontier: hold it, but never beyond the bound.
              /\ Cardinality(buffer[b][o] \cup {c}) <= BufferBound
              /\ buffer' = [buffer EXCEPT ![b][o] = @ \cup {c}]
              /\ UNCHANGED applied
    /\ received' = [received EXCEPT ![b][o] = @ \cup {c}]
    /\ UNCHANGED minted

\* The gap filled, so the buffer drains in counter order.
Drain(n, o) ==
    /\ (applied[n][o] + 1) \in buffer[n][o]
    /\ applied' = [applied EXCEPT ![n][o] = @ + 1]
    /\ buffer' = [buffer EXCEPT ![n][o] = {d \in @ : d > applied[n][o] + 1}]
    /\ UNCHANGED <<minted, received>>

\* The escape hatch a bounded buffer needs: rather than apply across a hole,
\* the node takes the sender's whole contiguous prefix for that origin.
StateTransfer(a, b, o) ==
    /\ Linked(a, b)
    /\ applied[a][o] > applied[b][o]
    /\ applied' = [applied EXCEPT ![b][o] = applied[a][o]]
    /\ buffer' = [buffer EXCEPT ![b][o] = {c \in @ : c > applied[a][o]}]
    /\ received' = [received EXCEPT ![b][o] = @ \cup (1..applied[a][o])]
    /\ UNCHANGED minted

Next ==
    \/ \E o \in Origins : Mint(o)
    \/ \E a \in Nodes, b \in Nodes, o \in Origins, c \in Counters : Deliver(a, b, o, c)
    \/ \E n \in Nodes, o \in Origins : Drain(n, o)
    \/ \E a \in Nodes, b \in Nodes, o \in Origins : StateTransfer(a, b, o)

Spec == Init /\ [][Next]_vars

\* Rounds keep happening between every ordered pair, and a node that can
\* drain eventually does. Minting stays optional.
FairSpec ==
    Spec
    /\ \A s \in Nodes, t \in Nodes, p \in Origins :
        WF_vars(StateTransfer(s, t, p))
    /\ \A u \in Nodes, v \in Nodes, q \in Origins, k \in Counters :
        WF_vars(Deliver(u, v, q, k))
    /\ \A n \in Nodes, r \in Origins : WF_vars(Drain(n, r))

-----------------------------------------------------------------------------

TypeOK ==
    /\ minted \in [Origins -> 0..MaxOps]
    /\ applied \in [Nodes -> [Origins -> 0..MaxOps]]
    /\ \A n \in Nodes, o \in Origins : buffer[n][o] \subseteq Counters
    /\ \A n \in Nodes, o \in Origins : received[n][o] \subseteq Counters

\* A node never claims a prefix its origin has not minted.
AppliedWithinMinted ==
    \A n \in Nodes, o \in Origins : applied[n][o] <= minted[o]

\* The reorder buffer holds only counters strictly above the frontier, so
\* the applied prefix stays contiguous and a version vector can express it.
BufferIsAboveFrontier ==
    \A n \in Nodes, o \in Origins :
        \A c \in buffer[n][o] : c > applied[n][o]

\* The bound is a hard one: an overflow must become a state transfer, never
\* an apply across a hole.
BufferRespectsBound ==
    \A n \in Nodes, o \in Origins : Cardinality(buffer[n][o]) <= BufferBound

\* Buffered work is invisible until applied, so a held counter is never
\* counted twice once the gap closes.
BufferAndPrefixAreDisjoint ==
    \A n \in Nodes, o \in Origins :
        buffer[n][o] \cap (1..applied[n][o]) = {}

\* Delivery never invents an operation: anything a node holds, applied or
\* buffered, was minted by its origin.
HeldWorkWasMinted ==
    \A n \in Nodes, o \in Origins :
        \A c \in Holds(n, o) : c <= minted[o]

\* The claim the reorder buffer exists to make: a node's applied prefix
\* contains only operations it actually received, so no delta was ever
\* applied across a hole on the strength of a later one arriving first.
AppliedPrefixWasReceived ==
    \A n \in Nodes, o \in Origins :
        (1..applied[n][o]) \subseteq received[n][o]

\* Liveness: fair rounds drive every node to the full minted prefix, with
\* nothing stranded in a buffer.
EventualDelivery ==
    <>[](\A n \in Nodes, o \in Origins :
            applied[n][o] = minted[o] /\ buffer[n][o] = {})

\* What a master outage must not break: slaves still agree with each other.
\* Checked against `SlaveFairSpec`, where only slave-to-slave links are fair,
\* so the master may stall forever. Slaves converge on everything that
\* reached the slave set; operations stranded on the master are out of reach
\* by construction and are excluded.
SlavesAgreeWithoutMaster ==
    <>[](\A a \in Slaves, b \in Slaves, o \in Origins :
            applied[a][o] = applied[b][o])

\* Only slave-to-slave rounds are fair here: the master may stall forever,
\* which is the partition case.
SlaveFairSpec ==
    Spec
    /\ \A g \in Nodes, h \in Nodes, w \in Origins :
        SlaveLinked(g, h) => WF_vars(StateTransfer(g, h, w))
    /\ \A x \in Nodes, y \in Nodes, z \in Origins, j \in Counters :
        SlaveLinked(x, y) => WF_vars(Deliver(x, y, z, j))
    /\ \A m \in Nodes, e \in Origins : WF_vars(Drain(m, e))

=============================================================================
