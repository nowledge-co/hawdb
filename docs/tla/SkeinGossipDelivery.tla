--------------------------- MODULE SkeinGossipDelivery ---------------------------
(***************************************************************************)
(* Delivery for the master-slave CRDT contract, per the Roles and Delivery *)
(* section of docs/specs/SKEIN_CRDT_REPLICATION_SPEC.md.                   *)
(*                                                                         *)
(* Layered against SkeinCrdtReplication.tla rather than merged into it.    *)
(* That model checks what the join computes once a batch arrives; this one *)
(* checks what arrives, and what is allowed to. Keeping them apart is what *)
(* makes three or more replicas checkable.                                 *)
(*                                                                         *)
(* The rule this model exists to check is that a slave's local operation   *)
(* stays pending until the master confirms it, and that gossip carries     *)
(* confirmed operations only. Pending work reaches the master over the     *)
(* session and nowhere else, so the master has seen everything that exists *)
(* anywhere in the deployment.                                             *)
(*                                                                         *)
(* Because gossip then carries one totally ordered log, a digest is a      *)
(* single integer and a response is a contiguous run, which is why no      *)
(* reorder buffer appears here. `held` is explicit state rather than       *)
(* derived, so that an action shipping pending work is expressible and     *)
(* therefore catchable.                                                    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Naturals

CONSTANTS Nodes, Master, MaxOps

ASSUME Master \in Nodes
ASSUME MaxOps \in Nat \ {0}

VARIABLES
    minted,     \* [Nodes -> Nat]: operations a replica has minted
    logPos,     \* [Ops -> Nat]: confirmation position, 0 while unconfirmed
    logLen,     \* Nat: length of the master's confirmation log
    confirmed,  \* [Nodes -> Nat]: confirmation position a replica has applied
    held        \* [Nodes -> SUBSET Ops]: operations a replica actually has

vars == <<minted, logPos, logLen, confirmed, held>>

Slaves == Nodes \ {Master}
Ops == Nodes \X (1..MaxOps)
MintedBy(n) == {op \in Ops : op[1] = n /\ op[2] <= minted[n]}
ConfirmedOps == {op \in Ops : logPos[op] > 0}
PrefixAt(position) == {op \in Ops : logPos[op] > 0 /\ logPos[op] <= position}

\* A replica's own operation that the master has not yet confirmed.
Pending(n) == MintedBy(n) \ ConfirmedOps

Init ==
    /\ minted = [n \in Nodes |-> 0]
    /\ logPos = [op \in Ops |-> 0]
    /\ logLen = 0
    /\ confirmed = [n \in Nodes |-> 0]
    /\ held = [n \in Nodes |-> {}]

\* A slave writes locally. The operation is pending: held here and nowhere
\* else until the master confirms it.
MintSlave(s) ==
    /\ s \in Slaves
    /\ minted[s] < MaxOps
    /\ minted' = [minted EXCEPT ![s] = @ + 1]
    /\ held' = [held EXCEPT ![s] = @ \cup {<<s, minted[s] + 1>>}]
    /\ UNCHANGED <<logPos, logLen, confirmed>>

\* The master writes, which confirms in the same step because it appends to
\* its own log.
MintMaster ==
    /\ minted[Master] < MaxOps
    /\ minted' = [minted EXCEPT ![Master] = @ + 1]
    /\ logPos' = [logPos EXCEPT ![<<Master, minted[Master] + 1>>] = logLen + 1]
    /\ logLen' = logLen + 1
    /\ confirmed' = [confirmed EXCEPT ![Master] = logLen + 1]
    /\ held' = [held EXCEPT ![Master] = @ \cup {<<Master, minted[Master] + 1>>}]

\* The session: a slave pushes one pending operation and the master appends
\* it. This is the only way pending work leaves the replica that minted it.
Confirm(s, op) ==
    /\ s \in Slaves
    /\ op \in Pending(s)
    /\ op \in held[s]
    /\ logPos' = [logPos EXCEPT ![op] = logLen + 1]
    /\ logLen' = logLen + 1
    /\ confirmed' = [confirmed EXCEPT ![Master] = logLen + 1]
    /\ held' = [held EXCEPT ![Master] = @ \cup {op}]
    /\ UNCHANGED minted

\* The session, other direction: a slave pulls the confirmed run above its
\* position.
PullFromMaster(s) ==
    /\ s \in Slaves
    /\ confirmed[Master] > confirmed[s]
    /\ confirmed' = [confirmed EXCEPT ![s] = confirmed[Master]]
    /\ held' = [held EXCEPT ![s] = @ \cup PrefixAt(confirmed[Master])]
    /\ UNCHANGED <<minted, logPos, logLen>>

\* Gossip: one slave catches another up on the confirmation log and on
\* nothing else. The digest is a single position and the response is the
\* contiguous run above it, so a hole cannot appear.
Gossip(a, b) ==
    /\ a \in Slaves /\ b \in Slaves /\ a # b
    /\ confirmed[a] > confirmed[b]
    /\ confirmed' = [confirmed EXCEPT ![b] = confirmed[a]]
    /\ held' = [held EXCEPT ![b] = @ \cup PrefixAt(confirmed[a])]
    /\ UNCHANGED <<minted, logPos, logLen>>

Next ==
    \/ \E s \in Nodes : MintSlave(s)
    \/ MintMaster
    \/ \E s \in Nodes, op \in Ops : Confirm(s, op)
    \/ \E s \in Nodes : PullFromMaster(s)
    \/ \E a \in Nodes, b \in Nodes : Gossip(a, b)

Spec == Init /\ [][Next]_vars

\* Sessions and gossip rounds keep happening, and pending work keeps being
\* offered for confirmation. Minting stays optional.
FairSpec ==
    Spec
    /\ \A s \in Nodes, op \in Ops : WF_vars(Confirm(s, op))
    /\ \A t \in Nodes : WF_vars(PullFromMaster(t))
    /\ \A a \in Nodes, b \in Nodes : WF_vars(Gossip(a, b))

\* Only gossip is fair here: the master may stall forever, which is the
\* partition case.
SlaveFairSpec ==
    Spec
    /\ \A g \in Nodes, h \in Nodes : WF_vars(Gossip(g, h))

-----------------------------------------------------------------------------

TypeOK ==
    /\ minted \in [Nodes -> 0..MaxOps]
    /\ logLen \in 0..(Cardinality(Nodes) * MaxOps)
    /\ \A n \in Nodes : held[n] \subseteq Ops

\* The claim this model exists for: a replica holds only confirmed work and
\* its own. A gossip action shipping pending operations would break it.
HeldIsConfirmedOrOwn ==
    \A n \in Nodes : held[n] \subseteq (PrefixAt(confirmed[n]) \cup MintedBy(n))

\* A replica never claims a position beyond the log, and the master's own
\* position is the log itself.
ConfirmedWithinLog ==
    /\ \A n \in Nodes : confirmed[n] <= logLen
    /\ confirmed[Master] = logLen

\* Positions are assigned contiguously and uniquely, which is what lets a
\* digest be one integer.
LogIsContiguousAndUnique ==
    /\ \A op \in Ops : logPos[op] <= logLen
    /\ \A m \in Ops, n \in Ops :
        (logPos[m] > 0 /\ logPos[m] = logPos[n]) => m = n
    /\ \A i \in 1..logLen : \E op \in Ops : logPos[op] = i

\* Catching up by position loses nothing: a replica holds the whole prefix
\* its position names.
PositionImpliesPrefix ==
    \A n \in Nodes : PrefixAt(confirmed[n]) \subseteq held[n]

\* Liveness: fair sessions and rounds confirm every operation and carry it
\* to every replica.
EventualDelivery ==
    <>[](\A n \in Nodes : confirmed[n] = logLen /\ Pending(n) = {})

\* A master outage leaves slaves agreeing on the confirmed prefix. They do
\* not converge on each other's pending work, which is the stated cost of
\* the confirmation rule rather than a defect.
SlavesAgreeWithoutMaster ==
    <>[](\A a \in Slaves, b \in Slaves : confirmed[a] = confirmed[b])

=============================================================================
