------------------------ MODULE HawDBPropertyIndexPruning ------------------------
(***************************************************************************)
(* Scan pruning against declared property indexes, per the Property Index  *)
(* section of docs/STORAGE.md.                                             *)
(*                                                                         *)
(* Pruning is an optimization: replacing a full scan with an index lookup  *)
(* must not change which nodes a filter selects. That is the whole content *)
(* of this model, and it is what makes the declared-index design safe to   *)
(* adopt. An index that covers every property is trivially safe to trust;  *)
(* one that covers only declared properties is not, and the pruner has to  *)
(* know the difference.                                                    *)
(*                                                                         *)
(* The model deliberately keeps two evaluations of the same filter side by *)
(* side — the pruned one and the ground truth — so that any disagreement   *)
(* is a violated invariant rather than a silently wrong answer, which is   *)
(* how this class of bug presents in a database.                           *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Naturals

CONSTANTS Nodes, Properties, Values

ASSUME Nodes # {} /\ Properties # {} /\ Values # {}

VARIABLES
    present,   \* [Nodes -> BOOLEAN]: the node is live
    property,  \* [Nodes -> [Properties -> Values \cup {"absent"}]]
    declared,  \* SUBSET Properties: properties with a declared index
    index      \* [Properties -> [Values -> SUBSET Nodes]]: index content

vars == <<present, property, declared, index>>

Slots == Values \cup {"absent"}

\* Ground truth: the nodes a filter selects, computed without any index.
Matching(prop, value) ==
    {node \in Nodes : present[node] /\ property[node][prop] = value}

Init ==
    /\ present = [node \in Nodes |-> FALSE]
    /\ property = [node \in Nodes |-> [prop \in Properties |-> "absent"]]
    /\ declared = {}
    /\ index = [prop \in Properties |-> [value \in Values |-> {}]]

\* Creating a node indexes it under every declared property and no others.
\* An undeclared property is deliberately left out of the index, which is
\* the write-side saving this design exists for.
CreateNode(node, assignment) ==
    /\ ~present[node]
    /\ present' = [present EXCEPT ![node] = TRUE]
    /\ property' = [property EXCEPT ![node] = assignment]
    /\ index' = [prop \in Properties |->
                    [value \in Values |->
                        IF prop \in declared /\ assignment[prop] = value
                          THEN index[prop][value] \cup {node}
                          ELSE index[prop][value]]]
    /\ UNCHANGED declared

\* Removing a node retracts it from every index it could appear in.
DeleteNode(node) ==
    /\ present[node]
    /\ present' = [present EXCEPT ![node] = FALSE]
    /\ property' = [property EXCEPT ![node] = [prop \in Properties |-> "absent"]]
    /\ index' = [prop \in Properties |->
                    [value \in Values |-> index[prop][value] \ {node}]]
    /\ UNCHANGED declared

\* Declaring an index must backfill the nodes that already exist. Without
\* this the index would be missing exactly the rows written before the
\* declaration, and the pruner would trust it.
DeclareIndex(prop) ==
    /\ prop \notin declared
    /\ declared' = declared \cup {prop}
    /\ index' = [index EXCEPT ![prop] =
                    [value \in Values |-> Matching(prop, value)]]
    /\ UNCHANGED <<present, property>>

\* Dropping an index releases its content, after which the pruner must stop
\* trusting it.
DropIndex(prop) ==
    /\ prop \in declared
    /\ declared' = declared \ {prop}
    /\ index' = [index EXCEPT ![prop] = [value \in Values |-> {}]]
    /\ UNCHANGED <<present, property>>

Next ==
    \/ \E node \in Nodes, assignment \in [Properties -> Slots] :
        CreateNode(node, assignment)
    \/ \E node \in Nodes : DeleteNode(node)
    \/ \E prop \in Properties : DeclareIndex(prop)
    \/ \E prop \in Properties : DropIndex(prop)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------

\* The pruner's contract. A candidate set is offered only for a declared
\* property; otherwise it declines and the caller scans. `Pruned` models the
\* answer a query actually returns under each outcome.
Prunes(prop) == prop \in declared
Pruned(prop, value) ==
    IF Prunes(prop) THEN index[prop][value] ELSE Matching(prop, value)

(* An exact two-branch OR is admitted only when both equality indexes are   *)
(* declared. Set union gives the executor's candidate-identity dedup rule.  *)
UnionMatching(prop1, value1, prop2, value2) ==
    Matching(prop1, value1) \cup Matching(prop2, value2)
UnionPrunes(prop1, prop2) == prop1 \in declared /\ prop2 \in declared
UnionPruned(prop1, value1, prop2, value2) ==
    IF UnionPrunes(prop1, prop2)
      THEN index[prop1][value1] \cup index[prop2][value2]
      ELSE UnionMatching(prop1, value1, prop2, value2)

TypeOK ==
    /\ present \in [Nodes -> BOOLEAN]
    /\ property \in [Nodes -> [Properties -> Slots]]
    /\ declared \subseteq Properties
    /\ \A prop \in Properties, value \in Values :
        index[prop][value] \subseteq Nodes

\* The property this model exists for: pruning is invisible in the result.
\* A query answered from an index returns exactly what a full scan would.
PruningPreservesResults ==
    \A prop \in Properties, value \in Values :
        Pruned(prop, value) = Matching(prop, value)

UnionPruningPreservesResults ==
    \A prop1, prop2 \in Properties, value1, value2 \in Values :
        UnionPruned(prop1, value1, prop2, value2) =
            UnionMatching(prop1, value1, prop2, value2)

\* A declared index is complete: it holds every live node carrying the
\* value, so trusting it as exact is justified.
DeclaredIndexIsComplete ==
    \A prop \in declared, value \in Values :
        index[prop][value] = Matching(prop, value)

\* An undeclared property carries no index content at all, so a pruner that
\* consulted one would read an empty set rather than a stale one.
UndeclaredPropertyHasNoIndex ==
    \A prop \in Properties \ declared, value \in Values :
        index[prop][value] = {}

\* Index entries never name a node that is gone.
IndexReferencesLiveNodes ==
    \A prop \in Properties, value \in Values :
        \A node \in index[prop][value] : present[node]

=============================================================================
