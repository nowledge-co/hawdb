-------------------- MODULE HawDBExactGraphCounts --------------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* Basic graph counts are canonical mutation state, not sampled optimizer   *)
(* statistics. Each applied record mutation changes the live set and every  *)
(* affected total, label, or relationship-type counter atomically. WAL      *)
(* replay refines the same apply transition.                                *)
(***************************************************************************)

CONSTANTS Nodes, Relationships, Labels, RelTypes, NodeLabels,
          RelationshipType, MaxCommits

ASSUME /\ Nodes /= {}
       /\ Relationships /= {}
       /\ Labels /= {}
       /\ RelTypes /= {}
       /\ IsFiniteSet(Nodes)
       /\ IsFiniteSet(Relationships)
       /\ IsFiniteSet(Labels)
       /\ IsFiniteSet(RelTypes)
       /\ Cardinality(RelTypes) >= 2
       /\ NodeLabels \in [Nodes -> SUBSET Labels]
       /\ RelationshipType \in [Relationships -> RelTypes]
       /\ MaxCommits \in Nat

VARIABLES liveNodes, liveRelationships, totalNodeCount,
          totalRelationshipCount, labelCounts, relTypeCounts, commits

vars == <<liveNodes, liveRelationships, totalNodeCount,
          totalRelationshipCount, labelCounts, relTypeCounts, commits>>

CanonicalLabelCount(label) ==
    Cardinality({node \in liveNodes : label \in NodeLabels[node]})

CanonicalRelTypeCount(relType) ==
    Cardinality({relationship \in liveRelationships :
        RelationshipType[relationship] = relType})

FirstNode == CHOOSE node \in Nodes : TRUE
FirstRelationship == CHOOSE relationship \in Relationships : TRUE
FirstRelType == CHOOSE relType \in RelTypes : TRUE
SecondRelType == CHOOSE relType \in RelTypes \ {FirstRelType} : TRUE

ModelNodeLabels ==
    [node \in Nodes |-> IF node = FirstNode THEN Labels ELSE {}]

ModelRelationshipType ==
    [relationship \in Relationships |->
        IF relationship = FirstRelationship THEN FirstRelType ELSE SecondRelType]

Init ==
    /\ liveNodes = {}
    /\ liveRelationships = {}
    /\ totalNodeCount = 0
    /\ totalRelationshipCount = 0
    /\ labelCounts = [label \in Labels |-> 0]
    /\ relTypeCounts = [relType \in RelTypes |-> 0]
    /\ commits = 0

InsertNode ==
    /\ commits < MaxCommits
    /\ \E node \in Nodes \ liveNodes:
        /\ liveNodes' = liveNodes \cup {node}
        /\ totalNodeCount' = totalNodeCount + 1
        /\ labelCounts' = [label \in Labels |->
            labelCounts[label] + IF label \in NodeLabels[node] THEN 1 ELSE 0]
        /\ commits' = commits + 1
        /\ UNCHANGED <<liveRelationships, totalRelationshipCount,
                       relTypeCounts>>

DeleteNode ==
    /\ commits < MaxCommits
    /\ \E node \in liveNodes:
        /\ liveNodes' = liveNodes \ {node}
        /\ totalNodeCount' = totalNodeCount - 1
        /\ labelCounts' = [label \in Labels |->
            labelCounts[label] - IF label \in NodeLabels[node] THEN 1 ELSE 0]
        /\ commits' = commits + 1
        /\ UNCHANGED <<liveRelationships, totalRelationshipCount,
                       relTypeCounts>>

InsertRelationship ==
    /\ commits < MaxCommits
    /\ \E relationship \in Relationships \ liveRelationships:
        /\ liveRelationships' = liveRelationships \cup {relationship}
        /\ totalRelationshipCount' = totalRelationshipCount + 1
        /\ relTypeCounts' = [relType \in RelTypes |->
            relTypeCounts[relType]
                + IF relType = RelationshipType[relationship] THEN 1 ELSE 0]
        /\ commits' = commits + 1
        /\ UNCHANGED <<liveNodes, totalNodeCount, labelCounts>>

DeleteRelationship ==
    /\ commits < MaxCommits
    /\ \E relationship \in liveRelationships:
        /\ liveRelationships' = liveRelationships \ {relationship}
        /\ totalRelationshipCount' = totalRelationshipCount - 1
        /\ relTypeCounts' = [relType \in RelTypes |->
            relTypeCounts[relType]
                - IF relType = RelationshipType[relationship] THEN 1 ELSE 0]
        /\ commits' = commits + 1
        /\ UNCHANGED <<liveNodes, totalNodeCount, labelCounts>>

Next == InsertNode \/ DeleteNode \/ InsertRelationship \/ DeleteRelationship

TypeOK ==
    /\ liveNodes \subseteq Nodes
    /\ liveRelationships \subseteq Relationships
    /\ totalNodeCount \in Nat
    /\ totalRelationshipCount \in Nat
    /\ labelCounts \in [Labels -> Nat]
    /\ relTypeCounts \in [RelTypes -> Nat]
    /\ commits \in 0..MaxCommits

FastCountQueriesMatchCanonical ==
    /\ totalNodeCount = Cardinality(liveNodes)
    /\ totalRelationshipCount = Cardinality(liveRelationships)
    /\ \A label \in Labels:
        labelCounts[label] = CanonicalLabelCount(label)
    /\ \A relType \in RelTypes:
        relTypeCounts[relType] = CanonicalRelTypeCount(relType)

Spec == Init /\ [][Next]_vars

=============================================================================
