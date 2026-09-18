---------------- MODULE HawDBContentThreadReconcile ----------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A Thread reconciliation validates the complete occurrence mapping before *)
(* mutation, then reorders preserved messages and their explicit or legacy   *)
(* anchors while inserting new occurrences in one mixed durable commit.      *)
(***************************************************************************)

CONSTANT MaxEpoch

ASSUME MaxEpoch \in Nat \ {0}

Messages == {"a", "b", "c"}
Anchors == {"a", "b"}
Orders == 0..2
MessagePayloadValues == {"payload-a", "payload-b", "payload-c"}
AnchorPayloadValues == {"legacy-a", "explicit-b"}
Phases == {"idle", "active", "durable"}

MessagePayloads ==
    [message \in Messages |->
        CASE message = "a" -> "payload-a"
          [] message = "b" -> "payload-b"
          [] OTHER -> "payload-c"]

AnchorPayloads ==
    [anchor \in Anchors |->
        IF anchor = "a" THEN "legacy-a" ELSE "explicit-b"]

InitialOrder ==
    [message \in Messages |->
        CASE message = "a" -> 0
          [] message = "b" -> 1
          [] OTHER -> 2]

TargetOrder ==
    [message \in Messages |->
        CASE message = "a" -> 2
          [] message = "b" -> 0
          [] OTHER -> 1]

InitialAnchorOrder ==
    [anchor \in Anchors |-> IF anchor = "a" THEN 0 ELSE 1]

TargetAnchorOrder ==
    [anchor \in Anchors |-> IF anchor = "a" THEN 2 ELSE 0]

InitialThread ==
    [graphUpdated |-> FALSE,
     documentUpdated |-> FALSE,
     present |-> {"a", "b"},
     order |-> InitialOrder,
     anchorOrder |-> InitialAnchorOrder,
     messagePayload |-> MessagePayloads,
     anchorPayload |-> AnchorPayloads,
     summaryCount |-> 2]

TargetThread ==
    [graphUpdated |-> TRUE,
     documentUpdated |-> TRUE,
     present |-> Messages,
     order |-> TargetOrder,
     anchorOrder |-> TargetAnchorOrder,
     messagePayload |-> MessagePayloads,
     anchorPayload |-> AnchorPayloads,
     summaryCount |-> Cardinality(Messages)]

ThreadStates ==
    [graphUpdated : BOOLEAN,
     documentUpdated : BOOLEAN,
     present : SUBSET Messages,
     order : [Messages -> Orders],
     anchorOrder : [Anchors -> Orders],
     messagePayload : [Messages -> MessagePayloadValues],
     anchorPayload : [Anchors -> AnchorPayloadValues],
     summaryCount : 0..Cardinality(Messages)]

HistoryState(history, epoch) ==
    IF epoch = 0 THEN InitialThread ELSE history[epoch]

VARIABLES
    canonical,
    commitEpoch,
    durableHistory,
    phase,
    workspace,
    invalidMappingRejected,
    rejectedCanonical,
    rejectedEpoch

vars == <<
    canonical,
    commitEpoch,
    durableHistory,
    phase,
    workspace,
    invalidMappingRejected,
    rejectedCanonical,
    rejectedEpoch
>>

Init ==
    /\ canonical = InitialThread
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ workspace = InitialThread
    /\ invalidMappingRejected = FALSE
    /\ rejectedCanonical = InitialThread
    /\ rejectedEpoch = 0

RejectInvalidMapping ==
    /\ phase = "idle"
    /\ ~invalidMappingRejected
    /\ invalidMappingRejected' = TRUE
    /\ rejectedCanonical' = canonical
    /\ rejectedEpoch' = commitEpoch
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, phase, workspace>>

BeginReconcile ==
    /\ phase = "idle"
    /\ canonical # TargetThread
    /\ commitEpoch < MaxEpoch
    /\ phase' = "active"
    /\ workspace' = canonical
    /\ invalidMappingRejected' = FALSE
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, rejectedCanonical, rejectedEpoch
       >>

StageGraph ==
    /\ phase = "active"
    /\ ~workspace.graphUpdated
    /\ workspace' = [workspace EXCEPT !.graphUpdated = TRUE]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

StageDocument ==
    /\ phase = "active"
    /\ ~workspace.documentUpdated
    /\ workspace' = [workspace EXCEPT !.documentUpdated = TRUE]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

StageMessageA ==
    /\ phase = "active"
    /\ workspace.order["a"] # TargetOrder["a"]
    /\ workspace' = [workspace EXCEPT !.order["a"] = TargetOrder["a"]]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

StageMessageB ==
    /\ phase = "active"
    /\ workspace.order["b"] # TargetOrder["b"]
    /\ workspace' = [workspace EXCEPT !.order["b"] = TargetOrder["b"]]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

StageLegacyAnchor ==
    /\ phase = "active"
    /\ workspace.anchorOrder["a"] # TargetAnchorOrder["a"]
    /\ workspace' =
         [workspace EXCEPT !.anchorOrder["a"] = TargetAnchorOrder["a"]]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

StageExplicitAnchor ==
    /\ phase = "active"
    /\ workspace.anchorOrder["b"] # TargetAnchorOrder["b"]
    /\ workspace' =
         [workspace EXCEPT !.anchorOrder["b"] = TargetAnchorOrder["b"]]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

InsertMessageC ==
    /\ phase = "active"
    /\ "c" \notin workspace.present
    /\ workspace' =
         [workspace EXCEPT
            !.present = @ \cup {"c"},
            !.order["c"] = TargetOrder["c"]]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

UpdateSummary ==
    /\ phase = "active"
    /\ workspace.summaryCount # Cardinality(Messages)
    /\ workspace' =
         [workspace EXCEPT !.summaryCount = Cardinality(Messages)]
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase,
        invalidMappingRejected, rejectedCanonical, rejectedEpoch
       >>

MakeDurable ==
    /\ phase = "active"
    /\ workspace = TargetThread
    /\ durableHistory' = Append(durableHistory, workspace)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonical, commitEpoch, workspace, invalidMappingRejected,
        rejectedCanonical, rejectedEpoch
       >>

Publish ==
    /\ phase = "durable"
    /\ canonical' = workspace
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ UNCHANGED <<
        durableHistory, workspace, invalidMappingRejected,
        rejectedCanonical, rejectedEpoch
       >>

Rollback ==
    /\ phase = "active"
    /\ phase' = "idle"
    /\ workspace' = canonical
    /\ invalidMappingRejected' = FALSE
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, rejectedCanonical, rejectedEpoch
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recovered == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonical' = recovered
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ workspace' = recovered
    /\ invalidMappingRejected' = FALSE
    /\ rejectedCanonical' = recovered
    /\ rejectedEpoch' = recoveredEpoch
    /\ UNCHANGED durableHistory

Next ==
    \/ RejectInvalidMapping
    \/ BeginReconcile
    \/ StageGraph
    \/ StageDocument
    \/ StageMessageA
    \/ StageMessageB
    \/ StageLegacyAnchor
    \/ StageExplicitAnchor
    \/ InsertMessageC
    \/ UpdateSummary
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonical \in ThreadStates
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(ThreadStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ workspace \in ThreadStates
    /\ invalidMappingRejected \in BOOLEAN
    /\ rejectedCanonical \in ThreadStates
    /\ rejectedEpoch \in 0..MaxEpoch

VisibilityFollowsDurability ==
    canonical = HistoryState(durableHistory, commitEpoch)

CanonicalReconcileIsAtomic ==
    canonical \in {InitialThread, TargetThread}

DurableReconcileIsComplete ==
    \A index \in 1..Len(durableHistory): durableHistory[index] = TargetThread

PreparedReconcileIsComplete ==
    phase = "durable" => workspace = TargetThread

InvalidMappingIsAtomic ==
    invalidMappingRejected =>
        /\ phase = "idle"
        /\ canonical = rejectedCanonical
        /\ commitEpoch = rejectedEpoch

PreservedOccurrencePayloadsAreImmutable ==
    /\ canonical.messagePayload = MessagePayloads
    /\ workspace.messagePayload = MessagePayloads

PreservedAnchorPayloadsAreImmutable ==
    /\ canonical.anchorPayload = AnchorPayloads
    /\ workspace.anchorPayload = AnchorPayloads

PublishedAnchorsFollowOccurrences ==
    \A anchor \in Anchors:
        canonical.anchorOrder[anchor] = canonical.order[anchor]

PublishedSummaryIsExact ==
    canonical.summaryCount = Cardinality(canonical.present)

=============================================================================
