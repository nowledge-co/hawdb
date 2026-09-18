-------------------- MODULE HawDBContentThreadDelete --------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A whole-Thread delete discovers the exact relational document closure, *)
(* then removes graph and relational ownership in one durable transaction. *)
(* Missing and repeated deletes stop after a read-only preflight.           *)
(***************************************************************************)

CONSTANT MaxEpoch

ASSUME MaxEpoch \in Nat \ {0}

GraphMessages == {"graph-a", "graph-b"}
RelationalMessages == {"row-a", "row-b", "row-legacy"}
ThreadIdentities == {"public", "input"}
Documents == {"owned-empty", "legacy-message"}
Anchors == {"owned-anchor", "legacy-anchor"}
UnrelatedPayloads == {"retained"}

InitialThread ==
    [thread |-> TRUE,
     graphMessages |-> GraphMessages,
     relationalMessages |-> RelationalMessages,
     identities |-> ThreadIdentities,
     documents |-> Documents,
     anchors |-> Anchors,
     unrelated |-> "retained"]

DeletedThread ==
    [thread |-> FALSE,
     graphMessages |-> {},
     relationalMessages |-> {},
     identities |-> {},
     documents |-> {},
     anchors |-> {},
     unrelated |-> "retained"]

ThreadStates ==
    [thread : BOOLEAN,
     graphMessages : SUBSET GraphMessages,
     relationalMessages : SUBSET RelationalMessages,
     identities : SUBSET ThreadIdentities,
     documents : SUBSET Documents,
     anchors : SUBSET Anchors,
     unrelated : UnrelatedPayloads]

Phases == {"idle", "active", "durable"}

RecoveredState(history) ==
    IF Len(history) = 0 THEN InitialThread ELSE history[Len(history)]

VARIABLES
    canonical,
    workspace,
    discoveredDocuments,
    commitEpoch,
    durableHistory,
    phase,
    missingNoopObserved,
    missingNoopStateBefore,
    missingNoopStateAfter,
    missingNoopEpochBefore,
    missingNoopEpochAfter,
    repeatedNoopObserved,
    repeatedNoopStateBefore,
    repeatedNoopStateAfter,
    repeatedNoopEpochBefore,
    repeatedNoopEpochAfter

vars == <<
    canonical,
    workspace,
    discoveredDocuments,
    commitEpoch,
    durableHistory,
    phase,
    missingNoopObserved,
    missingNoopStateBefore,
    missingNoopStateAfter,
    missingNoopEpochBefore,
    missingNoopEpochAfter,
    repeatedNoopObserved,
    repeatedNoopStateBefore,
    repeatedNoopStateAfter,
    repeatedNoopEpochBefore,
    repeatedNoopEpochAfter
>>

Init ==
    /\ canonical = InitialThread
    /\ workspace = InitialThread
    /\ discoveredDocuments = {}
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ missingNoopObserved = FALSE
    /\ missingNoopStateBefore = InitialThread
    /\ missingNoopStateAfter = InitialThread
    /\ missingNoopEpochBefore = 0
    /\ missingNoopEpochAfter = 0
    /\ repeatedNoopObserved = FALSE
    /\ repeatedNoopStateBefore = InitialThread
    /\ repeatedNoopStateAfter = InitialThread
    /\ repeatedNoopEpochBefore = 0
    /\ repeatedNoopEpochAfter = 0

CheckMissingThread ==
    /\ phase = "idle"
    /\ canonical = InitialThread
    /\ ~missingNoopObserved
    /\ missingNoopObserved' = TRUE
    /\ missingNoopStateBefore' = canonical
    /\ missingNoopStateAfter' = canonical
    /\ missingNoopEpochBefore' = commitEpoch
    /\ missingNoopEpochAfter' = commitEpoch
    /\ UNCHANGED <<
        canonical, workspace, discoveredDocuments, commitEpoch,
        durableHistory, phase, repeatedNoopObserved,
        repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

BeginDelete ==
    /\ phase = "idle"
    /\ canonical = InitialThread
    /\ commitEpoch < MaxEpoch
    /\ workspace' = canonical
    /\ discoveredDocuments' = Documents
    /\ phase' = "active"
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

StageGraphMessages ==
    /\ phase = "active"
    /\ workspace.graphMessages # {}
    /\ workspace' = [workspace EXCEPT !.graphMessages = {}]
    /\ UNCHANGED <<
        canonical, discoveredDocuments, commitEpoch, durableHistory, phase,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

StageThreadIdentities ==
    /\ phase = "active"
    /\ workspace.identities # {}
    /\ workspace' = [workspace EXCEPT !.identities = {}]
    /\ UNCHANGED <<
        canonical, discoveredDocuments, commitEpoch, durableHistory, phase,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

StageGraphThread ==
    /\ phase = "active"
    /\ workspace.thread
    /\ workspace' = [workspace EXCEPT !.thread = FALSE]
    /\ UNCHANGED <<
        canonical, discoveredDocuments, commitEpoch, durableHistory, phase,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

StageAnchors ==
    /\ phase = "active"
    /\ workspace.anchors # {}
    /\ workspace' = [workspace EXCEPT !.anchors = {}]
    /\ UNCHANGED <<
        canonical, discoveredDocuments, commitEpoch, durableHistory, phase,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

StageRelationalMessages ==
    /\ phase = "active"
    /\ workspace.relationalMessages # {}
    /\ workspace' = [workspace EXCEPT !.relationalMessages = {}]
    /\ UNCHANGED <<
        canonical, discoveredDocuments, commitEpoch, durableHistory, phase,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

StageDocuments ==
    /\ phase = "active"
    /\ workspace.documents # {}
    /\ workspace' = [workspace EXCEPT !.documents = {}]
    /\ UNCHANGED <<
        canonical, discoveredDocuments, commitEpoch, durableHistory, phase,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

MakeDurable ==
    /\ phase = "active"
    /\ workspace = DeletedThread
    /\ durableHistory' = Append(durableHistory, workspace)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonical, workspace, discoveredDocuments, commitEpoch,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

Publish ==
    /\ phase = "durable"
    /\ canonical' = workspace
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ UNCHANGED <<
        workspace, discoveredDocuments, durableHistory,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

Rollback ==
    /\ phase = "active"
    /\ workspace' = canonical
    /\ discoveredDocuments' = {}
    /\ phase' = "idle"
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory,
        missingNoopObserved, missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

CheckRepeatedDelete ==
    /\ phase = "idle"
    /\ canonical = DeletedThread
    /\ ~repeatedNoopObserved
    /\ repeatedNoopObserved' = TRUE
    /\ repeatedNoopStateBefore' = canonical
    /\ repeatedNoopStateAfter' = canonical
    /\ repeatedNoopEpochBefore' = commitEpoch
    /\ repeatedNoopEpochAfter' = commitEpoch
    /\ UNCHANGED <<
        canonical, workspace, discoveredDocuments, commitEpoch,
        durableHistory, phase, missingNoopObserved,
        missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter
       >>

CrashRecover ==
    LET recovered == RecoveredState(durableHistory) IN
    /\ canonical' = recovered
    /\ workspace' = recovered
    /\ discoveredDocuments' = {}
    /\ commitEpoch' = Len(durableHistory)
    /\ phase' = "idle"
    /\ UNCHANGED <<
        durableHistory, missingNoopObserved,
        missingNoopStateBefore, missingNoopStateAfter,
        missingNoopEpochBefore, missingNoopEpochAfter,
        repeatedNoopObserved, repeatedNoopStateBefore, repeatedNoopStateAfter,
        repeatedNoopEpochBefore, repeatedNoopEpochAfter
       >>

Next ==
    \/ CheckMissingThread
    \/ BeginDelete
    \/ StageGraphMessages
    \/ StageThreadIdentities
    \/ StageGraphThread
    \/ StageAnchors
    \/ StageRelationalMessages
    \/ StageDocuments
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CheckRepeatedDelete
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonical \in ThreadStates
    /\ workspace \in ThreadStates
    /\ discoveredDocuments \in SUBSET Documents
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(ThreadStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ missingNoopObserved \in BOOLEAN
    /\ missingNoopStateBefore \in ThreadStates
    /\ missingNoopStateAfter \in ThreadStates
    /\ missingNoopEpochBefore \in 0..MaxEpoch
    /\ missingNoopEpochAfter \in 0..MaxEpoch
    /\ repeatedNoopObserved \in BOOLEAN
    /\ repeatedNoopStateBefore \in ThreadStates
    /\ repeatedNoopStateAfter \in ThreadStates
    /\ repeatedNoopEpochBefore \in 0..MaxEpoch
    /\ repeatedNoopEpochAfter \in 0..MaxEpoch

VisibilityFollowsDurability ==
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)

CanonicalDeleteIsAtomic ==
    canonical = InitialThread \/ canonical = DeletedThread

DurableDeleteIsComplete ==
    \A index \in 1..Len(durableHistory) : durableHistory[index] = DeletedThread

PreparedDeleteIsComplete ==
    phase = "durable" => workspace = DeletedThread

SelectedDocumentClosureIsExact ==
    discoveredDocuments = {} \/ discoveredDocuments = Documents

UnrelatedPayloadIsImmutable ==
    /\ canonical.unrelated = "retained"
    /\ workspace.unrelated = "retained"
    /\ \A index \in 1..Len(durableHistory) :
          durableHistory[index].unrelated = "retained"

PublishedDeleteIsComplete ==
    canonical = DeletedThread =>
        /\ ~canonical.thread
        /\ canonical.graphMessages = {}
        /\ canonical.relationalMessages = {}
        /\ canonical.identities = {}
        /\ canonical.documents = {}
        /\ canonical.anchors = {}

MissingPreflightIsNoop ==
    missingNoopObserved =>
        /\ missingNoopStateAfter = missingNoopStateBefore
        /\ missingNoopEpochAfter = missingNoopEpochBefore

RepeatedPreflightIsNoop ==
    repeatedNoopObserved =>
        /\ repeatedNoopStateBefore = DeletedThread
        /\ repeatedNoopStateAfter = repeatedNoopStateBefore
        /\ repeatedNoopEpochAfter = repeatedNoopEpochBefore

=============================================================================
