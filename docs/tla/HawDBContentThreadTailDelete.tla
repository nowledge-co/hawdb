------------------ MODULE HawDBContentThreadTailDelete ------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A Thread tail delete selects exact occurrence identities, then stages   *)
(* graph count, message rows, message anchors, and the document summary in  *)
(* one workspace. Only the complete durable target may become visible.     *)
(***************************************************************************)

CONSTANT MaxEpoch

ASSUME MaxEpoch \in Nat \ {0}

Messages == {"a", "b", "c", "d"}
RetainedMessages == {"a", "b"}
TailMessages == {"c", "d"}

Anchors == {"head", "explicit", "legacy", "citation"}
MessageAnchors == {"head", "explicit", "legacy"}
TailMessageAnchors == {"explicit", "legacy"}
RetainedAnchors == {"head", "citation"}

MessagePayloadValues ==
    {"payload-a", "payload-b", "payload-c", "payload-d"}
AnchorPayloadValues ==
    {"anchor-head", "anchor-explicit", "anchor-legacy", "anchor-citation"}

MessagePayloads ==
    [message \in Messages |->
        CASE message = "a" -> "payload-a"
          [] message = "b" -> "payload-b"
          [] message = "c" -> "payload-c"
          [] OTHER -> "payload-d"]

AnchorPayloads ==
    [anchor \in Anchors |->
        CASE anchor = "head" -> "anchor-head"
          [] anchor = "explicit" -> "anchor-explicit"
          [] anchor = "legacy" -> "anchor-legacy"
          [] OTHER -> "anchor-citation"]

AnchorOccurrence ==
    [anchor \in MessageAnchors |->
        CASE anchor = "head" -> "a"
          [] anchor = "explicit" -> "c"
          [] OTHER -> "d"]

InitialThread ==
    [messages |-> Messages,
     anchors |-> Anchors,
     graphCount |-> Cardinality(Messages),
     documentCount |-> Cardinality(Messages),
     messagePayload |-> MessagePayloads,
     anchorPayload |-> AnchorPayloads]

TargetThread ==
    [messages |-> RetainedMessages,
     anchors |-> RetainedAnchors,
     graphCount |-> Cardinality(RetainedMessages),
     documentCount |-> Cardinality(RetainedMessages),
     messagePayload |-> MessagePayloads,
     anchorPayload |-> AnchorPayloads]

ThreadStates ==
    [messages : SUBSET Messages,
     anchors : SUBSET Anchors,
     graphCount : 0..Cardinality(Messages),
     documentCount : 0..Cardinality(Messages),
     messagePayload : [Messages -> MessagePayloadValues],
     anchorPayload : [Anchors -> AnchorPayloadValues]]

Phases == {"idle", "active", "durable"}

HistoryState(history, epoch) ==
    IF epoch = 0 THEN InitialThread ELSE history[epoch]

VARIABLES
    canonical,
    workspace,
    candidates,
    commitEpoch,
    durableHistory,
    phase,
    emptyNoopObserved,
    emptyNoopStateBefore,
    emptyNoopStateAfter,
    emptyNoopEpochBefore,
    emptyNoopEpochAfter

vars == <<
    canonical,
    workspace,
    candidates,
    commitEpoch,
    durableHistory,
    phase,
    emptyNoopObserved,
    emptyNoopStateBefore,
    emptyNoopStateAfter,
    emptyNoopEpochBefore,
    emptyNoopEpochAfter
>>

Init ==
    /\ canonical = InitialThread
    /\ workspace = InitialThread
    /\ candidates = {}
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ emptyNoopObserved = FALSE
    /\ emptyNoopStateBefore = InitialThread
    /\ emptyNoopStateAfter = InitialThread
    /\ emptyNoopEpochBefore = 0
    /\ emptyNoopEpochAfter = 0

CheckEmptyTail ==
    /\ phase = "idle"
    /\ ~emptyNoopObserved
    /\ emptyNoopObserved' = TRUE
    /\ emptyNoopStateBefore' = canonical
    /\ emptyNoopStateAfter' = canonical
    /\ emptyNoopEpochBefore' = commitEpoch
    /\ emptyNoopEpochAfter' = commitEpoch
    /\ UNCHANGED <<
        canonical, workspace, candidates, commitEpoch, durableHistory, phase
       >>

BeginTailDelete ==
    /\ phase = "idle"
    /\ canonical = InitialThread
    /\ commitEpoch < MaxEpoch
    /\ workspace' = canonical
    /\ candidates' = TailMessages
    /\ phase' = "active"
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, emptyNoopObserved,
        emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

StageGraphCount ==
    /\ phase = "active"
    /\ workspace.graphCount # Cardinality(RetainedMessages)
    /\ workspace' =
         [workspace EXCEPT !.graphCount = Cardinality(RetainedMessages)]
    /\ UNCHANGED <<
        canonical, candidates, commitEpoch, durableHistory, phase,
        emptyNoopObserved, emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

StageMessageDelete ==
    /\ phase = "active"
    /\ workspace.messages # RetainedMessages
    /\ workspace' = [workspace EXCEPT !.messages = RetainedMessages]
    /\ UNCHANGED <<
        canonical, candidates, commitEpoch, durableHistory, phase,
        emptyNoopObserved, emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

StageMessageAnchorDelete ==
    /\ phase = "active"
    /\ workspace.anchors # RetainedAnchors
    /\ workspace' = [workspace EXCEPT !.anchors = RetainedAnchors]
    /\ UNCHANGED <<
        canonical, candidates, commitEpoch, durableHistory, phase,
        emptyNoopObserved, emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

StageDocumentSummary ==
    /\ phase = "active"
    /\ workspace.documentCount # Cardinality(RetainedMessages)
    /\ workspace' =
         [workspace EXCEPT !.documentCount = Cardinality(RetainedMessages)]
    /\ UNCHANGED <<
        canonical, candidates, commitEpoch, durableHistory, phase,
        emptyNoopObserved, emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

MakeDurable ==
    /\ phase = "active"
    /\ workspace = TargetThread
    /\ durableHistory' = Append(durableHistory, workspace)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonical, workspace, candidates, commitEpoch,
        emptyNoopObserved, emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

Publish ==
    /\ phase = "durable"
    /\ canonical' = workspace
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ UNCHANGED <<
        workspace, candidates, durableHistory, emptyNoopObserved,
        emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

Rollback ==
    /\ phase = "active"
    /\ workspace' = canonical
    /\ candidates' = {}
    /\ phase' = "idle"
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, emptyNoopObserved,
        emptyNoopStateBefore, emptyNoopStateAfter,
        emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recovered == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonical' = recovered
    /\ workspace' = recovered
    /\ candidates' = {}
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ UNCHANGED <<
        durableHistory, emptyNoopObserved, emptyNoopStateBefore,
        emptyNoopStateAfter, emptyNoopEpochBefore, emptyNoopEpochAfter
       >>

Next ==
    \/ CheckEmptyTail
    \/ BeginTailDelete
    \/ StageGraphCount
    \/ StageMessageDelete
    \/ StageMessageAnchorDelete
    \/ StageDocumentSummary
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonical \in ThreadStates
    /\ workspace \in ThreadStates
    /\ candidates \in SUBSET Messages
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(ThreadStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ emptyNoopObserved \in BOOLEAN
    /\ emptyNoopStateBefore \in ThreadStates
    /\ emptyNoopStateAfter \in ThreadStates
    /\ emptyNoopEpochBefore \in 0..MaxEpoch
    /\ emptyNoopEpochAfter \in 0..MaxEpoch

VisibilityFollowsDurability ==
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ canonical = HistoryState(durableHistory, commitEpoch)

CanonicalTailDeleteIsAtomic ==
    canonical \in {InitialThread, TargetThread}

DurableTailDeleteIsComplete ==
    \A index \in 1..Len(durableHistory):
        durableHistory[index] = TargetThread

PreparedTailDeleteIsComplete ==
    phase = "durable" => workspace = TargetThread

SelectedTailIsExact ==
    phase \in {"active", "durable"} => candidates = TailMessages

PublishedCountsAreExact ==
    /\ canonical.graphCount = Cardinality(canonical.messages)
    /\ canonical.documentCount = Cardinality(canonical.messages)

PublishedMessageAnchorsHaveOccurrences ==
    \A anchor \in canonical.anchors \intersect MessageAnchors:
        AnchorOccurrence[anchor] \in canonical.messages

RetainedPayloadsAreImmutable ==
    /\ canonical.messagePayload = MessagePayloads
    /\ workspace.messagePayload = MessagePayloads
    /\ canonical.anchorPayload = AnchorPayloads
    /\ workspace.anchorPayload = AnchorPayloads

NonMessageAnchorSurvives ==
    /\ "citation" \in canonical.anchors
    /\ "citation" \in workspace.anchors

DeletedTailIsAbsentAfterPublication ==
    canonical = TargetThread =>
        /\ canonical.messages \intersect TailMessages = {}
        /\ canonical.anchors \intersect TailMessageAnchors = {}

EmptyTailIsNoop ==
    emptyNoopObserved =>
        /\ emptyNoopStateBefore = emptyNoopStateAfter
        /\ emptyNoopEpochBefore = emptyNoopEpochAfter

=============================================================================
