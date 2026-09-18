-------------------- MODULE HawDBTransactionIndexOverlay -------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* One authoritative transaction pins committed row and index views and    *)
(* applies successful statements to bounded private overlays. Reads combine*)
(* the pinned bases and overlays. Rejected statements are atomic across     *)
(* both overlays, and canonical visibility advances only after durability.  *)
(***************************************************************************)

CONSTANT MaxEpoch, MaxOverlayEntries

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ MaxOverlayEntries \in Nat \ {0}

Keys == 1..2
BaseState == {1}
Phases == {"idle", "active", "prepared", "durable"}

ApplyChange(state, key, present) ==
    IF present THEN state \cup {key} ELSE state \ {key}

RangeOf(order) == {order[index] : index \in DOMAIN order}

BaseVisitOrders(base) ==
    {order \in [1..Cardinality(base) -> Keys] : RangeOf(order) = base}

ExactTombstoneMerge(base, current, order) ==
    LET retainedIndices ==
            {index \in DOMAIN order : order[index] \notin (base \ current)}
    IN {order[index] : index \in retainedIndices} \cup (current \ base)

HistoryState(history, epoch) ==
    IF epoch = 0 THEN BaseState ELSE history[epoch]

VARIABLES
    canonicalState,
    commitEpoch,
    durableHistory,
    phase,
    baseEpoch,
    baseIndexState,
    workspaceState,
    workspaceIndexState,
    rowOverlayEntries,
    indexOverlayEntries,
    rowOverlayVersion,
    indexOverlayVersion,
    lastAcceptedState,
    lastAcceptedRowEntries,
    lastAcceptedIndexEntries,
    lastAcceptedRowVersion,
    lastAcceptedIndexVersion,
    statementRejected,
    workspaceRead,
    readState

vars == <<
    canonicalState,
    commitEpoch,
    durableHistory,
    phase,
    baseEpoch,
    baseIndexState,
    workspaceState,
    workspaceIndexState,
    rowOverlayEntries,
    indexOverlayEntries,
    rowOverlayVersion,
    indexOverlayVersion,
    lastAcceptedState,
    lastAcceptedRowEntries,
    lastAcceptedIndexEntries,
    lastAcceptedRowVersion,
    lastAcceptedIndexVersion,
    statementRejected,
    workspaceRead,
    readState
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ baseEpoch = 0
    /\ baseIndexState = BaseState
    /\ workspaceState = BaseState
    /\ workspaceIndexState = BaseState
    /\ rowOverlayEntries = 0
    /\ indexOverlayEntries = 0
    /\ rowOverlayVersion = 0
    /\ indexOverlayVersion = 0
    /\ lastAcceptedState = BaseState
    /\ lastAcceptedRowEntries = 0
    /\ lastAcceptedIndexEntries = 0
    /\ lastAcceptedRowVersion = 0
    /\ lastAcceptedIndexVersion = 0
    /\ statementRejected = FALSE
    /\ workspaceRead = FALSE
    /\ readState = BaseState

Begin ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ phase' = "active"
    /\ baseEpoch' = commitEpoch
    /\ baseIndexState' = canonicalState
    /\ workspaceState' = canonicalState
    /\ workspaceIndexState' = canonicalState
    /\ rowOverlayEntries' = 0
    /\ indexOverlayEntries' = 0
    /\ rowOverlayVersion' = 0
    /\ indexOverlayVersion' = 0
    /\ lastAcceptedState' = canonicalState
    /\ lastAcceptedRowEntries' = 0
    /\ lastAcceptedIndexEntries' = 0
    /\ lastAcceptedRowVersion' = 0
    /\ lastAcceptedIndexVersion' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ readState' = canonicalState
    /\ UNCHANGED <<canonicalState, commitEpoch, durableHistory>>

StageStatement(key, present, rowCost, indexCost) ==
    /\ phase = "active"
    /\ key \in Keys
    /\ present \in BOOLEAN
    /\ rowCost \in 0..MaxOverlayEntries
    /\ indexCost \in 0..MaxOverlayEntries
    /\ rowOverlayVersion < MaxOverlayEntries
    /\ indexOverlayVersion < MaxOverlayEntries
    /\ rowOverlayEntries + rowCost <= MaxOverlayEntries
    /\ indexOverlayEntries + indexCost <= MaxOverlayEntries
    /\ LET next == ApplyChange(workspaceState, key, present) IN
       /\ workspaceState' = next
       /\ workspaceIndexState' = next
       /\ lastAcceptedState' = next
    /\ rowOverlayEntries' = rowOverlayEntries + rowCost
    /\ indexOverlayEntries' = indexOverlayEntries + indexCost
    /\ rowOverlayVersion' = rowOverlayVersion + 1
    /\ indexOverlayVersion' = indexOverlayVersion + 1
    /\ lastAcceptedRowEntries' = rowOverlayEntries + rowCost
    /\ lastAcceptedIndexEntries' = indexOverlayEntries + indexCost
    /\ lastAcceptedRowVersion' = rowOverlayVersion + 1
    /\ lastAcceptedIndexVersion' = indexOverlayVersion + 1
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase,
        baseEpoch, baseIndexState, readState
       >>

RejectStatement(rowCost, indexCost) ==
    /\ phase = "active"
    /\ rowCost \in 0..MaxOverlayEntries
    /\ indexCost \in 0..MaxOverlayEntries
    /\ \/ rowOverlayVersion = MaxOverlayEntries
       \/ indexOverlayVersion = MaxOverlayEntries
       \/ rowOverlayEntries + rowCost > MaxOverlayEntries
       \/ indexOverlayEntries + indexCost > MaxOverlayEntries
    /\ statementRejected' = TRUE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase,
        baseEpoch, baseIndexState, workspaceState, workspaceIndexState,
        rowOverlayEntries, indexOverlayEntries,
        rowOverlayVersion, indexOverlayVersion, lastAcceptedState,
        lastAcceptedRowEntries, lastAcceptedIndexEntries,
        lastAcceptedRowVersion, lastAcceptedIndexVersion, readState
       >>

WorkspaceRead ==
    /\ phase = "active"
    /\ workspaceRead' = TRUE
    /\ readState' = workspaceIndexState
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase,
        baseEpoch, baseIndexState, workspaceState, workspaceIndexState,
        rowOverlayEntries, indexOverlayEntries,
        rowOverlayVersion, indexOverlayVersion, lastAcceptedState,
        lastAcceptedRowEntries, lastAcceptedIndexEntries,
        lastAcceptedRowVersion, lastAcceptedIndexVersion
       >>

PrepareCommit ==
    /\ phase = "active"
    /\ phase' = "prepared"
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory,
        baseEpoch, baseIndexState, workspaceState, workspaceIndexState,
        rowOverlayEntries, indexOverlayEntries,
        rowOverlayVersion, indexOverlayVersion, lastAcceptedState,
        lastAcceptedRowEntries, lastAcceptedIndexEntries,
        lastAcceptedRowVersion, lastAcceptedIndexVersion, readState
       >>

MakeDurable ==
    /\ phase = "prepared"
    /\ durableHistory' = Append(durableHistory, workspaceState)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, baseEpoch, baseIndexState,
        workspaceState, workspaceIndexState,
        rowOverlayEntries, indexOverlayEntries,
        rowOverlayVersion, indexOverlayVersion, lastAcceptedState,
        lastAcceptedRowEntries, lastAcceptedIndexEntries,
        lastAcceptedRowVersion, lastAcceptedIndexVersion,
        statementRejected, workspaceRead, readState
       >>

Publish ==
    /\ phase = "durable"
    /\ canonicalState' = workspaceState
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ rowOverlayEntries' = 0
    /\ indexOverlayEntries' = 0
    /\ rowOverlayVersion' = 0
    /\ indexOverlayVersion' = 0
    /\ lastAcceptedRowEntries' = 0
    /\ lastAcceptedIndexEntries' = 0
    /\ lastAcceptedRowVersion' = 0
    /\ lastAcceptedIndexVersion' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ UNCHANGED <<
        durableHistory, baseEpoch, baseIndexState,
        workspaceState, workspaceIndexState, lastAcceptedState, readState
       >>

Rollback ==
    /\ phase \in {"active", "prepared"}
    /\ phase' = "idle"
    /\ workspaceState' = canonicalState
    /\ workspaceIndexState' = canonicalState
    /\ rowOverlayEntries' = 0
    /\ indexOverlayEntries' = 0
    /\ rowOverlayVersion' = 0
    /\ indexOverlayVersion' = 0
    /\ lastAcceptedState' = canonicalState
    /\ lastAcceptedRowEntries' = 0
    /\ lastAcceptedIndexEntries' = 0
    /\ lastAcceptedRowVersion' = 0
    /\ lastAcceptedIndexVersion' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ readState' = canonicalState
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, baseEpoch, baseIndexState
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recoveredState == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonicalState' = recoveredState
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ baseEpoch' = recoveredEpoch
    /\ baseIndexState' = recoveredState
    /\ workspaceState' = recoveredState
    /\ workspaceIndexState' = recoveredState
    /\ rowOverlayEntries' = 0
    /\ indexOverlayEntries' = 0
    /\ rowOverlayVersion' = 0
    /\ indexOverlayVersion' = 0
    /\ lastAcceptedState' = recoveredState
    /\ lastAcceptedRowEntries' = 0
    /\ lastAcceptedIndexEntries' = 0
    /\ lastAcceptedRowVersion' = 0
    /\ lastAcceptedIndexVersion' = 0
    /\ statementRejected' = FALSE
    /\ workspaceRead' = FALSE
    /\ readState' = recoveredState
    /\ UNCHANGED durableHistory

Next ==
    \/ Begin
    \/ \E key \in Keys, present \in BOOLEAN,
          rowCost \in 0..MaxOverlayEntries,
          indexCost \in 0..MaxOverlayEntries:
          StageStatement(key, present, rowCost, indexCost)
    \/ \E rowCost \in 0..MaxOverlayEntries,
          indexCost \in 0..MaxOverlayEntries:
          RejectStatement(rowCost, indexCost)
    \/ WorkspaceRead
    \/ PrepareCommit
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(SUBSET Keys)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ baseEpoch \in 0..MaxEpoch
    /\ baseIndexState \subseteq Keys
    /\ workspaceState \subseteq Keys
    /\ workspaceIndexState \subseteq Keys
    /\ rowOverlayEntries \in Nat
    /\ indexOverlayEntries \in Nat
    /\ rowOverlayVersion \in Nat
    /\ indexOverlayVersion \in Nat
    /\ lastAcceptedState \subseteq Keys
    /\ lastAcceptedRowEntries \in Nat
    /\ lastAcceptedIndexEntries \in Nat
    /\ lastAcceptedRowVersion \in Nat
    /\ lastAcceptedIndexVersion \in Nat
    /\ statementRejected \in BOOLEAN
    /\ workspaceRead \in BOOLEAN
    /\ readState \subseteq Keys

VisibilityFollowsDurability ==
    /\ commitEpoch <= Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ canonicalState = HistoryState(durableHistory, commitEpoch)

PinnedBaseDoesNotDrift ==
    phase # "idle" =>
        /\ baseEpoch <= commitEpoch
        /\ baseIndexState = HistoryState(durableHistory, baseEpoch)

WorkspaceIndexMatchesRows ==
    phase \in {"active", "prepared", "durable"} =>
        workspaceIndexState = workspaceState

WorkspaceOverlayIsBounded ==
    /\ rowOverlayEntries <= MaxOverlayEntries
    /\ indexOverlayEntries <= MaxOverlayEntries
    /\ rowOverlayVersion <= MaxOverlayEntries
    /\ indexOverlayVersion <= MaxOverlayEntries

RowAndIndexOverlayVersionsMatch == rowOverlayVersion = indexOverlayVersion

RejectedStatementIsAtomic ==
    statementRejected =>
        /\ phase = "active"
        /\ workspaceState = lastAcceptedState
        /\ workspaceIndexState = lastAcceptedState
        /\ rowOverlayEntries = lastAcceptedRowEntries
        /\ indexOverlayEntries = lastAcceptedIndexEntries
        /\ rowOverlayVersion = lastAcceptedRowVersion
        /\ indexOverlayVersion = lastAcceptedIndexVersion

ReadYourOwnWritesUsesOverlay ==
    workspaceRead =>
        /\ phase = "active"
        /\ readState = workspaceState
        /\ readState = workspaceIndexState

PrefixMergeIsOrderIndependent ==
    \A base \in SUBSET Keys, current \in SUBSET Keys:
        \A order \in BaseVisitOrders(base):
            ExactTombstoneMerge(base, current, order) = current

=============================================================================
