-------------------------- MODULE HawDBRowLiveView --------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A complete recovery row view is extended by immutable live batches.     *)
(* DML/DDL stage the next view before WAL. A graph-only commit first makes  *)
(* its WAL durable, then stages the identity-only view advance. Canonical   *)
(* state and the view advance only after the commit is durable. Admission   *)
(* corruption make the acceleration view unavailable without weakening the *)
(* canonical WAL-backed state. Reader pins never drift.                     *)
(***************************************************************************)

CONSTANT MaxEpoch, MaxLiveEntries

ASSUME /\ MaxEpoch \in Nat \ {0, 1}
       /\ MaxLiveEntries \in Nat \ {0}

Keys == 1..2
BaseEpoch == 1
BaseState == {1}
Phases == {"idle", "staged", "durable"}
CommitKinds == {"none", "dml", "graph", "ddl"}

ApplyChange(state, key, present) ==
    IF present THEN state \cup {key} ELSE state \ {key}

HistoryState(history, epoch) ==
    IF epoch = BaseEpoch
    THEN BaseState
    ELSE history[epoch - BaseEpoch]

VARIABLES
    canonicalState,
    commitEpoch,
    durableHistory,
    phase,
    stagedKind,
    stagedState,
    stagedViewPossible,
    stagedViewEntries,
    viewAvailable,
    viewEpoch,
    viewState,
    liveEntries,
    liveBatches,
    lastCommitInvalidatedView,
    readerPinned,
    readerEpoch,
    readerState,
    workspaceRead,
    workspaceState,
    workspaceUsedView,
    sqlUsesView

vars == <<
    canonicalState,
    commitEpoch,
    durableHistory,
    phase,
    stagedKind,
    stagedState,
    stagedViewPossible,
    stagedViewEntries,
    viewAvailable,
    viewEpoch,
    viewState,
    liveEntries,
    liveBatches,
    lastCommitInvalidatedView,
    readerPinned,
    readerEpoch,
    readerState,
    workspaceRead,
    workspaceState,
    workspaceUsedView,
    sqlUsesView
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = BaseEpoch
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ stagedKind = "none"
    /\ stagedState = BaseState
    /\ stagedViewPossible = FALSE
    /\ stagedViewEntries = 0
    /\ viewAvailable = TRUE
    /\ viewEpoch = BaseEpoch
    /\ viewState = BaseState
    /\ liveEntries = 0
    /\ liveBatches = 0
    /\ lastCommitInvalidatedView = FALSE
    /\ readerPinned = FALSE
    /\ readerEpoch = BaseEpoch
    /\ readerState = BaseState
    /\ workspaceRead = FALSE
    /\ workspaceState = BaseState
    /\ workspaceUsedView = FALSE
    /\ sqlUsesView = FALSE

StageDml(key, present) ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ key \in Keys
    /\ present \in BOOLEAN
    /\ phase' = "staged"
    /\ stagedKind' = "dml"
    /\ stagedState' = ApplyChange(canonicalState, key, present)
    /\ stagedViewPossible' = viewAvailable /\ liveEntries < MaxLiveEntries
    /\ stagedViewEntries' = liveEntries + 1
    /\ workspaceRead' = FALSE
    /\ workspaceState' = canonicalState
    /\ workspaceUsedView' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory,
        viewAvailable, viewEpoch, viewState, liveEntries, liveBatches,
        lastCommitInvalidatedView, readerPinned, readerEpoch, readerState,
        sqlUsesView
        >>

DurableGraphCommit ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ durableHistory' = Append(durableHistory, canonicalState)
    /\ phase' = "durable"
    /\ stagedKind' = "graph"
    /\ stagedState' = canonicalState
    /\ stagedViewPossible' = viewAvailable
    /\ stagedViewEntries' = liveEntries
    /\ workspaceRead' = FALSE
    /\ workspaceState' = canonicalState
    /\ workspaceUsedView' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch,
        viewAvailable, viewEpoch, viewState, liveEntries, liveBatches,
        lastCommitInvalidatedView, readerPinned, readerEpoch, readerState,
        sqlUsesView
        >>

StageDdl ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ phase' = "staged"
    /\ stagedKind' = "ddl"
    /\ stagedState' = canonicalState
    /\ stagedViewPossible' = FALSE
    /\ stagedViewEntries' = liveEntries
    /\ workspaceRead' = FALSE
    /\ workspaceState' = canonicalState
    /\ workspaceUsedView' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory,
        viewAvailable, viewEpoch, viewState, liveEntries, liveBatches,
        lastCommitInvalidatedView, readerPinned, readerEpoch, readerState,
        sqlUsesView
        >>

WorkspaceRead ==
    /\ phase = "staged"
    /\ workspaceRead' = TRUE
    /\ workspaceState' = stagedState
    /\ workspaceUsedView' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase, stagedKind,
        stagedState, stagedViewPossible, stagedViewEntries,
        viewAvailable, viewEpoch, viewState, liveEntries, liveBatches,
        lastCommitInvalidatedView, readerPinned, readerEpoch, readerState,
        sqlUsesView
        >>

DurableCommit ==
    /\ phase = "staged"
    /\ durableHistory' = Append(durableHistory, stagedState)
    /\ phase' = "durable"
    /\ workspaceRead' = FALSE
    /\ workspaceState' = canonicalState
    /\ workspaceUsedView' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, stagedKind, stagedState,
        stagedViewPossible, stagedViewEntries,
        viewAvailable, viewEpoch, viewState, liveEntries, liveBatches,
        lastCommitInvalidatedView, readerPinned, readerEpoch, readerState,
        sqlUsesView
        >>

PublishCommit ==
    /\ phase = "durable"
    /\ commitEpoch' = commitEpoch + 1
    /\ canonicalState' = stagedState
    /\ phase' = "idle"
    /\ stagedKind' = "none"
    /\ stagedState' = stagedState
    /\ stagedViewPossible' = FALSE
    /\ stagedViewEntries' = 0
    /\ viewAvailable' = stagedViewPossible
    /\ viewEpoch' = IF stagedViewPossible THEN commitEpoch + 1 ELSE viewEpoch
    /\ viewState' = IF stagedViewPossible THEN stagedState ELSE viewState
    /\ liveEntries' = IF stagedViewPossible THEN stagedViewEntries ELSE liveEntries
    /\ liveBatches' =
        IF stagedViewPossible /\ stagedKind = "dml"
        THEN liveBatches + 1
        ELSE liveBatches
    /\ lastCommitInvalidatedView' = ~stagedViewPossible
    /\ workspaceRead' = FALSE
    /\ workspaceState' = stagedState
    /\ workspaceUsedView' = FALSE
    /\ UNCHANGED <<
        durableHistory, readerPinned, readerEpoch, readerState, sqlUsesView
        >>

PinReader ==
    /\ phase = "idle"
    /\ viewAvailable
    /\ ~readerPinned
    /\ readerPinned' = TRUE
    /\ readerEpoch' = viewEpoch
    /\ readerState' = viewState
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase, stagedKind,
        stagedState, stagedViewPossible, stagedViewEntries,
        viewAvailable, viewEpoch, viewState, liveEntries, liveBatches,
        lastCommitInvalidatedView, workspaceRead, workspaceState,
        workspaceUsedView, sqlUsesView
        >>

PoisonCurrentView ==
    /\ phase = "idle"
    /\ viewAvailable
    /\ viewAvailable' = FALSE
    /\ lastCommitInvalidatedView' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, durableHistory, phase, stagedKind,
        stagedState, stagedViewPossible, stagedViewEntries,
        viewEpoch, viewState, liveEntries, liveBatches,
        readerPinned, readerEpoch, readerState, workspaceRead,
        workspaceState, workspaceUsedView, sqlUsesView
        >>

CrashRecover ==
    /\ phase \in Phases
    /\ LET recoveredEpoch == BaseEpoch + Len(durableHistory) IN
       LET recoveredState == HistoryState(durableHistory, recoveredEpoch) IN
        /\ canonicalState' = recoveredState
        /\ commitEpoch' = recoveredEpoch
        /\ phase' = "idle"
        /\ stagedKind' = "none"
        /\ stagedState' = recoveredState
        /\ stagedViewPossible' = FALSE
        /\ stagedViewEntries' = 0
        /\ viewAvailable' = TRUE
        /\ viewEpoch' = recoveredEpoch
        /\ viewState' = recoveredState
        /\ liveEntries' = 0
        /\ liveBatches' = 0
        /\ lastCommitInvalidatedView' = FALSE
        /\ readerPinned' = FALSE
        /\ readerEpoch' = recoveredEpoch
        /\ readerState' = recoveredState
        /\ workspaceRead' = FALSE
        /\ workspaceState' = recoveredState
        /\ workspaceUsedView' = FALSE
        /\ sqlUsesView' = FALSE
        /\ UNCHANGED durableHistory

Next ==
    \/ \E key \in Keys, present \in BOOLEAN: StageDml(key, present)
    \/ DurableGraphCommit
    \/ StageDdl
    \/ WorkspaceRead
    \/ DurableCommit
    \/ PublishCommit
    \/ PinReader
    \/ PoisonCurrentView
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in BaseEpoch..MaxEpoch
    /\ durableHistory \in Seq(SUBSET Keys)
    /\ Len(durableHistory) <= MaxEpoch - BaseEpoch
    /\ phase \in Phases
    /\ stagedKind \in CommitKinds
    /\ stagedState \subseteq Keys
    /\ stagedViewPossible \in BOOLEAN
    /\ stagedViewEntries \in Nat
    /\ viewAvailable \in BOOLEAN
    /\ viewEpoch \in BaseEpoch..MaxEpoch
    /\ viewState \subseteq Keys
    /\ liveEntries \in Nat
    /\ liveBatches \in Nat
    /\ lastCommitInvalidatedView \in BOOLEAN
    /\ readerPinned \in BOOLEAN
    /\ readerEpoch \in BaseEpoch..MaxEpoch
    /\ readerState \subseteq Keys
    /\ workspaceRead \in BOOLEAN
    /\ workspaceState \subseteq Keys
    /\ workspaceUsedView \in BOOLEAN
    /\ sqlUsesView \in BOOLEAN

VisibilityFollowsDurability ==
    /\ commitEpoch <= BaseEpoch + Len(durableHistory)
    /\ phase # "durable" => commitEpoch = BaseEpoch + Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = BaseEpoch + Len(durableHistory)
    /\ canonicalState = HistoryState(durableHistory, commitEpoch)

CurrentViewIsExact ==
    viewAvailable =>
        /\ viewEpoch = commitEpoch
        /\ viewState = canonicalState

LiveOverlayIsBounded == liveEntries <= MaxLiveEntries

InvalidatedPublicationFailsClosed ==
    lastCommitInvalidatedView => ~viewAvailable

PinnedReaderDoesNotDrift ==
    readerPinned => readerState = HistoryState(durableHistory, readerEpoch)

ReadYourOwnWritesUsesWorkspace ==
    workspaceRead =>
        /\ phase = "staged"
        /\ workspaceState = stagedState
        /\ ~workspaceUsedView

ProductionSqlRemainsCanonical == ~sqlUsesView

=============================================================================
