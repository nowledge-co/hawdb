------------------- MODULE HawDBAppendMixedTransaction -------------------
EXTENDS Naturals, Sequences

(***************************************************************************)
(* One mixed transaction stages graph, mutable row-page, and strict append  *)
(* mutations in a private workspace. The complete workspace receives one   *)
(* durability decision and one visible commit epoch. A crash recovers the   *)
(* complete durable state, including a durable but unacknowledged commit.   *)
(* A pinned reader never observes later publication.                        *)
(***************************************************************************)

CONSTANTS MaxEpoch, MutatePartialPublish

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ MutatePartialPublish \in BOOLEAN

Epochs == 0..MaxEpoch
Phases == {"idle", "staged", "durable"}

InitialState == [graph |-> 0, row |-> 0, append |-> <<>>]

StateType(state) ==
    /\ state.graph \in Epochs
    /\ state.row \in Epochs
    /\ Len(state.append) <= MaxEpoch
    /\ \A index \in 1..Len(state.append): state.append[index] \in Epochs

HistoryState(history, count) ==
    IF count = 0 THEN InitialState ELSE history[count]

VARIABLES
    canonicalState,
    commitEpoch,
    durableHistory,
    visibleCommitCount,
    phase,
    workspaceState,
    graphStaged,
    rowStaged,
    appendStaged,
    readerPinned,
    readerEpoch,
    readerState

vars == <<
    canonicalState,
    commitEpoch,
    durableHistory,
    visibleCommitCount,
    phase,
    workspaceState,
    graphStaged,
    rowStaged,
    appendStaged,
    readerPinned,
    readerEpoch,
    readerState
>>

Init ==
    /\ canonicalState = InitialState
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ visibleCommitCount = 0
    /\ phase = "idle"
    /\ workspaceState = InitialState
    /\ graphStaged = FALSE
    /\ rowStaged = FALSE
    /\ appendStaged = FALSE
    /\ readerPinned = FALSE
    /\ readerEpoch = 0
    /\ readerState = InitialState

BeginTransaction ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ phase' = "staged"
    /\ workspaceState' = canonicalState
    /\ graphStaged' = FALSE
    /\ rowStaged' = FALSE
    /\ appendStaged' = FALSE
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        durableHistory,
        visibleCommitCount,
        readerPinned,
        readerEpoch,
        readerState
        >>

StageGraph ==
    /\ phase = "staged"
    /\ ~graphStaged
    /\ workspaceState' =
        [workspaceState EXCEPT !.graph = commitEpoch + 1]
    /\ graphStaged' = TRUE
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        durableHistory,
        visibleCommitCount,
        phase,
        rowStaged,
        appendStaged,
        readerPinned,
        readerEpoch,
        readerState
        >>

StageRow ==
    /\ phase = "staged"
    /\ ~rowStaged
    /\ workspaceState' =
        [workspaceState EXCEPT !.row = commitEpoch + 1]
    /\ rowStaged' = TRUE
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        durableHistory,
        visibleCommitCount,
        phase,
        graphStaged,
        appendStaged,
        readerPinned,
        readerEpoch,
        readerState
        >>

StageAppend ==
    /\ phase = "staged"
    /\ ~appendStaged
    /\ workspaceState' =
        [workspaceState EXCEPT !.append = Append(@, commitEpoch + 1)]
    /\ appendStaged' = TRUE
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        durableHistory,
        visibleCommitCount,
        phase,
        graphStaged,
        rowStaged,
        readerPinned,
        readerEpoch,
        readerState
        >>

DurableCommit ==
    /\ phase = "staged"
    /\ graphStaged
    /\ rowStaged
    /\ appendStaged
    /\ durableHistory' = Append(durableHistory, workspaceState)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        visibleCommitCount,
        workspaceState,
        graphStaged,
        rowStaged,
        appendStaged,
        readerPinned,
        readerEpoch,
        readerState
        >>

PublishCompleteCommit ==
    /\ phase = "durable"
    /\ canonicalState' = workspaceState
    /\ commitEpoch' = commitEpoch + 1
    /\ visibleCommitCount' = Len(durableHistory)
    /\ phase' = "idle"
    /\ graphStaged' = FALSE
    /\ rowStaged' = FALSE
    /\ appendStaged' = FALSE
    /\ UNCHANGED <<
        durableHistory,
        workspaceState,
        readerPinned,
        readerEpoch,
        readerState
        >>

PublishPartialCommit ==
    /\ MutatePartialPublish
    /\ phase = "durable"
    /\ canonicalState' =
        [canonicalState EXCEPT !.graph = workspaceState.graph]
    /\ commitEpoch' = commitEpoch + 1
    /\ visibleCommitCount' = Len(durableHistory)
    /\ phase' = "idle"
    /\ graphStaged' = FALSE
    /\ rowStaged' = FALSE
    /\ appendStaged' = FALSE
    /\ UNCHANGED <<
        durableHistory,
        workspaceState,
        readerPinned,
        readerEpoch,
        readerState
        >>

Rollback ==
    /\ phase = "staged"
    /\ phase' = "idle"
    /\ workspaceState' = canonicalState
    /\ graphStaged' = FALSE
    /\ rowStaged' = FALSE
    /\ appendStaged' = FALSE
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        durableHistory,
        visibleCommitCount,
        readerPinned,
        readerEpoch,
        readerState
        >>

PinReader ==
    /\ phase = "idle"
    /\ ~readerPinned
    /\ readerPinned' = TRUE
    /\ readerEpoch' = commitEpoch
    /\ readerState' = canonicalState
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        durableHistory,
        visibleCommitCount,
        phase,
        workspaceState,
        graphStaged,
        rowStaged,
        appendStaged
        >>

ReleaseReader ==
    /\ readerPinned
    /\ readerPinned' = FALSE
    /\ UNCHANGED <<
        canonicalState,
        commitEpoch,
        durableHistory,
        visibleCommitCount,
        phase,
        workspaceState,
        graphStaged,
        rowStaged,
        appendStaged,
        readerEpoch,
        readerState
        >>

CrashRecover ==
    /\ canonicalState' = HistoryState(durableHistory, Len(durableHistory))
    /\ commitEpoch' = Len(durableHistory)
    /\ visibleCommitCount' = Len(durableHistory)
    /\ phase' = "idle"
    /\ workspaceState' = HistoryState(durableHistory, Len(durableHistory))
    /\ graphStaged' = FALSE
    /\ rowStaged' = FALSE
    /\ appendStaged' = FALSE
    /\ readerPinned' = FALSE
    /\ readerEpoch' = Len(durableHistory)
    /\ readerState' = HistoryState(durableHistory, Len(durableHistory))
    /\ UNCHANGED durableHistory

Next ==
    \/ BeginTransaction
    \/ StageGraph
    \/ StageRow
    \/ StageAppend
    \/ DurableCommit
    \/ PublishCompleteCommit
    \/ PublishPartialCommit
    \/ Rollback
    \/ PinReader
    \/ ReleaseReader
    \/ CrashRecover

TypeOK ==
    /\ StateType(canonicalState)
    /\ commitEpoch \in Epochs
    /\ durableHistory \in Seq([graph : Epochs, row : Epochs, append : Seq(Epochs)])
    /\ Len(durableHistory) <= MaxEpoch
    /\ visibleCommitCount \in 0..Len(durableHistory)
    /\ phase \in Phases
    /\ StateType(workspaceState)
    /\ graphStaged \in BOOLEAN
    /\ rowStaged \in BOOLEAN
    /\ appendStaged \in BOOLEAN
    /\ readerPinned \in BOOLEAN
    /\ readerEpoch \in Epochs
    /\ StateType(readerState)

VisibleStateIsDurablePrefix ==
    canonicalState = HistoryState(durableHistory, visibleCommitCount)

VisibleCountMatchesCommitEpoch == visibleCommitCount = commitEpoch

MixedComponentsShareEpoch ==
    /\ canonicalState.graph = commitEpoch
    /\ canonicalState.row = commitEpoch
    /\ Len(canonicalState.append) = commitEpoch
    /\ \A index \in 1..Len(canonicalState.append):
        canonicalState.append[index] = index

ReaderDoesNotDrift ==
    ~readerPinned
    \/ readerState = HistoryState(durableHistory, readerEpoch)

Spec == Init /\ [][Next]_vars

=============================================================================
