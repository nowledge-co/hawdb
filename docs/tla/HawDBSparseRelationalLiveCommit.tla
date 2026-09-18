------------------- MODULE HawDBSparseRelationalLiveCommit -------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* One metadata-only relational commit hydrates a bounded exact workspace, *)
(* stages canonical state and both live views without publishing them, then *)
(* appends WAL before making the three visible identities advance together. *)
(* Failure before WAL preserves the old database. A crash after WAL may     *)
(* discard unpublished candidates, but replay publishes the same epoch.     *)
(***************************************************************************)

Keys == {"k1", "k2", "k3"}
MaxWorkspaceEntries == 2

VARIABLES
    phase,
    requiredAccess,
    hydratedAccess,
    workspaceClosed,
    stateStaged,
    rowViewStaged,
    indexViewStaged,
    durableWalEpoch,
    visibleStateEpoch,
    visibleRowEpoch,
    visibleIndexEpoch

vars == <<
    phase,
    requiredAccess,
    hydratedAccess,
    workspaceClosed,
    stateStaged,
    rowViewStaged,
    indexViewStaged,
    durableWalEpoch,
    visibleStateEpoch,
    visibleRowEpoch,
    visibleIndexEpoch
>>

Init ==
    /\ phase = "idle"
    /\ requiredAccess = {}
    /\ hydratedAccess = {}
    /\ workspaceClosed = FALSE
    /\ stateStaged = FALSE
    /\ rowViewStaged = FALSE
    /\ indexViewStaged = FALSE
    /\ durableWalEpoch = 0
    /\ visibleStateEpoch = 0
    /\ visibleRowEpoch = 0
    /\ visibleIndexEpoch = 0

Begin(required) ==
    /\ phase = "idle"
    /\ required \subseteq Keys
    /\ required # {}
    /\ phase' = "hydrating"
    /\ requiredAccess' = required
    /\ hydratedAccess' = {}
    /\ workspaceClosed' = FALSE
    /\ UNCHANGED <<
        stateStaged, rowViewStaged, indexViewStaged, durableWalEpoch,
        visibleStateEpoch, visibleRowEpoch, visibleIndexEpoch
        >>

Hydrate(key) ==
    /\ phase = "hydrating"
    /\ Cardinality(requiredAccess) <= MaxWorkspaceEntries
    /\ key \in requiredAccess \ hydratedAccess
    /\ hydratedAccess' = hydratedAccess \cup {key}
    /\ UNCHANGED <<
        phase, requiredAccess, workspaceClosed, stateStaged, rowViewStaged,
        indexViewStaged, durableWalEpoch, visibleStateEpoch, visibleRowEpoch,
        visibleIndexEpoch
        >>

CloseWorkspace ==
    /\ phase = "hydrating"
    /\ hydratedAccess = requiredAccess
    /\ Cardinality(hydratedAccess) <= MaxWorkspaceEntries
    /\ phase' = "closed"
    /\ workspaceClosed' = TRUE
    /\ UNCHANGED <<
        requiredAccess, hydratedAccess, stateStaged, rowViewStaged,
        indexViewStaged, durableWalEpoch, visibleStateEpoch, visibleRowEpoch,
        visibleIndexEpoch
        >>

RejectOversized ==
    /\ phase = "hydrating"
    /\ Cardinality(requiredAccess) > MaxWorkspaceEntries
    /\ phase' = "rejected"
    /\ requiredAccess' = {}
    /\ hydratedAccess' = {}
    /\ workspaceClosed' = FALSE
    /\ UNCHANGED <<
        stateStaged, rowViewStaged, indexViewStaged, durableWalEpoch,
        visibleStateEpoch, visibleRowEpoch, visibleIndexEpoch
        >>

StageState ==
    /\ phase = "closed"
    /\ workspaceClosed
    /\ phase' = "state_staged"
    /\ stateStaged' = TRUE
    /\ UNCHANGED <<
        requiredAccess, hydratedAccess, workspaceClosed, rowViewStaged,
        indexViewStaged, durableWalEpoch, visibleStateEpoch, visibleRowEpoch,
        visibleIndexEpoch
        >>

StageViews ==
    /\ phase = "state_staged"
    /\ stateStaged
    /\ phase' = "views_staged"
    /\ rowViewStaged' = TRUE
    /\ indexViewStaged' = TRUE
    /\ UNCHANGED <<
        requiredAccess, hydratedAccess, workspaceClosed, stateStaged,
        durableWalEpoch, visibleStateEpoch, visibleRowEpoch, visibleIndexEpoch
        >>

FailBeforeWal ==
    /\ phase \in {"hydrating", "closed", "state_staged", "views_staged"}
    /\ phase' = "rejected"
    /\ requiredAccess' = {}
    /\ hydratedAccess' = {}
    /\ workspaceClosed' = FALSE
    /\ stateStaged' = FALSE
    /\ rowViewStaged' = FALSE
    /\ indexViewStaged' = FALSE
    /\ UNCHANGED <<
        durableWalEpoch, visibleStateEpoch, visibleRowEpoch, visibleIndexEpoch
        >>

AppendWal ==
    /\ phase = "views_staged"
    /\ workspaceClosed
    /\ stateStaged
    /\ rowViewStaged
    /\ indexViewStaged
    /\ phase' = "wal_durable"
    /\ durableWalEpoch' = 1
    /\ UNCHANGED <<
        requiredAccess, hydratedAccess, workspaceClosed, stateStaged,
        rowViewStaged, indexViewStaged, visibleStateEpoch, visibleRowEpoch,
        visibleIndexEpoch
        >>

Publish ==
    /\ phase = "wal_durable"
    /\ durableWalEpoch = 1
    /\ phase' = "committed"
    /\ requiredAccess' = {}
    /\ hydratedAccess' = {}
    /\ stateStaged' = FALSE
    /\ rowViewStaged' = FALSE
    /\ indexViewStaged' = FALSE
    /\ visibleStateEpoch' = 1
    /\ visibleRowEpoch' = 1
    /\ visibleIndexEpoch' = 1
    /\ UNCHANGED <<workspaceClosed, durableWalEpoch>>

CrashAfterWal ==
    /\ phase = "wal_durable"
    /\ durableWalEpoch = 1
    /\ phase' = "recovering"
    /\ requiredAccess' = {}
    /\ hydratedAccess' = {}
    /\ stateStaged' = FALSE
    /\ rowViewStaged' = FALSE
    /\ indexViewStaged' = FALSE
    /\ UNCHANGED <<
        workspaceClosed, durableWalEpoch, visibleStateEpoch, visibleRowEpoch,
        visibleIndexEpoch
        >>

Recover ==
    /\ phase = "recovering"
    /\ durableWalEpoch = 1
    /\ workspaceClosed
    /\ phase' = "committed"
    /\ visibleStateEpoch' = 1
    /\ visibleRowEpoch' = 1
    /\ visibleIndexEpoch' = 1
    /\ UNCHANGED <<
        requiredAccess, hydratedAccess, workspaceClosed, stateStaged,
        rowViewStaged, indexViewStaged, durableWalEpoch
        >>

Next ==
    \/ \E required \in SUBSET Keys: Begin(required)
    \/ \E key \in Keys: Hydrate(key)
    \/ CloseWorkspace
    \/ RejectOversized
    \/ StageState
    \/ StageViews
    \/ FailBeforeWal
    \/ AppendWal
    \/ Publish
    \/ CrashAfterWal
    \/ Recover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {
        "idle", "hydrating", "closed", "state_staged", "views_staged",
        "wal_durable", "recovering", "committed", "rejected"
        }
    /\ requiredAccess \subseteq Keys
    /\ hydratedAccess \subseteq Keys
    /\ workspaceClosed \in BOOLEAN
    /\ stateStaged \in BOOLEAN
    /\ rowViewStaged \in BOOLEAN
    /\ indexViewStaged \in BOOLEAN
    /\ durableWalEpoch \in 0..1
    /\ visibleStateEpoch \in 0..1
    /\ visibleRowEpoch \in 0..1
    /\ visibleIndexEpoch \in 0..1

VisiblePublicationIsAtomic ==
    /\ visibleStateEpoch = visibleRowEpoch
    /\ visibleStateEpoch = visibleIndexEpoch

PublicationRequiresDurableWal ==
    visibleStateEpoch = 1 => durableWalEpoch = 1

DurableWalRequiresClosedWorkspace ==
    durableWalEpoch = 1 => workspaceClosed

RejectedCommitPreservesOldDatabase ==
    phase = "rejected" =>
        /\ durableWalEpoch = 0
        /\ visibleStateEpoch = 0
        /\ visibleRowEpoch = 0
        /\ visibleIndexEpoch = 0

CommittedWorkspaceIsDiscarded ==
    phase = "committed" =>
        /\ requiredAccess = {}
        /\ hydratedAccess = {}
        /\ ~stateStaged
        /\ ~rowViewStaged
        /\ ~indexViewStaged

CommittedEpochIsVisible ==
    phase = "committed" => visibleStateEpoch = 1

=============================================================================
