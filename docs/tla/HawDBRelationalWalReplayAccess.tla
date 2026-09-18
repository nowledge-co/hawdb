----------------- MODULE HawDBRelationalWalReplayAccess -----------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* A relational DML commit captures its complete primary-key working set    *)
(* before WAL durability. Predicate reads, including non-matches, are part   *)
(* of that set rather than only the keys with a final row change. Recovery   *)
(* may hydrate only those keys, but it must reject an unauthenticated or      *)
(* semantically drifted set before applying the transaction.                 *)
(***************************************************************************)

Keys == {1, 2, 3}
AccessSets == SUBSET Keys
MaxAccessEntries == 2

VARIABLES
    phase,
    predicateReadAccess,
    changedAccess,
    transactionAccess,
    walAccess,
    walAuthenticated,
    hydratedAccess,
    transactionApplied,
    fullRowsMaterialized

vars == <<
    phase,
    predicateReadAccess,
    changedAccess,
    transactionAccess,
    walAccess,
    walAuthenticated,
    hydratedAccess,
    transactionApplied,
    fullRowsMaterialized
>>

Init ==
    /\ phase = "idle"
    /\ predicateReadAccess = {}
    /\ changedAccess = {}
    /\ transactionAccess = {}
    /\ walAccess = {}
    /\ walAuthenticated = FALSE
    /\ hydratedAccess = {}
    /\ transactionApplied = FALSE
    /\ fullRowsMaterialized = FALSE

CommitExact(predicateReads, changes) ==
    /\ phase = "idle"
    /\ predicateReads \in AccessSets
    /\ changes \in AccessSets
    /\ Cardinality(predicateReads \union changes) <= MaxAccessEntries
    /\ phase' = "durable"
    /\ predicateReadAccess' = predicateReads
    /\ changedAccess' = changes
    /\ transactionAccess' = predicateReads \union changes
    /\ walAccess' = predicateReads \union changes
    /\ walAuthenticated' = TRUE
    /\ UNCHANGED <<hydratedAccess, transactionApplied, fullRowsMaterialized>>

RejectOversized(predicateReads, changes) ==
    /\ phase = "idle"
    /\ predicateReads \in AccessSets
    /\ changes \in AccessSets
    /\ Cardinality(predicateReads \union changes) > MaxAccessEntries
    /\ phase' = "admissionRejected"
    /\ predicateReadAccess' = predicateReads
    /\ changedAccess' = changes
    /\ transactionAccess' = predicateReads \union changes
    /\ UNCHANGED <<
        walAccess, walAuthenticated, hydratedAccess,
        transactionApplied, fullRowsMaterialized
       >>

TamperWal(replacement) ==
    /\ phase = "durable"
    /\ replacement \in AccessSets
    /\ phase' = "recovering"
    /\ walAccess' = replacement
    /\ walAuthenticated' = FALSE
    /\ UNCHANGED <<
        predicateReadAccess, changedAccess, transactionAccess, hydratedAccess,
        transactionApplied, fullRowsMaterialized
       >>

BeginRecovery ==
    /\ phase = "durable"
    /\ phase' = "recovering"
    /\ UNCHANGED <<
        predicateReadAccess, changedAccess, transactionAccess,
        walAccess, walAuthenticated, hydratedAccess,
        transactionApplied, fullRowsMaterialized
       >>

HydrateExactAccess ==
    /\ phase = "recovering"
    /\ walAuthenticated
    /\ walAccess = transactionAccess
    /\ Cardinality(walAccess) <= MaxAccessEntries
    /\ phase' = "hydrated"
    /\ hydratedAccess' = walAccess
    /\ UNCHANGED <<
        predicateReadAccess, changedAccess, transactionAccess,
        walAccess, walAuthenticated,
        transactionApplied, fullRowsMaterialized
       >>

RejectInvalidAccess ==
    /\ phase = "recovering"
    /\ (~walAuthenticated
        \/ walAccess # transactionAccess
        \/ Cardinality(walAccess) > MaxAccessEntries)
    /\ phase' = "rejected"
    /\ UNCHANGED <<
        predicateReadAccess, changedAccess, transactionAccess,
        walAccess, walAuthenticated, hydratedAccess,
        transactionApplied, fullRowsMaterialized
       >>

ApplyRecoveredTransaction ==
    /\ phase = "hydrated"
    /\ hydratedAccess = transactionAccess
    /\ phase' = "serving"
    /\ transactionApplied' = TRUE
    /\ UNCHANGED <<
        predicateReadAccess, changedAccess, transactionAccess,
        walAccess, walAuthenticated, hydratedAccess,
        fullRowsMaterialized
       >>

Next ==
    \/ \E predicateReads \in AccessSets, changes \in AccessSets:
        CommitExact(predicateReads, changes)
    \/ \E predicateReads \in AccessSets, changes \in AccessSets:
        RejectOversized(predicateReads, changes)
    \/ \E replacement \in AccessSets: TamperWal(replacement)
    \/ BeginRecovery
    \/ HydrateExactAccess
    \/ RejectInvalidAccess
    \/ ApplyRecoveredTransaction

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {
        "idle", "durable", "admissionRejected", "recovering",
        "hydrated", "rejected", "serving"
       }
    /\ predicateReadAccess \in AccessSets
    /\ changedAccess \in AccessSets
    /\ transactionAccess \in AccessSets
    /\ walAccess \in AccessSets
    /\ walAuthenticated \in BOOLEAN
    /\ hydratedAccess \in AccessSets
    /\ transactionApplied \in BOOLEAN
    /\ fullRowsMaterialized \in BOOLEAN

AuthenticatedWalIsExact ==
    walAuthenticated => walAccess = transactionAccess

CommittedAccessIsComplete ==
    phase \in {"durable", "recovering", "hydrated", "serving"} =>
        transactionAccess = predicateReadAccess \union changedAccess

WalCoversPredicateReads ==
    walAuthenticated => predicateReadAccess \subseteq walAccess

ServingRequiresExactBoundedAccess ==
    phase = "serving" =>
        /\ walAuthenticated
        /\ walAccess = transactionAccess
        /\ hydratedAccess = transactionAccess
        /\ predicateReadAccess \subseteq hydratedAccess
        /\ Cardinality(hydratedAccess) <= MaxAccessEntries
        /\ transactionApplied

RejectedRecoveryNeverApplies ==
    phase \in {"admissionRejected", "rejected"} => ~transactionApplied

RecoveryHydrationIsBounded ==
    phase \in {"hydrated", "serving"} =>
        /\ hydratedAccess = walAccess
        /\ Cardinality(hydratedAccess) <= MaxAccessEntries

SparseRecoveryNeverMaterializesAllRows ==
    fullRowsMaterialized = FALSE

=============================================================================
