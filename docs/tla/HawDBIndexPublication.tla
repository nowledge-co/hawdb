-------------------- MODULE HawDBIndexPublication --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Immutable row and index roots publish through one generation-fenced     *)
(* manifest. Open validates only that compact root identity and leaves leaf *)
(* pages cold. A query loads its required page on demand; corruption fails  *)
(* the query and poisons the open handle.                                   *)
(***************************************************************************)

CONSTANT MaxEpoch, MaxPage

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ MaxPage \in Nat \ {0}

Epochs == 0..MaxEpoch
Pages == 1..MaxPage
BuildPhases == {"idle", "building", "durable"}
QueryStates == {"idle", "reading", "succeeded", "failed"}
ConstraintStates == {"idle", "reading", "accepted", "durable", "failed"}

VARIABLES
    canonicalEpoch,
    rowRootEpoch,
    indexRootEpoch,
    rootGeneration,
    buildPhase,
    buildTarget,
    buildBaseGeneration,
    durableIndexEpochs,
    handleOpen,
    queriesStarted,
    loadedPages,
    corruptPages,
    queryState,
    requiredPage,
    poisoned,
    stalePublishRejected,
    visibleRowEpoch,
    visibleIndexEpoch,
    authoritativeHandle,
    constraintState,
    constraintTarget,
    constraintPage,
    durableWalEpochs,
    constraintRejected,
    materializedPostingsResident

vars == <<
    canonicalEpoch,
    rowRootEpoch,
    indexRootEpoch,
    rootGeneration,
    buildPhase,
    buildTarget,
    buildBaseGeneration,
    durableIndexEpochs,
    handleOpen,
    queriesStarted,
    loadedPages,
    corruptPages,
    queryState,
    requiredPage,
    poisoned,
    stalePublishRejected,
    visibleRowEpoch,
    visibleIndexEpoch,
    authoritativeHandle,
    constraintState,
    constraintTarget,
    constraintPage,
    durableWalEpochs,
    constraintRejected,
    materializedPostingsResident
>>

authorityMutationVars == <<
    authoritativeHandle,
    constraintState,
    constraintTarget,
    constraintPage,
    durableWalEpochs,
    constraintRejected,
    materializedPostingsResident
>>

Init ==
    /\ canonicalEpoch = 0
    /\ rowRootEpoch = 0
    /\ indexRootEpoch = 0
    /\ rootGeneration = 0
    /\ buildPhase = "idle"
    /\ buildTarget = 0
    /\ buildBaseGeneration = 0
    /\ durableIndexEpochs = {0}
    /\ handleOpen = FALSE
    /\ queriesStarted = FALSE
    /\ loadedPages = {}
    /\ corruptPages = {}
    /\ queryState = "idle"
    /\ requiredPage = 0
    /\ poisoned = FALSE
    /\ stalePublishRejected = FALSE
    /\ visibleRowEpoch = 0
    /\ visibleIndexEpoch = 0
    /\ authoritativeHandle = FALSE
    /\ constraintState = "idle"
    /\ constraintTarget = 0
    /\ constraintPage = 0
    /\ durableWalEpochs = {}
    /\ constraintRejected = FALSE
    /\ materializedPostingsResident = TRUE

Commit ==
    /\ canonicalEpoch < MaxEpoch
    /\ ~authoritativeHandle
    /\ constraintState = "idle"
    /\ canonicalEpoch' = canonicalEpoch + 1
    /\ visibleRowEpoch' = canonicalEpoch + 1
    /\ UNCHANGED <<
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        visibleIndexEpoch
        >>
    /\ UNCHANGED authorityMutationVars

BeginBuild ==
    /\ buildPhase = "idle"
    /\ rowRootEpoch < canonicalEpoch
    /\ buildPhase' = "building"
    /\ buildTarget' = canonicalEpoch
    /\ buildBaseGeneration' = rootGeneration
    /\ stalePublishRejected' = FALSE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        visibleRowEpoch,
        visibleIndexEpoch
        >>
    /\ UNCHANGED authorityMutationVars

PersistIndexPages ==
    /\ buildPhase = "building"
    /\ buildPhase' = "durable"
    /\ durableIndexEpochs' = durableIndexEpochs \cup {buildTarget}
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildTarget,
        buildBaseGeneration,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch
        >>
    /\ UNCHANGED authorityMutationVars

(***************************************************************************)
(* A competing complete checkpoint may win while this builder is active.  *)
(* This represents the generation CAS race without modeling two builders.  *)
(***************************************************************************)
PublishCompetingRoot ==
    /\ buildPhase \in {"building", "durable"}
    /\ canonicalEpoch > rowRootEpoch
    /\ constraintState = "idle"
    /\ rowRootEpoch' = canonicalEpoch
    /\ indexRootEpoch' = canonicalEpoch
    /\ visibleRowEpoch' = canonicalEpoch
    /\ visibleIndexEpoch' = canonicalEpoch
    /\ rootGeneration' = rootGeneration + 1
    /\ durableIndexEpochs' = durableIndexEpochs \cup {canonicalEpoch}
    /\ UNCHANGED <<
        canonicalEpoch,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>
    /\ UNCHANGED authorityMutationVars

PublishBuiltRoot ==
    /\ buildPhase = "durable"
    /\ buildTarget = canonicalEpoch
    /\ buildTarget \in durableIndexEpochs
    /\ buildBaseGeneration = rootGeneration
    /\ constraintState = "idle"
    /\ rowRootEpoch' = buildTarget
    /\ indexRootEpoch' = buildTarget
    /\ visibleRowEpoch' = buildTarget
    /\ visibleIndexEpoch' = buildTarget
    /\ rootGeneration' = rootGeneration + 1
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ buildBaseGeneration' = rootGeneration + 1
    /\ UNCHANGED <<
        canonicalEpoch,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>
    /\ UNCHANGED authorityMutationVars

RejectStalePublish ==
    /\ buildPhase = "durable"
    /\ \/ buildTarget # canonicalEpoch
       \/ buildBaseGeneration # rootGeneration
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ buildBaseGeneration' = rootGeneration
    /\ stalePublishRejected' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        visibleRowEpoch,
        visibleIndexEpoch
        >>
    /\ UNCHANGED authorityMutationVars

OpenHandle ==
    /\ ~handleOpen
    /\ rowRootEpoch = indexRootEpoch
    /\ indexRootEpoch \in durableIndexEpochs
    /\ handleOpen' = TRUE
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ queryState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ authoritativeHandle' = FALSE
    /\ materializedPostingsResident' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        corruptPages,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        constraintState,
        constraintTarget,
        constraintPage,
        durableWalEpochs,
        constraintRejected
        >>

OpenAuthoritativeHandle ==
    /\ ~handleOpen
    /\ rowRootEpoch = indexRootEpoch
    /\ indexRootEpoch \in durableIndexEpochs
    /\ visibleRowEpoch = canonicalEpoch
    /\ visibleIndexEpoch = canonicalEpoch
    /\ constraintState = "idle"
    /\ handleOpen' = TRUE
    /\ authoritativeHandle' = TRUE
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ queryState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ materializedPostingsResident' = FALSE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        corruptPages,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        constraintState,
        constraintTarget,
        constraintPage,
        durableWalEpochs,
        constraintRejected
        >>

BeginLookup ==
    /\ handleOpen
    /\ ~poisoned
    /\ queryState = "idle"
    /\ constraintState = "idle"
    /\ \E page \in Pages:
        /\ requiredPage' = page
        /\ queryState' = "reading"
    /\ queriesStarted' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        loadedPages,
        corruptPages,
        poisoned,
        stalePublishRejected
        >>
    /\ UNCHANGED <<visibleRowEpoch, visibleIndexEpoch>>
    /\ UNCHANGED authorityMutationVars

ReadHealthyPage ==
    /\ queryState = "reading"
    /\ requiredPage \notin corruptPages
    /\ loadedPages' = loadedPages \cup {requiredPage}
    /\ queryState' = "succeeded"
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        corruptPages,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>
    /\ UNCHANGED <<visibleRowEpoch, visibleIndexEpoch>>
    /\ UNCHANGED authorityMutationVars

ReadCorruptPage ==
    /\ queryState = "reading"
    /\ requiredPage \in corruptPages
    /\ queryState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        requiredPage,
        stalePublishRejected
        >>
    /\ UNCHANGED <<visibleRowEpoch, visibleIndexEpoch>>
    /\ UNCHANGED authorityMutationVars

FinishLookup ==
    /\ queryState = "succeeded"
    /\ queryState' = "idle"
    /\ requiredPage' = 0
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        poisoned,
        stalePublishRejected
        >>
    /\ UNCHANGED <<visibleRowEpoch, visibleIndexEpoch>>
    /\ UNCHANGED authorityMutationVars

CorruptColdPage ==
    /\ \E page \in Pages \ loadedPages:
        corruptPages' = corruptPages \cup {page}
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected
        >>
    /\ UNCHANGED <<visibleRowEpoch, visibleIndexEpoch>>
    /\ UNCHANGED authorityMutationVars

(***************************************************************************)
(* Authoritative mutations read one current index view before the WAL. A   *)
(* semantic rejection or corrupt page cannot append WAL or advance either  *)
(* visible epoch. Once the WAL is durable, normal publication or recovery  *)
(* advances row and index visibility together.                             *)
(***************************************************************************)
BeginAuthoritativeConstraint ==
    /\ handleOpen
    /\ authoritativeHandle
    /\ ~poisoned
    /\ queryState = "idle"
    /\ constraintState = "idle"
    /\ visibleRowEpoch = canonicalEpoch
    /\ visibleIndexEpoch = canonicalEpoch
    /\ canonicalEpoch < MaxEpoch
    /\ \E page \in Pages:
        /\ constraintPage' = page
        /\ constraintState' = "reading"
    /\ constraintTarget' = canonicalEpoch + 1
    /\ constraintRejected' = FALSE
    /\ queriesStarted' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        authoritativeHandle,
        materializedPostingsResident,
        durableWalEpochs
        >>

AcceptAuthoritativeConstraint ==
    /\ constraintState = "reading"
    /\ constraintPage \notin corruptPages
    /\ ~poisoned
    /\ constraintState' = "accepted"
    /\ loadedPages' = loadedPages \cup {constraintPage}
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        authoritativeHandle,
        materializedPostingsResident,
        constraintTarget,
        constraintPage,
        durableWalEpochs,
        constraintRejected
        >>

RejectAuthoritativeConstraint ==
    /\ constraintState = "reading"
    /\ constraintPage \notin corruptPages
    /\ constraintState' = "failed"
    /\ constraintRejected' = TRUE
    /\ loadedPages' = loadedPages \cup {constraintPage}
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        authoritativeHandle,
        materializedPostingsResident,
        constraintTarget,
        constraintPage,
        durableWalEpochs
        >>

FailAuthoritativeConstraint ==
    /\ constraintState = "reading"
    /\ constraintPage \in corruptPages
    /\ constraintState' = "failed"
    /\ constraintRejected' = TRUE
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        authoritativeHandle,
        materializedPostingsResident,
        constraintTarget,
        constraintPage,
        durableWalEpochs
        >>

PersistAuthoritativeWal ==
    /\ constraintState = "accepted"
    /\ ~poisoned
    /\ constraintTarget \notin durableWalEpochs
    /\ constraintState' = "durable"
    /\ durableWalEpochs' = durableWalEpochs \cup {constraintTarget}
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        authoritativeHandle,
        materializedPostingsResident,
        constraintTarget,
        constraintPage,
        constraintRejected
        >>

PublishAuthoritativeMutation ==
    /\ constraintState = "durable"
    /\ constraintTarget = canonicalEpoch + 1
    /\ constraintTarget \in durableWalEpochs
    /\ canonicalEpoch' = constraintTarget
    /\ visibleRowEpoch' = constraintTarget
    /\ visibleIndexEpoch' = constraintTarget
    /\ constraintState' = "idle"
    /\ constraintTarget' = 0
    /\ constraintPage' = 0
    /\ constraintRejected' = FALSE
    /\ UNCHANGED <<
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        authoritativeHandle,
        materializedPostingsResident,
        durableWalEpochs
        >>

ResetRejectedConstraint ==
    /\ constraintState = "failed"
    /\ ~poisoned
    /\ constraintState' = "idle"
    /\ constraintTarget' = 0
    /\ constraintPage' = 0
    /\ constraintRejected' = FALSE
    /\ UNCHANGED <<
        canonicalEpoch,
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        buildPhase,
        buildTarget,
        buildBaseGeneration,
        durableIndexEpochs,
        handleOpen,
        queriesStarted,
        loadedPages,
        corruptPages,
        queryState,
        requiredPage,
        poisoned,
        stalePublishRejected,
        visibleRowEpoch,
        visibleIndexEpoch,
        authoritativeHandle,
        materializedPostingsResident,
        durableWalEpochs
        >>

CrashAndRecover ==
    /\ handleOpen \/ buildPhase # "idle"
    /\ handleOpen' = FALSE
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ queryState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ buildBaseGeneration' = rootGeneration
    /\ canonicalEpoch' = IF constraintState = "durable" THEN constraintTarget ELSE canonicalEpoch
    /\ visibleRowEpoch' = IF constraintState = "durable" THEN constraintTarget ELSE visibleRowEpoch
    /\ visibleIndexEpoch' = IF constraintState = "durable" THEN constraintTarget ELSE visibleIndexEpoch
    /\ authoritativeHandle' = FALSE
    /\ materializedPostingsResident' = FALSE
    /\ constraintState' = "idle"
    /\ constraintTarget' = 0
    /\ constraintPage' = 0
    /\ constraintRejected' = FALSE
    /\ UNCHANGED <<
        rowRootEpoch,
        indexRootEpoch,
        rootGeneration,
        durableIndexEpochs,
        corruptPages,
        stalePublishRejected,
        durableWalEpochs
        >>

Next ==
    \/ Commit
    \/ BeginBuild
    \/ PersistIndexPages
    \/ PublishCompetingRoot
    \/ PublishBuiltRoot
    \/ RejectStalePublish
    \/ OpenHandle
    \/ OpenAuthoritativeHandle
    \/ BeginLookup
    \/ ReadHealthyPage
    \/ ReadCorruptPage
    \/ FinishLookup
    \/ CorruptColdPage
    \/ BeginAuthoritativeConstraint
    \/ AcceptAuthoritativeConstraint
    \/ RejectAuthoritativeConstraint
    \/ FailAuthoritativeConstraint
    \/ PersistAuthoritativeWal
    \/ PublishAuthoritativeMutation
    \/ ResetRejectedConstraint
    \/ CrashAndRecover

TypeOK ==
    /\ canonicalEpoch \in Epochs
    /\ rowRootEpoch \in Epochs
    /\ indexRootEpoch \in Epochs
    /\ rootGeneration \in Nat
    /\ buildPhase \in BuildPhases
    /\ buildTarget \in Epochs
    /\ buildBaseGeneration \in Nat
    /\ durableIndexEpochs \subseteq Epochs
    /\ handleOpen \in BOOLEAN
    /\ queriesStarted \in BOOLEAN
    /\ loadedPages \subseteq Pages
    /\ corruptPages \subseteq Pages
    /\ queryState \in QueryStates
    /\ requiredPage \in 0..MaxPage
    /\ poisoned \in BOOLEAN
    /\ stalePublishRejected \in BOOLEAN
    /\ visibleRowEpoch \in Epochs
    /\ visibleIndexEpoch \in Epochs
    /\ authoritativeHandle \in BOOLEAN
    /\ constraintState \in ConstraintStates
    /\ constraintTarget \in Epochs
    /\ constraintPage \in 0..MaxPage
    /\ durableWalEpochs \subseteq Epochs
    /\ constraintRejected \in BOOLEAN
    /\ materializedPostingsResident \in BOOLEAN

RowAndIndexRootsAgree == rowRootEpoch = indexRootEpoch

PublishedIndexIsDurable == indexRootEpoch \in durableIndexEpochs

PublishedRootsAreNotFuture == rowRootEpoch <= canonicalEpoch

OpenDoesNotWarmLeafPages ==
    handleOpen /\ ~queriesStarted => loadedPages = {}

SuccessfulLookupReadVerifiedPage ==
    queryState = "succeeded" =>
        /\ requiredPage \in loadedPages
        /\ requiredPage \notin corruptPages
        /\ ~poisoned

CorruptionPoisonsOpenHandle ==
    poisoned =>
        /\ handleOpen
        /\ \/ queryState = "failed"
           \/ constraintState = "failed"

AuthoritativeVisibleEpochsAgree ==
    authoritativeHandle =>
        /\ handleOpen
        /\ visibleRowEpoch = canonicalEpoch
        /\ visibleIndexEpoch = canonicalEpoch

AuthoritativeIndexIsRecoverable ==
    authoritativeHandle =>
        /\ indexRootEpoch \in durableIndexEpochs
        /\ \/ visibleIndexEpoch = indexRootEpoch
           \/ visibleIndexEpoch \in durableWalEpochs

AuthoritativeOmitsMaterializedPostings ==
    authoritativeHandle => ~materializedPostingsResident

ConstraintCheckUsesCurrentView ==
    constraintState # "idle" =>
        /\ authoritativeHandle
        /\ handleOpen
        /\ visibleRowEpoch = canonicalEpoch
        /\ visibleIndexEpoch = canonicalEpoch
        /\ constraintTarget = canonicalEpoch + 1

AcceptedConstraintHasNoWal ==
    constraintState = "accepted" => constraintTarget \notin durableWalEpochs

DurableMutationHasWal ==
    constraintState = "durable" => constraintTarget \in durableWalEpochs

RejectedConstraintLeavesCanonicalState ==
    constraintRejected =>
        /\ constraintState = "failed"
        /\ constraintTarget \notin durableWalEpochs
        /\ visibleRowEpoch = canonicalEpoch
        /\ visibleIndexEpoch = canonicalEpoch

AuthoritativeAdvanceHasDurableWal ==
    authoritativeHandle /\ canonicalEpoch > indexRootEpoch =>
        canonicalEpoch \in durableWalEpochs

Spec == Init /\ [][Next]_vars

=============================================================================
