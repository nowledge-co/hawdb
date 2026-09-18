---------------- MODULE HawDBRelationalIndexShadowPublication ----------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Relational index candidates are generation-specific, rebuildable files. *)
(* A candidate root set is selectable only when it exactly covers every     *)
(* primary, unique, secondary, and foreign-key-support role required by the *)
(* pinned catalog/schema digest. Candidate pages and their root manifest     *)
(* become durable before a matching                                          *)
(* canonical checkpoint may bind that exact generation. The optional binding *)
(* records the generation/epoch, exact root set, and both artifact digests.  *)
(* Candidate admission failure publishes no binding, while corruption or a   *)
(* crash orphan never prevents canonical recovery in non-authoritative modes. *)
(* A DemandPaged integrity failure fails the selected read closed, while      *)
(* Shadow mode only records candidate unavailability.                         *)
(***************************************************************************)

CONSTANT MaxGeneration, MaxEpoch, MaxPage

ASSUME /\ MaxGeneration \in Nat \ {0}
       /\ MaxEpoch \in Nat \ {0}
       /\ MaxPage \in Nat \ {0}

Generations == 0..MaxGeneration
Epochs == 0..MaxEpoch
Pages == 1..MaxPage
CandidateIds == [generation : Generations, epoch : Epochs]
BuildPhases == {
    "idle",
    "building_incomplete",
    "building_complete",
    "pages_durable_incomplete",
    "pages_durable_complete",
    "candidate_durable"
}
SelectionModes == {"none", "shadow", "demand"}
ReadStates == {"idle", "reading", "succeeded", "failed"}

Candidate(generation, epoch) ==
    [generation |-> generation, epoch |-> epoch]

VARIABLES
    canonicalGeneration,
    canonicalEpoch,
    canonicalHistory,
    canonicalCandidateBindings,
    buildPhase,
    buildGeneration,
    buildEpoch,
    durableArtifacts,
    durableCandidateManifests,
    exactRootSetManifests,
    abandonedCandidates,
    corruptCandidateManifests,
    canonicalOpen,
    candidateUnavailable,
    demandReadFailed,
    handleOpen,
    selectionMode,
    handleGeneration,
    handleEpoch,
    queriesStarted,
    loadedPages,
    corruptPages,
    readState,
    requiredPage,
    poisoned

vars == <<
    canonicalGeneration,
    canonicalEpoch,
    canonicalHistory,
    canonicalCandidateBindings,
    buildPhase,
    buildGeneration,
    buildEpoch,
    durableArtifacts,
    durableCandidateManifests,
    exactRootSetManifests,
    abandonedCandidates,
    corruptCandidateManifests,
    canonicalOpen,
    candidateUnavailable,
    demandReadFailed,
    handleOpen,
    selectionMode,
    handleGeneration,
    handleEpoch,
    queriesStarted,
    loadedPages,
    corruptPages,
    readState,
    requiredPage,
    poisoned
>>

Init ==
    /\ canonicalGeneration = 0
    /\ canonicalEpoch = 0
    /\ canonicalHistory = {Candidate(0, 0)}
    /\ canonicalCandidateBindings = {}
    /\ buildPhase = "idle"
    /\ buildGeneration = 0
    /\ buildEpoch = 0
    /\ durableArtifacts = {}
    /\ durableCandidateManifests = {}
    /\ exactRootSetManifests = {}
    /\ abandonedCandidates = {}
    /\ corruptCandidateManifests = {}
    /\ canonicalOpen = FALSE
    /\ candidateUnavailable = FALSE
    /\ demandReadFailed = FALSE
    /\ handleOpen = FALSE
    /\ selectionMode = "none"
    /\ handleGeneration = 0
    /\ handleEpoch = 0
    /\ queriesStarted = FALSE
    /\ loadedPages = {}
    /\ corruptPages = {}
    /\ readState = "idle"
    /\ requiredPage = 0
    /\ poisoned = FALSE

BeginCandidate ==
    /\ buildPhase = "idle"
    /\ canonicalGeneration < MaxGeneration
    /\ \E generation \in (canonicalGeneration + 1)..MaxGeneration,
          epoch \in canonicalEpoch..MaxEpoch:
        /\ buildGeneration' = generation
        /\ buildEpoch' = epoch
    /\ buildPhase' = "building_incomplete"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

CompleteRequiredRootSet ==
    /\ buildPhase = "building_incomplete"
    /\ buildPhase' = "building_complete"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

(***************************************************************************)
(* A missing role, an extra root, a role/identity mismatch, or a schema     *)
(* digest change all collapse to an incomplete logical root-set predicate.  *)
(***************************************************************************)
InvalidateRequiredRootSet ==
    /\ buildPhase = "building_complete"
    /\ buildPhase' = "building_incomplete"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

PersistCandidatePages ==
    /\ buildPhase \in {"building_incomplete", "building_complete"}
    /\ buildPhase' = IF buildPhase = "building_complete"
                     THEN "pages_durable_complete"
                     ELSE "pages_durable_incomplete"
    /\ durableArtifacts' = durableArtifacts \cup {buildGeneration}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildGeneration,
        buildEpoch,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

PersistCandidateManifest ==
    /\ buildPhase \in {"pages_durable_incomplete", "pages_durable_complete"}
    /\ buildGeneration \in durableArtifacts
    /\ buildPhase' = "candidate_durable"
    /\ durableCandidateManifests' =
        durableCandidateManifests \cup {Candidate(buildGeneration, buildEpoch)}
    /\ exactRootSetManifests' =
        IF buildPhase = "pages_durable_complete"
        THEN exactRootSetManifests \cup {Candidate(buildGeneration, buildEpoch)}
        ELSE exactRootSetManifests
    /\ corruptCandidateManifests' =
        IF buildPhase = "pages_durable_complete"
        THEN corruptCandidateManifests
        ELSE corruptCandidateManifests \cup {Candidate(buildGeneration, buildEpoch)}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        abandonedCandidates,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

PublishCheckpointWithCandidate ==
    /\ buildPhase = "candidate_durable"
    /\ Candidate(buildGeneration, buildEpoch) \in durableCandidateManifests
    /\ Candidate(buildGeneration, buildEpoch) \in exactRootSetManifests
    /\ buildGeneration > canonicalGeneration
    /\ buildEpoch >= canonicalEpoch
    /\ canonicalGeneration' = buildGeneration
    /\ canonicalEpoch' = buildEpoch
    /\ canonicalHistory' =
        canonicalHistory \cup {Candidate(buildGeneration, buildEpoch)}
    /\ canonicalCandidateBindings' =
        canonicalCandidateBindings \cup {Candidate(buildGeneration, buildEpoch)}
    /\ buildPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ UNCHANGED <<
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

(***************************************************************************)
(* Canonical checkpoint publication is independent from a non-authoritative *)
(* candidate. This is the admission/error path used by Shadow and            *)
(* DemandPaged while the materialized constraint oracle still exists.        *)
(***************************************************************************)
PublishCheckpointWithoutCandidate ==
    /\ buildPhase = "idle"
    /\ canonicalGeneration < MaxGeneration
    /\ \E generation \in (canonicalGeneration + 1)..MaxGeneration,
          epoch \in canonicalEpoch..MaxEpoch:
        /\ canonicalGeneration' = generation
        /\ canonicalEpoch' = epoch
        /\ canonicalHistory' = canonicalHistory \cup {Candidate(generation, epoch)}
        /\ Candidate(generation, epoch) \notin durableCandidateManifests
    /\ UNCHANGED <<
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

CrashBeforeCheckpoint ==
    /\ buildPhase # "idle"
    /\ abandonedCandidates' =
        abandonedCandidates \cup {Candidate(buildGeneration, buildEpoch)}
    /\ buildPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

OpenCanonical ==
    /\ ~canonicalOpen
    /\ canonicalOpen' = TRUE
    /\ candidateUnavailable' = FALSE
    /\ demandReadFailed' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

OpenExactCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in durableCandidateManifests \ corruptCandidateManifests
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in canonicalCandidateBindings
    /\ Candidate(canonicalGeneration, canonicalEpoch) \in exactRootSetManifests
    /\ \E mode \in {"shadow", "demand"}: selectionMode' = mode
    /\ handleOpen' = TRUE
    /\ handleGeneration' = canonicalGeneration
    /\ handleEpoch' = canonicalEpoch
    /\ candidateUnavailable' = FALSE
    /\ demandReadFailed' = FALSE
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        corruptPages
        >>

ObserveMissingCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ \/ Candidate(canonicalGeneration, canonicalEpoch)
            \notin canonicalCandidateBindings
       \/ Candidate(canonicalGeneration, canonicalEpoch)
            \notin durableCandidateManifests
    /\ \E mode \in {"shadow", "demand"}: selectionMode' = mode
    /\ candidateUnavailable' = TRUE
    /\ demandReadFailed' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        handleOpen,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

CorruptCandidateManifest ==
    /\ \E candidate \in durableCandidateManifests:
        corruptCandidateManifests' = corruptCandidateManifests \cup {candidate}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

RejectCorruptShadowCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in corruptCandidateManifests
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in canonicalCandidateBindings
    /\ selectionMode' = "shadow"
    /\ candidateUnavailable' = TRUE
    /\ demandReadFailed' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        handleOpen,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

RejectCorruptDemandCandidate ==
    /\ canonicalOpen
    /\ ~handleOpen
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in corruptCandidateManifests
    /\ Candidate(canonicalGeneration, canonicalEpoch)
        \in canonicalCandidateBindings
    /\ selectionMode' = "demand"
    /\ candidateUnavailable' = TRUE
    /\ demandReadFailed' = TRUE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        handleOpen,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        readState,
        requiredPage,
        poisoned
        >>

BeginPageRead ==
    /\ handleOpen
    /\ ~poisoned
    /\ readState = "idle"
    /\ \E page \in Pages: requiredPage' = page
    /\ queriesStarted' = TRUE
    /\ readState' = "reading"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        loadedPages,
        corruptPages,
        poisoned
        >>

ReadHealthyPage ==
    /\ readState = "reading"
    /\ requiredPage \notin corruptPages
    /\ loadedPages' = loadedPages \cup {requiredPage}
    /\ readState' = "succeeded"
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        corruptPages,
        requiredPage,
        poisoned
        >>

ReadCorruptPage ==
    /\ readState = "reading"
    /\ requiredPage \in corruptPages
    /\ readState' = "failed"
    /\ poisoned' = TRUE
    /\ demandReadFailed' = (demandReadFailed \/ (selectionMode = "demand"))
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        requiredPage
        >>

FinishRead ==
    /\ readState = "succeeded"
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        corruptPages,
        poisoned
        >>

CorruptColdPage ==
    /\ \E page \in Pages \ loadedPages:
        corruptPages' = corruptPages \cup {page}
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        canonicalOpen,
        candidateUnavailable,
        demandReadFailed,
        handleOpen,
        selectionMode,
        handleGeneration,
        handleEpoch,
        queriesStarted,
        loadedPages,
        readState,
        requiredPage,
        poisoned
        >>

CrashDatabase ==
    /\ canonicalOpen \/ handleOpen
    /\ canonicalOpen' = FALSE
    /\ candidateUnavailable' = FALSE
    /\ demandReadFailed' = FALSE
    /\ handleOpen' = FALSE
    /\ selectionMode' = "none"
    /\ handleGeneration' = 0
    /\ handleEpoch' = 0
    /\ queriesStarted' = FALSE
    /\ loadedPages' = {}
    /\ readState' = "idle"
    /\ requiredPage' = 0
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration,
        canonicalEpoch,
        canonicalHistory,
        canonicalCandidateBindings,
        buildPhase,
        buildGeneration,
        buildEpoch,
        durableArtifacts,
        durableCandidateManifests,
        exactRootSetManifests,
        abandonedCandidates,
        corruptCandidateManifests,
        corruptPages
        >>

Next ==
    \/ BeginCandidate
    \/ CompleteRequiredRootSet
    \/ InvalidateRequiredRootSet
    \/ PersistCandidatePages
    \/ PersistCandidateManifest
    \/ PublishCheckpointWithCandidate
    \/ PublishCheckpointWithoutCandidate
    \/ CrashBeforeCheckpoint
    \/ OpenCanonical
    \/ OpenExactCandidate
    \/ ObserveMissingCandidate
    \/ CorruptCandidateManifest
    \/ RejectCorruptShadowCandidate
    \/ RejectCorruptDemandCandidate
    \/ BeginPageRead
    \/ ReadHealthyPage
    \/ ReadCorruptPage
    \/ FinishRead
    \/ CorruptColdPage
    \/ CrashDatabase

TypeOK ==
    /\ canonicalGeneration \in Generations
    /\ canonicalEpoch \in Epochs
    /\ canonicalHistory \subseteq CandidateIds
    /\ canonicalCandidateBindings \subseteq CandidateIds
    /\ buildPhase \in BuildPhases
    /\ buildGeneration \in Generations
    /\ buildEpoch \in Epochs
    /\ durableArtifacts \subseteq (1..MaxGeneration)
    /\ durableCandidateManifests \subseteq CandidateIds
    /\ exactRootSetManifests \subseteq durableCandidateManifests
    /\ abandonedCandidates \subseteq CandidateIds
    /\ corruptCandidateManifests \subseteq durableCandidateManifests
    /\ canonicalOpen \in BOOLEAN
    /\ candidateUnavailable \in BOOLEAN
    /\ demandReadFailed \in BOOLEAN
    /\ handleOpen \in BOOLEAN
    /\ selectionMode \in SelectionModes
    /\ handleGeneration \in Generations
    /\ handleEpoch \in Epochs
    /\ queriesStarted \in BOOLEAN
    /\ loadedPages \subseteq Pages
    /\ corruptPages \subseteq Pages
    /\ readState \in ReadStates
    /\ requiredPage \in 0..MaxPage
    /\ poisoned \in BOOLEAN

CandidateManifestHasDurablePages ==
    \A candidate \in durableCandidateManifests:
        candidate.generation \in durableArtifacts

CanonicalBindingHasDurableExactCandidate ==
    /\ canonicalCandidateBindings \subseteq durableCandidateManifests
    /\ canonicalCandidateBindings \subseteq exactRootSetManifests
    /\ canonicalCandidateBindings \subseteq canonicalHistory

CanonicalGenerationNeverRegresses ==
    /\ Candidate(canonicalGeneration, canonicalEpoch) \in canonicalHistory
    /\ \A candidate \in canonicalHistory:
        candidate.generation <= canonicalGeneration

OpenHandlePinsSelectedCandidate ==
    handleOpen =>
        /\ canonicalOpen
        /\ Candidate(handleGeneration, handleEpoch) \in canonicalHistory
        /\ Candidate(handleGeneration, handleEpoch) \in durableCandidateManifests
        /\ Candidate(handleGeneration, handleEpoch) \in canonicalCandidateBindings
        /\ handleGeneration <= canonicalGeneration

SelectableCandidateHasExactRequiredRoots ==
    handleOpen =>
        Candidate(handleGeneration, handleEpoch) \in exactRootSetManifests

OpenDoesNotWarmPages ==
    handleOpen /\ ~queriesStarted => loadedPages = {}

SuccessfulReadVerifiedPage ==
    readState = "succeeded" =>
        /\ requiredPage \in loadedPages
        /\ requiredPage \notin corruptPages
        /\ ~poisoned

CandidateFailurePreservesCanonicalOpen ==
    (candidateUnavailable \/ demandReadFailed) => canonicalOpen

DemandIntegrityFailureFailsClosed ==
    demandReadFailed =>
        /\ selectionMode = "demand"
        /\ canonicalOpen

CorruptionPoisonsOnlyCandidateHandle ==
    poisoned =>
        /\ canonicalOpen
        /\ handleOpen
        /\ readState = "failed"

Spec == Init /\ [][Next]_vars

=============================================================================
