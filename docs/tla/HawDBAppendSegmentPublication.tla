------------------ MODULE HawDBAppendSegmentPublication ------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Durable append batches remain recoverable from the active checkpoint    *)
(* plus the WAL suffix. A checkpoint candidate writes immutable segments   *)
(* before its manifest, publishes the selector last, and advances the WAL  *)
(* floor only after that complete checkpoint is canonical. Readers pin a   *)
(* selected generation, and reclamation cannot remove active, previous, or *)
(* pinned generations.                                                     *)
(***************************************************************************)

CONSTANTS
    Readers,
    MaxEpoch,
    MaxGeneration,
    MutateManifestBeforeSegment,
    MutateReclaimPinned

ASSUME /\ Readers # {}
       /\ MaxEpoch \in Nat \ {0}
       /\ MaxGeneration \in Nat \ {0}
       /\ MutateManifestBeforeSegment \in BOOLEAN
       /\ MutateReclaimPinned \in BOOLEAN

Epochs == 0..MaxEpoch
Generations == 0..MaxGeneration
ReaderGenerations == (-1)..MaxGeneration
PendingPhases == {"none", "appended", "durable"}
CandidatePhases == {"idle", "building", "segmentsDurable", "manifestDurable"}

VARIABLES
    walDurableEpoch,
    visibleEpoch,
    pendingPhase,
    pendingEpoch,
    checkpointEpoch,
    walFloorEpoch,
    activeGeneration,
    previousGeneration,
    nextGeneration,
    candidatePhase,
    candidateGeneration,
    candidateEpoch,
    publishedGenerations,
    durableSegments,
    generationEpoch,
    readerGeneration,
    reclaimedGenerations

vars == <<
    walDurableEpoch,
    visibleEpoch,
    pendingPhase,
    pendingEpoch,
    checkpointEpoch,
    walFloorEpoch,
    activeGeneration,
    previousGeneration,
    nextGeneration,
    candidatePhase,
    candidateGeneration,
    candidateEpoch,
    publishedGenerations,
    durableSegments,
    generationEpoch,
    readerGeneration,
    reclaimedGenerations
>>

PinnedReaders ==
    {reader \in Readers : readerGeneration[reader] # -1}

PinnedGenerations ==
    {readerGeneration[reader] : reader \in PinnedReaders}

CandidateGenerations ==
    IF candidatePhase = "idle" THEN {} ELSE {candidateGeneration}

RequiredGenerations ==
    {activeGeneration, previousGeneration}
        \cup PinnedGenerations
        \cup CandidateGenerations

Init ==
    /\ walDurableEpoch = 0
    /\ visibleEpoch = 0
    /\ pendingPhase = "none"
    /\ pendingEpoch = 0
    /\ checkpointEpoch = 0
    /\ walFloorEpoch = 0
    /\ activeGeneration = 0
    /\ previousGeneration = 0
    /\ nextGeneration = 1
    /\ candidatePhase = "idle"
    /\ candidateGeneration = 0
    /\ candidateEpoch = 0
    /\ publishedGenerations = {0}
    /\ durableSegments = {0}
    /\ generationEpoch =
        [generation \in Generations |-> IF generation = 0 THEN 0 ELSE -1]
    /\ readerGeneration = [reader \in Readers |-> -1]
    /\ reclaimedGenerations = {}

BeginCommit ==
    /\ pendingPhase = "none"
    /\ visibleEpoch < MaxEpoch
    /\ pendingPhase' = "appended"
    /\ pendingEpoch' = visibleEpoch + 1
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        readerGeneration,
        reclaimedGenerations
        >>

SyncWal ==
    /\ pendingPhase = "appended"
    /\ pendingEpoch = walDurableEpoch + 1
    /\ walDurableEpoch' = pendingEpoch
    /\ pendingPhase' = "durable"
    /\ UNCHANGED <<
        visibleEpoch,
        pendingEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        readerGeneration,
        reclaimedGenerations
        >>

PublishCommit ==
    /\ pendingPhase = "durable"
    /\ pendingEpoch <= walDurableEpoch
    /\ visibleEpoch' = pendingEpoch
    /\ pendingPhase' = "none"
    /\ pendingEpoch' = 0
    /\ UNCHANGED <<
        walDurableEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        readerGeneration,
        reclaimedGenerations
        >>

BeginCheckpoint ==
    /\ candidatePhase = "idle"
    /\ checkpointEpoch < visibleEpoch
    /\ nextGeneration <= MaxGeneration
    /\ candidatePhase' = "building"
    /\ candidateGeneration' = nextGeneration
    /\ candidateEpoch' = visibleEpoch
    /\ nextGeneration' = nextGeneration + 1
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        readerGeneration,
        reclaimedGenerations
        >>

PersistSegments ==
    /\ candidatePhase = "building"
    /\ candidateGeneration \notin durableSegments
    /\ durableSegments' = durableSegments \cup {candidateGeneration}
    /\ generationEpoch' =
        [generationEpoch EXCEPT ![candidateGeneration] = candidateEpoch]
    /\ candidatePhase' = "segmentsDurable"
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidateGeneration,
        candidateEpoch,
        publishedGenerations,
        readerGeneration,
        reclaimedGenerations
        >>

PersistManifest ==
    /\ (candidatePhase = "segmentsDurable"
        \/ (MutateManifestBeforeSegment /\ candidatePhase = "building"))
    /\ publishedGenerations' =
        publishedGenerations \cup {candidateGeneration}
    /\ candidatePhase' = "manifestDurable"
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidateGeneration,
        candidateEpoch,
        durableSegments,
        generationEpoch,
        readerGeneration,
        reclaimedGenerations
        >>

PublishSelector ==
    /\ candidatePhase = "manifestDurable"
    /\ previousGeneration' = activeGeneration
    /\ activeGeneration' = candidateGeneration
    /\ checkpointEpoch' = candidateEpoch
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateEpoch' = 0
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        walFloorEpoch,
        nextGeneration,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        readerGeneration,
        reclaimedGenerations
        >>

PinReader(reader) ==
    /\ reader \in Readers
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = activeGeneration]
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        reclaimedGenerations
        >>

ReleaseReader(reader) ==
    /\ reader \in Readers
    /\ readerGeneration[reader] # -1
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        reclaimedGenerations
        >>

ReclaimGeneration(generation) ==
    /\ generation \in publishedGenerations \ {0}
    /\ generation \notin {activeGeneration, previousGeneration}
    /\ (candidatePhase = "idle" \/ generation # candidateGeneration)
    /\ (MutateReclaimPinned \/ generation \notin PinnedGenerations)
    /\ publishedGenerations' = publishedGenerations \ {generation}
    /\ durableSegments' = durableSegments \ {generation}
    /\ reclaimedGenerations' = reclaimedGenerations \cup {generation}
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        generationEpoch,
        readerGeneration
        >>

TruncateWal ==
    /\ walFloorEpoch < checkpointEpoch
    /\ walFloorEpoch' = checkpointEpoch
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        checkpointEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        readerGeneration,
        reclaimedGenerations
        >>

CrashRecover ==
    /\ visibleEpoch' = walDurableEpoch
    /\ pendingPhase' = "none"
    /\ pendingEpoch' = 0
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateEpoch' = 0
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ UNCHANGED <<
        walDurableEpoch,
        checkpointEpoch,
        walFloorEpoch,
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedGenerations,
        durableSegments,
        generationEpoch,
        reclaimedGenerations
        >>

Next ==
    \/ BeginCommit
    \/ SyncWal
    \/ PublishCommit
    \/ BeginCheckpoint
    \/ PersistSegments
    \/ PersistManifest
    \/ PublishSelector
    \/ \E reader \in Readers: PinReader(reader)
    \/ \E reader \in Readers: ReleaseReader(reader)
    \/ \E generation \in Generations: ReclaimGeneration(generation)
    \/ TruncateWal
    \/ CrashRecover

TypeOK ==
    /\ walDurableEpoch \in Epochs
    /\ visibleEpoch \in Epochs
    /\ pendingPhase \in PendingPhases
    /\ pendingEpoch \in Epochs
    /\ checkpointEpoch \in Epochs
    /\ walFloorEpoch \in Epochs
    /\ activeGeneration \in Generations
    /\ previousGeneration \in Generations
    /\ nextGeneration \in 1..(MaxGeneration + 1)
    /\ candidatePhase \in CandidatePhases
    /\ candidateGeneration \in Generations
    /\ candidateEpoch \in Epochs
    /\ publishedGenerations \subseteq Generations
    /\ durableSegments \subseteq Generations
    /\ generationEpoch \in [Generations -> (-1)..MaxEpoch]
    /\ readerGeneration \in [Readers -> ReaderGenerations]
    /\ reclaimedGenerations \subseteq Generations

VisibleOnlyAfterWal == visibleEpoch <= walDurableEpoch

CheckpointDoesNotLeadVisibility == checkpointEpoch <= visibleEpoch

ManifestReferencesCompleteSegments ==
    publishedGenerations \subseteq durableSegments

ActiveManifestIsPublished ==
    {activeGeneration, previousGeneration} \subseteq publishedGenerations

ActiveGenerationMatchesCheckpoint ==
    generationEpoch[activeGeneration] = checkpointEpoch

PinnedReaderGenerationIsRetained ==
    PinnedGenerations \subseteq publishedGenerations
    /\ PinnedGenerations \subseteq durableSegments

ReclamationPreservesRequiredGenerations ==
    RequiredGenerations \cap reclaimedGenerations = {}

WalTruncationHasCheckpoint == walFloorEpoch <= checkpointEpoch

ReclaimedGenerationIsUnreachable ==
    reclaimedGenerations \cap publishedGenerations = {}

Spec == Init /\ [][Next]_vars

=============================================================================
