------------------- MODULE HawDBOverflowExactCompaction -------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Exact relational overflow compaction scans the row-visible closure under *)
(* one source epoch, admits the complete rewrite before candidate creation, *)
(* rewrites every reachable envelope into a fresh physical generation, and  *)
(* selects the row/overflow pair only through the outer checkpoint manifest. *)
(* Source mutations may add or remove reachable values. Older roots remain   *)
(* readable while pinned. A stale or rejected scan never changes selection.  *)
(***************************************************************************)

CONSTANT Readers, Extents, MaxGeneration, MaxEpoch

ASSUME /\ Readers # {}
       /\ Extents # {}
       /\ MaxGeneration \in Nat \ {0, 1}
       /\ MaxEpoch \in Nat \ {0}

Generations == 1..MaxGeneration
ReaderGenerations == {-1} \cup Generations
Phases == {
    "idle",
    "scanning",
    "admitted",
    "extentsDurable",
    "rootDurable",
    "manifestDurable"
}

EmptyPhysical == [extent \in Extents |-> 0]

VARIABLES
    sourceEpoch,
    rowReachable,
    selectedGeneration,
    previousGeneration,
    nextGeneration,
    publishedGenerations,
    rootByGeneration,
    rootEpoch,
    physicalByGeneration,
    availableExtentGenerations,
    durableRootGenerations,
    durableManifestGenerations,
    phase,
    scanEpoch,
    candidateGeneration,
    candidateRoot,
    readerGeneration,
    admissionRejected,
    staleRejected,
    cancelled

vars == <<
    sourceEpoch,
    rowReachable,
    selectedGeneration,
    previousGeneration,
    nextGeneration,
    publishedGenerations,
    rootByGeneration,
    rootEpoch,
    physicalByGeneration,
    availableExtentGenerations,
    durableRootGenerations,
    durableManifestGenerations,
    phase,
    scanEpoch,
    candidateGeneration,
    candidateRoot,
    readerGeneration,
    admissionRejected,
    staleRejected,
    cancelled
>>

PinnedGenerations ==
    {readerGeneration[reader] : reader \in Readers} \ {-1}

RetainedGenerations ==
    {selectedGeneration, previousGeneration} \cup PinnedGenerations

RootReadable(generation) ==
    /\ generation \in publishedGenerations
    /\ generation \in availableExtentGenerations
    /\ generation \in durableRootGenerations
    /\ generation \in durableManifestGenerations
    /\ \A extent \in rootByGeneration[generation]:
        physicalByGeneration[generation][extent] \in availableExtentGenerations

Init ==
    /\ sourceEpoch = 1
    /\ rowReachable = {}
    /\ selectedGeneration = 1
    /\ previousGeneration = 1
    /\ nextGeneration = 2
    /\ publishedGenerations = {1}
    /\ rootByGeneration =
        [generation \in Generations |-> {}]
    /\ rootEpoch = [generation \in Generations |-> IF generation = 1 THEN 1 ELSE 0]
    /\ physicalByGeneration =
        [generation \in Generations |->
            IF generation = 1
            THEN [extent \in Extents |-> 1]
            ELSE EmptyPhysical]
    /\ availableExtentGenerations = {1}
    /\ durableRootGenerations = {1}
    /\ durableManifestGenerations = {1}
    /\ phase = "idle"
    /\ scanEpoch = 0
    /\ candidateGeneration = 1
    /\ candidateRoot = {}
    /\ readerGeneration = [reader \in Readers |-> -1]
    /\ admissionRejected = FALSE
    /\ staleRejected = FALSE
    /\ cancelled = FALSE

ChangeReachableSet ==
    /\ sourceEpoch < MaxEpoch
    /\ \E replacement \in SUBSET Extents:
        /\ replacement # rowReachable
        /\ rowReachable' = replacement
    /\ sourceEpoch' = sourceEpoch + 1
    /\ UNCHANGED <<
        selectedGeneration, previousGeneration, nextGeneration,
        publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, durableManifestGenerations,
        phase, scanEpoch, candidateGeneration, candidateRoot,
        readerGeneration, admissionRejected, staleRejected, cancelled
        >>

BeginScan ==
    /\ phase = "idle"
    /\ nextGeneration <= MaxGeneration
    /\ phase' = "scanning"
    /\ scanEpoch' = sourceEpoch
    /\ candidateGeneration' = nextGeneration
    /\ candidateRoot' = {}
    /\ admissionRejected' = FALSE
    /\ staleRejected' = FALSE
    /\ cancelled' = FALSE
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, durableManifestGenerations, readerGeneration
        >>

RejectAdmission ==
    /\ phase = "scanning"
    /\ phase' = "idle"
    /\ candidateRoot' = {}
    /\ admissionRejected' = TRUE
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, durableManifestGenerations,
        scanEpoch, candidateGeneration, readerGeneration, staleRejected,
        cancelled
        >>

AdmitExactClosure ==
    /\ phase = "scanning"
    /\ scanEpoch = sourceEpoch
    /\ phase' = "admitted"
    /\ candidateRoot' = rowReachable
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, durableManifestGenerations,
        scanEpoch, candidateGeneration, readerGeneration,
        admissionRejected, staleRejected, cancelled
        >>

RejectStaleScan ==
    /\ phase # "idle"
    /\ scanEpoch # sourceEpoch
    /\ phase' = "idle"
    /\ candidateRoot' = {}
    /\ staleRejected' = TRUE
    /\ rootByGeneration' =
        [rootByGeneration EXCEPT ![candidateGeneration] = {}]
    /\ rootEpoch' =
        [rootEpoch EXCEPT ![candidateGeneration] = 0]
    /\ physicalByGeneration' =
        [physicalByGeneration EXCEPT ![candidateGeneration] = EmptyPhysical]
    /\ availableExtentGenerations' =
        availableExtentGenerations \ {candidateGeneration}
    /\ durableRootGenerations' =
        durableRootGenerations \ {candidateGeneration}
    /\ durableManifestGenerations' =
        durableManifestGenerations \ {candidateGeneration}
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations,
        scanEpoch, candidateGeneration, readerGeneration, admissionRejected,
        cancelled
        >>

CancelCompaction ==
    /\ phase # "idle"
    /\ phase' = "idle"
    /\ candidateRoot' = {}
    /\ cancelled' = TRUE
    /\ rootByGeneration' =
        [rootByGeneration EXCEPT ![candidateGeneration] = {}]
    /\ rootEpoch' =
        [rootEpoch EXCEPT ![candidateGeneration] = 0]
    /\ physicalByGeneration' =
        [physicalByGeneration EXCEPT ![candidateGeneration] = EmptyPhysical]
    /\ availableExtentGenerations' =
        availableExtentGenerations \ {candidateGeneration}
    /\ durableRootGenerations' =
        durableRootGenerations \ {candidateGeneration}
    /\ durableManifestGenerations' =
        durableManifestGenerations \ {candidateGeneration}
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations,
        scanEpoch, candidateGeneration, readerGeneration,
        admissionRejected, staleRejected
        >>

PersistRewrittenExtents ==
    /\ phase = "admitted"
    /\ scanEpoch = sourceEpoch
    /\ phase' = "extentsDurable"
    /\ availableExtentGenerations' =
        availableExtentGenerations \cup {candidateGeneration}
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, durableRootGenerations,
        durableManifestGenerations, scanEpoch, candidateGeneration,
        candidateRoot, readerGeneration, admissionRejected, staleRejected,
        cancelled
        >>

PersistExactRoot ==
    /\ phase = "extentsDurable"
    /\ scanEpoch = sourceEpoch
    /\ phase' = "rootDurable"
    /\ rootByGeneration' =
        [rootByGeneration EXCEPT ![candidateGeneration] = candidateRoot]
    /\ rootEpoch' =
        [rootEpoch EXCEPT ![candidateGeneration] = scanEpoch]
    /\ physicalByGeneration' =
        [physicalByGeneration EXCEPT
            ![candidateGeneration] =
                [extent \in Extents |->
                    IF extent \in candidateRoot THEN candidateGeneration ELSE 0]]
    /\ durableRootGenerations' =
        durableRootGenerations \cup {candidateGeneration}
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, availableExtentGenerations,
        durableManifestGenerations, scanEpoch, candidateGeneration,
        candidateRoot, readerGeneration, admissionRejected, staleRejected,
        cancelled
        >>

PersistGenerationManifest ==
    /\ phase = "rootDurable"
    /\ phase' = "manifestDurable"
    /\ durableManifestGenerations' =
        durableManifestGenerations \cup {candidateGeneration}
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, scanEpoch, candidateGeneration,
        candidateRoot, readerGeneration, admissionRejected, staleRejected,
        cancelled
        >>

PublishOuterManifest ==
    /\ phase = "manifestDurable"
    /\ scanEpoch = sourceEpoch
    /\ rootByGeneration[candidateGeneration] = rowReachable
    /\ previousGeneration' = selectedGeneration
    /\ selectedGeneration' = candidateGeneration
    /\ publishedGenerations' = publishedGenerations \cup {candidateGeneration}
    /\ nextGeneration' = candidateGeneration + 1
    /\ phase' = "idle"
    /\ candidateRoot' = {}
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, durableManifestGenerations,
        scanEpoch, candidateGeneration, readerGeneration,
        admissionRejected, staleRejected, cancelled
        >>

PinReader(reader) ==
    /\ reader \in Readers
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = selectedGeneration]
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, durableManifestGenerations,
        phase, scanEpoch, candidateGeneration, candidateRoot,
        admissionRejected, staleRejected, cancelled
        >>

UnpinReader(reader) ==
    /\ reader \in Readers
    /\ readerGeneration[reader] # -1
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, availableExtentGenerations,
        durableRootGenerations, durableManifestGenerations,
        phase, scanEpoch, candidateGeneration, candidateRoot,
        admissionRejected, staleRejected, cancelled
        >>

Reclaim ==
    /\ phase = "idle"
    /\ availableExtentGenerations' =
        availableExtentGenerations \cap RetainedGenerations
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations, rootByGeneration, rootEpoch,
        physicalByGeneration, durableRootGenerations,
        durableManifestGenerations, phase, scanEpoch,
        candidateGeneration, candidateRoot, readerGeneration,
        admissionRejected, staleRejected, cancelled
        >>

Crash ==
    /\ phase # "idle"
    /\ phase' = "idle"
    /\ candidateRoot' = {}
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ rootByGeneration' =
        [rootByGeneration EXCEPT ![candidateGeneration] = {}]
    /\ rootEpoch' =
        [rootEpoch EXCEPT ![candidateGeneration] = 0]
    /\ physicalByGeneration' =
        [physicalByGeneration EXCEPT ![candidateGeneration] = EmptyPhysical]
    /\ availableExtentGenerations' =
        availableExtentGenerations \ {candidateGeneration}
    /\ durableRootGenerations' =
        durableRootGenerations \ {candidateGeneration}
    /\ durableManifestGenerations' =
        durableManifestGenerations \ {candidateGeneration}
    /\ UNCHANGED <<
        sourceEpoch, rowReachable, selectedGeneration, previousGeneration,
        nextGeneration, publishedGenerations,
        scanEpoch, candidateGeneration, admissionRejected, staleRejected,
        cancelled
        >>

Next ==
    \/ ChangeReachableSet
    \/ BeginScan
    \/ RejectAdmission
    \/ AdmitExactClosure
    \/ RejectStaleScan
    \/ CancelCompaction
    \/ PersistRewrittenExtents
    \/ PersistExactRoot
    \/ PersistGenerationManifest
    \/ PublishOuterManifest
    \/ \E reader \in Readers: PinReader(reader)
    \/ \E reader \in Readers: UnpinReader(reader)
    \/ Reclaim
    \/ Crash

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ sourceEpoch \in 1..MaxEpoch
    /\ rowReachable \subseteq Extents
    /\ selectedGeneration \in Generations
    /\ previousGeneration \in Generations
    /\ nextGeneration \in 2..(MaxGeneration + 1)
    /\ publishedGenerations \subseteq Generations
    /\ rootByGeneration \in [Generations -> SUBSET Extents]
    /\ rootEpoch \in [Generations -> 0..MaxEpoch]
    /\ physicalByGeneration \in [Generations -> [Extents -> 0..MaxGeneration]]
    /\ availableExtentGenerations \subseteq Generations
    /\ durableRootGenerations \subseteq Generations
    /\ durableManifestGenerations \subseteq Generations
    /\ phase \in Phases
    /\ scanEpoch \in 0..MaxEpoch
    /\ candidateGeneration \in Generations
    /\ candidateRoot \subseteq Extents
    /\ readerGeneration \in [Readers -> ReaderGenerations]
    /\ admissionRejected \in BOOLEAN
    /\ staleRejected \in BOOLEAN
    /\ cancelled \in BOOLEAN

SelectedRootCoversVisibleRows ==
    rootEpoch[selectedGeneration] = sourceEpoch
        => rowReachable \subseteq rootByGeneration[selectedGeneration]

OuterManifestIsPublishLast ==
    \A generation \in publishedGenerations \cap RetainedGenerations:
        /\ generation \in durableRootGenerations
        /\ generation \in durableManifestGenerations
        /\ generation \in availableExtentGenerations

ExactPublishedClosure ==
    rootEpoch[selectedGeneration] = sourceEpoch
        => rootByGeneration[selectedGeneration] = rowReachable

FreshPhysicalRewrite ==
    \A generation \in publishedGenerations \ {1}:
        \A extent \in rootByGeneration[generation]:
            physicalByGeneration[generation][extent] = generation

RejectedWorkDoesNotSelectCandidate ==
    (admissionRejected \/ staleRejected \/ cancelled)
        => candidateGeneration # selectedGeneration

CandidateIsUnselectedBeforeOuterManifest ==
    phase # "idle" => candidateGeneration # selectedGeneration

NoArtifactsBeforeAdmission ==
    phase \in {"scanning", "admitted"}
        => candidateGeneration \notin durableRootGenerations
           /\ candidateGeneration \notin durableManifestGenerations

PinnedReadersRemainReadable ==
    \A reader \in Readers:
        readerGeneration[reader] # -1 => RootReadable(readerGeneration[reader])

SelectedRootRemainsReadable == RootReadable(selectedGeneration)

=============================================================================
