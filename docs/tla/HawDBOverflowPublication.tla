---------------------- MODULE HawDBOverflowPublication ----------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Relational overflow values are content-addressed immutable envelopes. A  *)
(* generation writes only new envelopes, reuses physical generations only  *)
(* for reachable digests selected by the base root, then publishes          *)
(* generation-manifest artifacts before replacing the latest manifest. A    *)
(* row root can bind only the exact selected overflow generation. Readers   *)
(* pin immutable roots; reclamation retains the transitive physical extent  *)
(* closure of the active, previous, row-bound, and reader-pinned roots.      *)
(***************************************************************************)

CONSTANT Readers, Extents, MaxGeneration

ASSUME /\ Readers # {}
       /\ Extents # {}
       /\ MaxGeneration \in Nat \ {0}

Generations == 0..MaxGeneration
ReaderGenerations == (-1)..MaxGeneration
CandidatePhases == {
    "idle",
    "building",
    "extentsDurable",
    "rootDurable",
    "manifestDurable"
}

EmptyPhysical == [extent \in Extents |-> 0]

VARIABLES
    activeGeneration,
    previousGeneration,
    nextGeneration,
    publishedRoots,
    rootByGeneration,
    physicalByGeneration,
    durableExtentArtifacts,
    durableRootArtifacts,
    durableGenerationManifests,
    candidatePhase,
    candidateGeneration,
    candidateBaseGeneration,
    candidateRoot,
    candidatePhysical,
    readerGeneration,
    rowBindings,
    staleCandidateRejected

vars == <<
    activeGeneration,
    previousGeneration,
    nextGeneration,
    publishedRoots,
    rootByGeneration,
    physicalByGeneration,
    durableExtentArtifacts,
    durableRootArtifacts,
    durableGenerationManifests,
    candidatePhase,
    candidateGeneration,
    candidateBaseGeneration,
    candidateRoot,
    candidatePhysical,
    readerGeneration,
    rowBindings,
    staleCandidateRejected
>>

PinnedReaders == {reader \in Readers : readerGeneration[reader] # -1}
PinnedGenerations == {readerGeneration[reader] : reader \in PinnedReaders}

RetainedGenerations ==
    {0, activeGeneration, previousGeneration}
    \cup PinnedGenerations
    \cup rowBindings

PhysicalClosure(root, physical) ==
    {physical[extent] : extent \in root}

RetainedPhysicalArtifacts ==
    UNION {
        PhysicalClosure(
            rootByGeneration[generation],
            physicalByGeneration[generation]
        ) : generation \in RetainedGenerations
    }

RootReadable(generation) ==
    /\ generation \in publishedRoots
    /\ generation \in durableRootArtifacts
    /\ generation \in durableGenerationManifests
    /\ PhysicalClosure(
            rootByGeneration[generation],
            physicalByGeneration[generation]
       ) \subseteq durableExtentArtifacts

CandidateStale ==
    /\ candidatePhase # "idle"
    /\ candidateBaseGeneration # activeGeneration

Init ==
    /\ activeGeneration = 0
    /\ previousGeneration = 0
    /\ nextGeneration = 1
    /\ publishedRoots = {0}
    /\ rootByGeneration = [generation \in Generations |-> {}]
    /\ physicalByGeneration =
        [generation \in Generations |-> EmptyPhysical]
    /\ durableExtentArtifacts = {0}
    /\ durableRootArtifacts = {0}
    /\ durableGenerationManifests = {0}
    /\ candidatePhase = "idle"
    /\ candidateGeneration = 0
    /\ candidateBaseGeneration = 0
    /\ candidateRoot = {}
    /\ candidatePhysical = EmptyPhysical
    /\ readerGeneration = [reader \in Readers |-> -1]
    /\ rowBindings = {}
    /\ staleCandidateRejected = FALSE

BeginPublication ==
    /\ candidatePhase = "idle"
    /\ nextGeneration <= MaxGeneration
    /\ \E target \in SUBSET Extents:
        /\ candidateRoot' = target
        /\ candidatePhysical' =
            [extent \in Extents |->
                IF extent \in target
                    /\ extent \in rootByGeneration[activeGeneration]
                THEN physicalByGeneration[activeGeneration][extent]
                ELSE IF extent \in target
                     THEN nextGeneration
                     ELSE 0]
    /\ candidatePhase' = "building"
    /\ candidateGeneration' = nextGeneration
    /\ candidateBaseGeneration' = activeGeneration
    /\ nextGeneration' = nextGeneration + 1
    /\ staleCandidateRejected' = FALSE
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        durableGenerationManifests,
        readerGeneration,
        rowBindings
        >>

PersistCandidateExtents ==
    /\ candidatePhase = "building"
    /\ candidatePhase' = "extentsDurable"
    /\ durableExtentArtifacts' =
        durableExtentArtifacts \cup {candidateGeneration}
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableRootArtifacts,
        durableGenerationManifests,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        readerGeneration,
        rowBindings,
        staleCandidateRejected
        >>

PersistCandidateRoot ==
    /\ candidatePhase = "extentsDurable"
    /\ candidatePhase' = "rootDurable"
    /\ durableRootArtifacts' =
        durableRootArtifacts \cup {candidateGeneration}
    /\ rootByGeneration' =
        [rootByGeneration EXCEPT ![candidateGeneration] = candidateRoot]
    /\ physicalByGeneration' =
        [physicalByGeneration EXCEPT
            ![candidateGeneration] = candidatePhysical]
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        durableExtentArtifacts,
        durableGenerationManifests,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        readerGeneration,
        rowBindings,
        staleCandidateRejected
        >>

PersistCandidateManifest ==
    /\ candidatePhase = "rootDurable"
    /\ candidatePhase' = "manifestDurable"
    /\ durableGenerationManifests' =
        durableGenerationManifests \cup {candidateGeneration}
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        readerGeneration,
        rowBindings,
        staleCandidateRejected
        >>

PublishLatestManifest ==
    /\ candidatePhase = "manifestDurable"
    /\ candidateBaseGeneration = activeGeneration
    /\ PhysicalClosure(candidateRoot, candidatePhysical)
        \subseteq durableExtentArtifacts
    /\ previousGeneration' = activeGeneration
    /\ activeGeneration' = candidateGeneration
    /\ publishedRoots' = publishedRoots \cup {candidateGeneration}
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateBaseGeneration' = 0
    /\ candidateRoot' = {}
    /\ candidatePhysical' = EmptyPhysical
    /\ UNCHANGED <<
        nextGeneration,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        durableGenerationManifests,
        readerGeneration,
        rowBindings,
        staleCandidateRejected
        >>

(***************************************************************************)
(* A competing publisher may commit a later fresh generation while the     *)
(* candidate is being built. It copies the selected root and demonstrates   *)
(* that the original candidate's base fence must be checked again.           *)
(***************************************************************************)
CompetingPublication ==
    /\ candidatePhase # "idle"
    /\ nextGeneration <= MaxGeneration
    /\ LET generation == nextGeneration
       IN /\ rootByGeneration' =
                [rootByGeneration EXCEPT
                    ![generation] = rootByGeneration[activeGeneration]]
          /\ physicalByGeneration' =
                [physicalByGeneration EXCEPT
                    ![generation] = physicalByGeneration[activeGeneration]]
          /\ durableExtentArtifacts' =
                durableExtentArtifacts \cup {generation}
          /\ durableRootArtifacts' =
                durableRootArtifacts \cup {generation}
          /\ durableGenerationManifests' =
                durableGenerationManifests \cup {generation}
          /\ publishedRoots' = publishedRoots \cup {generation}
          /\ previousGeneration' = activeGeneration
          /\ activeGeneration' = generation
          /\ nextGeneration' = generation + 1
    /\ UNCHANGED <<
        candidatePhase,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        readerGeneration,
        rowBindings,
        staleCandidateRejected
        >>

RejectStaleCandidate ==
    /\ CandidateStale
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateBaseGeneration' = 0
    /\ candidateRoot' = {}
    /\ candidatePhysical' = EmptyPhysical
    /\ staleCandidateRejected' = TRUE
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        durableGenerationManifests,
        readerGeneration,
        rowBindings
        >>

PinReader(reader) ==
    /\ reader \in Readers
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = activeGeneration]
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        durableGenerationManifests,
        candidatePhase,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        rowBindings,
        staleCandidateRejected
        >>

UnpinReader(reader) ==
    /\ reader \in Readers
    /\ readerGeneration[reader] # -1
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        durableGenerationManifests,
        candidatePhase,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        rowBindings,
        staleCandidateRejected
        >>

PublishRowBinding ==
    /\ activeGeneration # 0
    /\ RootReadable(activeGeneration)
    /\ rowBindings' = {activeGeneration}
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        durableGenerationManifests,
        candidatePhase,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        readerGeneration,
        staleCandidateRejected
        >>

Reclaim ==
    /\ candidatePhase = "idle"
    /\ publishedRoots' = RetainedGenerations
    /\ durableExtentArtifacts' = RetainedPhysicalArtifacts
    /\ durableRootArtifacts' = RetainedGenerations
    /\ durableGenerationManifests' = RetainedGenerations
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        rootByGeneration,
        physicalByGeneration,
        candidatePhase,
        candidateGeneration,
        candidateBaseGeneration,
        candidateRoot,
        candidatePhysical,
        readerGeneration,
        rowBindings,
        staleCandidateRejected
        >>

Crash ==
    /\ candidatePhase # "idle" \/ PinnedReaders # {}
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateBaseGeneration' = 0
    /\ candidateRoot' = {}
    /\ candidatePhysical' = EmptyPhysical
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ UNCHANGED <<
        activeGeneration,
        previousGeneration,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        physicalByGeneration,
        durableExtentArtifacts,
        durableRootArtifacts,
        durableGenerationManifests,
        rowBindings,
        staleCandidateRejected
        >>

Next ==
    \/ BeginPublication
    \/ PersistCandidateExtents
    \/ PersistCandidateRoot
    \/ PersistCandidateManifest
    \/ PublishLatestManifest
    \/ CompetingPublication
    \/ RejectStaleCandidate
    \/ \E reader \in Readers: PinReader(reader)
    \/ \E reader \in Readers: UnpinReader(reader)
    \/ PublishRowBinding
    \/ Reclaim
    \/ Crash

Spec == Init /\ [][Next]_vars /\ WF_vars(RejectStaleCandidate)

TypeOK ==
    /\ activeGeneration \in Generations
    /\ previousGeneration \in Generations
    /\ nextGeneration \in 1..(MaxGeneration + 1)
    /\ publishedRoots \subseteq Generations
    /\ rootByGeneration \in [Generations -> SUBSET Extents]
    /\ physicalByGeneration \in
        [Generations -> [Extents -> Generations]]
    /\ durableExtentArtifacts \subseteq Generations
    /\ durableRootArtifacts \subseteq Generations
    /\ durableGenerationManifests \subseteq Generations
    /\ candidatePhase \in CandidatePhases
    /\ candidateGeneration \in Generations
    /\ candidateBaseGeneration \in Generations
    /\ candidateRoot \subseteq Extents
    /\ candidatePhysical \in [Extents -> Generations]
    /\ readerGeneration \in [Readers -> ReaderGenerations]
    /\ rowBindings \subseteq Generations
    /\ staleCandidateRejected \in BOOLEAN

LatestManifestIsPublishLast == RootReadable(activeGeneration)

PublishedRootsAreComplete ==
    \A generation \in publishedRoots: RootReadable(generation)

PinnedRootsRemainReadable ==
    \A generation \in PinnedGenerations: RootReadable(generation)

CandidateUsesContentAddressedReuse ==
    candidatePhase = "idle"
    \/ \A extent \in candidateRoot
        \cap rootByGeneration[candidateBaseGeneration]:
        candidatePhysical[extent] =
            physicalByGeneration[candidateBaseGeneration][extent]

CandidateWritesNewContentToFreshGeneration ==
    candidatePhase = "idle"
    \/ \A extent \in candidateRoot
        \ rootByGeneration[candidateBaseGeneration]:
        candidatePhysical[extent] = candidateGeneration

DurableCandidateRootHasExtentClosure ==
    candidatePhase \in {"building", "extentsDurable"}
    \/ PhysicalClosure(candidateRoot, candidatePhysical)
        \subseteq durableExtentArtifacts

RowBindingsSelectCompleteOverflowRoots ==
    \A generation \in rowBindings: RootReadable(generation)

ReclamationPreservesRequiredPhysicalClosure ==
    \A generation \in RetainedGenerations:
        PhysicalClosure(
            rootByGeneration[generation],
            physicalByGeneration[generation]
        ) \subseteq durableExtentArtifacts

PublishedGenerationDoesNotRegress == previousGeneration <= activeGeneration

StaleCandidateEventuallyTerminates ==
    CandidateStale ~> candidatePhase = "idle"

=============================================================================
