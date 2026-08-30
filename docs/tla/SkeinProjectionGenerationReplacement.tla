-------------- MODULE SkeinProjectionGenerationReplacement --------------
EXTENDS Integers, Naturals

(***************************************************************************)
(* One projection owner selects exactly one immutable generation. A        *)
(* candidate can become visible only after its complete artifact is sealed *)
(* and only if the expected head still matches. Readers pin the selected   *)
(* generation, while incremental reclamation may remove only inactive and  *)
(* unpinned artifacts. A crash discards writer state without changing the  *)
(* durable selected head.                                                   *)
(***************************************************************************)

CONSTANTS Readers, MaxGeneration

ASSUME /\ Readers # {}
       /\ MaxGeneration \in Nat \ {0}

Generations == 0..MaxGeneration
OptionalGenerations == (-1)..MaxGeneration
WriterPhases == {"idle", "staging", "sealed"}

VARIABLES
    activeGeneration,
    generationArtifacts,
    sealedGenerations,
    writerPhase,
    candidateGeneration,
    expectedHead,
    readerGeneration

vars == <<
    activeGeneration,
    generationArtifacts,
    sealedGenerations,
    writerPhase,
    candidateGeneration,
    expectedHead,
    readerGeneration
>>

Init ==
    /\ activeGeneration = 0
    /\ generationArtifacts = {0}
    /\ sealedGenerations = {0}
    /\ writerPhase = "idle"
    /\ candidateGeneration = 0
    /\ expectedHead = 0
    /\ readerGeneration = [reader \in Readers |-> -1]

BeginCandidate(generation) ==
    /\ writerPhase = "idle"
    /\ generation \in (activeGeneration + 1)..MaxGeneration
    /\ generation \notin generationArtifacts
    /\ writerPhase' = "staging"
    /\ candidateGeneration' = generation
    /\ expectedHead' = activeGeneration
    /\ generationArtifacts' = generationArtifacts \cup {generation}
    /\ UNCHANGED <<
        activeGeneration,
        sealedGenerations,
        readerGeneration
        >>

SealCandidate ==
    /\ writerPhase = "staging"
    /\ candidateGeneration \in generationArtifacts
    /\ sealedGenerations' = sealedGenerations \cup {candidateGeneration}
    /\ writerPhase' = "sealed"
    /\ UNCHANGED <<
        activeGeneration,
        generationArtifacts,
        candidateGeneration,
        expectedHead,
        readerGeneration
        >>

PublishCandidate ==
    /\ writerPhase = "sealed"
    /\ candidateGeneration \in sealedGenerations
    /\ expectedHead = activeGeneration
    /\ candidateGeneration > activeGeneration
    /\ activeGeneration' = candidateGeneration
    /\ writerPhase' = "idle"
    /\ candidateGeneration' = 0
    /\ expectedHead' = activeGeneration'
    /\ UNCHANGED <<
        generationArtifacts,
        sealedGenerations,
        readerGeneration
        >>

AbandonCandidate ==
    /\ writerPhase \in {"staging", "sealed"}
    /\ writerPhase' = "idle"
    /\ candidateGeneration' = 0
    /\ expectedHead' = activeGeneration
    /\ UNCHANGED <<
        activeGeneration,
        generationArtifacts,
        sealedGenerations,
        readerGeneration
        >>

BeginRead(reader) ==
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = activeGeneration]
    /\ UNCHANGED <<
        activeGeneration,
        generationArtifacts,
        sealedGenerations,
        writerPhase,
        candidateGeneration,
        expectedHead
        >>

EndRead(reader) ==
    /\ readerGeneration[reader] >= 0
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ UNCHANGED <<
        activeGeneration,
        generationArtifacts,
        sealedGenerations,
        writerPhase,
        candidateGeneration,
        expectedHead
        >>

ReclaimGeneration(generation) ==
    /\ generation \in generationArtifacts
    /\ generation # activeGeneration
    /\ generation # candidateGeneration
    /\ \A reader \in Readers: readerGeneration[reader] # generation
    /\ generationArtifacts' = generationArtifacts \ {generation}
    /\ sealedGenerations' = sealedGenerations \ {generation}
    /\ UNCHANGED <<
        activeGeneration,
        writerPhase,
        candidateGeneration,
        expectedHead,
        readerGeneration
        >>

CrashAndRecover ==
    /\ writerPhase' = "idle"
    /\ candidateGeneration' = 0
    /\ expectedHead' = activeGeneration
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ UNCHANGED <<
        activeGeneration,
        generationArtifacts,
        sealedGenerations
        >>

Next ==
    \/ \E generation \in Generations: BeginCandidate(generation)
    \/ SealCandidate
    \/ PublishCandidate
    \/ AbandonCandidate
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ \E generation \in Generations: ReclaimGeneration(generation)
    \/ CrashAndRecover

TypeOK ==
    /\ activeGeneration \in Generations
    /\ generationArtifacts \subseteq Generations
    /\ sealedGenerations \subseteq Generations
    /\ sealedGenerations \subseteq generationArtifacts
    /\ writerPhase \in WriterPhases
    /\ candidateGeneration \in Generations
    /\ expectedHead \in Generations
    /\ readerGeneration \in [Readers -> OptionalGenerations]

ActiveGenerationIsComplete ==
    activeGeneration \in sealedGenerations

PinnedReadersRemainReadable ==
    \A reader \in Readers:
        readerGeneration[reader] = -1 \/
            readerGeneration[reader] \in generationArtifacts

CandidateIsInvisible ==
    writerPhase \in {"staging", "sealed"} =>
        activeGeneration # candidateGeneration

SealedWriterHasCompleteArtifact ==
    writerPhase = "sealed" =>
        candidateGeneration \in sealedGenerations

Spec == Init /\ [][Next]_vars

=============================================================================
