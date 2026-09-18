-------------------- MODULE HawDBGenerationReclamation --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Readers pin immutable generations. Reclamation is deliberately coarse:  *)
(* while any reader exists, no generation is removed. Without readers, the  *)
(* current and immediately previous generations remain available.           *)
(***************************************************************************)

CONSTANT Readers, MaxGeneration

ASSUME /\ Readers # {}
       /\ MaxGeneration \in Nat

Generations == 0..MaxGeneration
ReaderGenerations == (-1)..MaxGeneration
WriterPhases == {"idle", "building", "durable"}

VARIABLES
    currentGeneration,
    availableGenerations,
    readerGeneration,
    writerPhase,
    targetGeneration

vars == <<
    currentGeneration,
    availableGenerations,
    readerGeneration,
    writerPhase,
    targetGeneration
>>

NoReaders == \A reader \in Readers: readerGeneration[reader] = -1

Init ==
    /\ currentGeneration = 0
    /\ availableGenerations = {0}
    /\ readerGeneration = [reader \in Readers |-> -1]
    /\ writerPhase = "idle"
    /\ targetGeneration = 0

BeginRead(reader) ==
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = currentGeneration]
    /\ UNCHANGED <<
        currentGeneration,
        availableGenerations,
        writerPhase,
        targetGeneration
        >>

EndRead(reader) ==
    /\ readerGeneration[reader] >= 0
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ UNCHANGED <<
        currentGeneration,
        availableGenerations,
        writerPhase,
        targetGeneration
        >>

BeginCheckpoint ==
    /\ writerPhase = "idle"
    /\ currentGeneration < MaxGeneration
    /\ writerPhase' = "building"
    /\ targetGeneration' = currentGeneration + 1
    /\ UNCHANGED <<
        currentGeneration,
        availableGenerations,
        readerGeneration
        >>

PersistGeneration ==
    /\ writerPhase = "building"
    /\ availableGenerations' =
        availableGenerations \cup {targetGeneration}
    /\ writerPhase' = "durable"
    /\ UNCHANGED <<
        currentGeneration,
        readerGeneration,
        targetGeneration
        >>

PublishGeneration ==
    /\ writerPhase = "durable"
    /\ targetGeneration \in availableGenerations
    /\ currentGeneration' = targetGeneration
    /\ writerPhase' = "idle"
    /\ targetGeneration' = 0
    /\ UNCHANGED <<availableGenerations, readerGeneration>>

Reclaim ==
    /\ writerPhase = "idle"
    /\ NoReaders
    /\ availableGenerations' =
        {generation \in availableGenerations:
            generation >= currentGeneration - 1}
    /\ UNCHANGED <<
        currentGeneration,
        readerGeneration,
        writerPhase,
        targetGeneration
        >>

Crash ==
    /\ writerPhase' = "idle"
    /\ targetGeneration' = 0
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ UNCHANGED <<currentGeneration, availableGenerations>>

Next ==
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ BeginCheckpoint
    \/ PersistGeneration
    \/ PublishGeneration
    \/ Reclaim
    \/ Crash

TypeOK ==
    /\ currentGeneration \in Generations
    /\ availableGenerations \subseteq Generations
    /\ readerGeneration \in [Readers -> ReaderGenerations]
    /\ writerPhase \in WriterPhases
    /\ targetGeneration \in Generations

ManifestGenerationAvailable ==
    currentGeneration \in availableGenerations

PinnedReadersRemainAvailable ==
    \A reader \in Readers:
        readerGeneration[reader] = -1 \/
            readerGeneration[reader] \in availableGenerations

PreviousGenerationRetained ==
    currentGeneration = 0 \/
        currentGeneration - 1 \in availableGenerations

PublishedOnlyAfterDurable ==
    writerPhase = "durable" =>
        targetGeneration \in availableGenerations

Spec == Init /\ [][Next]_vars

=============================================================================
