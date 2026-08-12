-------------------- MODULE SkeinCompactionVisibility --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Layered columnar visibility: the store serves the ordered merge of      *)
(* base groups filtered by generation-scoped deletion vectors, delta       *)
(* groups, and the in-memory memtable. Flush publishes memtable changes    *)
(* as delta rows plus deletion-vector marks in a new generation without    *)
(* rewriting base bytes. Compaction rewrites the representation of a new   *)
(* generation without changing the visible state. Published generations    *)
(* are immutable; readers pin one generation and must observe the same     *)
(* durable view for the lifetime of the pin.                               *)
(*                                                                         *)
(* Compaction is split into prepare and publish. Preparation binds its     *)
(* output to one source generation. Publication is a generation-guarded   *)
(* compare-and-swap: if a flush advanced the current generation, the stale *)
(* output is discarded rather than losing newly published deletes.        *)
(*                                                                         *)
(* Records carry per-key version numbers, and the scan is modeled as the   *)
(* set of emitted versions per key. This makes the deletion vector's real  *)
(* obligation checkable: a flush that adds a superseding delta row but     *)
(* fails to mark the stale base row emits two versions of one key, which   *)
(* a key-presence abstraction cannot distinguish from a correct merge.     *)
(*                                                                         *)
(* The memtable is WAL-backed: a crash drops readers and in-flight         *)
(* publication, never committed memtable state (replay reconstructs it).   *)
(***************************************************************************)

CONSTANT Keys, Readers, MaxVersion, MaxGeneration

ASSUME /\ Keys # {}
       /\ Readers # {}
       /\ MaxVersion \in Nat \ {0}
       /\ MaxGeneration \in Nat \ {0}

Generations == 0..MaxGeneration
ReaderGenerations == (-1)..MaxGeneration
Versions == 0..MaxVersion
MemOps == {"none", "put", "del"}

Group ==
    [base: [Keys -> Versions],
     dv: SUBSET Keys,
     delta: [Keys -> Versions]]

EmptyColumn == [key \in Keys |-> 0]

EmptyGroup == [base |-> EmptyColumn, dv |-> {}, delta |-> EmptyColumn]

VARIABLES
    minted,
    present,
    mem,
    groups,
    available,
    currentGeneration,
    readerGeneration,
    readerView,
    compactionSourceGeneration,
    compactionOutput

vars == <<
    minted,
    present,
    mem,
    groups,
    available,
    currentGeneration,
    readerGeneration,
    readerView,
    compactionSourceGeneration,
    compactionOutput
>>

(***************************************************************************)
(* The versions a scan of generation g emits for one key: the base row     *)
(* unless the deletion vector masks it, plus the delta row if present.     *)
(* A correct layered merge emits at most one version per key.              *)
(***************************************************************************)
EmittedVersionsFromGroup(group, key) ==
    LET baseVersions ==
            IF group.base[key] # 0 /\ key \notin group.dv
            THEN {group.base[key]}
            ELSE {}
        deltaVersions ==
            IF group.delta[key] # 0 THEN {group.delta[key]} ELSE {}
    IN baseVersions \cup deltaVersions

EmittedVersions(generation, key) ==
    EmittedVersionsFromGroup(groups[generation], key)

DurableView(generation) ==
    [key \in Keys |-> EmittedVersions(generation, key)]

(***************************************************************************)
(* A read at the current generation overlays the memtable on the durable   *)
(* layers.                                                                 *)
(***************************************************************************)
ReadVersions(key) ==
    IF mem[key] = "put" THEN {minted[key]}
    ELSE IF mem[key] = "del" THEN {}
    ELSE EmittedVersions(currentGeneration, key)

ExpectedVersions(key) ==
    IF present[key] THEN {minted[key]} ELSE {}

NoReaders == \A reader \in Readers: readerGeneration[reader] = -1

MemIsEmpty == \A key \in Keys: mem[key] = "none"

Init ==
    /\ minted = [key \in Keys |-> 0]
    /\ present = [key \in Keys |-> FALSE]
    /\ mem = [key \in Keys |-> "none"]
    /\ groups = [generation \in Generations |-> EmptyGroup]
    /\ available = {0}
    /\ currentGeneration = 0
    /\ readerGeneration = [reader \in Readers |-> -1]
    /\ readerView = [reader \in Readers |-> [key \in Keys |-> {}]]
    /\ compactionSourceGeneration = -1
    /\ compactionOutput = EmptyGroup

CommitPut(key) ==
    /\ minted[key] < MaxVersion
    /\ minted' = [minted EXCEPT ![key] = minted[key] + 1]
    /\ present' = [present EXCEPT ![key] = TRUE]
    /\ mem' = [mem EXCEPT ![key] = "put"]
    /\ UNCHANGED <<
        groups,
        available,
        currentGeneration,
        readerGeneration,
        readerView,
        compactionSourceGeneration,
        compactionOutput
        >>

CommitDelete(key) ==
    /\ present' = [present EXCEPT ![key] = FALSE]
    /\ mem' = [mem EXCEPT ![key] = "del"]
    /\ UNCHANGED <<
        minted,
        groups,
        available,
        currentGeneration,
        readerGeneration,
        readerView,
        compactionSourceGeneration,
        compactionOutput
        >>

(***************************************************************************)
(* Flush publishes generation N+1 from generation N: every base row        *)
(* superseded by a memtable put or removed by a memtable delete is marked  *)
(* in the deletion vector, memtable puts become delta rows, and base       *)
(* bytes are referenced, never rewritten.                                  *)
(***************************************************************************)
Flush ==
    /\ currentGeneration < MaxGeneration
    /\ ~MemIsEmpty
    /\ LET current == groups[currentGeneration]
           flushed ==
               [base |-> current.base,
                dv |->
                    current.dv
                        \cup {key \in Keys:
                                current.base[key] # 0 /\ mem[key] # "none"},
                delta |->
                    [key \in Keys |->
                        IF mem[key] = "put" THEN minted[key]
                        ELSE IF mem[key] = "del" THEN 0
                        ELSE current.delta[key]]]
       IN groups' =
           [groups EXCEPT ![currentGeneration + 1] = flushed]
    /\ available' = available \cup {currentGeneration + 1}
    /\ currentGeneration' = currentGeneration + 1
    /\ mem' = [key \in Keys |-> "none"]
    /\ UNCHANGED <<
        minted,
        present,
        readerGeneration,
        readerView,
        compactionSourceGeneration,
        compactionOutput
        >>

(***************************************************************************)
(* Preparation materializes a merged representation and records the exact  *)
(* source generation whose row ordinals and deletion vector it consumed.   *)
(***************************************************************************)
PrepareCompaction ==
    /\ compactionSourceGeneration = -1
    /\ currentGeneration < MaxGeneration
    /\ LET current == groups[currentGeneration]
           merged ==
               [key \in Keys |->
                   IF current.delta[key] # 0
                   THEN current.delta[key]
                   ELSE IF key \notin current.dv
                   THEN current.base[key]
                   ELSE 0]
       IN compactionOutput' =
           [base |-> merged, dv |-> {}, delta |-> EmptyColumn]
    /\ compactionSourceGeneration' = currentGeneration
    /\ UNCHANGED <<
        minted,
        present,
        mem,
        groups,
        available,
        currentGeneration,
        readerGeneration,
        readerView
        >>

(***************************************************************************)
(* Publication is a manifest compare-and-swap. If a flush or another       *)
(* publication advanced the source, the prepared output cannot publish.    *)
(***************************************************************************)
PublishCompaction ==
    /\ compactionSourceGeneration = currentGeneration
    /\ currentGeneration < MaxGeneration
    /\ groups' =
        [groups EXCEPT ![currentGeneration + 1] = compactionOutput]
    /\ available' = available \cup {currentGeneration + 1}
    /\ currentGeneration' = currentGeneration + 1
    /\ compactionSourceGeneration' = -1
    /\ compactionOutput' = EmptyGroup
    /\ UNCHANGED <<
        minted,
        present,
        mem,
        readerGeneration,
        readerView
        >>

DiscardStaleCompaction ==
    /\ compactionSourceGeneration # -1
    /\ compactionSourceGeneration # currentGeneration
    /\ compactionSourceGeneration' = -1
    /\ compactionOutput' = EmptyGroup
    /\ UNCHANGED <<
        minted,
        present,
        mem,
        groups,
        available,
        currentGeneration,
        readerGeneration,
        readerView
        >>

BeginRead(reader) ==
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = currentGeneration]
    /\ readerView' =
        [readerView EXCEPT ![reader] = DurableView(currentGeneration)]
    /\ UNCHANGED <<
        minted,
        present,
        mem,
        groups,
        available,
        currentGeneration,
        compactionSourceGeneration,
        compactionOutput
        >>

EndRead(reader) ==
    /\ readerGeneration[reader] >= 0
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ readerView' =
        [readerView EXCEPT ![reader] = [key \in Keys |-> {}]]
    /\ UNCHANGED <<
        minted,
        present,
        mem,
        groups,
        available,
        currentGeneration,
        compactionSourceGeneration,
        compactionOutput
        >>

(***************************************************************************)
(* Coarse reclamation, matching SkeinGenerationReclamation: while any      *)
(* reader exists no generation is removed; without readers the current     *)
(* and immediately previous generations remain available.                  *)
(***************************************************************************)
Reclaim ==
    /\ NoReaders
    /\ available' =
        {generation \in available:
            generation >= currentGeneration - 1}
    /\ UNCHANGED <<
        minted,
        present,
        mem,
        groups,
        currentGeneration,
        readerGeneration,
        readerView,
        compactionSourceGeneration,
        compactionOutput
        >>

(***************************************************************************)
(* A crash drops readers. Published generations are durable, and the       *)
(* memtable is reconstructed by WAL replay, so committed state survives.   *)
(***************************************************************************)
CrashAndRecover ==
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ readerView' = [reader \in Readers |-> [key \in Keys |-> {}]]
    /\ compactionSourceGeneration' = -1
    /\ compactionOutput' = EmptyGroup
    /\ UNCHANGED <<
        minted,
        present,
        mem,
        groups,
        available,
        currentGeneration
        >>

Next ==
    \/ \E key \in Keys: CommitPut(key)
    \/ \E key \in Keys: CommitDelete(key)
    \/ Flush
    \/ PrepareCompaction
    \/ PublishCompaction
    \/ DiscardStaleCompaction
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Reclaim
    \/ CrashAndRecover

TypeOK ==
    /\ minted \in [Keys -> Versions]
    /\ present \in [Keys -> BOOLEAN]
    /\ mem \in [Keys -> MemOps]
    /\ groups \in [Generations -> Group]
    /\ available \subseteq Generations
    /\ currentGeneration \in Generations
    /\ readerGeneration \in [Readers -> ReaderGenerations]
    /\ readerView \in [Readers -> [Keys -> SUBSET Versions]]
    /\ compactionSourceGeneration \in ReaderGenerations
    /\ compactionOutput \in Group

(***************************************************************************)
(* The layered read at the current generation overlaid with the memtable   *)
(* emits exactly the committed logical state: one current version for a    *)
(* live key, nothing for a deleted key, and never a stale duplicate.       *)
(* Flush and compaction preserving this with unchanged logical state is    *)
(* the compaction-identity obligation.                                     *)
(***************************************************************************)
LayeredReadEqualsLogicalState ==
    \A key \in Keys: ReadVersions(key) = ExpectedVersions(key)

(***************************************************************************)
(* Published generations are immutable: a pinned reader observes the       *)
(* durable view recorded at pin time for the lifetime of the pin.          *)
(***************************************************************************)
PinnedViewIsImmutable ==
    \A reader \in Readers:
        readerGeneration[reader] = -1 \/
            DurableView(readerGeneration[reader]) = readerView[reader]

PinnedGenerationsRemainAvailable ==
    \A reader \in Readers:
        readerGeneration[reader] = -1 \/
            readerGeneration[reader] \in available

CurrentGenerationAvailable ==
    currentGeneration \in available

(***************************************************************************)
(* Structural obligations of the layered representation: a scan emits at   *)
(* most one version per key, and deletion vectors only ever mark rows      *)
(* that exist in the base column.                                          *)
(***************************************************************************)
ScanNeverEmitsDuplicates ==
    \A generation \in available:
        \A key \in Keys:
            Cardinality(EmittedVersions(generation, key)) <= 1

DeletionVectorsCoverBaseOnly ==
    \A generation \in available:
        \A key \in groups[generation].dv:
            groups[generation].base[key] # 0

(***************************************************************************)
(* The prepared output is an identity transform of the exact source        *)
(* generation it names. Publication can succeed only while that source is  *)
(* still current, so a stale output cannot erase a newer deletion vector.  *)
(***************************************************************************)
PreparedCompactionIsIdentity ==
    compactionSourceGeneration = -1 \/
        \A key \in Keys:
            EmittedVersionsFromGroup(compactionOutput, key) =
                EmittedVersions(compactionSourceGeneration, key)

CompactionSourceNeverLeadsCurrent ==
    compactionSourceGeneration = -1 \/
        compactionSourceGeneration <= currentGeneration

Spec == Init /\ [][Next]_vars

=============================================================================
