------------------------- MODULE HawDBRowDeltaRuns -------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A relational row root is extended by immutable, bounded delta runs.     *)
(* Replay coalesces primary-key changes in a finite dirty map and flushes  *)
(* complete ordered run sets before publishing an immutable generation     *)
(* manifest. The abstract row-root fence includes generation, epoch, schema *)
(* digest, column count, and exact final row count. The latest manifest is  *)
(* replaced only after that fence and the previous-delta fence are          *)
(* revalidated. Failed or poisoned candidates remain unreachable, while     *)
(* readers retain their selected generation.                               *)
(***************************************************************************)

CONSTANT MaxEpoch, DirtyBudget, MaxDeltaGeneration, MaxBaseGeneration

ASSUME /\ MaxEpoch \in Nat \ {0, 1}
       /\ DirtyBudget \in Nat \ {0}
       /\ MaxDeltaGeneration \in Nat \ {0, 1}
       /\ MaxBaseGeneration \in Nat \ {0, 1}

Keys == 1..2
BaseGeneration == 1
BaseEpoch == 1
BaseState == {1}
Phases == {"idle", "replaying", "runsDurable", "manifestDurable", "unavailable"}

ApplyChange(state, change) ==
    IF change.present
    THEN state \cup {change.key}
    ELSE state \ {change.key}

RECURSIVE ApplyPrefix(_, _, _)
ApplyPrefix(state, log, count) ==
    IF count = 0
    THEN state
    ELSE ApplyChange(ApplyPrefix(state, log, count - 1), log[count])

ApplyOverlay(state, keys, values) ==
    {key \in Keys : IF key \in keys THEN values[key] ELSE key \in state}

ApplyRun(state, run) == ApplyOverlay(state, run.keys, run.values)

RECURSIVE ApplyRuns(_, _)
ApplyRuns(state, runs) ==
    IF Len(runs) = 0
    THEN state
    ELSE ApplyRuns(ApplyRun(state, Head(runs)), Tail(runs))

StateAtEpoch(log, epoch) == ApplyPrefix(BaseState, log, epoch - BaseEpoch)

VARIABLES
    canonicalState,
    commitEpoch,
    wal,
    rowBaseGeneration,
    phase,
    nextDeltaGeneration,
    candidateGeneration,
    candidateBaseGeneration,
    candidateBaseEpoch,
    candidateTargetEpoch,
    candidateWal,
    candidateExpectedPrevious,
    replayCursor,
    candidateState,
    candidateRowCount,
    dirtyKeys,
    dirtyValues,
    candidateRuns,
    durableRunSets,
    durableManifests,
    candidateNeedsOverflow,
    overflowRootReady,
    candidateOverflowClosed,
    poisoned,
    selectedGeneration,
    selectedBaseGeneration,
    selectedEpoch,
    selectedWal,
    selectedState,
    selectedRowCount,
    readerPinned,
    readerGeneration,
    readerEpoch,
    readerWal,
    readerState,
    readerRowCount,
    sqlUsesDelta

vars == <<
    canonicalState,
    commitEpoch,
    wal,
    rowBaseGeneration,
    phase,
    nextDeltaGeneration,
    candidateGeneration,
    candidateBaseGeneration,
    candidateBaseEpoch,
    candidateTargetEpoch,
    candidateWal,
    candidateExpectedPrevious,
    replayCursor,
    candidateState,
    candidateRowCount,
    dirtyKeys,
    dirtyValues,
    candidateRuns,
    durableRunSets,
    durableManifests,
    candidateNeedsOverflow,
    overflowRootReady,
    candidateOverflowClosed,
    poisoned,
    selectedGeneration,
    selectedBaseGeneration,
    selectedEpoch,
    selectedWal,
    selectedState,
    selectedRowCount,
    readerPinned,
    readerGeneration,
    readerEpoch,
    readerWal,
    readerState,
    readerRowCount,
    sqlUsesDelta
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = BaseEpoch
    /\ wal = <<>>
    /\ rowBaseGeneration = BaseGeneration
    /\ phase = "idle"
    /\ nextDeltaGeneration = 1
    /\ candidateGeneration = 0
    /\ candidateBaseGeneration = 0
    /\ candidateBaseEpoch = 0
    /\ candidateTargetEpoch = 0
    /\ candidateWal = <<>>
    /\ candidateExpectedPrevious = 0
    /\ replayCursor = 1
    /\ candidateState = BaseState
    /\ candidateRowCount = Cardinality(BaseState)
    /\ dirtyKeys = {}
    /\ dirtyValues = [key \in Keys |-> FALSE]
    /\ candidateRuns = <<>>
    /\ durableRunSets = {}
    /\ durableManifests = {}
    /\ candidateNeedsOverflow = FALSE
    /\ overflowRootReady = FALSE
    /\ candidateOverflowClosed = FALSE
    /\ poisoned = FALSE
    /\ selectedGeneration = 0
    /\ selectedBaseGeneration = BaseGeneration
    /\ selectedEpoch = BaseEpoch
    /\ selectedWal = <<>>
    /\ selectedState = BaseState
    /\ selectedRowCount = Cardinality(BaseState)
    /\ readerPinned = FALSE
    /\ readerGeneration = 0
    /\ readerEpoch = BaseEpoch
    /\ readerWal = <<>>
    /\ readerState = BaseState
    /\ readerRowCount = Cardinality(BaseState)
    /\ sqlUsesDelta = FALSE

Commit(present) ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ present \in BOOLEAN
    /\ \E key \in Keys:
        LET change == [key |-> key, present |-> present]
        IN
            /\ canonicalState' = ApplyChange(canonicalState, change)
            /\ wal' = Append(wal, change)
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        rowBaseGeneration, phase, nextDeltaGeneration,
        candidateGeneration, candidateBaseGeneration, candidateBaseEpoch,
        candidateTargetEpoch, candidateWal, candidateExpectedPrevious, replayCursor,
        candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, candidateOverflowClosed, poisoned,
        selectedGeneration, selectedBaseGeneration, selectedEpoch, selectedWal,
        selectedState, selectedRowCount, readerPinned, readerGeneration, readerEpoch, readerWal,
        readerState, readerRowCount, sqlUsesDelta
        >>

BeginGeneration(needsOverflow) ==
    /\ phase = "idle"
    /\ rowBaseGeneration = BaseGeneration
    /\ nextDeltaGeneration <= MaxDeltaGeneration
    /\ needsOverflow \in BOOLEAN
    /\ phase' = "replaying"
    /\ nextDeltaGeneration' = nextDeltaGeneration + 1
    /\ candidateGeneration' = nextDeltaGeneration
    /\ candidateBaseGeneration' = rowBaseGeneration
    /\ candidateBaseEpoch' = BaseEpoch
    /\ candidateTargetEpoch' = commitEpoch
    /\ candidateWal' = <<>>
    /\ candidateExpectedPrevious' = selectedGeneration
    /\ replayCursor' = 1
    /\ candidateState' = BaseState
    /\ candidateRowCount' = Cardinality(BaseState)
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ candidateRuns' = <<>>
    /\ candidateNeedsOverflow' = needsOverflow
    /\ candidateOverflowClosed' = FALSE
    /\ poisoned' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        durableRunSets, durableManifests, overflowRootReady,
        selectedGeneration, selectedBaseGeneration, selectedEpoch, selectedWal,
        selectedState, selectedRowCount, readerPinned, readerGeneration, readerEpoch, readerWal,
        readerState, readerRowCount, sqlUsesDelta
        >>

ReplayNext ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ LET change == wal[replayCursor] IN
        /\ \/ change.key \in dirtyKeys
           \/ Cardinality(dirtyKeys) < DirtyBudget
        /\ candidateState' = ApplyChange(candidateState, change)
        /\ candidateRowCount' = Cardinality(ApplyChange(candidateState, change))
        /\ dirtyKeys' = dirtyKeys \cup {change.key}
        /\ dirtyValues' = [dirtyValues EXCEPT ![change.key] = change.present]
        /\ candidateWal' = Append(candidateWal, change)
    /\ replayCursor' = replayCursor + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration, phase,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateExpectedPrevious,
        candidateRuns, durableRunSets, durableManifests,
        candidateNeedsOverflow, overflowRootReady, candidateOverflowClosed,
        poisoned, selectedGeneration, selectedBaseGeneration, selectedEpoch, selectedWal,
        selectedState, selectedRowCount, readerPinned, readerGeneration, readerEpoch, readerWal,
        readerState, readerRowCount, sqlUsesDelta
        >>

FlushDirty ==
    /\ phase = "replaying"
    /\ dirtyKeys # {}
    /\ candidateRuns' = Append(
        candidateRuns,
        [keys |-> dirtyKeys, values |-> dirtyValues]
        )
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration, phase,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, durableRunSets, durableManifests,
        candidateNeedsOverflow, overflowRootReady, candidateOverflowClosed,
        poisoned, selectedGeneration, selectedBaseGeneration, selectedEpoch, selectedWal,
        selectedState, selectedRowCount, readerPinned, readerGeneration, readerEpoch, readerWal,
        readerState, readerRowCount, sqlUsesDelta
        >>

FinishReplay ==
    /\ phase = "replaying"
    /\ replayCursor > Len(wal)
    /\ dirtyKeys = {}
    /\ candidateState = canonicalState
    /\ candidateRowCount = Cardinality(canonicalState)
    /\ candidateTargetEpoch = commitEpoch
    /\ candidateWal = wal
    /\ phase' = "runsDurable"
    /\ durableRunSets' = durableRunSets \cup {candidateGeneration}
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableManifests, candidateNeedsOverflow, overflowRootReady,
        candidateOverflowClosed, poisoned, selectedGeneration,
        selectedBaseGeneration, selectedEpoch, selectedWal, selectedState, selectedRowCount, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

PrepareOverflowRoot ==
    /\ ~overflowRootReady
    /\ overflowRootReady' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration, phase,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        candidateOverflowClosed, poisoned, selectedGeneration,
        selectedBaseGeneration, selectedEpoch, selectedWal, selectedState, selectedRowCount, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

ValidateOverflowClosure ==
    /\ phase = "runsDurable"
    /\ ~candidateOverflowClosed
    /\ \/ ~candidateNeedsOverflow
       \/ overflowRootReady
    /\ candidateOverflowClosed' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration, phase,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, poisoned, selectedGeneration,
        selectedBaseGeneration, selectedEpoch, selectedWal, selectedState, selectedRowCount, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

RejectMissingOverflow ==
    /\ phase = "runsDurable"
    /\ candidateNeedsOverflow
    /\ ~overflowRootReady
    /\ phase' = "unavailable"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, candidateOverflowClosed, selectedGeneration,
        selectedBaseGeneration, selectedEpoch, selectedWal, selectedState, selectedRowCount, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

PersistGenerationManifest ==
    /\ phase = "runsDurable"
    /\ candidateOverflowClosed
    /\ candidateGeneration \in durableRunSets
    /\ phase' = "manifestDurable"
    /\ durableManifests' = durableManifests \cup {candidateGeneration}
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, candidateNeedsOverflow, overflowRootReady,
        candidateOverflowClosed, poisoned, selectedGeneration,
        selectedBaseGeneration, selectedEpoch, selectedWal, selectedState, selectedRowCount, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

PublishLatestManifest ==
    /\ phase = "manifestDurable"
    /\ ~poisoned
    /\ candidateGeneration \in durableManifests
    /\ candidateBaseGeneration = rowBaseGeneration
    /\ candidateExpectedPrevious = selectedGeneration
    /\ candidateTargetEpoch = commitEpoch
    /\ candidateState = canonicalState
    /\ phase' = "idle"
    /\ selectedGeneration' = candidateGeneration
    /\ selectedBaseGeneration' = candidateBaseGeneration
    /\ selectedEpoch' = candidateTargetEpoch
    /\ selectedWal' = candidateWal
    /\ selectedState' = candidateState
    /\ selectedRowCount' = candidateRowCount
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, candidateOverflowClosed, poisoned, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

PublishCompetitor ==
    /\ phase = "manifestDurable"
    /\ candidateGeneration < MaxDeltaGeneration
    /\ selectedGeneration = candidateExpectedPrevious
    /\ selectedGeneration' = candidateGeneration + 1
    /\ selectedBaseGeneration' = rowBaseGeneration
    /\ selectedEpoch' = commitEpoch
    /\ selectedWal' = wal
    /\ selectedState' = canonicalState
    /\ selectedRowCount' = Cardinality(canonicalState)
    /\ durableRunSets' = durableRunSets \cup {candidateGeneration + 1}
    /\ durableManifests' = durableManifests \cup {candidateGeneration + 1}
    /\ nextDeltaGeneration' = candidateGeneration + 2
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration, phase,
        candidateGeneration, candidateBaseGeneration, candidateBaseEpoch,
        candidateTargetEpoch, candidateWal, candidateExpectedPrevious, replayCursor,
        candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        candidateNeedsOverflow, overflowRootReady, candidateOverflowClosed,
        poisoned, readerPinned, readerGeneration, readerEpoch, readerWal, readerState,
        readerRowCount, sqlUsesDelta
        >>

AdvanceRowBase ==
    /\ phase = "manifestDurable"
    /\ rowBaseGeneration < MaxBaseGeneration
    /\ rowBaseGeneration' = rowBaseGeneration + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, nextDeltaGeneration,
        candidateGeneration, candidateBaseGeneration, candidateBaseEpoch,
        candidateTargetEpoch, candidateWal, candidateExpectedPrevious, replayCursor,
        candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, candidateOverflowClosed, poisoned,
        selectedGeneration, selectedBaseGeneration, selectedEpoch, selectedWal,
        selectedState, selectedRowCount, readerPinned, readerGeneration, readerEpoch, readerWal,
        readerState, readerRowCount, sqlUsesDelta
        >>

RejectStaleCandidate ==
    /\ phase = "manifestDurable"
    /\ \/ candidateBaseGeneration # rowBaseGeneration
       \/ candidateExpectedPrevious # selectedGeneration
    /\ phase' = "unavailable"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, candidateOverflowClosed, selectedGeneration,
        selectedBaseGeneration, selectedEpoch, selectedWal, selectedState, selectedRowCount, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

RejectPartialBatch ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ phase' = "unavailable"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, candidateOverflowClosed, selectedGeneration,
        selectedBaseGeneration, selectedEpoch, selectedWal, selectedState, selectedRowCount, readerPinned,
        readerGeneration, readerEpoch, readerWal, readerState, readerRowCount, sqlUsesDelta
        >>

PinReader ==
    /\ ~readerPinned
    /\ selectedGeneration # 0
    /\ readerPinned' = TRUE
    /\ readerGeneration' = selectedGeneration
    /\ readerEpoch' = selectedEpoch
    /\ readerWal' = selectedWal
    /\ readerState' = selectedState
    /\ readerRowCount' = selectedRowCount
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration, phase,
        nextDeltaGeneration, candidateGeneration, candidateBaseGeneration,
        candidateBaseEpoch, candidateTargetEpoch, candidateWal, candidateExpectedPrevious,
        replayCursor, candidateState, candidateRowCount, dirtyKeys, dirtyValues, candidateRuns,
        durableRunSets, durableManifests, candidateNeedsOverflow,
        overflowRootReady, candidateOverflowClosed, poisoned,
        selectedGeneration, selectedBaseGeneration, selectedEpoch, selectedWal,
        selectedState, selectedRowCount, sqlUsesDelta
        >>

Crash ==
    /\ phase # "idle"
    /\ phase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateBaseGeneration' = 0
    /\ candidateBaseEpoch' = 0
    /\ candidateTargetEpoch' = 0
    /\ candidateWal' = <<>>
    /\ candidateExpectedPrevious' = selectedGeneration
    /\ replayCursor' = 1
    /\ candidateState' = BaseState
    /\ candidateRowCount' = Cardinality(BaseState)
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ candidateRuns' = <<>>
    /\ candidateNeedsOverflow' = FALSE
    /\ candidateOverflowClosed' = FALSE
    /\ poisoned' = FALSE
    /\ readerPinned' = FALSE
    /\ readerGeneration' = 0
    /\ readerEpoch' = BaseEpoch
    /\ readerWal' = <<>>
    /\ readerState' = BaseState
    /\ readerRowCount' = Cardinality(BaseState)
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, rowBaseGeneration,
        nextDeltaGeneration, durableRunSets, durableManifests,
        overflowRootReady, selectedGeneration, selectedBaseGeneration,
        selectedEpoch, selectedWal, selectedState, selectedRowCount, sqlUsesDelta
        >>

Next ==
    \/ Commit(TRUE)
    \/ Commit(FALSE)
    \/ BeginGeneration(TRUE)
    \/ BeginGeneration(FALSE)
    \/ ReplayNext
    \/ FlushDirty
    \/ FinishReplay
    \/ PrepareOverflowRoot
    \/ ValidateOverflowClosure
    \/ RejectMissingOverflow
    \/ PersistGenerationManifest
    \/ PublishLatestManifest
    \/ PublishCompetitor
    \/ AdvanceRowBase
    \/ RejectStaleCandidate
    \/ RejectPartialBatch
    \/ PinReader
    \/ Crash

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in BaseEpoch..MaxEpoch
    /\ wal \in Seq([key: Keys, present: BOOLEAN])
    /\ rowBaseGeneration \in BaseGeneration..MaxBaseGeneration
    /\ phase \in Phases
    /\ nextDeltaGeneration \in Nat
    /\ candidateGeneration \in 0..MaxDeltaGeneration
    /\ candidateBaseGeneration \in 0..MaxBaseGeneration
    /\ candidateBaseEpoch \in 0..MaxEpoch
    /\ candidateTargetEpoch \in 0..MaxEpoch
    /\ candidateWal \in Seq([key: Keys, present: BOOLEAN])
    /\ candidateExpectedPrevious \in 0..MaxDeltaGeneration
    /\ replayCursor \in 1..(Len(wal) + 1)
    /\ candidateState \subseteq Keys
    /\ candidateRowCount \in 0..Cardinality(Keys)
    /\ dirtyKeys \subseteq Keys
    /\ dirtyValues \in [Keys -> BOOLEAN]
    /\ candidateRuns \in Seq([keys: SUBSET Keys, values: [Keys -> BOOLEAN]])
    /\ durableRunSets \subseteq 1..MaxDeltaGeneration
    /\ durableManifests \subseteq 1..MaxDeltaGeneration
    /\ candidateNeedsOverflow \in BOOLEAN
    /\ overflowRootReady \in BOOLEAN
    /\ candidateOverflowClosed \in BOOLEAN
    /\ poisoned \in BOOLEAN
    /\ selectedGeneration \in 0..MaxDeltaGeneration
    /\ selectedBaseGeneration \in BaseGeneration..MaxBaseGeneration
    /\ selectedEpoch \in BaseEpoch..MaxEpoch
    /\ selectedWal \in Seq([key: Keys, present: BOOLEAN])
    /\ selectedState \subseteq Keys
    /\ selectedRowCount \in 0..Cardinality(Keys)
    /\ readerPinned \in BOOLEAN
    /\ readerGeneration \in 0..MaxDeltaGeneration
    /\ readerEpoch \in BaseEpoch..MaxEpoch
    /\ readerWal \in Seq([key: Keys, present: BOOLEAN])
    /\ readerState \subseteq Keys
    /\ readerRowCount \in 0..Cardinality(Keys)
    /\ sqlUsesDelta \in BOOLEAN

CanonicalEqualsWal ==
    /\ Len(wal) = commitEpoch - BaseEpoch
    /\ canonicalState = ApplyPrefix(BaseState, wal, Len(wal))

DirtyOverlayIsBounded == Cardinality(dirtyKeys) <= DirtyBudget

CandidateBindsImmutableBaseEpoch ==
    candidateGeneration # 0 => candidateBaseEpoch = BaseEpoch

ReplayPrefixEquivalent ==
    phase = "replaying" =>
        /\ candidateState = ApplyPrefix(BaseState, wal, replayCursor - 1)
        /\ candidateWal = SubSeq(wal, 1, replayCursor - 1)
        /\ candidateState = ApplyOverlay(
            ApplyRuns(BaseState, candidateRuns),
            dirtyKeys,
            dirtyValues
            )

ManifestPublicationIsLast ==
    /\ durableManifests \subseteq durableRunSets
    /\ selectedGeneration # 0 =>
        /\ selectedGeneration \in durableRunSets
        /\ selectedGeneration \in durableManifests

SelectedGenerationIsComplete ==
    selectedGeneration # 0 =>
        /\ selectedEpoch <= commitEpoch
        /\ selectedWal = SubSeq(wal, 1, selectedEpoch - BaseEpoch)
        /\ selectedState = StateAtEpoch(wal, selectedEpoch)
        /\ selectedRowCount = Cardinality(selectedState)

OnlyClosedOverflowPublishes ==
    selectedGeneration = candidateGeneration /\ candidateGeneration # 0 =>
        \/ ~candidateNeedsOverflow
        \/ overflowRootReady

PoisonedCandidateIsUnreachable ==
    poisoned => candidateGeneration # selectedGeneration

StaleCandidateIsUnreachable ==
    phase = "manifestDurable" /\
        (candidateBaseGeneration # rowBaseGeneration \/
         candidateExpectedPrevious # selectedGeneration)
    => candidateGeneration # selectedGeneration

PinnedReaderDoesNotDrift ==
    readerPinned =>
        /\ readerGeneration \in durableManifests
        /\ readerWal = SubSeq(wal, 1, readerEpoch - BaseEpoch)
        /\ readerState = StateAtEpoch(wal, readerEpoch)
        /\ readerRowCount = Cardinality(readerState)

ProductionSqlRemainsOnCanonicalPath == ~sqlUsesDelta

=============================================================================
