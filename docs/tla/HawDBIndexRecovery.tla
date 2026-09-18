------------------------- MODULE HawDBIndexRecovery -------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A checkpoint-selected immutable index root is recovered by replaying WAL *)
(* into a bounded dirty overlay. Full overlays become immutable candidate    *)
(* delta pages. Candidate generations are never allowed to replace files     *)
(* selected by an older manifest, and become visible only after replay is    *)
(* complete and the new manifest is published. A published base/recovery     *)
(* view then advances through bounded immutable live batches. Graph-only     *)
(* commits advance its epoch without adding entries; capture invalidation    *)
(* removes the current view instead of serving stale postings.               *)
(***************************************************************************)

CONSTANT MaxEpoch, DirtyBudget, LiveBudget, MaxAttempts

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ DirtyBudget \in Nat \ {0}
       /\ LiveBudget \in Nat \ {0}
       /\ MaxAttempts \in Nat \ {0}

Keys == 1..2
BaseGeneration == 1
BaseEpoch == 0
BaseState == {1}
Phases == {"idle", "replaying", "publishing", "unavailable"}

ApplyChange(state, change) ==
    IF change.present
    THEN state \cup {change.key}
    ELSE state \ {change.key}

RECURSIVE ApplyChanges(_, _)
ApplyChanges(state, changes) ==
    IF Len(changes) = 0
    THEN state
    ELSE ApplyChanges(ApplyChange(state, Head(changes)), Tail(changes))

ApplyOverlay(state, keys, values) ==
    {key \in Keys : IF key \in keys THEN values[key] ELSE key \in state}

ApplyPage(state, page) == ApplyOverlay(state, page.keys, page.values)

RECURSIVE ApplyPages(_, _)
ApplyPages(state, pages) ==
    IF Len(pages) = 0
    THEN state
    ELSE ApplyPages(ApplyPage(state, Head(pages)), Tail(pages))

VARIABLES
    canonicalState,
    commitEpoch,
    wal,
    phase,
    nextGeneration,
    candidateGeneration,
    candidateEpoch,
    candidateWal,
    replayCursor,
    recoveredState,
    dirtyKeys,
    dirtyValues,
    candidatePages,
    durableCandidateGenerations,
    publishedGeneration,
    publishedEpoch,
    publishedWal,
    publishedPages,
    publishedState,
    liveViewAvailable,
    liveViewEpoch,
    liveViewState,
    liveChanges,
    readerPinned,
    readerEpoch,
    readerRecoveryEpoch,
    readerRecoveryWal,
    readerState,
    schemaInvalidated,
    sqlUsesRecoveredIndex

vars == <<
    canonicalState,
    commitEpoch,
    wal,
    phase,
    nextGeneration,
    candidateGeneration,
    candidateEpoch,
    candidateWal,
    replayCursor,
    recoveredState,
    dirtyKeys,
    dirtyValues,
    candidatePages,
    durableCandidateGenerations,
    publishedGeneration,
    publishedEpoch,
    publishedWal,
    publishedPages,
    publishedState,
    liveViewAvailable,
    liveViewEpoch,
    liveViewState,
    liveChanges,
    readerPinned,
    readerEpoch,
    readerRecoveryEpoch,
    readerRecoveryWal,
    readerState,
    schemaInvalidated,
    sqlUsesRecoveredIndex
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = BaseEpoch
    /\ wal = <<>>
    /\ phase = "idle"
    /\ nextGeneration = BaseGeneration + 1
    /\ candidateGeneration = 0
    /\ candidateEpoch = 0
    /\ candidateWal = <<>>
    /\ replayCursor = 1
    /\ recoveredState = BaseState
    /\ dirtyKeys = {}
    /\ dirtyValues = [key \in Keys |-> FALSE]
    /\ candidatePages = <<>>
    /\ durableCandidateGenerations = {}
    /\ publishedGeneration = 0
    /\ publishedEpoch = BaseEpoch
    /\ publishedWal = <<>>
    /\ publishedPages = <<>>
    /\ publishedState = BaseState
    /\ liveViewAvailable = TRUE
    /\ liveViewEpoch = BaseEpoch
    /\ liveViewState = BaseState
    /\ liveChanges = <<>>
    /\ readerPinned = FALSE
    /\ readerEpoch = BaseEpoch
    /\ readerRecoveryEpoch = BaseEpoch
    /\ readerRecoveryWal = <<>>
    /\ readerState = BaseState
    /\ schemaInvalidated = FALSE
    /\ sqlUsesRecoveredIndex = FALSE

CommitInsert ==
    /\ phase = "idle"
    /\ publishedGeneration = 0
    /\ commitEpoch < MaxEpoch
    /\ \E key \in Keys:
        /\ canonicalState' = canonicalState \cup {key}
        /\ wal' = Append(wal, [key |-> key, present |-> TRUE])
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, nextGeneration, candidateGeneration, candidateEpoch, candidateWal,
        replayCursor, recoveredState, dirtyKeys, dirtyValues,
        candidatePages, durableCandidateGenerations, publishedGeneration,
        publishedEpoch, publishedWal, publishedPages, publishedState, liveViewAvailable,
        liveViewEpoch, liveViewState, liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal,
        readerState, schemaInvalidated, sqlUsesRecoveredIndex
        >>

CommitDelete ==
    /\ phase = "idle"
    /\ publishedGeneration = 0
    /\ commitEpoch < MaxEpoch
    /\ \E key \in Keys:
        /\ canonicalState' = canonicalState \ {key}
        /\ wal' = Append(wal, [key |-> key, present |-> FALSE])
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, nextGeneration, candidateGeneration, candidateEpoch, candidateWal,
        replayCursor, recoveredState, dirtyKeys, dirtyValues,
        candidatePages, durableCandidateGenerations, publishedGeneration,
        publishedEpoch, publishedWal, publishedPages, publishedState, liveViewAvailable,
        liveViewEpoch, liveViewState, liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal,
        readerState, schemaInvalidated, sqlUsesRecoveredIndex
        >>

BeginRecovery ==
    /\ phase = "idle"
    /\ commitEpoch > BaseEpoch
    /\ nextGeneration <= BaseGeneration + MaxAttempts
    /\ phase' = "replaying"
    /\ nextGeneration' = nextGeneration + 1
    /\ candidateGeneration' = nextGeneration
    /\ candidateEpoch' = commitEpoch
    /\ candidateWal' = <<>>
    /\ replayCursor' = 1
    /\ recoveredState' = BaseState
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ candidatePages' = <<>>
    /\ schemaInvalidated' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, durableCandidateGenerations,
        publishedGeneration, publishedEpoch, publishedWal, publishedPages, publishedState,
        liveViewAvailable, liveViewEpoch, liveViewState, liveChanges,
        readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState, sqlUsesRecoveredIndex
        >>

ReplayNext ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ LET change == wal[replayCursor] IN
        /\ \/ change.key \in dirtyKeys
           \/ Cardinality(dirtyKeys) < DirtyBudget
        /\ recoveredState' = ApplyChange(recoveredState, change)
        /\ dirtyKeys' = dirtyKeys \cup {change.key}
        /\ dirtyValues' = [dirtyValues EXCEPT ![change.key] = change.present]
        /\ candidateWal' = Append(candidateWal, change)
    /\ replayCursor' = replayCursor + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, nextGeneration,
        candidateGeneration, candidateEpoch, candidatePages,
        durableCandidateGenerations, publishedGeneration, publishedEpoch, publishedWal,
        publishedPages, publishedState, liveViewAvailable, liveViewEpoch,
        liveViewState, liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState,
        schemaInvalidated, sqlUsesRecoveredIndex
        >>

FlushDirty ==
    /\ phase = "replaying"
    /\ dirtyKeys # {}
    /\ candidatePages' = Append(
        candidatePages,
        [keys |-> dirtyKeys, values |-> dirtyValues]
        )
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, nextGeneration,
        candidateGeneration, candidateEpoch, candidateWal, replayCursor, recoveredState,
        durableCandidateGenerations, publishedGeneration, publishedEpoch, publishedWal,
        publishedPages, publishedState, liveViewAvailable, liveViewEpoch,
        liveViewState, liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState,
        schemaInvalidated, sqlUsesRecoveredIndex
        >>

FinishReplay ==
    /\ phase = "replaying"
    /\ replayCursor > Len(wal)
    /\ dirtyKeys = {}
    /\ recoveredState = canonicalState
    /\ candidateWal = wal
    /\ phase' = "publishing"
    /\ durableCandidateGenerations' =
        durableCandidateGenerations \cup {candidateGeneration}
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        candidateGeneration, candidateEpoch, candidateWal, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, publishedGeneration,
        publishedEpoch, publishedWal, publishedPages, publishedState, liveViewAvailable,
        liveViewEpoch, liveViewState, liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal,
        readerState, schemaInvalidated, sqlUsesRecoveredIndex
        >>

PublishManifest ==
    /\ phase = "publishing"
    /\ candidateEpoch = commitEpoch
    /\ candidateGeneration \in durableCandidateGenerations
    /\ phase' = "idle"
    /\ publishedGeneration' = candidateGeneration
    /\ publishedEpoch' = candidateEpoch
    /\ publishedWal' = candidateWal
    /\ publishedPages' = candidatePages
    /\ publishedState' = ApplyPages(BaseState, candidatePages)
    /\ liveViewAvailable' = TRUE
    /\ liveViewEpoch' = candidateEpoch
    /\ liveViewState' = ApplyPages(BaseState, candidatePages)
    /\ liveChanges' = <<>>
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        candidateGeneration, candidateEpoch, candidateWal, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, durableCandidateGenerations,
        readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

InvalidateSchema ==
    /\ phase = "replaying"
    /\ phase' = "unavailable"
    /\ schemaInvalidated' = TRUE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        candidateGeneration, candidateEpoch, candidateWal, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, durableCandidateGenerations,
        publishedGeneration, publishedEpoch, publishedWal, publishedPages, publishedState,
        liveViewAvailable, liveViewEpoch, liveViewState, liveChanges,
        readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState, sqlUsesRecoveredIndex
        >>

CrashCandidate ==
    /\ phase \in {"replaying", "publishing"}
    /\ phase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateEpoch' = 0
    /\ candidateWal' = <<>>
    /\ replayCursor' = 1
    /\ recoveredState' = BaseState
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ candidatePages' = <<>>
    /\ schemaInvalidated' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, nextGeneration,
        durableCandidateGenerations, publishedGeneration, publishedEpoch, publishedWal,
        publishedPages, publishedState, liveViewAvailable, liveViewEpoch,
        liveViewState, liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState,
        sqlUsesRecoveredIndex
        >>

LiveCommitInsert ==
    /\ phase = "idle"
    /\ publishedGeneration # 0
    /\ liveViewAvailable
    /\ liveViewEpoch = commitEpoch
    /\ commitEpoch < MaxEpoch
    /\ Len(liveChanges) < LiveBudget
    /\ \E key \in Keys:
        /\ canonicalState' = canonicalState \cup {key}
        /\ wal' = Append(wal, [key |-> key, present |-> TRUE])
        /\ liveChanges' = Append(
            liveChanges,
            [key |-> key, present |-> TRUE, epoch |-> commitEpoch + 1]
            )
    /\ commitEpoch' = commitEpoch + 1
    /\ liveViewEpoch' = commitEpoch + 1
    /\ liveViewState' = canonicalState'
    /\ UNCHANGED <<
        phase, nextGeneration, candidateGeneration, candidateEpoch, candidateWal,
        replayCursor, recoveredState, dirtyKeys, dirtyValues, candidatePages,
        durableCandidateGenerations, publishedGeneration, publishedEpoch, publishedWal,
        publishedPages, publishedState, liveViewAvailable, readerPinned,
        readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

LiveCommitDelete ==
    /\ phase = "idle"
    /\ publishedGeneration # 0
    /\ liveViewAvailable
    /\ liveViewEpoch = commitEpoch
    /\ commitEpoch < MaxEpoch
    /\ Len(liveChanges) < LiveBudget
    /\ \E key \in Keys:
        /\ canonicalState' = canonicalState \ {key}
        /\ wal' = Append(wal, [key |-> key, present |-> FALSE])
        /\ liveChanges' = Append(
            liveChanges,
            [key |-> key, present |-> FALSE, epoch |-> commitEpoch + 1]
            )
    /\ commitEpoch' = commitEpoch + 1
    /\ liveViewEpoch' = commitEpoch + 1
    /\ liveViewState' = canonicalState'
    /\ UNCHANGED <<
        phase, nextGeneration, candidateGeneration, candidateEpoch, candidateWal,
        replayCursor, recoveredState, dirtyKeys, dirtyValues, candidatePages,
        durableCandidateGenerations, publishedGeneration, publishedEpoch, publishedWal,
        publishedPages, publishedState, liveViewAvailable, readerPinned,
        readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

GraphOnlyCommit ==
    /\ phase = "idle"
    /\ publishedGeneration # 0
    /\ liveViewAvailable
    /\ liveViewEpoch = commitEpoch
    /\ commitEpoch < MaxEpoch
    /\ \E key \in Keys:
        wal' = Append(
            wal,
            [key |-> key, present |-> key \in canonicalState]
            )
    /\ commitEpoch' = commitEpoch + 1
    /\ liveViewEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        canonicalState, phase, nextGeneration, candidateGeneration,
        candidateEpoch, candidateWal, replayCursor, recoveredState, dirtyKeys, dirtyValues,
        candidatePages, durableCandidateGenerations, publishedGeneration,
        publishedEpoch, publishedWal, publishedPages, publishedState, liveViewAvailable,
        liveViewState, liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState,
        schemaInvalidated, sqlUsesRecoveredIndex
        >>

InvalidateLiveCommit ==
    /\ phase = "idle"
    /\ publishedGeneration # 0
    /\ liveViewAvailable
    /\ liveViewEpoch = commitEpoch
    /\ commitEpoch < MaxEpoch
    /\ \E key \in Keys:
        /\ canonicalState' = canonicalState \cup {key}
        /\ wal' = Append(wal, [key |-> key, present |-> TRUE])
    /\ commitEpoch' = commitEpoch + 1
    /\ liveViewAvailable' = FALSE
    /\ UNCHANGED <<
        phase, nextGeneration, candidateGeneration, candidateEpoch, candidateWal,
        replayCursor, recoveredState, dirtyKeys, dirtyValues, candidatePages,
        durableCandidateGenerations, publishedGeneration, publishedEpoch, publishedWal,
        publishedPages, publishedState, liveViewEpoch, liveViewState,
        liveChanges, readerPinned, readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState,
        schemaInvalidated, sqlUsesRecoveredIndex
        >>

PinLiveView ==
    /\ phase = "idle"
    /\ ~readerPinned
    /\ liveViewAvailable
    /\ liveViewEpoch = commitEpoch
    /\ readerPinned' = TRUE
    /\ readerEpoch' = liveViewEpoch
    /\ readerRecoveryEpoch' = publishedEpoch
    /\ readerRecoveryWal' = publishedWal
    /\ readerState' = liveViewState
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, nextGeneration,
        candidateGeneration, candidateEpoch, candidateWal, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, durableCandidateGenerations,
        publishedGeneration, publishedEpoch, publishedWal, publishedPages, publishedState,
        liveViewAvailable, liveViewEpoch, liveViewState, liveChanges,
        schemaInvalidated, sqlUsesRecoveredIndex
        >>

ReleasePinnedView ==
    /\ readerPinned
    /\ readerPinned' = FALSE
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, nextGeneration,
        candidateGeneration, candidateEpoch, candidateWal, replayCursor, recoveredState,
        dirtyKeys, dirtyValues, candidatePages, durableCandidateGenerations,
        publishedGeneration, publishedEpoch, publishedWal, publishedPages, publishedState,
        liveViewAvailable, liveViewEpoch, liveViewState, liveChanges,
        readerEpoch, readerRecoveryEpoch, readerRecoveryWal, readerState, schemaInvalidated,
        sqlUsesRecoveredIndex
        >>

Next ==
    \/ CommitInsert
    \/ CommitDelete
    \/ BeginRecovery
    \/ ReplayNext
    \/ FlushDirty
    \/ FinishReplay
    \/ PublishManifest
    \/ InvalidateSchema
    \/ CrashCandidate
    \/ LiveCommitInsert
    \/ LiveCommitDelete
    \/ GraphOnlyCommit
    \/ InvalidateLiveCommit
    \/ PinLiveView
    \/ ReleasePinnedView

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in 0..MaxEpoch
    /\ wal \in Seq([key : Keys, present : BOOLEAN])
    /\ phase \in Phases
    /\ nextGeneration \in (BaseGeneration + 1)..(BaseGeneration + MaxAttempts + 1)
    /\ candidateGeneration \in 0..(BaseGeneration + MaxAttempts)
    /\ candidateEpoch \in 0..MaxEpoch
    /\ candidateWal \in Seq([key : Keys, present : BOOLEAN])
    /\ replayCursor \in Nat \ {0}
    /\ recoveredState \subseteq Keys
    /\ dirtyKeys \subseteq Keys
    /\ dirtyValues \in [Keys -> BOOLEAN]
    /\ candidatePages \in Seq([keys : SUBSET Keys, values : [Keys -> BOOLEAN]])
    /\ durableCandidateGenerations \subseteq 1..(BaseGeneration + MaxAttempts)
    /\ publishedGeneration \in 0..(BaseGeneration + MaxAttempts)
    /\ publishedEpoch \in 0..MaxEpoch
    /\ publishedWal \in Seq([key : Keys, present : BOOLEAN])
    /\ publishedPages \in Seq([keys : SUBSET Keys, values : [Keys -> BOOLEAN]])
    /\ publishedState \subseteq Keys
    /\ liveViewAvailable \in BOOLEAN
    /\ liveViewEpoch \in 0..MaxEpoch
    /\ liveViewState \subseteq Keys
    /\ liveChanges \in Seq([
        key : Keys,
        present : BOOLEAN,
        epoch : 1..MaxEpoch
        ])
    /\ readerPinned \in BOOLEAN
    /\ readerEpoch \in 0..MaxEpoch
    /\ readerRecoveryEpoch \in 0..MaxEpoch
    /\ readerRecoveryWal \in Seq([key : Keys, present : BOOLEAN])
    /\ readerState \subseteq Keys
    /\ schemaInvalidated \in BOOLEAN
    /\ sqlUsesRecoveredIndex \in BOOLEAN

CanonicalEqualsWal == canonicalState = ApplyChanges(BaseState, wal)

DirtyOverlayBounded == Cardinality(dirtyKeys) <= DirtyBudget

LiveOverlayBounded == Len(liveChanges) <= LiveBudget

ReplayPrefixEquivalent ==
    phase \in {"replaying", "publishing"} =>
        /\ recoveredState = ApplyChanges(BaseState, SubSeq(wal, 1, replayCursor - 1))
        /\ candidateWal = SubSeq(wal, 1, replayCursor - 1)

CandidateMergeEquivalent ==
    phase \in {"replaying", "publishing"} =>
        recoveredState = ApplyOverlay(
            ApplyPages(BaseState, candidatePages),
            dirtyKeys,
            dirtyValues
            )

PublishedManifestSelectsCompletePages ==
    /\ publishedState = ApplyPages(BaseState, publishedPages)
    /\ (publishedGeneration # 0 =>
        publishedWal = SubSeq(wal, 1, publishedEpoch - BaseEpoch))

SelectedRecoveryMatchesCanonical ==
    publishedGeneration # 0 /\ publishedEpoch = commitEpoch =>
        publishedState = canonicalState

LiveViewMergeEquivalent ==
    liveViewAvailable =>
        liveViewState = ApplyChanges(publishedState, liveChanges)

CurrentLiveViewMatchesCanonical ==
    liveViewAvailable /\ liveViewEpoch = commitEpoch =>
        liveViewState = canonicalState

LiveChangeEpochsAreOrdered ==
    /\ \A index \in 1..Len(liveChanges):
        /\ publishedEpoch < liveChanges[index].epoch
        /\ liveChanges[index].epoch <= liveViewEpoch
    /\ \A index \in 2..Len(liveChanges):
        liveChanges[index - 1].epoch < liveChanges[index].epoch

PinnedReaderDoesNotDrift ==
    readerPinned =>
        /\ readerEpoch <= commitEpoch
        /\ readerRecoveryEpoch <= readerEpoch
        /\ readerRecoveryWal = SubSeq(wal, 1, readerRecoveryEpoch - BaseEpoch)
        /\ (liveViewAvailable /\ readerEpoch = liveViewEpoch =>
            readerState = liveViewState)

UnavailableLiveViewIsNotCurrent ==
    ~liveViewAvailable => liveViewEpoch < commitEpoch

CandidateGenerationIsIsolated ==
    phase \in {"replaying", "publishing"} =>
        candidateGeneration # publishedGeneration

SchemaInvalidationCannotPublish ==
    schemaInvalidated => phase = "unavailable"

ProductionSqlRemainsOnOracle == ~sqlUsesRecoveredIndex

ConstraintLookup(state, key) == key \in state

ConstraintQualificationCanRun ==
    /\ phase = "idle"
    /\ liveViewAvailable
    /\ liveViewEpoch = commitEpoch

ConstraintQualificationMatchesOracle ==
    ConstraintQualificationCanRun =>
        \A key \in Keys:
            ConstraintLookup(liveViewState, key) =
                ConstraintLookup(canonicalState, key)

Spec == Init /\ [][Next]_vars

=============================================================================
