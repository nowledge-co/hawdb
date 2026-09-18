-------------------------- MODULE HawDBRowRecovery --------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A checkpoint-pinned row root is recovered by replaying every consecutive *)
(* WAL fragment into a bounded dirty map. Pressure flushes complete dirty    *)
(* maps into immutable disk runs; no database-sized recovery overlay is      *)
(* retained. The complete base-plus-run state becomes selectable only after  *)
(* the generation manifest is durable and the latest selector is published. *)
(* A crash before that final replacement leaves the candidate unreachable.   *)
(* SQL remains disabled and an Arc-like reader pin never follows later roots.*)
(***************************************************************************)

CONSTANT MaxEpoch, DirtyBudget, RunBudget

ASSUME /\ MaxEpoch \in Nat \ {0, 1}
       /\ DirtyBudget \in Nat \ {0}
       /\ RunBudget \in Nat \ {0}

Keys == 1..2
Kinds == {"row", "empty", "schema"}
Phases == {"unmounted", "replaying", "candidate", "ready", "unavailable"}
PublicationStages == {"none", "runs", "manifest", "published"}
FailureKinds == {"none", "missing", "stale", "corrupt", "capacity", "runs", "schema"}

BaseGeneration == 1
BaseEpoch == 1
BaseState == {1}

ApplyRecord(state, record) ==
    CASE record.kind = "row" -> (state \ record.keys) \cup record.inserted
      [] OTHER -> state

RECURSIVE ApplyWalPrefix(_, _, _)
ApplyWalPrefix(state, log, count) ==
    IF count = 0
    THEN state
    ELSE ApplyRecord(ApplyWalPrefix(state, log, count - 1), log[count])

ApplyOverlay(state, keys, values) ==
    {key \in Keys : IF key \in keys THEN values[key] ELSE key \in state}

VARIABLES
    canonicalState,
    commitEpoch,
    wal,
    phase,
    baseGeneration,
    latestRootGeneration,
    replayCursor,
    visibleEpoch,
    dirtyKeys,
    dirtyValues,
    durableRunState,
    runCount,
    candidateState,
    publicationStage,
    viewAvailable,
    viewGeneration,
    viewEpoch,
    viewState,
    readerPinned,
    readerGeneration,
    readerEpoch,
    readerState,
    failureKind,
    pageSlotsRead,
    sqlUsesRowView

vars == <<
    canonicalState,
    commitEpoch,
    wal,
    phase,
    baseGeneration,
    latestRootGeneration,
    replayCursor,
    visibleEpoch,
    dirtyKeys,
    dirtyValues,
    durableRunState,
    runCount,
    candidateState,
    publicationStage,
    viewAvailable,
    viewGeneration,
    viewEpoch,
    viewState,
    readerPinned,
    readerGeneration,
    readerEpoch,
    readerState,
    failureKind,
    pageSlotsRead,
    sqlUsesRowView
>>

Init ==
    /\ canonicalState = BaseState
    /\ commitEpoch = BaseEpoch
    /\ wal = <<>>
    /\ phase = "unmounted"
    /\ baseGeneration = 0
    /\ latestRootGeneration = BaseGeneration
    /\ replayCursor = 1
    /\ visibleEpoch = BaseEpoch
    /\ dirtyKeys = {}
    /\ dirtyValues = [key \in Keys |-> FALSE]
    /\ durableRunState = BaseState
    /\ runCount = 0
    /\ candidateState = BaseState
    /\ publicationStage = "none"
    /\ viewAvailable = FALSE
    /\ viewGeneration = 0
    /\ viewEpoch = 0
    /\ viewState = {}
    /\ readerPinned = FALSE
    /\ readerGeneration = 0
    /\ readerEpoch = 0
    /\ readerState = {}
    /\ failureKind = "none"
    /\ pageSlotsRead = FALSE
    /\ sqlUsesRowView = FALSE

CommitRow ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ \E changed \in SUBSET Keys:
        /\ changed # {}
        /\ \E inserted \in SUBSET changed:
            LET record == [
                epoch |-> commitEpoch + 1,
                kind |-> "row",
                keys |-> changed,
                inserted |-> inserted
                ]
            IN
                /\ canonicalState' = ApplyRecord(canonicalState, record)
                /\ wal' = Append(wal, record)
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, baseGeneration, latestRootGeneration, replayCursor,
        visibleEpoch, dirtyKeys, dirtyValues, durableRunState, runCount,
        candidateState, publicationStage, viewAvailable, viewGeneration,
        viewEpoch, viewState, readerPinned, readerGeneration, readerEpoch,
        readerState, failureKind, pageSlotsRead, sqlUsesRowView
        >>

CommitTwoFragments ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ \E firstChanged \in SUBSET Keys:
        /\ firstChanged # {}
        /\ \E firstInserted \in SUBSET firstChanged:
            /\ \E secondChanged \in SUBSET Keys:
                /\ secondChanged # {}
                /\ \E secondInserted \in SUBSET secondChanged:
                    LET epoch == commitEpoch + 1
                        first == [
                            epoch |-> epoch,
                            kind |-> "row",
                            keys |-> firstChanged,
                            inserted |-> firstInserted
                            ]
                        second == [
                            epoch |-> epoch,
                            kind |-> "row",
                            keys |-> secondChanged,
                            inserted |-> secondInserted
                            ]
                    IN
                        /\ canonicalState' =
                            ApplyRecord(ApplyRecord(canonicalState, first), second)
                        /\ wal' = wal \o <<first, second>>
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        phase, baseGeneration, latestRootGeneration, replayCursor,
        visibleEpoch, dirtyKeys, dirtyValues, durableRunState, runCount,
        candidateState, publicationStage, viewAvailable, viewGeneration,
        viewEpoch, viewState, readerPinned, readerGeneration, readerEpoch,
        readerState, failureKind, pageSlotsRead, sqlUsesRowView
        >>

CommitEmpty ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ wal' = Append(wal, [
        epoch |-> commitEpoch + 1,
        kind |-> "empty",
        keys |-> {},
        inserted |-> {}
        ])
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        canonicalState, phase, baseGeneration, latestRootGeneration,
        replayCursor, visibleEpoch, dirtyKeys, dirtyValues, durableRunState,
        runCount, candidateState, publicationStage, viewAvailable,
        viewGeneration, viewEpoch, viewState, readerPinned, readerGeneration,
        readerEpoch, readerState, failureKind, pageSlotsRead, sqlUsesRowView
        >>

CommitSchema ==
    /\ phase = "unmounted"
    /\ commitEpoch < MaxEpoch
    /\ wal' = Append(wal, [
        epoch |-> commitEpoch + 1,
        kind |-> "schema",
        keys |-> {},
        inserted |-> {}
        ])
    /\ commitEpoch' = commitEpoch + 1
    /\ UNCHANGED <<
        canonicalState, phase, baseGeneration, latestRootGeneration,
        replayCursor, visibleEpoch, dirtyKeys, dirtyValues, durableRunState,
        runCount, candidateState, publicationStage, viewAvailable,
        viewGeneration, viewEpoch, viewState, readerPinned, readerGeneration,
        readerEpoch, readerState, failureKind, pageSlotsRead, sqlUsesRowView
        >>

MountValidBase ==
    /\ phase = "unmounted"
    /\ phase' = "replaying"
    /\ baseGeneration' = BaseGeneration
    /\ replayCursor' = 1
    /\ visibleEpoch' = BaseEpoch
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ durableRunState' = BaseState
    /\ runCount' = 0
    /\ candidateState' = BaseState
    /\ publicationStage' = "none"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "none"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, latestRootGeneration,
        viewGeneration, viewEpoch, viewState, readerPinned, readerGeneration,
        readerEpoch, readerState, pageSlotsRead, sqlUsesRowView
        >>

MountInvalidBase(reason) ==
    /\ phase = "unmounted"
    /\ reason \in {"missing", "stale", "corrupt"}
    /\ phase' = "unavailable"
    /\ baseGeneration' = 0
    /\ viewAvailable' = FALSE
    /\ failureKind' = reason
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, latestRootGeneration, replayCursor,
        visibleEpoch, dirtyKeys, dirtyValues, durableRunState, runCount,
        candidateState, publicationStage, viewGeneration, viewEpoch,
        viewState, readerPinned, readerGeneration, readerEpoch, readerState,
        pageSlotsRead, sqlUsesRowView
        >>

FlushDirty ==
    /\ phase = "replaying"
    /\ dirtyKeys # {}
    /\ runCount < RunBudget
    /\ durableRunState' = ApplyOverlay(durableRunState, dirtyKeys, dirtyValues)
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ runCount' = runCount + 1
    /\ publicationStage' = "runs"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, candidateState,
        viewAvailable, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

ReplayRowFragment ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ LET record == wal[replayCursor] IN
        /\ record.kind = "row"
        /\ Cardinality(record.keys) <= DirtyBudget
        /\ Cardinality(dirtyKeys \cup record.keys) <= DirtyBudget
        /\ dirtyKeys' = dirtyKeys \cup record.keys
        /\ dirtyValues' = [key \in Keys |->
            IF key \in record.keys
            THEN key \in record.inserted
            ELSE dirtyValues[key]]
        /\ visibleEpoch' = record.epoch
    /\ replayCursor' = replayCursor + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestRootGeneration, durableRunState, runCount, candidateState,
        publicationStage, viewAvailable, viewGeneration, viewEpoch, viewState,
        readerPinned, readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

ReplayEmpty ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ wal[replayCursor].kind = "empty"
    /\ visibleEpoch' = wal[replayCursor].epoch
    /\ replayCursor' = replayCursor + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestRootGeneration, dirtyKeys, dirtyValues, durableRunState,
        runCount, candidateState, publicationStage, viewAvailable,
        viewGeneration, viewEpoch, viewState, readerPinned, readerGeneration,
        readerEpoch, readerState, failureKind, pageSlotsRead, sqlUsesRowView
        >>

RejectCapacity ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ wal[replayCursor].kind = "row"
    /\ Cardinality(wal[replayCursor].keys) > DirtyBudget
    /\ phase' = "unavailable"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "capacity"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, dirtyKeys,
        dirtyValues, durableRunState, runCount, candidateState,
        publicationStage, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, pageSlotsRead,
        sqlUsesRowView
        >>

RejectRunCapacity ==
    /\ phase = "replaying"
    /\ runCount = RunBudget
    /\ \/ (replayCursor = Len(wal) + 1 /\ dirtyKeys # {})
       \/ (replayCursor <= Len(wal)
           /\ wal[replayCursor].kind = "row"
           /\ Cardinality(wal[replayCursor].keys) <= DirtyBudget
           /\ Cardinality(dirtyKeys \cup wal[replayCursor].keys) > DirtyBudget)
    /\ phase' = "unavailable"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "runs"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, dirtyKeys,
        dirtyValues, durableRunState, runCount, candidateState,
        publicationStage, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, pageSlotsRead,
        sqlUsesRowView
        >>

RejectSchema ==
    /\ phase = "replaying"
    /\ replayCursor <= Len(wal)
    /\ wal[replayCursor].kind = "schema"
    /\ phase' = "unavailable"
    /\ viewAvailable' = FALSE
    /\ failureKind' = "schema"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, dirtyKeys,
        dirtyValues, durableRunState, runCount, candidateState,
        publicationStage, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, pageSlotsRead,
        sqlUsesRowView
        >>

BeginCandidate ==
    /\ phase = "replaying"
    /\ replayCursor = Len(wal) + 1
    /\ visibleEpoch = commitEpoch
    /\ dirtyKeys = {} \/ runCount < RunBudget
    /\ candidateState' = ApplyOverlay(durableRunState, dirtyKeys, dirtyValues)
    /\ durableRunState' = ApplyOverlay(durableRunState, dirtyKeys, dirtyValues)
    /\ runCount' = runCount + IF dirtyKeys = {} THEN 0 ELSE 1
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ phase' = "candidate"
    /\ publicationStage' = "runs"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, viewAvailable,
        viewGeneration, viewEpoch, viewState, readerPinned, readerGeneration,
        readerEpoch, readerState, failureKind, pageSlotsRead, sqlUsesRowView
        >>

PersistCandidateManifest ==
    /\ phase = "candidate"
    /\ publicationStage = "runs"
    /\ publicationStage' = "manifest"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, dirtyKeys,
        dirtyValues, durableRunState, runCount, candidateState,
        viewAvailable, viewGeneration, viewEpoch, viewState, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

PublishCandidate ==
    /\ phase = "candidate"
    /\ publicationStage = "manifest"
    /\ candidateState = canonicalState
    /\ phase' = "ready"
    /\ publicationStage' = "published"
    /\ viewAvailable' = TRUE
    /\ viewGeneration' = baseGeneration
    /\ viewEpoch' = visibleEpoch
    /\ viewState' = candidateState
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, dirtyKeys,
        dirtyValues, durableRunState, runCount, candidateState, readerPinned,
        readerGeneration, readerEpoch, readerState, failureKind,
        pageSlotsRead, sqlUsesRowView
        >>

CrashCandidate ==
    /\ phase = "candidate"
    /\ phase' = "unmounted"
    /\ baseGeneration' = 0
    /\ replayCursor' = 1
    /\ visibleEpoch' = BaseEpoch
    /\ dirtyKeys' = {}
    /\ dirtyValues' = [key \in Keys |-> FALSE]
    /\ durableRunState' = BaseState
    /\ runCount' = 0
    /\ candidateState' = BaseState
    /\ publicationStage' = "none"
    /\ viewAvailable' = FALSE
    /\ viewGeneration' = 0
    /\ viewEpoch' = 0
    /\ viewState' = {}
    /\ failureKind' = "none"
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, latestRootGeneration, readerPinned,
        readerGeneration, readerEpoch, readerState, pageSlotsRead,
        sqlUsesRowView
        >>

PinReader ==
    /\ phase = "ready"
    /\ viewAvailable
    /\ ~readerPinned
    /\ readerPinned' = TRUE
    /\ readerGeneration' = viewGeneration
    /\ readerEpoch' = viewEpoch
    /\ readerState' = viewState
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        latestRootGeneration, replayCursor, visibleEpoch, dirtyKeys,
        dirtyValues, durableRunState, runCount, candidateState,
        publicationStage, viewAvailable, viewGeneration, viewEpoch, viewState,
        failureKind, pageSlotsRead, sqlUsesRowView
        >>

PublishNewRoot ==
    /\ phase = "ready"
    /\ latestRootGeneration = BaseGeneration
    /\ latestRootGeneration' = BaseGeneration + 1
    /\ UNCHANGED <<
        canonicalState, commitEpoch, wal, phase, baseGeneration,
        replayCursor, visibleEpoch, dirtyKeys, dirtyValues, durableRunState,
        runCount, candidateState, publicationStage, viewAvailable,
        viewGeneration, viewEpoch, viewState, readerPinned, readerGeneration,
        readerEpoch, readerState, failureKind, pageSlotsRead, sqlUsesRowView
        >>

Next ==
    \/ CommitRow
    \/ CommitTwoFragments
    \/ CommitEmpty
    \/ CommitSchema
    \/ MountValidBase
    \/ MountInvalidBase("missing")
    \/ MountInvalidBase("stale")
    \/ MountInvalidBase("corrupt")
    \/ FlushDirty
    \/ ReplayRowFragment
    \/ ReplayEmpty
    \/ RejectCapacity
    \/ RejectRunCapacity
    \/ RejectSchema
    \/ BeginCandidate
    \/ PersistCandidateManifest
    \/ PublishCandidate
    \/ CrashCandidate
    \/ PinReader
    \/ PublishNewRoot

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalState \subseteq Keys
    /\ commitEpoch \in BaseEpoch..MaxEpoch
    /\ wal \in Seq([
        epoch : BaseEpoch..MaxEpoch,
        kind : Kinds,
        keys : SUBSET Keys,
        inserted : SUBSET Keys
        ])
    /\ phase \in Phases
    /\ baseGeneration \in 0..BaseGeneration
    /\ latestRootGeneration \in BaseGeneration..(BaseGeneration + 1)
    /\ replayCursor \in 1..(Len(wal) + 1)
    /\ visibleEpoch \in BaseEpoch..MaxEpoch
    /\ dirtyKeys \subseteq Keys
    /\ dirtyValues \in [Keys -> BOOLEAN]
    /\ durableRunState \subseteq Keys
    /\ runCount \in 0..RunBudget
    /\ candidateState \subseteq Keys
    /\ publicationStage \in PublicationStages
    /\ viewAvailable \in BOOLEAN
    /\ viewGeneration \in 0..BaseGeneration
    /\ viewEpoch \in 0..MaxEpoch
    /\ viewState \subseteq Keys
    /\ readerPinned \in BOOLEAN
    /\ readerGeneration \in 0..BaseGeneration
    /\ readerEpoch \in 0..MaxEpoch
    /\ readerState \subseteq Keys
    /\ failureKind \in FailureKinds
    /\ pageSlotsRead \in BOOLEAN
    /\ sqlUsesRowView \in BOOLEAN

WalEpochsAreContiguous ==
    \A position \in 1..Len(wal):
        /\ wal[position].inserted \subseteq wal[position].keys
        /\ IF position = 1
           THEN wal[position].epoch = BaseEpoch + 1
           ELSE \/ wal[position].epoch = wal[position - 1].epoch
                \/ wal[position].epoch = wal[position - 1].epoch + 1

CanonicalEqualsWal ==
    canonicalState = ApplyWalPrefix(BaseState, wal, Len(wal))

DirtyStateIsBounded == Cardinality(dirtyKeys) <= DirtyBudget

ReplayPrefixEquivalent ==
    phase \in {"replaying", "candidate", "ready"} =>
        ApplyOverlay(durableRunState, dirtyKeys, dirtyValues) =
            ApplyWalPrefix(BaseState, wal, replayCursor - 1)

RejectedFragmentIsAtomic ==
    phase = "unavailable" /\ failureKind \in {"capacity", "runs", "schema"} =>
        ApplyOverlay(durableRunState, dirtyKeys, dirtyValues) =
            ApplyWalPrefix(BaseState, wal, replayCursor - 1)

CandidateIsNotVisible == phase = "candidate" => ~viewAvailable

OnlyManifestLastRecoveryIsVisible ==
    viewAvailable =>
        /\ phase = "ready"
        /\ publicationStage = "published"
        /\ viewGeneration = BaseGeneration
        /\ viewEpoch = commitEpoch
        /\ viewState = canonicalState

UnavailableRecoveryNeverServes == phase = "unavailable" => ~viewAvailable

ColdMountDoesNotReadPageSlots == ~pageSlotsRead

ProductionSqlRemainsOnCanonicalOracle == ~sqlUsesRowView

PinnedReaderDoesNotDrift ==
    readerPinned =>
        /\ readerGeneration = BaseGeneration
        /\ readerEpoch <= commitEpoch
        /\ readerState = ApplyWalPrefix(BaseState, wal, Len(wal))

NewRootDoesNotMovePinnedViews ==
    readerPinned /\ latestRootGeneration > BaseGeneration =>
        readerGeneration = BaseGeneration

=============================================================================
