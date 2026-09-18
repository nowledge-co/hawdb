------------------ MODULE HawDBRelationalRowSnapshotRead ------------------
EXTENDS Integers, Naturals, Sequences, FiniteSets

(***************************************************************************)
(* One pinned relational snapshot composes an immutable checkpoint with    *)
(* disk-backed recovery versions and immutable live versions. The overlay  *)
(* function is a logical ghost oracle, not a resident implementation map.  *)
(* Physical cursor and heap bounds are modeled separately by               *)
(* HawDBRelationalOverlayStreamingMerge. Live versions override recovery,  *)
(* tombstones                                                               *)
(* suppress checkpoint rows, and a later current view cannot move the      *)
(* pinned reader. Admission, cancellation, and callback panic do not poison*)
(* the reader; corruption does. An unresolved overflow reference is resolved*)
(* only through the relational state pinned at the same visible epoch.      *)
(* A read-only out-of-core activation may omit or detach its materialized    *)
(* base only after the canonical serving view is ready; it never falls back.*)
(***************************************************************************)

CONSTANT MaxOverlayEntries, MaxOverlayBytes

ASSUME /\ MaxOverlayEntries \in 1..3
       /\ MaxOverlayBytes \in 1..8

Keys == 1..3
NoVersion == -1
Deleted == 0
Values == {NoVersion, Deleted, 1, 2, 3, 4, 5}
BaseEpoch == 1
VisibleEpoch == 3
OverflowValue == 5

Base == [key \in Keys |-> key]
Recovery == [key \in Keys |->
    CASE key = 1 -> 4
      [] key = 2 -> Deleted
      [] OTHER -> NoVersion]
Live == [key \in Keys |->
    CASE key = 1 -> NoVersion
      [] key = 2 -> 5
      [] OTHER -> Deleted]

NewestOverlay(key) ==
    IF Live[key] # NoVersion THEN Live[key] ELSE Recovery[key]

VisibleValue(key) ==
    IF NewestOverlay(key) # NoVersion THEN NewestOverlay(key) ELSE Base[key]

InRange(key, lower, upper) == lower < key /\ key < upper

EntryBytes(key, value) == key + IF value = Deleted THEN 0 ELSE 1

EmptyOverlay == [key \in Keys |-> NoVersion]

ExpectedOverlay(lower, upper) ==
    [key \in Keys |->
        IF InRange(key, lower, upper)
        THEN NewestOverlay(key)
        ELSE NoVersion]

ExpectedRows(lower, upper) ==
    (IF InRange(1, lower, upper) /\ VisibleValue(1) # Deleted
     THEN <<1>> ELSE <<>>) \o
    (IF InRange(2, lower, upper) /\ VisibleValue(2) # Deleted
     THEN <<2>> ELSE <<>>) \o
    (IF InRange(3, lower, upper) /\ VisibleValue(3) # Deleted
     THEN <<3>> ELSE <<>>)

OverlayNeedsResolution(candidate) ==
    \E key \in Keys: candidate[key] = OverflowValue

IsPrefix(prefix, sequence) ==
    Len(prefix) <= Len(sequence)
    /\ \A index \in 1..Len(prefix): prefix[index] = sequence[index]

VARIABLES
    readState,
    outcome,
    lowerBound,
    upperBound,
    entryBudget,
    byteBudget,
    overlay,
    overlayEntries,
    overlayBytes,
    cursor,
    emittedRows,
    resolvedOverflow,
    poisoned,
    stoppedEarly,
    currentViewEpoch,
    servingState,
    schemaWalDurable

vars == <<
    readState,
    outcome,
    lowerBound,
    upperBound,
    entryBudget,
    byteBudget,
    overlay,
    overlayEntries,
    overlayBytes,
    cursor,
    emittedRows,
    resolvedOverflow,
    poisoned,
    stoppedEarly,
    currentViewEpoch,
    servingState,
    schemaWalDurable
>>

ServingReady == servingState \in {"ready", "readyAfterSchema", "readyDetached"}

DetachedBase == servingState \in {"readyDetached", "unavailableDetached"}

Init ==
    /\ readState = "idle"
    /\ outcome = "none"
    /\ lowerBound = 0
    /\ upperBound = 4
    /\ entryBudget = MaxOverlayEntries
    /\ byteBudget = MaxOverlayBytes
    /\ overlay = EmptyOverlay
    /\ overlayEntries = 0
    /\ overlayBytes = 0
    /\ cursor = 1
    /\ emittedRows = <<>>
    /\ resolvedOverflow = FALSE
    /\ poisoned = FALSE
    /\ stoppedEarly = FALSE
    /\ currentViewEpoch = VisibleEpoch
    /\ servingState = "ready"
    /\ schemaWalDurable = FALSE

BeginRead(lower, upper, entries, bytes) ==
    /\ readState = "idle"
    /\ ServingReady
    /\ lower \in 0..2
    /\ upper \in 2..4
    /\ lower < upper
    /\ entries \in 1..MaxOverlayEntries
    /\ bytes \in 1..MaxOverlayBytes
    /\ readState' = "recovery"
    /\ outcome' = "none"
    /\ lowerBound' = lower
    /\ upperBound' = upper
    /\ entryBudget' = entries
    /\ byteBudget' = bytes
    /\ overlay' = EmptyOverlay
    /\ overlayEntries' = 0
    /\ overlayBytes' = 0
    /\ cursor' = 1
    /\ emittedRows' = <<>>
    /\ resolvedOverflow' = FALSE
    /\ stoppedEarly' = FALSE
    /\ UNCHANGED <<poisoned, currentViewEpoch, servingState, schemaWalDurable>>

CandidateValue ==
    IF readState = "recovery" THEN Recovery[cursor] ELSE Live[cursor]

CandidateEntries ==
    overlayEntries + IF overlay[cursor] = NoVersion THEN 1 ELSE 0

CandidateBytes ==
    IF overlay[cursor] = NoVersion
    THEN overlayBytes + EntryBytes(cursor, CandidateValue)
    ELSE overlayBytes - EntryBytes(cursor, overlay[cursor])
         + EntryBytes(cursor, CandidateValue)

SkipCandidate ==
    /\ readState \in {"recovery", "live"}
    /\ cursor \in Keys
    /\ (~InRange(cursor, lowerBound, upperBound)
        \/ CandidateValue = NoVersion)
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

AdmitCandidate ==
    /\ readState \in {"recovery", "live"}
    /\ cursor \in Keys
    /\ InRange(cursor, lowerBound, upperBound)
    /\ CandidateValue # NoVersion
    /\ CandidateEntries <= entryBudget
    /\ CandidateBytes <= byteBudget
    /\ overlay' = [overlay EXCEPT ![cursor] = CandidateValue]
    /\ overlayEntries' = CandidateEntries
    /\ overlayBytes' = CandidateBytes
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        emittedRows, resolvedOverflow, poisoned, stoppedEarly, currentViewEpoch, servingState,
        schemaWalDurable
        >>

RejectCandidate ==
    /\ readState \in {"recovery", "live"}
    /\ cursor \in Keys
    /\ InRange(cursor, lowerBound, upperBound)
    /\ CandidateValue # NoVersion
    /\ (CandidateEntries > entryBudget \/ CandidateBytes > byteBudget)
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

BeginLiveCollection ==
    /\ readState = "recovery"
    /\ cursor > 3
    /\ readState' = "live"
    /\ cursor' = 1
    /\ UNCHANGED <<
        outcome, lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, emittedRows, resolvedOverflow, poisoned, stoppedEarly,
        currentViewEpoch, servingState, schemaWalDurable
        >>

BeginStreaming ==
    /\ readState = "live"
    /\ cursor > 3
    /\ readState' = IF OverlayNeedsResolution(overlay) THEN "resolving" ELSE "reading"
    /\ cursor' = 1
    /\ UNCHANGED <<
        outcome, lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, emittedRows, resolvedOverflow, poisoned, stoppedEarly,
        currentViewEpoch, servingState, schemaWalDurable
        >>

ResolveOverflowFromPinnedState ==
    /\ readState = "resolving"
    /\ OverlayNeedsResolution(overlay)
    /\ readState' = "reading"
    /\ resolvedOverflow' = TRUE
    /\ UNCHANGED <<
        outcome, lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, poisoned, stoppedEarly,
        currentViewEpoch, servingState, schemaWalDurable
        >>

RejectOverflowAdmission ==
    /\ readState = "resolving"
    /\ OverlayNeedsResolution(overlay)
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

ReadNextKey ==
    /\ readState = "reading"
    /\ cursor \in Keys
    /\ emittedRows' =
        IF InRange(cursor, lowerBound, upperBound)
           /\ VisibleValue(cursor) # Deleted
        THEN Append(emittedRows, cursor)
        ELSE emittedRows
    /\ cursor' = cursor + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, resolvedOverflow, poisoned, stoppedEarly,
        currentViewEpoch, servingState, schemaWalDurable
        >>

FinishRead ==
    /\ readState = "reading"
    /\ cursor > 3
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

StopEarly ==
    /\ readState = "reading"
    /\ Len(emittedRows) > 0
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ stoppedEarly' = TRUE
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        currentViewEpoch, servingState, schemaWalDurable
        >>

CancelRead ==
    /\ readState \in {"recovery", "live", "resolving", "reading"}
    /\ readState' = "stopped"
    /\ outcome' = "cancel"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

CallbackPanics ==
    /\ readState = "reading"
    /\ Len(emittedRows) > 0
    /\ readState' = "stopped"
    /\ outcome' = "panic"
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

DetectCorruption ==
    /\ readState \in {"recovery", "live", "resolving", "reading"}
    /\ readState' = "failed"
    /\ outcome' = "corruption"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, stoppedEarly,
        currentViewEpoch, servingState, schemaWalDurable
        >>

AdvanceCurrentView ==
    /\ ServingReady
    /\ servingState # "readyDetached"
    /\ currentViewEpoch = VisibleEpoch
    /\ currentViewEpoch' = VisibleEpoch + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, servingState, schemaWalDurable
        >>

StartBeforeFirstCheckpoint ==
    /\ readState = "idle"
    /\ servingState = "ready"
    /\ ~schemaWalDurable
    /\ servingState' = "missing"
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, schemaWalDurable
        >>

ReadInMemoryCanonical ==
    /\ readState = "idle"
    /\ servingState = "missing"
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ emittedRows' = ExpectedRows(0, 4)
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

LoseServingResources ==
    /\ readState = "idle"
    /\ ServingReady
    /\ servingState' =
        IF servingState = "readyDetached" THEN "unavailableDetached" ELSE "unavailable"
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, schemaWalDurable
        >>

RejectUnavailableReader ==
    /\ readState = "idle"
    /\ servingState \in {"unavailable", "unavailableDetached", "schemaRequired"}
    /\ readState' = "failed"
    /\ outcome' = "corruption"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        lowerBound, upperBound, entryBudget, byteBudget, overlay,
        overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow,
        stoppedEarly, currentViewEpoch, servingState, schemaWalDurable
        >>

RequireSchemaCheckpoint ==
    /\ readState = "idle"
    /\ servingState = "ready"
    /\ currentViewEpoch = VisibleEpoch
    /\ servingState' = "schemaRequired"
    /\ schemaWalDurable' = FALSE
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch
        >>

DurablySyncSchemaWal ==
    /\ readState = "idle"
    /\ servingState = "schemaRequired"
    /\ ~schemaWalDurable
    /\ schemaWalDurable' = TRUE
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, servingState
        >>

CrashBeforeSchemaWalSync ==
    /\ readState = "idle"
    /\ servingState = "schemaRequired"
    /\ ~schemaWalDurable
    /\ servingState' = "ready"
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, schemaWalDurable
        >>

BeginSchemaCheckpoint ==
    /\ readState = "idle"
    /\ servingState = "schemaRequired"
    /\ schemaWalDurable
    /\ servingState' = "checkpointing"
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, schemaWalDurable
        >>

CrashBeforeSchemaManifest ==
    /\ readState = "idle"
    /\ servingState = "checkpointing"
    /\ servingState' = "schemaRequired"
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, schemaWalDurable
        >>

PublishSchemaCheckpoint ==
    /\ readState = "idle"
    /\ servingState = "checkpointing"
    /\ currentViewEpoch = VisibleEpoch
    /\ servingState' = "readyAfterSchema"
    /\ currentViewEpoch' = VisibleEpoch + 1
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, schemaWalDurable
        >>

ActivateDetachedBase ==
    /\ readState = "idle"
    /\ servingState \in {"ready", "readyAfterSchema"}
    /\ servingState' = "readyDetached"
    /\ UNCHANGED <<
        readState, outcome, lowerBound, upperBound, entryBudget, byteBudget,
        overlay, overlayEntries, overlayBytes, cursor, emittedRows, resolvedOverflow, poisoned,
        stoppedEarly, currentViewEpoch, schemaWalDurable
        >>

Next ==
    \/ \E lower \in 0..2, upper \in 2..4,
          entries \in 1..MaxOverlayEntries, bytes \in 1..MaxOverlayBytes:
          BeginRead(lower, upper, entries, bytes)
    \/ SkipCandidate
    \/ AdmitCandidate
    \/ RejectCandidate
    \/ BeginLiveCollection
    \/ BeginStreaming
    \/ ResolveOverflowFromPinnedState
    \/ RejectOverflowAdmission
    \/ ReadNextKey
    \/ FinishRead
    \/ StopEarly
    \/ CancelRead
    \/ CallbackPanics
    \/ DetectCorruption
    \/ AdvanceCurrentView
    \/ StartBeforeFirstCheckpoint
    \/ ReadInMemoryCanonical
    \/ LoseServingResources
    \/ RejectUnavailableReader
    \/ RequireSchemaCheckpoint
    \/ DurablySyncSchemaWal
    \/ CrashBeforeSchemaWalSync
    \/ BeginSchemaCheckpoint
    \/ CrashBeforeSchemaManifest
    \/ PublishSchemaCheckpoint
    \/ ActivateDetachedBase

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ readState \in {
        "idle", "recovery", "live", "resolving", "reading", "succeeded", "failed", "stopped"
        }
    /\ outcome \in {"none", "success", "admission", "cancel", "panic", "corruption"}
    /\ lowerBound \in 0..2
    /\ upperBound \in 2..4
    /\ entryBudget \in 1..MaxOverlayEntries
    /\ byteBudget \in 1..MaxOverlayBytes
    /\ overlay \in [Keys -> Values]
    /\ overlayEntries \in 0..3
    /\ overlayBytes \in 0..8
    /\ cursor \in 1..4
    /\ emittedRows \in Seq(Keys)
    /\ resolvedOverflow \in BOOLEAN
    /\ poisoned \in BOOLEAN
    /\ stoppedEarly \in BOOLEAN
    /\ currentViewEpoch \in VisibleEpoch..(VisibleEpoch + 1)
    /\ servingState \in {
        "ready", "readyAfterSchema", "readyDetached", "missing", "unavailable",
        "unavailableDetached", "schemaRequired", "checkpointing"
        }
    /\ schemaWalDurable \in BOOLEAN

ActiveOverlayStaysWithinAdmission ==
    readState \in {"recovery", "live", "resolving", "reading", "succeeded"} =>
        /\ overlayEntries <= entryBudget
        /\ overlayBytes <= byteBudget

CollectedOverlayUsesNewestVersion ==
    (ServingReady /\ readState \in {"resolving", "reading", "succeeded"}) =>
        overlay = ExpectedOverlay(lowerBound, upperBound)

OverflowIsResolvedBeforeStreaming ==
    (readState \in {"reading", "succeeded"} /\ OverlayNeedsResolution(overlay)) =>
        resolvedOverflow

ServingAuthorityIsFailClosed ==
    /\ readState \in {"recovery", "live", "resolving", "reading"} =>
        ServingReady
    /\ servingState \in {
        "unavailable", "unavailableDetached", "schemaRequired", "checkpointing"
        } =>
        readState \notin {"recovery", "live", "resolving", "reading", "succeeded"}

DetachedBaseNeverFallsBack ==
    /\ DetachedBase => servingState # "missing"
    /\ (DetachedBase /\ readState \in {"recovery", "live", "resolving", "reading", "succeeded"}) =>
        servingState = "readyDetached"

SchemaCheckpointPublishesBeforeServing ==
    /\ servingState \in {"schemaRequired", "checkpointing"} =>
        currentViewEpoch = VisibleEpoch
    /\ servingState = "readyAfterSchema" =>
        currentViewEpoch = VisibleEpoch + 1

SchemaCheckpointWaitsForDurableWal ==
    servingState \in {"checkpointing", "readyAfterSchema"} => schemaWalDurable

EmittedRowsStayOrderedAndVisible ==
    IsPrefix(emittedRows, ExpectedRows(lowerBound, upperBound))

FullSuccessMatchesPinnedOracle ==
    (readState = "succeeded" /\ ~stoppedEarly) =>
        emittedRows = ExpectedRows(lowerBound, upperBound)

OnlyCorruptionPoisons == poisoned <=> outcome = "corruption"

PinnedIdentityDoesNotDrift ==
    /\ BaseEpoch = 1
    /\ VisibleEpoch = 3
    /\ currentViewEpoch >= VisibleEpoch

=============================================================================
