--------------- MODULE HawDBRelationalOverlayStreamingMerge ---------------
EXTENDS Integers, Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A pinned relational range read primes at most one projected head from   *)
(* each recovery run or live batch. The smallest key is selected, all heads*)
(* for that key are coalesced by epoch, and only then is each source        *)
(* advanced. The selected head merges with the immutable checkpoint key.   *)
(* Source count, distinct overlay work, and heap plus one working head are  *)
(* admitted. A tombstone suppresses the checkpoint; an equal-epoch         *)
(* duplicate is corruption.                                                *)
(***************************************************************************)

CONSTANT MaxSources, MaxOpenFiles, MaxOverlayEntries, MaxOverlayBytes

ASSUME /\ MaxSources \in 1..3
       /\ MaxOpenFiles \in 1..MaxSources
       /\ MaxOverlayEntries \in 1..4
       /\ MaxOverlayBytes \in 1..12

Sources == 1..3
RecoverySources == {1, 2}
Keys == 1..4
AllFields == 1..4
RequestedFields == {1, 3}
NoKey == 0
Deleted == 0
OverflowValue == 5

SourceRows(source) ==
    CASE source = 1 ->
            <<[key |-> 1, epoch |-> 2, value |-> 4],
              [key |-> 2, epoch |-> 2, value |-> Deleted]>>
      [] source = 2 ->
            <<[key |-> 2, epoch |-> 3, value |-> OverflowValue],
              [key |-> 3, epoch |-> 3, value |-> Deleted]>>
      [] OTHER ->
            <<[key |-> 3, epoch |-> 4, value |-> Deleted],
              [key |-> 4, epoch |-> 4, value |-> 2]>>

BaseKeys == {1, 2, 3}
BaseValue(key) == key

ExpectedProcessedKeys == <<1, 2, 3, 4>>
ExpectedProcessedValues == <<4, OverflowValue, Deleted, 2>>
ExpectedRows == <<1, 2, 4>>

IsPrefix(prefix, sequence) ==
    Len(prefix) <= Len(sequence)
    /\ \A index \in 1..Len(prefix): prefix[index] = sequence[index]

SourceHead(source, positions) ==
    IF positions[source] <= Len(SourceRows(source))
    THEN SourceRows(source)[positions[source]]
    ELSE [key |-> NoKey, epoch |-> 0, value |-> Deleted]

ActiveSources(positions) ==
    {source \in Sources: SourceHead(source, positions).key # NoKey}

EntryBytes(head) == head.key + IF head.value = Deleted THEN 0 ELSE 1

BufferedBytes(positions) ==
    EntryBytes(SourceHead(1, positions)) +
    EntryBytes(SourceHead(2, positions)) +
    EntryBytes(SourceHead(3, positions))

CandidateKeys(positions, baseRemaining) ==
    baseRemaining \cup
    {SourceHead(source, positions).key:
        source \in ActiveSources(positions)}

MinimumKey(keys) ==
    CHOOSE key \in keys: \A other \in keys: key <= other

NextKey(positions, baseRemaining) ==
    MinimumKey(CandidateKeys(positions, baseRemaining))

SourcesAtKey(key, positions) ==
    {source \in ActiveSources(positions): SourceHead(source, positions).key = key}

NewestSource(key, positions) ==
    CHOOSE source \in SourcesAtKey(key, positions):
        \A other \in SourcesAtKey(key, positions):
            SourceHead(source, positions).epoch >= SourceHead(other, positions).epoch

SelectedValue(key, positions) ==
    IF SourcesAtKey(key, positions) = {}
    THEN BaseValue(key)
    ELSE SourceHead(NewestSource(key, positions), positions).value

SelectedEpoch(key, positions) ==
    IF SourcesAtKey(key, positions) = {}
    THEN 0
    ELSE SourceHead(NewestSource(key, positions), positions).epoch

EqualEpochDuplicate(key, positions) ==
    \E left, right \in SourcesAtKey(key, positions):
        /\ left # right
        /\ SourceHead(left, positions).epoch = SourceHead(right, positions).epoch

AdvanceSources(key, positions) ==
    [source \in Sources |->
        IF source \in SourcesAtKey(key, positions)
        THEN positions[source] + 1
        ELSE positions[source]]

Max(left, right) == IF left >= right THEN left ELSE right
Min(left, right) == IF left <= right THEN left ELSE right

VARIABLES
    readState,
    outcome,
    entryBudget,
    byteBudget,
    positions,
    baseRemaining,
    residentBytes,
    openFiles,
    peakOpenFiles,
    peakResidentBytes,
    peakBufferedEntries,
    overlayEntries,
    processedKeys,
    processedValues,
    selectedEpochs,
    emittedRows,
    resolvedOverflow,
    stoppedEarly,
    poisoned,
    pinnedEpoch,
    currentEpoch,
    decodedRecoveryFields,
    validatedRecoveryFields

vars == <<
    readState,
    outcome,
    entryBudget,
    byteBudget,
    positions,
    baseRemaining,
    residentBytes,
    openFiles,
    peakOpenFiles,
    peakResidentBytes,
    peakBufferedEntries,
    overlayEntries,
    processedKeys,
    processedValues,
    selectedEpochs,
    emittedRows,
    resolvedOverflow,
    stoppedEarly,
    poisoned,
    pinnedEpoch,
    currentEpoch,
    decodedRecoveryFields,
    validatedRecoveryFields
>>

DonePositions == [source \in Sources |-> Len(SourceRows(source)) + 1]
InitialPositions == [source \in Sources |-> 1]

Init ==
    /\ readState = "idle"
    /\ outcome = "none"
    /\ entryBudget = MaxOverlayEntries
    /\ byteBudget = MaxOverlayBytes
    /\ positions = DonePositions
    /\ baseRemaining = BaseKeys
    /\ residentBytes = 0
    /\ openFiles = 0
    /\ peakOpenFiles = 0
    /\ peakResidentBytes = 0
    /\ peakBufferedEntries = 0
    /\ overlayEntries = 0
    /\ processedKeys = <<>>
    /\ processedValues = <<>>
    /\ selectedEpochs = <<>>
    /\ emittedRows = <<>>
    /\ resolvedOverflow = FALSE
    /\ stoppedEarly = FALSE
    /\ poisoned = FALSE
    /\ pinnedEpoch = 4
    /\ currentEpoch = 4
    /\ decodedRecoveryFields = {}
    /\ validatedRecoveryFields = {}

BeginRead(entries, bytes) ==
    /\ readState = "idle"
    /\ entries \in 1..MaxOverlayEntries
    /\ bytes \in 1..MaxOverlayBytes
    /\ readState' = "prepared"
    /\ outcome' = "none"
    /\ entryBudget' = entries
    /\ byteBudget' = bytes
    /\ positions' = InitialPositions
    /\ baseRemaining' = BaseKeys
    /\ residentBytes' = 0
    /\ openFiles' = 0
    /\ peakOpenFiles' = 0
    /\ peakResidentBytes' = 0
    /\ peakBufferedEntries' = 0
    /\ overlayEntries' = 0
    /\ processedKeys' = <<>>
    /\ processedValues' = <<>>
    /\ selectedEpochs' = <<>>
    /\ emittedRows' = <<>>
    /\ resolvedOverflow' = FALSE
    /\ stoppedEarly' = FALSE
    /\ decodedRecoveryFields' = {}
    /\ validatedRecoveryFields' = {}
    /\ UNCHANGED <<poisoned, pinnedEpoch, currentEpoch>>

PrimeSources ==
    /\ readState = "prepared"
    /\ readState' = "priming"
    /\ residentBytes' = BufferedBytes(positions)
    /\ openFiles' = Min(Cardinality(ActiveSources(positions)), MaxOpenFiles)
    /\ peakOpenFiles' = Min(Cardinality(ActiveSources(positions)), MaxOpenFiles)
    /\ peakResidentBytes' = BufferedBytes(positions)
    /\ peakBufferedEntries' = Cardinality(ActiveSources(positions))
    /\ decodedRecoveryFields' = RequestedFields
    /\ validatedRecoveryFields' = AllFields
    /\ UNCHANGED <<
        outcome, entryBudget, byteBudget, positions, baseRemaining,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly, poisoned,
        pinnedEpoch, currentEpoch
        >>

AcceptPrimedSources ==
    /\ readState = "priming"
    /\ Cardinality(ActiveSources(positions)) <= entryBudget
    /\ residentBytes <= byteBudget
    /\ readState' = "merging"
    /\ UNCHANGED <<
        outcome, entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly, poisoned,
        pinnedEpoch, currentEpoch, decodedRecoveryFields, validatedRecoveryFields
        >>

RejectPrimedSources ==
    /\ readState = "priming"
    /\ \/ Cardinality(ActiveSources(positions)) > entryBudget
       \/ residentBytes > byteBudget
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ UNCHANGED <<
        entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly, poisoned,
        pinnedEpoch, currentEpoch, decodedRecoveryFields, validatedRecoveryFields
        >>

MergeNext ==
    /\ readState = "merging"
    /\ CandidateKeys(positions, baseRemaining) # {}
    /\ LET key == NextKey(positions, baseRemaining)
           sources == SourcesAtKey(key, positions)
           value == SelectedValue(key, positions)
           epoch == SelectedEpoch(key, positions)
           nextPositions == AdvanceSources(key, positions)
           nextEntries == overlayEntries + IF sources = {} THEN 0 ELSE 1
           workingBytes == IF sources = {} THEN 0 ELSE EntryBytes(
                [key |-> key, epoch |-> epoch, value |-> value])
           nextResident == BufferedBytes(nextPositions)
       IN /\ ~EqualEpochDuplicate(key, positions)
          /\ nextEntries <= entryBudget
          /\ nextResident + workingBytes <= byteBudget
          /\ positions' = nextPositions
          /\ baseRemaining' = baseRemaining \ {key}
          /\ residentBytes' = nextResident
          /\ peakResidentBytes' = Max(peakResidentBytes, nextResident + workingBytes)
          /\ peakBufferedEntries' = Max(
                peakBufferedEntries,
                Cardinality(ActiveSources(nextPositions)) +
                    IF sources = {} THEN 0 ELSE 1)
          /\ overlayEntries' = nextEntries
          /\ processedKeys' = Append(processedKeys, key)
          /\ processedValues' = Append(processedValues, value)
          /\ selectedEpochs' = Append(selectedEpochs, epoch)
          /\ emittedRows' = IF value = Deleted
                THEN emittedRows
                ELSE Append(emittedRows, key)
          /\ resolvedOverflow' = (resolvedOverflow \/ (value = OverflowValue))
          /\ decodedRecoveryFields' =
                IF sources \cap RecoverySources = {}
                THEN decodedRecoveryFields
                ELSE decodedRecoveryFields \cup RequestedFields
          /\ validatedRecoveryFields' =
                IF sources \cap RecoverySources = {}
                THEN validatedRecoveryFields
                ELSE validatedRecoveryFields \cup AllFields
          /\ UNCHANGED <<
                readState, outcome, entryBudget, byteBudget, openFiles, peakOpenFiles,
                stoppedEarly, poisoned, pinnedEpoch, currentEpoch
                >>

RejectNextAdmission ==
    /\ readState = "merging"
    /\ CandidateKeys(positions, baseRemaining) # {}
    /\ LET key == NextKey(positions, baseRemaining)
           sources == SourcesAtKey(key, positions)
           value == SelectedValue(key, positions)
           nextPositions == AdvanceSources(key, positions)
           nextEntries == overlayEntries + IF sources = {} THEN 0 ELSE 1
           workingBytes == IF sources = {} THEN 0 ELSE EntryBytes(
                [key |-> key, epoch |-> SelectedEpoch(key, positions), value |-> value])
       IN /\ ~EqualEpochDuplicate(key, positions)
          /\ \/ nextEntries > entryBudget
             \/ BufferedBytes(nextPositions) + workingBytes > byteBudget
    /\ readState' = "failed"
    /\ outcome' = "admission"
    /\ UNCHANGED <<
        entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly, poisoned,
        pinnedEpoch, currentEpoch, decodedRecoveryFields, validatedRecoveryFields
        >>

DetectDuplicateCorruption ==
    /\ readState = "merging"
    /\ CandidateKeys(positions, baseRemaining) # {}
    /\ EqualEpochDuplicate(NextKey(positions, baseRemaining), positions)
    /\ readState' = "failed"
    /\ outcome' = "corruption"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly,
        pinnedEpoch, currentEpoch, decodedRecoveryFields, validatedRecoveryFields
        >>

FinishRead ==
    /\ readState = "merging"
    /\ CandidateKeys(positions, baseRemaining) = {}
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ UNCHANGED <<
        entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly, poisoned,
        pinnedEpoch, currentEpoch, decodedRecoveryFields, validatedRecoveryFields
        >>

StopEarly ==
    /\ readState = "merging"
    /\ Len(processedKeys) > 0
    /\ readState' = "succeeded"
    /\ outcome' = "success"
    /\ stoppedEarly' = TRUE
    /\ UNCHANGED <<
        entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, poisoned,
        pinnedEpoch, currentEpoch, decodedRecoveryFields, validatedRecoveryFields
        >>

CancelRead ==
    /\ readState \in {"prepared", "priming", "merging"}
    /\ readState' = "stopped"
    /\ outcome' = "cancel"
    /\ UNCHANGED <<
        entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly, poisoned,
        pinnedEpoch, currentEpoch, decodedRecoveryFields, validatedRecoveryFields
        >>

AdvanceCurrentView ==
    /\ currentEpoch = pinnedEpoch
    /\ currentEpoch' = currentEpoch + 1
    /\ UNCHANGED <<
        readState, outcome, entryBudget, byteBudget, positions, baseRemaining,
        residentBytes, openFiles, peakOpenFiles, peakResidentBytes, peakBufferedEntries,
        overlayEntries, processedKeys, processedValues, selectedEpochs,
        emittedRows, resolvedOverflow, stoppedEarly, poisoned, pinnedEpoch,
        decodedRecoveryFields, validatedRecoveryFields
        >>

Next ==
    \/ \E entries \in 1..MaxOverlayEntries, bytes \in 1..MaxOverlayBytes:
        BeginRead(entries, bytes)
    \/ PrimeSources
    \/ AcceptPrimedSources
    \/ RejectPrimedSources
    \/ MergeNext
    \/ RejectNextAdmission
    \/ DetectDuplicateCorruption
    \/ FinishRead
    \/ StopEarly
    \/ CancelRead
    \/ AdvanceCurrentView

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ readState \in {"idle", "prepared", "priming", "merging", "succeeded", "failed", "stopped"}
    /\ outcome \in {"none", "success", "admission", "cancel", "corruption"}
    /\ entryBudget \in 1..MaxOverlayEntries
    /\ byteBudget \in 1..MaxOverlayBytes
    /\ positions \in [Sources -> 1..3]
    /\ baseRemaining \subseteq BaseKeys
    /\ residentBytes \in Nat
    /\ openFiles \in Nat
    /\ peakOpenFiles \in Nat
    /\ peakResidentBytes \in Nat
    /\ peakBufferedEntries \in Nat
    /\ overlayEntries \in Nat
    /\ processedKeys \in Seq(Keys)
    /\ processedValues \in Seq({Deleted, 1, 2, 3, 4, OverflowValue})
    /\ selectedEpochs \in Seq(0..4)
    /\ emittedRows \in Seq(Keys)
    /\ resolvedOverflow \in BOOLEAN
    /\ stoppedEarly \in BOOLEAN
    /\ poisoned \in BOOLEAN
    /\ pinnedEpoch = 4
    /\ currentEpoch \in 4..5
    /\ decodedRecoveryFields \subseteq AllFields
    /\ validatedRecoveryFields \subseteq AllFields

StreamingStateIsBounded ==
    /\ openFiles <= MaxOpenFiles
    /\ peakOpenFiles <= MaxOpenFiles
    /\ (readState \in {"merging", "succeeded"} =>
        /\ residentBytes <= byteBudget
        /\ peakResidentBytes <= byteBudget
        /\ peakBufferedEntries <= MaxSources + 1
        /\ overlayEntries <= entryBudget)

PreparedSourcesAreLazy ==
    readState = "prepared" =>
        /\ openFiles = 0
        /\ peakOpenFiles = 0
        /\ residentBytes = 0
        /\ decodedRecoveryFields = {}
        /\ validatedRecoveryFields = {}

OneHeadPerSource ==
    Cardinality(ActiveSources(positions)) <= MaxSources

NewestVersionWins ==
    /\ IsPrefix(processedKeys, ExpectedProcessedKeys)
    /\ IsPrefix(processedValues, ExpectedProcessedValues)

RowsStayOrderedAndVisible == IsPrefix(emittedRows, ExpectedRows)

FullSuccessMatchesOracle ==
    (readState = "succeeded" /\ ~stoppedEarly) =>
        /\ processedKeys = ExpectedProcessedKeys
        /\ processedValues = ExpectedProcessedValues
        /\ emittedRows = ExpectedRows

OverflowResolvesBeforeVisibility ==
    OverflowValue \in {processedValues[index]: index \in 1..Len(processedValues)} =>
        resolvedOverflow

OnlyCorruptionPoisons == poisoned <=> outcome = "corruption"

PinnedEpochDoesNotDrift == pinnedEpoch = 4 /\ currentEpoch >= pinnedEpoch

RecoveryDecodeIsProjected == decodedRecoveryFields \subseteq RequestedFields

RecoveryDecodeValidatesFullRow ==
    decodedRecoveryFields # {} => validatedRecoveryFields = AllFields

=============================================================================
