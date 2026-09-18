---------------- MODULE HawDBCompositePropertyProjection ----------------
EXTENDS FiniteSets, Integers

(***************************************************************************)
(* A checkpoint-bound composite-property projection supplies provisional   *)
(* base candidates for one ordered tuple or one equality-prefix plus the    *)
(* immediately following range. Canonical rows validate those candidates,   *)
(* while the post-checkpoint COW/WAL overlay shadows changed base rows and   *)
(* contributes current matches. Corrupt selected blocks fail the lookup and *)
(* poison the handle instead of falling back.                               *)
(***************************************************************************)

BaseNodes == {"base-match"}
DeltaNodes == {"delta-insert"}
Nodes == BaseNodes \union DeltaNodes
ProjectionCandidates == BaseNodes
QueryStates == {"idle", "reading-base", "reading-overlay", "succeeded", "failed"}

(***************************************************************************)
(* A finite ordered-key witness for persistent composite prefix-range      *)
(* pruning. Prefix 1 is bound by equality and the following integer is     *)
(* constrained to [12, 25). Blocks 1 and 3 are outside that interval.      *)
(***************************************************************************)

CompositeKeys == {<<0, 100>>, <<1, 5>>, <<1, 10>>, <<1, 20>>, <<1, 30>>, <<2, 15>>}
Blocks == {1, 2, 3}
BlockKeys ==
    [block \in Blocks |->
        CASE block = 1 -> {<<0, 100>>, <<1, 5>>, <<1, 10>>}
          [] block = 2 -> {<<1, 20>>, <<1, 30>>}
          [] OTHER -> {<<2, 15>>}]
BlockMin == [block \in Blocks |->
    CASE block = 1 -> <<0, 100>>
      [] block = 2 -> <<1, 20>>
      [] OTHER -> <<2, 15>>]
BlockMax == [block \in Blocks |->
    CASE block = 1 -> <<1, 10>>
      [] block = 2 -> <<1, 30>>
      [] OTHER -> <<2, 15>>]
EqualityPrefix == 1
LowerBound == 12
UpperBound == 25

CompositeRangeMatches(key) ==
    /\ key[1] = EqualityPrefix
    /\ key[2] >= LowerBound
    /\ key[2] < UpperBound

RangeBlockMightMatch(block) ==
    /\ BlockMax[block][1] >= EqualityPrefix
    /\ BlockMin[block][1] <= EqualityPrefix
    /\ (BlockMax[block][1] # EqualityPrefix \/ BlockMax[block][2] >= LowerBound)
    /\ (BlockMin[block][1] # EqualityPrefix \/ BlockMin[block][2] < UpperBound)

FullScanRangeResult == {key \in CompositeKeys: CompositeRangeMatches(key)}
ProjectionRangeResult ==
    {key \in UNION {
        IF RangeBlockMightMatch(block) THEN BlockKeys[block] ELSE {}:
            block \in Blocks
    }: CompositeRangeMatches(key)}

VARIABLES
    overlayShadowed,
    overlayMatches,
    queryState,
    baseResult,
    visibleResult,
    selectedBlockLoaded,
    selectedBlockCorrupt,
    poisoned

vars == <<
    overlayShadowed,
    overlayMatches,
    queryState,
    baseResult,
    visibleResult,
    selectedBlockLoaded,
    selectedBlockCorrupt,
    poisoned
>>

CanonicalMatches == (BaseNodes \ overlayShadowed) \union overlayMatches

Init ==
    /\ overlayShadowed = {}
    /\ overlayMatches = {}
    /\ queryState = "idle"
    /\ baseResult = {}
    /\ visibleResult = {}
    /\ selectedBlockLoaded = FALSE
    /\ selectedBlockCorrupt = FALSE
    /\ poisoned = FALSE

UpdateBaseAway ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayShadowed' = overlayShadowed \union BaseNodes
    /\ overlayMatches' = overlayMatches \ BaseNodes
    /\ UNCHANGED <<
        queryState, baseResult, visibleResult, selectedBlockLoaded,
        selectedBlockCorrupt, poisoned
       >>

UpdateBaseToMatch ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayShadowed' = overlayShadowed \union BaseNodes
    /\ overlayMatches' = overlayMatches \union BaseNodes
    /\ UNCHANGED <<
        queryState, baseResult, visibleResult, selectedBlockLoaded,
        selectedBlockCorrupt, poisoned
       >>

InsertMatchingDelta ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayMatches' = overlayMatches \union DeltaNodes
    /\ UNCHANGED <<
        overlayShadowed, queryState, baseResult, visibleResult,
        selectedBlockLoaded, selectedBlockCorrupt, poisoned
       >>

DeleteMatchingDelta ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayMatches' = overlayMatches \ DeltaNodes
    /\ UNCHANGED <<
        overlayShadowed, queryState, baseResult, visibleResult,
        selectedBlockLoaded, selectedBlockCorrupt, poisoned
       >>

CorruptSelectedBlock ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ selectedBlockCorrupt' = TRUE
    /\ UNCHANGED <<
        overlayShadowed, overlayMatches, queryState, baseResult,
        visibleResult, selectedBlockLoaded, poisoned
       >>

BeginLookup ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ queryState' = "reading-base"
    /\ baseResult' = {}
    /\ visibleResult' = {}
    /\ UNCHANGED <<
        overlayShadowed, overlayMatches, selectedBlockLoaded,
        selectedBlockCorrupt, poisoned
       >>

ReadVerifiedBaseCandidates ==
    /\ queryState = "reading-base"
    /\ ~selectedBlockCorrupt
    /\ queryState' = "reading-overlay"
    /\ selectedBlockLoaded' = TRUE
    /\ baseResult' = ProjectionCandidates \ overlayShadowed
    /\ UNCHANGED <<
        overlayShadowed, overlayMatches, visibleResult,
        selectedBlockCorrupt, poisoned
       >>

RejectCorruptSelectedBlock ==
    /\ queryState = "reading-base"
    /\ selectedBlockCorrupt
    /\ queryState' = "failed"
    /\ selectedBlockLoaded' = TRUE
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        overlayShadowed, overlayMatches, baseResult, visibleResult,
        selectedBlockCorrupt
       >>

MergeOverlay ==
    /\ queryState = "reading-overlay"
    /\ queryState' = "succeeded"
    /\ visibleResult' = baseResult \union overlayMatches
    /\ UNCHANGED <<
        overlayShadowed, overlayMatches, baseResult, selectedBlockLoaded,
        selectedBlockCorrupt, poisoned
       >>

Next ==
    \/ UpdateBaseAway
    \/ UpdateBaseToMatch
    \/ InsertMatchingDelta
    \/ DeleteMatchingDelta
    \/ CorruptSelectedBlock
    \/ BeginLookup
    \/ ReadVerifiedBaseCandidates
    \/ RejectCorruptSelectedBlock
    \/ MergeOverlay

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ overlayShadowed \subseteq BaseNodes
    /\ overlayMatches \subseteq Nodes
    /\ queryState \in QueryStates
    /\ baseResult \subseteq BaseNodes
    /\ visibleResult \subseteq Nodes
    /\ selectedBlockLoaded \in BOOLEAN
    /\ selectedBlockCorrupt \in BOOLEAN
    /\ poisoned \in BOOLEAN

ColdOpenDoesNotLoadSelectedBlocks ==
    queryState = "idle" => ~selectedBlockLoaded

SuccessfulLookupIsCanonical ==
    queryState = "succeeded" => visibleResult = CanonicalMatches

CorruptionNeverSucceeds ==
    selectedBlockCorrupt /\ selectedBlockLoaded => queryState # "succeeded"

FailedLookupPoisonsHandle ==
    queryState = "failed" => poisoned

PoisonedHandleCannotStartLookup ==
    poisoned => queryState # "idle"

CompositeRangeBlockPruningIsSound ==
    \A block \in Blocks:
        \A key \in BlockKeys[block]:
            CompositeRangeMatches(key) => RangeBlockMightMatch(block)

CompositeRangeProjectionMatchesFullScan ==
    ProjectionRangeResult = FullScanRangeResult

=============================================================================
