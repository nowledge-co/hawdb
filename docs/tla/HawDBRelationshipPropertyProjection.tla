---------------- MODULE HawDBRelationshipPropertyProjection ----------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* A relationship-property projection is selected only when it is bound to *)
(* the canonical generation and its estimated global posting work does not *)
(* exceed the bound endpoint adjacency work. Both paths verify canonical    *)
(* endpoint and predicate state, then merge the post-checkpoint COW/WAL      *)
(* overlay. Corruption fails closed only after selecting the projection.     *)
(***************************************************************************)

BaseRelationships == {"base-match", "base-other", "wrong-endpoint"}
DeltaRelationships == {"delta-match"}
Relationships == BaseRelationships \union DeltaRelationships
BaseCanonicalMatches == {"base-match"}
ProjectionCandidates == {"base-match", "wrong-endpoint"}
QueryStates == {"idle", "reading-property", "reading-adjacency",
                "reading-overlay", "succeeded", "failed"}
Paths == {"none", "property", "adjacency"}
Generations == 1..3
Estimates == 1..2

VARIABLES
    canonicalGeneration,
    projectionGeneration,
    projectionEstimate,
    adjacencyEstimate,
    overlayShadowed,
    overlayMatches,
    queryState,
    selectedPath,
    baseResult,
    visibleResult,
    selectedBlockLoaded,
    selectedBlockCorrupt,
    poisoned

vars == <<
    canonicalGeneration,
    projectionGeneration,
    projectionEstimate,
    adjacencyEstimate,
    overlayShadowed,
    overlayMatches,
    queryState,
    selectedPath,
    baseResult,
    visibleResult,
    selectedBlockLoaded,
    selectedBlockCorrupt,
    poisoned
>>

ProjectionCurrent == projectionGeneration = canonicalGeneration
ProjectionPreferred ==
    ProjectionCurrent /\ projectionEstimate <= adjacencyEstimate
CanonicalMatches ==
    (BaseCanonicalMatches \ overlayShadowed) \union overlayMatches

Init ==
    /\ canonicalGeneration = 1
    /\ projectionGeneration \in 0..1
    /\ projectionEstimate \in Estimates
    /\ adjacencyEstimate \in Estimates
    /\ overlayShadowed = {}
    /\ overlayMatches = {}
    /\ queryState = "idle"
    /\ selectedPath = "none"
    /\ baseResult = {}
    /\ visibleResult = {}
    /\ selectedBlockLoaded = FALSE
    /\ selectedBlockCorrupt = FALSE
    /\ poisoned = FALSE

PublishCurrentProjection ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ projectionGeneration' = canonicalGeneration
    /\ selectedBlockCorrupt' = FALSE
    /\ UNCHANGED <<
        canonicalGeneration, projectionEstimate, adjacencyEstimate,
        overlayShadowed, overlayMatches, queryState, selectedPath,
        baseResult, visibleResult, selectedBlockLoaded, poisoned
       >>

PublishCheckpointWithoutProjection ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ canonicalGeneration < 3
    /\ canonicalGeneration' = canonicalGeneration + 1
    /\ UNCHANGED <<
        projectionGeneration, projectionEstimate, adjacencyEstimate,
        overlayShadowed, overlayMatches, queryState, selectedPath,
        baseResult, visibleResult, selectedBlockLoaded,
        selectedBlockCorrupt, poisoned
       >>

UpdateBaseAway ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayShadowed' = overlayShadowed \union {"base-match"}
    /\ overlayMatches' = overlayMatches \ {"base-match"}
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, queryState, selectedPath, baseResult,
        visibleResult, selectedBlockLoaded, selectedBlockCorrupt, poisoned
       >>

UpdateBaseToMatch ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayShadowed' = overlayShadowed \union {"base-other"}
    /\ overlayMatches' = overlayMatches \union {"base-other"}
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, queryState, selectedPath, baseResult,
        visibleResult, selectedBlockLoaded, selectedBlockCorrupt, poisoned
       >>

InsertMatchingDelta ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayMatches' = overlayMatches \union DeltaRelationships
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, queryState, selectedPath,
        baseResult, visibleResult, selectedBlockLoaded,
        selectedBlockCorrupt, poisoned
       >>

DeleteMatchingDelta ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ overlayMatches' = overlayMatches \ DeltaRelationships
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, queryState, selectedPath,
        baseResult, visibleResult, selectedBlockLoaded,
        selectedBlockCorrupt, poisoned
       >>

CorruptSelectedBlock ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ selectedBlockCorrupt' = TRUE
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, overlayMatches, queryState,
        selectedPath, baseResult, visibleResult, selectedBlockLoaded, poisoned
       >>

BeginLookup ==
    /\ queryState = "idle"
    /\ ~poisoned
    /\ IF ProjectionPreferred
          THEN /\ queryState' = "reading-property"
               /\ selectedPath' = "property"
          ELSE /\ queryState' = "reading-adjacency"
               /\ selectedPath' = "adjacency"
    /\ baseResult' = {}
    /\ visibleResult' = {}
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, overlayMatches,
        selectedBlockLoaded, selectedBlockCorrupt, poisoned
       >>

ReadVerifiedPropertyCandidates ==
    /\ queryState = "reading-property"
    /\ ~selectedBlockCorrupt
    /\ queryState' = "reading-overlay"
    /\ selectedBlockLoaded' = TRUE
    /\ baseResult' =
          (ProjectionCandidates \intersect BaseCanonicalMatches) \ overlayShadowed
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, overlayMatches, selectedPath,
        visibleResult, selectedBlockCorrupt, poisoned
       >>

RejectCorruptSelectedBlock ==
    /\ queryState = "reading-property"
    /\ selectedBlockCorrupt
    /\ queryState' = "failed"
    /\ selectedBlockLoaded' = TRUE
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, overlayMatches, selectedPath,
        baseResult, visibleResult, selectedBlockCorrupt
       >>

ReadVerifiedAdjacency ==
    /\ queryState = "reading-adjacency"
    /\ queryState' = "reading-overlay"
    /\ baseResult' = BaseCanonicalMatches \ overlayShadowed
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, overlayMatches, selectedPath,
        visibleResult, selectedBlockLoaded, selectedBlockCorrupt, poisoned
       >>

MergeOverlay ==
    /\ queryState = "reading-overlay"
    /\ queryState' = "succeeded"
    /\ visibleResult' = baseResult \union overlayMatches
    /\ UNCHANGED <<
        canonicalGeneration, projectionGeneration, projectionEstimate,
        adjacencyEstimate, overlayShadowed, overlayMatches, selectedPath,
        baseResult, selectedBlockLoaded, selectedBlockCorrupt, poisoned
       >>

Next ==
    \/ PublishCurrentProjection
    \/ PublishCheckpointWithoutProjection
    \/ UpdateBaseAway
    \/ UpdateBaseToMatch
    \/ InsertMatchingDelta
    \/ DeleteMatchingDelta
    \/ CorruptSelectedBlock
    \/ BeginLookup
    \/ ReadVerifiedPropertyCandidates
    \/ RejectCorruptSelectedBlock
    \/ ReadVerifiedAdjacency
    \/ MergeOverlay

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalGeneration \in Generations
    /\ projectionGeneration \in 0..3
    /\ projectionEstimate \in Estimates
    /\ adjacencyEstimate \in Estimates
    /\ overlayShadowed \subseteq BaseRelationships
    /\ overlayMatches \subseteq Relationships
    /\ queryState \in QueryStates
    /\ selectedPath \in Paths
    /\ baseResult \subseteq BaseRelationships
    /\ visibleResult \subseteq Relationships
    /\ selectedBlockLoaded \in BOOLEAN
    /\ selectedBlockCorrupt \in BOOLEAN
    /\ poisoned \in BOOLEAN

PropertySelectionIsCurrentAndCheaper ==
    selectedPath = "property" => ProjectionPreferred

AdjacencyFallbackStaysCold ==
    selectedPath = "adjacency" => ~selectedBlockLoaded

SuccessfulLookupIsCanonical ==
    queryState = "succeeded" => visibleResult = CanonicalMatches

CorruptSelectedProjectionNeverSucceeds ==
    selectedPath = "property" /\ selectedBlockCorrupt /\ selectedBlockLoaded
        => queryState # "succeeded"

FailedLookupPoisonsHandle ==
    queryState = "failed" => poisoned

PoisonedHandleCannotStartLookup ==
    poisoned => queryState # "idle"

=============================================================================
