---------------- MODULE HawDBKnowledgeRetrievalPipeline ----------------
EXTENDS Integers, Naturals

(***************************************************************************)
(* A bounded knowledge retrieval reads one pinned canonical graph snapshot, *)
(* filters search identities before graph expansion, reranks compact IDs,   *)
(* applies TopK, and only then hydrates canonical output. Budget rejection  *)
(* fails the whole request and never returns a partial hydrated result.      *)
(***************************************************************************)

CONSTANT MaxCandidates, MaxFanout, TopKLimit, QueryBudget, ResultBudget,
         PayloadBytes

ASSUME /\ MaxCandidates \in Nat \ {0}
       /\ MaxFanout \in Nat \ {0}
       /\ TopKLimit \in 1..MaxCandidates
       /\ QueryBudget \in Nat \ {0}
       /\ ResultBudget \in Nat \ {0}
       /\ PayloadBytes \in Nat \ {0}

Stages == 0..6
Statuses == {"running", "succeeded", "failed"}
SnapshotEpoch == 1

Min(left, right) == IF left < right THEN left ELSE right

VARIABLES
    stage,
    status,
    searchEpoch,
    graphEpoch,
    candidateCount,
    filteredCount,
    expandedCount,
    rankedCount,
    topKCount,
    hydratedCount,
    graphExpansionAuthorized,
    workingBytes,
    resultBytes

vars == <<
    stage,
    status,
    searchEpoch,
    graphEpoch,
    candidateCount,
    filteredCount,
    expandedCount,
    rankedCount,
    topKCount,
    hydratedCount,
    graphExpansionAuthorized,
    workingBytes,
    resultBytes
>>

Init ==
    /\ stage = 0
    /\ status = "running"
    /\ searchEpoch = SnapshotEpoch
    /\ graphEpoch = SnapshotEpoch
    /\ candidateCount = 0
    /\ filteredCount = 0
    /\ expandedCount = 0
    /\ rankedCount = 0
    /\ topKCount = 0
    /\ hydratedCount = 0
    /\ graphExpansionAuthorized = FALSE
    /\ workingBytes = 0
    /\ resultBytes = 0

GenerateCandidates(count) ==
    /\ status = "running"
    /\ stage = 0
    /\ count \in 0..MaxCandidates
    /\ count <= QueryBudget
    /\ candidateCount' = count
    /\ workingBytes' = count
    /\ stage' = 1
    /\ UNCHANGED <<
        status, searchEpoch, graphEpoch, filteredCount, expandedCount,
        rankedCount, topKCount, hydratedCount, graphExpansionAuthorized,
        resultBytes
       >>

RejectCandidateMemory(count) ==
    /\ status = "running"
    /\ stage = 0
    /\ count \in 0..MaxCandidates
    /\ count > QueryBudget
    /\ status' = "failed"
    /\ UNCHANGED <<
        stage, searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, rankedCount, topKCount, hydratedCount,
        graphExpansionAuthorized, workingBytes, resultBytes
       >>

ApplyMetadataFilter(kept) ==
    /\ status = "running"
    /\ stage = 1
    /\ kept \in 0..candidateCount
    /\ candidateCount + kept <= QueryBudget
    /\ filteredCount' = kept
    /\ workingBytes' = candidateCount + kept
    /\ stage' = 2
    /\ UNCHANGED <<
        status, searchEpoch, graphEpoch, candidateCount, expandedCount,
        rankedCount, topKCount, hydratedCount, graphExpansionAuthorized,
        resultBytes
       >>

RejectFilterMemory(kept) ==
    /\ status = "running"
    /\ stage = 1
    /\ kept \in 0..candidateCount
    /\ candidateCount + kept > QueryBudget
    /\ status' = "failed"
    /\ UNCHANGED <<
        stage, searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, rankedCount, topKCount, hydratedCount,
        graphExpansionAuthorized, workingBytes, resultBytes
       >>

ExpandAuthorizedGraph(edges) ==
    /\ status = "running"
    /\ stage = 2
    /\ edges \in 0..(filteredCount * MaxFanout)
    /\ workingBytes + edges <= QueryBudget
    /\ expandedCount' = edges
    /\ graphExpansionAuthorized' = TRUE
    /\ workingBytes' = workingBytes + edges
    /\ stage' = 3
    /\ UNCHANGED <<
        status, searchEpoch, graphEpoch, candidateCount, filteredCount,
        rankedCount, topKCount, hydratedCount, resultBytes
       >>

RejectExpansionMemory(edges) ==
    /\ status = "running"
    /\ stage = 2
    /\ edges \in 0..(filteredCount * MaxFanout)
    /\ workingBytes + edges > QueryBudget
    /\ status' = "failed"
    /\ UNCHANGED <<
        stage, searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, rankedCount, topKCount, hydratedCount,
        graphExpansionAuthorized, workingBytes, resultBytes
       >>

Rerank ==
    /\ status = "running"
    /\ stage = 3
    /\ workingBytes + filteredCount <= QueryBudget
    /\ rankedCount' = filteredCount
    /\ workingBytes' = workingBytes + filteredCount
    /\ stage' = 4
    /\ UNCHANGED <<
        status, searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, topKCount, hydratedCount, graphExpansionAuthorized,
        resultBytes
       >>

RejectRerankMemory ==
    /\ status = "running"
    /\ stage = 3
    /\ workingBytes + filteredCount > QueryBudget
    /\ status' = "failed"
    /\ UNCHANGED <<
        stage, searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, rankedCount, topKCount, hydratedCount,
        graphExpansionAuthorized, workingBytes, resultBytes
       >>

ApplyTopK ==
    /\ status = "running"
    /\ stage = 4
    /\ topKCount' = Min(rankedCount, TopKLimit)
    /\ workingBytes' = Min(rankedCount, TopKLimit)
    /\ stage' = 5
    /\ UNCHANGED <<
        status, searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, rankedCount, hydratedCount, graphExpansionAuthorized,
        resultBytes
       >>

HydrateCanonicalOutput ==
    /\ status = "running"
    /\ stage = 5
    /\ topKCount * PayloadBytes <= ResultBudget
    /\ hydratedCount' = topKCount
    /\ resultBytes' = topKCount * PayloadBytes
    /\ workingBytes' = 0
    /\ stage' = 6
    /\ status' = "succeeded"
    /\ UNCHANGED <<
        searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, rankedCount, topKCount, graphExpansionAuthorized
       >>

RejectResultPayload ==
    /\ status = "running"
    /\ stage = 5
    /\ topKCount * PayloadBytes > ResultBudget
    /\ status' = "failed"
    /\ workingBytes' = 0
    /\ UNCHANGED <<
        stage, searchEpoch, graphEpoch, candidateCount, filteredCount,
        expandedCount, rankedCount, topKCount, hydratedCount,
        graphExpansionAuthorized, resultBytes
       >>

Next ==
    \/ \E count \in 0..MaxCandidates: GenerateCandidates(count)
    \/ \E count \in 0..MaxCandidates: RejectCandidateMemory(count)
    \/ \E kept \in 0..MaxCandidates: ApplyMetadataFilter(kept)
    \/ \E kept \in 0..MaxCandidates: RejectFilterMemory(kept)
    \/ \E edges \in 0..(MaxCandidates * MaxFanout): ExpandAuthorizedGraph(edges)
    \/ \E edges \in 0..(MaxCandidates * MaxFanout): RejectExpansionMemory(edges)
    \/ Rerank
    \/ RejectRerankMemory
    \/ ApplyTopK
    \/ HydrateCanonicalOutput
    \/ RejectResultPayload

TypeOK ==
    /\ stage \in Stages
    /\ status \in Statuses
    /\ searchEpoch \in 0..SnapshotEpoch
    /\ graphEpoch \in 0..SnapshotEpoch
    /\ candidateCount \in 0..MaxCandidates
    /\ filteredCount \in 0..MaxCandidates
    /\ expandedCount \in 0..(MaxCandidates * MaxFanout)
    /\ rankedCount \in 0..MaxCandidates
    /\ topKCount \in 0..TopKLimit
    /\ hydratedCount \in 0..TopKLimit
    /\ graphExpansionAuthorized \in BOOLEAN
    /\ workingBytes \in Nat
    /\ resultBytes \in Nat

PinnedSnapshot == searchEpoch = graphEpoch

WorkingMemoryBounded == workingBytes <= QueryBudget

ResultPayloadBounded == resultBytes <= ResultBudget

GraphExpansionIsAuthorized == expandedCount > 0 => graphExpansionAuthorized

CanonicalHydrationFollowsTopK == hydratedCount > 0 => stage = 6

CanonicalHydrationContainsOnlyTopK == hydratedCount = 0 \/ hydratedCount = topKCount

FailureReturnsNoPartialResult == status = "failed" => hydratedCount = 0

SuccessCompletesEveryStage == status = "succeeded" => stage = 6

Spec == Init /\ [][Next]_vars

=============================================================================
