---------------- MODULE HawDBStableIdentityPublication ----------------
EXTENDS FiniteSets, Naturals

(***************************************************************************)
(* The physical-id to stable-identity mapping is an independently published *)
(* export/import artifact, not a canonical query index. Fixed-size pages     *)
(* first become durable in an immutable generation artifact. A bounded       *)
(* selector is published last and names exactly one complete generation.     *)
(* Readers pin the selected generation, and reclamation can remove only an   *)
(* unselected, unpinned artifact. Corruption of a selected page fails closed. *)
(***************************************************************************)

CONSTANTS Pages, Epochs, Generations

ReaderStates == {
    "closed", "open", "reading", "lookup_succeeded", "scrubbing",
    "scrub_succeeded", "failed"
}
PageCopies == Generations \X Pages

VARIABLES
    selectedGeneration,
    generationCoverage,
    completeGenerations,
    retainedGenerations,
    candidateGeneration,
    candidateCoverage,
    candidatePages,
    candidateDurable,
    graphWalEpoch,
    graphWalMappingGeneration,
    graphVisibleEpoch,
    graphVisibleMappingGeneration,
    readerGeneration,
    readerState,
    selectedPage,
    scrubbedPages,
    corruptCopies,
    poisoned

vars == <<
    selectedGeneration,
    generationCoverage,
    completeGenerations,
    retainedGenerations,
    candidateGeneration,
    candidateCoverage,
    candidatePages,
    candidateDurable,
    graphWalEpoch,
    graphWalMappingGeneration,
    graphVisibleEpoch,
    graphVisibleMappingGeneration,
    readerGeneration,
    readerState,
    selectedPage,
    scrubbedPages,
    corruptCopies,
    poisoned
>>

Init ==
    /\ selectedGeneration = 0
    /\ generationCoverage = [generation \in Generations |-> 0]
    /\ completeGenerations = {}
    /\ retainedGenerations = {}
    /\ candidateGeneration = 0
    /\ candidateCoverage = 0
    /\ candidatePages = {}
    /\ candidateDurable = FALSE
    /\ graphWalEpoch = 0
    /\ graphWalMappingGeneration = 0
    /\ graphVisibleEpoch = 0
    /\ graphVisibleMappingGeneration = 0
    /\ readerGeneration = 0
    /\ readerState = "closed"
    /\ selectedPage = 0
    /\ scrubbedPages = {}
    /\ corruptCopies = {}
    /\ poisoned = FALSE

StartCandidate ==
    /\ candidateGeneration = 0
    /\ {generation \in Generations \ retainedGenerations:
            generation > selectedGeneration} # {}
    /\ candidateGeneration' \in
          {generation \in Generations \ retainedGenerations:
              generation > selectedGeneration}
    /\ candidateCoverage' \in Epochs
    /\ candidatePages' = {}
    /\ candidateDurable' = FALSE
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, graphWalEpoch, graphWalMappingGeneration,
        graphVisibleEpoch, graphVisibleMappingGeneration, readerGeneration,
        readerState, selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

WriteCandidatePage ==
    /\ candidateGeneration \in Generations
    /\ ~candidateDurable
    /\ candidatePages # Pages
    /\ \E page \in Pages \ candidatePages:
          candidatePages' = candidatePages \union {page}
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidateDurable, graphWalEpoch, graphWalMappingGeneration,
        graphVisibleEpoch, graphVisibleMappingGeneration, readerGeneration,
        readerState, selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

CompleteCandidateArtifact ==
    /\ candidateGeneration \in Generations
    /\ ~candidateDurable
    /\ candidatePages = Pages
    /\ generationCoverage' =
          [generationCoverage EXCEPT ![candidateGeneration] = candidateCoverage]
    /\ completeGenerations' = completeGenerations \union {candidateGeneration}
    /\ retainedGenerations' = retainedGenerations \union {candidateGeneration}
    /\ candidateDurable' = TRUE
    /\ UNCHANGED <<
        selectedGeneration, candidateGeneration, candidateCoverage,
        candidatePages, graphWalEpoch, graphWalMappingGeneration,
        graphVisibleEpoch, graphVisibleMappingGeneration, readerGeneration,
        readerState, selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

PublishCandidateSelector ==
    /\ candidateGeneration \in Generations
    /\ candidateDurable
    /\ candidateGeneration \in retainedGenerations
    /\ selectedGeneration' = candidateGeneration
    /\ candidateGeneration' = 0
    /\ candidateCoverage' = 0
    /\ candidatePages' = {}
    /\ candidateDurable' = FALSE
    /\ UNCHANGED <<
        generationCoverage, completeGenerations, retainedGenerations,
        graphWalEpoch, graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

CrashCandidate ==
    /\ candidateGeneration \in Generations
    /\ candidateGeneration' = 0
    /\ candidateCoverage' = 0
    /\ candidatePages' = {}
    /\ candidateDurable' = FALSE
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, graphWalEpoch, graphWalMappingGeneration,
        graphVisibleEpoch, graphVisibleMappingGeneration, readerGeneration,
        readerState, selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

ReclaimUnselectedGeneration ==
    /\ \E generation \in retainedGenerations:
          /\ generation # selectedGeneration
          /\ generation # candidateGeneration
          /\ generation # readerGeneration
          /\ retainedGenerations' = retainedGenerations \ {generation}
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        candidateGeneration, candidateCoverage, candidatePages,
        candidateDurable, graphWalEpoch, graphWalMappingGeneration,
        graphVisibleEpoch, graphVisibleMappingGeneration, readerGeneration,
        readerState, selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

AppendInitialImportWal ==
    /\ graphWalEpoch = 0
    /\ selectedGeneration \in completeGenerations
    /\ generationCoverage[selectedGeneration] \in Epochs
    /\ graphWalEpoch' = generationCoverage[selectedGeneration]
    /\ graphWalMappingGeneration' = selectedGeneration
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

PublishImportedGraph ==
    /\ graphWalEpoch \in Epochs
    /\ graphVisibleEpoch = 0
    /\ graphVisibleEpoch' = graphWalEpoch
    /\ graphVisibleMappingGeneration' = graphWalMappingGeneration
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, corruptCopies, poisoned
       >>

OpenPinnedReader ==
    /\ readerState = "closed"
    /\ ~poisoned
    /\ selectedGeneration \in completeGenerations
    /\ selectedGeneration \in retainedGenerations
    /\ readerGeneration' = selectedGeneration
    /\ readerState' = "open"
    /\ selectedPage' = 0
    /\ scrubbedPages' = {}
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, corruptCopies, poisoned
       >>

BeginDemandLookup ==
    /\ readerState = "open"
    /\ ~poisoned
    /\ selectedPage' \in Pages
    /\ readerState' = "reading"
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, scrubbedPages,
        corruptCopies, poisoned
       >>

ReadSelectedPage ==
    /\ readerState = "reading"
    /\ <<readerGeneration, selectedPage>> \notin corruptCopies
    /\ readerState' = "lookup_succeeded"
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies, poisoned
       >>

RejectCorruptSelectedPage ==
    /\ readerState = "reading"
    /\ <<readerGeneration, selectedPage>> \in corruptCopies
    /\ readerState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies
       >>

BeginDeepScrub ==
    /\ readerState = "open"
    /\ ~poisoned
    /\ readerState' = "scrubbing"
    /\ scrubbedPages' = {}
    /\ selectedPage' = 0
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, corruptCopies,
        poisoned
       >>

ScrubCleanPage ==
    /\ readerState = "scrubbing"
    /\ \E page \in Pages \ scrubbedPages:
          /\ <<readerGeneration, page>> \notin corruptCopies
          /\ scrubbedPages' = scrubbedPages \union {page}
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, corruptCopies, poisoned
       >>

RejectCorruptScrubPage ==
    /\ readerState = "scrubbing"
    /\ \E page \in Pages \ scrubbedPages:
          <<readerGeneration, page>> \in corruptCopies
    /\ readerState' = "failed"
    /\ poisoned' = TRUE
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies
       >>

CompleteDeepScrub ==
    /\ readerState = "scrubbing"
    /\ scrubbedPages = Pages
    /\ readerState' = "scrub_succeeded"
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, selectedPage,
        scrubbedPages, corruptCopies, poisoned
       >>

CorruptSelectedPage ==
    /\ selectedGeneration \in retainedGenerations
    /\ \E page \in Pages:
          corruptCopies' = corruptCopies \union {<<selectedGeneration, page>>}
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, readerGeneration, readerState,
        selectedPage, scrubbedPages, poisoned
       >>

CloseReader ==
    /\ readerState \in {"open", "lookup_succeeded", "scrub_succeeded", "failed"}
    /\ readerState' = "closed"
    /\ readerGeneration' = 0
    /\ selectedPage' = 0
    /\ scrubbedPages' = {}
    /\ UNCHANGED <<
        selectedGeneration, generationCoverage, completeGenerations,
        retainedGenerations, candidateGeneration, candidateCoverage,
        candidatePages, candidateDurable, graphWalEpoch,
        graphWalMappingGeneration, graphVisibleEpoch,
        graphVisibleMappingGeneration, corruptCopies, poisoned
       >>

Next ==
    \/ StartCandidate
    \/ WriteCandidatePage
    \/ CompleteCandidateArtifact
    \/ PublishCandidateSelector
    \/ CrashCandidate
    \/ ReclaimUnselectedGeneration
    \/ AppendInitialImportWal
    \/ PublishImportedGraph
    \/ OpenPinnedReader
    \/ BeginDemandLookup
    \/ ReadSelectedPage
    \/ RejectCorruptSelectedPage
    \/ BeginDeepScrub
    \/ ScrubCleanPage
    \/ RejectCorruptScrubPage
    \/ CompleteDeepScrub
    \/ CorruptSelectedPage
    \/ CloseReader

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ selectedGeneration \in {0} \union Generations
    /\ generationCoverage \in [Generations -> {0} \union Epochs]
    /\ completeGenerations \subseteq Generations
    /\ retainedGenerations \subseteq completeGenerations
    /\ candidateGeneration \in {0} \union Generations
    /\ candidateCoverage \in {0} \union Epochs
    /\ candidatePages \subseteq Pages
    /\ candidateDurable \in BOOLEAN
    /\ graphWalEpoch \in {0} \union Epochs
    /\ graphWalMappingGeneration \in {0} \union Generations
    /\ graphVisibleEpoch \in {0} \union Epochs
    /\ graphVisibleMappingGeneration \in {0} \union Generations
    /\ readerGeneration \in {0} \union Generations
    /\ readerState \in ReaderStates
    /\ selectedPage \in {0} \union Pages
    /\ scrubbedPages \subseteq Pages
    /\ corruptCopies \subseteq PageCopies
    /\ poisoned \in BOOLEAN

SelectorSelectsCompleteGeneration ==
    selectedGeneration # 0 => selectedGeneration \in completeGenerations

SelectorRetainsSelectedGeneration ==
    selectedGeneration # 0 => selectedGeneration \in retainedGenerations

DurableCandidatePrecedesSelector ==
    candidateDurable =>
        /\ candidateGeneration \in completeGenerations
        /\ candidateGeneration \in retainedGenerations
        /\ candidatePages = Pages

WalNeverLeadsStableIdentity ==
    graphWalEpoch # 0 =>
        /\ graphWalMappingGeneration \in completeGenerations
        /\ generationCoverage[graphWalMappingGeneration] = graphWalEpoch

VisibleGraphNeverLeadsStableIdentity ==
    graphVisibleEpoch # 0 =>
        /\ graphVisibleMappingGeneration \in completeGenerations
        /\ generationCoverage[graphVisibleMappingGeneration] = graphVisibleEpoch

PinnedReaderUsesRetainedCompleteGeneration ==
    readerState # "closed" =>
        /\ readerGeneration \in completeGenerations
        /\ readerGeneration \in retainedGenerations

SuccessfulLookupHasSelectedPage ==
    readerState = "lookup_succeeded" => selectedPage \in Pages

SuccessfulScrubVisitedEveryPage ==
    readerState = "scrub_succeeded" => scrubbedPages = Pages

FailedReadPoisonsReader ==
    readerState = "failed" => poisoned

PoisonedReaderCannotBeReading ==
    poisoned => readerState \notin {"reading", "scrubbing"}

=============================================================================
