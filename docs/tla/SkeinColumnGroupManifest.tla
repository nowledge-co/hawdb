-------------------- MODULE SkeinColumnGroupManifest --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Layered column-group metadata publication. Immutable table directories   *)
(* and their referenced artifacts become durable before one active manifest *)
(* atomically selects a complete catalog generation. A candidate carries    *)
(* the generation it was prepared from; publication is a compare-and-swap   *)
(* under the filesystem publish lease.                                      *)
(*                                                                           *)
(* Readers open only the active manifest. Orphan candidates and durable      *)
(* artifacts are never discovered by scanning the directory. A crash drops  *)
(* volatile preparation and open readers, preserving the prior published    *)
(* manifest and all immutable durable files.                                 *)
(***************************************************************************)

CONSTANT Tables, Readers, MaxGeneration

ASSUME /\ Tables # {}
       /\ Readers # {}
       /\ MaxGeneration \in Nat \ {0}

Generations == 0..MaxGeneration
ReaderGenerations == (-1)..MaxGeneration

EmptyCatalog == [table \in Tables |-> 0]

VARIABLES
    activeGeneration,
    activeCatalog,
    publishedCatalogs,
    durableArtifacts,
    durableDirectories,
    candidateParent,
    candidateGeneration,
    candidateCatalog,
    candidateState,
    readerGeneration,
    readerCatalog

vars == <<
    activeGeneration,
    activeCatalog,
    publishedCatalogs,
    durableArtifacts,
    durableDirectories,
    candidateParent,
    candidateGeneration,
    candidateCatalog,
    candidateState,
    readerGeneration,
    readerCatalog
>>

CandidateStates == {"none", "artifacts", "directories", "manifest"}

Init ==
    /\ activeGeneration = 0
    /\ activeCatalog = EmptyCatalog
    /\ publishedCatalogs = {[generation |-> 0, catalog |-> EmptyCatalog]}
    /\ durableArtifacts = {0}
    /\ durableDirectories = {0}
    /\ candidateParent = -1
    /\ candidateGeneration = 0
    /\ candidateCatalog = EmptyCatalog
    /\ candidateState = "none"
    /\ readerGeneration = [reader \in Readers |-> -1]
    /\ readerCatalog = [reader \in Readers |-> EmptyCatalog]

BeginCandidate(changed) ==
    /\ candidateState = "none"
    /\ activeGeneration < MaxGeneration
    /\ changed \in SUBSET Tables
    /\ changed # {}
    /\ candidateParent' = activeGeneration
    /\ candidateGeneration' = activeGeneration + 1
    /\ candidateCatalog' =
        [table \in Tables |->
            IF table \in changed
            THEN activeGeneration + 1
            ELSE activeCatalog[table]]
    /\ candidateState' = "artifacts"
    /\ UNCHANGED <<
        activeGeneration,
        activeCatalog,
        publishedCatalogs,
        durableArtifacts,
        durableDirectories,
        readerGeneration,
        readerCatalog
        >>

MakeArtifactsDurable ==
    /\ candidateState = "artifacts"
    /\ durableArtifacts' = durableArtifacts \cup {candidateGeneration}
    /\ candidateState' = "directories"
    /\ UNCHANGED <<
        activeGeneration,
        activeCatalog,
        publishedCatalogs,
        durableDirectories,
        candidateParent,
        candidateGeneration,
        candidateCatalog,
        readerGeneration,
        readerCatalog
        >>

MakeDirectoriesDurable ==
    /\ candidateState = "directories"
    /\ candidateGeneration \in durableArtifacts
    /\ durableDirectories' = durableDirectories \cup {candidateGeneration}
    /\ candidateState' = "manifest"
    /\ UNCHANGED <<
        activeGeneration,
        activeCatalog,
        publishedCatalogs,
        durableArtifacts,
        candidateParent,
        candidateGeneration,
        candidateCatalog,
        readerGeneration,
        readerCatalog
        >>

(***************************************************************************)
(* Another process or thread may complete its independently prepared        *)
(* candidate while this candidate is between preparation and publication.   *)
(* The publish lease serializes the filesystem replacement, but it does not  *)
(* make an already prepared parent token current. This action abstracts the   *)
(* competing candidate's artifact/directory durability and atomic manifest   *)
(* replace as one step, leaving the local candidate stale.                    *)
(***************************************************************************)
PublishRacingManifest(changed) ==
    /\ candidateState # "none"
    /\ activeGeneration < MaxGeneration
    /\ changed \in SUBSET Tables
    /\ changed # {}
    /\ LET next == activeGeneration + 1
       IN /\ activeGeneration' = next
          /\ activeCatalog' =
              [table \in Tables |->
                  IF table \in changed THEN next ELSE activeCatalog[table]]
          /\ publishedCatalogs' =
              publishedCatalogs \cup
                  {[generation |-> next,
                    catalog |->
                        [table \in Tables |->
                            IF table \in changed THEN next ELSE activeCatalog[table]]]}
          /\ durableArtifacts' = durableArtifacts \cup {next}
          /\ durableDirectories' = durableDirectories \cup {next}
    /\ UNCHANGED <<
        candidateParent,
        candidateGeneration,
        candidateCatalog,
        candidateState,
        readerGeneration,
        readerCatalog
        >>

PublishCandidate ==
    /\ candidateState = "manifest"
    /\ candidateParent = activeGeneration
    /\ candidateGeneration \in durableArtifacts
    /\ candidateGeneration \in durableDirectories
    /\ activeGeneration' = candidateGeneration
    /\ activeCatalog' = candidateCatalog
    /\ publishedCatalogs' =
        publishedCatalogs \cup
            {[generation |-> candidateGeneration, catalog |-> candidateCatalog]}
    /\ candidateParent' = -1
    /\ candidateGeneration' = 0
    /\ candidateCatalog' = EmptyCatalog
    /\ candidateState' = "none"
    /\ UNCHANGED <<
        durableArtifacts,
        durableDirectories,
        readerGeneration,
        readerCatalog
        >>

DiscardStaleCandidate ==
    /\ candidateState # "none"
    /\ candidateParent # activeGeneration
    /\ candidateParent' = -1
    /\ candidateGeneration' = 0
    /\ candidateCatalog' = EmptyCatalog
    /\ candidateState' = "none"
    /\ UNCHANGED <<
        activeGeneration,
        activeCatalog,
        publishedCatalogs,
        durableArtifacts,
        durableDirectories,
        readerGeneration,
        readerCatalog
        >>

BeginRead(reader) ==
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = activeGeneration]
    /\ readerCatalog' =
        [readerCatalog EXCEPT ![reader] = activeCatalog]
    /\ UNCHANGED <<
        activeGeneration,
        activeCatalog,
        publishedCatalogs,
        durableArtifacts,
        durableDirectories,
        candidateParent,
        candidateGeneration,
        candidateCatalog,
        candidateState
        >>

EndRead(reader) ==
    /\ readerGeneration[reader] # -1
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ readerCatalog' = [readerCatalog EXCEPT ![reader] = EmptyCatalog]
    /\ UNCHANGED <<
        activeGeneration,
        activeCatalog,
        publishedCatalogs,
        durableArtifacts,
        durableDirectories,
        candidateParent,
        candidateGeneration,
        candidateCatalog,
        candidateState
        >>

Crash ==
    /\ candidateParent' = -1
    /\ candidateGeneration' = 0
    /\ candidateCatalog' = EmptyCatalog
    /\ candidateState' = "none"
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ readerCatalog' = [reader \in Readers |-> EmptyCatalog]
    /\ UNCHANGED <<
        activeGeneration,
        activeCatalog,
        publishedCatalogs,
        durableArtifacts,
        durableDirectories
        >>

Next ==
    \/ \E changed \in SUBSET Tables: BeginCandidate(changed)
    \/ MakeArtifactsDurable
    \/ MakeDirectoriesDurable
    \/ \E changed \in SUBSET Tables: PublishRacingManifest(changed)
    \/ PublishCandidate
    \/ DiscardStaleCandidate
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Crash

TypeOK ==
    /\ activeGeneration \in Generations
    /\ activeCatalog \in [Tables -> Generations]
    /\ publishedCatalogs \subseteq
        [generation: Generations, catalog: [Tables -> Generations]]
    /\ durableArtifacts \subseteq Generations
    /\ durableDirectories \subseteq Generations
    /\ candidateParent \in (-1)..MaxGeneration
    /\ candidateGeneration \in Generations
    /\ candidateCatalog \in [Tables -> Generations]
    /\ candidateState \in CandidateStates
    /\ readerGeneration \in [Readers -> ReaderGenerations]
    /\ readerCatalog \in [Readers -> [Tables -> Generations]]

ActiveManifestReferencesDurableMetadata ==
    /\ activeGeneration \in durableArtifacts
    /\ activeGeneration \in durableDirectories

ActiveCatalogNeverLeadsManifest ==
    \A table \in Tables: activeCatalog[table] <= activeGeneration

ActiveCatalogReferencesDurableDirectories ==
    \A table \in Tables: activeCatalog[table] \in durableDirectories

PublishedGenerationHasOneIdentity ==
    \A left, right \in publishedCatalogs:
        left.generation = right.generation => left.catalog = right.catalog

ActiveCatalogWasPublished ==
    [generation |-> activeGeneration, catalog |-> activeCatalog]
        \in publishedCatalogs

PreparedDirectoriesFollowArtifacts ==
    candidateState = "manifest" =>
        /\ candidateGeneration \in durableArtifacts
        /\ candidateGeneration \in durableDirectories

PinnedCatalogIsImmutable ==
    \A reader \in Readers:
        readerGeneration[reader] # -1 =>
            /\ readerGeneration[reader] <= activeGeneration
            /\ \A table \in Tables:
                readerCatalog[reader][table] <= readerGeneration[reader]
            /\ [generation |-> readerGeneration[reader],
                catalog |-> readerCatalog[reader]] \in publishedCatalogs

StaleCandidateCannotPublish ==
    candidateState = "manifest" /\ candidateParent # activeGeneration =>
        ~ENABLED PublishCandidate

Spec == Init /\ [][Next]_vars

=============================================================================
