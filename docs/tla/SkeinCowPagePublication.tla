------------------- MODULE SkeinCowPagePublication -------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Canonical row pages use immutable physical identities. A commit becomes  *)
(* visible only after its WAL record is durable. A checkpoint writes new    *)
(* versions of dirty pages, optionally relocates clean pages, reuses other  *)
(* page references from the selected                                      *)
(* root, and publishes one generation-fenced manifest only after every new  *)
(* page is durable. Readers pin manifest generations, so reclamation keeps  *)
(* the complete page-reference closure of active, previous, and pinned      *)
(* roots. A crash discards an unpublished candidate and reconstructs the    *)
(* dirty overlay from the durable WAL prefix.                               *)
(***************************************************************************)

CONSTANT Readers, MaxEpoch, MaxGeneration, MaxPage

ASSUME /\ Readers # {}
       /\ MaxEpoch \in Nat \ {0}
       /\ MaxGeneration \in Nat \ {0}
       /\ MaxPage \in Nat \ {0}

Epochs == 0..MaxEpoch
CommitEpochs == 1..MaxEpoch
Generations == 0..MaxGeneration
Pages == 1..MaxPage
ReaderGenerations == (-1)..MaxGeneration
PendingPhases == {"none", "appended", "durable"}
CandidatePhases == {
    "idle",
    "building",
    "overflowDurable",
    "pagesDurable",
    "rootDurable",
    "manifestDurable"
}
(* This dense address is a bijection with the immutable (generation,page) *)
(* identity. It changes only representation, never the transition graph. *)
PageRefs == 1..((MaxGeneration + 1) * MaxPage)
RootType == [Pages -> Generations]

PageRef(generation, page) ==
    generation * MaxPage + page

RefGeneration(ref) == (ref - 1) \div MaxPage
RefPage(ref) == ((ref - 1) % MaxPage) + 1

ASSUME /\ PageRefs = {PageRef(generation, page) :
                         generation \in Generations, page \in Pages}
       /\ \A generation \in Generations, page \in Pages:
           /\ RefGeneration(PageRef(generation, page)) = generation
           /\ RefPage(PageRef(generation, page)) = page

RootRefs(root) ==
    {PageRef(root[page], page) : page \in Pages}

VARIABLES
    walDurableEpoch,
    walDirtyByEpoch,
    visibleEpoch,
    dirtyPages,
    pendingPhase,
    pendingEpoch,
    pendingPage,
    activeGeneration,
    previousGeneration,
    manifestEpoch,
    nextGeneration,
    publishedRoots,
    rootByGeneration,
    generationEpoch,
    durableOverflowRoots,
    durableSchemaCatalogs,
    overflowEpoch,
    canonicalOverflowGeneration,
    durablePages,
    pageEpoch,
    candidatePhase,
    candidateGeneration,
    candidateEpoch,
    candidateBaseGeneration,
    candidateDirtyPages,
    candidateRoot,
    readerGeneration,
    staleCandidateRejected

vars == <<
    walDurableEpoch,
    walDirtyByEpoch,
    visibleEpoch,
    dirtyPages,
    pendingPhase,
    pendingEpoch,
    pendingPage,
    activeGeneration,
    previousGeneration,
    manifestEpoch,
    nextGeneration,
    publishedRoots,
    rootByGeneration,
    generationEpoch,
    durableOverflowRoots,
    durableSchemaCatalogs,
    overflowEpoch,
    canonicalOverflowGeneration,
    durablePages,
    pageEpoch,
    candidatePhase,
    candidateGeneration,
    candidateEpoch,
    candidateBaseGeneration,
    candidateDirtyPages,
    candidateRoot,
    readerGeneration,
    staleCandidateRejected
>>

DirtyAfter(lowerEpoch, upperEpoch) ==
    IF lowerEpoch = upperEpoch
    THEN {}
    ELSE UNION {
        walDirtyByEpoch[epoch] :
            epoch \in (lowerEpoch + 1)..upperEpoch
    }

WalDirtySuffix(afterEpoch) ==
    [epoch \in CommitEpochs |->
        IF epoch <= afterEpoch THEN {} ELSE walDirtyByEpoch[epoch]]

PinnedReaders ==
    {reader \in Readers : readerGeneration[reader] # -1}

PinnedGenerations ==
    {readerGeneration[reader] : reader \in PinnedReaders}

RequiredRootGenerations ==
    {activeGeneration, previousGeneration} \cup PinnedGenerations

RequiredPageRefs ==
    UNION {
        RootRefs(rootByGeneration[generation]) :
            generation \in RequiredRootGenerations
    }

StaleCandidate ==
    /\ candidatePhase # "idle"
    /\ candidateBaseGeneration # activeGeneration

CandidateWrittenPages ==
    {page \in Pages : candidateRoot[page] = candidateGeneration}

Init ==
    /\ walDurableEpoch = 0
    /\ walDirtyByEpoch = [epoch \in CommitEpochs |-> {}]
    /\ visibleEpoch = 0
    /\ dirtyPages = {}
    /\ pendingPhase = "none"
    /\ pendingEpoch = 0
    /\ pendingPage = 0
    /\ activeGeneration = 0
    /\ previousGeneration = 0
    /\ manifestEpoch = 0
    /\ nextGeneration = 1
    /\ publishedRoots = {0}
    /\ rootByGeneration =
        [generation \in Generations |-> [page \in Pages |-> 0]]
    /\ generationEpoch =
        [generation \in Generations |->
            IF generation = 0 THEN 0 ELSE -1]
    /\ durableOverflowRoots = {0}
    /\ durableSchemaCatalogs = {0}
    /\ overflowEpoch =
        [generation \in Generations |->
            IF generation = 0 THEN 0 ELSE -1]
    /\ canonicalOverflowGeneration = 0
    /\ durablePages = {ref \in PageRefs : RefGeneration(ref) = 0}
    /\ pageEpoch =
        [ref \in PageRefs |-> IF RefGeneration(ref) = 0 THEN 0 ELSE -1]
    /\ candidatePhase = "idle"
    /\ candidateGeneration = 0
    /\ candidateEpoch = 0
    /\ candidateBaseGeneration = 0
    /\ candidateDirtyPages = {}
    /\ candidateRoot = [page \in Pages |-> 0]
    /\ readerGeneration = [reader \in Readers |-> -1]
    /\ staleCandidateRejected = FALSE

BeginCommit ==
    /\ pendingPhase = "none"
    /\ visibleEpoch < MaxEpoch
    /\ pendingPhase' = "appended"
    /\ pendingEpoch' = visibleEpoch + 1
    /\ \E page \in 0..MaxPage: pendingPage' = page
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

SyncWal ==
    /\ pendingPhase = "appended"
    /\ pendingEpoch = walDurableEpoch + 1
    /\ walDirtyByEpoch' =
        [walDirtyByEpoch EXCEPT
            ![pendingEpoch] = IF pendingPage = 0 THEN {} ELSE {pendingPage}]
    /\ walDurableEpoch' = pendingEpoch
    /\ pendingPhase' = "durable"
    /\ UNCHANGED <<
        visibleEpoch,
        dirtyPages,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

PublishCommit ==
    /\ pendingPhase = "durable"
    /\ pendingEpoch = visibleEpoch + 1
    /\ pendingEpoch <= walDurableEpoch
    /\ visibleEpoch' = pendingEpoch
    /\ dirtyPages' =
        IF pendingPage = 0 THEN dirtyPages ELSE dirtyPages \cup {pendingPage}
    /\ pendingPhase' = "none"
    /\ pendingEpoch' = 0
    /\ pendingPage' = 0
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

BeginCheckpoint ==
    /\ candidatePhase = "idle"
    /\ pendingPhase = "none"
    /\ manifestEpoch <= visibleEpoch
    /\ nextGeneration <= MaxGeneration
    /\ candidatePhase' = "building"
    /\ candidateGeneration' = nextGeneration
    /\ candidateEpoch' = visibleEpoch
    /\ candidateBaseGeneration' = activeGeneration
    /\ candidateDirtyPages' = dirtyPages
    /\ \E relocatedGenerations \in SUBSET {
            rootByGeneration[activeGeneration][page] : page \in Pages \ dirtyPages}:
        candidateRoot' =
            [page \in Pages |->
                IF page \in dirtyPages
                    \/ rootByGeneration[activeGeneration][page] \in relocatedGenerations
                THEN nextGeneration
                ELSE rootByGeneration[activeGeneration][page]]
    /\ nextGeneration' = nextGeneration + 1
    /\ staleCandidateRejected' = FALSE
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        readerGeneration
        >>

PersistCandidatePages ==
    /\ candidatePhase = "overflowDurable"
    /\ LET refs == {
            PageRef(candidateGeneration, page) :
                page \in CandidateWrittenPages
        }
       IN /\ \A ref \in refs: pageEpoch[ref] = -1
          /\ durablePages' = durablePages \cup refs
          /\ pageEpoch' =
              [ref \in PageRefs |->
                  IF ref \in refs
                  THEN IF RefPage(ref) \in candidateDirtyPages
                       THEN candidateEpoch
                       ELSE pageEpoch[PageRef(
                           rootByGeneration[candidateBaseGeneration][RefPage(ref)], RefPage(ref))]
                  ELSE pageEpoch[ref]]
    /\ candidatePhase' = "pagesDurable"
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

PersistCandidateOverflow ==
    /\ candidatePhase = "building"
    /\ candidateGeneration \notin durableOverflowRoots
    /\ durableOverflowRoots' =
        durableOverflowRoots \cup {candidateGeneration}
    /\ overflowEpoch' =
        [overflowEpoch EXCEPT ![candidateGeneration] = candidateEpoch]
    /\ candidatePhase' = "overflowDurable"
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableSchemaCatalogs,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

PersistCandidateRoot ==
    /\ candidatePhase = "pagesDurable"
    /\ candidatePhase' = "rootDurable"
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

PersistCandidateManifest ==
    /\ candidatePhase = "rootDurable"
    /\ candidatePhase' = "manifestDurable"
    /\ durableSchemaCatalogs' =
        durableSchemaCatalogs \cup {candidateGeneration}
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

PublishCheckpoint ==
    /\ candidatePhase = "manifestDurable"
    /\ candidateBaseGeneration = activeGeneration
    /\ candidateGeneration \in durableOverflowRoots
    /\ candidateGeneration \in durableSchemaCatalogs
    /\ overflowEpoch[candidateGeneration] = candidateEpoch
    /\ \A page \in CandidateWrittenPages:
        PageRef(candidateGeneration, page) \in durablePages
    /\ previousGeneration' = activeGeneration
    /\ activeGeneration' = candidateGeneration
    /\ canonicalOverflowGeneration' = candidateGeneration
    /\ manifestEpoch' = candidateEpoch
    /\ walDirtyByEpoch' = WalDirtySuffix(candidateEpoch)
    /\ publishedRoots' = publishedRoots \cup {candidateGeneration}
    /\ rootByGeneration' =
        [rootByGeneration EXCEPT ![candidateGeneration] = candidateRoot]
    /\ generationEpoch' =
        [generationEpoch EXCEPT ![candidateGeneration] = candidateEpoch]
    /\ dirtyPages' = DirtyAfter(candidateEpoch, visibleEpoch)
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateEpoch' = 0
    /\ candidateBaseGeneration' = candidateGeneration
    /\ candidateDirtyPages' = {}
    /\ candidateRoot' = [page \in Pages |-> 0]
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        nextGeneration,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        durablePages,
        pageEpoch,
        readerGeneration,
        staleCandidateRejected
        >>

(***************************************************************************)
(* A complete competing checkpoint may win after this candidate captured   *)
(* its base generation. It uses a fresh generation and full-page rewrite so *)
(* the older candidate can only be rejected, never overwrite the winner.   *)
(***************************************************************************)
PublishCompetingCheckpoint ==
    /\ candidatePhase # "idle"
    /\ nextGeneration <= MaxGeneration
    /\ LET generation == nextGeneration
           root == [page \in Pages |-> generation]
           refs == RootRefs(root)
       IN /\ \A ref \in refs: pageEpoch[ref] = -1
          /\ durablePages' = durablePages \cup refs
          /\ pageEpoch' =
              [ref \in PageRefs |->
                  IF ref \in refs THEN visibleEpoch ELSE pageEpoch[ref]]
          /\ rootByGeneration' =
              [rootByGeneration EXCEPT ![generation] = root]
          /\ generationEpoch' =
              [generationEpoch EXCEPT ![generation] = visibleEpoch]
          /\ durableOverflowRoots' = durableOverflowRoots \cup {generation}
          /\ durableSchemaCatalogs' = durableSchemaCatalogs \cup {generation}
          /\ overflowEpoch' =
              [overflowEpoch EXCEPT ![generation] = visibleEpoch]
          /\ canonicalOverflowGeneration' = generation
          /\ publishedRoots' = publishedRoots \cup {generation}
          /\ previousGeneration' = activeGeneration
          /\ activeGeneration' = generation
          /\ nextGeneration' = generation + 1
    /\ manifestEpoch' = visibleEpoch
    /\ walDirtyByEpoch' = WalDirtySuffix(visibleEpoch)
    /\ dirtyPages' = {}
    /\ UNCHANGED <<
        walDurableEpoch,
        visibleEpoch,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

RejectStaleCandidate ==
    /\ candidatePhase # "idle"
    /\ candidateBaseGeneration # activeGeneration
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateEpoch' = 0
    /\ candidateBaseGeneration' = activeGeneration
    /\ candidateDirtyPages' = {}
    /\ candidateRoot' = [page \in Pages |-> 0]
    /\ staleCandidateRejected' = TRUE
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        readerGeneration
        >>

BeginRead(reader) ==
    /\ readerGeneration[reader] = -1
    /\ readerGeneration' =
        [readerGeneration EXCEPT ![reader] = activeGeneration]
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        staleCandidateRejected
        >>

EndRead(reader) ==
    /\ readerGeneration[reader] # -1
    /\ readerGeneration' = [readerGeneration EXCEPT ![reader] = -1]
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        staleCandidateRejected
        >>

Reclaim ==
    /\ candidatePhase = "idle"
    /\ \/ publishedRoots # RequiredRootGenerations
       \/ durablePages # RequiredPageRefs
    /\ publishedRoots' = RequiredRootGenerations
    /\ rootByGeneration' =
        [generation \in Generations |->
            IF generation \in RequiredRootGenerations
            THEN rootByGeneration[generation]
            ELSE [page \in Pages |-> 0]]
    /\ durablePages' = durablePages \cap RequiredPageRefs
    /\ durableOverflowRoots' =
        durableOverflowRoots \cap RequiredRootGenerations
    /\ durableSchemaCatalogs' =
        durableSchemaCatalogs \cap RequiredRootGenerations
    (* Keep -1 distinct from a retired identity that was already written. *)
    /\ generationEpoch' =
        [generation \in Generations |->
            IF generation \in publishedRoots' \/ generationEpoch[generation] = -1
            THEN generationEpoch[generation]
            ELSE 0]
    /\ pageEpoch' =
        [ref \in PageRefs |->
            IF ref \in durablePages' \/ pageEpoch[ref] = -1
            THEN pageEpoch[ref]
            ELSE 0]
    /\ overflowEpoch' =
        [generation \in Generations |->
            IF generation \in durableOverflowRoots' \/ overflowEpoch[generation] = -1
            THEN overflowEpoch[generation]
            ELSE 0]
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        visibleEpoch,
        dirtyPages,
        pendingPhase,
        pendingEpoch,
        pendingPage,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        canonicalOverflowGeneration,
        candidatePhase,
        candidateGeneration,
        candidateEpoch,
        candidateBaseGeneration,
        candidateDirtyPages,
        candidateRoot,
        readerGeneration,
        staleCandidateRejected
        >>

CrashAndRecover ==
    /\ \/ pendingPhase # "none"
       \/ candidatePhase # "idle"
       \/ PinnedReaders # {}
       \/ visibleEpoch # walDurableEpoch
    /\ visibleEpoch' = walDurableEpoch
    /\ dirtyPages' = DirtyAfter(manifestEpoch, walDurableEpoch)
    /\ pendingPhase' = "none"
    /\ pendingEpoch' = 0
    /\ pendingPage' = 0
    /\ candidatePhase' = "idle"
    /\ candidateGeneration' = 0
    /\ candidateEpoch' = 0
    /\ candidateBaseGeneration' = activeGeneration
    /\ candidateDirtyPages' = {}
    /\ candidateRoot' = [page \in Pages |-> 0]
    /\ readerGeneration' = [reader \in Readers |-> -1]
    /\ UNCHANGED <<
        walDurableEpoch,
        walDirtyByEpoch,
        activeGeneration,
        previousGeneration,
        manifestEpoch,
        nextGeneration,
        publishedRoots,
        rootByGeneration,
        generationEpoch,
        durableOverflowRoots,
        durableSchemaCatalogs,
        overflowEpoch,
        canonicalOverflowGeneration,
        durablePages,
        pageEpoch,
        staleCandidateRejected
        >>

Next ==
    \/ BeginCommit
    \/ SyncWal
    \/ PublishCommit
    \/ BeginCheckpoint
    \/ PersistCandidateOverflow
    \/ PersistCandidatePages
    \/ PersistCandidateRoot
    \/ PersistCandidateManifest
    \/ PublishCheckpoint
    \/ PublishCompetingCheckpoint
    \/ RejectStaleCandidate
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Reclaim
    \/ CrashAndRecover

TypeOK ==
    /\ walDurableEpoch \in Epochs
    /\ walDirtyByEpoch \in [CommitEpochs -> SUBSET Pages]
    /\ visibleEpoch \in Epochs
    /\ dirtyPages \subseteq Pages
    /\ pendingPhase \in PendingPhases
    /\ pendingEpoch \in Epochs
    /\ pendingPage \in 0..MaxPage
    /\ activeGeneration \in Generations
    /\ previousGeneration \in Generations
    /\ manifestEpoch \in Epochs
    /\ nextGeneration \in 1..(MaxGeneration + 1)
    /\ publishedRoots \subseteq Generations
    /\ rootByGeneration \in [Generations -> RootType]
    /\ generationEpoch \in [Generations -> (-1)..MaxEpoch]
    /\ durableOverflowRoots \subseteq Generations
    /\ durableSchemaCatalogs \subseteq Generations
    /\ overflowEpoch \in [Generations -> (-1)..MaxEpoch]
    /\ canonicalOverflowGeneration \in Generations
    /\ durablePages \subseteq PageRefs
    /\ pageEpoch \in [PageRefs -> (-1)..MaxEpoch]
    /\ candidatePhase \in CandidatePhases
    /\ candidateGeneration \in Generations
    /\ candidateEpoch \in Epochs
    /\ candidateBaseGeneration \in Generations
    /\ candidateDirtyPages \subseteq Pages
    /\ candidateRoot \in RootType
    /\ readerGeneration \in [Readers -> ReaderGenerations]
    /\ staleCandidateRejected \in BOOLEAN

VisibleOnlyAfterWal ==
    visibleEpoch <= walDurableEpoch

ManifestDoesNotLeadVisibleState ==
    manifestEpoch <= visibleEpoch

DirtyOverlayMatchesVisibleWal ==
    dirtyPages = DirtyAfter(manifestEpoch, visibleEpoch)

ActiveAndPreviousRootsRemainPublished ==
    /\ activeGeneration \in publishedRoots
    /\ previousGeneration \in publishedRoots

PublishedGenerationDoesNotRegress ==
    activeGeneration >= previousGeneration

PublishedEpochDoesNotRegress ==
    generationEpoch[activeGeneration] >= generationEpoch[previousGeneration]

PinnedRootsRemainPublished ==
    PinnedGenerations \subseteq publishedRoots

PublishedRootsReferenceDurablePages ==
    \A generation \in publishedRoots:
        /\ generationEpoch[generation] >= 0
        /\ RootRefs(rootByGeneration[generation]) \subseteq durablePages
        /\ \A page \in Pages:
            LET ref == PageRef(rootByGeneration[generation][page], page)
            IN /\ pageEpoch[ref] >= 0
               /\ pageEpoch[ref] <= generationEpoch[generation]

PublishedRootsReferenceDurableOverflow ==
    \A generation \in publishedRoots:
        /\ generation \in durableOverflowRoots
        /\ overflowEpoch[generation] = generationEpoch[generation]

PublishedRootsReferenceDurableSchemas ==
    publishedRoots \subseteq durableSchemaCatalogs

CanonicalRowOverflowGenerationAgreement ==
    /\ activeGeneration = canonicalOverflowGeneration
    /\ generationEpoch[activeGeneration] =
        overflowEpoch[canonicalOverflowGeneration]

CandidateUsesFreshImmutableIdentity ==
    candidatePhase = "idle" \/
        /\ candidateGeneration < nextGeneration
        /\ candidateGeneration \notin publishedRoots
        /\ generationEpoch[candidateGeneration] = -1
        /\ candidateBaseGeneration \in publishedRoots
        /\ candidateEpoch <= visibleEpoch

DurableCandidateIsNotCanonicalUntilCheckpointPublication ==
    candidatePhase = "idle" \/ candidateGeneration # canonicalOverflowGeneration

RetiredPayloadIsReleased ==
    /\ \A ref \in PageRefs \ durablePages: pageEpoch[ref] \in {-1, 0}
    /\ \A generation \in Generations \ publishedRoots:
        generationEpoch[generation] \in {-1, 0}
    /\ \A generation \in Generations \ durableOverflowRoots:
        overflowEpoch[generation] \in {-1, 0}

RetiredRootMetadataIsReleased ==
    \A generation \in Generations \ publishedRoots:
        rootByGeneration[generation] = [page \in Pages |-> 0]

CheckpointedWalMetadataIsReleased ==
    \A epoch \in CommitEpochs:
        epoch <= manifestEpoch => walDirtyByEpoch[epoch] = {}

(* Rust releases PreparedCheckpoint on publication, rejection, and unwind. *)
(* No transition reads an idle candidateRoot before overwriting it; use one *)
(* canonical absent value instead of exploring unreachable object contents. *)
IdleCandidateRootIsReleased ==
    candidatePhase = "idle" => candidateRoot = [page \in Pages |-> 0]

CandidateRootCopiesDirtyAndSelectedPages ==
    candidatePhase = "idle" \/
        \A page \in Pages:
            IF page \in candidateDirtyPages
            THEN candidateRoot[page] = candidateGeneration
            ELSE candidateRoot[page] \in {
                candidateGeneration, rootByGeneration[candidateBaseGeneration][page]}

RelocationSelectsWholeGenerations ==
    candidatePhase # "idle" =>
        \A left, right \in Pages \ candidateDirtyPages:
            rootByGeneration[candidateBaseGeneration][left] =
                rootByGeneration[candidateBaseGeneration][right] =>
                (candidateRoot[left] = candidateGeneration) =
                    (candidateRoot[right] = candidateGeneration)

RelocationPreservesSourceEpoch ==
    candidatePhase \in {"pagesDurable", "rootDurable", "manifestDurable"} =>
        \A page \in CandidateWrittenPages \ candidateDirtyPages:
            pageEpoch[PageRef(candidateGeneration, page)] =
                pageEpoch[PageRef(rootByGeneration[candidateBaseGeneration][page], page)]

DurableCandidateHasCompletePages ==
    candidatePhase \in {"pagesDurable", "rootDurable", "manifestDurable"} =>
        \A page \in CandidateWrittenPages:
            LET ref == PageRef(candidateGeneration, page)
            IN /\ ref \in durablePages
               /\ (page \in candidateDirtyPages => pageEpoch[ref] = candidateEpoch)

ManifestCandidateHasDurableClosure ==
    candidatePhase = "manifestDurable" =>
        RootRefs(candidateRoot) \subseteq durablePages

ReclamationPreservesRequiredClosure ==
    /\ RequiredRootGenerations \subseteq publishedRoots
    /\ RequiredPageRefs \subseteq durablePages
    /\ RequiredRootGenerations \subseteq durableOverflowRoots
    /\ RequiredRootGenerations \subseteq durableSchemaCatalogs

StaleCandidateRejectionIsTerminal ==
    staleCandidateRejected => candidatePhase = "idle"

StaleCandidateEventuallyTerminates ==
    StaleCandidate ~> (candidatePhase = "idle")

(* RejectStaleCandidate always changes candidatePhase from non-idle to idle. *)
(* Thus <<RejectStaleCandidate>>_candidatePhase and                          *)
(* <<RejectStaleCandidate>>_vars both equal RejectStaleCandidate: their      *)
(* enabled predicates and weak fairness obligations are identical. The      *)
(* scalar subscript avoids comparing all state fields on every TLC edge.   *)
Spec == Init /\ [][Next]_vars /\ WF_candidatePhase(RejectStaleCandidate)

=============================================================================
