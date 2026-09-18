---------------- MODULE HawDBContentSpaceMergeOwnership ----------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A Space merge moves selected Thread and Source owners through one mixed  *)
(* graph and relational commit. The same source-space guard applies to each  *)
(* selected owner. A stale selection remains unchanged while eligible graph, *)
(* document, and dependent message/chunk views publish together.             *)
(***************************************************************************)

CONSTANTS MaxEpoch, PayloadDigest

ASSUME MaxEpoch \in Nat \ {0}

Owners == {"thread", "source", "stale"}
Spaces == {"source", "target", "stale-current"}
Phases == {"idle", "active", "durable"}
OwnerSpaces == [Owners -> Spaces]

InitialSpace(owner) ==
    IF owner = "stale" THEN "stale-current" ELSE "source"

InitialOwners == [owner \in Owners |-> InitialSpace(owner)]

InitialState ==
    [graph |-> InitialOwners,
     document |-> InitialOwners,
     dependentRows |-> InitialOwners,
     payload |-> PayloadDigest]

PublishedStates ==
    [graph : OwnerSpaces,
     document : OwnerSpaces,
     dependentRows : OwnerSpaces,
     payload : {PayloadDigest}]

HistoryState(history, epoch) ==
    IF epoch = 0 THEN InitialState ELSE history[epoch]

MoveIfEligible(spaces, owner) ==
    IF spaces[owner] = "source"
    THEN [spaces EXCEPT ![owner] = "target"]
    ELSE spaces

ExpectedWorkspace(spaces, processed) ==
    [owner \in Owners |->
        IF owner \in processed /\ spaces[owner] = "source"
        THEN "target"
        ELSE spaces[owner]]

VARIABLES
    canonicalGraph,
    canonicalDocument,
    canonicalDependentRows,
    canonicalPayload,
    commitEpoch,
    durableHistory,
    phase,
    processedOwners,
    workspaceGraph,
    workspaceDocument,
    workspaceDependentRows,
    workspacePayload,
    lastPublishedGraph,
    lastPublishedDocument,
    lastPublishedDependentRows

vars == <<
    canonicalGraph,
    canonicalDocument,
    canonicalDependentRows,
    canonicalPayload,
    commitEpoch,
    durableHistory,
    phase,
    processedOwners,
    workspaceGraph,
    workspaceDocument,
    workspaceDependentRows,
    workspacePayload,
    lastPublishedGraph,
    lastPublishedDocument,
    lastPublishedDependentRows
>>

Init ==
    /\ canonicalGraph = InitialOwners
    /\ canonicalDocument = InitialOwners
    /\ canonicalDependentRows = InitialOwners
    /\ canonicalPayload = PayloadDigest
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ processedOwners = {}
    /\ workspaceGraph = InitialOwners
    /\ workspaceDocument = InitialOwners
    /\ workspaceDependentRows = InitialOwners
    /\ workspacePayload = PayloadDigest
    /\ lastPublishedGraph = InitialOwners
    /\ lastPublishedDocument = InitialOwners
    /\ lastPublishedDependentRows = InitialOwners

BeginMerge ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ phase' = "active"
    /\ processedOwners' = {}
    /\ workspaceGraph' = canonicalGraph
    /\ workspaceDocument' = canonicalDocument
    /\ workspaceDependentRows' = canonicalDependentRows
    /\ workspacePayload' = canonicalPayload
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalDependentRows,
        canonicalPayload, commitEpoch, durableHistory,
        lastPublishedGraph, lastPublishedDocument, lastPublishedDependentRows
       >>

StageOwner(owner) ==
    /\ phase = "active"
    /\ owner \in Owners \ processedOwners
    /\ workspaceGraph' = MoveIfEligible(workspaceGraph, owner)
    /\ workspaceDocument' = MoveIfEligible(workspaceDocument, owner)
    /\ workspaceDependentRows' = MoveIfEligible(workspaceDependentRows, owner)
    /\ processedOwners' = processedOwners \cup {owner}
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalDependentRows,
        canonicalPayload, commitEpoch, durableHistory, phase,
        workspacePayload,
        lastPublishedGraph, lastPublishedDocument, lastPublishedDependentRows
       >>

MakeDurable ==
    /\ phase = "active"
    /\ processedOwners = Owners
    /\ durableHistory' = Append(
        durableHistory,
        [graph |-> workspaceGraph,
         document |-> workspaceDocument,
         dependentRows |-> workspaceDependentRows,
         payload |-> workspacePayload]
       )
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalDependentRows,
        canonicalPayload, commitEpoch, processedOwners,
        workspaceGraph, workspaceDocument, workspaceDependentRows,
        workspacePayload,
        lastPublishedGraph, lastPublishedDocument, lastPublishedDependentRows
       >>

Publish ==
    /\ phase = "durable"
    /\ canonicalGraph' = workspaceGraph
    /\ canonicalDocument' = workspaceDocument
    /\ canonicalDependentRows' = workspaceDependentRows
    /\ canonicalPayload' = workspacePayload
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ processedOwners' = {}
    /\ lastPublishedGraph' = workspaceGraph
    /\ lastPublishedDocument' = workspaceDocument
    /\ lastPublishedDependentRows' = workspaceDependentRows
    /\ UNCHANGED <<
        durableHistory, workspaceGraph, workspaceDocument,
        workspaceDependentRows, workspacePayload
       >>

Rollback ==
    /\ phase = "active"
    /\ phase' = "idle"
    /\ processedOwners' = {}
    /\ workspaceGraph' = canonicalGraph
    /\ workspaceDocument' = canonicalDocument
    /\ workspaceDependentRows' = canonicalDependentRows
    /\ workspacePayload' = canonicalPayload
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalDependentRows,
        canonicalPayload, commitEpoch, durableHistory,
        lastPublishedGraph, lastPublishedDocument, lastPublishedDependentRows
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recovered == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonicalGraph' = recovered.graph
    /\ canonicalDocument' = recovered.document
    /\ canonicalDependentRows' = recovered.dependentRows
    /\ canonicalPayload' = recovered.payload
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ processedOwners' = {}
    /\ workspaceGraph' = recovered.graph
    /\ workspaceDocument' = recovered.document
    /\ workspaceDependentRows' = recovered.dependentRows
    /\ workspacePayload' = recovered.payload
    /\ lastPublishedGraph' = recovered.graph
    /\ lastPublishedDocument' = recovered.document
    /\ lastPublishedDependentRows' = recovered.dependentRows
    /\ UNCHANGED durableHistory

Next ==
    \/ BeginMerge
    \/ \E owner \in Owners: StageOwner(owner)
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalGraph \in OwnerSpaces
    /\ canonicalDocument \in OwnerSpaces
    /\ canonicalDependentRows \in OwnerSpaces
    /\ canonicalPayload = PayloadDigest
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(PublishedStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ processedOwners \in SUBSET Owners
    /\ workspaceGraph \in OwnerSpaces
    /\ workspaceDocument \in OwnerSpaces
    /\ workspaceDependentRows \in OwnerSpaces
    /\ workspacePayload = PayloadDigest
    /\ lastPublishedGraph \in OwnerSpaces
    /\ lastPublishedDocument \in OwnerSpaces
    /\ lastPublishedDependentRows \in OwnerSpaces

VisibilityFollowsDurability ==
    /\ commitEpoch <= Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ LET current == HistoryState(durableHistory, commitEpoch) IN
       /\ canonicalGraph = current.graph
       /\ canonicalDocument = current.document
       /\ canonicalDependentRows = current.dependentRows
       /\ canonicalPayload = current.payload

CanonicalOwnershipIsAtomic ==
    \A owner \in Owners:
        /\ canonicalGraph[owner] = canonicalDocument[owner]
        /\ canonicalGraph[owner] = canonicalDependentRows[owner]

WorkspaceFollowsGuard ==
    phase = "active" =>
        /\ workspaceGraph = ExpectedWorkspace(canonicalGraph, processedOwners)
        /\ workspaceDocument = ExpectedWorkspace(canonicalDocument, processedOwners)
        /\ workspaceDependentRows = ExpectedWorkspace(
            canonicalDependentRows,
            processedOwners
           )

StaleSelectionIsPreserved ==
    /\ canonicalGraph["stale"] = "stale-current"
    /\ canonicalDocument["stale"] = "stale-current"
    /\ canonicalDependentRows["stale"] = "stale-current"

MovedKindsAgree ==
    phase = "idle" =>
        /\ canonicalGraph["thread"] = canonicalDocument["thread"]
        /\ canonicalGraph["source"] = canonicalDocument["source"]

PayloadIsPreserved ==
    /\ canonicalPayload = PayloadDigest
    /\ workspacePayload = PayloadDigest

PublishedMergeIsComplete ==
    phase = "idle" =>
        /\ canonicalGraph = lastPublishedGraph
        /\ canonicalDocument = lastPublishedDocument
        /\ canonicalDependentRows = lastPublishedDependentRows

=============================================================================
