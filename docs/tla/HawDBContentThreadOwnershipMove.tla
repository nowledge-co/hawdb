---------------- MODULE HawDBContentThreadOwnershipMove ----------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A guarded thread ownership batch may contain rows from different source  *)
(* spaces. Each graph Thread, relational document, and message set moves    *)
(* only when its current owner matches the corresponding preview guard.     *)
(* The complete batch becomes visible through one durable mixed commit.      *)
(***************************************************************************)

CONSTANTS MaxEpoch, PayloadDigest

ASSUME MaxEpoch \in Nat \ {0}

Owners == {"a", "b", "stale"}
Spaces == {"default", "archive", "stale-current", "stale-preview", "work"}
Phases == {"idle", "active", "durable"}
OwnerSpaces == [Owners -> Spaces]

InitialSpace(owner) ==
    CASE owner = "a" -> "default"
      [] owner = "b" -> "archive"
      [] OTHER -> "stale-current"

ExpectedSpace(owner) ==
    CASE owner = "a" -> "default"
      [] owner = "b" -> "archive"
      [] OTHER -> "stale-preview"

InitialOwners == [owner \in Owners |-> InitialSpace(owner)]

InitialState ==
    [graph |-> InitialOwners,
     document |-> InitialOwners,
     messages |-> InitialOwners,
     payload |-> PayloadDigest]

PublishedStates ==
    [graph : OwnerSpaces,
     document : OwnerSpaces,
     messages : OwnerSpaces,
     payload : {PayloadDigest}]

HistoryState(history, epoch) ==
    IF epoch = 0 THEN InitialState ELSE history[epoch]

MoveIfGuardMatches(spaces, owner) ==
    IF spaces[owner] = ExpectedSpace(owner)
    THEN [spaces EXCEPT ![owner] = "work"]
    ELSE spaces

ExpectedWorkspace(spaces, processed) ==
    [owner \in Owners |->
        IF owner \in processed /\ spaces[owner] = ExpectedSpace(owner)
        THEN "work"
        ELSE spaces[owner]]

VARIABLES
    canonicalGraph,
    canonicalDocument,
    canonicalMessages,
    canonicalPayload,
    commitEpoch,
    durableHistory,
    phase,
    processedOwners,
    workspaceGraph,
    workspaceDocument,
    workspaceMessages,
    workspacePayload,
    lastPublishedGraph,
    lastPublishedDocument,
    lastPublishedMessages

vars == <<
    canonicalGraph,
    canonicalDocument,
    canonicalMessages,
    canonicalPayload,
    commitEpoch,
    durableHistory,
    phase,
    processedOwners,
    workspaceGraph,
    workspaceDocument,
    workspaceMessages,
    workspacePayload,
    lastPublishedGraph,
    lastPublishedDocument,
    lastPublishedMessages
>>

Init ==
    /\ canonicalGraph = InitialOwners
    /\ canonicalDocument = InitialOwners
    /\ canonicalMessages = InitialOwners
    /\ canonicalPayload = PayloadDigest
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ processedOwners = {}
    /\ workspaceGraph = InitialOwners
    /\ workspaceDocument = InitialOwners
    /\ workspaceMessages = InitialOwners
    /\ workspacePayload = PayloadDigest
    /\ lastPublishedGraph = InitialOwners
    /\ lastPublishedDocument = InitialOwners
    /\ lastPublishedMessages = InitialOwners

BeginBatch ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ phase' = "active"
    /\ processedOwners' = {}
    /\ workspaceGraph' = canonicalGraph
    /\ workspaceDocument' = canonicalDocument
    /\ workspaceMessages' = canonicalMessages
    /\ workspacePayload' = canonicalPayload
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalMessages,
        canonicalPayload, commitEpoch, durableHistory,
        lastPublishedGraph, lastPublishedDocument, lastPublishedMessages
       >>

StageOwner(owner) ==
    /\ phase = "active"
    /\ owner \in Owners \ processedOwners
    /\ workspaceGraph' = MoveIfGuardMatches(workspaceGraph, owner)
    /\ workspaceDocument' = MoveIfGuardMatches(workspaceDocument, owner)
    /\ workspaceMessages' = MoveIfGuardMatches(workspaceMessages, owner)
    /\ processedOwners' = processedOwners \cup {owner}
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalMessages,
        canonicalPayload, commitEpoch, durableHistory, phase,
        workspacePayload,
        lastPublishedGraph, lastPublishedDocument, lastPublishedMessages
       >>

MakeDurable ==
    /\ phase = "active"
    /\ processedOwners = Owners
    /\ durableHistory' = Append(
        durableHistory,
        [graph |-> workspaceGraph,
         document |-> workspaceDocument,
         messages |-> workspaceMessages,
         payload |-> workspacePayload]
       )
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalMessages,
        canonicalPayload, commitEpoch, processedOwners,
        workspaceGraph, workspaceDocument, workspaceMessages,
        workspacePayload,
        lastPublishedGraph, lastPublishedDocument, lastPublishedMessages
       >>

Publish ==
    /\ phase = "durable"
    /\ canonicalGraph' = workspaceGraph
    /\ canonicalDocument' = workspaceDocument
    /\ canonicalMessages' = workspaceMessages
    /\ canonicalPayload' = workspacePayload
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ processedOwners' = {}
    /\ lastPublishedGraph' = workspaceGraph
    /\ lastPublishedDocument' = workspaceDocument
    /\ lastPublishedMessages' = workspaceMessages
    /\ UNCHANGED <<
        durableHistory, workspaceGraph, workspaceDocument,
        workspaceMessages, workspacePayload
       >>

Rollback ==
    /\ phase = "active"
    /\ phase' = "idle"
    /\ processedOwners' = {}
    /\ workspaceGraph' = canonicalGraph
    /\ workspaceDocument' = canonicalDocument
    /\ workspaceMessages' = canonicalMessages
    /\ workspacePayload' = canonicalPayload
    /\ UNCHANGED <<
        canonicalGraph, canonicalDocument, canonicalMessages,
        canonicalPayload, commitEpoch, durableHistory,
        lastPublishedGraph, lastPublishedDocument, lastPublishedMessages
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recovered == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonicalGraph' = recovered.graph
    /\ canonicalDocument' = recovered.document
    /\ canonicalMessages' = recovered.messages
    /\ canonicalPayload' = recovered.payload
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ processedOwners' = {}
    /\ workspaceGraph' = recovered.graph
    /\ workspaceDocument' = recovered.document
    /\ workspaceMessages' = recovered.messages
    /\ workspacePayload' = recovered.payload
    /\ lastPublishedGraph' = recovered.graph
    /\ lastPublishedDocument' = recovered.document
    /\ lastPublishedMessages' = recovered.messages
    /\ UNCHANGED durableHistory

Next ==
    \/ BeginBatch
    \/ \E owner \in Owners: StageOwner(owner)
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalGraph \in OwnerSpaces
    /\ canonicalDocument \in OwnerSpaces
    /\ canonicalMessages \in OwnerSpaces
    /\ canonicalPayload = PayloadDigest
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(PublishedStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ processedOwners \in SUBSET Owners
    /\ workspaceGraph \in OwnerSpaces
    /\ workspaceDocument \in OwnerSpaces
    /\ workspaceMessages \in OwnerSpaces
    /\ workspacePayload = PayloadDigest
    /\ lastPublishedGraph \in OwnerSpaces
    /\ lastPublishedDocument \in OwnerSpaces
    /\ lastPublishedMessages \in OwnerSpaces

VisibilityFollowsDurability ==
    /\ commitEpoch <= Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ LET current == HistoryState(durableHistory, commitEpoch) IN
       /\ canonicalGraph = current.graph
       /\ canonicalDocument = current.document
       /\ canonicalMessages = current.messages
       /\ canonicalPayload = current.payload

CanonicalOwnershipIsAtomic ==
    \A owner \in Owners:
        /\ canonicalGraph[owner] = canonicalDocument[owner]
        /\ canonicalGraph[owner] = canonicalMessages[owner]

WorkspaceFollowsGuards ==
    phase = "active" =>
        /\ workspaceGraph = ExpectedWorkspace(canonicalGraph, processedOwners)
        /\ workspaceDocument = ExpectedWorkspace(canonicalDocument, processedOwners)
        /\ workspaceMessages = ExpectedWorkspace(canonicalMessages, processedOwners)

StalePreviewIsPreserved ==
    /\ canonicalGraph["stale"] = "stale-current"
    /\ canonicalDocument["stale"] = "stale-current"
    /\ canonicalMessages["stale"] = "stale-current"

PayloadIsPreserved ==
    /\ canonicalPayload = PayloadDigest
    /\ workspacePayload = PayloadDigest

PublishedBatchIsComplete ==
    phase = "idle" =>
        /\ canonicalGraph = lastPublishedGraph
        /\ canonicalDocument = lastPublishedDocument
        /\ canonicalMessages = lastPublishedMessages

=============================================================================
