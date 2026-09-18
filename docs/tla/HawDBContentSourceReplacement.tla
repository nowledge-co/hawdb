------------------- MODULE HawDBContentSourceReplacement -------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A Source chunk write is a whole-document replacement inside one mixed   *)
(* graph and relational transaction. The workspace deletes the old rows,   *)
(* inserts exactly the requested set, updates graph/document counts, and    *)
(* becomes visible only after one durable commit. A rejected duplicate      *)
(* chunk-order statement leaves all prior workspace state intact.           *)
(***************************************************************************)

CONSTANT MaxEpoch

ASSUME MaxEpoch \in Nat \ {0}

Chunks == 1..3
ChunkSets == SUBSET Chunks
Phases == {"idle", "active", "summarized", "durable"}

InitialSource ==
    [rows |-> Chunks,
     graphCount |-> Cardinality(Chunks),
     documentCount |-> Cardinality(Chunks)]

SourceStates ==
    [rows : ChunkSets,
     graphCount : 0..Cardinality(Chunks),
     documentCount : 0..Cardinality(Chunks)]

HistoryState(history, epoch) ==
    IF epoch = 0 THEN InitialSource ELSE history[epoch]

VARIABLES
    canonicalRows,
    graphChunkCount,
    documentItemCount,
    commitEpoch,
    durableHistory,
    phase,
    targetRows,
    workspaceRows,
    workspaceGraphCount,
    workspaceDocumentCount,
    lastAcceptedRows,
    statementRejected,
    duplicateRejected,
    lastPublishedTarget

vars == <<
    canonicalRows,
    graphChunkCount,
    documentItemCount,
    commitEpoch,
    durableHistory,
    phase,
    targetRows,
    workspaceRows,
    workspaceGraphCount,
    workspaceDocumentCount,
    lastAcceptedRows,
    statementRejected,
    duplicateRejected,
    lastPublishedTarget
>>

Init ==
    /\ canonicalRows = Chunks
    /\ graphChunkCount = Cardinality(Chunks)
    /\ documentItemCount = Cardinality(Chunks)
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ targetRows = Chunks
    /\ workspaceRows = Chunks
    /\ workspaceGraphCount = Cardinality(Chunks)
    /\ workspaceDocumentCount = Cardinality(Chunks)
    /\ lastAcceptedRows = Chunks
    /\ statementRejected = FALSE
    /\ duplicateRejected = FALSE
    /\ lastPublishedTarget = Chunks

BeginReplacement(target) ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ target \in ChunkSets
    /\ phase' = "active"
    /\ targetRows' = target
    /\ workspaceRows' = {}
    /\ workspaceGraphCount' = Cardinality(target)
    /\ workspaceDocumentCount' = documentItemCount
    /\ lastAcceptedRows' = {}
    /\ statementRejected' = FALSE
    /\ duplicateRejected' = FALSE
    /\ UNCHANGED <<
        canonicalRows, graphChunkCount, documentItemCount,
        commitEpoch, durableHistory, lastPublishedTarget
       >>

InsertChunk(chunk) ==
    /\ phase = "active"
    /\ chunk \in targetRows \ workspaceRows
    /\ workspaceRows' = workspaceRows \cup {chunk}
    /\ lastAcceptedRows' = workspaceRows \cup {chunk}
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<
        canonicalRows, graphChunkCount, documentItemCount,
        commitEpoch, durableHistory, phase, targetRows,
        workspaceGraphCount, workspaceDocumentCount,
        duplicateRejected, lastPublishedTarget
       >>

RejectDuplicateOrder ==
    /\ phase = "active"
    /\ workspaceRows # {}
    /\ statementRejected' = TRUE
    /\ duplicateRejected' = TRUE
    /\ UNCHANGED <<
        canonicalRows, graphChunkCount, documentItemCount,
        commitEpoch, durableHistory, phase, targetRows,
        workspaceRows, workspaceGraphCount, workspaceDocumentCount,
        lastAcceptedRows, lastPublishedTarget
       >>

UpdateDocumentSummary ==
    /\ phase = "active"
    /\ workspaceRows = targetRows
    /\ phase' = "summarized"
    /\ workspaceDocumentCount' = Cardinality(workspaceRows)
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<
        canonicalRows, graphChunkCount, documentItemCount,
        commitEpoch, durableHistory, targetRows, workspaceRows,
        workspaceGraphCount, lastAcceptedRows, duplicateRejected,
        lastPublishedTarget
       >>

MakeDurable ==
    /\ phase = "summarized"
    /\ durableHistory' = Append(
        durableHistory,
        [rows |-> workspaceRows,
         graphCount |-> workspaceGraphCount,
         documentCount |-> workspaceDocumentCount]
       )
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonicalRows, graphChunkCount, documentItemCount,
        commitEpoch, targetRows, workspaceRows, workspaceGraphCount,
        workspaceDocumentCount, lastAcceptedRows, statementRejected,
        duplicateRejected, lastPublishedTarget
       >>

Publish ==
    /\ phase = "durable"
    /\ canonicalRows' = workspaceRows
    /\ graphChunkCount' = workspaceGraphCount
    /\ documentItemCount' = workspaceDocumentCount
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ statementRejected' = FALSE
    /\ duplicateRejected' = FALSE
    /\ lastPublishedTarget' = targetRows
    /\ UNCHANGED <<
        durableHistory, targetRows, workspaceRows, workspaceGraphCount,
        workspaceDocumentCount, lastAcceptedRows
       >>

Rollback ==
    /\ phase \in {"active", "summarized"}
    /\ phase' = "idle"
    /\ targetRows' = canonicalRows
    /\ workspaceRows' = canonicalRows
    /\ workspaceGraphCount' = graphChunkCount
    /\ workspaceDocumentCount' = documentItemCount
    /\ lastAcceptedRows' = canonicalRows
    /\ statementRejected' = FALSE
    /\ duplicateRejected' = FALSE
    /\ UNCHANGED <<
        canonicalRows, graphChunkCount, documentItemCount,
        commitEpoch, durableHistory, lastPublishedTarget
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recovered == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonicalRows' = recovered.rows
    /\ graphChunkCount' = recovered.graphCount
    /\ documentItemCount' = recovered.documentCount
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ targetRows' = recovered.rows
    /\ workspaceRows' = recovered.rows
    /\ workspaceGraphCount' = recovered.graphCount
    /\ workspaceDocumentCount' = recovered.documentCount
    /\ lastAcceptedRows' = recovered.rows
    /\ statementRejected' = FALSE
    /\ duplicateRejected' = FALSE
    /\ lastPublishedTarget' = recovered.rows
    /\ UNCHANGED durableHistory

Next ==
    \/ \E target \in ChunkSets: BeginReplacement(target)
    \/ \E chunk \in Chunks: InsertChunk(chunk)
    \/ RejectDuplicateOrder
    \/ UpdateDocumentSummary
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalRows \in ChunkSets
    /\ graphChunkCount \in 0..Cardinality(Chunks)
    /\ documentItemCount \in 0..Cardinality(Chunks)
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(SourceStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ targetRows \in ChunkSets
    /\ workspaceRows \in ChunkSets
    /\ workspaceGraphCount \in 0..Cardinality(Chunks)
    /\ workspaceDocumentCount \in 0..Cardinality(Chunks)
    /\ lastAcceptedRows \in ChunkSets
    /\ statementRejected \in BOOLEAN
    /\ duplicateRejected \in BOOLEAN
    /\ lastPublishedTarget \in ChunkSets

VisibilityFollowsDurability ==
    /\ commitEpoch <= Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ LET current == HistoryState(durableHistory, commitEpoch) IN
       /\ canonicalRows = current.rows
       /\ graphChunkCount = current.graphCount
       /\ documentItemCount = current.documentCount

CanonicalSourceIsConsistent ==
    /\ graphChunkCount = Cardinality(canonicalRows)
    /\ documentItemCount = Cardinality(canonicalRows)

PreparedReplacementIsExact ==
    phase \in {"summarized", "durable"} =>
        /\ workspaceRows = targetRows
        /\ workspaceGraphCount = Cardinality(targetRows)
        /\ workspaceDocumentCount = Cardinality(targetRows)

RejectedStatementIsAtomic ==
    statementRejected =>
        /\ phase = "active"
        /\ workspaceRows = lastAcceptedRows

PublishedReplacementIsExact ==
    phase = "idle" => canonicalRows = lastPublishedTarget

EmptyReplacementClearsEveryCount ==
    lastPublishedTarget = {} =>
        /\ canonicalRows = {}
        /\ graphChunkCount = 0
        /\ documentItemCount = 0

=============================================================================
