---------------- MODULE HawDBContentSourceOwnershipMove ----------------
EXTENDS Naturals, Sequences

(***************************************************************************)
(* A Source workspace move updates graph ownership and the relational      *)
(* content-document owner inside one mixed transaction. The chunk set and  *)
(* payload identity are immutable inputs to this operation. Publication is  *)
(* allowed only after both ownership writes are durable. A missing Source   *)
(* is a no-op and cannot advance the canonical commit epoch.                *)
(***************************************************************************)

CONSTANTS MaxEpoch, ChunkCount, PayloadDigest

ASSUME MaxEpoch \in Nat \ {0}
ASSUME ChunkCount \in Nat \ {0}

Spaces == {"default", "work"}
Phases == {"idle", "graph_staged", "ready", "durable"}

InitialSource ==
    [graphSpace |-> "default",
     documentSpace |-> "default",
     chunkCount |-> ChunkCount,
     payloadDigest |-> PayloadDigest]

SourceStates ==
    [graphSpace : Spaces,
     documentSpace : Spaces,
     chunkCount : {ChunkCount},
     payloadDigest : {PayloadDigest}]

HistoryState(history, epoch) ==
    IF epoch = 0 THEN InitialSource ELSE history[epoch]

VARIABLES
    canonicalGraphSpace,
    canonicalDocumentSpace,
    canonicalChunkCount,
    canonicalPayloadDigest,
    commitEpoch,
    durableHistory,
    phase,
    targetSpace,
    workspaceGraphSpace,
    workspaceDocumentSpace,
    workspaceChunkCount,
    workspacePayloadDigest,
    lastPublishedSpace,
    missingNoopObserved,
    missingNoopGraphSpace,
    missingNoopDocumentSpace,
    missingNoopEpoch

vars == <<
    canonicalGraphSpace,
    canonicalDocumentSpace,
    canonicalChunkCount,
    canonicalPayloadDigest,
    commitEpoch,
    durableHistory,
    phase,
    targetSpace,
    workspaceGraphSpace,
    workspaceDocumentSpace,
    workspaceChunkCount,
    workspacePayloadDigest,
    lastPublishedSpace,
    missingNoopObserved,
    missingNoopGraphSpace,
    missingNoopDocumentSpace,
    missingNoopEpoch
>>

Init ==
    /\ canonicalGraphSpace = "default"
    /\ canonicalDocumentSpace = "default"
    /\ canonicalChunkCount = ChunkCount
    /\ canonicalPayloadDigest = PayloadDigest
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ targetSpace = "default"
    /\ workspaceGraphSpace = "default"
    /\ workspaceDocumentSpace = "default"
    /\ workspaceChunkCount = ChunkCount
    /\ workspacePayloadDigest = PayloadDigest
    /\ lastPublishedSpace = "default"
    /\ missingNoopObserved = FALSE
    /\ missingNoopGraphSpace = "default"
    /\ missingNoopDocumentSpace = "default"
    /\ missingNoopEpoch = 0

StageGraphMove(target) ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ target \in Spaces \ {canonicalGraphSpace}
    /\ phase' = "graph_staged"
    /\ targetSpace' = target
    /\ workspaceGraphSpace' = target
    /\ workspaceDocumentSpace' = canonicalDocumentSpace
    /\ workspaceChunkCount' = canonicalChunkCount
    /\ workspacePayloadDigest' = canonicalPayloadDigest
    /\ missingNoopObserved' = FALSE
    /\ UNCHANGED <<
        canonicalGraphSpace, canonicalDocumentSpace,
        canonicalChunkCount, canonicalPayloadDigest,
        commitEpoch, durableHistory, lastPublishedSpace,
        missingNoopGraphSpace, missingNoopDocumentSpace, missingNoopEpoch
       >>

StageDocumentMove ==
    /\ phase = "graph_staged"
    /\ phase' = "ready"
    /\ workspaceDocumentSpace' = targetSpace
    /\ UNCHANGED <<
        canonicalGraphSpace, canonicalDocumentSpace,
        canonicalChunkCount, canonicalPayloadDigest,
        commitEpoch, durableHistory, targetSpace,
        workspaceGraphSpace, workspaceChunkCount, workspacePayloadDigest,
        lastPublishedSpace, missingNoopObserved,
        missingNoopGraphSpace, missingNoopDocumentSpace, missingNoopEpoch
       >>

MakeDurable ==
    /\ phase = "ready"
    /\ durableHistory' = Append(
        durableHistory,
        [graphSpace |-> workspaceGraphSpace,
         documentSpace |-> workspaceDocumentSpace,
         chunkCount |-> workspaceChunkCount,
         payloadDigest |-> workspacePayloadDigest]
       )
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonicalGraphSpace, canonicalDocumentSpace,
        canonicalChunkCount, canonicalPayloadDigest,
        commitEpoch, targetSpace,
        workspaceGraphSpace, workspaceDocumentSpace,
        workspaceChunkCount, workspacePayloadDigest,
        lastPublishedSpace, missingNoopObserved,
        missingNoopGraphSpace, missingNoopDocumentSpace, missingNoopEpoch
       >>

Publish ==
    /\ phase = "durable"
    /\ canonicalGraphSpace' = workspaceGraphSpace
    /\ canonicalDocumentSpace' = workspaceDocumentSpace
    /\ canonicalChunkCount' = workspaceChunkCount
    /\ canonicalPayloadDigest' = workspacePayloadDigest
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ lastPublishedSpace' = targetSpace
    /\ UNCHANGED <<
        durableHistory, targetSpace,
        workspaceGraphSpace, workspaceDocumentSpace,
        workspaceChunkCount, workspacePayloadDigest,
        missingNoopObserved,
        missingNoopGraphSpace, missingNoopDocumentSpace, missingNoopEpoch
       >>

Rollback ==
    /\ phase \in {"graph_staged", "ready"}
    /\ phase' = "idle"
    /\ targetSpace' = canonicalGraphSpace
    /\ workspaceGraphSpace' = canonicalGraphSpace
    /\ workspaceDocumentSpace' = canonicalDocumentSpace
    /\ workspaceChunkCount' = canonicalChunkCount
    /\ workspacePayloadDigest' = canonicalPayloadDigest
    /\ UNCHANGED <<
        canonicalGraphSpace, canonicalDocumentSpace,
        canonicalChunkCount, canonicalPayloadDigest,
        commitEpoch, durableHistory, lastPublishedSpace,
        missingNoopObserved,
        missingNoopGraphSpace, missingNoopDocumentSpace, missingNoopEpoch
       >>

MissingSourceNoop ==
    /\ phase = "idle"
    /\ ~missingNoopObserved
    /\ missingNoopObserved' = TRUE
    /\ missingNoopGraphSpace' = canonicalGraphSpace
    /\ missingNoopDocumentSpace' = canonicalDocumentSpace
    /\ missingNoopEpoch' = commitEpoch
    /\ UNCHANGED <<
        canonicalGraphSpace, canonicalDocumentSpace,
        canonicalChunkCount, canonicalPayloadDigest,
        commitEpoch, durableHistory, phase, targetSpace,
        workspaceGraphSpace, workspaceDocumentSpace,
        workspaceChunkCount, workspacePayloadDigest, lastPublishedSpace
       >>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recovered == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonicalGraphSpace' = recovered.graphSpace
    /\ canonicalDocumentSpace' = recovered.documentSpace
    /\ canonicalChunkCount' = recovered.chunkCount
    /\ canonicalPayloadDigest' = recovered.payloadDigest
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ targetSpace' = recovered.graphSpace
    /\ workspaceGraphSpace' = recovered.graphSpace
    /\ workspaceDocumentSpace' = recovered.documentSpace
    /\ workspaceChunkCount' = recovered.chunkCount
    /\ workspacePayloadDigest' = recovered.payloadDigest
    /\ lastPublishedSpace' = recovered.graphSpace
    /\ missingNoopObserved' = FALSE
    /\ UNCHANGED <<
        durableHistory,
        missingNoopGraphSpace, missingNoopDocumentSpace, missingNoopEpoch
       >>

Next ==
    \/ \E target \in Spaces: StageGraphMove(target)
    \/ StageDocumentMove
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ MissingSourceNoop
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonicalGraphSpace \in Spaces
    /\ canonicalDocumentSpace \in Spaces
    /\ canonicalChunkCount = ChunkCount
    /\ canonicalPayloadDigest = PayloadDigest
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(SourceStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ targetSpace \in Spaces
    /\ workspaceGraphSpace \in Spaces
    /\ workspaceDocumentSpace \in Spaces
    /\ workspaceChunkCount = ChunkCount
    /\ workspacePayloadDigest = PayloadDigest
    /\ lastPublishedSpace \in Spaces
    /\ missingNoopObserved \in BOOLEAN
    /\ missingNoopGraphSpace \in Spaces
    /\ missingNoopDocumentSpace \in Spaces
    /\ missingNoopEpoch \in 0..MaxEpoch

VisibilityFollowsDurability ==
    /\ commitEpoch <= Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ LET current == HistoryState(durableHistory, commitEpoch) IN
       /\ canonicalGraphSpace = current.graphSpace
       /\ canonicalDocumentSpace = current.documentSpace
       /\ canonicalChunkCount = current.chunkCount
       /\ canonicalPayloadDigest = current.payloadDigest

CanonicalOwnershipIsAtomic ==
    canonicalGraphSpace = canonicalDocumentSpace

OwnershipMovePreservesContent ==
    /\ canonicalChunkCount = ChunkCount
    /\ canonicalPayloadDigest = PayloadDigest

PreparedMoveIsComplete ==
    phase \in {"ready", "durable"} =>
        /\ workspaceGraphSpace = targetSpace
        /\ workspaceDocumentSpace = targetSpace
        /\ workspaceChunkCount = ChunkCount
        /\ workspacePayloadDigest = PayloadDigest

PublishedMoveIsExact ==
    phase = "idle" =>
        /\ canonicalGraphSpace = lastPublishedSpace
        /\ canonicalDocumentSpace = lastPublishedSpace

MissingSourceDoesNotPublish ==
    missingNoopObserved =>
        /\ phase = "idle"
        /\ canonicalGraphSpace = missingNoopGraphSpace
        /\ canonicalDocumentSpace = missingNoopDocumentSpace
        /\ commitEpoch = missingNoopEpoch

=============================================================================
