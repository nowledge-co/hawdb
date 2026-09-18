-------------------- MODULE HawDBContentThreadUpsert --------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A Thread content write creates the graph Thread, relational document,   *)
(* message occurrences, and document summary in one mixed transaction. The *)
(* public Thread id and storage id belong to separate identity domains. A   *)
(* conflicting message upsert changes payload but preserves creation time,  *)
(* and a rejected foreign-key statement changes no accepted workspace.      *)
(***************************************************************************)

CONSTANT MaxEpoch

ASSUME MaxEpoch \in Nat \ {0}

Messages == {"a", "b"}
Payloads == {"missing", "v1", "v2"}
CreationTimes == {"missing", "original"}
OwnerIds == {"none", "public", "storage"}
Phases == {"idle", "active", "summarized", "durable"}

EmptyPayloads == [message \in Messages |-> "missing"]
EmptyCreationTimes == [message \in Messages |-> "missing"]

InitialThread ==
    [graph |-> FALSE,
     documentOwner |-> "none",
     documentPayload |-> "missing",
     documentCreated |-> "missing",
     messagePayload |-> EmptyPayloads,
     messageCreated |-> EmptyCreationTimes,
     summaryCount |-> 0]

ThreadStates ==
    [graph : BOOLEAN,
     documentOwner : OwnerIds,
     documentPayload : Payloads,
     documentCreated : CreationTimes,
     messagePayload : [Messages -> Payloads],
     messageCreated : [Messages -> CreationTimes],
     summaryCount : 0..Cardinality(Messages)]

HistoryState(history, epoch) ==
    IF epoch = 0 THEN InitialThread ELSE history[epoch]

CompleteThread(state) ==
    /\ state.graph
    /\ state.documentOwner = "storage"
    /\ state.documentPayload = "v2"
    /\ state.documentCreated = "original"
    /\ \A message \in Messages:
          /\ state.messagePayload[message] # "missing"
          /\ state.messageCreated[message] = "original"
    /\ state.messagePayload["b"] = "v2"
    /\ state.summaryCount = Cardinality(Messages)

VARIABLES
    canonical,
    commitEpoch,
    durableHistory,
    phase,
    workspace,
    lastAcceptedWorkspace,
    statementRejected,
    lastPublished

vars == <<
    canonical,
    commitEpoch,
    durableHistory,
    phase,
    workspace,
    lastAcceptedWorkspace,
    statementRejected,
    lastPublished
>>

Init ==
    /\ canonical = InitialThread
    /\ commitEpoch = 0
    /\ durableHistory = <<>>
    /\ phase = "idle"
    /\ workspace = InitialThread
    /\ lastAcceptedWorkspace = InitialThread
    /\ statementRejected = FALSE
    /\ lastPublished = InitialThread

BeginUpsert ==
    /\ phase = "idle"
    /\ commitEpoch < MaxEpoch
    /\ phase' = "active"
    /\ workspace' = canonical
    /\ lastAcceptedWorkspace' = canonical
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, lastPublished>>

StageGraph ==
    /\ phase = "active"
    /\ ~workspace.graph
    /\ LET next == [workspace EXCEPT !.graph = TRUE] IN
       /\ workspace' = next
       /\ lastAcceptedWorkspace' = next
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, phase, lastPublished>>

StageDocument ==
    /\ phase = "active"
    /\ workspace.documentOwner = "none"
    /\ LET next ==
         [workspace EXCEPT
            !.documentOwner = "storage",
            !.documentPayload = "v1",
            !.documentCreated = "original"]
       IN
       /\ workspace' = next
       /\ lastAcceptedWorkspace' = next
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, phase, lastPublished>>

UpsertDocument ==
    /\ phase = "active"
    /\ workspace.documentOwner = "storage"
    /\ workspace.documentPayload = "v1"
    /\ LET next == [workspace EXCEPT !.documentPayload = "v2"] IN
       /\ workspace' = next
       /\ lastAcceptedWorkspace' = next
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, phase, lastPublished>>

UpsertMessage(message) ==
    /\ phase = "active"
    /\ message \in Messages
    /\ IF workspace.messagePayload[message] = "missing"
          THEN LET next ==
                 [workspace EXCEPT
                    !.messagePayload[message] = "v1",
                    !.messageCreated[message] = "original"]
               IN /\ workspace' = next
                  /\ lastAcceptedWorkspace' = next
          ELSE /\ message = "b"
               /\ LET next ==
                    [workspace EXCEPT !.messagePayload[message] = "v2"]
                  IN /\ workspace' = next
                     /\ lastAcceptedWorkspace' = next
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, phase, lastPublished>>

RejectMissingDocument ==
    /\ phase = "active"
    /\ workspace.messagePayload # EmptyPayloads
    /\ statementRejected' = TRUE
    /\ UNCHANGED <<
        canonical, commitEpoch, durableHistory, phase, workspace,
        lastAcceptedWorkspace, lastPublished
       >>

UpdateSummary ==
    /\ phase = "active"
    /\ workspace.graph
    /\ workspace.documentOwner = "storage"
    /\ workspace.documentPayload = "v2"
    /\ \A message \in Messages: workspace.messagePayload[message] # "missing"
    /\ workspace.messagePayload["b"] = "v2"
    /\ phase' = "summarized"
    /\ workspace' =
         [workspace EXCEPT !.summaryCount = Cardinality(Messages)]
    /\ lastAcceptedWorkspace' = workspace'
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, lastPublished>>

MakeDurable ==
    /\ phase = "summarized"
    /\ CompleteThread(workspace)
    /\ durableHistory' = Append(durableHistory, workspace)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        canonical, commitEpoch, workspace, lastAcceptedWorkspace,
        statementRejected, lastPublished
       >>

Publish ==
    /\ phase = "durable"
    /\ canonical' = workspace
    /\ commitEpoch' = commitEpoch + 1
    /\ phase' = "idle"
    /\ statementRejected' = FALSE
    /\ lastPublished' = workspace
    /\ UNCHANGED <<durableHistory, workspace, lastAcceptedWorkspace>>

Rollback ==
    /\ phase \in {"active", "summarized"}
    /\ phase' = "idle"
    /\ workspace' = canonical
    /\ lastAcceptedWorkspace' = canonical
    /\ statementRejected' = FALSE
    /\ UNCHANGED <<canonical, commitEpoch, durableHistory, lastPublished>>

CrashRecover ==
    LET recoveredEpoch == Len(durableHistory) IN
    LET recovered == HistoryState(durableHistory, recoveredEpoch) IN
    /\ canonical' = recovered
    /\ commitEpoch' = recoveredEpoch
    /\ phase' = "idle"
    /\ workspace' = recovered
    /\ lastAcceptedWorkspace' = recovered
    /\ statementRejected' = FALSE
    /\ lastPublished' = recovered
    /\ UNCHANGED durableHistory

Next ==
    \/ BeginUpsert
    \/ StageGraph
    \/ StageDocument
    \/ UpsertDocument
    \/ \E message \in Messages: UpsertMessage(message)
    \/ RejectMissingDocument
    \/ UpdateSummary
    \/ MakeDurable
    \/ Publish
    \/ Rollback
    \/ CrashRecover

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ canonical \in ThreadStates
    /\ commitEpoch \in 0..MaxEpoch
    /\ durableHistory \in Seq(ThreadStates)
    /\ Len(durableHistory) <= MaxEpoch
    /\ phase \in Phases
    /\ workspace \in ThreadStates
    /\ lastAcceptedWorkspace \in ThreadStates
    /\ statementRejected \in BOOLEAN
    /\ lastPublished \in ThreadStates

VisibilityFollowsDurability ==
    /\ commitEpoch <= Len(durableHistory)
    /\ phase = "durable" => commitEpoch + 1 = Len(durableHistory)
    /\ phase # "durable" => commitEpoch = Len(durableHistory)
    /\ canonical = HistoryState(durableHistory, commitEpoch)

CanonicalThreadIsAtomic ==
    canonical = InitialThread \/ CompleteThread(canonical)

PreparedThreadIsComplete ==
    phase \in {"summarized", "durable"} => CompleteThread(workspace)

RejectedStatementIsAtomic ==
    statementRejected => workspace = lastAcceptedWorkspace

CreationTimeSurvivesConflict ==
    \A state \in {canonical, workspace, lastAcceptedWorkspace, lastPublished}:
        /\ (state.documentPayload # "missing" =>
                state.documentCreated = "original")
        /\ \A message \in Messages:
              state.messagePayload[message] # "missing" =>
                  state.messageCreated[message] = "original"

StorageIdentityIsNeverPublicIdentity ==
    \A state \in {canonical, workspace, lastAcceptedWorkspace, lastPublished}:
        state.documentOwner \in {"none", "storage"}

PublishedUpsertIsExact ==
    phase = "idle" => canonical = lastPublished

=============================================================================
