------------------------ MODULE HawDBWalGroupCommit ------------------------
EXTENDS FiniteSets, Integers, Naturals, Sequences

CONSTANT Requests, HeavyRequest, MaxGroupSize, MaxGroupBytes, MaxRequestBytes

ASSUME Requests # {}
       /\ HeavyRequest \in Requests
       /\ MaxGroupSize \in Nat \ {0}
       /\ MaxGroupBytes \in Nat \ {0}
       /\ MaxRequestBytes \in Nat \ {0}

VARIABLES requestPhase,
          requestLsn,
          queue,
          group,
          groupPhase,
          leaderActive,
          nextLsn,
          durableLsn,
          visibleLsn,
          databasePoisoned

vars == <<requestPhase,
          requestLsn,
          queue,
          group,
          groupPhase,
          leaderActive,
          nextLsn,
          durableLsn,
          visibleLsn,
          databasePoisoned>>

RequestPhases == {"idle", "queued", "applied", "synced", "completed", "failed"}
GroupPhases == {"none", "collecting", "sealed", "durable"}
TerminalPhases == {"completed", "failed"}

RequestBytes(request) == IF request = HeavyRequest THEN MaxRequestBytes ELSE 1

RECURSIVE SequenceBytes(_)
SequenceBytes(sequence) ==
    IF sequence = <<>>
      THEN 0
      ELSE RequestBytes(Head(sequence)) + SequenceBytes(Tail(sequence))

SeqSet(sequence) ==
    {sequence[index] : index \in 1..Len(sequence)}

QueueRequests == SeqSet(queue)
GroupRequests == SeqSet(group)
AssignedRequests == {request \in Requests : requestLsn[request] > 0}
AssignedLsns == {requestLsn[request] : request \in AssignedRequests}

Init ==
    /\ requestPhase = [request \in Requests |-> "idle"]
    /\ requestLsn = [request \in Requests |-> 0]
    /\ queue = <<>>
    /\ group = <<>>
    /\ groupPhase = "none"
    /\ leaderActive = FALSE
    /\ nextLsn = 1
    /\ durableLsn = 0
    /\ visibleLsn = 0
    /\ databasePoisoned = FALSE

Submit(request) ==
    /\ requestPhase[request] = "idle"
    /\ ~databasePoisoned
    /\ requestPhase' = [requestPhase EXCEPT ![request] = "queued"]
    /\ queue' = Append(queue, request)
    /\ UNCHANGED <<requestLsn, group, groupPhase, leaderActive, nextLsn,
                    durableLsn, visibleLsn, databasePoisoned>>

StartGroup ==
    /\ ~databasePoisoned
    /\ ~leaderActive
    /\ groupPhase = "none"
    /\ queue # <<>>
    /\ leaderActive' = TRUE
    /\ groupPhase' = "collecting"
    /\ UNCHANGED <<requestPhase, requestLsn, queue, group, nextLsn,
                    durableLsn, visibleLsn, databasePoisoned>>

ExecuteFront ==
    LET request == Head(queue) IN
    /\ ~databasePoisoned
    /\ leaderActive
    /\ groupPhase = "collecting"
    /\ queue # <<>>
    /\ Len(group) < MaxGroupSize
    /\ SequenceBytes(group) < MaxGroupBytes
    /\ requestPhase[request] = "queued"
    /\ requestPhase' = [requestPhase EXCEPT ![request] = "applied"]
    /\ requestLsn' = [requestLsn EXCEPT ![request] = nextLsn]
    /\ queue' = Tail(queue)
    /\ group' = Append(group, request)
    /\ nextLsn' = nextLsn + 1
    /\ UNCHANGED <<groupPhase, leaderActive, durableLsn, visibleLsn,
                    databasePoisoned>>

SealGroup ==
    /\ ~databasePoisoned
    /\ leaderActive
    /\ groupPhase = "collecting"
    /\ group # <<>>
    /\ \A request \in GroupRequests: requestPhase[request] = "applied"
    /\ groupPhase' = "sealed"
    /\ UNCHANGED <<requestPhase, requestLsn, queue, group, leaderActive,
                    nextLsn, durableLsn, visibleLsn, databasePoisoned>>

SyncGroup ==
    /\ ~databasePoisoned
    /\ leaderActive
    /\ groupPhase = "sealed"
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in GroupRequests THEN "synced" ELSE requestPhase[request]]
    /\ durableLsn' = nextLsn - 1
    /\ visibleLsn' = nextLsn - 1
    /\ groupPhase' = "durable"
    /\ UNCHANGED <<requestLsn, queue, group, leaderActive, nextLsn,
                    databasePoisoned>>

FailGroupSync ==
    /\ ~databasePoisoned
    /\ leaderActive
    /\ groupPhase = "sealed"
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in GroupRequests THEN "failed" ELSE requestPhase[request]]
    /\ group' = <<>>
    /\ groupPhase' = "none"
    /\ leaderActive' = FALSE
    /\ databasePoisoned' = TRUE
    /\ UNCHANGED <<requestLsn, queue, nextLsn, durableLsn, visibleLsn>>

LeaderPanics ==
    /\ ~databasePoisoned
    /\ leaderActive
    /\ groupPhase \in {"collecting", "sealed"}
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in GroupRequests THEN "failed" ELSE requestPhase[request]]
    /\ group' = <<>>
    /\ groupPhase' = "none"
    /\ leaderActive' = FALSE
    /\ databasePoisoned' = TRUE
    /\ UNCHANGED <<requestLsn, queue, nextLsn, durableLsn, visibleLsn>>

CompleteGroup ==
    /\ ~databasePoisoned
    /\ leaderActive
    /\ groupPhase = "durable"
    /\ \A request \in GroupRequests: requestPhase[request] = "synced"
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in GroupRequests THEN "completed" ELSE requestPhase[request]]
    /\ group' = <<>>
    /\ groupPhase' = "none"
    /\ leaderActive' = FALSE
    /\ UNCHANGED <<requestLsn, queue, nextLsn, durableLsn, visibleLsn,
                    databasePoisoned>>

RejectQueuedAfterPoison ==
    /\ databasePoisoned
    /\ queue # <<>>
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in QueueRequests THEN "failed" ELSE requestPhase[request]]
    /\ queue' = <<>>
    /\ UNCHANGED <<requestLsn, group, groupPhase, leaderActive, nextLsn,
                    durableLsn, visibleLsn, databasePoisoned>>

FinishBarrier == SyncGroup \/ FailGroupSync

Next ==
    \/ \E request \in Requests: Submit(request)
    \/ StartGroup
    \/ ExecuteFront
    \/ SealGroup
    \/ FinishBarrier
    \/ LeaderPanics
    \/ CompleteGroup
    \/ RejectQueuedAfterPoison

TypeOK ==
    /\ requestPhase \in [Requests -> RequestPhases]
    /\ requestLsn \in [Requests -> Nat]
    /\ queue \in Seq(Requests)
    /\ group \in Seq(Requests)
    /\ groupPhase \in GroupPhases
    /\ leaderActive \in BOOLEAN
    /\ nextLsn \in Nat \ {0}
    /\ durableLsn \in Nat
    /\ visibleLsn \in Nat
    /\ databasePoisoned \in BOOLEAN

RequestsHaveSingleOwner ==
    /\ Cardinality(QueueRequests) = Len(queue)
    /\ Cardinality(GroupRequests) = Len(group)
    /\ QueueRequests \intersect GroupRequests = {}

QueueAndGroupMatchPhases ==
    /\ \A request \in QueueRequests: requestPhase[request] = "queued"
    /\ \A request \in GroupRequests:
        requestPhase[request] \in {"applied", "synced"}

GroupIsBounded ==
    /\ Len(group) <= MaxGroupSize
    /\ SequenceBytes(group) <= MaxGroupBytes + MaxRequestBytes - 1

LeaderOwnsGroup ==
    /\ (groupPhase = "none" => ~leaderActive /\ group = <<>>)
    /\ (groupPhase # "none" => leaderActive)

AssignedLsnsAreContiguous == AssignedLsns = 1..(nextLsn - 1)

CompletedOnlyAfterSharedSync ==
    \A request \in Requests:
        requestPhase[request] = "completed" =>
            /\ requestLsn[request] > 0
            /\ requestLsn[request] <= durableLsn
            /\ requestLsn[request] <= visibleLsn

VisibleStateIsDurable == visibleLsn = durableLsn

PoisonedGroupIsReleased == databasePoisoned => ~leaderActive /\ group = <<>>

Fairness ==
    /\ WF_vars(StartGroup)
    /\ WF_vars(ExecuteFront)
    /\ WF_vars(SealGroup)
    /\ WF_vars(FinishBarrier)
    /\ WF_vars(CompleteGroup)
    /\ WF_vars(RejectQueuedAfterPoison)

Spec == Init /\ [][Next]_vars /\ Fairness

QueuedRequestTerminates ==
    \A request \in Requests:
        requestPhase[request] = "queued" ~> requestPhase[request] \in TerminalPhases

=============================================================================
