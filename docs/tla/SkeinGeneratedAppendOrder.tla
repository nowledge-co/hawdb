-------------------- MODULE SkeinGeneratedAppendOrder --------------------
EXTENDS FiniteSets, Naturals, Sequences

(***************************************************************************)
(* Generated strict-append order values are allocated only when the serial  *)
(* commit sequencer removes a request from the queue. A request contributes  *)
(* one contiguous interval to the table-wide WAL history. The interval does  *)
(* not become visible until the shared durability barrier completes. Abort  *)
(* and exhaustion leave the allocator unchanged. Recovery retains an exact  *)
(* durable WAL prefix and resets the next value from that prefix.             *)
(***************************************************************************)

CONSTANTS Requests, MaxSequence, MaxRows, MaxGroup

ASSUME /\ Requests # {}
       /\ MaxSequence \in Nat \ {0}
       /\ MaxRows \in Nat \ {0}
       /\ MaxGroup \in Nat \ {0}

RequestPhases == {
    "idle", "queued", "written", "synced", "completed",
    "aborted", "exhausted", "failed"
}

AssignmentType == [request : Requests, first : 1..MaxSequence, last : 1..MaxSequence]

VARIABLES
    requestPhase,
    rowCount,
    assignedFirst,
    assignedLast,
    queue,
    group,
    walHistory,
    durableCount,
    visibleCount,
    nextValue,
    poisoned

vars == <<
    requestPhase,
    rowCount,
    assignedFirst,
    assignedLast,
    queue,
    group,
    walHistory,
    durableCount,
    visibleCount,
    nextValue,
    poisoned
>>

SeqSet(sequence) == {sequence[index] : index \in 1..Len(sequence)}

HistoryRequests(history, count) ==
    {history[index].request : index \in 1..count}

HistoryEnd(history, count) ==
    IF count = 0 THEN 0 ELSE history[count].last

Prefix(history, count) ==
    IF count = 0 THEN <<>> ELSE SubSeq(history, 1, count)

Init ==
    /\ requestPhase = [request \in Requests |-> "idle"]
    /\ rowCount = [request \in Requests |-> 0]
    /\ assignedFirst = [request \in Requests |-> 0]
    /\ assignedLast = [request \in Requests |-> 0]
    /\ queue = <<>>
    /\ group = <<>>
    /\ walHistory = <<>>
    /\ durableCount = 0
    /\ visibleCount = 0
    /\ nextValue = 1
    /\ poisoned = FALSE

Submit(request, rows) ==
    /\ ~poisoned
    /\ requestPhase[request] = "idle"
    /\ rows \in 1..MaxRows
    /\ requestPhase' = [requestPhase EXCEPT ![request] = "queued"]
    /\ rowCount' = [rowCount EXCEPT ![request] = rows]
    /\ queue' = Append(queue, request)
    /\ UNCHANGED <<assignedFirst, assignedLast, group, walHistory,
                    durableCount, visibleCount, nextValue, poisoned>>

AbortHead ==
    LET request == Head(queue) IN
    /\ ~poisoned
    /\ queue # <<>>
    /\ requestPhase[request] = "queued"
    /\ requestPhase' = [requestPhase EXCEPT ![request] = "aborted"]
    /\ queue' = Tail(queue)
    /\ UNCHANGED <<rowCount, assignedFirst, assignedLast, group, walHistory,
                    durableCount, visibleCount, nextValue, poisoned>>

RejectExhaustedHead ==
    LET request == Head(queue) IN
    /\ ~poisoned
    /\ queue # <<>>
    /\ requestPhase[request] = "queued"
    /\ nextValue + rowCount[request] - 1 > MaxSequence
    /\ requestPhase' = [requestPhase EXCEPT ![request] = "exhausted"]
    /\ queue' = Tail(queue)
    /\ UNCHANGED <<rowCount, assignedFirst, assignedLast, group, walHistory,
                    durableCount, visibleCount, nextValue, poisoned>>

WriteHeadAtCommitBoundary ==
    LET request == Head(queue) IN
    LET last == nextValue + rowCount[request] - 1 IN
    /\ ~poisoned
    /\ queue # <<>>
    /\ requestPhase[request] = "queued"
    /\ Len(group) < MaxGroup
    /\ last <= MaxSequence
    /\ requestPhase' = [requestPhase EXCEPT ![request] = "written"]
    /\ assignedFirst' = [assignedFirst EXCEPT ![request] = nextValue]
    /\ assignedLast' = [assignedLast EXCEPT ![request] = last]
    /\ queue' = Tail(queue)
    /\ group' = Append(group, request)
    /\ walHistory' = Append(walHistory,
            [request |-> request, first |-> nextValue, last |-> last])
    /\ nextValue' = last + 1
    /\ UNCHANGED <<rowCount, durableCount, visibleCount, poisoned>>

SyncGroup ==
    /\ ~poisoned
    /\ group # <<>>
    /\ \A request \in SeqSet(group): requestPhase[request] = "written"
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in SeqSet(group) THEN "synced" ELSE requestPhase[request]]
    /\ durableCount' = Len(walHistory)
    /\ visibleCount' = Len(walHistory)
    /\ UNCHANGED <<rowCount, assignedFirst, assignedLast, queue, group,
                    walHistory, nextValue, poisoned>>

CompleteGroup ==
    /\ ~poisoned
    /\ group # <<>>
    /\ \A request \in SeqSet(group): requestPhase[request] = "synced"
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in SeqSet(group) THEN "completed" ELSE requestPhase[request]]
    /\ group' = <<>>
    /\ UNCHANGED <<rowCount, assignedFirst, assignedLast, queue, walHistory,
                    durableCount, visibleCount, nextValue, poisoned>>

FailGroupSync ==
    /\ ~poisoned
    /\ group # <<>>
    /\ \A request \in SeqSet(group): requestPhase[request] = "written"
    /\ requestPhase' =
        [request \in Requests |->
            IF request \in SeqSet(group) THEN "failed" ELSE requestPhase[request]]
    /\ poisoned' = TRUE
    /\ UNCHANGED <<rowCount, assignedFirst, assignedLast, queue, group,
                    walHistory, durableCount, visibleCount, nextValue>>

Recover ==
    /\ \E recoveredCount \in durableCount..Len(walHistory):
        LET durableRequests == HistoryRequests(walHistory, recoveredCount) IN
        /\ requestPhase' =
            [request \in Requests |->
                IF request \in durableRequests
                THEN "completed"
                ELSE IF requestPhase[request] \in {"queued", "written", "synced", "failed"}
                THEN "failed"
                ELSE requestPhase[request]]
        /\ assignedFirst' =
            [request \in Requests |->
                IF request \in durableRequests THEN assignedFirst[request] ELSE 0]
        /\ assignedLast' =
            [request \in Requests |->
                IF request \in durableRequests THEN assignedLast[request] ELSE 0]
        /\ walHistory' = Prefix(walHistory, recoveredCount)
        /\ durableCount' = recoveredCount
        /\ visibleCount' = recoveredCount
        /\ nextValue' = HistoryEnd(walHistory, recoveredCount) + 1
    /\ queue' = <<>>
    /\ group' = <<>>
    /\ poisoned' = FALSE
    /\ UNCHANGED rowCount

Next ==
    \/ \E request \in Requests: \E rows \in 1..MaxRows: Submit(request, rows)
    \/ AbortHead
    \/ RejectExhaustedHead
    \/ WriteHeadAtCommitBoundary
    \/ SyncGroup
    \/ CompleteGroup
    \/ FailGroupSync
    \/ Recover

TypeOK ==
    /\ requestPhase \in [Requests -> RequestPhases]
    /\ rowCount \in [Requests -> 0..MaxRows]
    /\ assignedFirst \in [Requests -> 0..MaxSequence]
    /\ assignedLast \in [Requests -> 0..MaxSequence]
    /\ queue \in Seq(Requests)
    /\ group \in Seq(Requests)
    /\ walHistory \in Seq(AssignmentType)
    /\ durableCount \in 0..Len(walHistory)
    /\ visibleCount \in 0..Len(walHistory)
    /\ nextValue \in 1..(MaxSequence + 1)
    /\ poisoned \in BOOLEAN

HistoryIsContiguous ==
    \A index \in 1..Len(walHistory):
        /\ walHistory[index].first =
            IF index = 1 THEN 1 ELSE walHistory[index - 1].last + 1
        /\ walHistory[index].last =
            walHistory[index].first + rowCount[walHistory[index].request] - 1

HistoryHasUniqueRequests ==
    Cardinality(HistoryRequests(walHistory, Len(walHistory))) = Len(walHistory)

AllocatorMatchesWalTail ==
    nextValue = HistoryEnd(walHistory, Len(walHistory)) + 1

VisibilityIsExactDurablePrefix == visibleCount = durableCount

CompletedOnlyAfterDurability ==
    \A request \in Requests:
        requestPhase[request] \in {"synced", "completed"}
        => request \in HistoryRequests(walHistory, visibleCount)

PendingAssignmentsAreNotVisible ==
    \A request \in Requests:
        requestPhase[request] = "written"
        => request \notin HistoryRequests(walHistory, visibleCount)

AbortAndExhaustionConsumeNothing ==
    \A request \in Requests:
        requestPhase[request] \in {"aborted", "exhausted"}
        => /\ assignedFirst[request] = 0
           /\ assignedLast[request] = 0

GroupIsBounded == Len(group) <= MaxGroup

Spec == Init /\ [][Next]_vars

=============================================================================
