---------------- MODULE HawDBGovernedConflictRetry ----------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Small, Large, Capacity, LargeSteps, Escalate, PublishRejected
Actors == Small \union {Large}
ASSUME /\ Cardinality(Small) = Capacity /\ Capacity > 1 /\ Large \notin Small
       /\ LargeSteps \in Nat \ {0} /\ {Escalate, PublishRejected} \subseteq BOOLEAN
VARIABLES phase, remaining, queue, retried, stale, largeVisible, repeatedConflict
vars == <<phase, remaining, queue, retried, stale, largeVisible, repeatedConflict>>
Holders == {a \in Actors: phase[a] \in {"work", "durable"}}
LargeWeight == IF Escalate /\ retried THEN Capacity ELSE 1
Weight(a) == IF a = Large THEN LargeWeight ELSE 1
Used == Cardinality(Holders \intersect Small) + IF Large \in Holders THEN LargeWeight ELSE 0
Init ==
    /\ LET owner == CHOOSE a \in Small: TRUE
       IN /\ phase = [a \in Actors |-> IF a \in {Large, owner} THEN "work" ELSE "idle"]
          /\ remaining = [a \in Actors |-> IF a = Large THEN LargeSteps ELSE IF a = owner THEN 1 ELSE 0]
    /\ queue = <<>> /\ retried = FALSE /\ stale = FALSE
    /\ largeVisible = FALSE /\ repeatedConflict = FALSE
Enqueue(a) ==
    /\ a \in Small /\ phase[a] = "idle"
    /\ phase' = [phase EXCEPT ![a] = "waiting"] /\ queue' = Append(queue, a)
    /\ UNCHANGED <<remaining, retried, stale, largeVisible, repeatedConflict>>
Grant(a) ==
    /\ phase[a] = "waiting" /\ Head(queue) = a /\ Used + Weight(a) <= Capacity
    /\ phase' = [phase EXCEPT ![a] = "work"] /\ queue' = Tail(queue)
    /\ remaining' = [remaining EXCEPT ![a] = IF a = Large THEN LargeSteps ELSE 1]
    /\ stale' = IF a = Large THEN FALSE ELSE stale
    /\ UNCHANGED <<retried, largeVisible, repeatedConflict>>
Work(a) ==
    /\ phase[a] = "work" /\ remaining[a] > 0
    /\ remaining' = [remaining EXCEPT ![a] = @ - 1]
    /\ UNCHANGED <<phase, queue, retried, stale, largeVisible, repeatedConflict>>
SmallCommit(a) ==
    /\ a \in Small /\ phase[a] = "work" /\ remaining[a] = 0
    /\ phase' = [phase EXCEPT ![a] = "durable"]
    /\ stale' = (stale \/ phase[Large] = "work")
    /\ UNCHANGED <<remaining, queue, retried, largeVisible, repeatedConflict>>
Conflict ==
    /\ phase[Large] = "work" /\ remaining[Large] = 0 /\ stale
    /\ phase' = [phase EXCEPT ![Large] = "waiting"] /\ queue' = Append(queue, Large)
    /\ repeatedConflict' = (repeatedConflict \/ retried)
    /\ retried' = TRUE /\ stale' = FALSE
    /\ largeVisible' = (largeVisible \/ PublishRejected)
    /\ UNCHANGED remaining
LargeCommit ==
    /\ phase[Large] = "work" /\ remaining[Large] = 0 /\ ~stale
    /\ phase' = [phase EXCEPT ![Large] = "durable"] /\ largeVisible' = TRUE
    /\ UNCHANGED <<remaining, queue, retried, stale, repeatedConflict>>
Retire(a) ==
    /\ phase[a] = "durable"
    /\ phase' = [phase EXCEPT ![a] = IF a = Large THEN "done" ELSE "idle"]
    /\ UNCHANGED <<remaining, queue, retried, stale, largeVisible, repeatedConflict>>
Next == Conflict \/ LargeCommit \/
        (\E a \in Actors: Enqueue(a) \/ Grant(a) \/ Work(a) \/ SmallCommit(a) \/ Retire(a))
TypeInvariant ==
    /\ phase \in [Actors -> {"idle", "waiting", "work", "durable", "done"}]
    /\ remaining \in [Actors -> 0..LargeSteps]
    /\ queue \in Seq(Actors) /\ Len(queue) <= Cardinality(Actors)
    /\ {queue[i]: i \in 1..Len(queue)} = {a \in Actors: phase[a] = "waiting"}
    /\ Cardinality({queue[i]: i \in 1..Len(queue)}) = Len(queue)
    /\ {retried, stale, largeVisible, repeatedConflict} \subseteq BOOLEAN
CapacityBounded == Used <= Capacity
AtMostOneConflict == ~repeatedConflict
RejectedNeverPublished == largeVisible <=> phase[Large] \in {"durable", "done"}
RetryOwnsCapacity == retried /\ Large \in Holders => Holders = {Large}
LargeCompletes == <> (phase[Large] = "done")
Spec == Init /\ [][Next]_vars /\ WF_vars(Conflict) /\ WF_vars(LargeCommit)
        /\ (\A a \in Actors: WF_vars(Grant(a)) /\ WF_vars(Work(a))
                             /\ WF_vars(SmallCommit(a)) /\ WF_vars(Retire(a)))
NoConflictWitness == ~retried
NoRetryCommitWitness == ~(retried /\ largeVisible)
====================================================================
