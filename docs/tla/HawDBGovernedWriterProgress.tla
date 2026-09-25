------------------- MODULE HawDBGovernedWriterProgress -------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Small, Large, Capacity, LargeSteps, AllowBypass, FairService
Actors == Small \union {Large}
ASSUME /\ Small # {} /\ Large \notin Small
       /\ Capacity = Cardinality(Small) /\ Capacity > 1
       /\ LargeSteps \in Nat \ {0}
       /\ {AllowBypass, FairService} \subseteq BOOLEAN

VARIABLES phase, remaining, queue, largeVisible, bypassed, finishedSmall, repeatedSmall
vars == <<phase, remaining, queue, largeVisible, bypassed, finishedSmall, repeatedSmall>>
Holders == {a \in Actors : phase[a] \in {"work", "durable"}}
Used == Cardinality(Holders \intersect Small) + IF Large \in Holders THEN Capacity ELSE 0
Weight(a) == IF a = Large THEN Capacity ELSE 1

(* Two older small owners are already active when the large writer queues. *)
Init ==
    /\ phase = [a \in Actors |-> IF a = Large THEN "waiting" ELSE "work"]
    /\ remaining = [a \in Actors |-> IF a = Large THEN 0 ELSE 1]
    /\ queue = <<Large>>
    /\ largeVisible = FALSE /\ bypassed = FALSE
    /\ finishedSmall = {} /\ repeatedSmall = FALSE

Enqueue(a) ==
    /\ a \in Small /\ phase[a] = "idle"
    /\ phase' = [phase EXCEPT ![a] = "waiting"]
    /\ queue' = Append(queue, a)
    /\ UNCHANGED <<remaining, largeVisible, bypassed, finishedSmall, repeatedSmall>>

Grant(a) ==
    /\ phase[a] = "waiting"
    /\ AllowBypass \/ Head(queue) = a
    /\ Used + Weight(a) <= Capacity
    /\ phase' = [phase EXCEPT ![a] = "work"]
    /\ remaining' = [remaining EXCEPT ![a] = IF a = Large THEN LargeSteps ELSE 1]
    /\ LET i == CHOOSE j \in 1..Len(queue): queue[j] = a
       IN queue' = SubSeq(queue, 1, i - 1) \o SubSeq(queue, i + 1, Len(queue))
    /\ bypassed' = (bypassed \/ (a \in Small /\ phase[Large] = "waiting"))
    /\ repeatedSmall' = (repeatedSmall \/ (a \in finishedSmall /\ largeVisible))
    /\ UNCHANGED <<largeVisible, finishedSmall>>

Work(a) ==
    /\ phase[a] = "work" /\ remaining[a] > 0
    /\ remaining' = [remaining EXCEPT ![a] = @ - 1]
    /\ UNCHANGED <<phase, queue, largeVisible, bypassed, finishedSmall, repeatedSmall>>

(* Successful serialized validation/WAL/sync abstracted as one step; the
   admitted owner remains charged until result delivery and retirement. *)
Commit(a) ==
    /\ phase[a] = "work" /\ remaining[a] = 0
    /\ phase' = [phase EXCEPT ![a] = "durable"]
    /\ largeVisible' = (largeVisible \/ a = Large)
    /\ UNCHANGED <<remaining, queue, bypassed, finishedSmall, repeatedSmall>>

Retire(a) ==
    /\ phase[a] = "durable"
    /\ phase' = [phase EXCEPT ![a] = IF a = Large THEN "done" ELSE "idle"]
    /\ finishedSmall' = finishedSmall \union ({a} \intersect Small)
    /\ UNCHANGED <<remaining, queue, largeVisible, bypassed, repeatedSmall>>

Next == \E a \in Actors: Enqueue(a) \/ Grant(a) \/ Work(a) \/ Commit(a) \/ Retire(a)
TypeInvariant ==
    /\ phase \in [Actors -> {"idle", "waiting", "work", "durable", "done"}]
    /\ remaining \in [Actors -> 0..LargeSteps]
    /\ queue \in Seq(Actors) /\ Len(queue) <= Cardinality(Actors)
    /\ {queue[i]: i \in 1..Len(queue)} = {a \in Actors: phase[a] = "waiting"}
    /\ Cardinality({queue[i]: i \in 1..Len(queue)}) = Len(queue)
    /\ {largeVisible, bypassed, repeatedSmall} \subseteq BOOLEAN
    /\ finishedSmall \subseteq Small
CapacityBounded == Used <= Capacity
NoBypass == ~bypassed
LargeOwnsCapacity == Large \in Holders => Holders = {Large}
WholeLargePublication == largeVisible <=> phase[Large] \in {"durable", "done"}
LargeCompletes == <> (phase[Large] = "done")

Spec == Init /\ [][Next]_vars
        /\ (\A a \in Actors: WF_vars(Grant(a)))
        /\ (IF FairService THEN
              \A a \in Actors: WF_vars(Work(a)) /\ WF_vars(Commit(a)) /\ WF_vars(Retire(a))
            ELSE TRUE)

NoYoungWaiterWitness == ~(phase[Large] = "waiting" /\ Len(queue) > 1)
NoRepeatedSmallWitness == ~repeatedSmall
=============================================================================
