------------------- MODULE HawDBGovernedWriterProgress -------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Small, Large, Capacity, LargeSteps, AllowBypass, FairService, BackgroundLarge, DisableAging
Actors == Small \union {Large}
ASSUME /\ Small # {} /\ Large \notin Small
       /\ Capacity = Cardinality(Small) /\ Capacity > 1
       /\ LargeSteps \in Nat \ {0}
       /\ {AllowBypass, FairService, BackgroundLarge, DisableAging} \subseteq BOOLEAN

VARIABLES phase, remaining, queue, largeVisible, bypassed, finishedSmall, repeatedSmall, aged
vars == <<phase, remaining, queue, largeVisible, bypassed, finishedSmall, repeatedSmall, aged>>
Holders == {a \in Actors : phase[a] \in {"work", "durable"}}
Used == Cardinality(Holders \intersect Small) + IF Large \in Holders THEN Capacity ELSE 0
Weight(a) == IF a = Large THEN Capacity ELSE 1
SmallQueued == {i \in 1..Len(queue): queue[i] \in Small}
Selected(a) ==
    IF BackgroundLarge /\ ~aged /\ SmallQueued # {}
    THEN LET i == CHOOSE j \in SmallQueued: \A k \in SmallQueued: j <= k
         IN queue[i] = a
    ELSE Head(queue) = a

(* One monotone time transition abstracts crossing the host aging deadline. *)
Age == /\ BackgroundLarge /\ ~DisableAging /\ ~aged
       /\ aged' = TRUE
       /\ UNCHANGED <<phase, remaining, queue, largeVisible, bypassed,
                       finishedSmall, repeatedSmall>>

(* Two older small owners are already active when the large writer queues. *)
Init ==
    /\ phase = [a \in Actors |-> IF a = Large THEN "waiting" ELSE "work"]
    /\ remaining = [a \in Actors |-> IF a = Large THEN 0 ELSE 1]
    /\ queue = <<Large>>
    /\ largeVisible = FALSE /\ bypassed = FALSE
    /\ finishedSmall = {} /\ repeatedSmall = FALSE
    /\ aged = ~BackgroundLarge

Enqueue(a) ==
    /\ a \in Small /\ phase[a] = "idle"
    /\ phase' = [phase EXCEPT ![a] = "waiting"]
    /\ queue' = Append(queue, a)
    /\ UNCHANGED <<remaining, largeVisible, bypassed, finishedSmall, repeatedSmall, aged>>

Grant(a) ==
    /\ phase[a] = "waiting"
    /\ AllowBypass \/ Selected(a)
    /\ Used + Weight(a) <= Capacity
    /\ phase' = [phase EXCEPT ![a] = "work"]
    /\ remaining' = [remaining EXCEPT ![a] = IF a = Large THEN LargeSteps ELSE 1]
    /\ LET i == CHOOSE j \in 1..Len(queue): queue[j] = a
       IN queue' = SubSeq(queue, 1, i - 1) \o SubSeq(queue, i + 1, Len(queue))
    /\ bypassed' = (bypassed \/ (a \in Small /\ phase[Large] = "waiting"
                               /\ (~BackgroundLarge \/ aged)))
    /\ repeatedSmall' = (repeatedSmall \/ (a \in finishedSmall /\ largeVisible))
    /\ UNCHANGED <<largeVisible, finishedSmall, aged>>

Work(a) ==
    /\ phase[a] = "work" /\ remaining[a] > 0
    /\ remaining' = [remaining EXCEPT ![a] = @ - 1]
    /\ UNCHANGED <<phase, queue, largeVisible, bypassed, finishedSmall, repeatedSmall, aged>>

(* Successful serialized validation/WAL/sync abstracted as one step; the
   admitted owner remains charged until result delivery and retirement. *)
Commit(a) ==
    /\ phase[a] = "work" /\ remaining[a] = 0
    /\ phase' = [phase EXCEPT ![a] = "durable"]
    /\ largeVisible' = (largeVisible \/ a = Large)
    /\ UNCHANGED <<remaining, queue, bypassed, finishedSmall, repeatedSmall, aged>>

Retire(a) ==
    /\ phase[a] = "durable"
    /\ phase' = [phase EXCEPT ![a] = IF a = Large THEN "done" ELSE "idle"]
    /\ finishedSmall' = finishedSmall \union ({a} \intersect Small)
    /\ UNCHANGED <<remaining, queue, largeVisible, bypassed, repeatedSmall, aged>>

Next == Age \/ \E a \in Actors: Enqueue(a) \/ Grant(a) \/ Work(a) \/ Commit(a) \/ Retire(a)
TypeInvariant ==
    /\ phase \in [Actors -> {"idle", "waiting", "work", "durable", "done"}]
    /\ remaining \in [Actors -> 0..LargeSteps]
    /\ queue \in Seq(Actors) /\ Len(queue) <= Cardinality(Actors)
    /\ {queue[i]: i \in 1..Len(queue)} = {a \in Actors: phase[a] = "waiting"}
    /\ Cardinality({queue[i]: i \in 1..Len(queue)}) = Len(queue)
    /\ {largeVisible, bypassed, repeatedSmall, aged} \subseteq BOOLEAN
    /\ finishedSmall \subseteq Small
CapacityBounded == Used <= Capacity
NoBypass == ~bypassed
LargeOwnsCapacity == Large \in Holders => Holders = {Large}
WholeLargePublication == largeVisible <=> phase[Large] \in {"durable", "done"}
LargeCompletes == <> (phase[Large] = "done")

Spec == Init /\ [][Next]_vars /\ WF_vars(Age)
        /\ (\A a \in Actors: WF_vars(Grant(a)))
        /\ (IF FairService THEN
              \A a \in Actors: WF_vars(Work(a)) /\ WF_vars(Commit(a)) /\ WF_vars(Retire(a))
            ELSE TRUE)

NoYoungWaiterWitness == ~(phase[Large] = "waiting" /\ Len(queue) > 1)
NoRepeatedSmallWitness == ~repeatedSmall
NoUnagedForegroundWitness == ~(BackgroundLarge /\ ~aged /\ phase[Large] = "waiting"
                                /\ Holders \intersect finishedSmall # {})
NoAgedWaitingWitness == ~(BackgroundLarge /\ aged /\ phase[Large] = "waiting"
                          /\ Len(queue) > 1)
=============================================================================
