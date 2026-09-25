-------------------- MODULE HawDBLockWaitFairness --------------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Transactions, Keys, MaxWaiters, AllowBypass, IgnoreOwners, FairRelease
VARIABLES queue, held, mode, span, bypassed, compatiblePass
vars == <<queue, held, mode, span, bypassed, compatiblePass>>
Modes == {"S", "X", "O"}
Spans == (SUBSET Keys) \ {{}}
Queued == {queue[i]: i \in 1..Len(queue)}
Compatible(a, b) == (mode[a] = "S" /\ mode[b] = "S") \/ (mode[a] = "O" /\ mode[b] = "O")
Conflicts(a, b) == span[a] \intersect span[b] # {} /\ ~Compatible(a, b)
Older(t) == {queue[i]: i \in 1..((CHOOSE j \in 1..Len(queue): queue[j] = t) - 1)}
Init ==
    /\ queue = <<>> /\ held = {}
    /\ mode = [t \in Transactions |-> "S"]
    /\ span = [t \in Transactions |-> Keys]
    /\ bypassed = FALSE /\ compatiblePass = FALSE
Request(t, m, s) ==
    /\ t \notin Queued \union held /\ Len(queue) < MaxWaiters
    /\ queue' = Append(queue, t)
    /\ mode' = [mode EXCEPT ![t] = m]
    /\ span' = [span EXCEPT ![t] = s]
    /\ UNCHANGED <<held, bypassed, compatiblePass>>
Grant(t) ==
    /\ t \in Queued
    /\ IgnoreOwners \/ \A owner \in held: ~Conflicts(t, owner)
    /\ AllowBypass \/ \A older \in Older(t): ~Conflicts(t, older)
    /\ queue' = SelectSeq(queue, LAMBDA other: other # t)
    /\ held' = held \union {t}
    /\ bypassed' = (bypassed \/ \E older \in Older(t): Conflicts(t, older))
    /\ compatiblePass' = (compatiblePass \/ (Older(t) # {} /\ \A older \in Older(t): ~Conflicts(t, older)))
    /\ UNCHANGED <<mode, span>>
Release(t) ==
    /\ t \in held
    /\ held' = held \ {t}
    /\ UNCHANGED <<queue, mode, span, bypassed, compatiblePass>>
Next ==
    \/ \E t \in Transactions, m \in Modes, s \in Spans: Request(t, m, s)
    \/ \E t \in Transactions: Grant(t) \/ Release(t)
Spec ==
    /\ Init /\ [][Next]_vars
    /\ \A t \in Transactions: WF_vars(Grant(t))
    /\ IF FairRelease THEN \A t \in Transactions: WF_vars(Release(t)) ELSE TRUE
TypeInvariant ==
    /\ queue \in Seq(Transactions) /\ Len(queue) <= MaxWaiters
    /\ Cardinality(Queued) = Len(queue) /\ held \subseteq Transactions
    /\ Queued \intersect held = {}
    /\ mode \in [Transactions -> Modes] /\ span \in [Transactions -> Spans]
    /\ bypassed \in BOOLEAN /\ compatiblePass \in BOOLEAN
NoBypass == ~bypassed
NoConflictingHolders == \A a, b \in held: a # b => ~Conflicts(a, b)
EventualGrant == \A t \in Transactions: [](t \in Queued => <> (t \in held))
(* False reachability probe: compatible younger work must remain possible. *)
NoCompatiblePassWitness == ~compatiblePass
=============================================================================
