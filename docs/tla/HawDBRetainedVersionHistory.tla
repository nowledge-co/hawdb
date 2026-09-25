---------------- MODULE HawDBRetainedVersionHistory ----------------
EXTENDS Integers, FiniteSets
CONSTANTS Roots, Limit, SkipAdmission, RefundEveryDrop, SkipRefund, UnderReservePrune
Owners == {"canonical", "writer", "reader"}
Pending == {"p1", "p2"}
Holders == Owners \union Pending
None == "none"
VARIABLES holder, weight, target, working, phase, used, observed, refused, cancelled, pruned
vars == <<holder,weight,target,working,phase,used,observed,refused,cancelled,pruned>>
Live(h) == {r \in Roots: \E x \in Holders: h[x] = r}
Refs(r) == Cardinality({x \in Holders: holder[x] = r})
RECURSIVE Sum(_, _)
Sum(s,w) == IF s = {} THEN 0 ELSE LET r == CHOOSE x \in s: TRUE IN w[r] + Sum(s \ {r},w)
Clean(h,w) == [r \in Roots |-> IF r \in Live(h) THEN w[r] ELSE 0]
Release(r,h) == IF ~SkipRefund /\ (r \notin Live(h) \/ RefundEveryDrop) THEN weight[r] ELSE 0
Init ==
    /\ holder = [h \in Holders |-> IF h = "canonical" THEN CHOOSE r \in Roots: TRUE ELSE None]
    /\ weight = [r \in Roots |-> IF r = holder["canonical"] THEN 1 ELSE 0]
    /\ target = weight /\ working = [r \in Roots |-> 0]
    /\ phase = [p \in Pending |-> "none"]
    /\ used = 1 /\ observed = 0 /\ refused = FALSE /\ cancelled = FALSE /\ pruned = FALSE
Capture(h) ==
    /\ h \in {"reader","writer"} /\ holder[h] = None /\ holder["canonical"] # None
    /\ holder' = [holder EXCEPT ![h] = holder["canonical"]]
    /\ observed' = IF h = "reader" THEN weight[holder["canonical"]] ELSE observed
    /\ UNCHANGED <<weight,target,working,phase,used,refused,cancelled,pruned>>
Drop(h) ==
    /\ holder[h] # None
    /\ LET next == [holder EXCEPT ![h] = None]
       IN /\ used' = used - Release(holder[h],next)
          /\ holder' = next /\ weight' = Clean(next,weight)
          /\ target' = Clean(next,target) /\ working' = Clean(next,working)
    /\ phase' = IF h \in Pending THEN [phase EXCEPT ![h] = "none"] ELSE phase
    /\ observed' = IF h = "reader" THEN 0 ELSE observed
    /\ cancelled' = (cancelled \/ h \in Pending)
    /\ UNCHANGED <<refused,pruned>>
Need(source,n,prune) == IF prune THEN weight[holder[source]] ELSE n
Charge(source,n,prune) == IF prune /\ UnderReservePrune THEN n ELSE Need(source,n,prune)
Eligible(source,n,prune) ==
    /\ source \in {"canonical","writer"} /\ holder[source] # None
    /\ n \in 1..2
    /\ IF prune THEN (n < weight[holder[source]] /\ Refs(holder[source]) > 1)
       ELSE n >= weight[holder[source]]
Stage(p,source,r,n,prune) ==
    /\ holder[p] = None /\ r \notin Live(holder) /\ Eligible(source,n,prune)
    /\ SkipAdmission \/ used + Charge(source,n,prune) <= Limit
    /\ holder' = [holder EXCEPT ![p] = r]
    /\ weight' = [weight EXCEPT ![r] = Charge(source,n,prune)]
    /\ target' = [target EXCEPT ![r] = n]
    /\ working' = [working EXCEPT ![r] = Need(source,n,prune)]
    /\ phase' = [phase EXCEPT ![p] = "copying"]
    /\ used' = used + Charge(source,n,prune)
    /\ UNCHANGED <<observed,refused,cancelled,pruned>>
Refuse(p,source,n,prune) ==
    /\ holder[p] = None /\ Eligible(source,n,prune)
    /\ used + Charge(source,n,prune) > Limit
    /\ refused' = TRUE
    /\ UNCHANGED <<holder,weight,target,working,phase,used,observed,cancelled,pruned>>
Finish(p) ==
    /\ phase[p] = "copying"
    /\ LET r == holder[p]
       IN /\ used' = used - (weight[r] - target[r])
          /\ weight' = [weight EXCEPT ![r] = target[r]]
          /\ working' = [working EXCEPT ![r] = 0]
    /\ phase' = [phase EXCEPT ![p] = "ready"]
    /\ pruned' = (pruned \/ working[holder[p]] > target[holder[p]])
    /\ UNCHANGED <<holder,target,observed,refused,cancelled>>
Publish(p,destination) ==
    /\ phase[p] = "ready" /\ destination \in {"canonical","writer"}
    /\ LET next == [holder EXCEPT ![destination] = holder[p], ![p] = None]
       IN /\ used' = IF holder[destination] = None THEN used ELSE used - Release(holder[destination],next)
          /\ holder' = next /\ weight' = Clean(next,weight)
          /\ target' = Clean(next,target) /\ working' = Clean(next,working)
    /\ phase' = [phase EXCEPT ![p] = "none"]
    /\ UNCHANGED <<observed,refused,cancelled,pruned>>
PruneUnique(h,n) ==
    /\ h \in {"canonical","writer"} /\ holder[h] # None /\ Refs(holder[h]) = 1
    /\ n \in 1..2 /\ n < weight[holder[h]]
    /\ used' = used - (weight[holder[h]] - n)
    /\ weight' = [weight EXCEPT ![holder[h]] = n]
    /\ target' = [target EXCEPT ![holder[h]] = n]
    /\ UNCHANGED <<holder,working,phase,observed,refused,cancelled,pruned>>
Next == (\E h \in {"reader","writer"}: Capture(h))
     \/ (\E h \in Holders: Drop(h))
     \/ (\E p \in Pending, s \in {"canonical","writer"}, r \in Roots, n \in 1..2, prune \in BOOLEAN: Stage(p,s,r,n,prune))
     \/ (\E p \in Pending, s \in {"canonical","writer"}, n \in 1..2, prune \in BOOLEAN: Refuse(p,s,n,prune))
     \/ (\E p \in Pending: Finish(p))
     \/ (\E p \in Pending, h \in {"canonical","writer"}: Publish(p,h))
     \/ (\E h \in {"canonical","writer"}, n \in 1..2: PruneUnique(h,n))
TypeInvariant ==
    /\ holder \in [Holders -> Roots \union {None}]
    /\ weight \in [Roots -> 0..2] /\ target \in [Roots -> 0..2] /\ working \in [Roots -> 0..2]
    /\ phase \in [Pending -> {"none","copying","ready"}]
    /\ used \in Int /\ observed \in 0..2 /\ {refused,cancelled,pruned} \subseteq BOOLEAN
    /\ \A p \in Pending: (phase[p] = "none") = (holder[p] = None)
    /\ \A r \in Roots: (weight[r] > 0) = (r \in Live(holder))
ExactAccounting == used = Sum(Live(holder),weight)
BoundedRetainedRoots == 0 <= used /\ used <= Limit
CopyingCovered == \A r \in Roots: working[r] <= weight[r]
ReaderRootUnchanged == holder["reader"] # None => weight[holder["reader"]] = observed
NoRefusalWitness == ~refused
NoCancellationWitness == ~cancelled
NoSharedPruneWitness == ~pruned
=============================================================================
