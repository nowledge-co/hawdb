---------------- MODULE HawDBSearchSortQuantum ----------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS SkipOnYield, ForgetPeek, ResetTotal, RefundRead, IgnoreComparison,
          LateAdmission, EarlyDone, ReviveFailure
VARIABLES phase, resume, li, ri, a, b, chosen, output, width, caps, total,
          oracle, step, leftPeek, rightPeek, peeked, failedEver, deferred,
          flushed, leftRemoved, rightRemoved, completed
vars == <<phase,resume,li,ri,a,b,chosen,output,width,caps,total,oracle,step,
          leftPeek,rightPeek,peeked,failedEver,deferred,flushed,leftRemoved,rightRemoved,completed>>
Left == <<1,3>>
Right == <<2,4>>
Key(i) == IF i <= 2 THEN 1 ELSE 2
Zero == [work |-> 0,read |-> 0,spill |-> 0]
Cost(w,r,s) == [work |-> w,read |-> r,spill |-> s]
Add(x,y) == [f \in DOMAIN Zero |-> x[f]+y[f]]
Fits(x,y,limit) == \A f \in DOMAIN Zero: x[f]+y[f] <= limit[f]
Init == /\ phase = "openL" /\ resume = "openL" /\ li = 1 /\ ri = 1
 /\ a = 0 /\ b = 0 /\ chosen = 0 /\ output = <<>> /\ width \in {1,3}
 /\ caps \in {[work |-> w,read |-> r,spill |-> s]: w \in {1,3},r \in {2,5,9},s \in {2,5,9}}
 /\ total = Zero /\ oracle = Zero /\ step = Zero
 /\ leftPeek = FALSE /\ rightPeek = FALSE /\ peeked = {}
 /\ failedEver = FALSE /\ deferred = FALSE /\ flushed = FALSE
 /\ leftRemoved = FALSE /\ rightRemoved = FALSE /\ completed = FALSE
TotalCap == [work |-> 100,read |-> 100,spill |-> 100]
CurrentCost == CASE phase \in {"openL","openR"} -> Cost(1,1,0)
 [] phase = "openO" -> Cost(1,0,1)
 [] phase = "fillL" -> IF a # 0 \/ li > 2 THEN Cost(1,0,0)
                         ELSE IF leftPeek THEN Cost(1,width+1,0) ELSE Cost(1,1,0)
 [] phase = "fillR" -> IF b # 0 \/ ri > 2 THEN Cost(1,0,0)
                         ELSE IF rightPeek THEN Cost(1,width+1,0) ELSE Cost(1,1,0)
 [] phase = "choose" -> Cost(IF a # 0 /\ b # 0 THEN 2 ELSE 1,0,0)
 [] phase = "copy" -> Cost(1,width,width+1)
 [] OTHER -> Cost(1,0,0)
CanRun == LateAdmission \/ (Fits(step,CurrentCost,caps) /\ Fits(total,CurrentCost,TotalCap))
Step == /\ phase \notin {"yield","failed","done"} /\ CanRun
 /\ LET cost == CurrentCost
        charged == IF IgnoreComparison /\ phase = "choose" THEN [cost EXCEPT !.work = 0]
                   ELSE IF RefundRead THEN [cost EXCEPT !.read = 0] ELSE cost
    IN /\ total' = Add(total,charged) /\ oracle' = Add(oracle,cost) /\ step' = Add(step,charged)
 /\ phase' = (CASE phase = "openL" -> "openR" [] phase = "openR" -> "openO"
     [] phase = "openO" -> "fillL"
     [] phase = "fillL" -> IF a # 0 \/ li > 2 \/ leftPeek THEN "fillR" ELSE "fillL"
     [] phase = "fillR" -> IF b # 0 \/ ri > 2 \/ rightPeek THEN "choose" ELSE "fillR"
     [] phase = "choose" -> IF a = 0 /\ b = 0 THEN "flush" ELSE "copy"
     [] phase = "copy" -> IF EarlyDone THEN "done" ELSE "fillL"
     [] phase = "flush" -> "removeL" [] phase = "removeL" -> "removeR"
     [] phase = "removeR" -> "done")
 /\ li' = li + (IF phase = "fillL" /\ a = 0 /\ li <= 2 /\ leftPeek THEN 1 ELSE 0)
 /\ ri' = ri + (IF phase = "fillR" /\ b = 0 /\ ri <= 2 /\ rightPeek THEN 1 ELSE 0)
 /\ a' = (IF phase = "fillL" /\ a = 0 /\ li <= 2 /\ leftPeek THEN Left[li]
          ELSE IF phase = "copy" /\ chosen = a THEN 0 ELSE a)
 /\ b' = (IF phase = "fillR" /\ b = 0 /\ ri <= 2 /\ rightPeek THEN Right[ri]
          ELSE IF phase = "copy" /\ chosen = b THEN 0 ELSE b)
 /\ leftPeek' = (IF phase = "fillL" /\ a = 0 /\ li <= 2 THEN ~leftPeek ELSE leftPeek)
 /\ rightPeek' = (IF phase = "fillR" /\ b = 0 /\ ri <= 2 THEN ~rightPeek ELSE rightPeek)
 /\ peeked' = (IF phase = "fillL" /\ a = 0 /\ li <= 2 /\ ~leftPeek THEN peeked \cup {Left[li]}
     ELSE IF phase = "fillR" /\ b = 0 /\ ri <= 2 /\ ~rightPeek THEN peeked \cup {Right[ri]} ELSE peeked)
 /\ chosen' = (IF phase = "choose" THEN
       IF a = 0 THEN b ELSE IF b = 0 THEN a ELSE IF Key(a) <= Key(b) THEN a ELSE b ELSE chosen)
 /\ output' = (IF phase = "copy" THEN Append(output,chosen) ELSE output)
 /\ flushed' = (flushed \/ phase = "flush")
 /\ leftRemoved' = (leftRemoved \/ phase = "removeL")
 /\ rightRemoved' = (rightRemoved \/ phase = "removeR")
 /\ completed' = (completed \/ phase = "removeR")
 /\ UNCHANGED <<resume,width,caps,failedEver,deferred>>
YieldOrRefuse == /\ phase \notin {"yield","failed","done"} /\ ~CanRun
 /\ LET impossible == ~Fits(Zero,CurrentCost,caps) \/ ~Fits(total,CurrentCost,TotalCap)
    IN /\ phase' = (IF impossible THEN "failed" ELSE "yield")
       /\ failedEver' = (failedEver \/ impossible)
       /\ deferred' = (deferred \/ ~impossible)
 /\ resume' = phase
 /\ li' = (IF SkipOnYield /\ phase = "fillL" /\ a = 0 /\ li <= 2 THEN li+1 ELSE li)
 /\ UNCHANGED <<ri,a,b,chosen,output,width,caps,total,oracle,step,leftPeek,rightPeek,peeked,
                 flushed,leftRemoved,rightRemoved,completed>>
BeginStep == /\ phase = "yield" /\ phase' = resume /\ step' = Zero
 /\ total' = (IF ResetTotal THEN Zero ELSE total)
 /\ leftPeek' = (IF ForgetPeek THEN FALSE ELSE leftPeek)
 /\ rightPeek' = (IF ForgetPeek THEN FALSE ELSE rightPeek)
 /\ UNCHANGED <<resume,li,ri,a,b,chosen,output,width,caps,oracle,peeked,failedEver,deferred,
                 flushed,leftRemoved,rightRemoved,completed>>
Fail == /\ phase \notin {"failed","done"} /\ phase' = "failed" /\ failedEver' = TRUE
 /\ UNCHANGED <<resume,li,ri,a,b,chosen,output,width,caps,total,oracle,step,leftPeek,rightPeek,peeked,
                 deferred,flushed,leftRemoved,rightRemoved,completed>>
Revive == /\ ReviveFailure /\ phase = "failed" /\ phase' = "openL"
 /\ UNCHANGED <<resume,li,ri,a,b,chosen,output,width,caps,total,oracle,step,leftPeek,rightPeek,peeked,
                 failedEver,deferred,flushed,leftRemoved,rightRemoved,completed>>
Next == Step \/ YieldOrRefuse \/ BeginStep \/ Fail \/ Revive
Loaded == {Left[i]: i \in 1..li-1} \cup {Right[i]: i \in 1..ri-1}
Held == {output[i]: i \in 1..Len(output)} \cup ({a,b} \ {0})
NoInputLoss == Loaded = Held
NoDuplicateOwnership == Cardinality(Loaded) = Len(output) + (IF a # 0 THEN 1 ELSE 0) + (IF b # 0 THEN 1 ELSE 0)
NoEarlyResult == phase = "done" => /\ output = <<1,2,3,4>> /\ flushed /\ leftRemoved /\ rightRemoved
NoLostPeek == /\ ((phase = "fillL" /\ a = 0 /\ li <= 2 /\ Left[li] \in peeked) => leftPeek)
             /\ ((phase = "fillR" /\ b = 0 /\ ri <= 2 /\ Right[ri] \in peeked) => rightPeek)
NoUnchargedIO == total = oracle
NoBudgetOverflow == Fits(Zero,step,caps) /\ Fits(Zero,total,TotalCap)
NoIgnoredFailure == failedEver => phase = "failed"
TypeOK == /\ li \in 1..3 /\ ri \in 1..3 /\ a \in 0..4 /\ b \in 0..4
 /\ phase \in {"openL","openR","openO","fillL","fillR","choose","copy","flush","removeL","removeR","done","yield","failed"}
NoCompletionWitness == ~completed
NoDeferredCompletionWitness == ~(completed /\ deferred)
=============================================================================
