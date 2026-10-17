---------------- MODULE HawDBProjectionFeedCursor ----------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS ResetTotals, RefundDeferral, IgnoreMetadataWork, SkipItem,
          IgnoreError, DropEnd, ReadLive, LateAdmission
VARIABLES tape, live, pos, phase, total, step, caps, delivered, oracleWork,
          failedEver, failed, done, deferred, completeProgress, deferredProgress
vars == <<tape,live,pos,phase,total,step,caps,delivered,oracleWork,
          failedEver,failed,done,deferred,completeProgress,deferredProgress>>
Zero == [work |-> 0,items |-> 0,bytes |-> 0]
StepCaps == [work |-> 4,items |-> 1,bytes |-> 3]
Frame(kind,work,bytes,emit,value) == [kind |-> kind,work |-> work,bytes |-> bytes,emit |-> emit,value |-> value]
Input(width) == <<Frame("table",1,1,FALSE,0),Frame("key",2,width,TRUE,1),
                   Frame("commit",1,0,FALSE,0),Frame("node",1,1,TRUE,2),
                   Frame("end",1,0,TRUE,3)>>
Init == /\ \E width \in 1..4: tape = Input(width)
 /\ live = tape /\ pos = 1 /\ phase = "visit"
 /\ caps \in {[work |-> w,items |-> i,bytes |-> b]: w \in {6,12},i \in {2,3},b \in {3,5}}
 /\ total = [Zero EXCEPT !.work = 1] /\ oracleWork = 1
 /\ step = Zero /\ delivered = <<>>
 /\ failedEver = FALSE /\ failed = FALSE /\ done = FALSE /\ deferred = FALSE
 /\ completeProgress = FALSE /\ deferredProgress = FALSE
Current == tape[pos]
Begin == /\ phase = "visit" /\ ~failed /\ ~done
 /\ LET frame == Current
        yielding == step.work = StepCaps.work \/ step.items = StepCaps.items
                    \/ (frame.work <= StepCaps.work /\ frame.work > StepCaps.work-step.work)
        refused == frame.work > caps.work-total.work \/ frame.work > StepCaps.work
                    \/ (frame.emit /\ total.items = caps.items)
        cost == IF IgnoreMetadataWork /\ ~frame.emit THEN 0 ELSE frame.work
    IN /\ phase' = (IF yielding THEN "yield" ELSE IF refused THEN "failed" ELSE "measured")
       /\ failed' = (refused /\ ~yielding)
       /\ total' = (IF yielding \/ refused THEN total ELSE [total EXCEPT !.work = @+cost])
       /\ step' = (IF yielding \/ refused THEN step ELSE [step EXCEPT !.work = @+cost])
       /\ oracleWork' = oracleWork + (IF yielding \/ refused THEN 0 ELSE frame.work)
 /\ UNCHANGED <<tape,live,pos,caps,delivered,failedEver,done,deferred,completeProgress,deferredProgress>>
Admit == /\ phase = "measured"
 /\ LET frame == Current
        refused == frame.bytes > caps.bytes-total.bytes \/ frame.bytes > StepCaps.bytes
        yielding == ~refused /\ frame.bytes > StepCaps.bytes-step.bytes
        accepted == LateAdmission \/ (~refused /\ ~yielding)
    IN /\ phase' = (IF accepted THEN "callback" ELSE IF refused THEN "failed" ELSE "yield")
       /\ failed' = (refused /\ ~accepted)
       /\ total' = (IF accepted THEN [total EXCEPT !.bytes = @+frame.bytes,!.items = @+(IF frame.emit THEN 1 ELSE 0)]
                     ELSE IF RefundDeferral /\ yielding THEN [total EXCEPT !.work = @-frame.work] ELSE total)
       /\ step' = (IF accepted THEN [step EXCEPT !.bytes = @+frame.bytes,!.items = @+(IF frame.emit THEN 1 ELSE 0)] ELSE step)
       /\ deferred' = (deferred \/ yielding)
 /\ UNCHANGED <<tape,live,pos,caps,delivered,oracleWork,failedEver,done,completeProgress,deferredProgress>>
Callback(error) == /\ phase = "callback" /\ error \in BOOLEAN
 /\ (~error \/ Current.emit)
 /\ LET frame == Current
        success == ~error \/ IgnoreError
        finished == success /\ frame.kind = "end"
        value == IF ReadLive THEN live[pos].value ELSE frame.value
    IN /\ pos' = (IF success THEN pos+1 ELSE pos)
       /\ phase' = (IF finished THEN "done" ELSE IF success THEN "visit" ELSE "failed")
       /\ delivered' = (IF success /\ frame.emit /\ ~SkipItem THEN Append(delivered,value) ELSE delivered)
       /\ failedEver' = (failedEver \/ error)
       /\ failed' = ~success
       /\ done' = finished
       /\ completeProgress' = (completeProgress \/ finished)
       /\ deferredProgress' = (deferredProgress \/ (finished /\ deferred))
 /\ UNCHANGED <<tape,live,total,step,caps,oracleWork,deferred>>
Reset == /\ phase = "yield" /\ phase' = "visit" /\ step' = Zero
 /\ total' = (IF ResetTotals THEN Zero ELSE total)
 /\ UNCHANGED <<tape,live,pos,caps,delivered,oracleWork,failedEver,failed,done,deferred,completeProgress,deferredProgress>>
MutateLive == /\ ~done /\ ~failed /\ live = tape
 /\ live' = [i \in 1..Len(tape) |-> [tape[i] EXCEPT !.value = @+10]]
 /\ UNCHANGED <<tape,pos,phase,total,step,caps,delivered,oracleWork,failedEver,failed,done,deferred,completeProgress,deferredProgress>>
EarlyEnd == /\ DropEnd /\ phase = "visit" /\ Current.kind = "end"
 /\ done' = TRUE /\ phase' = "done"
 /\ UNCHANGED <<tape,live,pos,total,step,caps,delivered,oracleWork,failedEver,failed,deferred,completeProgress,deferredProgress>>
Next == Begin \/ Admit \/ (\E error \in BOOLEAN: Callback(error)) \/ Reset \/ MutateLive \/ EarlyEnd
Expected == SelectSeq(SubSeq(tape,1,pos-1),LAMBDA frame: frame.emit)
NoSkippedInput == delivered = [i \in 1..Len(Expected) |-> Expected[i].value]
NoUnadmittedEnd == done => pos = Len(tape)+1 /\ NoSkippedInput /\ ~failed
NoIgnoredError == failedEver => failed
NoBudgetOverflow == /\ \A field \in {"work","items","bytes"}: total[field] <= caps[field] /\ step[field] <= StepCaps[field]
NoWorkRefund == total.work >= oracleWork
TypeOK == /\ pos \in 1..Len(tape)+1 /\ phase \in {"visit","measured","callback","yield","failed","done"}
 /\ total.work \in Nat /\ step.work \in Nat /\ failed \in BOOLEAN /\ done \in BOOLEAN
NoCompleteProgress == ~completeProgress
NoDeferredProgress == ~deferredProgress
=============================================================================
