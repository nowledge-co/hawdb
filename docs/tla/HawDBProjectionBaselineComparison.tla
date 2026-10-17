---------------- MODULE HawDBProjectionBaselineComparison ----------------
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS SkipSource, DeltaAsExpected, DedupOwners, CountOnly, IgnoreVectors, IgnoreCount, ReadLive, IgnoreMapperError, IgnoreEpoch, ResetLookups, PublishMatch, ReleasePinEarly, ReleasePermitEarly, IgnoreCancel
VARIABLES fixture, db, head, phase, sourceEpoch, baseEpoch, owners, rows, seen, endAck, pos, compared, lookups, actualLookups, cap, error, cancelled, pin, permit, visible, progress, concurrentProgress, heldProgress
vars == <<fixture,db,head,phase,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
OwnerId(n) == IF fixture="duplicate" THEN 1 ELSE n
Doc(n,v) == <<OwnerId(n),v,0>>
Canonical == {Doc(n,0):n \in owners}
Actual == CASE fixture="extra" -> Canonical \cup {<<3,0,0>>}
 [] fixture="missing" -> {Doc(1,0)}
 [] fixture="body" -> {Doc(1,1),Doc(2,0)}
 [] fixture="vector" -> {<<1,0,1>>,Doc(2,0)}
 [] fixture="swap" -> {Doc(1,0),<<3,0,0>>}
 [] OTHER -> Canonical
Working == phase \in {"mapping","ordered","comparing","complete"}
ResourcesOwned == Working \/ phase="failed"
Ready == Working /\ (~cancelled \/ IgnoreCancel)
Unique == Cardinality({OwnerId(n):n \in owners})=Cardinality(owners)
Expected == IF DeltaAsExpected THEN rows \cup Actual ELSE rows
Init == /\ fixture \in {"match","extra","missing","body","vector","swap","duplicate","empty","lagged"}
 /\ db=(IF fixture="lagged" THEN 1 ELSE 0) /\ head=0 /\ phase="idle"
 /\ sourceEpoch=0 /\ baseEpoch=0 /\ owners=(IF fixture="empty" THEN {} ELSE {1,2})
 /\ rows={} /\ seen={} /\ endAck=FALSE /\ pos=1 /\ compared={}
 /\ lookups=0 /\ actualLookups=0 /\ cap \in {1,4} /\ error=FALSE
 /\ cancelled=FALSE /\ pin=FALSE /\ permit=FALSE /\ visible=0
 /\ progress=FALSE /\ concurrentProgress=FALSE /\ heldProgress=FALSE
Start == /\ phase="idle" /\ (db=baseEpoch \/ IgnoreEpoch)
 /\ phase'="mapping" /\ sourceEpoch'=db /\ pin'=TRUE /\ permit'=TRUE
 /\ UNCHANGED <<fixture,db,head,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,visible,progress,concurrentProgress,heldProgress>>
Write == /\ Working /\ db=0 /\ db'=1
 /\ UNCHANGED <<fixture,head,phase,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
ChangeHead == /\ Working /\ head=0 /\ head'=1
 /\ UNCHANGED <<fixture,db,phase,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
Map(n,ok) == /\ Ready /\ phase="mapping" /\ n \in owners \ seen /\ ok \in BOOLEAN
 /\ LET stop == ~ok /\ ~IgnoreMapperError
    IN /\ phase'=(IF stop THEN "failed" ELSE phase)
       /\ seen'=(IF stop THEN seen ELSE seen \cup {n})
       /\ rows'=(IF stop \/ (SkipSource /\ n=2) THEN rows ELSE rows \cup {Doc(n,IF ReadLive THEN db ELSE 0)})
 /\ error'=(error \/ ~ok)
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,endAck,pos,compared,lookups,actualLookups,cap,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
End == /\ Ready /\ phase="mapping" /\ seen=owners
 /\ phase'="ordered" /\ endAck'=TRUE
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,rows,seen,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
Order == /\ Ready /\ phase="ordered"
 /\ phase'=(IF Unique \/ DedupOwners THEN "comparing" ELSE "failed")
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
Compare(row) == /\ Ready /\ phase="comparing" /\ row \in Expected \ compared
 /\ (lookups<cap \/ ResetLookups)
 /\ LET equal == row \in Actual \/ CountOnly \/
         (IgnoreVectors /\ \E a \in Actual:a[1]=row[1] /\ a[2]=row[2])
    IN /\ phase'=(IF equal THEN phase ELSE "failed")
       /\ compared'=(IF equal THEN compared \cup {row} ELSE compared)
 /\ pos'=pos+1 /\ lookups'=(IF ResetLookups THEN 1 ELSE lookups+1)
 /\ actualLookups'=actualLookups+1
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,rows,seen,endAck,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
Complete == /\ Ready /\ phase="comparing" /\ compared=Expected
 /\ phase'=(IF IgnoreCount \/ Cardinality(Expected)=Cardinality(Actual) THEN "complete" ELSE "failed")
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
Finish == /\ Ready /\ phase="complete"
 /\ phase'="matched" /\ pin'=FALSE /\ permit'=FALSE /\ progress'=TRUE
 /\ concurrentProgress'=(db>sourceEpoch) /\ heldProgress'=(head=1)
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,visible>>
Cancel == /\ Working /\ ~cancelled /\ cancelled'=TRUE
 /\ UNCHANGED <<fixture,db,head,phase,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,pin,permit,visible,progress,concurrentProgress,heldProgress>>
Refuse == /\ Working /\ ((cancelled /\ ~IgnoreCancel) \/ (phase="comparing" /\ lookups>=cap /\ compared#Expected /\ ~ResetLookups))
 /\ phase'="failed"
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,visible,progress,concurrentProgress,heldProgress>>
LosePin == /\ ResourcesOwned /\ pin /\ ReleasePinEarly /\ pin'=FALSE
 /\ UNCHANGED <<fixture,db,head,phase,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,permit,visible,progress,concurrentProgress,heldProgress>>
LosePermit == /\ ResourcesOwned /\ permit /\ ReleasePermitEarly /\ permit'=FALSE
 /\ UNCHANGED <<fixture,db,head,phase,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,visible,progress,concurrentProgress,heldProgress>>
Publish == /\ phase="matched" /\ PublishMatch /\ visible=0 /\ visible'=1
 /\ UNCHANGED <<fixture,db,head,phase,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,pin,permit,progress,concurrentProgress,heldProgress>>
Retire == /\ phase#"retired" /\ phase'="retired" /\ pin'=FALSE /\ permit'=FALSE
 /\ UNCHANGED <<fixture,db,head,sourceEpoch,baseEpoch,owners,rows,seen,endAck,pos,compared,lookups,actualLookups,cap,error,cancelled,visible,progress,concurrentProgress,heldProgress>>
Next == Start \/ Write \/ ChangeHead \/ (\E n \in owners,ok \in BOOLEAN:Map(n,ok)) \/ End \/ Order \/ (\E r \in Expected:Compare(r)) \/ Complete \/ Finish \/ Cancel \/ Refuse \/ LosePin \/ LosePermit \/ Publish \/ Retire
TypeOK == actualLookups \in 0..3 /\ pos \in 1..4 /\ lookups \in 0..3
CompleteInput == phase="matched" => seen=owners /\ endAck /\ rows=Canonical
ExactHistoricalMatch == phase="matched" => Actual=Canonical
UniqueOwners == phase="matched" => Unique
PinnedMapping == \A row \in rows:row[2]=0
NoFailedMatch == phase="matched" => ~error
CorrectEpoch == phase="matched" => sourceEpoch=baseEpoch
CumulativeLookupAdmission == actualLookups<=cap
NoActivation == visible=0
PinnedWhileWorking == ResourcesOwned => pin
AdmittedWhileWorking == ResourcesOwned => permit
NoCancelledMatch == phase="matched" => ~cancelled
NoProgressWitness == ~progress
NoConcurrentWitness == ~concurrentProgress
NoHeldHeadWitness == ~heldProgress
=============================================================================
