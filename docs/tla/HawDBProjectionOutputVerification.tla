---------------- MODULE HawDBProjectionOutputVerification ----------------
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS SkipMutation, ReadLive, DropPinEarly, ChangeChecker, ResetAdmission, IgnoreError, IgnoreCancel, PrematureFinish, PublishInspected, ReleasePermitEarly, BeforeOrdering
VARIABLES db, pinned, phase, ordered, cursor, checked, endAck, used, actual, cap, hadError, cancelled, checker, run, sealedRun, permit, visible, witness, observed
vars == <<db,pinned,phase,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,permit,visible,witness,observed>>
Expected == <<1,2>>
Min(a,b) == IF a<b THEN a ELSE b
Ready == phase="checking" /\ (~cancelled \/ IgnoreCancel)
Init == /\ db=0 /\ pinned=TRUE /\ phase="ordered" /\ ordered \in BOOLEAN
 /\ cursor=1 /\ checked= << >> /\ endAck=FALSE /\ used=2 /\ actual=2
 /\ cap \in {3,7} /\ hadError=FALSE /\ cancelled=FALSE /\ checker="fixed"
 /\ run=Expected /\ sealedRun= << >> /\ permit=TRUE /\ visible=0 /\ witness=FALSE /\ observed= << >>
Mutate == /\ db=0 /\ db'=1
 /\ UNCHANGED <<pinned,phase,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,permit,visible,witness,observed>>
Start == /\ phase="ordered" /\ (ordered \/ BeforeOrdering)
 /\ phase'="checking" /\ used'=(IF ResetAdmission THEN 0 ELSE used)
 /\ UNCHANGED <<db,pinned,ordered,cursor,checked,endAck,actual,cap,hadError,cancelled,checker,run,sealedRun,permit,visible,witness,observed>>
Check(ok) == /\ Ready /\ cursor<=3 /\ used<cap /\ ok \in BOOLEAN
 /\ LET bad == ~ok \/ (ReadLive /\ db#0)
        stop == bad /\ ~IgnoreError
    IN /\ phase'=(IF stop THEN "failed" ELSE phase)
       /\ checked'=(IF ~stop /\ cursor<=2 THEN Append(checked,run[cursor]) ELSE checked)
       /\ cursor'=(IF stop THEN cursor ELSE cursor+1)
       /\ endAck'=(endAck \/ (~stop /\ cursor=3))
       /\ hadError'=(hadError \/ bad)
 /\ used'=used+1 /\ actual'=actual+1 /\ observed'=Append(observed, IF ReadLive THEN db ELSE 0)
 /\ UNCHANGED <<db,pinned,ordered,cap,cancelled,checker,run,sealedRun,permit,visible,witness>>
Skip == /\ Ready /\ SkipMutation /\ cursor<=2 /\ cursor'=cursor+1
 /\ UNCHANGED <<db,pinned,phase,ordered,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,permit,visible,witness,observed>>
Cancel == /\ phase \in {"ordered","checking"} /\ ~cancelled /\ cancelled'=TRUE
 /\ UNCHANGED <<db,pinned,phase,ordered,cursor,checked,endAck,used,actual,cap,hadError,checker,run,sealedRun,permit,visible,witness,observed>>
Refuse == /\ phase="checking" /\ (used>=cap \/ (cancelled /\ ~IgnoreCancel)) /\ phase'="failed"
 /\ UNCHANGED <<db,pinned,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,permit,visible,witness,observed>>
Finish == /\ Ready /\ (cursor=4 \/ (PrematureFinish /\ cursor=3))
 /\ phase'="sealed" /\ pinned'=FALSE /\ sealedRun'=run /\ witness'=TRUE
 /\ UNCHANGED <<db,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,permit,visible,observed>>
LosePin == /\ Ready /\ DropPinEarly /\ pinned /\ pinned'=FALSE
 /\ UNCHANGED <<db,phase,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,permit,visible,witness,observed>>
ReplaceChecker == /\ Ready /\ ChangeChecker /\ checker="fixed" /\ checker'="other"
 /\ UNCHANGED <<db,pinned,phase,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,run,sealedRun,permit,visible,witness,observed>>
Publish == /\ phase="sealed" /\ PublishInspected /\ visible=0 /\ visible'=1
 /\ UNCHANGED <<db,pinned,phase,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,permit,witness,observed>>
LosePermit == /\ phase#"retired" /\ ReleasePermitEarly /\ permit /\ permit'=FALSE
 /\ UNCHANGED <<db,pinned,phase,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,visible,witness,observed>>
Retire == /\ phase#"retired" /\ phase'="retired" /\ pinned'=FALSE /\ permit'=FALSE
 /\ UNCHANGED <<db,ordered,cursor,checked,endAck,used,actual,cap,hadError,cancelled,checker,run,sealedRun,visible,witness,observed>>
Next == Mutate \/ Start \/ (\E ok \in BOOLEAN:Check(ok)) \/ Skip \/ Cancel \/ Refuse \/ Finish \/ LosePin \/ ReplaceChecker \/ Publish \/ LosePermit \/ Retire
TypeOK == cursor \in 1..4 /\ used \in 0..7 /\ actual \in 2..9 /\ Len(checked)<=2
NoSkippedOutput == phase \in {"checking","sealed"} => checked=SubSeq(Expected,1,Min(cursor-1,2))
FixedSource == \A i \in 1..Len(observed): observed[i]=0
PinnedUntilChecksFinish == phase="checking" => pinned
FixedVerifier == checker="fixed"
CumulativeAdmission == actual<=cap
NoFailedSeal == phase="sealed" => ~hadError
NoCancelledSeal == phase="sealed" => ~cancelled
NoSealWithoutEnd == phase="sealed" => endAck /\ checked=Expected /\ sealedRun=Expected
NoActivation == visible=0
NoUnleasedStage == phase#"retired" => permit
ClosedOrdering == phase \in {"checking","sealed"} => ordered
NoSuccessWitness == ~witness
=============================================================================
