---------------- MODULE HawDBProjectionFullSource ----------------
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS EarlyEnd, SkipTable, DropKey, RepeatKey, ReadLive, ResetTotals,
 ChargeLate, ForgetPending, IgnoreError, IgnoreCancel, SkipMetadataAdmission, SkipEndAck
VARIABLES db, phase, pinned, pos, pending, seen, used, actual, step,
 stepNumber, cancelled, hadError, metadata, metadataCap, cap, completed, badDuplicate, progress
vars == <<db,phase,pinned,pos,pending,seen,used,actual,step,stepNumber,
 cancelled,hadError,metadata,metadataCap,cap,completed,badDuplicate,progress>>
Node(n) == <<"node",n>>
Table(t) == <<"table",t>>
Key(k) == <<"key",1,k>>
End == <<"end">>
Inputs(version) == <<Node(0),Node(1),Table(1),Key(1),Key(IF version=0 THEN 2 ELSE 3),Table(2),End>>
\* Node 1 is an allocation hole. Table 2 is empty but must be announced.
Item == IF ReadLive THEN Inputs(db)[pos] ELSE Inputs(pinned)[pos]
Expected == Inputs(pinned)
IsKey(item) == item[1]="key"
Weight(item) == IF IsKey(item) THEN 2 ELSE 1
Zero == [work |-> 0,items |-> 0,bytes |-> 0]
Plus(a,b) == [work |-> a.work+b.work,items |-> a.items+b.items,bytes |-> a.bytes+b.bytes]
Within(a,b) == a.work<=b.work /\ a.items<=b.items /\ a.bytes<=b.bytes
Open == phase \in {"ready","active"}
Init == /\ db=0 /\ phase="idle" /\ pinned=0 /\ pos=1 /\ pending= << >> /\ seen= << >>
 /\ used=Zero /\ actual=Zero /\ step=[limit |-> [work |-> 3,items |-> 1,bytes |-> 2],used |-> Zero]
 /\ stepNumber=0 /\ cancelled=FALSE /\ hadError=FALSE /\ metadata=0 /\ metadataCap=0
 /\ cap \in {[work |-> 4,items |-> 12,bytes |-> 24],[work |-> 24,items |-> 12,bytes |-> 24]}
 /\ completed=FALSE /\ badDuplicate=FALSE /\ progress=FALSE
Capture(metaLimit) == /\ phase="idle" /\ metaLimit \in {1,3}
 /\ phase'=(IF metaLimit<3 /\ ~SkipMetadataAdmission THEN "failed" ELSE "ready")
 /\ pinned'=db /\ metadata'=(IF metaLimit<3 /\ ~SkipMetadataAdmission THEN 0 ELSE 3)
 /\ metadataCap'=metaLimit
 /\ UNCHANGED step
 /\ UNCHANGED <<db,pos,pending,seen,used,actual,stepNumber,cancelled,hadError,cap,completed,badDuplicate,progress>>
Mutate == /\ db=0 /\ phase # "done" /\ db'=1
 /\ UNCHANGED <<phase,pinned,pos,pending,seen,used,actual,step,stepNumber,cancelled,hadError,metadata,metadataCap,cap,completed,badDuplicate,progress>>
Start(limit) == /\ phase="ready" /\ stepNumber<8
 /\ limit \in {[work |-> w,items |-> i,bytes |-> b]:w \in {3,6},i \in {1,2},b \in {2,6}}
 /\ phase'="active" /\ stepNumber'=stepNumber+1 /\ step'=[limit |-> limit,used |-> Zero]
 /\ used'=(IF ResetTotals THEN Zero ELSE used)
 /\ UNCHANGED <<db,pinned,pos,pending,seen,actual,cancelled,hadError,metadata,metadataCap,cap,completed,badDuplicate,progress>>
Cost == IF pending= << >> /\ IsKey(Item) THEN [work |-> 3,items |-> 0,bytes |-> 0]
 ELSE [work |-> 1,items |-> 1,bytes |-> Weight(IF pending= << >> THEN Item ELSE pending)]
FitsTotal == Within(Plus(used,Cost),cap)
FitsStep == Within(Plus(step.used,Cost),step.limit)
Seek == /\ phase="active" /\ pending= << >> /\ IsKey(Item)
 /\ (~cancelled \/ IgnoreCancel) /\ FitsTotal /\ FitsStep
 /\ pending'=Item /\ used'=Plus(used,Cost) /\ actual'=Plus(actual,Cost)
 /\ step'=[step EXCEPT !.used=Plus(@,Cost)]
 /\ UNCHANGED <<db,phase,pinned,pos,seen,stepNumber,cancelled,hadError,metadata,metadataCap,cap,completed,badDuplicate,progress>>
Emit(error,cancelAtEnd) == /\ phase="active" /\ (pending# << >> \/ ~IsKey(Item))
 /\ (~cancelled \/ IgnoreCancel) /\ FitsTotal /\ FitsStep /\ error \in BOOLEAN /\ cancelAtEnd \in BOOLEAN
 /\ LET item == IF pending= << >> THEN Item ELSE pending
        finish == item=End
        stop == (error /\ ~IgnoreError) \/ (finish /\ cancelAtEnd /\ ~IgnoreCancel)
        omit == error \/ (SkipTable /\ item[1]="table") \/ (DropKey /\ IsKey(item))
        nextSeen == IF omit \/ stop THEN seen ELSE Append(seen,item)
    IN /\ phase'=(IF stop THEN "failed" ELSE IF finish THEN "done" ELSE phase)
       /\ seen'=nextSeen /\ pos'=(IF stop \/ finish \/ (RepeatKey /\ IsKey(item)) THEN pos ELSE pos+1)
       /\ pending'= << >> /\ completed'=finish /\ ~stop
       /\ badDuplicate'=(badDuplicate \/ (~omit /\ ~stop /\ item \in {seen[j]:j \in 1..Len(seen)}))
       /\ progress'=(progress \/ (finish /\ ~stop /\ nextSeen=Expected /\ stepNumber>1))
       /\ cancelled'=(cancelled \/ (finish /\ cancelAtEnd))
 /\ hadError'=(hadError \/ error)
 /\ used'=Plus(used,Cost) /\ actual'=Plus(actual,Cost) /\ step'=[step EXCEPT !.used=Plus(@,Cost)]
 /\ UNCHANGED <<db,pinned,stepNumber,metadata,metadataCap,cap>>
Yield == /\ phase="active" /\ FitsTotal /\ ~FitsStep /\ Within(Cost,step.limit)
 /\ phase'="ready"
 /\ pos'=(IF ForgetPending /\ pending# << >> THEN pos+1 ELSE pos)
 /\ pending'=(IF ForgetPending THEN <<>> ELSE pending)
 /\ UNCHANGED <<db,pinned,seen,used,actual,step,stepNumber,cancelled,hadError,metadata,metadataCap,cap,completed,badDuplicate,progress>>
Refuse == /\ phase="active" /\ (~FitsTotal \/ ~Within(Cost,step.limit) \/ (cancelled /\ ~IgnoreCancel))
 /\ phase'="failed" /\ actual'=(IF ChargeLate /\ ~FitsTotal THEN Plus(actual,Cost) ELSE actual)
 /\ UNCHANGED <<db,pinned,pos,pending,seen,used,step,stepNumber,cancelled,hadError,metadata,metadataCap,cap,completed,badDuplicate,progress>>
Cancel == /\ Open /\ ~cancelled /\ cancelled'=TRUE
 /\ UNCHANGED <<db,phase,pinned,pos,pending,seen,used,actual,step,stepNumber,hadError,metadata,metadataCap,cap,completed,badDuplicate,progress>>
Premature == /\ phase="active" /\ (EarlyEnd \/ (SkipEndAck /\ pos=Len(Expected)))
 /\ phase'="done" /\ completed'=TRUE
 /\ UNCHANGED <<db,pinned,pos,pending,seen,used,actual,step,stepNumber,cancelled,hadError,metadata,metadataCap,cap,badDuplicate,progress>>
Next == (\E m \in {1,3}:Capture(m)) \/ Mutate
 \/ (\E lim \in {[work |-> w,items |-> i,bytes |-> b]:w \in {3,6},i \in {1,2},b \in {2,6}}:Start(lim))
 \/ Seek \/ (\E e,c \in BOOLEAN:Emit(e,c)) \/ Yield \/ Refuse \/ Cancel \/ Premature
TypeOK == /\ phase \in {"idle","ready","active","failed","done"} /\ pos \in 1..Len(Expected)
 /\ stepNumber \in 0..8 /\ (pending= << >> \/ pending \in {Key(1),Key(2),Key(3)})
NoFalseCompletion == completed => seen=Expected
NoDuplicate == ~badDuplicate
NoOverrun == Within(actual,cap)
NoAcceptedFailure == completed => ~hadError
NoCancelledCompletion == completed => ~cancelled
MetadataAdmitted == phase \notin {"idle","failed"} => metadata<=metadataCap
NoProgressWitness == ~progress
=============================================================================
