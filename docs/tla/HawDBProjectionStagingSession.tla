---------------- MODULE HawDBProjectionStagingSession ----------------
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS SkipPending, LoseContinuation, ResetUsage, IgnoreMapperError,
 IgnoreCancel, EndFromFeed, ReadLive, PublishOpaque, SkipMemoryAdmission, MissingClose, ReleasePermitEarly
VARIABLES db, pinned, pos, pending, part, acknowledged, output, busy, ended,
 closed, ordered, sealed, failed, hadError, cancelled, used, actual, cap,
 memory, memoryCap, visible, witness, permit, retired
vars == <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,closed,
 ordered,sealed,failed,hadError,cancelled,used,actual,cap,memory,memoryCap,visible,witness,permit,retired>>
Inputs == <<"graph","key","end">>
Expected == << <<1,0,0>>,<<1,1,0>>,<<2,0,0>> >>
Open == ~failed /\ ~sealed /\ ~retired
Ready == Open /\ (~cancelled \/ IgnoreCancel)
Init == /\ db=0 /\ pinned=0 /\ pos=1 /\ pending=0 /\ part=0
 /\ acknowledged= << >> /\ output= << >> /\ busy=FALSE /\ ended=FALSE
 /\ closed=FALSE /\ ordered=FALSE /\ sealed=FALSE /\ failed=FALSE
 /\ hadError=FALSE /\ cancelled=FALSE /\ used=0 /\ actual=0
 /\ cap \in {3,8} /\ memory=0 /\ memoryCap \in {0,1,2}
 /\ visible=0 /\ witness=FALSE /\ permit=TRUE /\ retired=FALSE
Mutate == /\ Open /\ db=0 /\ db'=1
 /\ UNCHANGED <<pinned,pos,pending,part,acknowledged,output,busy,ended,closed,ordered,sealed,failed,hadError,cancelled,used,actual,cap,memory,memoryCap,visible,witness,permit,retired>>
Capture == /\ Ready /\ pending=0 /\ ~ended /\ ~busy /\ pos<=3
 /\ memoryCap>=1
 /\ pending'=pos /\ pos'=pos+1 /\ memory'=1
 /\ UNCHANGED <<db,pinned,part,acknowledged,output,busy,ended,closed,ordered,sealed,failed,hadError,cancelled,used,actual,cap,memoryCap,visible,witness,permit,retired>>
Map(error) == /\ Ready /\ pending#0 /\ ~busy /\ used<cap /\ error \in BOOLEAN
 /\ LET more == pending=1 /\ part=0
        nextMem == IF more THEN 2 ELSE 0
        stop == (error /\ ~IgnoreMapperError) \/ (nextMem>memoryCap /\ ~SkipMemoryAdmission)
        done == ~more \/ SkipPending
        version == IF ReadLive THEN db ELSE pinned
    IN /\ failed'=stop
       /\ hadError'=(hadError \/ error)
       /\ output'=(IF stop \/ pending=3 THEN output ELSE Append(output,<<pending,part,version>>))
       /\ acknowledged'=(IF ~stop /\ done THEN Append(acknowledged,pending) ELSE acknowledged)
       /\ ended'=(ended \/ (~stop /\ done /\ pending=3))
       /\ pending'=(IF ~stop /\ done THEN 0 ELSE pending)
       /\ part'=(IF stop \/ done THEN 0 ELSE IF LoseContinuation THEN 0 ELSE 1)
       /\ memory'=(IF stop THEN memory ELSE IF done THEN 0 ELSE nextMem)
       /\ busy' \in (IF stop \/ pending=3 THEN {FALSE} ELSE BOOLEAN)
 /\ used'=used+1 /\ actual'=actual+1
 /\ UNCHANGED <<db,pinned,pos,closed,ordered,sealed,cancelled,cap,memoryCap,visible,witness,permit,retired>>
Sort == /\ Ready /\ busy /\ busy'=FALSE
 /\ used'=(IF ResetUsage THEN 0 ELSE used)
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,ended,closed,ordered,sealed,failed,hadError,cancelled,actual,cap,memory,memoryCap,visible,witness,permit,retired>>
Close == /\ Ready /\ ended /\ pending=0 /\ ~closed
 /\ closed'=TRUE
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,ordered,sealed,failed,hadError,cancelled,used,actual,cap,memory,memoryCap,visible,witness,permit,retired>>
Order == /\ Ready /\ (closed \/ MissingClose) /\ ~busy /\ ~ordered
 /\ ordered'=TRUE
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,closed,sealed,failed,hadError,cancelled,used,actual,cap,memory,memoryCap,visible,witness,permit,retired>>
Seal == /\ Ready /\ ended /\ pending=0 /\ ordered
 /\ sealed'=TRUE /\ witness'=TRUE
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,closed,ordered,failed,hadError,cancelled,used,actual,cap,memory,memoryCap,visible,permit,retired>>
Cancel == /\ Open /\ ~cancelled /\ cancelled'=TRUE
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,closed,ordered,sealed,failed,hadError,used,actual,cap,memory,memoryCap,visible,witness,permit,retired>>
Refuse == /\ Open /\ ((pending#0 /\ used>=cap) \/ (cancelled /\ ~IgnoreCancel) \/ (pending=0 /\ ~ended /\ memoryCap<1))
 /\ failed'=TRUE
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,closed,ordered,sealed,hadError,cancelled,used,actual,cap,memory,memoryCap,visible,witness,permit,retired>>
Premature == /\ Ready /\ EndFromFeed /\ pos=4 /\ ~ended
 /\ ended'=TRUE /\ pending'=0 /\ memory'=0
 /\ UNCHANGED <<db,pinned,pos,part,acknowledged,output,busy,closed,ordered,sealed,failed,hadError,cancelled,used,actual,cap,memoryCap,visible,witness,permit,retired>>
Publish == /\ sealed /\ PublishOpaque /\ visible=0 /\ visible'=1
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,closed,ordered,sealed,failed,hadError,cancelled,used,actual,cap,memory,memoryCap,witness,permit,retired>>
Retire == /\ ~retired /\ permit
 /\ retired'=TRUE /\ permit'=FALSE
 /\ pending'=0 /\ part'=0 /\ output'= << >> /\ acknowledged'= << >>
 /\ ended'=FALSE /\ closed'=FALSE /\ ordered'=FALSE /\ sealed'=FALSE
 /\ failed'=TRUE /\ memory'=0 /\ busy'=FALSE
 /\ UNCHANGED <<db,pinned,pos,hadError,cancelled,used,actual,cap,memoryCap,visible,witness>>
LosePermit == /\ ReleasePermitEarly /\ ~retired /\ permit /\ permit'=FALSE
 /\ UNCHANGED <<db,pinned,pos,pending,part,acknowledged,output,busy,ended,closed,ordered,sealed,failed,hadError,cancelled,used,actual,cap,memory,memoryCap,visible,witness,retired>>
Next == Mutate \/ Capture \/ (\E e \in BOOLEAN:Map(e)) \/ Sort \/ Close \/ Order \/ Seal \/ Cancel \/ Refuse \/ Premature \/ Publish \/ Retire \/ LosePermit
TypeOK == /\ pos \in 1..4 /\ pending \in 0..3 /\ part \in 0..1 /\ used \in 0..8
 /\ visible \in 0..1 /\ Len(output)<=8
NoInputCustodyLoss == pending=0 => part=0
NoLostContinuation == ~failed /\ pending=1 /\ Len(output)=1 => part=1
NoMissingMapping == sealed => output=Expected
NoCompletePrefix == sealed => acknowledged= <<1,2,3>>
NoOverrun == actual<=cap
NoFailedSeal == sealed => ~hadError /\ ~failed
NoCancelledSeal == sealed => ~cancelled
NoMemoryOverrun == memory<=memoryCap
NoVisibleActivation == visible=0
NoUnclosedSeal == ordered => closed
NoUnleasedStage == ~retired => permit
NoSuccessWitness == ~witness
=============================================================================
