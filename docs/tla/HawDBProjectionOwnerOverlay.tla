---------------- MODULE HawDBProjectionOwnerOverlay ----------------
EXTENDS Integers, FiniteSets
CONSTANTS OmitOwner, IgnoreTombstone, DocIdPriority, HideDuplicateOwners, ReadCAtE, IgnoreRepeatConflict, ResetAdmission, SkipEnd, IgnoreContext
VARIABLES c, e, selected, seen, received, overlay, compatible, phase, ended, completed, out, charged, visits, cap, badRepeat
vars == <<c,e,selected,seen,received,overlay,compatible,phase,ended,completed,out,charged,visits,cap,badRepeat>>
Owners == {1,2}
Docs == {<<id,body,vector>>:id \in {"a","b"},body \in 0..1,vector \in 0..1}
None == <<"",0,0>>
States == [Owners -> (Docs \cup {None})]
Id(d) == IF d=None THEN "" ELSE d[1]
Nonempty(s) == {o \in Owners:s[o]#None}
Documents(s) == {s[o]:o \in Nonempty(s)}
Unique(s) == Cardinality({Id(s[o]):o \in Nonempty(s)})=Cardinality(Nonempty(s))
Changed == {o \in Owners:c[o]#e[o]}
ESeenDocs == {received[o]:o \in {x \in seen:received[x]#None}}
PriorityCandidates == {d \in Documents(c):~(\E x \in ESeenDocs:Id(x)=Id(d))} \cup ESeenDocs
PriorityIds == {Id(d):d \in PriorityCandidates}
PriorityOutput == {CHOOSE d \in PriorityCandidates:Id(d)=id : id \in PriorityIds}
Budget == visits<4 /\ (charged<cap \/ ResetAdmission)
Init == /\ c \in States /\ Unique(c) /\ e \in States /\ selected \in SUBSET Owners
 /\ (Changed \subseteq selected \/ OmitOwner) /\ compatible \in BOOLEAN /\ cap \in {1,4}
 /\ seen={} /\ received=c /\ overlay=c /\ phase="idle" /\ ended=FALSE
 /\ completed=FALSE /\ out={} /\ charged=0 /\ visits=0 /\ badRepeat=FALSE
Start == phase="idle"
 /\ phase'=(IF compatible \/ IgnoreContext THEN "mapping" ELSE "failed")
 /\ UNCHANGED <<c,e,selected,seen,received,overlay,compatible,ended,completed,out,charged,visits,cap,badRepeat>>
Map(o) == phase="mapping" /\ o \in selected \ seen /\ Budget
 /\ seen'=(seen \cup {o})
 /\ received'=([received EXCEPT ![o]=IF ReadCAtE THEN c[o] ELSE e[o]])
 /\ overlay'=(IF IgnoreTombstone /\ e[o]=None THEN overlay ELSE [overlay EXCEPT ![o]=IF ReadCAtE THEN c[o] ELSE e[o]])
 /\ charged'=(IF ResetAdmission THEN 1 ELSE charged+1)
 /\ visits'=(visits+1)
 /\ UNCHANGED <<c,e,selected,compatible,phase,ended,completed,out,cap,badRepeat>>
Repeat(o,value) == phase="mapping" /\ o \in seen /\ value \in {e[o],c[o]} /\ Budget
 /\ phase'=(IF value#received[o] /\ ~IgnoreRepeatConflict THEN "failed" ELSE phase)
 /\ received'=(IF value#received[o] /\ ~IgnoreRepeatConflict THEN received ELSE [received EXCEPT ![o]=value])
 /\ overlay'=(IF (value#received[o] /\ ~IgnoreRepeatConflict) \/ (IgnoreTombstone /\ value=None) THEN overlay ELSE [overlay EXCEPT ![o]=value])
 /\ badRepeat'=(badRepeat \/ value#received[o])
 /\ charged'=(IF ResetAdmission THEN 1 ELSE charged+1)
 /\ visits'=(visits+1)
 /\ UNCHANGED <<c,e,selected,seen,compatible,ended,completed,out,cap>>
End == phase="mapping" /\ (seen=selected \/ SkipEnd) /\ Budget
 /\ phase'=("reduce")
 /\ ended'=(seen=selected)
 /\ charged'=(IF ResetAdmission THEN 1 ELSE charged+1)
 /\ visits'=(visits+1)
 /\ UNCHANGED <<c,e,selected,seen,received,overlay,compatible,completed,out,cap,badRepeat>>
Reduce == phase="reduce"
 /\ phase'=(IF Unique(overlay) \/ HideDuplicateOwners \/ DocIdPriority THEN "complete" ELSE "failed")
 /\ completed'=(Unique(overlay) \/ HideDuplicateOwners \/ DocIdPriority)
 /\ out'=(IF DocIdPriority THEN PriorityOutput ELSE Documents(overlay))
 /\ UNCHANGED <<c,e,selected,seen,received,overlay,compatible,ended,charged,visits,cap,badRepeat>>
Refuse == phase="mapping" /\ (charged>=cap \/ visits>=4)
 /\ phase'=("failed")
 /\ UNCHANGED <<c,e,selected,seen,received,overlay,compatible,ended,completed,out,charged,visits,cap,badRepeat>>
Retire == phase#"retired"
 /\ phase'=("retired")
 /\ UNCHANGED <<c,e,selected,seen,received,overlay,compatible,ended,completed,out,charged,visits,cap,badRepeat>>
Next == Start \/ (\E o \in Owners:Map(o)) \/ (\E o \in Owners,value \in Docs \cup {None}:Repeat(o,value)) \/ End \/ Reduce \/ Refuse \/ Retire
TypeOK == c \in States /\ e \in States /\ overlay \in States /\ received \in States
 /\ selected \subseteq Owners /\ seen \subseteq selected /\ out \subseteq Docs
 /\ phase \in {"idle","mapping","reduce","complete","failed","retired"}
 /\ cap \in {1,4} /\ charged \in 0..4 /\ visits \in 0..4
 /\ {compatible,ended,completed,badRepeat} \subseteq BOOLEAN
CompleteCoverage == completed => ended /\ Changed \subseteq seen
CompleteOwnerState == completed => overlay=e
CompleteDocumentSet == completed => out=Documents(e)
UniqueResult == completed => Unique(e) /\ Unique(overlay)
NoConflictingResult == completed => ~badRepeat
ContextAuthority == completed => compatible
CumulativeAdmission == visits<=cap
NoCrossPhaseCollisionAccepted == ~(completed /\ (\E a,b \in Owners:a#b /\ a \in Changed /\ b \notin Changed /\ e[a]#None /\ Id(e[a])=Id(e[b])))
NoSuccessWitness == ~completed
NoDeletionWitness == ~(completed /\ (\E o \in Owners:c[o]#None /\ e[o]=None))
NoSwapWitness == ~(completed /\ Id(c[1])#"" /\ Id(c[2])#"" /\ Id(c[1])#Id(c[2]) /\ Id(e[1])=Id(c[2]) /\ Id(e[2])=Id(c[1]))
NoRepeatWitness == ~(completed /\ visits>Cardinality(selected)+1)
NoEmptyWitness == ~(completed /\ Documents(c)#{} /\ Documents(e)={})
=============================================================================
