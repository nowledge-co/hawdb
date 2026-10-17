---------------- MODULE HawDBProjectionFullRebuild ----------------
EXTENDS Integers, FiniteSets
CONSTANTS SkipSource, SkipEnd, DedupOwners, ReadLive, IgnoreMapperError, ResetCredits, RetainOld, PublishPrefix, IgnoreSource, IgnoreInstance, IgnoreBase, IgnoreDurability, IgnoreCancel, ReleasePinEarly, ReleasePermitEarly
VARIABLES shape, db, instance, head, durable, phase, source, sourceInstance, owners, seen, rows, ended, sealed, calls, actualCalls, cap, error, cancelled, pin, permit, visible, published, publishDb, publishInstance, publishBase, publishDurable, writeWitness,readerOpen
vars == <<shape,db,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
OwnerId(n) == IF shape="duplicate" THEN 1 ELSE n
Doc(n,e) == <<OwnerId(n),e,0>>
Canonical == {Doc(n,0):n \in owners}
Old == {<<3,0,1>>}
HeadDocs(h) == IF h=0 THEN Old ELSE {<<4,0,1>>}
Owned == phase \in {"mapping","ordered","building","prepared","failed"}
Working == Owned /\ phase#"failed"
Ready == Working /\ (~cancelled \/ IgnoreCancel)
Unique == Cardinality({OwnerId(n):n \in owners})=Cardinality(owners)
Init == /\ shape \in {"pair","duplicate","empty"} /\ db=0 /\ instance=0 /\ head=0
 /\ durable \in BOOLEAN /\ phase="idle" /\ source=0 /\ sourceInstance=0
 /\ owners=(IF shape="empty" THEN {} ELSE {1,2}) /\ seen={} /\ rows={} /\ ended=FALSE /\ sealed={}
 /\ calls=0 /\ actualCalls=0 /\ cap \in {1,3} /\ error=FALSE /\ cancelled=FALSE
 /\ pin=FALSE /\ permit=FALSE /\ visible=Old /\ published=FALSE
 /\ publishDb=0 /\ publishInstance=0 /\ publishBase=0 /\ publishDurable=FALSE /\ writeWitness=FALSE /\ readerOpen=TRUE
Start == phase="idle"
 /\ phase'=("mapping")
 /\ source'=(db)
 /\ sourceInstance'=(instance)
 /\ pin'=(TRUE)
 /\ permit'=(TRUE)
 /\ UNCHANGED <<shape,db,instance,head,durable,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Write == Working /\ db=0
 /\ db'=(1)
 /\ writeWitness'=(TRUE)
 /\ UNCHANGED <<shape,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,readerOpen>>
ReplaceInstance == Working /\ instance=0
 /\ instance'=(1)
 /\ UNCHANGED <<shape,db,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
ChangeHead == Working /\ head=0
 /\ head'=(1)
 /\ visible'=(HeadDocs(1))
 /\ UNCHANGED <<shape,db,instance,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Map(n,ok) == Ready /\ phase="mapping" /\ n \in owners \ seen /\ ok \in BOOLEAN /\ (calls<cap \/ ResetCredits)
 /\ phase'=((IF ok \/ IgnoreMapperError THEN phase ELSE "failed"))
 /\ seen'=((IF ok \/ IgnoreMapperError THEN seen \cup {n} ELSE seen))
 /\ rows'=((IF (~ok /\ ~IgnoreMapperError) \/ (SkipSource /\ n=2) THEN rows ELSE rows \cup {Doc(n,IF ReadLive THEN db ELSE source)}))
 /\ calls'=((IF ResetCredits THEN 1 ELSE calls+1))
 /\ actualCalls'=(actualCalls+1)
 /\ error'=(error \/ ~ok)
 /\ UNCHANGED <<shape,db,instance,head,durable,source,sourceInstance,owners,ended,sealed,cap,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
End == Ready /\ phase="mapping" /\ (seen=owners \/ SkipEnd) /\ (calls<cap \/ ResetCredits)
 /\ phase'=("ordered")
 /\ ended'=(~SkipEnd)
 /\ calls'=((IF ResetCredits THEN 1 ELSE calls+1))
 /\ actualCalls'=(actualCalls+1)
 /\ UNCHANGED <<shape,db,instance,head,durable,source,sourceInstance,owners,seen,rows,sealed,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Order == Ready /\ phase="ordered"
 /\ phase'=((IF Unique \/ DedupOwners THEN "building" ELSE "failed"))
 /\ UNCHANGED <<shape,db,instance,head,durable,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Build(row) == Ready /\ phase="building" /\ row \in rows \ sealed
 /\ sealed'=(sealed \cup {row})
 /\ UNCHANGED <<shape,db,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
LeakPrefix == Ready /\ phase="building" /\ PublishPrefix /\ sealed#{} /\ sealed#rows
 /\ visible'=(sealed)
 /\ UNCHANGED <<shape,db,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Prepare == Ready /\ phase="building" /\ sealed=rows
 /\ phase'=("prepared")
 /\ UNCHANGED <<shape,db,instance,head,durable,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Synchronize == Ready /\ phase="prepared" /\ db=source /\ ~durable
 /\ durable'=(TRUE)
 /\ UNCHANGED <<shape,db,instance,head,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Publish == Ready /\ phase="prepared" /\ (db=source \/ IgnoreSource) /\ (instance=sourceInstance \/ IgnoreInstance) /\ (head=0 \/ IgnoreBase) /\ (durable \/ IgnoreDurability)
 /\ phase'=("published")
 /\ visible'=((IF RetainOld THEN sealed \cup Old ELSE sealed))
 /\ published'=(TRUE)
 /\ publishDb'=(db)
 /\ publishInstance'=(instance)
 /\ publishBase'=(head)
 /\ publishDurable'=(durable)
 /\ pin'=(FALSE)
 /\ permit'=(FALSE)
 /\ UNCHANGED <<shape,db,instance,head,durable,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,writeWitness,readerOpen>>
Cancel == Working /\ ~cancelled
 /\ cancelled'=(TRUE)
 /\ UNCHANGED <<shape,db,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Refuse == Working /\ ((cancelled /\ ~IgnoreCancel) \/ (phase="mapping" /\ calls>=cap /\ ~ResetCredits) \/ (phase="prepared" /\ ((db#source /\ ~IgnoreSource) \/ (instance#sourceInstance /\ ~IgnoreInstance) \/ (head#0 /\ ~IgnoreBase) \/ (~durable /\ ~IgnoreDurability))))
 /\ phase'=("failed")
 /\ UNCHANGED <<shape,db,instance,head,durable,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
LosePin == Owned /\ pin /\ ReleasePinEarly
 /\ pin'=(FALSE)
 /\ UNCHANGED <<shape,db,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
LosePermit == Owned /\ permit /\ ReleasePermitEarly
 /\ permit'=(FALSE)
 /\ UNCHANGED <<shape,db,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
Retire == phase#"retired"
 /\ phase'=("retired")
 /\ pin'=(FALSE)
 /\ permit'=(FALSE)
 /\ UNCHANGED <<shape,db,instance,head,durable,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness,readerOpen>>
CloseReader == Owned /\ readerOpen
 /\ readerOpen'=FALSE
 /\ UNCHANGED <<shape,db,instance,head,durable,phase,source,sourceInstance,owners,seen,rows,ended,sealed,calls,actualCalls,cap,error,cancelled,pin,permit,visible,published,publishDb,publishInstance,publishBase,publishDurable,writeWitness>>
Next == Start \/ Write \/ ReplaceInstance \/ ChangeHead \/ (\E n \in owners,ok \in BOOLEAN:Map(n,ok)) \/ End \/ Order \/ (\E row \in rows:Build(row)) \/ LeakPrefix \/ Prepare \/ Synchronize \/ Publish \/ Cancel \/ Refuse \/ LosePin \/ LosePermit \/ CloseReader \/ Retire
TypeOK == /\ shape \in {"pair","duplicate","empty"}
 /\ phase \in {"idle","mapping","ordered","building","prepared","published","failed","retired"}
 /\ db \in 0..1 /\ instance \in 0..1 /\ head \in 0..1
 /\ source \in 0..1 /\ sourceInstance \in 0..1
 /\ cap \in {1,3} /\ calls \in 0..3 /\ actualCalls \in 0..3
 /\ owners \subseteq {1,2} /\ seen \subseteq owners
 /\ rows \subseteq {<<id,e,0>>:id \in {1,2},e \in 0..1} /\ sealed \subseteq rows
 /\ visible \subseteq ({<<id,e,0>>:id \in {1,2},e \in 0..1} \cup Old \cup HeadDocs(1))
 /\ publishDb \in 0..1 /\ publishInstance \in 0..1 /\ publishBase \in 0..1
 /\ {durable,ended,error,cancelled,pin,permit,published,publishDurable,writeWitness,readerOpen} \subseteq BOOLEAN
CompleteInput == (phase="prepared" \/ published) => seen=owners /\ ended /\ rows=Canonical
UniqueOwners == (phase="prepared" \/ published) => Unique
PinnedMapping == \A row \in rows:row[2]=source
NoFailedPublication == published => ~error /\ ~cancelled
CompleteReplacement == published => visible=Canonical
InvisiblePreparation == ~published => visible=HeadDocs(head)
SourceFence == published => publishDb=source
InstanceFence == published => publishInstance=sourceInstance
BaseFence == published => publishBase=0
DurableSource == published => publishDurable
CumulativeAdmission == actualCalls<=cap
PinnedWhileOwned == Owned => pin
AdmittedWhileOwned == Owned => permit
NoSuccessWitness == ~published
NoEmptyWitness == ~(published /\ owners={})
NoPartitionWitness == ~(published /\ Cardinality(sealed)=2)
NoWriteWitness == ~writeWitness
NoClosedReaderWitness == ~(published /\ ~readerOpen)
=============================================================================
