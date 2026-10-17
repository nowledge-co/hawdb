---------------- MODULE HawDBJoinedProjectionReads ----------------
EXTENDS Integers, FiniteSets
CONSTANTS MaxEpoch, LateTracking, OmitParent, ReturnedParentsOnly,
 CurrentParentsOnly, MissAbsentChild, SkipOldGroup, SkipNewGroup,
 SkipPayload, NarrowPage, SkipParentPayload, WrongAlias, WrongParent, IgnoreFailure
VARIABLES db, job, stale, badFinish, progress
vars == <<db,job,stale,badFinish,progress>>
Children == {1,2}
Parents == {1,2}
ChildKey(c) == <<0,c>>
GroupKey(p) == <<1,p>>
ParentKey(p) == <<2,p>>
Keys == {ChildKey(c):c \in Children} \union {GroupKey(p):p \in Parents} \union {ParentKey(p):p \in Parents}
Zero == [k \in Keys |-> 0]
Row == [live:BOOLEAN,parent:0..2,payload:0..1]
InitialRows(p) == [c \in Children |-> [live |-> TRUE,parent |-> IF c=1 THEN p ELSE 2,payload |-> 0]]
\* Parent 0=absent, 1/2=source with different projected payload, 3=filtered.
Source(p) == p \in {1,2}
Groups(row) == IF row.live /\ row.parent # 0 THEN {GroupKey(row.parent)} ELSE {}
Open == job.phase \in {"captured","read","failed"}
Tracked == Open /\ (~LateTracking \/ job.phase # "captured")
Selected(rows,q) == IF q="point" THEN {1} ELSE {c \in Children:rows[c].live /\ rows[c].parent=1}
Matching(rows,parents,q) == {c \in Selected(rows,q):rows[c].live /\ rows[c].parent # 0 /\ Source(parents[rows[c].parent])}
Top(matches) == {c \in matches:\A d \in matches:c<=d}
Output(rows,parents,q) == {<<c,rows[c].payload,parents[rows[c].parent]>>:c \in Top(Matching(rows,parents,q))}
Root(rows,q) == IF q="point" THEN {ChildKey(1)} ELSE {GroupKey(1)}
ParentIds(rows,q) == IF q="page" THEN {1} ELSE IF rows[1].live /\ rows[1].parent # 0 THEN {rows[1].parent} ELSE {}
Wanted(rows,q) == Root(rows,q) \union {ParentKey(p):p \in ParentIds(rows,q)}
Chosen(q) == LET rows == job.rows
 root == IF q="point" /\ MissAbsentChild /\ ~rows[1].live THEN {}
  ELSE IF q="point" /\ WrongAlias THEN {ChildKey(2)}
  ELSE IF q="page" /\ NarrowPage THEN {ChildKey(c):c \in Top(Matching(rows,job.parents,q))}
  ELSE Root(rows,q)
 parents == {p \in ParentIds(rows,q):~OmitParent /\
  (~ReturnedParentsOnly \/ Source(job.parents[p])) /\ (~CurrentParentsOnly \/ Source(db.parents[p]))}
 IN root \union {ParentKey(IF WrongParent THEN 1 ELSE p):p \in parents}
Valid == \A k \in job.keys:db.stamps[k] <= job.epoch
OracleValid == /\ Output(job.rows,job.parents,job.query)=Output(db.rows,db.parents,job.query)
 /\ \A k \in Wanted(job.rows,job.query):db.oracle[k] <= job.epoch
Init == /\ db \in {[epoch |-> 0,rows |-> InitialRows(p),parents |-> [k \in Parents |-> 1],stamps |-> Zero,oracle |-> Zero]:p \in Parents}
 /\ job=[phase |-> "idle",epoch |-> 0,rows |-> InitialRows(1),parents |-> [p \in Parents |-> 1],keys |-> {},query |-> "point"]
 /\ stale=FALSE /\ badFinish=FALSE /\ progress=FALSE
Capture == /\ job.phase="idle"
 /\ job'=[job EXCEPT !.phase="captured",!.rows=db.rows,!.parents=db.parents,!.epoch=db.epoch]
 /\ UNCHANGED <<db,stale,badFinish,progress>>
Read(q,ok) == /\ job.phase="captured" /\ q \in {"point","page"} /\ ok \in BOOLEAN
 /\ job'=[job EXCEPT !.phase=IF ok THEN "read" ELSE "failed",!.query=q,!.keys=IF ok THEN Chosen(q) ELSE {}]
 /\ UNCHANGED <<db,stale,badFinish,progress>>
ChildWrite(c,row) == /\ job.phase \in {"idle","captured","read"} /\ db.epoch < MaxEpoch
 /\ c \in Children /\ row \in Row /\ row # db.rows[c]
 /\ LET old == db.rows[c]
        reference == {ChildKey(c)} \union Groups(old) \union Groups(row)
        changed == ~(SkipPayload /\ old.live=row.live /\ old.parent=row.parent)
        emitted == {ChildKey(c)} \union (IF changed /\ ~SkipOldGroup THEN Groups(old) ELSE {}) \union (IF changed /\ ~SkipNewGroup THEN Groups(row) ELSE {})
    IN db'=[db EXCEPT !.epoch=@+1,!.rows[c]=row,
      !.stamps=[k \in Keys |-> IF k \in emitted /\ (k=ChildKey(c) \/ Tracked) THEN db.epoch+1 ELSE db.stamps[k]],
      !.oracle=[k \in Keys |-> IF k \in reference THEN db.epoch+1 ELSE db.oracle[k]]]
 /\ UNCHANGED <<job,stale,badFinish,progress>>
ParentWrite(p,value) == /\ job.phase \in {"idle","captured","read"} /\ db.epoch < MaxEpoch
 /\ p \in Parents /\ value \in 0..3 /\ value # db.parents[p]
 /\ LET emit == ~(SkipParentPayload /\ Source(value) /\ Source(db.parents[p]))
    IN db'=[db EXCEPT !.epoch=@+1,!.parents[p]=value,
       !.stamps=[k \in Keys |-> IF emit /\ k=ParentKey(p) THEN db.epoch+1 ELSE db.stamps[k]],
       !.oracle=[k \in Keys |-> IF k=ParentKey(p) THEN db.epoch+1 ELSE db.oracle[k]]]
 /\ UNCHANGED <<job,stale,badFinish,progress>>
Accept == /\ (job.phase="read" \/ (IgnoreFailure /\ job.phase="failed")) /\ Valid
 /\ stale'=(stale \/ ~OracleValid)
 /\ badFinish'=(badFinish \/ job.phase="failed")
 /\ progress'=(progress \/ (db.epoch>job.epoch /\ OracleValid))
 /\ job'=[job EXCEPT !.phase="done"]
 /\ UNCHANGED db
Next == Capture \/ (\E q \in {"point","page"},ok \in BOOLEAN:Read(q,ok))
 \/ (\E c \in Children,row \in Row:ChildWrite(c,row))
 \/ (\E p \in Parents,v \in 0..3:ParentWrite(p,v)) \/ Accept
TypeOK == /\ db.epoch \in 0..MaxEpoch /\ db.rows \in [Children -> Row]
 /\ db.parents \in [Parents -> 0..3] /\ db.stamps \in [Keys -> 0..MaxEpoch]
NoStaleAcceptance == ~stale
NoAcceptedFailure == ~badFinish
NoDisjointWitness == ~progress
=============================================================================
