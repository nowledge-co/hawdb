---------------- MODULE HawDBRelationalIndexPrefixReads ----------------
EXTENDS Integers, FiniteSets, Sequences
CONSTANTS MaxEpoch, LateTracking, SkipOldPrefix, SkipNewPrefix,
 SkipPayload, IgnoreReadBarrier, IgnoreIncomplete, TrustUnindexed,
 NarrowOr, NarrowNot, NarrowPage, PrunePinned, PublishBeforeWal, MixWriterSummary
VARIABLES db, job, pending, staleAccepted, falseWriterConflict, disjointProgress, writerProgress
vars == <<db,job,pending,staleAccepted,falseWriterConflict,disjointProgress,writerProgress>>
Slots == {1,2}
Groups == {1,2}
Prefixes == {<<g>>:g \in Groups} \union {<<g,o>>:g \in Groups,o \in {0,1}}
RowKey(s) == <<-1,s>>
Barrier == <<0>>
Keys == Prefixes \union {RowKey(s):s \in Slots} \union {Barrier}
Zero == [k \in Keys |-> 0]
Rows == [Slots -> [live:BOOLEAN,group:Groups,ordinal:{0,1},payload:{0,1}]]
InitialRows(firstOrdinal) == [s \in Slots |-> [live |-> TRUE,group |-> s,ordinal |-> IF s = 1 THEN firstOrdinal ELSE 1,payload |-> 0]]
Prefix(row) == IF row.live THEN {<<row.group>>,<<row.group,row.ordinal>>} ELSE {}
Open == job.phase \in {"captured","read"}
Tracked == Open /\ (~LateTracking \/ job.phase = "read")
Matches(row,q) == row.live /\ CASE q = "or" -> row.group = 1 \/ row.payload = 1
 [] q = "not" -> row.group # 1
 [] q = "composite" -> row.group = 1 /\ row.ordinal = 0
 [] q = "page" -> row.group = 1 /\ row.ordinal > 0
 [] OTHER -> row.group = 1
Output(rows,q) == LET selected == {s \in Slots:Matches(rows[s],q)}
 top == {s \in selected:\A t \in selected:rows[s].ordinal < rows[t].ordinal \/
  (rows[s].ordinal = rows[t].ordinal /\ s <= t)}
 IN {<<s,rows[s].group,rows[s].ordinal,rows[s].payload>>:s \in top}
Wanted(q) == IF q = "composite" THEN {<<1,0>>} ELSE {<<1>>}
Broad(q) == (~job.indexed /\ ~TrustUnindexed) \/ (q = "or" /\ ~NarrowOr) \/ (q = "not" /\ ~NarrowNot)
Chosen(q) == IF q = "page" /\ NarrowPage THEN
 {RowKey(s):s \in {t \in Slots:Matches(job.rows[t],q)}} ELSE Wanted(q)
IndexValid == /\ ~db.poisoned /\ db.instance = job.instance
 /\ (IF job.broad THEN db.epoch = job.epoch ELSE
       \A k \in job.keys \union (IF IgnoreReadBarrier THEN {} ELSE {Barrier}):db.stamps[k] <= job.epoch)
OracleValid == /\ Output(job.rows,job.query) = Output(db.rows,job.query)
 /\ (IF job.broad THEN db.epoch = job.epoch ELSE
       \A k \in Wanted(job.query) \union {Barrier}:db.oracle[k] <= job.epoch)
Reference(old,new,unknown) == Prefix(old) \union Prefix(new) \union (IF unknown THEN {Barrier} ELSE {})
Optimized(old,new,unknown) ==
 IF ~Tracked \/ ~db.indexed THEN {} ELSE
 IF unknown THEN IF IgnoreIncomplete THEN {} ELSE {Barrier} ELSE
 IF SkipPayload /\ old.group = new.group /\ old.ordinal = new.ordinal /\ old.live = new.live THEN {} ELSE
 (IF SkipOldPrefix THEN {} ELSE Prefix(old)) \union (IF SkipNewPrefix THEN {} ELSE Prefix(new))
ChangedStamps(slot,new,unknown) == LET touched == Optimized(db.rows[slot],new,unknown) \union {RowKey(slot)}
 IN [k \in Keys |-> IF k \in touched THEN db.epoch+1 ELSE db.stamps[k]]
IdlePending == [phase |-> "idle",slot |-> 1,new |-> InitialRows(0)[1],base |-> 0,unknown |-> FALSE,complete |-> FALSE]
Init == /\ db \in {[epoch |-> 0,indexed |-> i,rows |-> InitialRows(o),stamps |-> Zero,oracle |-> Zero,
                   rowStamps |-> [s \in Slots |-> 0],instance |-> 0,poisoned |-> FALSE]:i \in BOOLEAN,o \in {0,1}}
 /\ job = [phase |-> "idle",epoch |-> 0,rows |-> InitialRows(0),indexed |-> TRUE,query |-> "single",keys |-> {},broad |-> FALSE,instance |-> 0]
 /\ pending = IdlePending /\ staleAccepted = FALSE /\ falseWriterConflict = FALSE
 /\ disjointProgress = FALSE /\ writerProgress = FALSE
Capture == /\ job.phase = "idle" /\ pending.phase = "idle" /\ ~db.poisoned
 /\ job' = [job EXCEPT !.phase = "captured",!.epoch = db.epoch,!.rows = db.rows,!.indexed = db.indexed,!.instance = db.instance]
 /\ UNCHANGED <<db,pending,staleAccepted,falseWriterConflict,disjointProgress,writerProgress>>
Read(q) == /\ job.phase = "captured" /\ ~db.poisoned /\ db.instance = job.instance
 /\ q \in {"single","composite","page","or","not"}
 /\ job' = [job EXCEPT !.phase = "read",!.query = q,!.broad = Broad(q),!.keys = Chosen(q)]
 /\ UNCHANGED <<db,pending,staleAccepted,falseWriterConflict,disjointProgress,writerProgress>>
Prepare(slot,new,base,unknown) == /\ pending.phase = "idle" /\ ~db.poisoned /\ db.epoch < MaxEpoch
 /\ job.phase \in {"idle","captured","read"} /\ slot \in Slots
 /\ new \in [live:BOOLEAN,group:Groups,ordinal:{0,1},payload:{0,1}] /\ new # db.rows[slot]
 /\ base \in 0..db.epoch /\ db.rowStamps[slot] <= base /\ unknown \in BOOLEAN /\ (unknown => (Open /\ db.indexed))
 /\ LET bad == MixWriterSummary /\ \E k \in Prefix(new):db.stamps[k] > base
    IN /\ pending' = [phase |-> IF bad THEN "idle" ELSE "prepared",slot |-> slot,new |-> new,base |-> base,unknown |-> unknown,complete |-> FALSE]
       /\ falseWriterConflict' = (falseWriterConflict \/ bad)
       /\ db' = (IF PublishBeforeWal /\ ~bad THEN [db EXCEPT !.stamps = ChangedStamps(slot,new,unknown)] ELSE db)
 /\ UNCHANGED <<job,staleAccepted,disjointProgress,writerProgress>>
Wal(outcome) == /\ pending.phase = "prepared" /\ outcome \in {"ok","rollback","uncertain","torn"}
 /\ pending' = (IF outcome = "rollback" THEN IdlePending ELSE [pending EXCEPT !.phase = IF outcome = "ok" THEN "written" ELSE IF outcome = "rollback" THEN "idle" ELSE "failed",!.complete = outcome \in {"ok","uncertain"}])
 /\ db' = [db EXCEPT !.poisoned = outcome \in {"uncertain","torn"}]
 /\ UNCHANGED <<job,staleAccepted,falseWriterConflict,disjointProgress,writerProgress>>
Commit == /\ pending.phase = "written"
 /\ LET reference == Reference(db.rows[pending.slot],pending.new,pending.unknown)
        next == db.epoch+1
    IN db' = [db EXCEPT !.epoch = next,!.rows[pending.slot] = pending.new,
       !.stamps = ChangedStamps(pending.slot,pending.new,pending.unknown),
       !.oracle = [k \in Keys |-> IF k \in reference THEN next ELSE db.oracle[k]],!.rowStamps[pending.slot] = next]
 /\ writerProgress' = (writerProgress \/ (pending.base < db.epoch /\ \E k \in Prefix(pending.new):db.stamps[k] > pending.base))
 /\ pending' = IdlePending
 /\ UNCHANGED <<job,staleAccepted,falseWriterConflict,disjointProgress>>
Recover == /\ pending.phase = "failed" /\ db.instance = 0
 /\ LET next == db.epoch + (IF pending.complete THEN 1 ELSE 0)
    IN db' = [db EXCEPT !.epoch = next,!.rows[pending.slot] = IF pending.complete THEN pending.new ELSE @,
        !.stamps = Zero,!.rowStamps = [s \in Slots |-> next],!.instance = 1,!.poisoned = FALSE]
 /\ pending' = IdlePending
 /\ UNCHANGED <<job,staleAccepted,falseWriterConflict,disjointProgress,writerProgress>>
Prune(k) == /\ pending.phase = "idle" /\ Open /\ k \in Keys /\ db.stamps[k] # 0
 /\ (db.stamps[k] <= job.epoch \/ PrunePinned)
 /\ db' = [db EXCEPT !.stamps[k] = 0]
 /\ UNCHANGED <<job,pending,staleAccepted,falseWriterConflict,disjointProgress,writerProgress>>
Accept == /\ pending.phase = "idle" /\ job.phase = "read" /\ IndexValid
 /\ staleAccepted' = (staleAccepted \/ ~OracleValid)
 /\ disjointProgress' = (disjointProgress \/ (~job.broad /\ db.epoch > job.epoch /\ OracleValid))
 /\ job' = [job EXCEPT !.phase = "done"]
 /\ UNCHANGED <<db,pending,falseWriterConflict,writerProgress>>
Reject == /\ pending.phase \in {"idle","failed"} /\ Open /\ ~IndexValid
 /\ job' = [job EXCEPT !.phase = "rejected"]
 /\ UNCHANGED <<db,pending,staleAccepted,falseWriterConflict,disjointProgress,writerProgress>>
Next == Capture \/ (\E q \in {"single","composite","page","or","not"}:Read(q))
 \/ (\E s \in Slots,n \in [live:BOOLEAN,group:Groups,ordinal:{0,1},payload:{0,1}],b \in 0..db.epoch,u \in BOOLEAN:Prepare(s,n,b,u))
 \/ (\E o \in {"ok","rollback","uncertain","torn"}:Wal(o)) \/ Commit \/ Recover
 \/ (\E k \in Keys:Prune(k)) \/ Accept \/ Reject
TypeOK == /\ db.epoch \in 0..MaxEpoch /\ db.rows \in Rows /\ db.stamps \in [Keys -> 0..MaxEpoch]
 /\ job.phase \in {"idle","captured","read","done","rejected"} /\ db.instance \in {0,1}
NoStaleAcceptance == ~staleAccepted
NoFalseWriterConflict == ~falseWriterConflict
NoEarlyStamps == \A k \in Keys:db.stamps[k] <= db.epoch
NoDisjointWitness == ~disjointProgress
NoWriterWitness == ~writerProgress
=============================================================================
