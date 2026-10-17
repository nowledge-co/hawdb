---------------- MODULE HawDBProjectionCaptureClosure ----------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS MaxEpoch, SkipDirectSchema, SkipNestedSchema, SkipOpaque,
          SkipDirectEdge, SkipConnected, PublishBeforeWal, SkipReplayBarrier,
          DropCheckpointFloor, IgnoreRebuildRecord
VARIABLES db, checkpoint, wal, phase, job, consumer, bad, rebuildProgress, pointProgress
vars == <<db,checkpoint,wal,phase,job,consumer,bad,rebuildProgress,pointProgress>>
Kinds == {"point","schema","opaque","edge","connected","snapshot"}
World == [node |-> 0,schema |-> 0,opaque |-> 0,edge |-> FALSE,connected |-> FALSE]
View(w) == [i \in 1..3 |-> CASE i = 1 -> w.node + (IF w.edge THEN 4 ELSE 0)
 [] i = 2 -> w.schema+2*w.opaque [] OTHER -> IF w.connected THEN 1 ELSE -1]
After(w,k) == CASE k = "point" -> [w EXCEPT !.node = @+1]
 [] k = "schema" -> [w EXCEPT !.schema = @+1]
 [] k \in {"opaque","snapshot"} -> [w EXCEPT !.opaque = @+1]
 [] k = "edge" -> [w EXCEPT !.edge = TRUE]
 [] k = "connected" -> [w EXCEPT !.connected = TRUE]
Allowed(w,k) == (k # "edge" \/ ~w.edge) /\ (k # "connected" \/ ~w.connected)
Empty == [items |-> {},rebuild |-> FALSE]
Event(k,route,nested,replaying) ==
 LET unsupported == k \in {"schema","opaque"}
     omitted == (k = "schema" /\ ((SkipDirectSchema /\ route = "direct" /\ ~replaying)
                  \/ (SkipNestedSchema /\ nested)))
               \/ (k = "opaque" /\ SkipOpaque)
               \/ (replaying /\ SkipReplayBarrier /\ unsupported)
     gap == unsupported /\ ~omitted
     ids == CASE k = "point" -> {1}
         [] k = "edge" -> IF SkipDirectEdge /\ route = "direct" /\ ~replaying THEN {} ELSE {1}
         [] k = "connected" -> IF SkipConnected /\ route = "direct" /\ ~replaying THEN {} ELSE {3}
         [] k = "schema" /\ nested -> {1}
         [] OTHER -> {}
 IN [gap |-> gap,items |-> ids,rebuild |-> k = "snapshot"]
Install(state,k,route,nested,replaying) ==
 LET event == Event(k,route,nested,replaying)
 IN IF event.gap THEN [state EXCEPT !.floor = state.e,!.events = <<>>]
    ELSE IF event.items # {} \/ event.rebuild THEN
      [state EXCEPT !.events = Append(@,[epoch |-> state.e,items |-> event.items,rebuild |-> event.rebuild])]
    ELSE state
IdleJob == [kind |-> "point",route |-> "prepared",nested |-> FALSE,
            before |-> [w |-> World,e |-> 0,floor |-> 0,events |-> <<>>]]
Init == /\ db = [w |-> World,e |-> 0,floor |-> 0,events |-> <<>>]
 /\ checkpoint = db /\ wal = <<>> /\ phase = "idle" /\ job = IdleJob
 /\ consumer = [cursor |-> 0,index |-> View(World),done |-> FALSE,refused |-> FALSE]
 /\ bad = FALSE /\ rebuildProgress = FALSE /\ pointProgress = FALSE
Prepare(k,route,nested) == /\ phase = "idle" /\ ~consumer.done /\ db.e < MaxEpoch
 /\ k \in Kinds /\ route \in {"direct","prepared"} /\ nested \in BOOLEAN /\ Allowed(db.w,k)
 /\ job' = [kind |-> k,route |-> route,nested |-> nested,before |-> db]
 /\ phase' = "prepared"
 /\ db' = (IF PublishBeforeWal THEN
      LET future == [db EXCEPT !.e = @+1]
          staged == Install(future,k,route,nested,FALSE)
      IN [db EXCEPT !.floor = staged.floor,!.events = staged.events] ELSE db)
 /\ UNCHANGED <<checkpoint,wal,consumer,bad,rebuildProgress,pointProgress>>
Write(outcome) == /\ phase = "prepared" /\ outcome \in {"ok","partial","uncertain"}
 /\ wal' = Append(wal,[kind |-> job.kind,route |-> job.route,nested |-> job.nested,complete |-> outcome # "partial"])
 /\ phase' = (IF outcome = "ok" THEN "written" ELSE "failed")
 /\ UNCHANGED <<db,checkpoint,job,consumer,bad,rebuildProgress,pointProgress>>
Commit == /\ phase = "written"
 /\ LET next == [job.before EXCEPT !.w = After(job.before.w,job.kind),!.e = @+1]
    IN db' = Install(next,job.kind,job.route,job.nested,FALSE)
 /\ phase' = "idle"
 /\ UNCHANGED <<checkpoint,wal,job,consumer,bad,rebuildProgress,pointProgress>>
RECURSIVE ValidPrefix(_)
ValidPrefix(records) == IF records = <<>> \/ ~Head(records).complete THEN <<>>
 ELSE <<Head(records)>> \o ValidPrefix(Tail(records))
RECURSIVE Replay(_,_)
Replay(state,records) == IF records = <<>> THEN state ELSE
 LET record == Head(records)
     next == [state EXCEPT !.w = After(state.w,record.kind),!.e = @+1]
 IN Replay(Install(next,record.kind,record.route,record.nested,TRUE),Tail(records))
Recover == /\ db' = Replay(checkpoint,ValidPrefix(wal)) /\ wal' = ValidPrefix(wal) /\ phase' = "idle"
 /\ UNCHANGED <<checkpoint,job,consumer,bad,rebuildProgress,pointProgress>>
Checkpoint == /\ phase = "idle"
 /\ checkpoint' = (IF DropCheckpointFloor THEN [db EXCEPT !.floor = 0] ELSE db)
 /\ wal' = <<>>
 /\ UNCHANGED <<db,phase,job,consumer,bad,rebuildProgress,pointProgress>>
CatchUp == /\ phase = "idle" /\ ~consumer.done
 /\ LET selected == SelectSeq(db.events,LAMBDA event: event.epoch > consumer.cursor)
        refused == consumer.cursor < db.floor \/
          (~IgnoreRebuildRecord /\ \E i \in 1..Len(selected): selected[i].rebuild)
        ids == UNION {selected[i].items: i \in 1..Len(selected)}
        expected == View(db.w)
        applied == [i \in 1..3 |-> IF i \in ids THEN expected[i] ELSE consumer.index[i]]
    IN /\ consumer' = [consumer EXCEPT !.done = TRUE,!.refused = refused,
                        !.index = IF refused THEN consumer.index ELSE applied]
       /\ bad' = (bad \/ (~refused /\ applied # expected))
       /\ rebuildProgress' = (rebuildProgress \/ refused)
       /\ pointProgress' = (pointProgress \/ (~refused /\ consumer.cursor = db.floor /\ db.floor > 0 /\ db.e > db.floor))
 /\ UNCHANGED <<db,checkpoint,wal,phase,job>>
Rebuild == /\ phase = "idle" /\ consumer.done /\ consumer.refused
 /\ consumer' = [cursor |-> db.e,index |-> View(db.w),done |-> FALSE,refused |-> FALSE]
 /\ UNCHANGED <<db,checkpoint,wal,phase,job,bad,rebuildProgress,pointProgress>>
Next == (\E k \in Kinds,route \in {"direct","prepared"},nested \in BOOLEAN: Prepare(k,route,nested))
 \/ (\E outcome \in {"ok","partial","uncertain"}: Write(outcome)) \/ Commit \/ Recover \/ Checkpoint \/ CatchUp \/ Rebuild
TypeOK == /\ db.e \in 0..MaxEpoch /\ db.floor \in 0..MaxEpoch
 /\ phase \in {"idle","prepared","written","failed"} /\ consumer.cursor \in 0..MaxEpoch
NoStaleAcceptedProjection == ~bad
NoUncommittedCapture == phase \in {"prepared","written","failed"} =>
 /\ db.floor = job.before.floor /\ db.events = job.before.events /\ db.e = job.before.e
NoRebuildWitness == ~rebuildProgress
NoPointAfterRebuildWitness == ~pointProgress
=============================================================================
