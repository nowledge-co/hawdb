---------------- MODULE HawDBProjectionChangeCapture ----------------
EXTENDS Integers, FiniteSets, Sequences, TLC
CONSTANTS MaxEpoch, SkipColdNode, SkipColdRelationship, SkipColdAdjacency,
          SkipIncoming, SkipCreatedLabel, CaptureAfterApply, PublishBeforeWal,
          IgnoreGap, IgnoreMetadataFailure, SkipReplayCapture, DropCheckpointFloor,
          DeleteIntermediate
VARIABLES db, checkpoint, wal, phase, job, cold, capacity, consumer,
          bad, coldProgress, rebuildProgress
vars == <<db,checkpoint,wal,phase,job,cold,capacity,consumer,bad,coldProgress,rebuildProgress>>
Kinds == {"payload","rename","doubleRename","name","addOut","addIn","delOut","delIn","delRoot","delLabel","createLabelOut","createLabelIn","other"}
Initials == {[alive |-> TRUE, id |-> 1, payload |-> 0, labelAlive |-> l,
             label |-> 1, out |-> l /\ o, incoming |-> l /\ i]: l,o,i \in BOOLEAN}
After(w,k,a) == CASE
 k = "payload" -> [w EXCEPT !.payload = a-1]
 [] k \in {"rename","doubleRename"} -> [w EXCEPT !.id = IF k = "doubleRename" THEN 2 ELSE a]
 [] k = "name" -> [w EXCEPT !.label = a]
 [] k = "addOut" -> [w EXCEPT !.out = w.alive /\ w.labelAlive]
 [] k = "addIn" -> [w EXCEPT !.incoming = w.alive /\ w.labelAlive]
 [] k = "delOut" -> [w EXCEPT !.out = FALSE]
 [] k = "delIn" -> [w EXCEPT !.incoming = FALSE]
 [] k = "delRoot" -> [w EXCEPT !.alive = FALSE, !.out = FALSE, !.incoming = FALSE]
 [] k = "delLabel" -> [w EXCEPT !.labelAlive = FALSE, !.out = FALSE, !.incoming = FALSE]
 [] k = "createLabelOut" -> [w EXCEPT !.labelAlive = TRUE, !.label = a, !.out = w.alive]
 [] k = "createLabelIn" -> [w EXCEPT !.labelAlive = TRUE, !.label = a, !.incoming = w.alive]
 [] OTHER -> w
Allowed(w,k) == CASE k \in {"payload","rename","doubleRename","delRoot"} -> w.alive
 [] k \in {"name","delLabel"} -> w.labelAlive
 [] k \in {"createLabelOut","createLabelIn"} -> ~w.labelAlive
 [] OTHER -> TRUE
Root(w) == IF w.alive THEN {<<w.id,w.payload,IF w.labelAlive /\ (w.out \/ w.incoming) THEN w.label ELSE 0>>} ELSE {}
Projection(w) == Root(w) \cup {<<3,0,0>>}
Empty == [up |-> FALSE, del |-> {}]
Capture(b,n,k,c) ==
 LET m == IF CaptureAfterApply THEN n ELSE b
     nodeOK == ~(c /\ SkipColdNode)
     edgeOK == ~(c /\ SkipColdRelationship)
     adjacencyOK == ~(c /\ SkipColdAdjacency)
     fanout == m.alive /\ m.labelAlive /\ (m.out \/ (m.incoming /\ ~SkipIncoming)) /\ nodeOK /\ adjacencyOK
     up == CASE k \in {"payload","rename","doubleRename"} -> m.alive /\ nodeOK
           [] k \in {"name","delLabel"} -> fanout
           [] k \in {"addOut","addIn"} -> m.alive /\ m.labelAlive /\ nodeOK
           [] k \in {"delOut","delIn"} -> m.alive /\ m.labelAlive /\ nodeOK /\ edgeOK
           [] k \in {"createLabelOut","createLabelIn"} -> m.alive /\ ~SkipCreatedLabel /\ nodeOK
           [] OTHER -> FALSE
     del == IF k \in {"rename","doubleRename","delRoot"} /\ m.alive /\ nodeOK THEN {m.id} ELSE {}
 IN [up |-> up, del |-> del \cup (IF DeleteIntermediate /\ k = "doubleRename" THEN {3} ELSE {})]
Install(state, event, good) == IF ~good THEN [state EXCEPT !.floor = IF IgnoreGap THEN state.floor ELSE state.e, !.events = <<>>]
 ELSE IF event.up \/ event.del # {} THEN [state EXCEPT !.events = Append(@,[epoch |-> state.e, up |-> event.up, del |-> event.del])]
 ELSE state
RECURSIVE Replay(_,_)
Replay(base, records) == IF Len(records) = 0 THEN base ELSE
 LET record == Head(records)
     nextWorld == After(base.w,record.kind,record.arg)
     next == [base EXCEPT !.w = nextWorld, !.e = @+1]
     recorded == IF SkipReplayCapture THEN next ELSE Install(next,Capture(base.w,nextWorld,record.kind,TRUE),capacity)
 IN Replay(recorded,Tail(records))
Apply(index,w,events) ==
 LET deleted == UNION {events[i].del: i \in 1..Len(events)}
     up == \E i \in 1..Len(events): events[i].up
     rows == IF up THEN Root(w) ELSE {}
     overwritten == {row[1]: row \in rows}
 IN {row \in index: row[1] \notin deleted \cup overwritten} \cup rows
IdleJob == [kind |-> "other", arg |-> 1, before |-> CHOOSE w \in Initials: TRUE, after |-> CHOOSE w \in Initials: TRUE, event |-> Empty, failed |-> FALSE]
Init == /\ db \in {[w |-> w,e |-> 0,events |-> <<>>,floor |-> 0]: w \in Initials}
 /\ checkpoint = db /\ wal = <<>> /\ phase = "idle" /\ job = IdleJob
 /\ cold \in BOOLEAN /\ capacity \in BOOLEAN
 /\ consumer = [bound |-> FALSE, done |-> FALSE, cursor |-> 0, index |-> Projection(db.w)]
 /\ bad = FALSE /\ coldProgress = FALSE /\ rebuildProgress = FALSE
\* Binding includes the source-durability barrier, not just an epoch receipt.
\* Recovered unsynchronized records are synchronized before projection binding.
Bind == /\ phase = "idle" /\ ~consumer.bound
 /\ consumer' = [consumer EXCEPT !.bound = TRUE, !.cursor = db.e, !.index = Projection(db.w)]
 /\ wal' = [i \in 1..Len(wal) |-> [wal[i] EXCEPT !.durable = TRUE]]
 /\ UNCHANGED <<db,checkpoint,phase,job,cold,capacity,bad,coldProgress,rebuildProgress>>
Prepare(k,a,fault) == /\ phase = "idle" /\ ~consumer.done /\ db.e < MaxEpoch
 /\ k \in Kinds /\ a \in {1,2} /\ fault \in BOOLEAN /\ Allowed(db.w,k)
 /\ LET next == After(db.w,k,a)
        failed == fault /\ ~IgnoreMetadataFailure
        event == IF fault THEN Empty ELSE Capture(db.w,next,k,cold)
        future == [db EXCEPT !.e = @+1]
    IN /\ job' = [kind |-> k,arg |-> a,before |-> db.w,after |-> next,event |-> event,failed |-> failed]
       /\ phase' = (IF failed THEN "failed" ELSE "prepared")
       /\ db' = IF PublishBeforeWal /\ ~failed THEN LET published == Install(future,event,capacity)
                 IN [db EXCEPT !.events = published.events, !.floor = published.floor] ELSE db
 /\ UNCHANGED <<checkpoint,wal,cold,capacity,consumer,bad,coldProgress,rebuildProgress>>
Write(outcome,durable) == /\ phase = "prepared" /\ outcome \in {"ok","partial","uncertain"} /\ durable \in BOOLEAN
 /\ wal' = Append(wal,[kind |-> job.kind,arg |-> job.arg,complete |-> outcome # "partial",durable |-> outcome = "ok" \/ (outcome = "uncertain" /\ durable)])
 /\ phase' = (IF outcome = "ok" THEN "written" ELSE "failed")
 /\ UNCHANGED <<db,checkpoint,job,cold,capacity,consumer,bad,coldProgress,rebuildProgress>>
Commit == /\ phase = "written"
 /\ db' = Install([db EXCEPT !.w = job.after, !.e = @+1],job.event,capacity)
 /\ phase' = "idle"
 /\ UNCHANGED <<checkpoint,wal,job,cold,capacity,consumer,bad,coldProgress,rebuildProgress>>
Checkpoint == /\ phase = "idle" /\ db.e > checkpoint.e
 /\ checkpoint' = [db EXCEPT !.floor = IF DropCheckpointFloor THEN 0 ELSE @]
 /\ wal' = <<>>
 /\ UNCHANGED <<db,phase,job,cold,capacity,consumer,bad,coldProgress,rebuildProgress>>
Crash(keep) == /\ keep \in BOOLEAN /\ ~consumer.done
 /\ LET torn == Len(wal) # 0 /\ ~wal[Len(wal)].complete
        tailOK == Len(wal) = 0 \/ keep \/ wal[Len(wal)].durable
        retained == IF tailOK THEN wal ELSE SubSeq(wal,1,Len(wal)-1)
    IN /\ db' = (IF torn THEN db ELSE Replay(checkpoint,retained))
       /\ wal' = (IF torn THEN wal ELSE retained)
       /\ phase' = (IF torn THEN "recovery_failed" ELSE "idle")
 /\ job' = IdleJob /\ cold' = TRUE
 /\ UNCHANGED <<checkpoint,capacity,consumer,bad,coldProgress,rebuildProgress>>
Repair == /\ phase = "recovery_failed"
 /\ wal' = SubSeq(wal,1,Len(wal)-1)
 /\ db' = Replay(checkpoint,wal') /\ phase' = "idle"
 /\ UNCHANGED <<checkpoint,job,cold,capacity,consumer,bad,coldProgress,rebuildProgress>>
CatchUp == /\ phase = "idle" /\ consumer.bound /\ ~consumer.done /\ consumer.cursor < db.e /\ consumer.cursor >= db.floor
 /\ LET events == SelectSeq(db.events,LAMBDA event: event.epoch > consumer.cursor)
        next == Apply(consumer.index,db.w,events)
    IN /\ consumer' = [consumer EXCEPT !.done = TRUE, !.cursor = db.e, !.index = next]
       /\ bad' = (bad \/ next # Projection(db.w))
       /\ coldProgress' = (coldProgress \/ (cold /\ next # consumer.index /\ next = Projection(db.w)))
 /\ UNCHANGED <<db,checkpoint,wal,phase,job,cold,capacity,rebuildProgress>>
Rebuild == /\ phase = "idle" /\ consumer.bound /\ ~consumer.done /\ consumer.cursor < db.floor
 /\ consumer' = [consumer EXCEPT !.done = TRUE, !.cursor = db.e, !.index = Projection(db.w)]
 /\ rebuildProgress' = TRUE
 /\ UNCHANGED <<db,checkpoint,wal,phase,job,cold,capacity,bad,coldProgress>>
Next == Bind \/ (\E k \in Kinds,a \in {1,2},f \in BOOLEAN: Prepare(k,a,f))
 \/ (\E o \in {"ok","partial","uncertain"},d \in BOOLEAN: Write(o,d)) \/ Commit \/ Checkpoint \/ Repair
 \/ (\E keep \in BOOLEAN: Crash(keep)) \/ CatchUp \/ Rebuild
TypeOK == /\ db.e \in 0..MaxEpoch /\ db.floor \in 0..MaxEpoch+1 /\ checkpoint.e \in 0..MaxEpoch
 /\ phase \in {"idle","prepared","written","failed","recovery_failed"} /\ cold \in BOOLEAN /\ capacity \in BOOLEAN
 /\ Len(wal) <= MaxEpoch /\ consumer.cursor \in 0..MaxEpoch
 /\ bad \in BOOLEAN /\ coldProgress \in BOOLEAN /\ rebuildProgress \in BOOLEAN
NoStaleCatchUp == ~bad
NoUncommittedCapture == phase \in {"idle","failed"} =>
 /\ db.floor <= db.e /\ \A i \in 1..Len(db.events): db.events[i].epoch <= db.e
NoColdProgress == ~coldProgress
NoRebuildProgress == ~rebuildProgress
=============================================================================
