---------------- MODULE HawDBRelationalTableReadScope ----------------
EXTENDS Integers, FiniteSets
CONSTANTS MissingDomain, IgnoreDeletes, IgnoreOffPage, WrongTable,
 IgnoreBarrier, GlobalEpochFallback, SharedWriterIntent, EarlyStamp,
 ReadLive, IgnoreSchema
VARIABLES rows, snapshot, scope, effect, epoch, summary, barrier, schema,
 status, active, wal, fallback, observed, checked, accepted
vars == <<rows,snapshot,scope,effect,epoch,summary,barrier,schema,
 status,active,wal,fallback,observed,checked,accepted>>
Tables == {"source","other"}
Writers == {1,2}
Effects == {"insert","payload","delete","rename"}
InitialRows(kind) == [table \in Tables |->
 [key \in 1..3 |-> IF key=3 \/ (table="source" /\ key=2 /\ kind="insert")
                  THEN -1 ELSE 0]]
Init == /\ effect \in Effects /\ scope \in Tables
 /\ rows=InitialRows(effect) /\ snapshot=rows /\ epoch=0
 /\ summary=[table \in Tables |-> 0] /\ barrier=0 /\ schema=0
 /\ status=[writer \in Writers |-> "pending"]
 /\ active=0 /\ wal=FALSE /\ fallback=FALSE
 /\ observed=-2 /\ checked=FALSE /\ accepted=FALSE
Prepare(writer) == /\ ~checked /\ active=0 /\ status[writer]="pending"
 /\ ~(SharedWriterIntent /\ summary["source"]>0)
 /\ active'=writer /\ status'=[status EXCEPT ![writer]="staged"]
 /\ fallback' \in BOOLEAN /\ wal'=FALSE
 /\ summary'=(IF EarlyStamp THEN [summary EXCEPT !["source"]=epoch+1] ELSE summary)
 /\ UNCHANGED <<rows,snapshot,scope,effect,epoch,barrier,schema,observed,checked,accepted>>
Append == /\ active#0 /\ ~wal /\ wal'=TRUE
 /\ UNCHANGED <<rows,snapshot,scope,effect,epoch,summary,barrier,schema,
                status,active,fallback,observed,checked,accepted>>
FailWal == /\ active#0 /\ ~wal
 /\ status'=[status EXCEPT ![active]="failed"] /\ active'=0
 /\ UNCHANGED <<rows,snapshot,scope,effect,epoch,summary,barrier,schema,
                wal,fallback,observed,checked,accepted>>
AbortWriter(writer) == /\ ~checked /\ active=0 /\ status[writer]="pending"
 /\ SharedWriterIntent /\ summary["source"]>0
 /\ status'=[status EXCEPT ![writer]="aborted"]
 /\ UNCHANGED <<rows,snapshot,scope,effect,epoch,summary,barrier,schema,
                active,wal,fallback,observed,checked,accepted>>
ChangedRows(writer) ==
 IF writer=1 THEN [rows EXCEPT !["source"][1]=1]
 ELSE IF effect="delete" THEN [rows EXCEPT !["source"][2]=-1]
 ELSE IF effect="rename" THEN [rows EXCEPT !["source"][2]=-1, !["source"][3]=0]
 ELSE [rows EXCEPT !["source"][2]=1]
Publish == /\ active#0 /\ wal
 /\ rows'=ChangedRows(active) /\ epoch'=epoch+1
 /\ (LET skip == MissingDomain \/ (IgnoreDeletes /\ active=2 /\ effect="delete")
                 \/ (IgnoreOffPage /\ active=2)
        key == IF WrongTable THEN "other" ELSE "source"
    IN summary'=(IF fallback \/ skip THEN summary ELSE [summary EXCEPT ![key]=epoch+1]))
 /\ barrier'=(IF fallback THEN epoch+1 ELSE barrier)
 /\ status'=[status EXCEPT ![active]="committed"] /\ active'=0 /\ wal'=FALSE
 /\ UNCHANGED <<snapshot,scope,effect,schema,fallback,observed,checked,accepted>>
Ddl == /\ ~checked /\ active=0 /\ schema=0 /\ schema'=epoch+1 /\ epoch'=epoch+1
 /\ UNCHANGED <<rows,snapshot,scope,effect,summary,barrier,status,active,
                wal,fallback,observed,checked,accepted>>
Observe == /\ observed=-2
 /\ observed'=(IF ReadLive THEN rows[scope][1] ELSE snapshot[scope][1])
 /\ UNCHANGED <<rows,snapshot,scope,effect,epoch,summary,barrier,schema,
                status,active,wal,fallback,checked,accepted>>
Validate == /\ active=0 /\ ~checked
 /\ checked'=TRUE
 /\ accepted'=((IgnoreSchema \/ schema=0) /\ summary[scope]=0
              /\ (IgnoreBarrier \/ barrier=0) /\ (~GlobalEpochFallback \/ epoch=0))
 /\ UNCHANGED <<rows,snapshot,scope,effect,epoch,summary,barrier,schema,
                status,active,wal,fallback,observed>>
Next == (\E writer \in Writers: Prepare(writer) \/ AbortWriter(writer))
 \/ Append \/ FailWal \/ Publish \/ Ddl \/ Observe \/ Validate
TypeOK == /\ rows \in [Tables -> [1..3 -> {-1,0,1}]]
 /\ scope \in Tables /\ effect \in Effects /\ epoch \in 0..3
 /\ summary \in [Tables -> 0..3] /\ barrier \in 0..3 /\ schema \in 0..3
 /\ status \in [Writers -> {"pending","staged","failed","committed","aborted"}]
 /\ active \in {0,1,2} /\ wal \in BOOLEAN /\ fallback \in BOOLEAN
 /\ observed \in {-2,-1,0,1} /\ checked \in BOOLEAN /\ accepted \in BOOLEAN
ScopeSafe == checked /\ accepted => rows[scope]=snapshot[scope] /\ schema=0
UnrelatedAccepted == checked /\ scope="other" /\ barrier=0 /\ schema=0 => accepted
NoFalseWriterConflict == \A writer \in Writers: status[writer]#"aborted"
NoEarlySummary == /\ \A table \in Tables: summary[table]<=epoch
 /\ barrier<=epoch
PinnedObservation == observed#-2 => observed=snapshot[scope][1]
NoSuccessWitness == ~(checked /\ accepted /\ epoch>0)
NoBothWritersWitness == ~(\A writer \in Writers: status[writer]="committed")
=============================================================================
