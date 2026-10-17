---------------- MODULE HawDBRelationshipReadScope ----------------
EXTENDS Integers, FiniteSets
\* Each cell is a fixed physical relationship identity, grouped by immutable
\* type name. Cell 2 is outside the selected endpoint (cell 1). Absent
\* relationships have value -1; properties are abstracted by 0/1. A complete
\* native collector resolves old or same-transaction-created type metadata;
\* unknown identity or exhausted admission chooses fallback. No property
\* ownership, cold-index I/O bound, or C..E mapping is asserted by this model.
CONSTANTS MissingDomain, IgnoreDeletes, IgnoreProperties, IgnoreOffRoot, WrongType,
 IgnoreBarrier, GlobalEpochFallback, SharedWriterIntent, EarlyStamp,
 ReadLive, IgnoreSchema
VARIABLES edges, snapshot, scope, effect, epoch, summary, barrier, schema,
 status, active, wal, fallback, observed, checked, accepted
vars == <<edges,snapshot,scope,effect,epoch,summary,barrier,schema,
 status,active,wal,fallback,observed,checked,accepted>>
Kinds == {"source","other"}
Writers == {1,2}
Effects == {"create","property","delete"}
InitialEdges(operation) == [kind \in Kinds |->
 [key \in 1..3 |-> IF key=3 \/ (kind="source" /\ key=2 /\ operation="create")
                  THEN -1 ELSE 0]]
Init == /\ effect \in Effects /\ scope \in Kinds
 /\ edges=InitialEdges(effect) /\ snapshot=edges /\ epoch=0
 /\ summary=[kind \in Kinds |-> 0] /\ barrier=0 /\ schema=0
 /\ status=[writer \in Writers |-> "pending"]
 /\ active=0 /\ wal=FALSE /\ fallback=FALSE
 /\ observed=-2 /\ checked=FALSE /\ accepted=FALSE
Prepare(writer) == /\ ~checked /\ active=0 /\ status[writer]="pending"
 /\ ~(SharedWriterIntent /\ summary["source"]>0)
 /\ active'=writer /\ status'=[status EXCEPT ![writer]="staged"]
 /\ fallback' \in BOOLEAN /\ wal'=FALSE
 /\ summary'=(IF EarlyStamp THEN [summary EXCEPT !["source"]=epoch+1] ELSE summary)
 /\ UNCHANGED <<edges,snapshot,scope,effect,epoch,barrier,schema,observed,checked,accepted>>
Append == /\ active#0 /\ ~wal /\ wal'=TRUE
 /\ UNCHANGED <<edges,snapshot,scope,effect,epoch,summary,barrier,schema,
                status,active,fallback,observed,checked,accepted>>
FailWal == /\ active#0 /\ ~wal
 /\ status'=[status EXCEPT ![active]="failed"] /\ active'=0
 /\ UNCHANGED <<edges,snapshot,scope,effect,epoch,summary,barrier,schema,
                wal,fallback,observed,checked,accepted>>
AbortWriter(writer) == /\ ~checked /\ active=0 /\ status[writer]="pending"
 /\ SharedWriterIntent /\ summary["source"]>0
 /\ status'=[status EXCEPT ![writer]="aborted"]
 /\ UNCHANGED <<edges,snapshot,scope,effect,epoch,summary,barrier,schema,
                active,wal,fallback,observed,checked,accepted>>
ChangedEdges(writer) ==
 IF writer=1 THEN [edges EXCEPT !["source"][1]=1]
 ELSE IF effect="delete" THEN [edges EXCEPT !["source"][2]=-1]
 ELSE [edges EXCEPT !["source"][2]=1]
Publish == /\ active#0 /\ wal
 /\ edges'=ChangedEdges(active) /\ epoch'=epoch+1
 /\ (LET skip == MissingDomain \/ (IgnoreDeletes /\ active=2 /\ effect="delete")
                 \/ (IgnoreProperties /\ effect="property")
                 \/ (IgnoreOffRoot /\ active=2)
        key == IF WrongType THEN "other" ELSE "source"
    IN summary'=(IF fallback \/ skip THEN summary ELSE [summary EXCEPT ![key]=epoch+1]))
 /\ barrier'=(IF fallback THEN epoch+1 ELSE barrier)
 /\ status'=[status EXCEPT ![active]="committed"] /\ active'=0 /\ wal'=FALSE
 /\ UNCHANGED <<snapshot,scope,effect,schema,fallback,observed,checked,accepted>>
Ddl == /\ ~checked /\ active=0 /\ schema=0 /\ schema'=epoch+1 /\ epoch'=epoch+1
 /\ UNCHANGED <<edges,snapshot,scope,effect,summary,barrier,status,active,
                wal,fallback,observed,checked,accepted>>
Observe == /\ observed=-2
 /\ observed'=(IF ReadLive THEN edges[scope][1] ELSE snapshot[scope][1])
 /\ UNCHANGED <<edges,snapshot,scope,effect,epoch,summary,barrier,schema,
                status,active,wal,fallback,checked,accepted>>
Validate == /\ active=0 /\ ~checked
 /\ checked'=TRUE
 /\ accepted'=((IgnoreSchema \/ schema=0) /\ summary[scope]=0
              /\ (IgnoreBarrier \/ barrier=0) /\ (~GlobalEpochFallback \/ epoch=0))
 /\ UNCHANGED <<edges,snapshot,scope,effect,epoch,summary,barrier,schema,
                status,active,wal,fallback,observed>>
Next == (\E writer \in Writers: Prepare(writer) \/ AbortWriter(writer))
 \/ Append \/ FailWal \/ Publish \/ Ddl \/ Observe \/ Validate
TypeOK == /\ edges \in [Kinds -> [1..3 -> {-1,0,1}]]
 /\ scope \in Kinds /\ effect \in Effects /\ epoch \in 0..3
 /\ summary \in [Kinds -> 0..3] /\ barrier \in 0..3 /\ schema \in 0..3
 /\ status \in [Writers -> {"pending","staged","failed","committed","aborted"}]
 /\ active \in {0,1,2} /\ wal \in BOOLEAN /\ fallback \in BOOLEAN
 /\ observed \in {-2,-1,0,1} /\ checked \in BOOLEAN /\ accepted \in BOOLEAN
ScopeSafe == checked /\ accepted => edges[scope]=snapshot[scope] /\ schema=0
UnrelatedAccepted == checked /\ scope="other" /\ barrier=0 /\ schema=0 => accepted
NoFalseWriterConflict == \A writer \in Writers: status[writer]#"aborted"
NoEarlySummary == /\ \A kind \in Kinds: summary[kind]<=epoch
 /\ barrier<=epoch
PinnedObservation == observed#-2 => observed=snapshot[scope][1]
NoSuccessWitness == ~(checked /\ accepted /\ epoch>0)
NoBothWritersWitness == ~(\A writer \in Writers: status[writer]="committed")
=============================================================================
