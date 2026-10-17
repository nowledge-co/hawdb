---------------- MODULE HawDBProjectionLabelLookup ----------------
EXTENDS Integers, FiniteSets
CONSTANTS IgnoreAbsentGuard, IgnoreSchemaBarrier, WrongDomain, GlobalEpochFallback
VARIABLES labels, epoch, schemaStamp, domains, name, observed, operation, phase, accepted
vars == <<labels,epoch,schemaStamp,domains,name,observed,operation,phase,accepted>>
Names == {"Memory","Later"}
None == "absent"
Value(label,set) == IF label \in set THEN label ELSE None
Init == /\ labels={"Memory"} /\ epoch=0 /\ schemaStamp=0
 /\ domains=[label \in Names |-> 0] /\ name \in Names
 /\ observed=Value(name,labels) /\ operation="none" /\ phase="captured" /\ accepted=FALSE
ImplicitCreate == /\ phase="captured"
 /\ labels'=labels \cup {"Later"} /\ epoch'=1 /\ domains'=[domains EXCEPT !["Later"]=1]
 /\ operation'="implicit" /\ phase'="validate"
 /\ UNCHANGED <<schemaStamp,name,observed,accepted>>
PropertyWrite == /\ phase="captured"
 /\ epoch'=1 /\ domains'=[domains EXCEPT !["Memory"]=1]
 /\ operation'="property" /\ phase'="validate"
 /\ UNCHANGED <<labels,schemaStamp,name,observed,accepted>>
ExplicitDdl == /\ phase="captured"
 /\ labels'=labels \cup {"Later"} /\ epoch'=1 /\ schemaStamp'=1
 /\ operation'="ddl" /\ phase'="validate"
 /\ UNCHANGED <<domains,name,observed,accepted>>
Validate == /\ phase="validate"
 /\ LET key == IF WrongDomain THEN "Memory" ELSE name
        schemaOK == IgnoreSchemaBarrier \/ schemaStamp=0
        absenceOK == observed#None \/ IgnoreAbsentGuard \/ domains[key]=0
    IN accepted'=(schemaOK /\ absenceOK /\ (~GlobalEpochFallback \/ epoch=0))
 /\ phase'="done"
 /\ UNCHANGED <<labels,epoch,schemaStamp,domains,name,observed,operation>>
Next == ImplicitCreate \/ PropertyWrite \/ ExplicitDdl \/ Validate
TypeOK == labels \subseteq Names /\ name \in Names /\ epoch \in {0,1}
 /\ schemaStamp \in {0,1} /\ domains \in [Names -> {0,1}]
 /\ phase \in {"captured","validate","done"} /\ accepted \in BOOLEAN
ValidObservation == accepted => observed=Value(name,labels)
NoFalsePropertyRefusal == phase="done" /\ operation="property" => accepted
NoSuccessWitness == ~accepted
=============================================================================
