---------------- MODULE HawDBScopedGraphRead ----------------
EXTENDS Integers, FiniteSets
CONSTANTS SkipRoot, SkipTarget, SkipKind, WrongName, WildcardAsNamed,
 IgnoreRefusal, IgnoreBarrier, IgnoreSchema, GlobalEpochFallback
VARIABLES shape, held, priorRefusal, refused, data, stamps, barrier,
 schema, epoch, checked, accepted
vars == <<shape,held,priorRefusal,refused,data,stamps,barrier,schema,
 epoch,checked,accepted>>
Nodes == {"Memory","Label","OtherNode"}
Relationships == {"HAS_LABEL","OtherEdge"}
Sources == Nodes \cup Relationships
Names == Sources \cup {"NodeAll","RelAll"}
Choices == {{"Memory","Label","HAS_LABEL"},{"Label","HAS_LABEL"},
 {"Memory","HAS_LABEL"},{"Memory","Label"},{"OtherNode","Label","HAS_LABEL"},
 {"NodeAll","RelAll"},{"NodeAll","HAS_LABEL"},{"Memory","RelAll"},
 {"Memory","Label","RelAll"},{}}
Shapes == [root: {"Memory",""}, target: {"Label",""}, kind: {"HAS_LABEL",""}]
Required(s) == (IF s.root="" THEN Nodes ELSE {s.root})
 \cup (IF s.target="" THEN Nodes ELSE {s.target})
 \cup (IF s.kind="" THEN Relationships ELSE {s.kind})
CoversNode(label,h) == "NodeAll" \in h \/ (label#"" /\ label \in h)
CoversKind(kind,h) == "RelAll" \in h \/ (kind#"" /\ kind \in h)
Correct(s,h) == CoversNode(s.root,h) /\ CoversNode(s.target,h) /\ CoversKind(s.kind,h)
WrongNode(label,h) == CoversNode(label,h)
 \/ (WrongName /\ h \cap Nodes#{})
 \/ (WildcardAsNamed /\ label="" /\ h \cap Nodes#{})
WrongKind(kind,h) == CoversKind(kind,h)
 \/ (WildcardAsNamed /\ kind="" /\ h \cap Relationships#{})
Admitted(s,h) == (SkipRoot \/ WrongNode(s.root,h))
 /\ (SkipTarget \/ WrongNode(s.target,h)) /\ (SkipKind \/ WrongKind(s.kind,h))
Init == /\ shape \in Shapes /\ held \in Choices /\ priorRefusal \in BOOLEAN
 /\ refused=(priorRefusal \/ ~Admitted(shape,held))
 /\ data=[source \in Sources |-> 0] /\ stamps=[name \in Names |-> 0]
 /\ barrier=0 /\ schema=0 /\ epoch=0 /\ checked=FALSE /\ accepted=FALSE
Affected(source) == {source} \cup
 (IF source \in Nodes THEN {"NodeAll"} ELSE {"RelAll"})
Write(source) == /\ ~checked /\ epoch<2 /\ data[source]=0
 /\ data'=[data EXCEPT ![source]=1] /\ epoch'=epoch+1
 /\ (\E fallback \in BOOLEAN:
      /\ stamps'=(IF fallback THEN stamps ELSE
           [name \in Names |-> IF name \in Affected(source) THEN epoch+1 ELSE stamps[name]])
      /\ barrier'=(IF fallback THEN epoch+1 ELSE barrier))
 /\ UNCHANGED <<shape,held,priorRefusal,refused,schema,checked,accepted>>
Ddl == /\ ~checked /\ epoch<3 /\ schema=0
 /\ epoch'=epoch+1 /\ schema'=epoch+1
 /\ UNCHANGED <<shape,held,priorRefusal,refused,data,stamps,barrier,checked,accepted>>
Validate == /\ ~checked /\ checked'=TRUE
 /\ accepted'=((IgnoreRefusal \/ ~refused)
      /\ (\A key \in held: stamps[key]=0)
      /\ (IgnoreBarrier \/ barrier=0) /\ (IgnoreSchema \/ schema=0)
      /\ (~GlobalEpochFallback \/ epoch=0))
 /\ UNCHANGED <<shape,held,priorRefusal,refused,data,stamps,barrier,schema,epoch>>
Next == (\E source \in Sources: Write(source)) \/ Ddl \/ Validate
TypeOK == /\ shape \in Shapes /\ held \in Choices
 /\ priorRefusal \in BOOLEAN /\ refused \in BOOLEAN
 /\ data \in [Sources -> {0,1}] /\ stamps \in [Names -> 0..3]
 /\ barrier \in 0..3 /\ schema \in 0..3 /\ epoch \in 0..3
 /\ checked \in BOOLEAN /\ accepted \in BOOLEAN
ScopeSafe == checked /\ accepted => (\A source \in Required(shape): data[source]=0) /\ schema=0
RefusalSticky == checked /\ accepted => ~priorRefusal
UnrelatedAccepted == checked /\ ~refused /\ barrier=0 /\ schema=0
 /\ (\A key \in held: stamps[key]=0) => accepted
NoSuccessWitness == ~(checked /\ accepted /\ epoch>0)
=============================================================================
