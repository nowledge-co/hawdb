---------------- MODULE HawDBProjectionEndpointCapture ----------------
EXTENDS Integers, FiniteSets
CONSTANTS TrustV1, SkipProperty, SkipOtherType, DropEndpoint, DropOwnerOnly,
 IgnoreRefusal, EarlyPublish, ForgetTrimFloor, IgnoreMissingRows
VARIABLES head, floor, retained, records, good, wal, cp, refused,
 effect, kind, snapshot
vars == <<head,floor,retained,records,good,wal,cp,refused,effect,kind,snapshot>>
Epochs == 1..3
Owners == {1,2}
Empty == [e \in Epochs |-> {}]
State(h,f,r,p) == [head |-> h,floor |-> f,records |-> r,retained |-> p]
Coverage(s) == \A start \in s.floor..s.head:
 \A e \in (start+1)..s.head: e \in s.retained /\ s.records[e]=Owners
Produced == IF (SkipProperty /\ effect="property") \/ (SkipOtherType /\ kind="OTHER")
 THEN {} ELSE IF DropEndpoint THEN {1} ELSE Owners
OwnerOnly == effect="property" \/ kind="OTHER"
Init == /\ effect \in {"create","delete","property"} /\ kind \in {"HAS_LABEL","OTHER"}
 /\ head=1 /\ floor=(IF TrustV1 THEN 0 ELSE 1)
 /\ retained={1} /\ records=[Empty EXCEPT ![1]={1}]
 /\ good={} /\ wal={} /\ refused=FALSE
 /\ cp=[head |-> 1,floor |-> 0,retained |-> {1},records |-> records,
          declared |-> [e \in Epochs |-> Cardinality(records[e])],version |-> 1]
 /\ snapshot=State(1,1,records,retained)
Commit(admitted) == /\ ~refused /\ head<3 /\ admitted \in BOOLEAN
 /\ head'=head+1 /\ wal'=wal \cup {head+1}
 /\ good'=(IF admitted THEN good \cup {head+1} ELSE good)
 /\ retained'=(IF ~admitted \/ (DropOwnerOnly /\ OwnerOnly)
                 THEN retained ELSE retained \cup {head+1})
 /\ records'=(IF admitted THEN [records EXCEPT ![head+1]=Produced] ELSE records)
 /\ floor'=(IF ~admitted /\ ~IgnoreRefusal THEN head+1 ELSE floor)
 /\ UNCHANGED <<cp,refused,effect,kind,snapshot>>
FailedWal == /\ ~refused /\ head<3
 /\ retained'=(IF EarlyPublish THEN retained \cup {head+1} ELSE retained)
 /\ records'=(IF EarlyPublish THEN [records EXCEPT ![head+1]=Owners] ELSE records)
 /\ UNCHANGED <<head,floor,good,wal,cp,refused,effect,kind,snapshot>>
Trim == /\ ~refused /\ retained#{}
 /\ \E e \in retained:
      /\ \A prior \in retained: e<=prior
      /\ retained'=retained\{e}
      /\ floor'=(IF ForgetTrimFloor THEN floor ELSE IF e>floor THEN e ELSE floor)
 /\ UNCHANGED <<head,records,good,wal,cp,refused,effect,kind,snapshot>>
Capture == /\ ~refused /\ snapshot'=State(head,floor,records,retained)
 /\ UNCHANGED <<head,floor,retained,records,good,wal,cp,refused,effect,kind>>
Checkpoint == /\ ~refused
 /\ cp'=[head |-> head,floor |-> floor,retained |-> {e \in retained:e>floor},
          records |-> records,declared |-> [e \in Epochs |-> Cardinality(records[e])],version |-> 2]
 /\ wal'={}
 /\ UNCHANGED <<head,floor,retained,records,good,refused,effect,kind,snapshot>>
Damage == /\ cp.version=2 /\ cp.retained#{}
 /\ \E e \in cp.retained:
      /\ cp.records[e]#{}
      /\ \E owner \in cp.records[e]: cp'=[cp EXCEPT !.records[e]=@\{owner}]
 /\ UNCHANGED <<head,floor,retained,records,good,wal,refused,effect,kind,snapshot>>
Recover ==
 /\ LET valid == \A e \in cp.retained: Cardinality(cp.records[e])=cp.declared[e]
        qualified == cp.version=2 \/ TrustV1
        baseFloor == IF qualified THEN cp.floor ELSE cp.head
        tail == {e \in wal:e>cp.head}
        gaps == tail\good
        resume == {e \in {baseFloor} \cup gaps: \A other \in {baseFloor} \cup gaps: other<=e}
    IN /\ refused'=(~valid /\ ~IgnoreMissingRows)
       /\ head'=(IF tail={} THEN cp.head ELSE CHOOSE e \in tail:\A other \in tail:other<=e)
       /\ floor'=CHOOSE e \in resume: TRUE
       /\ retained'=(IF qualified THEN cp.retained ELSE {}) \cup (tail \cap good)
       /\ records'=[e \in Epochs |-> IF e \in tail /\ e \in good THEN Produced
                        ELSE IF qualified THEN cp.records[e] ELSE {}]
 /\ UNCHANGED <<good,wal,cp,effect,kind,snapshot>>
Next == (\E admitted \in BOOLEAN:Commit(admitted)) \/ FailedWal \/ Trim \/ Capture \/ Checkpoint \/ Damage \/ Recover
TypeOK == /\ head \in 1..3 /\ floor \in 0..3 /\ retained \subseteq Epochs
 /\ records \in [Epochs -> SUBSET Owners] /\ good \subseteq Epochs /\ wal \subseteq Epochs
 /\ refused \in BOOLEAN /\ effect \in {"create","delete","property"} /\ kind \in {"HAS_LABEL","OTHER"}
NoUncommittedFacts == ~refused => retained \subseteq 1..head /\ floor<=head
CompleteInterval == ~refused => Coverage(State(head,floor,records,retained))
CapturedInterval == Coverage(snapshot)
NoUpgradeWitness == ~(~refused /\ cp.version=2 /\ head>1 /\ floor=1 /\ 2 \in retained)
=============================================================================
