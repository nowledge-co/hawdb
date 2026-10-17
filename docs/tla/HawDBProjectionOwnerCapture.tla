---------------- MODULE HawDBProjectionOwnerCapture ----------------
EXTENDS Integers, FiniteSets
CONSTANTS TrustLegacyFloor, DropOwnerOnly, ForgetTrimFloor, PublishBeforeWal, IgnoreMissingRows
VARIABLES head, changed, records, floor, cp, wal, refused, snapshot
vars == <<head, changed, records, floor, cp, wal, refused, snapshot>>
MaxEpoch == 3
Epochs == 1..MaxEpoch
State(h,c,r,f) == [head |-> h,changed |-> c,records |-> r,floor |-> f]
Init ==
 /\ head=1 /\ changed={1} /\ records={}
 /\ floor=(IF TrustLegacyFloor THEN 0 ELSE 1)
 /\ cp=[head |-> 1, changed |-> {1}, records |-> {}, declared |-> {}, floor |-> 0, version |-> FALSE]
 /\ wal={} /\ refused=FALSE
 /\ snapshot=State(head,changed,records,floor)
Commit(ownerOnly) ==
 /\ ~refused /\ head<MaxEpoch /\ ownerOnly \in BOOLEAN
 /\ head'=head+1 /\ changed'=changed \cup {head+1}
 /\ records'=(IF ownerOnly /\ DropOwnerOnly THEN records ELSE records \cup {head+1})
 /\ wal'=wal \cup {head+1}
 /\ UNCHANGED <<floor,cp,refused,snapshot>>
FailedWal ==
 /\ ~refused /\ head<MaxEpoch
 /\ records'=(IF PublishBeforeWal THEN records \cup {head+1} ELSE records)
 /\ UNCHANGED <<head,changed,floor,cp,wal,refused,snapshot>>
Trim ==
 /\ ~refused /\ records#{}
 /\ \E e \in records:
     /\ \A prior \in records: e<=prior
     /\ records'=records\{e}
     /\ floor'=(IF ForgetTrimFloor THEN floor ELSE IF e>floor THEN e ELSE floor)
 /\ UNCHANGED <<head,changed,cp,wal,refused,snapshot>>
Capture ==
 /\ ~refused /\ snapshot'=State(head,changed,records,floor)
 /\ UNCHANGED <<head,changed,records,floor,cp,wal,refused>>
Checkpoint ==
 /\ ~refused
 /\ cp'=[head |-> head,changed |-> changed,records |-> {e \in records:e>floor},
          declared |-> {e \in records:e>floor},floor |-> floor,version |-> TRUE]
 /\ wal'={}
 /\ UNCHANGED <<head,changed,records,floor,refused,snapshot>>
Damage ==
 /\ cp.version /\ cp.records#{}
 /\ \E e \in cp.records: cp'=[cp EXCEPT !.records=@\{e}]
 /\ UNCHANGED <<head,changed,records,floor,wal,refused,snapshot>>
Recover ==
 /\ LET valid == ~cp.version \/ cp.records=cp.declared
        f == IF cp.version THEN cp.floor ELSE IF TrustLegacyFloor THEN cp.floor ELSE cp.head
        tail == {e \in wal:e>cp.head}
    IN /\ refused'=(~valid /\ ~IgnoreMissingRows)
       /\ head'=(IF tail={} THEN cp.head ELSE CHOOSE e \in tail:\A other \in tail:other<=e)
       /\ changed'=cp.changed \cup tail
       /\ records'=cp.records \cup tail
       /\ floor'=f
 /\ UNCHANGED <<cp,wal,snapshot>>
Next == (\E ownerOnly \in BOOLEAN:Commit(ownerOnly)) \/ FailedWal \/ Trim \/ Capture \/ Checkpoint \/ Damage \/ Recover
TypeOK == head \in 1..MaxEpoch /\ floor \in 0..MaxEpoch /\ changed \subseteq Epochs
 /\ records \subseteq Epochs /\ wal \subseteq Epochs /\ refused \in BOOLEAN
 /\ snapshot.head \in 1..MaxEpoch /\ snapshot.records \subseteq Epochs
NoUncommittedFacts == ~refused => records \subseteq 1..head /\ floor<=head
Coverage(s) == \A start \in s.floor..s.head: {e \in s.changed:e>start} \subseteq s.records
CompleteInterval == ~refused => Coverage(State(head,changed,records,floor))
CapturedInterval == Coverage(snapshot)
NoUpgradeWitness == ~(~refused /\ cp.version /\ head>1 /\ floor=1 /\ records#{})
Spec == Init /\ [][Next]_vars
=============================================================================
