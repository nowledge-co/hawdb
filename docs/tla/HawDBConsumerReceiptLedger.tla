---------------- MODULE HawDBConsumerReceiptLedger ----------------
EXTENDS Integers, FiniteSets
CONSTANTS ForgetPriorReceipt, EvictOnFull, GcBeforeRetireSync,
 ReuseRetiredId, ForgetAllocatorOnCrash, ForgetRetireFloor

(* Proposed bounded ledger within ONE fixed native consumer incarnation.
   Reservation and receipt installation each abstract a successful durability
   barrier. Atomic selector replacement is assumed; byte-level storage faults,
   source mapping and consumer incarnation checks are separate obligations. *)
VARIABLE s
vars == <<s>>
Operations == 1..3
Capacity == 2
Root(h, f, r) == [head |-> h, retired |-> f, receipts |-> r]
JobActive == s.phase \in {"reserved", "installed", "selected"}
Init == s=[phase |-> "idle", allocated |-> 0, everAllocated |-> {},
 job |-> 0, root |-> Root(0,0,{}), durableRoot |-> Root(0,0,{}),
 rootSynced |-> TRUE, files |-> {}, committed |-> {}, delivered |-> {},
 lostResponses |-> {}, crashed |-> FALSE, reused |-> FALSE,
 query |-> [id |-> 0, result |-> "none", returned |-> 0, head |-> 0, floor |-> 0]]

Begin == /\ s.phase="idle" /\ s.allocated<3
 /\ (Cardinality(s.root.receipts)<Capacity \/ EvictOnFull)
 /\ LET next == IF ReuseRetiredId /\ s.root.retired>0 THEN 1 ELSE s.allocated+1
    IN s'=[s EXCEPT !.phase="reserved", !.job=next, !.allocated=@+1,
       !.everAllocated=@ \cup {next}, !.reused=next \in s.everAllocated,
       !.root.receipts=IF EvictOnFull /\ Cardinality(@)>=Capacity
                      THEN {s.root.head} ELSE @]
InstallReceipt == /\ s.phase="reserved"
 /\ s'=[s EXCEPT !.phase="installed", !.files=@ \cup {s.job}]
Abort == /\ s.phase \in {"reserved","installed"}
 /\ s'=[s EXCEPT !.phase="idle", !.job=0]
Select == /\ s.phase="installed"
 /\ s'=[s EXCEPT !.phase="selected", !.rootSynced=FALSE,
       !.root=Root(s.job,s.root.retired,
          IF ForgetPriorReceipt THEN {s.job} ELSE s.root.receipts \cup {s.job})]
SyncRoot == /\ s.phase \in {"selected","retiring"} /\ ~s.rootSynced
 /\ s'=[s EXCEPT !.rootSynced=TRUE, !.durableRoot=s.root,
       !.committed=IF s.phase="selected" THEN @ \cup {s.job} ELSE @]
Respond(delivered) == /\ s.phase="selected" /\ s.rootSynced
 /\ s'=[s EXCEPT !.phase="idle", !.job=0,
       !.delivered=IF delivered THEN @ \cup {s.job} ELSE @,
       !.lostResponses=IF delivered THEN @ ELSE @ \cup {s.job}]
Retire(floor) == /\ s.phase="idle" /\ s.rootSynced
 /\ floor \in 1..2 /\ floor>s.root.retired /\ floor<s.root.head
 /\ {op \in s.committed: op<=floor} \subseteq s.delivered
 /\ s'=[s EXCEPT !.phase="retiring", !.rootSynced=FALSE,
       !.root=Root(s.root.head,floor,{op \in s.root.receipts: op>floor})]
FinishRetire == /\ s.phase="retiring" /\ s.rootSynced
 /\ s'=[s EXCEPT !.phase="idle"]
Cleanup ==
 /\ LET protected == s.root.receipts
            \cup (IF GcBeforeRetireSync THEN {} ELSE s.durableRoot.receipts)
            \cup (IF JobActive THEN {s.job} ELSE {})
    IN s'=[s EXCEPT !.files=@ \cap protected]
Crash(keepRoot) == /\ ~s.crashed
 /\ LET persisted == IF s.rootSynced \/ keepRoot THEN s.root ELSE s.durableRoot
        recovered == IF ForgetRetireFloor
                     THEN Root(persisted.head,0,persisted.receipts) ELSE persisted
    IN s'=[s EXCEPT !.phase="crashed", !.crashed=TRUE, !.job=0,
       !.root=recovered, !.durableRoot=recovered, !.rootSynced=TRUE,
       !.allocated=IF ForgetAllocatorOnCrash THEN persisted.head ELSE @,
       !.committed=@ \cup persisted.receipts]
Reopen == /\ s.phase="crashed" /\ s'=[s EXCEPT !.phase="idle"]
Lookup(op) == /\ s.phase="idle" /\ op \in 1..s.allocated
 /\ LET result == IF op<=s.root.retired THEN "expired"
                  ELSE IF op \in s.root.receipts THEN "committed" ELSE "not_committed"
    IN s'=[s EXCEPT !.query=[id |-> op, result |-> result,
         returned |-> IF result="committed" THEN op ELSE 0,
         head |-> s.root.head, floor |-> s.root.retired],
       !.delivered=IF result="committed" THEN @ \cup {op} ELSE @]
Next == Begin \/ InstallReceipt \/ Abort \/ Select \/ SyncRoot
 \/ (\E delivered \in BOOLEAN: Respond(delivered))
 \/ (\E floor \in 1..2: Retire(floor)) \/ FinishRetire \/ Cleanup
 \/ (\E keepRoot \in BOOLEAN: Crash(keepRoot)) \/ Reopen
 \/ (\E op \in Operations: Lookup(op))

TypeOK ==
 /\ s.phase \in {"idle","reserved","installed","selected","retiring","crashed"}
 /\ s.allocated \in 0..3 /\ s.job \in 0..3
 /\ s.root.head \in 0..3 /\ s.durableRoot.head \in 0..3
 /\ s.root.retired \in 0..2 /\ s.durableRoot.retired \in 0..2
 /\ \A key \in {"everAllocated","files","committed","delivered","lostResponses"}:
       s[key] \subseteq Operations
 /\ s.root.receipts \subseteq Operations /\ s.durableRoot.receipts \subseteq Operations
 /\ s.rootSynced \in BOOLEAN /\ s.crashed \in BOOLEAN /\ s.reused \in BOOLEAN
 /\ s.query.id \in 0..3 /\ s.query.returned \in 0..3
 /\ s.query.head \in 0..3 /\ s.query.floor \in 0..2
 /\ s.query.result \in {"none","committed","expired","not_committed"}
NoLostReceipt == \A op \in s.committed:
 op>s.root.retired => op \in s.root.receipts
DurableReceiptsRetained == s.durableRoot.receipts \subseteq s.files
SelectedReceiptsRetained == s.root.receipts \subseteq s.files
Bounded == Cardinality(s.root.receipts)<=Capacity
NoAllocationRollback == \A op \in s.everAllocated: op<=s.allocated
NoReexecution == ~s.reused
LookupSound ==
 /\ s.query.result="committed" =>
       s.query.id \in s.committed /\ s.query.returned=s.query.id
 /\ s.query.result="not_committed" => s.query.id \notin s.committed
 /\ s.query.result="expired" => s.query.id<=s.query.floor
NoLateRetryWitness == ~(s.query.id=1 /\ s.query.result="committed"
 /\ s.query.head>=2 /\ 1 \in s.lostResponses)
NoRetireAndContinueWitness == ~(s.root.head=3 /\ s.root.retired=1
 /\ s.query.id=1 /\ s.query.result="expired" /\ s.root.receipts={2,3})
=============================================================================
