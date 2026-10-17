---------------- MODULE HawDBSearchUnorderedMutations ----------------
EXTENDS Integers, Sequences, FiniteSets, TLC
CONSTANTS DropOnFlush, DigestEquality, IgnoreConflict, EarlyPrepare,
          RefundMerge, IgnoreFailure, PublishStale
VARIABLES collision, accepted, buffer, runs, ordered, cursor, seen, reduced,
          phase, failedEver, charged, actual, cap, base, stalePublished, completed
vars == <<collision,accepted,buffer,runs,ordered,cursor,seen,reduced,
          phase,failedEver,charged,actual,cap,base,stalePublished,completed>>
Inputs == 1..5
Keys == {"a","b"}
Key(i) == IF i <= 3 THEN "a" ELSE "b"
Upsert(i) == i \in {2,3}
Body(i) == IF i = 3 /\ collision THEN 1 ELSE 0
\* Different bodies deliberately have the same digest and length.
Digest(i) == 0
RECURSIVE Ordered(_)
Ordered(s) == IF s = {} THEN <<>> ELSE
 LET first == CHOOSE i \in s: \A j \in s:
                (IF Key(i) = "a" THEN 0 ELSE 1) < (IF Key(j) = "a" THEN 0 ELSE 1) \/ (Key(i) = Key(j) /\ i <= j)
 IN <<first>> \o Ordered(s \ {first})
Held == buffer \cup UNION runs
RunItems == UNION runs
Cost(s) == Cardinality(s)
Init == /\ collision \in BOOLEAN /\ accepted = {} /\ buffer = {} /\ runs = {}
 /\ ordered = <<>> /\ cursor = 1 /\ seen = {}
 /\ reduced = [key \in Keys |-> -1] /\ phase = "ingest" /\ failedEver = FALSE
 /\ charged = 0 /\ actual = 0 /\ cap \in {6,40} /\ base = 0
 /\ stalePublished = FALSE /\ completed = FALSE
Accept(i) == /\ phase = "ingest" /\ i \in Inputs \ accepted /\ Cardinality(buffer) < 2
 /\ accepted' = accepted \cup {i} /\ buffer' = buffer \cup {i}
 /\ UNCHANGED <<collision,runs,ordered,cursor,seen,reduced,phase,failedEver,
                charged,actual,cap,base,stalePublished,completed>>
Flush == /\ phase = "ingest" /\ buffer # {}
 /\ charged + Cost(buffer) <= cap
 /\ LET written == IF DropOnFlush THEN buffer \ {CHOOSE i \in buffer: TRUE} ELSE buffer
    IN runs' = runs \cup {written}
 /\ buffer' = {} /\ charged' = charged + Cost(buffer) /\ actual' = actual + Cost(buffer)
 /\ UNCHANGED <<collision,accepted,ordered,cursor,seen,reduced,phase,failedEver,
                cap,base,stalePublished,completed>>
Merge(a,b) == /\ phase = "ingest" /\ a \in runs /\ b \in runs /\ a # b
 /\ charged + 2*Cost(a \cup b) <= cap
 /\ runs' = (runs \ {a,b}) \cup {a \cup b}
 /\ charged' = charged + (IF RefundMerge THEN 0 ELSE 2*Cost(a \cup b))
 /\ actual' = actual + 2*Cost(a \cup b)
 /\ UNCHANGED <<collision,accepted,buffer,ordered,cursor,seen,reduced,phase,failedEver,
                cap,base,stalePublished,completed>>
Prepare == /\ phase = "ingest" /\ buffer = {} /\ Cardinality(runs) = 1
 /\ (EarlyPrepare \/ accepted = Inputs)
 /\ ordered' = Ordered(RunItems) /\ phase' = "reduce"
 /\ UNCHANGED <<collision,accepted,buffer,runs,cursor,seen,reduced,failedEver,
                charged,actual,cap,base,stalePublished,completed>>
Reduce == /\ phase = "reduce" /\ cursor <= Len(ordered)
 /\ LET i == ordered[cursor]
        previous == {j \in seen: Key(j) = Key(i) /\ Upsert(j)}
        conflict == Upsert(i) /\ \E j \in previous:
           Body(i) # Body(j) /\ ~(DigestEquality /\ Digest(i) = Digest(j))
        refused == conflict /\ ~IgnoreConflict
    IN /\ phase' = (IF refused THEN "failed" ELSE phase)
       /\ failedEver' = (failedEver \/ refused)
       /\ cursor' = (IF refused THEN cursor ELSE cursor+1)
       /\ seen' = (IF refused THEN seen ELSE seen \cup {i})
       /\ reduced' = (IF refused \/ ~Upsert(i) THEN reduced
                      ELSE [reduced EXCEPT ![Key(i)] = Body(i)])
 /\ UNCHANGED <<collision,accepted,buffer,runs,ordered,charged,actual,cap,base,stalePublished,completed>>
Finish == /\ phase = "reduce" /\ cursor = Len(ordered)+1 /\ phase' = "prepared"
 /\ UNCHANGED <<collision,accepted,buffer,runs,ordered,cursor,seen,reduced,failedEver,
                charged,actual,cap,base,stalePublished,completed>>
Publish == /\ phase = "prepared" /\ (base = 0 \/ PublishStale)
 /\ phase' = "published" /\ stalePublished' = (base # 0) /\ completed' = TRUE
 /\ UNCHANGED <<collision,accepted,buffer,runs,ordered,cursor,seen,reduced,failedEver,
                charged,actual,cap,base>>
ReplaceBase == /\ base = 0 /\ phase # "published" /\ base' = 1
 /\ UNCHANGED <<collision,accepted,buffer,runs,ordered,cursor,seen,reduced,phase,failedEver,
                charged,actual,cap,stalePublished,completed>>
Fail == /\ phase \notin {"failed","published"} /\ phase' = "failed" /\ failedEver' = TRUE
 /\ UNCHANGED <<collision,accepted,buffer,runs,ordered,cursor,seen,reduced,charged,actual,
                cap,base,stalePublished,completed>>
Revive == /\ IgnoreFailure /\ phase = "failed" /\ phase' = "ingest"
 /\ UNCHANGED <<collision,accepted,buffer,runs,ordered,cursor,seen,reduced,failedEver,
                charged,actual,cap,base,stalePublished,completed>>
Next == (\E i \in Inputs: Accept(i)) \/ Flush \/ (\E a,b \in runs: Merge(a,b))
 \/ Prepare \/ Reduce \/ Finish \/ Publish \/ ReplaceBase \/ Fail \/ Revive
TypeOK == /\ accepted \subseteq Inputs /\ buffer \subseteq Inputs
 /\ runs \subseteq SUBSET Inputs /\ seen \subseteq Inputs /\ charged \in Nat /\ actual \in Nat
 /\ phase \in {"ingest","reduce","prepared","published","failed"}
NoLostInput == Held = accepted
NoDoubleOwnership == /\ buffer \cap RunItems = {}
 /\ \A a,b \in runs: a # b => a \cap b = {}
NoEarlyPreparation == phase \in {"reduce","prepared","published"} => accepted = Inputs
NoIncompleteCandidate == phase \in {"prepared","published"} =>
 /\ seen = Inputs /\ ~collision /\ reduced = [key \in Keys |-> IF key = "a" THEN 0 ELSE -1]
NoIgnoredFailure == failedEver => phase = "failed"
NoBudgetRefund == charged = actual /\ charged <= cap
NoStalePublication == ~stalePublished
NoCompletionWitness == ~completed
=============================================================================
