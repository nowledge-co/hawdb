------------ MODULE HawDBRelationalProjectionReadValidation ------------
EXTENDS Integers, FiniteSets
CONSTANTS MaxEpoch, NarrowPartialKey, ExtractThroughOr, DropAbsent,
          LateTracking, SkipPayloadStamp, IgnoreBroadBarrier, IgnoreSchema,
          PrunePinned, ForgetRefusal
VARIABLES db, job, staleAccepted, disjointAccepted
vars == <<db, job, staleAccepted, disjointAccepted>>
DataKeys == {<<"a", 1>>, <<"a", 2>>, <<"b", 1>>, <<"b", 2>>}
A == <<"a", 1>>
B == <<"b", 1>>
DbKey == <<"database", 0>>
SchemaKey == <<"schema", 0>>
Keys == DataKeys \cup {DbKey, SchemaKey}
Queries == {"point", "second", "residual", "partial", "or", "null"}
InitialRows == [k \in DataKeys |-> CASE k = A -> 0 [] k = B -> 1 [] OTHER -> -1]
Zero == [k \in Keys |-> 0]
Open == job.phase \in {"captured", "read"}
OracleKeys(query) == CASE query \in {"point", "residual"} -> {A}
                         [] query = "second" -> {B}
                         [] query = "partial" -> {k \in DataKeys: k[1] = "a"}
                         [] query = "null" -> {}
                         [] OTHER -> DataKeys
Selected(rows, query) == {k \in DataKeys:
    rows[k] # -1 /\ CASE query = "point" -> k = A
                      [] query = "second" -> k = B
                      [] query = "residual" -> k = A /\ rows[k] = 1
                      [] query = "partial" -> k[1] = "a"
                      [] query = "or" -> k = A \/ rows[k] = 1
                      [] OTHER -> FALSE}
Output(rows, query) == {<<k, rows[k]>>: k \in Selected(rows, query)}
Chosen(query) == CASE query \in {"point", "residual"} -> {A}
                     [] query = "second" -> {B}
                     [] query = "partial" /\ NarrowPartialKey -> {A}
                     [] query = "or" /\ ExtractThroughOr -> {A}
                     [] OTHER -> {DbKey}
Footprint(query) == IF DropAbsent THEN
    {k \in Chosen(query): k = DbKey \/ job.rows[k] # -1} ELSE Chosen(query)
IndexValid == /\ ~job.refused
    /\ IF DbKey \in job.keys THEN db.epoch = job.epoch ELSE
          \A k \in job.keys \cup (IF IgnoreBroadBarrier THEN {} ELSE {DbKey})
                          \cup (IF IgnoreSchema THEN {} ELSE {SchemaKey}):
              db.stamps[k] <= job.epoch
OracleValid ==
    /\ db.oracle[SchemaKey] <= job.epoch /\ db.oracle[DbKey] <= job.epoch
    /\ \A query \in job.queries:
        /\ Output(job.rows, query) = Output(db.rows, query)
        /\ \A k \in OracleKeys(query): db.oracle[k] <= job.epoch

Init ==
    /\ db = [epoch |-> 0, rows |-> InitialRows, stamps |-> Zero, oracle |-> Zero]
    /\ job = [phase |-> "idle", epoch |-> 0, rows |-> InitialRows,
               queries |-> {}, keys |-> {}, refused |-> FALSE]
    /\ staleAccepted = FALSE /\ disjointAccepted = FALSE
Capture ==
    /\ job.phase = "idle"
    /\ job' = [job EXCEPT !.phase = "captured", !.epoch = db.epoch, !.rows = db.rows]
    /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
Read(query) ==
    /\ Open /\ query \in Queries \ job.queries /\ Cardinality(job.queries) < 2
    /\ LET keys == job.keys \cup Footprint(query)
           over == Cardinality(keys) > 1
       IN job' = [job EXCEPT !.phase = "read", !.queries = @ \cup {query},
                  !.keys = IF over THEN @ ELSE keys,
                  !.refused = (@ \/ (over /\ ~ForgetRefusal))]
    /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
Write(key, payload, broad) ==
    /\ db.epoch < MaxEpoch /\ key \in DataKeys /\ payload \in {-1, 0, 1}
    /\ broad \in BOOLEAN
    /\ LET next == db.epoch + 1
           oracleKeys == {key} \cup (IF broad THEN {DbKey} ELSE {})
           skipped == (LateTracking /\ job.phase = "captured") \/
                      (SkipPayloadStamp /\ db.rows[key] # -1 /\ payload # -1)
           optimized == IF broad THEN {DbKey} ELSE IF skipped THEN {} ELSE {key}
       IN db' = [db EXCEPT !.epoch = next, !.rows[key] = payload,
                  !.oracle = [k \in Keys |-> IF k \in oracleKeys THEN next ELSE db.oracle[k]],
                  !.stamps = [k \in Keys |-> IF k \in optimized THEN next ELSE db.stamps[k]]]
    /\ UNCHANGED <<job, staleAccepted, disjointAccepted>>
SchemaChange ==
    /\ db.epoch < MaxEpoch
    /\ db' = [db EXCEPT !.epoch = @ + 1, !.stamps[SchemaKey] = db.epoch + 1,
                        !.oracle[SchemaKey] = db.epoch + 1]
    /\ UNCHANGED <<job, staleAccepted, disjointAccepted>>
Prune(key) ==
    /\ key \in Keys /\ db.stamps[key] # 0
    /\ ~Open \/ db.stamps[key] <= job.epoch \/ PrunePinned
    /\ db' = [db EXCEPT !.stamps[key] = 0]
    /\ UNCHANGED <<job, staleAccepted, disjointAccepted>>
Accept ==
    /\ job.phase = "read" /\ IndexValid
    /\ job' = [job EXCEPT !.phase = "done"]
    /\ staleAccepted' = (staleAccepted \/ ~OracleValid)
    /\ disjointAccepted' = (disjointAccepted \/
         (job.queries = {"point"} /\ db.epoch > job.epoch /\ OracleValid))
    /\ UNCHANGED db
Reject ==
    /\ job.phase = "read" /\ ~IndexValid
    /\ job' = [job EXCEPT !.phase = "rejected"]
    /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
Next == Capture \/ (\E q \in Queries: Read(q))
        \/ (\E k \in DataKeys, p \in {-1, 0, 1}, b \in BOOLEAN: Write(k, p, b))
        \/ SchemaChange \/ (\E k \in Keys: Prune(k)) \/ Accept \/ Reject
TypeOK ==
    /\ db.epoch \in 0..MaxEpoch /\ db.rows \in [DataKeys -> {-1, 0, 1}]
    /\ db.stamps \in [Keys -> 0..MaxEpoch] /\ db.oracle \in [Keys -> 0..MaxEpoch]
    /\ job.phase \in {"idle", "captured", "read", "done", "rejected"}
    /\ job.epoch \in 0..MaxEpoch /\ job.rows \in [DataKeys -> {-1, 0, 1}]
    /\ job.queries \subseteq Queries /\ job.keys \subseteq Keys /\ job.refused \in BOOLEAN
    /\ staleAccepted \in BOOLEAN /\ disjointAccepted \in BOOLEAN
NoStaleAcceptance == ~staleAccepted
NoDisjointAcceptance == ~disjointAccepted
=============================================================================
