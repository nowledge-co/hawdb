---------------- MODULE HawDBIndexedProjectionReadValidation ----------------
EXTENDS Integers, FiniteSets

CONSTANTS MaxEpoch, LateTracking, SkipOldKey, SkipNewKey,
          SkipPayloadInvalidation, IgnoreReadBarrier, TrustUnindexedPlan,
          NarrowDisjunction, NarrowBoundedPhase, PrunePinned
VARIABLES db, job, staleAccepted, disjointAccepted
vars == <<db, job, staleAccepted, disjointAccepted>>
Slots == {1, 2}
Small == {"a", "b"}
Values == Small \cup {"large", "absent"}
Keys == Small \cup {"domain", "fallback"}
Zero == [k \in Keys |-> 0]
InitialRows == [s \in Slots |-> [key |-> IF s = 1 THEN "a" ELSE "absent", payload |-> 0]]
Open == job.phase \in {"captured", "read"}
ReadKeys(query) == CASE query = "point" -> {"a"}
                    [] query = "multi" -> Small
                    [] OTHER -> {"domain"}
ChosenKeys(query) ==
    IF query \in {"point", "multi"} /\ (db.indexed \/ TrustUnindexedPlan)
       THEN ReadKeys(query)
       ELSE IF (query = "or" /\ NarrowDisjunction) \/ (query = "bounded" /\ NarrowBoundedPhase) THEN {"a"} ELSE {"domain"}
Matches(row, query) ==
    /\ row.key # "absent"
    /\ CASE query \in {"point", "bounded"} -> row.key = "a"
         [] query = "multi" -> row.key \in Small
         [] query = "or" -> row.key = "a" \/ row.payload = 1
         [] OTHER -> TRUE
Output(rows, query) ==
    LET live == {s \in Slots: rows[s].key # "absent"}
        top == {s \in live: \A t \in live:
                  rows[s].payload < rows[t].payload \/
                  (rows[s].payload = rows[t].payload /\ s <= t)}
        candidates == IF query = "bounded" THEN top ELSE live
    IN {<<s, rows[s].key, rows[s].payload>> : s \in {t \in candidates: Matches(rows[t], query)}}
IndexValid ==
    \A k \in (job.keys \cup (IF IgnoreReadBarrier THEN {} ELSE {"fallback"})):
        db.stamps[k] <= job.epoch
(* Full history and row contents do not consult the optimized prunable index. *)
OracleValid ==
    /\ \A k \in (ReadKeys(job.query) \cup {"fallback"}): db.oracle[k] <= job.epoch
    /\ Output(job.rows, job.query) = Output(db.rows, job.query)

Init ==
    /\ db \in {[epoch |-> 0, indexed |-> i, rows |-> InitialRows, stamps |-> Zero,
                 oracle |-> Zero] : i \in BOOLEAN}
    /\ job = [phase |-> "idle", epoch |-> 0, rows |-> InitialRows,
               query |-> "point", keys |-> {}]
    /\ staleAccepted = FALSE /\ disjointAccepted = FALSE

Capture ==
    /\ job.phase = "idle"
    /\ job' = [job EXCEPT !.phase = "captured", !.epoch = db.epoch, !.rows = db.rows]
    /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
Read(query) ==
    /\ job.phase = "captured" /\ query \in {"point", "multi", "or", "domain", "bounded"}
    /\ job' = [job EXCEPT !.phase = "read", !.query = query, !.keys = ChosenKeys(query)]
    /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>

Write(slot, key, payload, unknown) ==
    /\ db.epoch < MaxEpoch /\ slot \in Slots /\ key \in Values /\ payload \in {0, 1}
    /\ unknown \in BOOLEAN
    /\ db.rows[slot] # [key |-> key, payload |-> payload]
    /\ db.rows[slot].key # "absent" \/ key # "absent"
    /\ LET next == db.epoch + 1
           old == db.rows[slot].key
           before == IF old \in Small THEN {old} ELSE {}
           after == IF key \in Small THEN {key} ELSE {}
           reference == before \cup after \cup {"domain"} \cup (IF unknown THEN {"fallback"} ELSE {})
           pointKeys == IF ~db.indexed \/ unknown \/ (SkipPayloadInvalidation /\ old = key)
                           THEN {}
                           ELSE (IF SkipOldKey THEN {} ELSE before) \cup
                                (IF SkipNewKey THEN {} ELSE after)
           tracked == Open /\ (~LateTracking \/ job.phase = "read")
           optimized == IF tracked
                           THEN pointKeys \cup {"domain"} \cup (IF unknown THEN {"fallback"} ELSE {})
                           ELSE {}
       IN db' = [db EXCEPT !.epoch = next,
                  !.rows[slot] = [key |-> key, payload |-> payload],
                  !.oracle = [k \in Keys |-> IF k \in reference THEN next ELSE db.oracle[k]],
                  !.stamps = [k \in Keys |-> IF k \in optimized THEN next ELSE db.stamps[k]]]
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
          (db.indexed /\ job.query = "point" /\ db.epoch > job.epoch /\ OracleValid))
    /\ UNCHANGED db
Reject ==
    /\ job.phase = "read" /\ ~IndexValid
    /\ job' = [job EXCEPT !.phase = "rejected"]
    /\ UNCHANGED <<db, staleAccepted, disjointAccepted>>
Next == Capture \/ (\E q \in {"point", "multi", "or", "domain", "bounded"}: Read(q))
        \/ (\E s \in Slots, k \in Values, p \in {0, 1}, u \in BOOLEAN: Write(s, k, p, u))
        \/ (\E k \in Keys: Prune(k)) \/ Accept \/ Reject
TypeOK ==
    /\ db.epoch \in 0..MaxEpoch /\ db.indexed \in BOOLEAN
    /\ db.rows \in [Slots -> [key: Values, payload: {0, 1}]]
    /\ db.stamps \in [Keys -> 0..MaxEpoch] /\ db.oracle \in [Keys -> 0..MaxEpoch]
    /\ job.phase \in {"idle", "captured", "read", "done", "rejected"}
    /\ job.epoch \in 0..MaxEpoch /\ job.rows \in [Slots -> [key: Values, payload: {0, 1}]]
    /\ job.keys \subseteq Keys /\ job.query \in {"point", "multi", "or", "domain", "bounded"}
    /\ {staleAccepted, disjointAccepted} \subseteq BOOLEAN
NoStaleAcceptance == ~staleAccepted
NoDisjointAcceptance == ~disjointAccepted
=============================================================================
