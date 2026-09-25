------------------ MODULE HawDBConstrainedInsertIntent ------------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Transactions, PrimaryKeys, UniqueValues,
          SkipUniqueStamp, StampNull, StampForeignReaders
Null == "null"
Absent == "absent"
Database == <<"database", "all">>
ASSUME /\ Transactions # {} /\ PrimaryKeys # {} /\ UniqueValues # {}
       /\ UniqueValues \intersect {Null, Absent} = {}
       /\ {SkipUniqueStamp, StampNull, StampForeignReaders} \subseteq BOOLEAN
UniqueKeys == {<<"unique", u>>: u \in UniqueValues \union {Null}}
Identities == {<<"row", p>>: p \in PrimaryKeys} \union UniqueKeys
              \union {Database, <<"foreign", "parent">>}
VARIABLES phase, base, kind, primary, unique, epoch, stamps, history, rows, parent
vars == <<phase, base, kind, primary, unique, epoch, stamps, history, rows, parent>>
Record(t) == [base |-> base[t], kind |-> kind[t], primary |-> primary[t], unique |-> unique[t]]
(* Independent oracle: business uniqueness, not optimized index identities.
   Reading the same parent never overlaps two pure inserts. *)
Overlap(a,b) == a.kind = "delete_parent" \/ b.kind = "delete_parent"
               \/ a.primary = b.primary \/ (a.unique # Null /\ a.unique = b.unique)
Keys(t) == IF kind[t] = "delete_parent" THEN {Database}
           ELSE {<<"row", primary[t]>>}
                \union (IF unique[t] # Null \/ StampNull THEN {<<"unique", unique[t]>>} ELSE {})
                \union (IF StampForeignReaders THEN {<<"foreign", "parent">>} ELSE {})
Stamped(t) == IF SkipUniqueStamp THEN Keys(t) \ UniqueKeys ELSE Keys(t)
IndexValid(t) == /\ \A k \in Keys(t): stamps[k] <= base[t]
                 /\ stamps[Database] <= base[t]
                 /\ kind[t] = "delete_parent" => epoch <= base[t]
ReferenceValid(t) == \A i \in (base[t]+1)..Len(history): ~Overlap(Record(t),history[i])
InsertValid(t) == /\ parent /\ rows[primary[t]] = Absent
                  /\ unique[t] = Null \/ (\A p \in PrimaryKeys: rows[p] # unique[t])
Init ==
    /\ phase = [t \in Transactions |-> "idle"] /\ base = [t \in Transactions |-> 0]
    /\ kind \in [Transactions -> {"insert", "delete_parent"}]
    /\ primary \in [Transactions -> PrimaryKeys]
    /\ unique \in [Transactions -> UniqueValues \union {Null}]
    /\ epoch = 0 /\ stamps = [k \in Identities |-> 0] /\ history = <<>>
    /\ rows = [p \in PrimaryKeys |-> Absent] /\ parent = TRUE
Begin(t) ==
    /\ phase[t] = "idle" /\ parent
    /\ kind[t] = "insert" => InsertValid(t)
    /\ phase' = [phase EXCEPT ![t] = "active"] /\ base' = [base EXCEPT ![t] = epoch]
    /\ UNCHANGED <<kind, primary, unique, epoch, stamps, history, rows, parent>>
Commit(t) ==
    /\ phase[t] = "active" /\ IndexValid(t)
    /\ kind[t] = "insert" => InsertValid(t)
    /\ phase' = [phase EXCEPT ![t] = "done"] /\ epoch' = epoch + 1
    /\ stamps' = [k \in Identities |-> IF k \in Stamped(t) THEN epoch + 1 ELSE stamps[k]]
    /\ history' = Append(history, Record(t))
    /\ rows' = IF kind[t] = "insert" THEN [rows EXCEPT ![primary[t]] = unique[t]]
                 ELSE [p \in PrimaryKeys |-> Absent]
    /\ parent' = IF kind[t] = "delete_parent" THEN FALSE ELSE parent
    /\ UNCHANGED <<base, kind, primary, unique>>
Reject(t) ==
    /\ phase[t] = "active" /\ ~IndexValid(t)
    /\ phase' = [phase EXCEPT ![t] = "rejected"]
    /\ UNCHANGED <<base, kind, primary, unique, epoch, stamps, history, rows, parent>>
Next == \E t \in Transactions: Begin(t) \/ Commit(t) \/ Reject(t)
TypeInvariant ==
    /\ phase \in [Transactions -> {"idle", "active", "done", "rejected"}]
    /\ base \in [Transactions -> 0..epoch]
    /\ kind \in [Transactions -> {"insert", "delete_parent"}]
    /\ primary \in [Transactions -> PrimaryKeys]
    /\ unique \in [Transactions -> UniqueValues \union {Null}]
    /\ epoch = Len(history) /\ epoch <= Cardinality(Transactions)
    /\ stamps \in [Identities -> 0..epoch]
    /\ rows \in [PrimaryKeys -> UniqueValues \union {Null, Absent}] /\ parent \in BOOLEAN
ValidationMatchesHistory == \A t \in Transactions: phase[t] = "active" => (IndexValid(t) <=> ReferenceValid(t))
ValidationPreservesConstraints == \A t \in Transactions:
    phase[t] = "active" /\ kind[t] = "insert" /\ IndexValid(t) => InsertValid(t)
UniqueRows == \A a,b \in PrimaryKeys: a # b /\ rows[a] \notin {Null, Absent} => rows[a] # rows[b]
ForeignKeysValid == ~parent => \A p \in PrimaryKeys: rows[p] = Absent
FirstCommitterWins == \A i \in 1..Len(history):
    \A j \in (history[i].base+1)..(i-1): ~Overlap(history[i],history[j])
NoDisjointWitness == ~(Len(history) = 2 /\ \A i \in 1..2: history[i].kind = "insert" /\ history[i].base = 0)
NoNullWitness == ~(Len(history) = 2 /\ \A i \in 1..2: history[i].kind = "insert" /\ history[i].unique = Null)
NoCascadeWitness == ~(Len(history) = 2 /\ history[1].kind = "insert" /\ history[2].kind = "delete_parent")
Spec == Init /\ [][Next]_vars
=============================================================================
