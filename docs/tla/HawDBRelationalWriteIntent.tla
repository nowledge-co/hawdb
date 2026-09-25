---------------- MODULE HawDBRelationalWriteIntent ----------------
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Transactions, Keys, DropNoopStamp, DropDeleteStamp
Values == {"absent", "original", "replacement"}
VARIABLES phase, base, key, value, epoch, data, stamps, history
vars == <<phase, base, key, value, epoch, data, stamps, history>>
Init ==
    /\ phase = [t \in Transactions |-> "idle"]
    /\ base = [t \in Transactions |-> 0]
    /\ key \in [Transactions -> Keys]
    /\ value \in [Transactions -> Values]
    /\ epoch = 0 /\ data \in [Keys -> Values]
    /\ stamps = [k \in Keys |-> 0] /\ history = <<>>
Begin(t) ==
    /\ phase[t] = "idle"
    /\ phase' = [phase EXCEPT ![t] = "active"]
    /\ base' = [base EXCEPT ![t] = epoch]
    /\ UNCHANGED <<key, value, epoch, data, stamps, history>>
IndexValid(t) == stamps[key[t]] <= base[t]
ReferenceValid(t) == \A i \in (base[t] + 1)..Len(history): history[i].key # key[t]
Commit(t) ==
    /\ phase[t] = "active" /\ IndexValid(t)
    /\ phase' = [phase EXCEPT ![t] = "done"]
    /\ epoch' = epoch + 1
    /\ data' = [data EXCEPT ![key[t]] = value[t]]
    /\ stamps' = IF (DropNoopStamp /\ data[key[t]] = value[t])
                      \/ (DropDeleteStamp /\ value[t] = "absent")
                   THEN stamps ELSE [stamps EXCEPT ![key[t]] = epoch']
    /\ history' = Append(history, [key |-> key[t], base |-> base[t], changed |-> data[key[t]] # value[t]])
    /\ UNCHANGED <<base, key, value>>
Reject(t) ==
    /\ phase[t] = "active" /\ ~IndexValid(t)
    /\ phase' = [phase EXCEPT ![t] = "rejected"]
    /\ UNCHANGED <<base, key, value, epoch, data, stamps, history>>
Next == \E t \in Transactions : Begin(t) \/ Commit(t) \/ Reject(t)
TypeInvariant ==
    /\ phase \in [Transactions -> {"idle", "active", "done", "rejected"}]
    /\ base \in [Transactions -> 0..Cardinality(Transactions)]
    /\ key \in [Transactions -> Keys] /\ value \in [Transactions -> Values]
    /\ epoch \in 0..Cardinality(Transactions) /\ data \in [Keys -> Values]
    /\ stamps \in [Keys -> 0..epoch]
    /\ history \in Seq([key: Keys, base: 0..epoch, changed: BOOLEAN])
    /\ Len(history) = epoch
ValidationMatchesIntentHistory == \A t \in Transactions : phase[t] = "active" => (IndexValid(t) = ReferenceValid(t))
FirstCommitterWins ==
    \A i \in 1..Len(history): \A j \in (history[i].base + 1)..(i - 1): history[i].key # history[j].key
NoNoopCommitWitness == \A i \in 1..Len(history): history[i].changed
=============================================================================
