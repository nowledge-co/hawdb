---------------- MODULE HawDBOptimisticCommitAdmission ----------------
EXTENDS Naturals, Integers, Sequences, FiniteSets
CONSTANTS Transactions, Keys, MaxEpoch, AllowMixed, ReleaseEarly, SkipValidation, ValidateDurableOnly
VARIABLES phase, base, key, intents, regular, batch, history, durable, crashed, batched
vars == <<phase, base, key, intents, regular, batch, history, durable, crashed, batched>>
Phases == {"idle", "active", "queued", "accepted", "rejected", "ready", "failedReady", "done", "conflict", "aborted"}
Protected == {"queued", "accepted", "rejected", "ready", "failedReady"}
Init ==
    /\ phase = [t \in Transactions |-> "idle"]
    /\ base = [t \in Transactions |-> 0]
    /\ key \in [Transactions -> Keys]
    /\ intents = {}
    /\ regular = "none"
    /\ batch = {}
    /\ history = <<>>
    /\ durable = 0
    /\ crashed = FALSE
    /\ batched = FALSE
Begin(t, k) ==
    /\ ~crashed /\ phase[t] = "idle" /\ batch = {}
    /\ phase' = [phase EXCEPT ![t] = "active"]
    /\ base' = [base EXCEPT ![t] = durable]
    /\ key' = [key EXCEPT ![t] = k]
    /\ UNCHANGED <<intents, regular, batch, history, durable, crashed, batched>>
Enqueue(t) ==
    /\ phase[t] = "active"
    /\ regular = "none" \/ (AllowMixed /\ regular = "shared")
    /\ phase' = [phase EXCEPT ![t] = "queued"]
    /\ intents' = intents \union {t}
    /\ UNCHANGED <<base, key, regular, batch, history, durable, crashed, batched>>
AcquireRegular(mode) ==
    /\ ~crashed /\ regular = "none"
    /\ intents = {} \/ (AllowMixed /\ mode = "shared")
    /\ regular' = mode
    /\ UNCHANGED <<phase, base, key, intents, batch, history, durable, crashed, batched>>
ReleaseRegular ==
    /\ regular # "none"
    /\ regular' = "none"
    /\ UNCHANGED <<phase, base, key, intents, batch, history, durable, crashed, batched>>
RegularWrite(k) ==
    /\ regular = "exclusive" /\ batch = {} /\ Len(history) < MaxEpoch
    /\ history' = Append(history, [tx |-> "regular", base |-> Len(history), key |-> k])
    /\ durable' = Len(history')
    /\ UNCHANGED <<phase, base, key, intents, regular, batch, crashed, batched>>
StartBatch(group) ==
    /\ batch = {} /\ group # {}
    /\ group \subseteq {t \in Transactions: phase[t] = "queued"}
    /\ Len(history) + Cardinality(group) <= MaxEpoch
    /\ batch' = group
    /\ UNCHANGED <<phase, base, key, intents, regular, history, durable, crashed, batched>>
Valid(t) == \A i \in (base[t] + 1)..(IF ValidateDurableOnly THEN durable ELSE Len(history)): history[i].key # key[t]
Process(t) ==
    /\ t \in batch /\ phase[t] = "queued"
    /\ LET valid == SkipValidation \/ Valid(t) IN
       /\ phase' = [phase EXCEPT ![t] = IF valid THEN "accepted" ELSE "rejected"]
       /\ history' = IF valid THEN Append(history, [tx |-> t, base |-> base[t], key |-> key[t]]) ELSE history
    /\ intents' = IF ReleaseEarly THEN intents \ {t} ELSE intents
    /\ UNCHANGED <<base, key, regular, batch, durable, crashed, batched>>
Sync ==
    /\ batch # {} /\ \A t \in batch: phase[t] \in {"accepted", "rejected"}
    /\ durable' = Len(history)
    /\ phase' = [t \in Transactions |-> IF t \in batch
                    THEN IF phase[t] = "accepted" THEN "ready" ELSE "failedReady" ELSE phase[t]]
    /\ batched' = (batched \/ Cardinality({t \in batch: phase[t] = "accepted"}) > 1)
    /\ batch' = {}
    /\ UNCHANGED <<base, key, intents, regular, history, crashed>>
Retire(t) ==
    /\ phase[t] \in {"ready", "failedReady"}
    /\ phase' = [phase EXCEPT ![t] = IF @ = "ready" THEN "done" ELSE "conflict"]
    /\ intents' = intents \ {t}
    /\ UNCHANGED <<base, key, regular, batch, history, durable, crashed, batched>>
Rollback(t) ==
    /\ phase[t] = "active"
    /\ phase' = [phase EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<base, key, intents, regular, batch, history, durable, crashed, batched>>
Crash ==
    /\ ~crashed
    /\ history' = SubSeq(history, 1, durable)
    /\ phase' = [t \in Transactions |-> "aborted"]
    /\ intents' = {} /\ regular' = "none" /\ batch' = {} /\ crashed' = TRUE
    /\ UNCHANGED <<base, key, durable, batched>>
Next ==
    \/ \E t \in Transactions, k \in Keys: Begin(t, k)
    \/ \E t \in Transactions: Enqueue(t) \/ Process(t) \/ Retire(t) \/ Rollback(t)
    \/ \E mode \in {"shared", "exclusive"}: AcquireRegular(mode)
    \/ \E k \in Keys: RegularWrite(k)
    \/ \E group \in SUBSET Transactions: StartBatch(group)
    \/ ReleaseRegular \/ Sync \/ Crash
TypeInvariant ==
    /\ phase \in [Transactions -> Phases] /\ base \in [Transactions -> 0..MaxEpoch]
    /\ key \in [Transactions -> Keys] /\ intents \subseteq Transactions /\ batch \subseteq Transactions
    /\ regular \in {"none", "shared", "exclusive"}
    /\ history \in Seq([tx: Transactions \union {"regular"}, base: 0..MaxEpoch, key: Keys])
    /\ Len(history) <= MaxEpoch /\ durable \in 0..Len(history)
    /\ crashed \in BOOLEAN /\ batched \in BOOLEAN
NoMixedHolders == regular = "none" \/ intents = {}
ProtectedUntilRetirement == \A t \in Transactions: phase[t] \in Protected => t \in intents
FirstCommitterWins ==
    \A i \in 1..Len(history): \A j \in (history[i].base + 1)..(i - 1): history[i].key # history[j].key
AcknowledgedDurable ==
    \A t \in Transactions: phase[t] = "done" => \E i \in 1..durable: history[i].tx = t
NoBatchWitness == ~batched
NoConflictWitness ==
    ~(\E t, u \in batch:
        /\ phase[t] = "accepted" /\ phase[u] = "rejected"
        /\ key[t] = key[u])
=============================================================================
