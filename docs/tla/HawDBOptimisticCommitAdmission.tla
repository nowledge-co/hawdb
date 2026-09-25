---------------- MODULE HawDBOptimisticCommitAdmission ----------------
EXTENDS Naturals, Integers, Sequences, FiniteSets
CONSTANTS Transactions, Keys, MaxEpoch, AllowMixed, ReleaseEarly, SkipValidation, ValidateDurableOnly, IgnorePoison, AcknowledgeFailedSync, LoseAcknowledgedPrefix, ReorderRecovery
VARIABLES phase, base, key, intents, regular, batch, history, durable, crashed, batched, acknowledged, appended, poisoned, poisonEpoch, uncertain
vars == <<phase, base, key, intents, regular, batch, history, durable, crashed, batched, acknowledged, appended, poisoned, poisonEpoch, uncertain>>
Phases == {"idle", "active", "queued", "accepted", "rejected", "ready", "failedReady", "done", "conflict", "aborted", "uncertainReady", "uncertainDone"}
Protected == {"queued", "accepted", "rejected", "ready", "failedReady", "uncertainReady"}
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
    /\ acknowledged = {} /\ appended = <<>>
    /\ poisoned = FALSE /\ poisonEpoch = 0 /\ uncertain = {}
Begin(t, k) ==
    /\ ~crashed /\ (~poisoned \/ IgnorePoison) /\ phase[t] = "idle" /\ batch = {}
    /\ phase' = [phase EXCEPT ![t] = "active"]
    /\ base' = [base EXCEPT ![t] = durable]
    /\ key' = [key EXCEPT ![t] = k]
    /\ UNCHANGED <<acknowledged, appended, poisoned, poisonEpoch, uncertain, intents, regular, batch, history, durable, crashed, batched>>
Enqueue(t) ==
    /\ phase[t] = "active"
    /\ regular = "none" \/ (AllowMixed /\ regular = "shared")
    /\ phase' = [phase EXCEPT ![t] = "queued"]
    /\ intents' = intents \union {t}
    /\ UNCHANGED <<acknowledged, appended, poisoned, poisonEpoch, uncertain, base, key, regular, batch, history, durable, crashed, batched>>
AcquireRegular(mode) ==
    /\ ~crashed /\ regular = "none"
    /\ intents = {} \/ (AllowMixed /\ mode = "shared")
    /\ regular' = mode
    /\ UNCHANGED <<acknowledged, appended, poisoned, poisonEpoch, uncertain, phase, base, key, intents, batch, history, durable, crashed, batched>>
ReleaseRegular ==
    /\ regular # "none"
    /\ regular' = "none"
    /\ UNCHANGED <<acknowledged, appended, poisoned, poisonEpoch, uncertain, phase, base, key, intents, batch, history, durable, crashed, batched>>
RegularWrite(k) ==
    /\ ~crashed /\ (~poisoned \/ IgnorePoison)
    /\ regular = "exclusive" /\ batch = {} /\ Len(history) < MaxEpoch
    /\ history' = Append(history, [tx |-> "regular", base |-> Len(history), key |-> k])
    /\ durable' = Len(history')
    /\ appended' = history'
    /\ acknowledged' = acknowledged \union {Len(history')}
    /\ UNCHANGED <<poisoned, poisonEpoch, uncertain, phase, base, key, intents, regular, batch, crashed, batched>>
StartBatch(group) ==
    /\ ~crashed /\ (~poisoned \/ IgnorePoison)
    /\ batch = {} /\ group # {}
    /\ group \subseteq {t \in Transactions: phase[t] = "queued"}
    /\ Len(history) + Cardinality(group) <= MaxEpoch
    /\ batch' = group
    /\ UNCHANGED <<acknowledged, appended, poisoned, poisonEpoch, uncertain, phase, base, key, intents, regular, history, durable, crashed, batched>>
Valid(t) == \A i \in (base[t] + 1)..(IF ValidateDurableOnly THEN durable ELSE Len(history)): history[i].key # key[t]
Process(t) ==
    /\ ~crashed /\ (~poisoned \/ IgnorePoison)
    /\ t \in batch /\ phase[t] = "queued"
    /\ LET valid == SkipValidation \/ Valid(t) IN
       /\ phase' = [phase EXCEPT ![t] = IF valid THEN "accepted" ELSE "rejected"]
       /\ history' = IF valid THEN Append(history, [tx |-> t, base |-> base[t], key |-> key[t]]) ELSE history
    /\ appended' = history'
    /\ intents' = IF ReleaseEarly THEN intents \ {t} ELSE intents
    /\ UNCHANGED <<acknowledged, poisoned, poisonEpoch, uncertain, base, key, regular, batch, durable, crashed, batched>>
Sync ==
    /\ batch # {} /\ \A t \in batch: phase[t] \in {"accepted", "rejected"}
    /\ durable' = Len(history)
    /\ phase' = [t \in Transactions |-> IF t \in batch
                    THEN IF phase[t] = "accepted" THEN "ready" ELSE "failedReady" ELSE phase[t]]
    /\ batched' = (batched \/ Cardinality({t \in batch: phase[t] = "accepted"}) > 1)
    /\ batch' = {}
    /\ UNCHANGED <<acknowledged, appended, poisoned, poisonEpoch, uncertain, base, key, intents, regular, history, crashed>>
Retire(t) ==
    /\ phase[t] \in {"ready", "failedReady", "uncertainReady"}
    /\ phase' = [phase EXCEPT ![t] = IF @ = "ready" THEN "done" ELSE IF @ = "uncertainReady" THEN "uncertainDone" ELSE "conflict"]
    /\ acknowledged' = IF phase[t] = "ready"
          THEN acknowledged \union {i \in 1..Len(history): history[i].tx = t}
          ELSE acknowledged
    /\ intents' = intents \ {t}
    /\ UNCHANGED <<appended, poisoned, poisonEpoch, uncertain, base, key, regular, batch, history, durable, crashed, batched>>
Rollback(t) ==
    /\ phase[t] = "active"
    /\ phase' = [phase EXCEPT ![t] = "aborted"]
    /\ UNCHANGED <<acknowledged, appended, poisoned, poisonEpoch, uncertain, base, key, intents, regular, batch, history, durable, crashed, batched>>
FailSync ==
    /\ batch # {} /\ \A t \in batch: phase[t] \in {"accepted", "rejected"}
    /\ \E t \in batch: phase[t] = "accepted"
    /\ poisoned' = TRUE /\ poisonEpoch' = Len(history)
    /\ uncertain' = uncertain \union {t \in batch: phase[t] = "accepted"}
    /\ phase' = [t \in Transactions |-> IF t \in batch
          THEN IF phase[t] = "rejected" THEN "failedReady"
               ELSE IF AcknowledgeFailedSync THEN "ready" ELSE "uncertainReady"
          ELSE phase[t]]
    /\ batch' = {}
    /\ UNCHANGED <<base, key, intents, regular, history, durable, crashed, batched, acknowledged, appended>>
Crash ==
    /\ ~crashed
    \* Complete unacknowledged records may survive a failed sync. A torn
    \* frame instead fails closed in Rust; this action models readable prefixes.
    /\ \E cut \in (IF LoseAcknowledgedPrefix THEN 0 ELSE durable)..Len(history):
        /\ history' = IF ReorderRecovery THEN [i \in 1..cut |-> history[cut - i + 1]] ELSE SubSeq(history, 1, cut)
        /\ durable' = cut
    /\ phase' = [t \in Transactions |-> "aborted"]
    /\ intents' = {} /\ regular' = "none" /\ batch' = {} /\ crashed' = TRUE
    /\ UNCHANGED <<base, key, batched, acknowledged, appended, poisoned, poisonEpoch, uncertain>>
Next ==
    \/ \E t \in Transactions, k \in Keys: Begin(t, k)
    \/ \E t \in Transactions: Enqueue(t) \/ Process(t) \/ Retire(t) \/ Rollback(t)
    \/ \E mode \in {"shared", "exclusive"}: AcquireRegular(mode)
    \/ \E k \in Keys: RegularWrite(k)
    \/ \E group \in SUBSET Transactions: StartBatch(group)
    \/ ReleaseRegular \/ Sync \/ FailSync \/ Crash
TypeInvariant ==
    /\ phase \in [Transactions -> Phases] /\ base \in [Transactions -> 0..MaxEpoch]
    /\ key \in [Transactions -> Keys] /\ intents \subseteq Transactions /\ batch \subseteq Transactions
    /\ regular \in {"none", "shared", "exclusive"}
    /\ history \in Seq([tx: Transactions \union {"regular"}, base: 0..MaxEpoch, key: Keys])
    /\ Len(history) <= MaxEpoch /\ durable \in 0..Len(history)
    /\ crashed \in BOOLEAN /\ batched \in BOOLEAN
    /\ acknowledged \subseteq 1..MaxEpoch /\ uncertain \subseteq Transactions
    /\ appended \in Seq([tx: Transactions \union {"regular"}, base: 0..MaxEpoch, key: Keys])
    /\ Len(appended) <= MaxEpoch /\ poisoned \in BOOLEAN /\ poisonEpoch \in 0..MaxEpoch
NoMixedHolders == regular = "none" \/ intents = {}
ProtectedUntilRetirement == \A t \in Transactions: phase[t] \in Protected => t \in intents
FirstCommitterWins ==
    \A i \in 1..Len(history): \A j \in (history[i].base + 1)..(i - 1): history[i].key # history[j].key
AcknowledgedDurable == acknowledged \subseteq 1..durable
RecoveredSerialPrefix == history = SubSeq(appended, 1, Len(history))
PoisonBlocksPublication == (poisoned /\ ~crashed) => Len(history) = poisonEpoch
FailedSyncNeverAcknowledged == \A i \in acknowledged: appended[i].tx \notin uncertain
NoUncertainRecoveryWitness == ~(crashed /\ \E i \in 1..Len(history): history[i].tx \in uncertain)
NoPostCrashAcknowledgementWitness == ~(crashed /\ \E i \in acknowledged: appended[i].tx \in Transactions)
NoBatchWitness == ~batched
NoConflictWitness ==
    ~(\E t, u \in batch:
        /\ phase[t] = "accepted" /\ phase[u] = "rejected"
        /\ key[t] = key[u])
=============================================================================
