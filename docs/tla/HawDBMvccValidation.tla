----------------------- MODULE HawDBMvccValidation -----------------------
EXTENDS Integers, Naturals, Sequences, FiniteSets

CONSTANTS Transactions, Keys, MaxEpoch,
          SkipValidation, SkipBroadWriterCheck, SkipBarrierStampCheck, PrunePinned,
          ResetWithActiveTransactions, PublishBeforeSync, IgnoreSourcePin

Barriers == {"database", "schema"}
Identities == Keys \union Barriers
WriteSets == ((SUBSET Keys) \ {{}}) \union {{"database"}, {"schema"}}
Phases == {"idle", "active", "prepared", "durable", "done", "rejected"}
ASSUME /\ Keys # {}
       /\ Keys \intersect Barriers = {}
       /\ Transactions # {}
       /\ "none" \notin Transactions
       /\ MaxEpoch \in Nat \ {0}
       /\ {SkipValidation, SkipBroadWriterCheck, SkipBarrierStampCheck, PrunePinned,
             ResetWithActiveTransactions, PublishBeforeSync, IgnoreSourcePin} \subseteq BOOLEAN

VARIABLES phase, readEpoch, writes, deleting, snapshot,
          history, visible, stamps, tombstones, owner, restarted, sourceEpoch
vars == <<phase, readEpoch, writes, deleting, snapshot,
          history, visible, stamps, tombstones, owner, restarted, sourceEpoch>>

EmptyStamps == [k \in Identities |-> 0]
Record(tx) == [base |-> readEpoch[tx], keys |-> writes[tx], delete |-> deleting[tx]]
Prefix(n) == SubSeq(history, 1, n)
Broad(w) == w \intersect Barriers # {}
Overlap(a, b) == a \intersect b # {} \/ Broad(a) \/ Broad(b)

(* Independent reference: retain the full durable history, not the index. *)
ReferenceValid(tx) ==
    \A i \in (readEpoch[tx] + 1)..Len(history):
        ~Overlap(writes[tx], history[i].keys)

IndexValid(tx) ==
    /\ \A k \in writes[tx]: stamps[k] <= readEpoch[tx]
    /\ \A b \in Barriers:
        /\ (SkipBarrierStampCheck \/ stamps[b] <= readEpoch[tx])
        /\ SkipBroadWriterCheck \/ b \notin writes[tx] \/ visible <= readEpoch[tx]

Pinned == {tx \in Transactions : phase[tx] \in {"active", "prepared", "durable"}}
CanPrune(k) ==
    /\ \A tx \in Pinned: stamps[k] < readEpoch[tx]
    /\ IgnoreSourcePin \/ sourceEpoch = -1 \/ stamps[k] < sourceEpoch

Init ==
    /\ phase = [tx \in Transactions |-> "idle"]
    /\ readEpoch = [tx \in Transactions |-> 0]
    /\ writes = [tx \in Transactions |-> {}]
    /\ deleting = [tx \in Transactions |-> FALSE]
    /\ snapshot = [tx \in Transactions |-> <<>>]
    /\ history = <<>>
    /\ visible = 0
    /\ stamps = EmptyStamps
    /\ tombstones = {}
    /\ owner = "none"
    /\ restarted = FALSE
    /\ sourceEpoch = -1

BeginAt(tx, w, d, epoch) ==
    /\ owner = "none"
    /\ phase[tx] = "idle"
    /\ visible < MaxEpoch
    /\ w \in WriteSets
    /\ d \in BOOLEAN
    /\ d => ~Broad(w)
    /\ phase' = [phase EXCEPT ![tx] = "active"]
    /\ readEpoch' = [readEpoch EXCEPT ![tx] = epoch]
    /\ writes' = [writes EXCEPT ![tx] = w]
    /\ deleting' = [deleting EXCEPT ![tx] = d]
    /\ snapshot' = [snapshot EXCEPT ![tx] = Prefix(epoch)]
    /\ UNCHANGED <<history, visible, stamps, tombstones, owner, restarted, sourceEpoch>>

Begin(tx, w, d) == BeginAt(tx, w, d, visible)
BeginFromSource(tx, w, d) == sourceEpoch >= 0 /\ BeginAt(tx, w, d, sourceEpoch)

CaptureSource ==
    /\ owner = "none"
    /\ sourceEpoch = -1
    /\ sourceEpoch' = visible
    /\ UNCHANGED <<phase, readEpoch, writes, deleting, snapshot, history,
                    visible, stamps, tombstones, owner, restarted>>

DropSource ==
    /\ sourceEpoch >= 0
    /\ sourceEpoch' = -1
    /\ UNCHANGED <<phase, readEpoch, writes, deleting, snapshot, history,
                    visible, stamps, tombstones, owner, restarted>>

Prepare(tx) ==
    /\ owner = "none"
    /\ phase[tx] = "active"
    /\ Len(history) < MaxEpoch
    /\ SkipValidation \/ IndexValid(tx)
    /\ phase' = [phase EXCEPT ![tx] = "prepared"]
    /\ owner' = tx
    /\ UNCHANGED <<readEpoch, writes, deleting, snapshot, history, visible,
                    stamps, tombstones, restarted, sourceEpoch>>

Reject(tx) ==
    /\ owner = "none"
    /\ phase[tx] = "active"
    /\ ~IndexValid(tx)
    /\ phase' = [phase EXCEPT ![tx] = "rejected"]
    /\ UNCHANGED <<readEpoch, writes, deleting, snapshot, history, visible,
                    stamps, tombstones, owner, restarted, sourceEpoch>>

Sync(tx) ==
    /\ owner = tx
    /\ phase[tx] = "prepared"
    /\ history' = Append(history, Record(tx))
    /\ phase' = [phase EXCEPT ![tx] = "durable"]
    /\ UNCHANGED <<readEpoch, writes, deleting, snapshot, visible,
                    stamps, tombstones, owner, restarted, sourceEpoch>>

Publish(tx) ==
    /\ owner = tx
    /\ phase[tx] = "durable" \/ (PublishBeforeSync /\ phase[tx] = "prepared")
    /\ visible' = visible + 1
    /\ stamps' = [k \in Identities |->
                     IF k \in writes[tx] THEN visible + 1 ELSE stamps[k]]
    /\ tombstones' = IF deleting[tx]
                       THEN tombstones \union writes[tx]
                       ELSE tombstones \ writes[tx]
    /\ phase' = [phase EXCEPT ![tx] = "done"]
    /\ owner' = "none"
    /\ UNCHANGED <<readEpoch, writes, deleting, snapshot, history, restarted, sourceEpoch>>

Rollback(tx) ==
    /\ phase[tx] = "active"
    /\ phase' = [phase EXCEPT ![tx] = "done"]
    /\ UNCHANGED <<readEpoch, writes, deleting, snapshot, history, visible,
                    stamps, tombstones, owner, restarted, sourceEpoch>>

Prune(k) ==
    /\ owner = "none"
    /\ k \in tombstones
    /\ PrunePinned \/ CanPrune(k)
    /\ stamps' = [stamps EXCEPT ![k] = 0]
    /\ tombstones' = tombstones \ {k}
    /\ UNCHANGED <<phase, readEpoch, writes, deleting, snapshot,
                    history, visible, owner, restarted, sourceEpoch>>

(* No transaction or snapshot survives a process restart. Canonical replay
   restores the full durable prefix; stamp history is process-local. *)
Crash ==
    /\ ~restarted
    /\ restarted' = TRUE
    /\ sourceEpoch' = -1
    /\ visible' = Len(history)
    /\ stamps' = EmptyStamps
    /\ tombstones' = {}
    /\ owner' = "none"
    /\ phase' = [tx \in Transactions |-> "idle"]
    /\ readEpoch' = [tx \in Transactions |-> 0]
    /\ writes' = [tx \in Transactions |-> {}]
    /\ deleting' = [tx \in Transactions |-> FALSE]
    /\ snapshot' = [tx \in Transactions |-> <<>>]
    /\ UNCHANGED history

BadReset ==
    /\ ResetWithActiveTransactions
    /\ owner = "none"
    /\ stamps # EmptyStamps
    /\ stamps' = EmptyStamps
    /\ tombstones' = {}
    /\ UNCHANGED <<phase, readEpoch, writes, deleting, snapshot,
                    history, visible, owner, restarted, sourceEpoch>>

Next ==
    \/ \E tx \in Transactions, w \in WriteSets, d \in BOOLEAN: Begin(tx, w, d)
    \/ \E tx \in Transactions: Prepare(tx) \/ Reject(tx) \/ Sync(tx)
                                  \/ Publish(tx) \/ Rollback(tx)
    \/ \E k \in Keys: Prune(k)
    \/ \E tx \in Transactions, w \in WriteSets, d \in BOOLEAN: BeginFromSource(tx, w, d)
    \/ CaptureSource
    \/ DropSource
    \/ Crash
    \/ BadReset

TypeInvariant ==
    /\ phase \in [Transactions -> Phases]
    /\ readEpoch \in [Transactions -> 0..MaxEpoch]
    /\ writes \in [Transactions -> SUBSET Identities]
    /\ deleting \in [Transactions -> BOOLEAN]
    /\ history \in Seq([base : 0..MaxEpoch, keys : SUBSET Identities, delete : BOOLEAN])
    /\ snapshot \in [Transactions -> Seq([base : 0..MaxEpoch, keys : SUBSET Identities, delete : BOOLEAN])]
    /\ Len(history) <= MaxEpoch
    /\ visible \in 0..MaxEpoch
    /\ stamps \in [Identities -> 0..MaxEpoch]
    /\ tombstones \subseteq Keys
    /\ owner \in Transactions \union {"none"}
    /\ restarted \in BOOLEAN
    /\ sourceEpoch \in -1..visible

DurableBeforeVisible == visible <= Len(history)
SinglePublisher ==
    {tx \in Transactions : phase[tx] \in {"prepared", "durable"}} =
        (IF owner = "none" THEN {} ELSE {owner})
StableSnapshots ==
    \A tx \in Pinned: snapshot[tx] = Prefix(readEpoch[tx])

(* Both soundness and completeness: unrelated commits do not cause a false
   version conflict. Compare only outside the serialized publication step. *)
ValidationMatchesHistory ==
    owner = "none" =>
        \A tx \in Transactions:
            phase[tx] = "active" => (IndexValid(tx) <=> ReferenceValid(tx))

FirstCommitterWins ==
    \A i \in 1..Len(history):
        \A j \in (history[i].base + 1)..(i - 1):
            ~Overlap(history[i].keys, history[j].keys)

(* Reachability probes, intentionally false in separate witness runs. *)
NoDisjointWitness ==
    ~\E i \in 1..Len(history): history[i].base + 1 < i
NoRestartCommitWitness ==
    ~(restarted /\ \E tx \in Transactions: phase[tx] = "done" /\
          \E i \in 1..Len(history): history[i] = Record(tx))
NoPruneWitness ==
    ~(~restarted /\ \E i \in 1..Len(history): history[i].delete /\
          \E k \in history[i].keys: stamps[k] = 0)

Spec == Init /\ [][Next]_vars
=============================================================================
