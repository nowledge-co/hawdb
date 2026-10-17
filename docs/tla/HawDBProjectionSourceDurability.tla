---------------- MODULE HawDBProjectionSourceDurability ----------------
EXTENDS Integers

CONSTANTS MaxEpoch, TrustRecoveredPolicy, TrustExistingWalName,
          SkipDirectoryBarrier, EarlyGroupKnowledge, AdvanceOnSyncFailure,
          ReleaseSyncOwner, PublishFromWrittenEpoch
VARIABLES db, captured, sourceEpoch, projectionEpoch, lastSyncEpoch,
          relaxedPublished
vars == <<db, captured, sourceEpoch, projectionEpoch, lastSyncEpoch,
          relaxedPublished>>

(* An epoch is one complete transaction already flushed to the OS. File and
   namespace durability are distinct; process reopen does not drop unsynced
   bytes, whereas power loss restores only a complete durable source prefix.
   Completed synchronization barriers are abstract atomic successful events. *)
RecoverablePrefix == IF db.nameDurable THEN db.fileDurable ELSE db.checkpoint
OwnerAvailable == db.phase = "idle" \/ ReleaseSyncOwner
Init ==
    /\ db = [epoch |-> 0, checkpoint |-> 0, fileDurable |-> 0,
              nameDurable |-> FALSE, known |-> 0, nameKnown |-> FALSE,
              policy |-> "relaxed", grouped |-> FALSE, phase |-> "idle",
              poisoned |-> FALSE, reopens |-> 0, crashes |-> 0]
    /\ captured = FALSE /\ sourceEpoch = 0 /\ projectionEpoch = 0
    /\ lastSyncEpoch = 0 /\ relaxedPublished = FALSE

Commit ==
    /\ OwnerAvailable /\ ~db.poisoned /\ db.epoch < MaxEpoch
    /\ LET next == db.epoch + 1
           synchronous == db.policy = "sync" /\ ~db.grouped
           namespace == synchronous /\ (~TrustExistingWalName \/ db.nameKnown)
       IN db' = [db EXCEPT !.epoch = next,
                  !.fileDurable = IF synchronous THEN next ELSE @,
                  !.nameDurable = @ \/ namespace,
                  !.nameKnown = @ \/ synchronous,
                  !.known = IF synchronous \/ (db.grouped /\ EarlyGroupKnowledge)
                               THEN next ELSE @]
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>

Capture ==
    /\ db.phase = "idle" /\ ~db.poisoned /\ ~captured
    /\ captured' = TRUE /\ sourceEpoch' = db.epoch
    /\ UNCHANGED <<db, projectionEpoch, lastSyncEpoch, relaxedPublished>>
Release ==
    /\ captured /\ captured' = FALSE
    /\ UNCHANGED <<db, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>

StartSync ==
    /\ db.phase = "idle" /\ ~db.grouped /\ ~db.poisoned
    /\ db.known < db.epoch
    /\ db' = [db EXCEPT !.phase = "wal"]
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>
SyncWal ==
    /\ db.phase = "wal"
    /\ db' = [db EXCEPT !.fileDurable = db.epoch,
                  !.phase = IF SkipDirectoryBarrier THEN "idle" ELSE "directory",
                  !.known = IF SkipDirectoryBarrier THEN db.epoch ELSE @,
                  !.nameKnown = IF SkipDirectoryBarrier THEN TRUE ELSE @]
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>
SyncDirectory ==
    /\ db.phase = "directory"
    /\ db' = [db EXCEPT !.nameDurable = TRUE, !.nameKnown = TRUE,
                  !.known = db.epoch, !.phase = "idle"]
    /\ lastSyncEpoch' = db.epoch
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, relaxedPublished>>
SyncFailure ==
    /\ db.phase \in {"wal", "directory"}
    /\ db' = [db EXCEPT !.phase = "idle", !.poisoned = TRUE,
                  !.known = IF AdvanceOnSyncFailure THEN db.epoch ELSE @]
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>

StartGroup ==
    /\ db.phase = "idle" /\ ~db.poisoned /\ ~db.grouped /\ db.policy = "sync"
    /\ db' = [db EXCEPT !.grouped = TRUE]
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>
FlushGroup ==
    /\ db.phase = "idle" /\ ~db.poisoned /\ db.grouped
    /\ db' = [db EXCEPT !.grouped = FALSE, !.fileDurable = db.epoch,
                  !.nameDurable = TRUE, !.nameKnown = TRUE, !.known = db.epoch]
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>
Checkpoint ==
    /\ db.phase = "idle" /\ ~db.poisoned /\ ~db.grouped
    /\ db' = [db EXCEPT !.checkpoint = db.epoch, !.fileDurable = db.epoch,
                  !.nameDurable = TRUE, !.nameKnown = TRUE, !.known = db.epoch]
    /\ UNCHANGED <<captured, sourceEpoch, projectionEpoch, lastSyncEpoch, relaxedPublished>>

Reopen(policy) ==
    /\ db.reopens = 0 /\ policy \in {"sync", "relaxed"}
    /\ db' = [db EXCEPT !.reopens = 1, !.policy = policy,
                  !.phase = "idle", !.grouped = FALSE, !.poisoned = FALSE,
                  !.known = IF TrustRecoveredPolicy /\ policy = "sync"
                               THEN db.epoch ELSE db.checkpoint,
                  !.nameKnown = FALSE]
    /\ captured' = FALSE /\ lastSyncEpoch' = 0
    /\ UNCHANGED <<sourceEpoch, projectionEpoch, relaxedPublished>>
PowerLoss ==
    /\ db.crashes = 0
    /\ db' = [db EXCEPT !.epoch = RecoverablePrefix, !.crashes = 1,
                  !.fileDurable = RecoverablePrefix, !.known = db.checkpoint,
                  !.nameKnown = FALSE, !.phase = "idle", !.grouped = FALSE,
                  !.poisoned = FALSE]
    /\ captured' = FALSE /\ lastSyncEpoch' = 0
    /\ UNCHANGED <<sourceEpoch, projectionEpoch, relaxedPublished>>

Publish ==
    /\ db.phase = "idle" /\ ~db.poisoned /\ ~db.grouped /\ captured
    /\ sourceEpoch <= IF PublishFromWrittenEpoch THEN db.epoch ELSE db.known
    /\ sourceEpoch >= projectionEpoch
    /\ projectionEpoch' = sourceEpoch
    /\ relaxedPublished' = (relaxedPublished \/
          (db.policy = "relaxed" /\ lastSyncEpoch >= sourceEpoch /\ sourceEpoch > 0))
    /\ UNCHANGED <<db, captured, sourceEpoch, lastSyncEpoch>>

Next == Commit \/ Capture \/ Release \/ StartSync \/ SyncWal \/ SyncDirectory
        \/ SyncFailure \/ StartGroup \/ FlushGroup \/ Checkpoint
        \/ (\E p \in {"sync", "relaxed"}: Reopen(p)) \/ PowerLoss \/ Publish
TypeOK ==
    /\ db.epoch \in 0..MaxEpoch /\ db.checkpoint \in 0..db.epoch
    /\ db.fileDurable \in db.checkpoint..db.epoch /\ db.known \in 0..db.epoch
    /\ db.policy \in {"sync", "relaxed"}
    /\ db.phase \in {"idle", "wal", "directory"}
    /\ db.reopens \in 0..1 /\ db.crashes \in 0..1
    /\ {db.nameDurable, db.nameKnown, db.grouped, db.poisoned,
          captured, relaxedPublished} \subseteq BOOLEAN
    /\ lastSyncEpoch \in 0..MaxEpoch
    /\ sourceEpoch \in 0..MaxEpoch /\ projectionEpoch \in 0..MaxEpoch
KnownPrefixSound == db.known <= RecoverablePrefix
KnownNamespaceSound == db.nameKnown => db.nameDurable
DurableProjectionSource == projectionEpoch <= RecoverablePrefix
NoRelaxedPublication == ~relaxedPublished
=============================================================================
