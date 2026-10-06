-------------------- MODULE HawDBLargeDocumentLifecycle --------------------
EXTENDS Naturals, FiniteSets

CONSTANTS PublishEarly, ReclaimPinned, DeliverEarly, ReleaseOwnerEarly,
          RollbackCommitted, IgnoreQuota

(* One candidate, one competing publication, and one old reader. Data flush and
   namespace sync are separate transitions. Crash selects either complete
   selector when rename is unsynchronized. This abstracts file checksums and
   platform fsync semantics; it is not a byte-level power-loss model. *)

Phases == {"idle", "capturing", "sealed", "prepared", "validated", "fence",
           "renamed", "published", "retained", "done", "crashed"}
Generations == {0, 1, 2}
Limit == 2

VARIABLES phase, active, stable, files, validated, durable, owner, quota,
          competing, pin, acknowledged, ambiguous, delivered, cleanupDenied,
          retried, lostReply, cancelled
vars == <<phase, active, stable, files, validated, durable, owner, quota,
          competing, pin, acknowledged, ambiguous, delivered, cleanupDenied,
          retried, lostReply, cancelled>>

Protected == {active, stable} \cup (IF pin THEN {0} ELSE {})
Private == 1 \in files /\ 1 \notin Protected
CanReserve == competing + quota < Limit \/ IgnoreQuota

Init == /\ phase = "idle"
        /\ active = 0 /\ stable = 0 /\ files = {0, 2}
        /\ validated = FALSE /\ durable = FALSE
        /\ owner = FALSE /\ quota = 0 /\ competing = 0
        /\ pin = TRUE /\ acknowledged = FALSE /\ ambiguous = FALSE
        /\ delivered = FALSE /\ cleanupDenied = FALSE /\ retried = FALSE
        /\ lostReply = FALSE /\ cancelled = FALSE

Pressure(n) == /\ n \in 0..Limit
               /\ n + quota <= Limit
               /\ competing' = n
               /\ UNCHANGED <<phase, active, stable, files, validated, durable,
                    owner, quota, pin, acknowledged, ambiguous, delivered,
                    cleanupDenied, retried, lostReply, cancelled>>

Capture == /\ phase = "idle" /\ CanReserve
           /\ phase' = "capturing" /\ owner' = TRUE /\ quota' = 1
           /\ files' = files \cup {1}
           /\ UNCHANGED <<active, stable, validated, durable, competing, pin,
                acknowledged, ambiguous, delivered, cleanupDenied, retried,
                lostReply, cancelled>>

Seal == /\ phase = "capturing"
        /\ phase' = "sealed" /\ quota' = 0
        /\ UNCHANGED <<active, stable, files, validated, durable, owner,
             competing, pin, acknowledged, ambiguous, delivered, cleanupDenied,
             retried, lostReply, cancelled>>

Prepare == /\ phase = "sealed" /\ CanReserve
           /\ phase' = "prepared" /\ quota' = 1
           /\ UNCHANGED <<active, stable, files, validated, durable, owner,
                competing, pin, acknowledged, ambiguous, delivered, cleanupDenied,
                retried, lostReply, cancelled>>

Validate == /\ phase = "prepared"
            /\ phase' = "validated" /\ validated' = TRUE
            /\ UNCHANGED <<active, stable, files, durable, owner, quota,
                 competing, pin, acknowledged, ambiguous, delivered,
                 cleanupDenied, retried, lostReply, cancelled>>

Flush == /\ phase = "validated"
         /\ phase' = "fence" /\ durable' = TRUE
         /\ UNCHANGED <<active, stable, files, validated, owner, quota,
              competing, pin, acknowledged, ambiguous, delivered, cleanupDenied,
              retried, lostReply, cancelled>>

Rename == /\ phase = "fence" \/ (PublishEarly /\ phase = "prepared")
          /\ active = 0
          /\ phase' = "renamed" /\ active' = 1
          /\ UNCHANGED <<stable, files, validated, durable, owner, quota,
               competing, pin, acknowledged, ambiguous, delivered, cleanupDenied,
               retried, lostReply, cancelled>>

CompetingPublish == /\ phase \in {"sealed", "prepared", "validated", "fence"}
                    /\ active = 0
                    /\ active' = 2 /\ stable' = 2 /\ files' = files \cup {2}
                    /\ UNCHANGED <<phase, validated, durable, owner,
                         quota, competing, pin, acknowledged, ambiguous,
                         delivered, cleanupDenied, retried, lostReply, cancelled>>

SyncSelector == /\ phase = "renamed"
                /\ stable' = 1 /\ phase' = "published"
                /\ quota' = 0 /\ owner' = FALSE
                /\ UNCHANGED <<active, files, validated, durable, competing,
                     pin, acknowledged, ambiguous, delivered, cleanupDenied,
                     retried, lostReply, cancelled>>

Reply == /\ phase = "published"
         /\ acknowledged' = TRUE
         /\ UNCHANGED <<phase, active, stable, files, validated, durable, owner,
              quota, competing, pin, ambiguous, delivered, cleanupDenied,
              retried, lostReply, cancelled>>

LoseReply == /\ phase = "published" /\ ~acknowledged
             /\ lostReply' = TRUE
             /\ UNCHANGED <<phase, active, stable, files, validated, durable,
                  owner, quota, competing, pin, acknowledged, ambiguous,
                  delivered, cleanupDenied, retried, cancelled>>

PostRenameFailure == /\ phase = "renamed"
                     /\ ambiguous' = TRUE /\ phase' = "retained" /\ quota' = 0
                     /\ UNCHANGED <<active, stable, files, validated, durable,
                          owner, competing, pin, acknowledged, delivered,
                          cleanupDenied, retried, lostReply, cancelled>>

Abort == /\ phase \in {"capturing", "sealed", "prepared", "validated", "fence"}
         /\ phase' = "retained" /\ quota' = 0 /\ cancelled' = TRUE
         /\ UNCHANGED <<active, stable, files, validated, durable, owner,
              competing, pin, acknowledged, ambiguous, delivered, cleanupDenied,
              retried, lostReply>>

CancelAfterCommit == /\ phase = "published"
                     /\ cancelled' = TRUE
                     /\ active' = IF RollbackCommitted THEN 0 ELSE active
                     /\ UNCHANGED <<phase, stable, files, validated, durable,
                          owner, quota, competing, pin, acknowledged, ambiguous,
                          delivered, cleanupDenied, retried, lostReply>>

DenyCleanup == /\ phase = "retained" /\ competing = Limit
               /\ cleanupDenied' = TRUE
               /\ UNCHANGED <<phase, active, stable, files, validated, durable,
                    owner, quota, competing, pin, acknowledged, ambiguous,
                    delivered, retried, lostReply, cancelled>>

Cleanup == /\ phase = "retained" /\ ~ambiguous /\ CanReserve
           /\ phase' = "done" /\ files' = files \ {1}
           /\ owner' = FALSE /\ retried' = cleanupDenied
           /\ UNCHANGED <<active, stable, validated, durable, quota, competing,
                pin, acknowledged, ambiguous, delivered, cleanupDenied,
                lostReply, cancelled>>

Deliver == /\ validated \/ DeliverEarly
           /\ phase \notin {"idle", "done", "crashed"}
           /\ delivered' = TRUE
           /\ UNCHANGED <<phase, active, stable, files, validated, durable,
                owner, quota, competing, pin, acknowledged, ambiguous,
                cleanupDenied, retried, lostReply, cancelled>>

Unpin == /\ pin /\ pin' = FALSE
         /\ UNCHANGED <<phase, active, stable, files, validated, durable,
              owner, quota, competing, acknowledged, ambiguous, delivered,
              cleanupDenied, retried, lostReply, cancelled>>

Reclaim(g) == /\ g \in files \ {active, stable}
              /\ g # 1 /\ (~pin \/ g # 0 \/ ReclaimPinned)
              /\ files' = files \ {g}
              /\ UNCHANGED <<phase, active, stable, validated, durable,
                   owner, quota, competing, pin, acknowledged, ambiguous,
                   delivered, cleanupDenied, retried, lostReply, cancelled>>

EarlyRelease == /\ ReleaseOwnerEarly /\ Private
                /\ owner' = FALSE
                /\ UNCHANGED <<phase, active, stable, files, validated, durable,
                     quota, competing, pin, acknowledged, ambiguous, delivered,
                     cleanupDenied, retried, lostReply, cancelled>>

Crash(g) == /\ phase # "crashed"
            /\ g \in {stable, active}
            /\ g # 1 \/ durable
            /\ active' = g /\ stable' = g /\ phase' = "crashed"
            /\ quota' = 0 /\ competing' = 0 /\ pin' = FALSE
            /\ owner' = (1 \in files /\ g # 1)
            /\ ambiguous' = FALSE
            /\ UNCHANGED <<files, validated, durable, acknowledged, delivered,
                 cleanupDenied, retried, lostReply, cancelled>>

Next == (\E n \in 0..Limit: Pressure(n)) \/ Capture \/ Seal \/ Prepare
        \/ Validate \/ Flush \/ Rename \/ CompetingPublish \/ SyncSelector
        \/ Reply \/ LoseReply \/ PostRenameFailure \/ Abort \/ CancelAfterCommit
        \/ DenyCleanup \/ Cleanup \/ Deliver \/ Unpin
        \/ (\E g \in Generations: Reclaim(g) \/ Crash(g)) \/ EarlyRelease
Spec == Init /\ [][Next]_vars

TypeOK == /\ phase \in Phases /\ active \in Generations /\ stable \in Generations
          /\ files \subseteq Generations /\ quota \in 0..1 /\ competing \in 0..Limit
ActiveComplete == 1 \in {active, stable} => validated /\ durable /\ 1 \in files
SelectedFilesRetained == Protected \subseteq files
PinnedRetained == pin => 0 \in files
OwnershipRetained == Private => owner
NoUnvalidatedDelivery == delivered => validated
CommittedNotRolledBack == acknowledged => active = 1 /\ stable = 1
DescriptorBound == quota + competing <= Limit
NoLostReply == ~lostReply
NoCleanupRetry == ~retried
=============================================================================
