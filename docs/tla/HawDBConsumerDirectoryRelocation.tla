---------------- MODULE HawDBConsumerDirectoryRelocation ----------------
EXTENDS Integers
CONSTANTS KeepOldPath, TrustFailedPublish, ReleaseNativeEarly, DeleteUncertain
(* Process-local custody around an already checkpointed directory rename.
   Sync success is an abstract namespace barrier. No byte-level power-loss,
   reader decoding, cross-parent durability or host incarnation proof is made. *)
VARIABLE s
vars == <<s>>
Init == s = [phase |-> "ready", path |-> "source", renamed |-> FALSE,
 valid |-> TRUE, native |-> TRUE, present |-> TRUE, oldReused |-> FALSE,
 badUse |-> FALSE, wroteForeign |-> FALSE]
Rename == /\ s.phase = "ready"
 /\ s' = [s EXCEPT !.phase = "renamed", !.renamed = TRUE,
                  !.path = IF KeepOldPath THEN "source" ELSE "destination"]
SyncSuccess == /\ s.phase = "renamed"
 /\ s' = [s EXCEPT !.phase = "published", !.path = "destination"]
PostRenameFailure == /\ s.phase = "renamed"
 /\ s' = [s EXCEPT !.phase = "uncertain", !.valid = TrustFailedPublish,
                  !.native = ~ReleaseNativeEarly]
ReuseSource == /\ s.renamed /\ ~s.oldReused
 /\ s' = [s EXCEPT !.oldReused = TRUE]
OwnerUse == /\ s.valid /\ s.phase \in {"ready", "published", "uncertain"}
 /\ s' = [s EXCEPT !.badUse = @ \/ s.phase = "uncertain",
                  !.wroteForeign = @ \/ (s.path = "source" /\ s.oldReused)]
Cleanup == /\ s.phase = "uncertain" /\ DeleteUncertain
 /\ s' = [s EXCEPT !.present = FALSE]
Close == /\ s.phase \in {"ready", "published", "uncertain"}
 /\ s' = [s EXCEPT !.phase = "closed", !.valid = FALSE, !.native = FALSE]
Next == Rename \/ SyncSuccess \/ PostRenameFailure \/ ReuseSource \/ OwnerUse
     \/ Cleanup \/ Close
TypeOK == /\ s.phase \in {"ready", "renamed", "published", "uncertain", "closed"}
 /\ s.path \in {"source", "destination"}
 /\ \A key \in {"renamed", "valid", "native", "present", "oldReused",
                 "badUse", "wroteForeign"}: s[key] \in BOOLEAN
PathFollowsRename == s.phase = "uncertain" => s.path = "destination"
FailedOwnerRevoked == ~s.badUse
NoForeignMutation == ~s.wroteForeign
NativeRetained == s.phase # "closed" => s.native
UncertainRetainsEvidence == s.renamed => s.present
=============================================================================
