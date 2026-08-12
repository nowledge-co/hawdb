-------------------- MODULE SkeinProjectionDurability --------------------
EXTENDS Integers, Naturals

(***************************************************************************)
(* A durable projection is a pure function of canonical state at its       *)
(* cursor epoch. The only durable incremental progress state is the        *)
(* cursor inside the projection manifest; cursor advance and delta         *)
(* artifact publication are one atomic manifest replace. Catch-up over     *)
(* (cursor, currentEpoch] is derived from WAL replay, so a projection may  *)
(* serve as Ready only while its cursor is at or above the WAL replay      *)
(* floor. Reclamation must not pass the cursor unless the projection lags  *)
(* beyond StaleLimit; passing it forces a full rebuild.                    *)
(***************************************************************************)

CONSTANT MaxEpoch, StaleLimit

ASSUME /\ MaxEpoch \in Nat \ {0}
       /\ StaleLimit \in Nat \ {0}

Epochs == 0..MaxEpoch
BuildPhases == {"idle", "building", "durable"}
Statuses == {"ready", "stale", "rebuilding"}

VARIABLES
    currentEpoch,
    replayFloor,
    cursor,
    buildPhase,
    buildTarget,
    durableEpochs,
    status

vars == <<
    currentEpoch,
    replayFloor,
    cursor,
    buildPhase,
    buildTarget,
    durableEpochs,
    status
>>

Init ==
    /\ currentEpoch = 0
    /\ replayFloor = 0
    /\ cursor = 0
    /\ buildPhase = "idle"
    /\ buildTarget = 0
    /\ durableEpochs = {0}
    /\ status = "ready"

Commit ==
    /\ currentEpoch < MaxEpoch
    /\ currentEpoch' = currentEpoch + 1
    /\ UNCHANGED <<
        replayFloor,
        cursor,
        buildPhase,
        buildTarget,
        durableEpochs,
        status
        >>

(***************************************************************************)
(* Reclamation that respects the projection cursor may always advance the  *)
(* replay floor up to the cursor.                                          *)
(***************************************************************************)
TruncateRespectingCursor ==
    /\ \E newFloor \in (replayFloor + 1)..cursor:
        replayFloor' = newFloor
    /\ UNCHANGED <<
        currentEpoch,
        cursor,
        buildPhase,
        buildTarget,
        durableEpochs,
        status
        >>

(***************************************************************************)
(* A projection lagging beyond StaleLimit no longer pins the WAL.          *)
(* Reclamation may pass its cursor, and doing so marks the projection      *)
(* stale: it must not serve as Ready again before a full rebuild.          *)
(***************************************************************************)
TruncatePastStaleCursor ==
    /\ currentEpoch - cursor > StaleLimit
    /\ \E newFloor \in (cursor + 1)..currentEpoch:
        replayFloor' = newFloor
    /\ status' = "stale"
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ UNCHANGED <<currentEpoch, cursor, durableEpochs>>

(***************************************************************************)
(* Incremental build: read the changefeed over (cursor, buildTarget],      *)
(* stream a delta artifact, make it durable, then publish the manifest.    *)
(* The changefeed is derivable only while the cursor is replayable.        *)
(***************************************************************************)
BeginIncrementalBuild ==
    /\ status = "ready"
    /\ buildPhase = "idle"
    /\ cursor < currentEpoch
    /\ cursor >= replayFloor
    /\ buildPhase' = "building"
    /\ buildTarget' = currentEpoch
    /\ UNCHANGED <<
        currentEpoch,
        replayFloor,
        cursor,
        durableEpochs,
        status
        >>

PersistDeltaArtifact ==
    /\ buildPhase = "building"
    /\ buildPhase' = "durable"
    /\ durableEpochs' = durableEpochs \cup {buildTarget}
    /\ UNCHANGED <<currentEpoch, replayFloor, cursor, buildTarget, status>>

(***************************************************************************)
(* Cursor advance and delta publication are the same atomic manifest      *)
(* replace. The cursor never moves except through this action or a        *)
(* completed full rebuild.                                                 *)
(***************************************************************************)
PublishManifest ==
    /\ buildPhase = "durable"
    /\ buildTarget \in durableEpochs
    /\ cursor' = buildTarget
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ UNCHANGED <<currentEpoch, replayFloor, durableEpochs, status>>

(***************************************************************************)
(* Full rebuild scans canonical state directly; it does not need the WAL.  *)
(* Its publication is the same atomic manifest replace.                    *)
(***************************************************************************)
BeginRebuild ==
    /\ status = "stale"
    /\ status' = "rebuilding"
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ UNCHANGED <<currentEpoch, replayFloor, cursor, durableEpochs>>

FinishRebuild ==
    /\ status = "rebuilding"
    /\ durableEpochs' = durableEpochs \cup {currentEpoch}
    /\ cursor' = currentEpoch
    /\ status' = "ready"
    /\ UNCHANGED <<currentEpoch, replayFloor, buildPhase, buildTarget>>

(***************************************************************************)
(* A crash discards in-flight build state but never the published          *)
(* manifest. Recovery recomputes serveability from the persisted cursor    *)
(* against the replay floor: at or above the floor the projection mounts   *)
(* and catches up; below the floor it must rebuild.                        *)
(***************************************************************************)
CrashAndRecover ==
    /\ buildPhase' = "idle"
    /\ buildTarget' = 0
    /\ status' = IF cursor >= replayFloor THEN "ready" ELSE "stale"
    /\ UNCHANGED <<currentEpoch, replayFloor, cursor, durableEpochs>>

Next ==
    \/ Commit
    \/ TruncateRespectingCursor
    \/ TruncatePastStaleCursor
    \/ BeginIncrementalBuild
    \/ PersistDeltaArtifact
    \/ PublishManifest
    \/ BeginRebuild
    \/ FinishRebuild
    \/ CrashAndRecover

TypeOK ==
    /\ currentEpoch \in Epochs
    /\ replayFloor \in Epochs
    /\ cursor \in Epochs
    /\ buildPhase \in BuildPhases
    /\ buildTarget \in Epochs
    /\ durableEpochs \subseteq Epochs
    /\ status \in Statuses

CursorNeverExceedsCanonical ==
    cursor <= currentEpoch

FloorNeverExceedsCanonical ==
    replayFloor <= currentEpoch

(***************************************************************************)
(* Serve-soundness: a Ready projection can always derive catch-up over    *)
(* (cursor, currentEpoch] from WAL replay, so base + deltas + catch-up    *)
(* equals a full rebuild at the current epoch.                             *)
(***************************************************************************)
ReadyImpliesCatchUpCoverage ==
    status = "ready" => cursor >= replayFloor

(***************************************************************************)
(* An in-flight delta covers exactly (cursor, buildTarget] with a target   *)
(* the canonical store has reached; publication can therefore never       *)
(* create a coverage gap or advertise unwritten epochs.                    *)
(***************************************************************************)
InFlightDeltaIsContiguous ==
    buildPhase \in {"building", "durable"} =>
        /\ buildTarget > cursor
        /\ buildTarget <= currentEpoch

(***************************************************************************)
(* Reclamation passes the cursor only by marking the projection stale.     *)
(***************************************************************************)
BelowFloorIsNeverServed ==
    cursor < replayFloor => status # "ready"

(***************************************************************************)
(* The manifest never references artifact coverage that was not made       *)
(* durable before publication: cursor advance and artifact fsync are one   *)
(* atomic manifest replace, in that order.                                 *)
(***************************************************************************)
CursorIsAlwaysDurable ==
    cursor \in durableEpochs

Spec == Init /\ [][Next]_vars

=============================================================================
