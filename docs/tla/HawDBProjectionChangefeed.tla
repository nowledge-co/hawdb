-------------------- MODULE HawDBProjectionChangefeed --------------------
EXTENDS Integers, Naturals, TLC

(***************************************************************************)
(* The canonical WAL contains mutation identity only. Search documents,    *)
(* embeddings, ranks, and the external projection watermark are deliberately*)
(* absent from changeKind and every other canonical variable.              *)
(***************************************************************************)

CONSTANT MaxEpoch

ASSUME MaxEpoch \in Nat \ {0}

Epochs == 0..MaxEpoch
CommittedEpochs == 1..MaxEpoch
ChangeKinds == {"none", "exact", "barrier"}
ConsumerPhases == {"idle", "reading", "processed"}

VARIABLES
    walEpoch,
    visibleEpoch,
    changeKind,
    resumeFloor,
    projectionEpoch,
    consumerPhase,
    batchTarget,
    processedThrough

vars == <<
    walEpoch,
    visibleEpoch,
    changeKind,
    resumeFloor,
    projectionEpoch,
    consumerPhase,
    batchTarget,
    processedThrough
>>

Init ==
    /\ walEpoch = 0
    /\ visibleEpoch = 0
    /\ changeKind = [epoch \in CommittedEpochs |-> "none"]
    /\ resumeFloor = 0
    /\ projectionEpoch = 0
    /\ consumerPhase = "idle"
    /\ batchTarget = 0
    /\ processedThrough = 0

(***************************************************************************)
(* WAL durability chooses either an exact bounded identity set or a fixed  *)
(* rebuild barrier. A canonical commit can never exist without one of them.*)
(***************************************************************************)
AppendWal(kind) ==
    /\ kind \in {"exact", "barrier"}
    /\ walEpoch < MaxEpoch
    /\ walEpoch' = walEpoch + 1
    /\ changeKind' = [changeKind EXCEPT ![walEpoch + 1] = kind]
    /\ UNCHANGED <<
        visibleEpoch,
        resumeFloor,
        projectionEpoch,
        consumerPhase,
        batchTarget,
        processedThrough
        >>

(***************************************************************************)
(* Visibility follows WAL durability. The implementation publishes the    *)
(* graph and relational state under this same commit epoch.                *)
(***************************************************************************)
PublishCanonical ==
    /\ visibleEpoch < walEpoch
    /\ visibleEpoch' = visibleEpoch + 1
    /\ UNCHANGED <<
        walEpoch,
        changeKind,
        resumeFloor,
        projectionEpoch,
        consumerPhase,
        batchTarget,
        processedThrough
        >>

(***************************************************************************)
(* A resumable batch owns whole exact changes after BeginResume. GC may    *)
(* advance the retained floor later because the returned batch has copied  *)
(* its bounded identities.                                                 *)
(***************************************************************************)
BeginResume ==
    /\ consumerPhase = "idle"
    /\ projectionEpoch >= resumeFloor
    /\ projectionEpoch < visibleEpoch
    /\ \E target \in (projectionEpoch + 1)..visibleEpoch:
        /\ \A epoch \in (projectionEpoch + 1)..target:
            changeKind[epoch] = "exact"
        /\ consumerPhase' = "reading"
        /\ batchTarget' = target
        /\ processedThrough' = projectionEpoch
    /\ UNCHANGED <<
        walEpoch,
        visibleEpoch,
        changeKind,
        resumeFloor,
        projectionEpoch
        >>

ProcessIdentitySet ==
    /\ consumerPhase = "reading"
    /\ processedThrough < batchTarget
    /\ processedThrough' = processedThrough + 1
    /\ UNCHANGED <<
        walEpoch,
        visibleEpoch,
        changeKind,
        resumeFloor,
        projectionEpoch,
        consumerPhase,
        batchTarget
        >>

FinishBatch ==
    /\ consumerPhase = "reading"
    /\ processedThrough = batchTarget
    /\ consumerPhase' = "processed"
    /\ UNCHANGED <<
        walEpoch,
        visibleEpoch,
        changeKind,
        resumeFloor,
        projectionEpoch,
        batchTarget,
        processedThrough
        >>

(***************************************************************************)
(* Projection content and its complete-through watermark publish as one   *)
(* external operation only after every selected commit was processed.     *)
(***************************************************************************)
PublishProjection ==
    /\ consumerPhase = "processed"
    /\ processedThrough = batchTarget
    /\ projectionEpoch' = batchTarget
    /\ consumerPhase' = "idle"
    /\ batchTarget' = 0
    /\ processedThrough' = batchTarget
    /\ UNCHANGED <<walEpoch, visibleEpoch, changeKind, resumeFloor>>

(***************************************************************************)
(* GC removes a whole oldest prefix. A cursor below the new floor cannot   *)
(* begin incremental resume; it must rebuild from canonical state.         *)
(***************************************************************************)
GarbageCollect ==
    /\ resumeFloor < visibleEpoch
    /\ \E newFloor \in (resumeFloor + 1)..visibleEpoch:
        resumeFloor' = newFloor
    /\ UNCHANGED <<
        walEpoch,
        visibleEpoch,
        changeKind,
        projectionEpoch,
        consumerPhase,
        batchTarget,
        processedThrough
        >>

FullRebuild ==
    /\ consumerPhase = "idle"
    /\ projectionEpoch < visibleEpoch
    /\ \/ projectionEpoch < resumeFloor
       \/ \E epoch \in (projectionEpoch + 1)..visibleEpoch:
            changeKind[epoch] = "barrier"
    /\ projectionEpoch' = visibleEpoch
    /\ processedThrough' = visibleEpoch
    /\ UNCHANGED <<
        walEpoch,
        visibleEpoch,
        changeKind,
        resumeFloor,
        consumerPhase,
        batchTarget
        >>

CrashAndRecover ==
    /\ consumerPhase' = "idle"
    /\ batchTarget' = 0
    /\ processedThrough' = projectionEpoch
    /\ UNCHANGED <<
        walEpoch,
        visibleEpoch,
        changeKind,
        resumeFloor,
        projectionEpoch
        >>

Next ==
    \/ AppendWal("exact")
    \/ AppendWal("barrier")
    \/ PublishCanonical
    \/ BeginResume
    \/ ProcessIdentitySet
    \/ FinishBatch
    \/ PublishProjection
    \/ GarbageCollect
    \/ FullRebuild
    \/ CrashAndRecover

TypeOK ==
    /\ walEpoch \in Epochs
    /\ visibleEpoch \in Epochs
    /\ changeKind \in [CommittedEpochs -> ChangeKinds]
    /\ resumeFloor \in Epochs
    /\ projectionEpoch \in Epochs
    /\ consumerPhase \in ConsumerPhases
    /\ batchTarget \in Epochs
    /\ processedThrough \in Epochs

DurableBeforeVisible == visibleEpoch <= walEpoch

ProjectionNeverExceedsCanonical == projectionEpoch <= visibleEpoch

FloorNeverExceedsCanonical == resumeFloor <= visibleEpoch

EveryWalCommitHasChangeIdentity ==
    /\ \A epoch \in 1..walEpoch: changeKind[epoch] # "none"
    /\ \A epoch \in (walEpoch + 1)..MaxEpoch: changeKind[epoch] = "none"

ActiveBatchIsWholeAndExact ==
    consumerPhase \in {"reading", "processed"} =>
        /\ projectionEpoch < batchTarget
        /\ batchTarget <= visibleEpoch
        /\ processedThrough \in projectionEpoch..batchTarget
        /\ \A epoch \in (projectionEpoch + 1)..batchTarget:
            changeKind[epoch] = "exact"

ProcessedPhaseIsComplete ==
    consumerPhase = "processed" => processedThrough = batchTarget

ExpiredIdleCursorCannotResume ==
    /\ consumerPhase = "idle"
    /\ projectionEpoch < resumeFloor
    => batchTarget = 0

Spec == Init /\ [][Next]_vars

=============================================================================
