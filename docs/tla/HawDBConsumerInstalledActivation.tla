---------------- MODULE HawDBConsumerInstalledActivation ----------------
EXTENDS Integers, FiniteSets

CONSTANTS BypassOperation, ForgetIncarnation, SeparateReceipt,
 SkipDataSync, SkipSourceSync, ReleaseSlotEarly, DropHeavyUnderFence,
 DecodeManifestUnderFence, TrustDirtyCheckpoint, AckBeforeHeadSync,
 ReclaimUnknown, RecoverAsEmpty, DropSourceFence

(* Proposed consumer protocol, NOT an implementation refinement. C-ready and
   native input closure are assumed qualified by separate components. File and
   directory barriers are abstract successful operations; individual bytes,
   torn writes, real I/O cost, cancellation fairness and multi-operation receipt
   retention are outside this finite model. *)
VARIABLE s
vars == <<s>>
Stamp(g, e, i, op) == [generation |-> g, epoch |-> e, incarnation |-> i, operation |-> op]
Initial == Stamp(0, 0, 0, 0)
Head(v, r) == [view |-> v, receipt |-> r]
Working == s.phase \in {"preparing", "cReady", "captured", "closed", "installed",
                         "checking", "validated", "selected"}
BeforeSelection == Working /\ s.phase # "selected"
IdentityOK == IF ForgetIncarnation
              THEN s.incarnation % 2 = s.capturedIncarnation % 2
              ELSE s.incarnation = s.capturedIncarnation
OracleOK == s.incarnation = s.capturedIncarnation /\ s.scope = s.capturedScope
Ready == /\ IdentityOK /\ s.scope = s.capturedScope
         /\ ~s.cancelled /\ ~s.uncertain /\ s.closed /\ 1 \in s.files
         /\ (s.dataSynced \/ SkipDataSync)
         /\ (s.sourceDurable >= s.epochE \/ SkipSourceSync)
NewView == Stamp(1, s.epochE, s.capturedIncarnation, 1)
BadRecovery == s.uncertain \/ s.head.view # s.head.receipt
 \/ s.head.view.generation \notin s.files
 \/ s.head.view.epoch > s.sourceDurable

Init == s = [phase |-> "idle", liveEpoch |-> 1, sourceDurable |-> 0,
 scope |-> 1, durableScope |-> 0, incarnation |-> 0,
 capturedIncarnation |-> 0, epochE |-> 0, capturedScope |-> 0,
 head |-> Head(Initial, Initial), durableHead |-> Head(Initial, Initial),
 files |-> {0}, dataSynced |-> FALSE, headSynced |-> FALSE,
 lease |-> TRUE, slot |-> FALSE, heavy |-> FALSE, fence |-> FALSE,
 dirty |-> FALSE, admittedDirty |-> FALSE, internalWrites |-> 0,
 closed |-> FALSE, cancelled |-> FALSE, heavyUnderFence |-> 0,
 selectedInvalid |-> FALSE, acknowledged |-> FALSE, crashed |-> FALSE,
 uncertain |-> FALSE, evidenceFiles |-> {}, recoveryReady |-> FALSE,
 badRecovery |-> FALSE, retryObserved |-> FALSE, retryReceipt |-> Initial,
 writerDuringBarrier |-> FALSE]

LegacyApply == /\ ~s.crashed /\ s.phase="idle" /\ ~s.dirty
 /\ s'=[s EXCEPT !.dirty=TRUE]
Begin == /\ s.phase="idle" /\ (~s.dirty \/ TrustDirtyCheckpoint)
 /\ s'=[s EXCEPT !.phase="preparing", !.capturedIncarnation=s.incarnation,
         !.slot=TRUE, !.heavy=TRUE, !.admittedDirty=s.dirty]
CReady == /\ s.phase="preparing"
 /\ s'=[s EXCEPT !.phase="cReady"]
CaptureE == /\ s.phase="cReady"
 /\ s'=[s EXCEPT !.phase="captured", !.epochE=s.liveEpoch, !.capturedScope=s.scope]
CloseNativeInput == /\ s.phase="captured"
 /\ s'=[s EXCEPT !.phase="closed", !.closed=TRUE]
Install == /\ s.phase="closed"
 /\ s'=[s EXCEPT !.phase="installed", !.files=@ \cup {1},
         !.evidenceFiles=IF s.uncertain THEN @ \cup {1} ELSE @]
SyncData == /\ s.phase="installed" /\ ~s.dataSynced
 /\ s'=[s EXCEPT !.dataSynced=TRUE]
SyncSource == /\ s.phase="installed" /\ s.sourceDurable<s.liveEpoch
 /\ s'=[s EXCEPT !.sourceDurable=s.liveEpoch, !.durableScope=s.scope]

SourceWrite(relevant) == /\ ~s.crashed /\ s.liveEpoch<2
 /\ (~s.fence \/ DropSourceFence)
 /\ s'=[s EXCEPT !.liveEpoch=2, !.scope=IF relevant THEN 2 ELSE @,
         !.writerDuringBarrier=@ \/ (s.phase="selected" /\ ~s.headSynced)]
Rebind == /\ ~s.crashed /\ s.incarnation<2 /\ (~s.fence \/ DropSourceFence)
 /\ s'=[s EXCEPT !.incarnation=@+1]
ConsumerWrite == /\ Working /\ ~s.crashed
 /\ (~s.slot \/ BypassOperation) /\ s.internalWrites=0
 /\ s'=[s EXCEPT !.internalWrites=1]
LoseSlot == /\ ReleaseSlotEarly /\ BeforeSelection /\ s.slot
 /\ s'=[s EXCEPT !.slot=FALSE]
Cancel == /\ BeforeSelection /\ ~s.cancelled
 /\ s'=[s EXCEPT !.cancelled=TRUE]

EnterFence == /\ s.phase="installed"
 /\ s'=[s EXCEPT !.phase="checking", !.fence=TRUE]
Validate == /\ s.phase="checking" /\ Ready
 /\ s'=[s EXCEPT !.phase="validated",
         !.heavyUnderFence=@+IF DecodeManifestUnderFence THEN 1 ELSE 0]
Reject == /\ s.phase \in {"checking", "validated"}
 /\ (~Ready \/ s.cancelled)
 /\ s'=[s EXCEPT !.phase="refused", !.fence=FALSE,
         !.heavy=~DropHeavyUnderFence,
         !.heavyUnderFence=@+IF DropHeavyUnderFence THEN 1 ELSE 0]
Select == /\ s.phase="validated" /\ ~s.cancelled /\ ~s.uncertain
 /\ s'=[s EXCEPT !.phase="selected", !.fence=FALSE,
         !.heavy=~DropHeavyUnderFence,
         !.heavyUnderFence=@+IF DropHeavyUnderFence THEN 1 ELSE 0,
         !.head=Head(NewView, IF SeparateReceipt THEN s.head.receipt ELSE NewView),
         !.selectedInvalid=~OracleOK]
SyncHead == /\ s.phase="selected" /\ ~s.headSynced /\ ~s.uncertain
 /\ s'=[s EXCEPT !.headSynced=TRUE, !.durableHead=s.head]
Respond(delivered) == /\ s.phase="selected" /\ ~s.uncertain
 /\ (s.headSynced \/ AckBeforeHeadSync)
 /\ s'=[s EXCEPT !.phase="done", !.acknowledged=delivered,
         !.fence=FALSE]
UncertainReturn == /\ s.phase="selected" /\ s.uncertain
 /\ s'=[s EXCEPT !.phase="uncertain", !.fence=FALSE]
Dispose == /\ s.phase \in {"done", "refused", "uncertain"} /\ s.heavy /\ ~s.fence
 /\ s'=[s EXCEPT !.heavy=FALSE, !.slot=FALSE, !.lease=FALSE]

Damage == /\ ~s.crashed /\ ~s.uncertain /\ 1 \in s.files
 /\ s'=[s EXCEPT !.uncertain=TRUE, !.evidenceFiles=s.files]
Cleanup == /\ ~s.slot
 /\ (~s.uncertain \/ ReclaimUnknown)
 /\ s'=[s EXCEPT !.files={g \in s.files: g=s.head.view.generation}]
Crash(keepHead, keepData) == /\ ~s.crashed /\ s.phase#"idle"
 /\ s'=[s EXCEPT !.phase="crashed", !.crashed=TRUE,
         !.head=IF s.headSynced \/ keepHead THEN s.head ELSE s.durableHead,
         !.files=IF s.dataSynced \/ keepData THEN s.files ELSE s.files\{1},
         !.liveEpoch=s.sourceDurable, !.scope=s.durableScope,
         !.fence=FALSE, !.heavy=FALSE, !.slot=FALSE, !.lease=FALSE]
Reopen == /\ s.phase="crashed"
 /\ s'=[s EXCEPT !.phase="recovered", !.badRecovery=BadRecovery,
         !.recoveryReady=~BadRecovery \/ RecoverAsEmpty]
Retry == /\ s.phase \in {"done", "recovered"} /\ ~BadRecovery
 /\ s.head.view.operation=1 /\ ~s.retryObserved
 /\ s'=[s EXCEPT !.retryObserved=TRUE, !.retryReceipt=s.head.receipt]

Next == LegacyApply \/ Begin \/ CReady \/ CaptureE \/ CloseNativeInput \/ Install
 \/ SyncData \/ SyncSource \/ (\E relevant \in BOOLEAN: SourceWrite(relevant))
 \/ Rebind \/ ConsumerWrite \/ LoseSlot \/ Cancel \/ EnterFence \/ Validate
 \/ Reject \/ Select \/ SyncHead \/ (\E delivered \in BOOLEAN: Respond(delivered))
 \/ UncertainReturn \/ Dispose \/ Damage \/ Cleanup
 \/ (\E keepHead,keepData \in BOOLEAN: Crash(keepHead,keepData)) \/ Reopen \/ Retry

TypeOK ==
 /\ s.phase \in {"idle", "preparing", "cReady", "captured", "closed",
                "installed", "checking", "validated", "selected", "refused", "done", "uncertain", "crashed", "recovered"}
 /\ s.liveEpoch \in 0..2 /\ s.sourceDurable \in 0..2
 /\ s.scope \in 0..2 /\ s.durableScope \in 0..2
 /\ s.incarnation \in 0..2 /\ s.capturedIncarnation \in 0..2
 /\ s.epochE \in 0..2 /\ s.capturedScope \in 0..2
 /\ s.files \subseteq {0,1} /\ s.evidenceFiles \subseteq {0,1}
 /\ s.internalWrites \in 0..1 /\ s.heavyUnderFence \in 0..1
 /\ \A key \in {"dataSynced","headSynced","lease","slot","heavy","fence",
                 "dirty","admittedDirty","closed","cancelled","selectedInvalid","acknowledged",
                 "crashed","uncertain","recoveryReady","badRecovery","retryObserved","writerDuringBarrier"}: s[key] \in BOOLEAN
SlotExclusion == Working => s.slot /\ s.internalWrites=0
CleanAdmission == ~s.admittedDirty
NoHeavyUnderFence == s.heavyUnderFence=0
TupleCoherent == s.head.view=s.head.receipt
ValidSelection == ~s.selectedInvalid
DurableOrdered == s.durableHead.view.generation=1 =>
 s.dataSynced /\ s.sourceDurable>=s.durableHead.view.epoch
AckOrdered == s.acknowledged => s.durableHead.view.generation=1
AckSurvives == s.crashed /\ s.acknowledged => s.head.view.operation=1
PendingRetained == s.phase \in {"installed","checking","validated"} => 1 \in s.files
UncertainRetains == s.uncertain /\ ~s.crashed => s.evidenceFiles \subseteq s.files
RecoveryFailClosed == s.recoveryReady => ~s.badRecovery
RetryCoherent == s.retryObserved => s.retryReceipt.operation=1
NoBarrierWriterWitness == ~(s.phase="done" /\ s.acknowledged /\ s.writerDuringBarrier)
NoLostResponseWitness == ~(s.phase="done" /\ ~s.acknowledged /\ s.headSynced /\ s.retryObserved)
NoUnrelatedWitness == ~(s.phase="done" /\ s.epochE=1 /\ s.liveEpoch=2 /\ s.scope=1)
NoReopenWitness == ~(s.phase="recovered" /\ s.recoveryReady /\ s.head.view.operation=1)
=============================================================================
