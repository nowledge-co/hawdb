-------------------- MODULE HawDBAutomaticCheckpoint --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Independent bounded safety oracle for one old/candidate handoff.         *)
(* Each transaction has distinct schema and data fragments. Checkpoint      *)
(* images are defined from the oracle prefix, not from replay bookkeeping.  *)
(* OS-flushed bytes can be lost, torn or reordered on power loss; a completed*)
(* synchronization barrier retains every covered byte. Catalog/checkpoint   *)
(* closure and selector synchronization are separate protocol boundaries.   *)
(* An uncertain selector fails closed without deleting either generation.  *)
(* This abstracts checksums/atomic rename and does not prove Rust refinement*)
(* or scheduler liveness, resource bounds, or a particular storage platform. *)
(* Job-owned memory and execution admission are separate logical leases.    *)
(* Pending candidates keep the complete prefix and yield execution.         *)
(* Retirement may defer and read adoption requires an available writer gate.*)
(* Serving allocation leases and physical resource accounting are separate. *)
(* A sealed prefix remains private while foreground writes advance.        *)
(* Rechecking the complete writer identity either retries on the same base *)
(* or enters the bounded selection gate; sealing itself does not freeze it.*)
(***************************************************************************)

CONSTANTS MaxCommit, DurabilityModes,
          SelectBeforeSync, DropSuffix, SplitTransaction, ReclaimPinned,
          LeakLease, SelectStalePrefix, LoseMemoryOnPause, KeepExecutionOnPause,
          ReadAdoptWhileBusy, ReleaseLeaseBeforeCleanup, ManualBeforeCleanup

ASSUME /\ MaxCommit \in Nat \ {0}
       /\ DurabilityModes \subseteq {"SyncOnEveryWrite", "SyncOnCheckpoint"}
       /\ DurabilityModes # {}
       /\ {SelectBeforeSync, DropSuffix, SplitTransaction,
             ReclaimPinned, LeakLease, SelectStalePrefix, LoseMemoryOnPause, KeepExecutionOnPause,
          ReadAdoptWhileBusy, ReleaseLeaseBeforeCleanup, ManualBeforeCleanup} \subseteq BOOLEAN

Generations == {0, 1}
Epochs == 0..MaxCommit
Fragments == {<<kind, epoch>> : kind \in {"schema", "data"},
                                epoch \in 1..MaxCommit}
Artifacts == {"checkpoint", "catalog"}
Transaction(epoch) == {<<"schema", epoch>>, <<"data", epoch>>}
Prefix(epoch) == {fragment \in Fragments : fragment[2] <= epoch}
Suffix(base, last) == Prefix(last) \ Prefix(base)

VARIABLES live, disk, os, job, observed, pin, lease, execution, admitted, frontendBusy
vars == <<live, disk, os, job, observed, pin, lease, execution, admitted, frontendBusy>>

Init ==
    /\ live \in [mode : {"running"}, durability : DurabilityModes,
                  epoch : {0}, active : {0}, pending : {0}, used : {FALSE},
                  crashed : {FALSE}]
    /\ disk = [head |-> 0, status |-> "complete",
                base |-> [g \in Generations |-> 0],
                wals |-> [g \in Generations |-> {}],
                artifacts |-> [g \in Generations |->
                                    IF g = 0 THEN Artifacts ELSE {}],
                present |-> {0}]
    /\ os = [wals |-> disk.wals, artifacts |-> disk.artifacts]
    /\ job = [phase |-> "idle", base |-> 0, end |-> 0, selection |-> 0,
                resume |-> "replaying", adoption |-> "none", readBusy |-> FALSE,
                manualBarrier |-> "none", manualUnsafe |-> FALSE]
    /\ observed = [ack |-> 0, barrier |-> 0, checkpoint |-> 0]
    /\ pin = [generation |-> 0, epoch |-> -1]
    /\ lease = FALSE
    /\ execution = FALSE
    /\ admitted = TRUE
    /\ frontendBusy = FALSE

FullPrefix(generation, wals) ==
    CHOOSE last \in disk.base[generation]..MaxCommit :
        /\ Suffix(disk.base[generation], last) \subseteq wals[generation]
        /\ \A other \in disk.base[generation]..MaxCommit :
               Suffix(disk.base[generation], other) \subseteq wals[generation]
                   => other <= last

HasClosure(generation) ==
    /\ generation \in disk.present
    /\ disk.artifacts[generation] = Artifacts

Recoverable(generation) ==
    /\ HasClosure(generation)
    /\ disk.wals[generation] =
           Suffix(disk.base[generation], FullPrefix(generation, disk.wals))

BeginWrite ==
    /\ live.mode = "running"
    /\ live.pending = 0
    /\ live.epoch < MaxCommit
    /\ job.phase \notin {"ready", "selecting", "selected"}
    /\ live' = [live EXCEPT !.pending = live.epoch + 1]
    /\ os' = [os EXCEPT !.wals[live.active] =
                          @ \cup {<<"schema", live.epoch + 1>>}]
    /\ UNCHANGED <<disk, job, observed, pin, lease, execution, admitted, frontendBusy>>

FinishWrite(reply) ==
    /\ live.mode = "running"
    /\ live.pending = live.epoch + 1
    /\ LET complete == os.wals[live.active] \cup
                           (IF SplitTransaction THEN {}
                            ELSE {<<"data", live.pending>>})
           synchronous == live.durability = "SyncOnEveryWrite"
       IN /\ os' = [os EXCEPT !.wals[live.active] = complete]
          /\ disk' = IF synchronous
                     THEN [disk EXCEPT !.wals[live.active] = complete]
                     ELSE disk
          /\ live' = [live EXCEPT !.epoch = live.pending, !.pending = 0]
          /\ observed' = [observed EXCEPT
                             !.ack = IF reply THEN live.pending ELSE @,
                             !.barrier = IF synchronous THEN live.pending
                                         ELSE @]
    /\ UNCHANGED <<job, pin, lease, execution, admitted, frontendBusy>>

Capture ==
    /\ live.mode = "running"
    /\ live.pending = 0
    /\ admitted
    /\ ~live.used
    /\ job.manualBarrier # "waiting"
    /\ live.active = 0
    /\ live' = [live EXCEPT !.used = TRUE]
    /\ job' = [job EXCEPT !.phase = "building", !.base = live.epoch,
                          !.end = live.epoch]
    /\ lease' = TRUE
    /\ UNCHANGED <<disk, os, observed, pin, admitted, frontendBusy>>

    /\ execution' = TRUE

BuildBase ==
    /\ live.mode = "running"
    /\ job.phase = "building"
    /\ disk' = [disk EXCEPT !.base[1] = job.base,
                            !.present = @ \cup {1}]
    /\ os' = [os EXCEPT !.artifacts[1] = Artifacts]
    /\ job' = [job EXCEPT !.phase = "replaying"]
    /\ UNCHANGED <<live, observed, pin, lease, execution, admitted, frontendBusy>>

ReplayOne ==
    /\ live.mode = "running"
    /\ job.phase = "replaying"
    /\ live.pending = 0
    /\ job.end < live.epoch
    /\ os' = [os EXCEPT !.wals[1] =
                          @ \cup (IF DropSuffix THEN {}
                                  ELSE Transaction(job.end + 1))]
    /\ job' = [job EXCEPT !.end = @ + 1]
    /\ UNCHANGED <<live, disk, observed, pin, lease, execution, admitted, frontendBusy>>

SyncCandidate ==
    /\ live.mode = "running"
    /\ job.phase = "replaying"
    /\ job.end <= live.epoch
    /\ disk' = [disk EXCEPT !.artifacts[1] = os.artifacts[1],
                            !.wals[1] = os.wals[1]]
    /\ job' = [job EXCEPT !.phase = "sealed"]
    /\ UNCHANGED <<live, os, observed, pin, lease, execution, admitted, frontendBusy>>

ResumeReplay ==
    /\ live.mode = "running"
    /\ job.phase = "sealed"
    /\ live.pending = 0
    /\ job.end < live.epoch
    /\ job' = [job EXCEPT !.phase = "replaying"]
    /\ UNCHANGED <<live, disk, os, observed, pin, lease, execution, admitted, frontendBusy>>

FreezeCompleteIdentity ==
    /\ live.mode = "running"
    /\ job.phase = "sealed"
    /\ live.pending = 0
    /\ (SelectStalePrefix \/ job.end = live.epoch)
    /\ job' = [job EXCEPT !.phase = "ready"]
    /\ UNCHANGED <<live, disk, os, observed, pin, lease, execution, admitted, frontendBusy>>

BeginSelection ==
    /\ live.mode = "running"
    /\ live.pending = 0
    /\ (SelectStalePrefix \/ job.end = live.epoch)
    /\ (job.phase = "ready"
          \/ (SelectBeforeSync /\ job.phase = "replaying"))
    /\ job' = [job EXCEPT !.phase = "selecting", !.selection = live.epoch]
    /\ UNCHANGED <<live, disk, os, observed, pin, lease, execution, admitted, frontendBusy>>

PersistSelector(reply) ==
    /\ live.mode = "running"
    /\ job.phase = "selecting"
    /\ disk' = [disk EXCEPT !.head = 1, !.status = "complete"]
    /\ job' = [job EXCEPT !.phase = "selected"]
    /\ observed' = [observed EXCEPT
                       !.barrier = job.selection,
                       !.checkpoint = IF reply THEN job.selection ELSE @]
    /\ UNCHANGED <<live, os, pin, lease, admitted, frontendBusy>>

    /\ execution' = FALSE

Adopt(frontend) ==
    /\ live.mode = "running"
    /\ job.phase = "selected"
    /\ (frontend = "write" \/ ~frontendBusy \/ ReadAdoptWhileBusy)
    /\ live' = [live EXCEPT !.active = 1]
    /\ job' = [job EXCEPT !.phase = "retirementPending", !.adoption = frontend,
                          !.readBusy = frontend = "read" /\ frontendBusy]
    /\ UNCHANGED <<disk, os, observed, pin, lease, execution, admitted, frontendBusy>>

AdmitRetirement ==
    /\ live.mode = "running"
    /\ job.phase = "retirementPending"
    /\ admitted
    /\ job' = [job EXCEPT !.phase = "retiring"]
    /\ execution' = TRUE
    /\ UNCHANGED <<live, disk, os, observed, pin, lease, admitted, frontendBusy>>

DeferRetirement ==
    /\ live.mode = "running"
    /\ job.phase = "retiring"
    /\ ~admitted
    /\ job' = [job EXCEPT !.phase = "retirementPending", !.resume = "retiring"]
    /\ execution' = KeepExecutionOnPause
    /\ lease' = ~LoseMemoryOnPause
    /\ UNCHANGED <<live, disk, os, observed, pin, admitted, frontendBusy>>

Retire ==
    /\ live.mode = "running"
    /\ job.phase = "retiring"
    /\ job' = [job EXCEPT !.phase = "releasing"]
    /\ lease' = IF ReleaseLeaseBeforeCleanup THEN FALSE ELSE lease
    /\ execution' = IF ReleaseLeaseBeforeCleanup THEN FALSE ELSE execution
    /\ UNCHANGED <<live, disk, os, observed, pin, admitted, frontendBusy>>

FinishRelease ==
    /\ live.mode = "running"
    /\ job.phase = "releasing"
    /\ job' = [job EXCEPT !.phase = "done"]
    /\ lease' = FALSE
    /\ execution' = FALSE
    /\ UNCHANGED <<live, disk, os, observed, pin, admitted, frontendBusy>>

PauseCandidate ==
    /\ live.mode = "running"
    /\ job.phase \in {"replaying", "sealed"}
    /\ ~admitted
    /\ job' = [job EXCEPT !.phase = "pending", !.resume = job.phase]
    /\ execution' = KeepExecutionOnPause
    /\ lease' = ~LoseMemoryOnPause
    /\ UNCHANGED <<live, disk, os, observed, pin, admitted, frontendBusy>>

ResumeCandidate ==
    /\ live.mode = "running"
    /\ job.phase = "pending"
    /\ admitted
    /\ job' = [job EXCEPT !.phase = job.resume]
    /\ execution' = TRUE
    /\ UNCHANGED <<live, disk, os, observed, pin, lease, admitted, frontendBusy>>

ChangeAdmission ==
    /\ live.mode = "running"
    /\ admitted' = ~admitted
    /\ UNCHANGED <<live, disk, os, job, observed, pin, lease, execution, frontendBusy>>

ChangeFrontendAvailability ==
    /\ live.mode = "running"
    /\ frontendBusy' = ~frontendBusy
    /\ UNCHANGED <<live, disk, os, job, observed, pin, lease, execution, admitted>>

PinReader ==
    /\ live.mode = "running"
    /\ live.pending = 0
    /\ pin.epoch = -1
    /\ pin' = [generation |-> live.active, epoch |-> live.epoch]
    /\ UNCHANGED <<live, disk, os, job, observed, lease, execution, admitted, frontendBusy>>

ReleaseReader ==
    /\ pin.epoch # -1
    /\ pin' = [pin EXCEPT !.epoch = -1]
    /\ UNCHANGED <<live, disk, os, job, observed, lease, execution, admitted, frontendBusy>>

ReclaimOld ==
    /\ live.mode = "running"
    /\ disk.head = 1
    /\ job.phase \in {"retiring", "done"}
    /\ 0 \in disk.present
    /\ (ReclaimPinned \/ pin.epoch = -1 \/ pin.generation # 0)
    /\ disk' = [disk EXCEPT !.present = @ \ {0}, !.wals[0] = {},
                            !.artifacts[0] = {}]
    /\ os' = [os EXCEPT !.wals[0] = {}, !.artifacts[0] = {}]
    /\ UNCHANGED <<live, job, observed, pin, lease, execution, admitted, frontendBusy>>

Cancel ==
    /\ live.mode = "running"
    /\ job.phase \in {"building", "replaying", "sealed", "ready", "pending"}
    /\ job' = [job EXCEPT !.phase = "discarding"]
    /\ lease' = IF ReleaseLeaseBeforeCleanup THEN FALSE ELSE lease
    /\ execution' = IF ReleaseLeaseBeforeCleanup THEN FALSE ELSE execution
    /\ UNCHANGED <<live, disk, os, observed, pin, admitted, frontendBusy>>

CleanupPrivate ==
    /\ live.mode = "running"
    /\ job.phase = "discarding"
    /\ disk' = [disk EXCEPT !.present = @ \ {1}, !.wals[1] = {},
                            !.artifacts[1] = {}]
    /\ os' = [os EXCEPT !.wals[1] = {}, !.artifacts[1] = {}]
    /\ job' = [job EXCEPT !.phase = "cancelled"]
    /\ lease' = LeakLease
    /\ UNCHANGED <<live, observed, pin, admitted, frontendBusy>>

    /\ execution' = FALSE

PowerLoss ==
    /\ live.mode = "running"
    /\ ~live.crashed
    /\ \E oldExtra \in SUBSET (os.wals[0] \ disk.wals[0]),
           candidateExtra \in SUBSET (os.wals[1] \ disk.wals[1]),
           catalogExtra \in SUBSET (os.artifacts[1] \ disk.artifacts[1]),
           status \in IF job.phase = "selecting"
                     THEN {"complete", "uncertain"} ELSE {"complete"} :
        LET surviving == [g \in Generations |-> disk.wals[g] \cup
                           (IF g = 0 THEN oldExtra ELSE candidateExtra)]
            closure == [g \in Generations |-> disk.artifacts[g] \cup
                           (IF g = 1 THEN catalogExtra ELSE {})]
        IN /\ disk' = [disk EXCEPT !.wals = surviving,
                                   !.artifacts = closure, !.status = status]
           /\ os' = [wals |-> surviving, artifacts |-> closure]
    /\ live' = [live EXCEPT !.mode = "crashed", !.crashed = TRUE,
                            !.pending = 0]
    /\ job' = [job EXCEPT !.phase = "crashed"]
    /\ pin' = [pin EXCEPT !.epoch = -1]
    /\ lease' = FALSE
    /\ UNCHANGED <<observed, admitted, frontendBusy>>

    /\ execution' = FALSE

Reopen ==
    /\ live.mode = "crashed"
    /\ live' = IF disk.status = "complete" /\ Recoverable(disk.head)
               THEN [live EXCEPT !.mode = "running", !.active = disk.head,
                                 !.epoch = FullPrefix(disk.head, disk.wals)]
               ELSE [live EXCEPT !.mode = "failed"]
    /\ job' = [job EXCEPT !.phase = "done"]
    /\ UNCHANGED <<disk, os, observed, pin, lease, execution, admitted, frontendBusy>>

RequestManualBarrier ==
    /\ live.mode = "running"
    /\ job.manualBarrier # "waiting"
    /\ job' = [job EXCEPT !.manualBarrier = "waiting"]
    /\ UNCHANGED <<live, disk, os, observed, pin, lease, execution, admitted, frontendBusy>>

CompleteManualBarrier ==
    /\ live.mode = "running"
    /\ job.manualBarrier = "waiting"
    /\ (ManualBeforeCleanup \/ (job.phase \in {"idle", "done", "cancelled"} /\ ~lease))
    /\ job' = [job EXCEPT !.manualBarrier = "complete",
                          !.manualUnsafe = job.phase \notin {"idle", "done", "cancelled"} \/ lease]
    /\ UNCHANGED <<live, disk, os, observed, pin, lease, execution, admitted, frontendBusy>>

Next == BeginWrite \/ (\E reply \in BOOLEAN : FinishWrite(reply))
        \/ Capture \/ BuildBase \/ ReplayOne \/ SyncCandidate
        \/ ResumeReplay \/ FreezeCompleteIdentity
        \/ BeginSelection \/ (\E reply \in BOOLEAN : PersistSelector(reply))
        \/ (\E frontend \in {"write", "read"} : Adopt(frontend))
        \/ AdmitRetirement \/ DeferRetirement \/ Retire \/ FinishRelease
        \/ PauseCandidate \/ ResumeCandidate \/ ChangeAdmission
        \/ ChangeFrontendAvailability \/ PinReader \/ ReleaseReader \/ ReclaimOld
        \/ Cancel \/ CleanupPrivate \/ RequestManualBarrier \/ CompleteManualBarrier
        \/ PowerLoss \/ Reopen

TypeOK ==
    /\ live \in [mode : {"running", "crashed", "failed"},
                  durability : DurabilityModes, epoch : Epochs,
                  active : Generations, pending : Epochs,
                  used : BOOLEAN, crashed : BOOLEAN]
    /\ disk \in [head : Generations, status : {"complete", "uncertain"},
                  base : [Generations -> Epochs],
                  wals : [Generations -> SUBSET Fragments],
                  artifacts : [Generations -> SUBSET Artifacts],
                  present : SUBSET Generations]
    /\ os \in [wals : [Generations -> SUBSET Fragments],
                artifacts : [Generations -> SUBSET Artifacts]]
    /\ job \in [phase : {"idle", "building", "replaying", "sealed", "ready",
                         "selecting", "selected", "pending", "retirementPending", "retiring", "releasing", "done",
                         "discarding", "cancelled", "crashed"},
                 base : Epochs, end : Epochs, selection : Epochs,
                 resume : {"replaying", "sealed", "retiring"}, adoption : {"none", "write", "read"},
                 readBusy : BOOLEAN, manualBarrier : {"none", "waiting", "complete"},
                 manualUnsafe : BOOLEAN]
    /\ observed \in [ack : Epochs, barrier : Epochs, checkpoint : Epochs]
    /\ pin \in [generation : Generations, epoch : (-1)..MaxCommit]
    /\ lease \in BOOLEAN
    /\ execution \in BOOLEAN
    /\ admitted \in BOOLEAN
    /\ frontendBusy \in BOOLEAN

SelectorReferencesSynchronizedClosure ==
    disk.status = "complete" => HasClosure(disk.head)

CandidateBaseStaysPinned ==
    1 \in disk.present => disk.base[1] = job.base

SelectedContainsCapturedPrefix ==
    disk.head = 1 /\ disk.status = "complete" =>
        Suffix(disk.base[1], job.selection) \subseteq disk.wals[1]

CompletedBarriersRemainReachable ==
    \E generation \in Generations :
        /\ HasClosure(generation)
        /\ FullPrefix(generation, disk.wals) >= observed.barrier

ServingContainsWholeTransactions ==
    live.mode = "running" =>
        Suffix(disk.base[live.active], live.epoch) \subseteq os.wals[live.active]

RecoveredHonorsBarriers ==
    live.mode = "running" /\ live.crashed => live.epoch >= observed.barrier

PinnedGenerationIsRetained ==
    pin.epoch # -1 =>
        /\ HasClosure(pin.generation)
        /\ Suffix(disk.base[pin.generation], pin.epoch)
               \subseteq os.wals[pin.generation]

CancelledReleasesLease == job.phase = "cancelled" => ~lease
CandidateOwnsLease ==
    live.mode = "running" /\
        job.phase \in {"building", "replaying", "sealed", "ready", "selecting",
                       "selected", "pending", "retirementPending", "retiring", "releasing", "discarding"} => lease
CancelledPreservesSelector ==
    job.phase \in {"discarding", "cancelled"} => disk.head = 0
UncertainSelectionFailsClosed ==
    disk.status = "uncertain" /\ live.mode # "crashed" => live.mode = "failed"

ParkedYieldsExecution ==
    job.phase \in {"pending", "selected", "retirementPending"} => ~execution
ExecutionOwnsMemory == execution => lease
ActiveStagesOwnExecution ==
    live.mode = "running" /\
        job.phase \in {"building", "replaying", "sealed", "ready", "selecting", "retiring", "releasing"}
        => execution
ReadAdoptRequiresWriterGate == ~job.readBusy
ManualBarrierWaitsForCleanup == ~job.manualUnsafe

(* Witness controls prove ordinary writes can overlap off-gate destruction. *)
NoWriterDuringDiscard == ~(live.pending # 0 /\ job.phase = "discarding")
NoWriterDuringRelease == ~(live.pending # 0 /\ job.phase = "releasing")

PendingResumesPreparation ==
    job.phase = "pending" => job.resume \in {"replaying", "sealed"}

(* Reachability controls deliberately violate these witness-only assertions. *)
PausedPrefixWasNeverResumed ==
    ~(job.phase = "sealed" /\ job.resume = "sealed" /\ execution)
DeferredRetirementWasNeverResumed ==
    ~(job.phase = "retiring" /\ job.resume = "retiring")
NoReadTriggeredAdoption == job.adoption # "read"

(* Witness-only controls distinguish permitted loss from strong durability. *)
AllAcknowledgementsRecover ==
    live.mode = "running" /\ live.crashed => live.epoch >= observed.ack
AllRecoveredTransactionsWereAcknowledged ==
    live.mode = "running" /\ live.crashed => live.epoch <= observed.ack
=============================================================================
