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
(***************************************************************************)

CONSTANTS MaxCommit, DurabilityModes,
          SelectBeforeSync, DropSuffix, SplitTransaction, ReclaimPinned,
          LeakLease

ASSUME /\ MaxCommit \in Nat \ {0}
       /\ DurabilityModes \subseteq {"SyncOnEveryWrite", "SyncOnCheckpoint"}
       /\ DurabilityModes # {}
       /\ {SelectBeforeSync, DropSuffix, SplitTransaction,
             ReclaimPinned, LeakLease} \subseteq BOOLEAN

Generations == {0, 1}
Epochs == 0..MaxCommit
Fragments == {<<kind, epoch>> : kind \in {"schema", "data"},
                                epoch \in 1..MaxCommit}
Artifacts == {"checkpoint", "catalog"}
Transaction(epoch) == {<<"schema", epoch>>, <<"data", epoch>>}
Prefix(epoch) == {fragment \in Fragments : fragment[2] <= epoch}
Suffix(base, last) == Prefix(last) \ Prefix(base)

VARIABLES live, disk, os, job, observed, pin, lease
vars == <<live, disk, os, job, observed, pin, lease>>

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
    /\ job = [phase |-> "idle", base |-> 0, end |-> 0, selection |-> 0]
    /\ observed = [ack |-> 0, barrier |-> 0, checkpoint |-> 0]
    /\ pin = [generation |-> 0, epoch |-> -1]
    /\ lease = FALSE

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
    /\ UNCHANGED <<disk, job, observed, pin, lease>>

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
    /\ UNCHANGED <<job, pin, lease>>

Capture ==
    /\ live.mode = "running"
    /\ live.pending = 0
    /\ ~live.used
    /\ live.active = 0
    /\ live' = [live EXCEPT !.used = TRUE]
    /\ job' = [job EXCEPT !.phase = "building", !.base = live.epoch,
                          !.end = live.epoch]
    /\ lease' = TRUE
    /\ UNCHANGED <<disk, os, observed, pin>>

BuildBase ==
    /\ live.mode = "running"
    /\ job.phase = "building"
    /\ disk' = [disk EXCEPT !.base[1] = job.base,
                            !.present = @ \cup {1}]
    /\ os' = [os EXCEPT !.artifacts[1] = Artifacts]
    /\ job' = [job EXCEPT !.phase = "replaying"]
    /\ UNCHANGED <<live, observed, pin, lease>>

ReplayOne ==
    /\ live.mode = "running"
    /\ job.phase = "replaying"
    /\ live.pending = 0
    /\ job.end < live.epoch
    /\ os' = [os EXCEPT !.wals[1] =
                          @ \cup (IF DropSuffix THEN {}
                                  ELSE Transaction(job.end + 1))]
    /\ job' = [job EXCEPT !.end = @ + 1]
    /\ UNCHANGED <<live, disk, observed, pin, lease>>

SyncCandidate ==
    /\ live.mode = "running"
    /\ job.phase = "replaying"
    /\ live.pending = 0
    /\ job.end = live.epoch
    /\ disk' = [disk EXCEPT !.artifacts[1] = os.artifacts[1],
                            !.wals[1] = os.wals[1]]
    /\ job' = [job EXCEPT !.phase = "ready"]
    /\ UNCHANGED <<live, os, observed, pin, lease>>

BeginSelection ==
    /\ live.mode = "running"
    /\ live.pending = 0
    /\ job.end = live.epoch
    /\ (job.phase = "ready"
          \/ (SelectBeforeSync /\ job.phase = "replaying"))
    /\ job' = [job EXCEPT !.phase = "selecting", !.selection = live.epoch]
    /\ UNCHANGED <<live, disk, os, observed, pin, lease>>

PersistSelector(reply) ==
    /\ live.mode = "running"
    /\ job.phase = "selecting"
    /\ disk' = [disk EXCEPT !.head = 1, !.status = "complete"]
    /\ job' = [job EXCEPT !.phase = "selected"]
    /\ observed' = [observed EXCEPT
                       !.barrier = job.selection,
                       !.checkpoint = IF reply THEN job.selection ELSE @]
    /\ UNCHANGED <<live, os, pin, lease>>

Adopt ==
    /\ live.mode = "running"
    /\ job.phase = "selected"
    /\ live' = [live EXCEPT !.active = 1]
    /\ job' = [job EXCEPT !.phase = "retiring"]
    /\ UNCHANGED <<disk, os, observed, pin, lease>>

Retire ==
    /\ live.mode = "running"
    /\ job.phase = "retiring"
    /\ job' = [job EXCEPT !.phase = "done"]
    /\ lease' = FALSE
    /\ UNCHANGED <<live, disk, os, observed, pin>>

PinReader ==
    /\ live.mode = "running"
    /\ live.pending = 0
    /\ pin.epoch = -1
    /\ pin' = [generation |-> live.active, epoch |-> live.epoch]
    /\ UNCHANGED <<live, disk, os, job, observed, lease>>

ReleaseReader ==
    /\ pin.epoch # -1
    /\ pin' = [pin EXCEPT !.epoch = -1]
    /\ UNCHANGED <<live, disk, os, job, observed, lease>>

ReclaimOld ==
    /\ live.mode = "running"
    /\ disk.head = 1
    /\ job.phase \in {"retiring", "done"}
    /\ 0 \in disk.present
    /\ (ReclaimPinned \/ pin.epoch = -1 \/ pin.generation # 0)
    /\ disk' = [disk EXCEPT !.present = @ \ {0}, !.wals[0] = {},
                            !.artifacts[0] = {}]
    /\ os' = [os EXCEPT !.wals[0] = {}, !.artifacts[0] = {}]
    /\ UNCHANGED <<live, job, observed, pin, lease>>

Cancel ==
    /\ live.mode = "running"
    /\ job.phase \in {"building", "replaying", "ready"}
    /\ job' = [job EXCEPT !.phase = "cancelling"]
    /\ UNCHANGED <<live, disk, os, observed, pin, lease>>

CleanupPrivate ==
    /\ live.mode = "running"
    /\ job.phase = "cancelling"
    /\ disk' = [disk EXCEPT !.present = @ \ {1}, !.wals[1] = {},
                            !.artifacts[1] = {}]
    /\ os' = [os EXCEPT !.wals[1] = {}, !.artifacts[1] = {}]
    /\ job' = [job EXCEPT !.phase = "cancelled"]
    /\ lease' = LeakLease
    /\ UNCHANGED <<live, observed, pin>>

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
    /\ UNCHANGED observed

Reopen ==
    /\ live.mode = "crashed"
    /\ live' = IF disk.status = "complete" /\ Recoverable(disk.head)
               THEN [live EXCEPT !.mode = "running", !.active = disk.head,
                                 !.epoch = FullPrefix(disk.head, disk.wals)]
               ELSE [live EXCEPT !.mode = "failed"]
    /\ job' = [job EXCEPT !.phase = "done"]
    /\ UNCHANGED <<disk, os, observed, pin, lease>>

Next == BeginWrite \/ (\E reply \in BOOLEAN : FinishWrite(reply))
        \/ Capture \/ BuildBase \/ ReplayOne \/ SyncCandidate
        \/ BeginSelection \/ (\E reply \in BOOLEAN : PersistSelector(reply))
        \/ Adopt \/ Retire \/ PinReader \/ ReleaseReader \/ ReclaimOld
        \/ Cancel \/ CleanupPrivate \/ PowerLoss \/ Reopen

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
    /\ job \in [phase : {"idle", "building", "replaying", "ready",
                         "selecting", "selected", "retiring", "done",
                         "cancelling", "cancelled", "crashed"},
                 base : Epochs, end : Epochs, selection : Epochs]
    /\ observed \in [ack : Epochs, barrier : Epochs, checkpoint : Epochs]
    /\ pin \in [generation : Generations, epoch : (-1)..MaxCommit]
    /\ lease \in BOOLEAN

SelectorReferencesSynchronizedClosure ==
    disk.status = "complete" => HasClosure(disk.head)

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
        job.phase \in {"building", "replaying", "ready", "selecting",
                       "selected", "retiring", "cancelling"} => lease
CancelledPreservesSelector ==
    job.phase \in {"cancelling", "cancelled"} => disk.head = 0
UncertainSelectionFailsClosed ==
    disk.status = "uncertain" /\ live.mode # "crashed" => live.mode = "failed"

(* Witness-only controls distinguish permitted loss from strong durability. *)
AllAcknowledgementsRecover ==
    live.mode = "running" /\ live.crashed => live.epoch >= observed.ack
AllRecoveredTransactionsWereAcknowledged ==
    live.mode = "running" /\ live.crashed => live.epoch <= observed.ack
=============================================================================
