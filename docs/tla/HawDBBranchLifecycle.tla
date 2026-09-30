----------------------- MODULE HawDBBranchLifecycle -----------------------
EXTENDS Integers, FiniteSets

(* Three branches model main -> child -> grandchild. A root atomically names
   schema and data, and is never a mutable directory generation. *)
CONSTANTS PublishEarly, MutateParent, ForgetCreating, SkipSweepRecheck,
          DeleteLeased, AllowSharedWriter, AllowUnsyncedFork, SplitSchemaData

Branches == 0..2
NoBranch == -1
Roots == 0..3
NoRoot == -1
States == {"Absent", "Creating", "Ready", "Deleting", "Deleted"}
Policy(b) == IF b = 1 THEN "Relaxed" ELSE "Sync"
Parent(b) == IF b = 1 THEN 0 ELSE 1
Closure(r) == CASE r = NoRoot -> {}
               [] r = 0 -> {"root0", "checkpoint0"}
               [] r = 1 -> {"root1", "checkpoint1"}
               [] r = 2 -> {"root2", "checkpoint0", "child-wal"}
               [] r = 3 -> {"root3", "checkpoint0", "child-wal", "grandchild-wal"}
Data(r) == CASE r = 0 -> {"seed"}
            [] r = 1 -> {"seed", "main-write"}
            [] r = 2 -> {"seed", "child-write"}
            [] r = 3 -> {"seed", "child-write", "grandchild-write"}
Schema(r) == CASE r = 0 -> {"id"}
              [] r = 1 -> {"id", "main-column"}
              [] r = 2 -> {"id", "child-column"}
              [] r = 3 -> {"id", "child-column", "grandchild-column"}
Files == UNION {Closure(r): r \in Roots}
Live(s, b) == s.state[b] \in {"Creating", "Ready", "Deleting"}
NextRoot(s, b) == CASE b = 0 /\ s.head[0] = 0 -> 1
                       [] b = 1 /\ s.head[1] = 0 -> 2
                       [] b = 2 /\ s.head[2] = 2 -> 3
                       [] OTHER -> NoRoot

VARIABLE s
vars == <<s>>

HeadFiles == UNION {IF Live(s, b) /\ s.head[b] # NoRoot THEN Closure(s.head[b]) ELSE {}: b \in Branches}
DurableHeadFiles == UNION {IF Live(s, b) /\ s.durableHead[b] # NoRoot THEN Closure(s.durableHead[b]) ELSE {}: b \in Branches}
BaseFiles == UNION {IF Live(s, b) /\ s.base[b] # NoRoot THEN Closure(s.base[b]) ELSE {}: b \in Branches}
LocalPinFiles == UNION {IF s.localOpen[b] THEN Closure(s.head[b]) ELSE {}: b \in Branches}
ForeignPinFiles == UNION {IF s.foreignOpen[b] THEN Closure(s.foreignPin[b]) ELSE {}: b \in Branches}
PinFiles == LocalPinFiles \cup ForeignPinFiles
CandidateFiles == UNION {Closure(s.candidate[b]): b \in Branches}
ProtectedBases == IF ForgetCreating
                     THEN UNION {IF s.state[b] \in {"Ready", "Deleting"} /\ s.base[b] # NoRoot
                                    THEN Closure(s.base[b]) ELSE {}: b \in Branches}
                     ELSE BaseFiles
Protected == HeadFiles \cup DurableHeadFiles \cup ProtectedBases \cup PinFiles \cup CandidateFiles

Init == s = [
  state |-> [b \in Branches |-> IF b = 0 THEN "Ready" ELSE "Absent"],
  head |-> [b \in Branches |-> IF b = 0 THEN 0 ELSE NoRoot],
  durableHead |-> [b \in Branches |-> IF b = 0 THEN 0 ELSE NoRoot],
  base |-> [b \in Branches |-> NoRoot], capturedSource |-> [b \in Branches |-> NoRoot],
  candidate |-> [b \in Branches |-> NoRoot], armed |-> [b \in Branches |-> FALSE],
  expectedData |-> [b \in Branches |-> IF b = 0 THEN Data(0) ELSE {}],
  expectedSchema |-> [b \in Branches |-> IF b = 0 THEN Schema(0) ELSE {}],
  acknowledged |-> [b \in Branches |-> NoRoot],
  localOpen |-> [b \in Branches |-> FALSE], foreignOpen |-> [b \in Branches |-> FALSE],
  foreignPin |-> [b \in Branches |-> NoRoot],
  active |-> NoBranch, durable |-> Closure(0), flushed |-> Closure(0), marked |-> {}, online |-> TRUE,
  switchFailed |-> FALSE, failedSource |-> NoBranch,
  recoveredCreate |-> FALSE, abortedCreate |-> FALSE, recoveredDelete |-> FALSE,
  relaxedLossObserved |-> FALSE]

PrepareCreate(c) ==
  /\ s.online /\ c \in {1, 2} /\ s.state[c] = "Absent"
  /\ s.state[Parent(c)] = "Ready" /\ s.candidate[Parent(c)] = NoRoot
  /\ (AllowUnsyncedFork \/ s.head[Parent(c)] = s.durableHead[Parent(c)])
  /\ s' = [s EXCEPT !.state[c] = "Creating", !.base[c] = s.head[Parent(c)],
                    !.capturedSource[c] = s.head[Parent(c)]]
InstallChildHead(c) ==
  /\ s.online /\ c \in {1, 2} /\ s.state[c] = "Creating" /\ s.head[c] = NoRoot
  /\ Closure(s.base[c]) \subseteq s.durable
  /\ s' = [s EXCEPT !.head[c] = s.base[c], !.durableHead[c] = s.base[c],
                    !.expectedData[c] = Data(s.base[c]), !.expectedSchema[c] = Schema(s.base[c])]
PublishCreate(c) == /\ s.online /\ c \in {1, 2} /\ s.state[c] = "Creating" /\ s.head[c] # NoRoot
                    /\ s' = [s EXCEPT !.state[c] = "Ready"]

Select(b) ==
  /\ s.online /\ b \in Branches /\ s.state[b] = "Ready" /\ ~s.foreignOpen[b] /\ s.candidate[b] = NoRoot
  /\ s' = [s EXCEPT !.localOpen = [x \in Branches |-> x = b], !.active = b,
                    !.switchFailed = FALSE, !.failedSource = NoBranch]
SelectBusy(b) ==
  /\ s.online /\ b \in Branches /\ b # s.active /\ s.foreignOpen[b]
  /\ s' = [s EXCEPT !.switchFailed = TRUE, !.failedSource = s.active]
OpenForeign(b) ==
  /\ s.online /\ b \in Branches /\ s.state[b] = "Ready" /\ ~s.foreignOpen[b] /\ s.foreignPin[b] = NoRoot
  /\ (AllowSharedWriter \/ ~s.localOpen[b])
  /\ s' = [s EXCEPT !.foreignOpen[b] = TRUE, !.foreignPin[b] = s.head[b]]
CloseForeign(b) == /\ s.online /\ b \in Branches /\ s.foreignOpen[b]
                   /\ s' = [s EXCEPT !.foreignOpen[b] = FALSE, !.foreignPin[b] = NoRoot]

PrepareWrite(b) ==
  /\ s.online /\ s.active = b /\ s.localOpen[b] /\ s.state[b] = "Ready"
  /\ s.candidate[b] = NoRoot /\ NextRoot(s, b) # NoRoot
  /\ s' = [s EXCEPT !.candidate[b] = NextRoot(s, b), !.armed[b] = FALSE]
(* A transaction can leave fully persisted, still-unreferenced artifacts behind.
   A later write may safely adopt them only while its candidate remains GC-rooted. *)
StageCandidateClosure(b) ==
  /\ s.online /\ b \in Branches /\ s.state[b] = "Ready" /\ s.candidate[b] = NoRoot
  /\ NextRoot(s, b) # NoRoot /\ ~(Closure(NextRoot(s, b)) \subseteq s.durable)
  /\ s' = [s EXCEPT !.durable = @ \cup Closure(NextRoot(s, b)),
                    !.flushed = @ \cup Closure(NextRoot(s, b))]
ArmCandidate(b) ==
  /\ s.online /\ s.candidate[b] # NoRoot /\ ~s.armed[b]
  /\ Closure(s.candidate[b]) \subseteq s.durable
  /\ s' = [s EXCEPT !.armed[b] = TRUE]
FlushObject(b, f) ==
  /\ s.online /\ s.candidate[b] # NoRoot /\ f \in Closure(s.candidate[b]) \ s.flushed
  /\ s' = [s EXCEPT !.flushed = @ \cup {f}]
SyncObject(b, f) ==
  /\ s.online /\ s.candidate[b] # NoRoot /\ f \in Closure(s.candidate[b]) \ s.durable
  /\ s' = [s EXCEPT !.durable = @ \cup {f}, !.flushed = @ \cup {f}]
PublishHead(b) ==
  /\ s.online /\ s.candidate[b] # NoRoot
  /\ (PublishEarly \/ Closure(s.candidate[b]) \subseteq s.flushed)
  /\ (Policy(b) = "Relaxed" \/ Closure(s.candidate[b]) \subseteq s.durable)
  /\ s' = [s EXCEPT !.head[b] = s.candidate[b],
       !.durableHead[b] = IF Policy(b) = "Sync" THEN s.candidate[b] ELSE @,
       !.expectedData[b] = Data(s.candidate[b]),
       !.expectedSchema[b] = IF SplitSchemaData THEN @ ELSE Schema(s.candidate[b]),
       !.acknowledged[b] = s.candidate[b], !.candidate[b] = NoRoot, !.armed[b] = FALSE,
       !.head[0] = IF MutateParent /\ b # 0 THEN s.candidate[b] ELSE @,
       !.expectedData[0] = IF MutateParent /\ b # 0 THEN Data(s.candidate[b]) ELSE @,
       !.expectedSchema[0] = IF MutateParent /\ b # 0 THEN Schema(s.candidate[b]) ELSE @]
SealCheckpoint(b) ==
  /\ s.online /\ s.state[b] = "Ready" /\ s.candidate[b] = NoRoot /\ s.head[b] # s.durableHead[b]
  /\ s' = [s EXCEPT !.durable = @ \cup Closure(s.head[b]), !.flushed = @ \cup Closure(s.head[b]),
                    !.durableHead[b] = s.head[b]]

BeginDelete(b) == /\ s.online /\ b \in {1,2} /\ s.state[b] = "Ready" /\ s.active # b
                  /\ s' = [s EXCEPT !.state[b] = "Deleting"]
FinalizeDelete(b) ==
  /\ s.online /\ b \in {1,2} /\ s.state[b] = "Deleting"
  /\ (DeleteLeased \/ (~s.localOpen[b] /\ ~s.foreignOpen[b] /\ s.candidate[b] = NoRoot))
  /\ s' = [s EXCEPT !.state[b] = "Deleted"]
Mark == /\ s.online /\ s' = [s EXCEPT !.marked = s.durable \ Protected]
Sweep(f) == /\ s.online /\ f \in s.marked \cap s.durable /\ (SkipSweepRecheck \/ f \notin Protected)
            /\ s' = [s EXCEPT !.durable = @ \ {f}, !.flushed = @ \ {f}, !.marked = @ \ {f}]
(* A local process crash releases only its own context. A foreign process can
   keep a pin on the root it admitted, even while this process recovers. *)
ProcessCrash ==
  /\ s.online
  /\ s' = [s EXCEPT !.online = FALSE,
       !.candidate = [b \in Branches |-> NoRoot], !.armed = [b \in Branches |-> FALSE],
       !.localOpen = [b \in Branches |-> FALSE],
       !.active = NoBranch, !.marked = {}, !.switchFailed = FALSE, !.failedSource = NoBranch]
(* Power loss kills every owner and can lose any relaxed, merely flushed head. *)
PowerLoss ==
  /\ s.online
  /\ s' = [s EXCEPT !.online = FALSE, !.head = [b \in Branches |-> s.durableHead[b]],
       !.expectedData = [b \in Branches |-> IF s.durableHead[b] = NoRoot THEN {} ELSE Data(s.durableHead[b])],
       !.expectedSchema = [b \in Branches |-> IF s.durableHead[b] = NoRoot THEN {} ELSE Schema(s.durableHead[b])],
       !.candidate = [b \in Branches |-> NoRoot], !.armed = [b \in Branches |-> FALSE],
       !.localOpen = [b \in Branches |-> FALSE], !.foreignOpen = [b \in Branches |-> FALSE],
       !.foreignPin = [b \in Branches |-> NoRoot], !.active = NoBranch,
       !.flushed = s.durable, !.marked = {}, !.switchFailed = FALSE, !.failedSource = NoBranch,
       !.relaxedLossObserved = @ \/ (s.acknowledged[1] # s.durableHead[1])]
Recover == /\ ~s.online
  /\ s' = [s EXCEPT !.online = TRUE,
       !.state = [b \in Branches |-> CASE @ [b] = "Creating" -> IF s.head[b] = NoRoot THEN "Deleted" ELSE "Ready"
                                         [] @ [b] = "Deleting" -> IF s.foreignOpen[b] THEN "Deleting" ELSE "Deleted"
                                         [] OTHER -> @ [b]],
       !.recoveredCreate = @ \/ (\E b \in {1,2}: s.state[b] = "Creating" /\ s.head[b] # NoRoot),
       !.abortedCreate = @ \/ (\E b \in {1,2}: s.state[b] = "Creating" /\ s.head[b] = NoRoot),
       !.recoveredDelete = @ \/ (\E b \in {1,2}: s.state[b] = "Deleting" /\ ~s.foreignOpen[b])]

Next == (\E c \in {1,2}: PrepareCreate(c) \/ InstallChildHead(c) \/ PublishCreate(c))
        \/ (\E b \in Branches: Select(b) \/ SelectBusy(b) \/ OpenForeign(b) \/ CloseForeign(b)
             \/ PrepareWrite(b) \/ StageCandidateClosure(b) \/ ArmCandidate(b) \/ PublishHead(b)
             \/ SealCheckpoint(b) \/ BeginDelete(b) \/ FinalizeDelete(b)
             \/ (\E f \in Files: FlushObject(b,f) \/ SyncObject(b,f)))
        \/ Mark \/ (\E f \in Files: Sweep(f)) \/ ProcessCrash \/ PowerLoss \/ Recover
Spec == Init /\ [][Next]_vars

TypeOK == /\ s.state \in [Branches -> States] /\ s.head \in [Branches -> Roots \cup {NoRoot}]
          /\ s.durableHead \in [Branches -> Roots \cup {NoRoot}] /\ s.base \in [Branches -> Roots \cup {NoRoot}]
          /\ s.capturedSource \in [Branches -> Roots \cup {NoRoot}] /\ s.candidate \in [Branches -> Roots \cup {NoRoot}]
          /\ s.armed \in [Branches -> BOOLEAN]
          /\ s.expectedData \in [Branches -> SUBSET {"seed", "main-write", "child-write", "grandchild-write"}]
          /\ s.expectedSchema \in [Branches -> SUBSET {"id", "main-column", "child-column", "grandchild-column"}]
          /\ s.localOpen \in [Branches -> BOOLEAN] /\ s.foreignOpen \in [Branches -> BOOLEAN]
          /\ s.foreignPin \in [Branches -> Roots \cup {NoRoot}]
          /\ s.active \in Branches \cup {NoBranch} /\ s.durable \subseteq Files /\ s.flushed \subseteq Files /\ s.marked \subseteq Files
          /\ s.online \in BOOLEAN /\ s.switchFailed \in BOOLEAN /\ s.failedSource \in Branches \cup {NoBranch}
          /\ s.recoveredCreate \in BOOLEAN /\ s.abortedCreate \in BOOLEAN /\ s.recoveredDelete \in BOOLEAN /\ s.relaxedLossObserved \in BOOLEAN
DurableHeadsComplete == DurableHeadFiles \subseteq s.durable
VisibleHeadsFlushed == HeadFiles \subseteq s.flushed
CreateSourcesRetained == BaseFiles \subseteq s.durable
LineageImmutable == \A b \in Branches: s.base[b] = s.capturedSource[b]
BranchStateAtomic == \A b \in Branches: Live(s,b) /\ s.head[b] # NoRoot => /\ s.expectedData[b] = Data(s.head[b]) /\ s.expectedSchema[b] = Schema(s.head[b])
ParentImmutableUnderDescendantWrites == s.head[0] \in {0,1}
DeletedHasNoLease == \A b \in Branches: s.state[b] = "Deleted" => ~s.localOpen[b] /\ ~s.foreignOpen[b]
SingleWriterPerBranch == \A b \in Branches: ~(s.localOpen[b] /\ s.foreignOpen[b])
SourcePreservedOnBusy == s.switchFailed => s.active = s.failedSource
SyncAcknowledgementsDurable == \A b \in Branches: Policy(b) = "Sync" /\ s.state[b] # "Deleted" /\ s.acknowledged[b] # NoRoot => Closure(s.acknowledged[b]) \subseteq s.durable
ReadyHasDurableHead == \A b \in Branches: s.state[b] = "Ready" => s.head[b] # NoRoot /\ s.durableHead[b] # NoRoot
ArmedCandidatesComplete == \A b \in Branches: s.armed[b] => Closure(s.candidate[b]) \subseteq s.durable
ForeignPinsFlushed == ForeignPinFiles \subseteq s.flushed
NoCreateRecovery == ~s.recoveredCreate
NoAbortedCreate == ~s.abortedCreate
NoDeleteRecovery == ~s.recoveredDelete
NoConcurrentBranchWriters == \A a,b \in Branches: a # b => ~(s.localOpen[a] /\ s.foreignOpen[b])
NoNestedFork == s.state[2] # "Ready"
NoLiveDescendantAfterParentDelete == ~(s.state[1] = "Deleted" /\ s.state[2] = "Ready")
NoRelaxedAcknowledgementLoss == ~s.relaxedLossObserved
=============================================================================
