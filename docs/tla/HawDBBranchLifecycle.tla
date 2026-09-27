----------------------- MODULE HawDBBranchLifecycle -----------------------
EXTENDS Integers, FiniteSets

CONSTANTS PublishEarly, MutateParent, ForgetCreating, SkipSweepRecheck,
          DeleteLeased, OpenExpired

(* Two independent writers and one non-reusable child identity. Roots stand
   for complete, immutable checkpoint/WAL closures, not individual rows.
   Parent checkpoint 1 replaces checkpoint 0; child roots retain their base.
   Per-object persistence permits crashes before a complete closure exists.
   Catalog/selector replacement is atomic; filesystem refinement is separate. *)
Branches == {0, 1}
Roots == 0..3
NoRoot == -1
States == {"Absent", "Creating", "Ready", "Expired", "Deleting", "Deleted"}
Closure(r) == CASE r = NoRoot -> {}
               [] r = 0 -> {"root0", "checkpoint0"}
               [] r = 1 -> {"root1", "checkpoint1"}
               [] r = 2 -> {"root2", "checkpoint0", "childWal0"}
               [] r = 3 -> {"root3", "checkpoint1", "childWal1"}
Values(r) == CASE r = NoRoot -> {}
              [] r = 0 -> {"seed"}
              [] r = 1 -> {"seed", "parent-write"}
              [] r = 2 -> {"seed", "child-write"}
              [] r = 3 -> {"seed", "parent-write", "child-write"}
Files == UNION {Closure(r): r \in Roots}
Owned(state) == state \notin {"Absent", "Deleted"}

VARIABLE s
vars == <<s>>

HeadFiles == UNION {IF Owned(s.state[b]) THEN Closure(s.head[b]) ELSE {}:
                   b \in Branches}
BaseFiles == IF Owned(s.state[1]) THEN Closure(s.base) ELSE {}
PinFiles == UNION {Closure(s.pin[b]): b \in Branches}
CandidateFiles == UNION {Closure(s.candidate[b]): b \in Branches}
Reachable == HeadFiles \cup BaseFiles \cup PinFiles \cup CandidateFiles
Protected == HeadFiles \cup PinFiles \cup CandidateFiles
             \cup (IF ForgetCreating /\ s.state[1] = "Creating"
                   THEN {} ELSE BaseFiles)

Init == s = [
    state |-> [b \in Branches |-> IF b = 0 THEN "Ready" ELSE "Absent"],
    head |-> [b \in Branches |-> IF b = 0 THEN 0 ELSE NoRoot],
    expected |-> [b \in Branches |-> IF b = 0 THEN Values(0) ELSE {}],
    base |-> NoRoot,
    capturedSource |-> NoRoot,
    pin |-> [b \in Branches |-> NoRoot],
    candidate |-> [b \in Branches |-> NoRoot],
    durable |-> Closure(0),
    marked |-> {},
    online |-> TRUE,
    invalidOpen |-> FALSE,
    recoveredCreate |-> FALSE,
    abortedCreate |-> FALSE,
    recoveredDelete |-> FALSE
]

(* Durable Creating reserves identity and pins the exact sealed source before
   releasing metadata serialization. Installing the head is a separate step. *)
PrepareCreate ==
    /\ s.online
    /\ s.state[1] = "Absent"
    /\ s' = [s EXCEPT !.state[1] = "Creating",
                     !.base = s.head[0], !.capturedSource = s.head[0],
                     !.expected[1] = Values(s.head[0])]

InstallChildHead ==
    /\ s.online
    /\ s.state[1] = "Creating"
    /\ s.head[1] = NoRoot
    /\ Closure(s.base) \subseteq s.durable
    /\ s' = [s EXCEPT !.head[1] = s.base]

PublishCreate ==
    /\ s.online
    /\ s.state[1] = "Creating"
    /\ s.head[1] = s.base
    /\ s' = [s EXCEPT !.state[1] = "Ready"]

Open(b) ==
    /\ s.online
    /\ s.pin[b] = NoRoot
    /\ s.state[b] = "Ready" \/ (OpenExpired /\ s.state[b] = "Expired")
    /\ s' = [s EXCEPT !.pin[b] = s.head[b],
                     !.invalidOpen = s.invalidOpen \/ s.state[b] # "Ready"]

Close(b) ==
    /\ s.online
    /\ s.pin[b] # NoRoot
    /\ s.candidate[b] = NoRoot
    /\ s' = [s EXCEPT !.pin[b] = NoRoot]

PrepareWrite(b) ==
    /\ s.online
    /\ s.pin[b] # NoRoot
    /\ s.state[b] \in {"Ready", "Expired"}
    /\ s.candidate[b] = NoRoot
    /\ IF b = 0 THEN s.head[b] = 0 ELSE s.head[b] = s.base
    /\ s' = [s EXCEPT !.candidate[b] = IF b = 0 THEN 1 ELSE s.base + 2]

PersistObject(b, f) ==
    /\ s.online
    /\ s.candidate[b] # NoRoot
    /\ f \in Closure(s.candidate[b]) \ s.durable
    /\ s' = [s EXCEPT !.durable = @ \cup {f}]

PublishHead(b) ==
    /\ s.online
    /\ s.candidate[b] # NoRoot
    /\ PublishEarly \/ Closure(s.candidate[b]) \subseteq s.durable
    /\ s' = [s EXCEPT !.head[b] = s.candidate[b],
                     !.head[0] = IF MutateParent /\ b = 1
                                 THEN s.candidate[b]
                                 ELSE IF b = 0 THEN s.candidate[b] ELSE @,
                     !.expected[b] = Values(s.candidate[b]),
                     !.candidate[b] = NoRoot]

DiscardWrite(b) ==
    /\ s.online
    /\ s.candidate[b] # NoRoot
    /\ s' = [s EXCEPT !.candidate[b] = NoRoot]

Expire ==
    /\ s.online
    /\ s.state[1] = "Ready"
    /\ s' = [s EXCEPT !.state[1] = "Expired"]

BeginDelete ==
    /\ s.online
    /\ s.state[1] \in {"Ready", "Expired"}
    /\ s' = [s EXCEPT !.state[1] = "Deleting"]

FinalizeDelete ==
    /\ s.online
    /\ s.state[1] = "Deleting"
    /\ DeleteLeased \/ (s.pin[1] = NoRoot /\ s.candidate[1] = NoRoot)
    /\ s' = [s EXCEPT !.state[1] = "Deleted"]

Mark ==
    /\ s.online
    /\ s' = [s EXCEPT !.marked = s.durable \ Protected]

Sweep(f) ==
    /\ s.online
    /\ f \in s.marked \cap s.durable
    /\ SkipSweepRecheck \/ f \notin Protected
    /\ s' = [s EXCEPT !.durable = @ \ {f}, !.marked = @ \ {f}]

Crash ==
    /\ s.online
    /\ s' = [s EXCEPT !.online = FALSE,
                     !.pin = [b \in Branches |-> NoRoot],
                     !.candidate = [b \in Branches |-> NoRoot], !.marked = {}]

(* Recovery takes the durable create decision to one complete outcome. An
   interrupted delete remains non-openable, then retires after lease loss. *)
Recover ==
    /\ ~s.online
    /\ s' = [s EXCEPT
          !.online = TRUE,
          !.state[1] = CASE s.state[1] = "Creating" ->
                           IF s.head[1] = NoRoot THEN "Deleted" ELSE "Ready"
                        [] s.state[1] = "Deleting" -> "Deleted"
                        [] OTHER -> @,
          !.recoveredCreate = @ \/ (s.state[1] = "Creating" /\ s.head[1] # NoRoot),
          !.abortedCreate = @ \/ (s.state[1] = "Creating" /\ s.head[1] = NoRoot),
          !.recoveredDelete = @ \/ s.state[1] = "Deleting"]

Next == PrepareCreate \/ InstallChildHead \/ PublishCreate
        \/ (\E b \in Branches: Open(b) \/ Close(b) \/ PrepareWrite(b)
                              \/ PublishHead(b) \/ DiscardWrite(b)
                              \/ (\E f \in Files: PersistObject(b, f)))
        \/ Expire \/ BeginDelete \/ FinalizeDelete \/ Mark
        \/ (\E f \in Files: Sweep(f)) \/ Crash \/ Recover
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ s.state \in [Branches -> States]
    /\ s.head \in [Branches -> Roots \cup {NoRoot}]
    /\ s.expected \in [Branches -> SUBSET {"seed", "parent-write", "child-write"}]
    /\ s.base \in Roots \cup {NoRoot}
    /\ s.capturedSource \in Roots \cup {NoRoot}
    /\ s.pin \in [Branches -> Roots \cup {NoRoot}]
    /\ s.candidate \in [Branches -> Roots \cup {NoRoot}]
    /\ s.durable \subseteq Files
    /\ s.marked \subseteq Files
    /\ s.online \in BOOLEAN
    /\ s.invalidOpen \in BOOLEAN
    /\ s.recoveredCreate \in BOOLEAN
    /\ s.abortedCreate \in BOOLEAN
    /\ s.recoveredDelete \in BOOLEAN

SelectedRootsComplete == HeadFiles \subseteq s.durable
PinnedRootsRetained == PinFiles \subseteq s.durable
CreateSourceRetained == BaseFiles \subseteq s.durable
LineageImmutable == s.base = s.capturedSource
BranchIsolation == \A b \in Branches:
    Owned(s.state[b]) /\ s.head[b] # NoRoot => Values(s.head[b]) = s.expected[b]
ParentImmutableUnderChildWrites == s.head[0] \in {0, 1}
DeletedHasNoLease == s.state[1] = "Deleted" => s.pin[1] = NoRoot
OpenAdmission == ~s.invalidOpen
ReadyHasRoot == \A b \in Branches:
    s.state[b] \in {"Ready", "Expired", "Deleting"} => s.head[b] # NoRoot

(* False invariants used only to obtain concrete success/interruption traces. *)
NoCreateRecovery == ~s.recoveredCreate
NoAbortedCreate == ~s.abortedCreate
NoDeleteRecovery == ~s.recoveredDelete
NoConcurrentWriters == s.pin[0] = NoRoot \/ s.pin[1] = NoRoot
NoExpiredHandle == s.state[1] # "Expired" \/ s.pin[1] = NoRoot
NoIndependentWrites == s.head[0] # 1 \/ s.head[1] \notin {2, 3}
=============================================================================
