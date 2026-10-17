---------------- MODULE HawDBSearchPreparedPublication ----------------
EXTENDS Integers, FiniteSets

CONSTANTS MaxGeneration, SkipBaseCheck, ReleaseFence, EarlySelector,
          CollectPrivateStage, CollectActiveClosure, CancelCommitted,
          ConflateDamagedSelectors, HoldLeaseDuringBuild, SkipWatermarkCheck
VARIABLES db, phase, base, private, installed, slots, owner, committed,
          staleActivation, concurrentProgress, checked, sourceWatermark, regressed
vars == <<db, phase, base, private, installed, slots, owner, committed,
          staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Publishers == {1, 2}
Parts == {1, 2}
Selectors == {"valid", "damagedA", "damagedB"}
Phases == {"idle", "building", "ready", "installing", "committed",
           "done", "lost", "rejected", "cancelled", "crashed"}
Current == [head |-> db.head, selector |-> db.selector]
SameBase(p) == base[p] = Current
BaseAccepted(p) == SameBase(p) \/ SkipBaseCheck \/
    (ConflateDamagedSelectors /\ base[p].head = db.head /\
     base[p].selector # "valid" /\ db.selector # "valid")
Generation(p) == base[p].head + 1
ActivationAllowed(p) == ~db.consumer /\ BaseAccepted(p) /\
    (~checked[p] \/ (db.selector = "valid" /\
       (sourceWatermark[p] >= db.watermark \/ SkipWatermarkCheck)))

Init ==
    /\ db = [head |-> 0, selector |-> "valid", selected |-> 0,
              consumer |-> FALSE, corruptions |-> 0, crashes |-> 0, watermark |-> 0]
    /\ phase = [p \in Publishers |-> "idle"]
    /\ base = [p \in Publishers |-> [head |-> 0, selector |-> "valid"]]
    /\ private = [p \in Publishers |-> {}]
    /\ installed = [p \in Publishers |-> {}]
    /\ slots = [g \in 1..MaxGeneration |-> [i \in Parts |-> 0]]
    /\ owner = 0 /\ committed = {} /\ staleActivation = FALSE
    /\ concurrentProgress = FALSE
    /\ checked = [p \in Publishers |-> FALSE]
    /\ sourceWatermark = [p \in Publishers |-> 0] /\ regressed = FALSE

Capture(p, sourceChecked, epoch) ==
    /\ phase[p] = "idle" /\ owner = 0 /\ ~db.consumer
    /\ db.head < MaxGeneration /\ sourceChecked \in BOOLEAN /\ epoch \in 0..2
    /\ checked' = [checked EXCEPT ![p] = sourceChecked]
    /\ sourceWatermark' = [sourceWatermark EXCEPT ![p] = epoch]
    /\ phase' = [phase EXCEPT ![p] = "building"]
    /\ base' = [base EXCEPT ![p] = Current]
    /\ owner' = IF HoldLeaseDuringBuild THEN p ELSE 0
    /\ UNCHANGED <<db, private, installed, slots, committed,
                    staleActivation, concurrentProgress, regressed>>
Build(p) ==
    /\ phase[p] = "building"
    /\ LET next == IF private[p] = {} THEN 1 ELSE 2
           complete == private[p] \cup {next}
       IN /\ private' = [private EXCEPT ![p] = complete]
          /\ phase' = [phase EXCEPT ![p] = IF complete = Parts THEN "ready" ELSE @]
          /\ owner' = IF complete = Parts /\ owner = p THEN 0 ELSE owner
    /\ UNCHANGED <<db, base, installed, slots, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Acquire(p) ==
    /\ phase[p] = "ready" /\ owner = 0
    /\ ActivationAllowed(p)
    /\ phase' = [phase EXCEPT ![p] = "installing"]
    /\ owner' = IF ReleaseFence THEN 0 ELSE p
    /\ UNCHANGED <<db, base, private, installed, slots, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Reject(p) ==
    /\ phase[p] = "ready" /\ owner = 0
    /\ ~ActivationAllowed(p)
    /\ phase' = [phase EXCEPT ![p] = "rejected"]
    /\ private' = [private EXCEPT ![p] = {}]
    /\ UNCHANGED <<db, base, installed, slots, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Install(p, i) ==
    /\ phase[p] = "installing" /\ (owner = p \/ ReleaseFence)
    /\ i \in private[p] \ installed[p]
    /\ slots' = [slots EXCEPT ![Generation(p)][i] = p]
    /\ installed' = [installed EXCEPT ![p] = @ \cup {i}]
    /\ UNCHANGED <<db, phase, base, private, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Select(p) ==
    /\ phase[p] = "installing" /\ (owner = p \/ ReleaseFence)
    /\ installed[p] = Parts \/ (EarlySelector /\ installed[p] # {})
    /\ staleActivation' = (staleActivation \/ ~SameBase(p) \/ db.consumer)
    /\ concurrentProgress' = (concurrentProgress \/
          (\E other \in Publishers \ {p}: phase[other] = "building"))
    /\ regressed' = (regressed \/ (checked[p] /\ sourceWatermark[p] < db.watermark))
    /\ db' = [db EXCEPT !.head = Generation(p), !.selector = "valid", !.selected = p,
                        !.watermark = sourceWatermark[p]]
    /\ committed' = committed \cup {p}
    /\ phase' = [phase EXCEPT ![p] = "committed"]
    /\ owner' = 0
    /\ UNCHANGED <<base, private, installed, slots, checked, sourceWatermark>>
Return(p, lost) ==
    /\ phase[p] = "committed" /\ lost \in BOOLEAN
    /\ phase' = [phase EXCEPT ![p] = IF lost THEN "lost" ELSE "done"]
    /\ private' = [private EXCEPT ![p] = {}]
    /\ UNCHANGED <<db, base, installed, slots, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Resolve(p) ==
    /\ phase[p] = "lost" /\ db.selected = p
    /\ phase' = [phase EXCEPT ![p] = "done"]
    /\ UNCHANGED <<db, base, private, installed, slots, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Cancel(p) ==
    /\ phase[p] \in {"building", "ready", "installing"} \/
          (CancelCommitted /\ phase[p] = "committed")
    /\ phase' = [phase EXCEPT ![p] = "cancelled"]
    /\ private' = [private EXCEPT ![p] = {}]
    /\ owner' = IF owner = p THEN 0 ELSE owner
    /\ UNCHANGED <<db, base, installed, slots, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
StageGC(p, i) ==
    /\ CollectPrivateStage /\ phase[p] \in {"ready", "installing"}
    /\ i \in private[p]
    /\ private' = [private EXCEPT ![p] = @ \ {i}]
    /\ UNCHANGED <<db, phase, base, installed, slots, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
ArtifactGC(g) ==
    /\ owner = 0 /\ (g # db.head \/ CollectActiveClosure)
    /\ slots[g] # [i \in Parts |-> 0]
    /\ slots' = [slots EXCEPT ![g] = [i \in Parts |-> 0]]
    /\ UNCHANGED <<db, phase, base, private, installed, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Damage ==
    /\ owner = 0 /\ db.corruptions < 2
    /\ db' = [db EXCEPT !.selector = IF db.corruptions = 0 THEN "damagedA" ELSE "damagedB",
                        !.corruptions = @ + 1]
    /\ UNCHANGED <<phase, base, private, installed, slots, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
BindConsumer ==
    /\ owner = 0 /\ ~db.consumer
    /\ db' = [db EXCEPT !.consumer = TRUE]
    /\ UNCHANGED <<phase, base, private, installed, slots, owner, committed,
                    staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>
Crash ==
    /\ db.crashes = 0
    /\ db' = [db EXCEPT !.crashes = 1]
    /\ phase' = [p \in Publishers |-> IF phase[p] \in
          {"building", "ready", "installing", "committed"} THEN "crashed" ELSE phase[p]]
    /\ private' = [p \in Publishers |-> {}] /\ owner' = 0
    /\ UNCHANGED <<base, installed, slots, committed, staleActivation, concurrentProgress, checked, sourceWatermark, regressed>>

Next == (\E p \in Publishers:
          (\E c \in BOOLEAN, epoch \in 0..2: Capture(p, c, epoch)) \/ Build(p) \/ Acquire(p) \/ Reject(p)
          \/ (\E i \in Parts: Install(p, i) \/ StageGC(p, i)) \/ Select(p)
          \/ (\E lost \in BOOLEAN: Return(p, lost)) \/ Resolve(p) \/ Cancel(p))
        \/ (\E g \in 1..MaxGeneration: ArtifactGC(g)) \/ Damage \/ BindConsumer \/ Crash

TypeOK ==
    /\ db.head \in 0..MaxGeneration /\ db.selected \in Publishers \cup {0}
    /\ db.selector \in Selectors /\ db.consumer \in BOOLEAN
    /\ phase \in [Publishers -> Phases]
    /\ base \in [Publishers -> [head: 0..MaxGeneration, selector: Selectors]]
    /\ private \in [Publishers -> SUBSET Parts] /\ installed \in [Publishers -> SUBSET Parts]
    /\ slots \in [1..MaxGeneration -> [Parts -> Publishers \cup {0}]]
    /\ owner \in Publishers \cup {0} /\ committed \subseteq Publishers
    /\ staleActivation \in BOOLEAN /\ concurrentProgress \in BOOLEAN
    /\ checked \in [Publishers -> BOOLEAN] /\ sourceWatermark \in [Publishers -> 0..2]
    /\ db.watermark \in 0..2 /\ regressed \in BOOLEAN
NoStaleActivation == ~staleActivation
CompleteActiveClosure == db.head = 0 \/
    (\A i \in Parts: slots[db.head][i] = db.selected)
PrivateStageRetained == \A p \in Publishers:
    phase[p] \in {"ready", "installing"} => private[p] = Parts
NoFalseRollback == \A p \in Publishers: phase[p] = "cancelled" => p \notin committed
NoPreparationLease == \A p \in Publishers: phase[p] = "building" => owner # p
NoConcurrentProgress == ~concurrentProgress
NoCheckedWatermarkRegression == ~regressed
=============================================================================
