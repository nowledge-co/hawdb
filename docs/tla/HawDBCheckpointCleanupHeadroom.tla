---------------- MODULE HawDBCheckpointCleanupHeadroom ----------------
EXTENDS Naturals, Integers, FiniteSets

CONSTANTS CapacityAt, DeferAt, WriteEstimate,
          SkipDiscardFence, SkipReleaseFence
ASSUME /\ CapacityAt > 0
       /\ DeferAt > CapacityAt
       /\ WriteEstimate > 0
       /\ WriteEstimate <= DeferAt - CapacityAt
       /\ {SkipDiscardFence, SkipReleaseFence} \subseteq BOOLEAN

Cleanup == {"discarding", "releasing"}
Phases == Cleanup \cup {"idle", "preparing"}
VARIABLES phase, used, frontend, capturedPhase, capturedUsed, waited, released, ticks
vars == <<phase, used, frontend, capturedPhase, capturedUsed, waited, released, ticks>>

Init ==
    /\ phase \in Cleanup \cup {"preparing"}
    /\ used \in 0..(DeferAt - 1)
    /\ frontend = "pending"
    /\ capturedPhase = "none"
    /\ capturedUsed = 0
    /\ waited = FALSE
    /\ released = FALSE
    /\ ticks = 0

WaitRequired ==
    /\ used >= CapacityAt
    /\ \/ phase = "preparing"
       \/ (phase = "discarding" /\ ~SkipDiscardFence)
       \/ (phase = "releasing" /\ ~SkipReleaseFence)

Admit ==
    /\ frontend = "pending"
    /\ ~WaitRequired
    /\ frontend' = "running"
    /\ capturedPhase' = phase
    /\ capturedUsed' = used
    /\ UNCHANGED <<phase, used, waited, released, ticks>>

Wait ==
    /\ frontend = "pending"
    /\ WaitRequired
    /\ ~waited
    /\ waited' = TRUE
    /\ UNCHANGED <<phase, used, frontend, capturedPhase, capturedUsed, released, ticks>>

Tick ==
    /\ frontend = "pending"
    /\ WaitRequired
    /\ ticks < 2
    /\ ticks' = ticks + 1
    /\ UNCHANGED <<phase, used, frontend, capturedPhase, capturedUsed, waited, released>>

Timeout ==
    /\ frontend = "pending"
    /\ WaitRequired
    /\ ticks = 2
    /\ frontend' = "busy"
    /\ UNCHANGED <<phase, used, capturedPhase, capturedUsed, waited, released, ticks>>

CompleteCleanup ==
    /\ phase \in Cleanup
    /\ frontend # "running"
    /\ phase' = "idle"
    /\ released' = TRUE
    /\ UNCHANGED <<used, frontend, capturedPhase, capturedUsed, waited, ticks>>

StartCheckpoint ==
    /\ phase = "idle"
    /\ frontend = "pending"
    /\ used >= CapacityAt
    /\ phase' = "preparing"
    /\ UNCHANGED <<used, frontend, capturedPhase, capturedUsed, waited, released, ticks>>

CompleteCheckpoint ==
    /\ phase = "preparing"
    /\ frontend # "running"
    /\ phase' \in Cleanup
    /\ used' \in 0..used
    /\ UNCHANGED <<frontend, capturedPhase, capturedUsed, waited, released, ticks>>

Write ==
    /\ frontend = "running"
    /\ frontend' = IF used + WriteEstimate < DeferAt THEN "committed" ELSE "deferred"
    /\ used' = IF used + WriteEstimate < DeferAt THEN used + 1 ELSE used
    /\ UNCHANGED <<phase, capturedPhase, capturedUsed, waited, released, ticks>>

Terminal ==
    /\ frontend \in {"busy", "committed", "deferred"}
    /\ UNCHANGED vars

Next == Admit \/ Wait \/ Tick \/ Timeout \/ CompleteCleanup \/ StartCheckpoint
        \/ CompleteCheckpoint \/ Write \/ Terminal
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in Phases
    /\ used \in 0..DeferAt
    /\ frontend \in {"pending", "running", "busy", "committed", "deferred"}
    /\ capturedPhase \in Phases \cup {"none"}
    /\ capturedUsed \in 0..DeferAt
    /\ waited \in BOOLEAN
    /\ released \in BOOLEAN
    /\ ticks \in 0..2

NoCleanupCapacityBypass == capturedPhase \in Cleanup => capturedUsed < CapacityAt
NoCleanupPressureRefusal == ~(frontend = "deferred" /\ capturedPhase \in Cleanup)
NoSmallWriterWitness == ~(frontend = "committed" /\ capturedPhase \in Cleanup)
NoCleanupReleaseWitness == ~(frontend = "committed" /\ waited /\ released)
=============================================================================
