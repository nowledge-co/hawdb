-------------------- MODULE HawDBSearchStageCleanup --------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Stages, Charges, Budget, RestoreOnUnwind, ReleaseBeforeTicketFree
ASSUME /\ IsFiniteSet(Stages) /\ Stages # {}
       /\ Charges \in [Stages -> Nat \ {0}]
       /\ Budget \in Nat
       /\ RestoreOnUnwind \in BOOLEAN
       /\ ReleaseBeforeTicketFree \in BOOLEAN

VARIABLES phase, evidence, tickets, leases, used
vars == <<phase, evidence, tickets, leases, used>>
Phases == {"idle", "reserved", "live", "pending", "attempt", "removed",
           "freed", "released", "lost"}

SmallCharges == [s \in Stages |-> IF s = CHOOSE first \in Stages: TRUE THEN 1 ELSE 2]

RECURSIVE Sum(_)
Sum(owners) == IF owners = {} THEN 0
              ELSE LET s == CHOOSE s \in owners: TRUE
                   IN Charges[s] + Sum(owners \ {s})

Init == /\ phase = [s \in Stages |-> "idle"]
        /\ evidence = {} /\ tickets = {} /\ leases = {} /\ used = 0

Reserve(s) ==
    /\ phase[s] = "idle" /\ used + Charges[s] <= Budget
    /\ phase' = [phase EXCEPT ![s] = "reserved"]
    /\ leases' = leases \union {s} /\ used' = used + Charges[s]
    /\ UNCHANGED <<evidence, tickets>>

Create(s) ==
    /\ phase[s] = "reserved"
    /\ phase' = [phase EXCEPT ![s] = "live"]
    /\ evidence' = evidence \union {s} /\ tickets' = tickets \union {s}
    /\ UNCHANGED <<leases, used>>

SetupFailure(s) ==
    /\ phase[s] = "reserved"
    /\ phase' = [phase EXCEPT ![s] = "released"]
    /\ leases' = leases \ {s} /\ used' = used - Charges[s]
    /\ UNCHANGED <<evidence, tickets>>

Begin(s) ==
    /\ phase[s] \in {"live", "pending"}
    /\ phase' = [phase EXCEPT ![s] = "attempt"]
    /\ UNCHANGED <<evidence, tickets, leases, used>>

Defer(s) ==
    /\ phase[s] = "attempt"
    \* Caller refusal, cancellation, I/O failure, or a bounded partial pass.
    /\ phase' = [phase EXCEPT ![s] = "pending"]
    /\ UNCHANGED <<evidence, tickets, leases, used>>

Remove(s) ==
    /\ phase[s] = "attempt"
    \* Only confirmed native removal, including a verified absent stage.
    /\ phase' = [phase EXCEPT ![s] = "removed"]
    /\ evidence' = evidence \ {s}
    /\ UNCHANGED <<tickets, leases, used>>

Unwind(s) ==
    /\ phase[s] \in {"attempt", "removed"}
    /\ phase' = [phase EXCEPT ![s] = IF RestoreOnUnwind THEN "pending" ELSE "lost"]
    /\ tickets' = IF RestoreOnUnwind THEN tickets ELSE tickets \ {s}
    /\ leases' = IF RestoreOnUnwind THEN leases ELSE leases \ {s}
    /\ used' = IF RestoreOnUnwind THEN used ELSE used - Charges[s]
    /\ UNCHANGED evidence

FreeTicket(s) ==
    /\ phase[s] = "removed"
    /\ phase' = [phase EXCEPT ![s] = "freed"]
    /\ tickets' = IF ReleaseBeforeTicketFree THEN tickets ELSE tickets \ {s}
    /\ leases' = IF ReleaseBeforeTicketFree THEN leases \ {s} ELSE leases
    /\ used' = IF ReleaseBeforeTicketFree THEN used - Charges[s] ELSE used
    /\ UNCHANGED evidence

Release(s) ==
    /\ phase[s] = "freed" /\ s \in leases /\ s \notin tickets
    /\ phase' = [phase EXCEPT ![s] = "released"]
    /\ leases' = leases \ {s} /\ used' = used - Charges[s]
    /\ UNCHANGED <<evidence, tickets>>

Next == \E s \in Stages:
    Reserve(s) \/ Create(s) \/ SetupFailure(s) \/ Begin(s) \/ Defer(s)
    \/ Remove(s) \/ Unwind(s) \/ FreeTicket(s) \/ Release(s)

TypeInvariant ==
    /\ phase \in [Stages -> Phases]
    /\ evidence \subseteq Stages /\ tickets \subseteq Stages /\ leases \subseteq Stages
    /\ used \in Nat

EvidenceHasOwner == evidence \subseteq leases
TicketMemoryCovered == tickets \subseteq leases
ExactRetainedAccounting == used = Sum(leases)
BudgetBounded == used <= Budget

OwnershipInvariant ==
    /\ TypeInvariant /\ EvidenceHasOwner /\ TicketMemoryCovered
    /\ ExactRetainedAccounting /\ BudgetBounded
    /\ \A s \in Stages:
        /\ (phase[s] \in {"reserved", "live", "pending", "attempt", "removed", "freed"})
              = (s \in leases)
        /\ (phase[s] \in {"live", "pending", "attempt", "removed"}) = (s \in tickets)
        /\ s \in evidence => phase[s] \in {"live", "pending", "attempt"}

Spec == Init /\ [][Next]_vars
=============================================================================
