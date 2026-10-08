-------------- MODULE HawDBRelationalIndexAttemptLifecycle --------------
EXTENDS Naturals, FiniteSets

CONSTANTS CloseZeroRefusal, ChargeBeforeOuterPreflight
Owners == {"statement", "transaction"}
Readers == {"parent", "child"}
Resources == {"page", "row"}
Zero == [r \in Resources |-> 0]
Charge(kind) == [r \in Resources |-> IF r = kind THEN 1 ELSE 0]

VARIABLES phase, limits, used, accepted, local, healthy, lastEvent
vars == <<phase, limits, used, accepted, local, healthy, lastEvent>>
AllHealthy == \A owner \in Owners: healthy[owner]
Fits(owner, kind) ==
    \A r \in Resources: used[owner][r] + Charge(kind)[r] <= limits[owner][r]

Init ==
    /\ phase = [reader \in Readers |-> "idle"]
    /\ limits \in [Owners -> [Resources -> 1..2]]
    /\ used = [owner \in Owners |-> Zero]
    /\ accepted = used
    /\ local = [reader \in Readers |-> Zero]
    /\ healthy = [owner \in Owners |-> TRUE]
    /\ lastEvent = "init"

Begin(reader) ==
    /\ AllHealthy
    /\ phase[reader] = "idle"
    /\ reader = "parent" \/ phase["parent"] = "active"
    /\ phase' = [phase EXCEPT ![reader] = "active"]
    /\ lastEvent' = "begin"
    /\ UNCHANGED <<limits, used, accepted, local, healthy>>

(* The inline preflight and commit admit one physical operation to both owners
   before payload or visitor reentry. Different owner caps remain independent. *)
Admit(reader, kind) ==
    /\ AllHealthy
    /\ phase[reader] = "active"
    /\ \A owner \in Owners: Fits(owner, kind)
    /\ used' = [owner \in Owners |->
           [r \in Resources |-> used[owner][r] + Charge(kind)[r]]]
    /\ accepted' = used'
    /\ local' = [local EXCEPT ![reader] =
           [r \in Resources |-> local[reader][r] + Charge(kind)[r]]]
    /\ lastEvent' = "admit"
    /\ UNCHANGED <<phase, limits, healthy>>

(* Only a typed owner-budget refusal of this attempt's first operation can
   preserve health. Actual partial work never gets refunded or reopened. *)
Refuse(reader, kind) ==
    /\ AllHealthy
    /\ phase[reader] = "active"
    /\ \E owner \in Owners: ~Fits(owner, kind)
    /\ phase' = [phase EXCEPT ![reader] = "done"]
    /\ IF local[reader] = Zero
       THEN /\ healthy' = IF CloseZeroRefusal
                          THEN [owner \in Owners |-> FALSE] ELSE healthy
            /\ lastEvent' = "zero-refusal"
       ELSE /\ healthy' = [owner \in Owners |-> FALSE]
            /\ lastEvent' = "failure"
    /\ UNCHANGED <<limits, used, accepted, local>>

(* This mutant fabricates accepted work in the first owner even though the
   second owner's preflight refuses the physical operation. *)
ChargeOneOwnerBeforeRefusal(reader, kind) ==
    /\ ChargeBeforeOuterPreflight
    /\ AllHealthy
    /\ phase[reader] = "active"
    /\ Fits("transaction", kind)
    /\ ~Fits("statement", kind)
    /\ used' = [used EXCEPT !["transaction"] =
           [r \in Resources |-> used["transaction"][r] + Charge(kind)[r]]]
    /\ lastEvent' = "partial-charge"
    /\ UNCHANGED <<phase, limits, accepted, local, healthy>>

Finish(reader) ==
    /\ AllHealthy
    /\ phase[reader] = "active"
    /\ reader = "child" \/ phase["child"] # "active"
    /\ phase' = [phase EXCEPT ![reader] = "done"]
    /\ lastEvent' = "finish"
    /\ UNCHANGED <<limits, used, accepted, local, healthy>>

(* Unknown errors and unwind close owners even when no charge was observed;
   zero counters alone cannot establish a safe pre-operation refusal. *)
Fail(reader) ==
    /\ phase[reader] = "active"
    /\ phase' = [phase EXCEPT ![reader] = "done"]
    /\ healthy' = [owner \in Owners |-> FALSE]
    /\ lastEvent' = "failure"
    /\ UNCHANGED <<limits, used, accepted, local>>

DescriptorRefusal(reader) ==
    /\ AllHealthy
    /\ phase[reader] = "active"
    /\ phase' = [phase EXCEPT ![reader] = "done"]
    /\ lastEvent' = "descriptor-refusal"
    /\ UNCHANGED <<limits, used, accepted, local, healthy>>

Next ==
    \/ \E reader \in Readers:
           Begin(reader) \/ Finish(reader) \/ Fail(reader) \/ DescriptorRefusal(reader)
    \/ \E reader \in Readers, kind \in Resources:
           Admit(reader, kind) \/ Refuse(reader, kind)
           \/ ChargeOneOwnerBeforeRefusal(reader, kind)

Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ phase \in [Readers -> {"idle", "active", "done"}]
    /\ limits \in [Owners -> [Resources -> 1..2]]
    /\ used \in [Owners -> [Resources -> 0..2]]
    /\ accepted \in [Owners -> [Resources -> 0..2]]
    /\ local \in [Readers -> [Resources -> 0..2]]
    /\ healthy \in [Owners -> BOOLEAN]
    /\ lastEvent \in {"init", "begin", "admit", "zero-refusal", "failure",
                       "partial-charge", "finish", "descriptor-refusal"}
AcceptedOperationsAreAtomic == used = accepted
BudgetsIncludeActiveWork ==
    \A owner \in Owners, r \in Resources: used[owner][r] <= limits[owner][r]
ZeroWorkRefusalPreservesHealth == lastEvent = "zero-refusal" => AllHealthy
FailureClosesOwners == lastEvent = "failure" => \A owner \in Owners: ~healthy[owner]
======================================================================
