-------------- MODULE HawDBRelationalStatementReadBudget --------------
EXTENDS Naturals, FiniteSets

CONSTANTS PrivateAdmission, ForgetOnRebind
Readers == {"parent", "child"}
Resources == {"page", "byte", "row", "overlay-entry", "overlay-byte"}
Kinds == {"page", "row", "overlay-entry", "overlay-byte"}
Zero == [r \in Resources |-> 0]
Charge(kind) == [r \in Resources |->
    IF r = kind \/ (kind = "page" /\ r = "byte") THEN 1 ELSE 0]

VARIABLES phase, limits, used, accepted, local
vars == <<phase, limits, used, accepted, local>>

Init ==
    /\ phase = [reader \in Readers |-> "idle"]
    /\ limits = [r \in Resources |-> 2]
    /\ used = Zero
    /\ accepted = Zero
    /\ local = [reader \in Readers |-> Zero]

Begin(reader) ==
    /\ phase[reader] = "idle"
    /\ reader = "parent" \/ phase["parent"] = "active"
    /\ phase' = [phase EXCEPT ![reader] = "active"]
    /\ UNCHANGED <<limits, used, accepted, local>>

Admit(reader, kind) ==
    /\ phase[reader] = "active"
    /\ LET charge == Charge(kind)
       IN /\ \A r \in Resources:
                 (IF PrivateAdmission THEN local[reader][r] ELSE used[r])
                     + charge[r] <= limits[r]
          /\ used' = [r \in Resources |-> used[r] + charge[r]]
          /\ accepted' = [r \in Resources |-> accepted[r] + charge[r]]
          /\ local' = [local EXCEPT ![reader] =
                 [r \in Resources |-> local[reader][r] + charge[r]]]
    /\ UNCHANGED <<phase, limits>>

(* Success, admission rejection, early stop and callback unwind all end an
   operation without refunding work that already happened. Pins, task stop,
   row order and corruption poison remain in the demand/snapshot models. *)
Finish(reader) ==
    /\ phase[reader] = "active"
    /\ reader = "child" \/ phase["child"] # "active"
    /\ phase' = [phase EXCEPT ![reader] = "done"]
    /\ UNCHANGED <<limits, used, accepted, local>>

Rebind ==
    /\ IF ForgetOnRebind
       THEN /\ used # Zero
            /\ used' = Zero
            /\ UNCHANGED limits
       ELSE /\ \E r \in Resources:
                    /\ limits[r] = 2
                    /\ used[r] <= 1
                    /\ limits' = [limits EXCEPT ![r] = 1]
            /\ UNCHANGED used
    /\ UNCHANGED <<phase, accepted, local>>

Next ==
    \/ \E reader \in Readers: Begin(reader) \/ Finish(reader)
    \/ \E reader \in Readers, kind \in Kinds: Admit(reader, kind)
    \/ Rebind

Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ phase \in [Readers -> {"idle", "active", "done"}]
    /\ limits \in [Resources -> 1..2]
    /\ used \in [Resources -> 0..4]
    /\ accepted \in [Resources -> 0..4]
    /\ local \in [Readers -> [Resources -> 0..2]]
BudgetsIncludeActiveWork == \A r \in Resources: accepted[r] <= limits[r]
AcceptedWorkIsNeverForgotten == used = accepted
======================================================================
