-------------------- MODULE SkeinRuntimeAdmission --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Memory admission with a stable capacity and a dynamic budget. Capacity  *)
(* is the stable maximum (explicit configuration and the cgroup limit      *)
(* ceiling); the budget is capacity further bounded by sensed              *)
(* availability, so it moves under the capacity as resources refresh. A    *)
(* request above capacity is statically unsatisfiable and terminates       *)
(* non-retryably at submission, before any dynamic saturation check. A     *)
(* request within capacity but above the currently uncommitted budget      *)
(* waits, retries as refreshes restore headroom, and can be cancelled.     *)
(***************************************************************************)

CONSTANT Capacity, Waiters

ASSUME /\ Capacity \in Nat \ {0}
       /\ Waiters # {}

Sizes == 1..(Capacity + 1)
Statuses == {"idle", "waiting", "admitted", "rejected", "cancelled"}

VARIABLES
    budget,
    requested,
    status

vars == <<budget, requested, status>>

AdmittedBytes ==
    LET admitted == {waiter \in Waiters: status[waiter] = "admitted"}
    IN LET Sum[chosen \in SUBSET admitted] ==
            IF chosen = {} THEN 0
            ELSE LET waiter == CHOOSE waiter \in chosen: TRUE
                 IN requested[waiter] + Sum[chosen \ {waiter}]
       IN Sum[admitted]

UncommittedBudget == budget - AdmittedBytes

Init ==
    /\ budget \in 0..Capacity
    /\ requested = [waiter \in Waiters |-> 0]
    /\ status = [waiter \in Waiters |-> "idle"]

(***************************************************************************)
(* Submission runs the static capacity check first: an over-capacity       *)
(* request is rejected terminally even if every dynamic gate is busy.      *)
(* Within capacity, the request admits against the uncommitted budget or   *)
(* waits.                                                                  *)
(***************************************************************************)
Submit(waiter, size) ==
    /\ status[waiter] = "idle"
    /\ requested' = [requested EXCEPT ![waiter] = size]
    /\ status' =
        [status EXCEPT ![waiter] =
            IF size > Capacity THEN "rejected"
            ELSE IF size <= UncommittedBudget THEN "admitted"
            ELSE "waiting"]
    /\ UNCHANGED budget

Retry(waiter) ==
    /\ status[waiter] = "waiting"
    /\ requested[waiter] <= UncommittedBudget
    /\ status' = [status EXCEPT ![waiter] = "admitted"]
    /\ UNCHANGED <<budget, requested>>

(***************************************************************************)
(* A resource refresh moves the dynamic budget anywhere under capacity,    *)
(* never above it.                                                         *)
(***************************************************************************)
Refresh ==
    /\ \E next \in 0..Capacity: budget' = next
    /\ UNCHANGED <<requested, status>>

Release(waiter) ==
    /\ status[waiter] = "admitted"
    /\ status' = [status EXCEPT ![waiter] = "idle"]
    /\ requested' = [requested EXCEPT ![waiter] = 0]
    /\ UNCHANGED budget

(***************************************************************************)
(* Cancellation and deadlines terminate a wait; rejected and cancelled     *)
(* are terminal for the submitted request.                                 *)
(***************************************************************************)
Cancel(waiter) ==
    /\ status[waiter] = "waiting"
    /\ status' = [status EXCEPT ![waiter] = "cancelled"]
    /\ UNCHANGED <<budget, requested>>

Reset(waiter) ==
    /\ status[waiter] \in {"rejected", "cancelled"}
    /\ status' = [status EXCEPT ![waiter] = "idle"]
    /\ requested' = [requested EXCEPT ![waiter] = 0]
    /\ UNCHANGED budget

Next ==
    \/ \E waiter \in Waiters: \E size \in Sizes: Submit(waiter, size)
    \/ \E waiter \in Waiters: Retry(waiter)
    \/ Refresh
    \/ \E waiter \in Waiters: Release(waiter)
    \/ \E waiter \in Waiters: Cancel(waiter)
    \/ \E waiter \in Waiters: Reset(waiter)

TypeOK ==
    /\ budget \in 0..Capacity
    /\ requested \in [Waiters -> 0..(Capacity + 1)]
    /\ status \in [Waiters -> Statuses]

BudgetNeverExceedsCapacity ==
    budget <= Capacity

(***************************************************************************)
(* The blocking property from the review: a statically unsatisfiable       *)
(* request never enters the waiting state, so no waiter polls forever for  *)
(* an admission that cannot come.                                          *)
(***************************************************************************)
OverCapacityNeverWaits ==
    \A waiter \in Waiters:
        status[waiter] = "waiting" => requested[waiter] <= Capacity

(***************************************************************************)
(* Terminal rejection is reserved for over-capacity requests; a request    *)
(* within capacity is never terminally rejected, only made to wait.        *)
(***************************************************************************)
RejectionIsOnlyForOverCapacity ==
    \A waiter \in Waiters:
        status[waiter] = "rejected" => requested[waiter] > Capacity

AdmittedNeverExceedsCapacity ==
    AdmittedBytes <= Capacity

Spec == Init /\ [][Next]_vars

=============================================================================
