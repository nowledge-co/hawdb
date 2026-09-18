-------------------- MODULE HawDBRuntimeAdmission --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Memory admission with a capacity and a dynamic budget. Capacity is       *)
(* stable with respect to sensed headroom, but a resource refresh may       *)
(* change it when the sensed host or cgroup policy ceiling changes; explicit *)
(* configuration remains an upper bound. The budget is further bounded by   *)
(* sensed availability. A request                                            *)
(* above current capacity terminates non-retryably before any dynamic gate. *)
(* A request within capacity but above the uncommitted budget waits. A      *)
(* capacity shrink does not revoke admitted work; it blocks further         *)
(* admission, and an affected waiter terminates on its next retry.          *)
(***************************************************************************)

CONSTANT MaxCapacity, Waiters

ASSUME /\ MaxCapacity \in Nat \ {0}
       /\ Waiters # {}

Sizes == 1..(MaxCapacity + 1)
Statuses == {"idle", "waiting", "admitted", "rejected", "cancelled"}

VARIABLES
    capacity,
    budget,
    requested,
    status

vars == <<capacity, budget, requested, status>>

AdmittedBytes ==
    LET admitted == {waiter \in Waiters: status[waiter] = "admitted"}
    IN LET Sum[chosen \in SUBSET admitted] ==
            IF chosen = {} THEN 0
            ELSE LET waiter == CHOOSE waiter \in chosen: TRUE
                 IN requested[waiter] + Sum[chosen \ {waiter}]
       IN Sum[admitted]

UncommittedBudget == budget - AdmittedBytes

Init ==
    /\ capacity \in 0..MaxCapacity
    /\ budget \in 0..capacity
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
            IF size > capacity THEN "rejected"
            ELSE IF size <= UncommittedBudget THEN "admitted"
            ELSE "waiting"]
    /\ UNCHANGED <<capacity, budget>>

Retry(waiter) ==
    /\ status[waiter] = "waiting"
    /\ IF requested[waiter] > capacity
          THEN status' = [status EXCEPT ![waiter] = "rejected"]
          ELSE /\ requested[waiter] <= UncommittedBudget
               /\ status' = [status EXCEPT ![waiter] = "admitted"]
    /\ UNCHANGED <<capacity, budget, requested>>

(***************************************************************************)
(* A resource refresh may move both capacity and budget, including to zero. *)
(* Existing admissions are not revoked when the policy ceiling shrinks.     *)
(***************************************************************************)
Refresh ==
    /\ \E next_capacity \in 0..MaxCapacity:
          \E next_budget \in 0..next_capacity:
            /\ capacity' = next_capacity
            /\ budget' = next_budget
    /\ UNCHANGED <<requested, status>>

Release(waiter) ==
    /\ status[waiter] = "admitted"
    /\ status' = [status EXCEPT ![waiter] = "idle"]
    /\ requested' = [requested EXCEPT ![waiter] = 0]
    /\ UNCHANGED <<capacity, budget>>

(***************************************************************************)
(* Cancellation and deadlines terminate a wait; rejected and cancelled     *)
(* are terminal for the submitted request.                                 *)
(***************************************************************************)
Cancel(waiter) ==
    /\ status[waiter] = "waiting"
    /\ status' = [status EXCEPT ![waiter] = "cancelled"]
    /\ UNCHANGED <<capacity, budget, requested>>

Reset(waiter) ==
    /\ status[waiter] \in {"rejected", "cancelled"}
    /\ status' = [status EXCEPT ![waiter] = "idle"]
    /\ requested' = [requested EXCEPT ![waiter] = 0]
    /\ UNCHANGED <<capacity, budget>>

Next ==
    \/ \E waiter \in Waiters: \E size \in Sizes: Submit(waiter, size)
    \/ \E waiter \in Waiters: Retry(waiter)
    \/ Refresh
    \/ \E waiter \in Waiters: Release(waiter)
    \/ \E waiter \in Waiters: Cancel(waiter)
    \/ \E waiter \in Waiters: Reset(waiter)

TypeOK ==
    /\ capacity \in 0..MaxCapacity
    /\ budget \in 0..capacity
    /\ requested \in [Waiters -> 0..(MaxCapacity + 1)]
    /\ status \in [Waiters -> Statuses]

BudgetNeverExceedsCapacity ==
    budget <= capacity

(***************************************************************************)
(* A statically unsatisfiable submission never enters the waiting state. A  *)
(* later capacity shrink may temporarily leave an over-capacity waiter, but *)
(* Retry is then enabled and can only terminate that waiter.                 *)
(***************************************************************************)
OverCapacityWaiterIsRejectable ==
    \A waiter \in Waiters:
        status[waiter] = "waiting" /\ requested[waiter] > capacity
            => ENABLED Retry(waiter)

SubmissionNeverWaitsAboveCapacity ==
    [][\A waiter \in Waiters:
        status[waiter] = "idle" /\ status'[waiter] = "waiting"
            => requested'[waiter] <= capacity']_vars

TerminalRejectionRequiresCurrentOverCapacity ==
    [][\A waiter \in Waiters:
        status[waiter] # "rejected" /\ status'[waiter] = "rejected"
            => requested'[waiter] > capacity']_vars

(***************************************************************************)
(* Refresh may create overcommit by lowering capacity below active work.    *)
(* Admission itself must never create it, and no admission may increase an  *)
(* existing overcommit. Release is the only way for admitted bytes to fall. *)
(***************************************************************************)
AdmissionNeverCreatesCapacityOvercommit ==
    [][AdmittedBytes' > AdmittedBytes
        => AdmittedBytes' <= capacity']_vars

CapacityOvercommitNeverGrows ==
    [][AdmittedBytes > capacity
        => AdmittedBytes' <= AdmittedBytes]_vars

Spec == Init /\ [][Next]_vars

=============================================================================
