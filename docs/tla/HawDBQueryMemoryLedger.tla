-------------------- MODULE HawDBQueryMemoryLedger --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Query-owned memory is split into operator accounts, but every reserve is *)
(* checked atomically against both the account budget and one query root.    *)
(* Failure and cancellation release every lease. Materialized results may   *)
(* remain charged at the completion-profile snapshot; the execution scope   *)
(* then drops the lease. Returned host rows do not carry the query ledger.   *)
(***************************************************************************)

CONSTANT QueryBudget, AccountBudget, Accounts, ResultAccount,
         ChildParents, Merges, MutateSkipChildAdmission, MutateEarlyBackingRelease

ASSUME /\ QueryBudget \in Nat \ {0}
       /\ AccountBudget \in Nat \ {0}
       /\ Accounts # {}
       /\ ResultAccount \in Accounts
       /\ ChildParents \subseteq Accounts \ {ResultAccount}
       /\ Merges # {}
       /\ MutateSkipChildAdmission \in BOOLEAN
       /\ MutateEarlyBackingRelease \in BOOLEAN

Statuses == {"running", "returned", "succeeded", "failed", "cancelled"}

Children == Merges \X ChildParents

(* used contains direct leases only; child reservations are charged once to
   their parent by AccountUsed. Child payload growth never charges the root
   a second time. An owner represents the last account/clone; a lease may
   outlive that owner, including a zero-byte lease. *)
VARIABLES used, peak, status, capacity, payload, owners, leases

vars == <<used, peak, status, capacity, payload, owners, leases>>

RECURSIVE BackingFor(_)
BackingFor(children) ==
    IF children = {} THEN 0
    ELSE LET child == CHOOSE child \in children: TRUE
         IN capacity[child] + BackingFor(children \ {child})

AccountUsed(account) == used[account] +
    BackingFor({child \in Children: child[2] = account})

RECURSIVE TotalFor(_)
TotalFor(accounts) ==
    IF accounts = {} THEN 0
    ELSE LET account == CHOOSE account \in accounts: TRUE
         IN AccountUsed(account) + TotalFor(accounts \ {account})

TotalUsed == TotalFor(Accounts)

ZeroAccounts == [account \in Accounts |-> 0]
ZeroChildren == [child \in Children |-> 0]

ClearChildren ==
    /\ capacity' = ZeroChildren
    /\ payload' = ZeroChildren
    /\ owners' = {}
    /\ leases' = {}

Init ==
    /\ used = ZeroAccounts
    /\ peak = 0
    /\ status = "running"
    /\ capacity = ZeroChildren
    /\ payload = ZeroChildren
    /\ owners = {}
    /\ leases = {}

Reserve(account) ==
    /\ status = "running"
    /\ AccountUsed(account) < AccountBudget
    /\ TotalUsed < QueryBudget
    /\ used' = [used EXCEPT ![account] = @ + 1]
    /\ peak' = IF peak >= TotalUsed + 1 THEN peak ELSE TotalUsed + 1
    /\ UNCHANGED <<status, capacity, payload, owners, leases>>

Release(account) ==
    /\ status \in {"running", "returned"}
    /\ used[account] > 0
    /\ used' = [used EXCEPT ![account] = @ - 1]
    /\ UNCHANGED <<peak, status, capacity, payload, owners, leases>>

Transfer(source, target) ==
    /\ status = "running"
    /\ source # target
    /\ used[source] > 0
    /\ AccountUsed(target) < AccountBudget
    /\ used' = [used EXCEPT ![source] = @ - 1, ![target] = @ + 1]
    /\ UNCHANGED <<peak, status, capacity, payload, owners, leases>>

(* Historical "returned" state names the materialized completion snapshot,
   before accumulator drop. It does not mean a public QueryRows owns a lease. *)
ReturnMaterialized ==
    /\ status = "running"
    /\ \A account \in Accounts \ {ResultAccount}: AccountUsed(account) = 0
    /\ status' = "returned"
    /\ UNCHANGED <<used, peak, capacity, payload, owners, leases>>

CompleteStreaming ==
    /\ status = "running"
    /\ TotalUsed = 0
    /\ status' = "succeeded"
    /\ UNCHANGED <<used, peak, capacity, payload, owners, leases>>

DropReturnedResult ==
    /\ status = "returned"
    /\ used' = ZeroAccounts
    /\ status' = "succeeded"
    /\ ClearChildren
    /\ UNCHANGED peak

Fail ==
    /\ status \in {"running", "returned"}
    /\ used' = ZeroAccounts
    /\ status' = "failed"
    /\ ClearChildren
    /\ UNCHANGED peak

Cancel ==
    /\ status \in {"running", "returned"}
    /\ used' = ZeroAccounts
    /\ status' = "cancelled"
    /\ ClearChildren
    /\ UNCHANGED peak

(* Reservation and root admission linearize together. The implementation
   reserves blocking then staging; failed staging admission drops the first
   reservation. Independent children allow that intermediate state. *)
CreateChild(child, bytes) ==
    /\ status = "running"
    /\ capacity[child] = 0
    /\ child \notin (owners \cup leases)
    /\ MutateSkipChildAdmission \/
        /\ AccountUsed(child[2]) + bytes <= AccountBudget
        /\ TotalUsed + bytes <= QueryBudget
    /\ capacity' = [capacity EXCEPT ![child] = bytes]
    /\ owners' = owners \cup {child}
    /\ peak' = IF peak >= TotalUsed + bytes THEN peak ELSE TotalUsed + bytes
    /\ UNCHANGED <<used, status, payload, leases>>

AcquireChildLease(child) ==
    /\ status = "running"
    /\ child \in owners \ leases
    /\ leases' = leases \cup {child}
    /\ UNCHANGED <<used, peak, status, capacity, payload, owners>>

GrowChild(child) ==
    /\ status = "running"
    /\ child \in leases
    /\ payload[child] < capacity[child]
    /\ payload' = [payload EXCEPT ![child] = @ + 1]
    /\ UNCHANGED <<used, peak, status, capacity, owners, leases>>

ShrinkChild(child) ==
    /\ status = "running"
    /\ child \in leases
    /\ payload[child] > 0
    /\ payload' = [payload EXCEPT ![child] = @ - 1]
    /\ UNCHANGED <<used, peak, status, capacity, owners, leases>>

DropChildOwner(child) ==
    /\ status = "running"
    /\ child \in owners
    /\ owners' = owners \ {child}
    /\ capacity' = IF child \notin leases \/ MutateEarlyBackingRelease
                    THEN [capacity EXCEPT ![child] = 0] ELSE capacity
    /\ UNCHANGED <<used, peak, status, payload, leases>>

DropChildLease(child) ==
    /\ status = "running"
    /\ child \in leases
    /\ leases' = leases \ {child}
    /\ payload' = [payload EXCEPT ![child] = 0]
    /\ capacity' = IF child \notin owners
                    THEN [capacity EXCEPT ![child] = 0] ELSE capacity
    /\ UNCHANGED <<used, peak, status, owners>>

Next ==
    \/ \E child \in Children, bytes \in 1..AccountBudget: CreateChild(child, bytes)
    \/ \E child \in Children: AcquireChildLease(child)
    \/ \E child \in Children: GrowChild(child)
    \/ \E child \in Children: ShrinkChild(child)
    \/ \E child \in Children: DropChildOwner(child)
    \/ \E child \in Children: DropChildLease(child)
    \/ \E account \in Accounts: Reserve(account)
    \/ \E account \in Accounts: Release(account)
    \/ \E source, target \in Accounts: Transfer(source, target)
    \/ ReturnMaterialized
    \/ CompleteStreaming
    \/ DropReturnedResult
    \/ Fail
    \/ Cancel

TypeOK ==
    /\ used \in [Accounts -> 0..AccountBudget]
    /\ peak \in 0..QueryBudget
    /\ status \in Statuses
    /\ capacity \in [Children -> 0..AccountBudget]
    /\ payload \in [Children -> 0..AccountBudget]
    /\ owners \subseteq Children
    /\ leases \subseteq Children

RootBudgetBounded ==
    TotalUsed <= QueryBudget

EveryAccountBounded ==
    \A account \in Accounts: AccountUsed(account) <= AccountBudget

TerminalStateHasNoLease ==
    status \in {"succeeded", "failed", "cancelled"} =>
        /\ TotalUsed = 0
        /\ owners = {}
        /\ leases = {}

ReturnedStateOnlyOwnsAdmittedMemory ==
    status = "returned" => TotalUsed <= QueryBudget

ReturnedStateOwnsOnlyResult ==
    status = "returned" =>
        \A account \in Accounts \ {ResultAccount}: AccountUsed(account) = 0

ChildLeaseBacked ==
    \A child \in Children:
        /\ (capacity[child] > 0) <=> (child \in owners \cup leases)
        /\ payload[child] <= capacity[child]
        /\ child \notin leases => payload[child] = 0

PeakCoversReservations == TotalUsed <= peak

Spec == Init /\ [][Next]_vars

=============================================================================
