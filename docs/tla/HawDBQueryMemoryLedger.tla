-------------------- MODULE HawDBQueryMemoryLedger --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Query-owned memory is split into operator accounts, but every reserve is *)
(* checked atomically against both the account budget and one query root.    *)
(* Failure and cancellation release every lease. Materialized results may   *)
(* remain charged after execution returns until ownership leaves the query   *)
(* result boundary; streaming completion cannot retain such a lease.         *)
(***************************************************************************)

CONSTANT QueryBudget, AccountBudget, Accounts, ResultAccount

ASSUME /\ QueryBudget \in Nat \ {0}
       /\ AccountBudget \in Nat \ {0}
       /\ Accounts # {}
       /\ ResultAccount \in Accounts

Statuses == {"running", "returned", "succeeded", "failed", "cancelled"}

VARIABLES used, peak, status

vars == <<used, peak, status>>

RECURSIVE TotalFor(_)
TotalFor(accounts) ==
    IF accounts = {} THEN 0
    ELSE LET account == CHOOSE account \in accounts: TRUE
         IN used[account] + TotalFor(accounts \ {account})

TotalUsed == TotalFor(Accounts)

ZeroAccounts == [account \in Accounts |-> 0]

Init ==
    /\ used = ZeroAccounts
    /\ peak = 0
    /\ status = "running"

Reserve(account) ==
    /\ status = "running"
    /\ used[account] < AccountBudget
    /\ TotalUsed < QueryBudget
    /\ used' = [used EXCEPT ![account] = @ + 1]
    /\ peak' = IF peak >= TotalUsed + 1 THEN peak ELSE TotalUsed + 1
    /\ UNCHANGED status

Release(account) ==
    /\ status \in {"running", "returned"}
    /\ used[account] > 0
    /\ used' = [used EXCEPT ![account] = @ - 1]
    /\ UNCHANGED <<peak, status>>

Transfer(source, target) ==
    /\ status = "running"
    /\ source # target
    /\ used[source] > 0
    /\ used[target] < AccountBudget
    /\ used' = [used EXCEPT ![source] = @ - 1, ![target] = @ + 1]
    /\ UNCHANGED <<peak, status>>

ReturnMaterialized ==
    /\ status = "running"
    /\ \A account \in Accounts \ {ResultAccount}: used[account] = 0
    /\ status' = "returned"
    /\ UNCHANGED <<used, peak>>

CompleteStreaming ==
    /\ status = "running"
    /\ TotalUsed = 0
    /\ status' = "succeeded"
    /\ UNCHANGED <<used, peak>>

DropReturnedResult ==
    /\ status = "returned"
    /\ used' = ZeroAccounts
    /\ status' = "succeeded"
    /\ UNCHANGED peak

Fail ==
    /\ status \in {"running", "returned"}
    /\ used' = ZeroAccounts
    /\ status' = "failed"
    /\ UNCHANGED peak

Cancel ==
    /\ status \in {"running", "returned"}
    /\ used' = ZeroAccounts
    /\ status' = "cancelled"
    /\ UNCHANGED peak

Next ==
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

RootBudgetBounded ==
    TotalUsed <= QueryBudget

EveryAccountBounded ==
    \A account \in Accounts: used[account] <= AccountBudget

TerminalStateHasNoLease ==
    status \in {"succeeded", "failed", "cancelled"} => TotalUsed = 0

ReturnedStateOnlyOwnsAdmittedMemory ==
    status = "returned" => TotalUsed <= QueryBudget

ReturnedStateOwnsOnlyResult ==
    status = "returned" =>
        \A account \in Accounts \ {ResultAccount}: used[account] = 0

Spec == Init /\ [][Next]_vars

=============================================================================
