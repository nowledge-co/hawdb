---------------- MODULE HawDBTransactionAdmissionLease ----------------
EXTENDS Naturals, FiniteSets
CONSTANTS Transactions, Capacity, RetainRequest
VARIABLES phase, callers, requests, callbacks, cancelled, durable
vars == <<phase, callers, requests, callbacks, cancelled, durable>>
Phases == {"idle", "active", "queued", "executed", "synced", "ready", "retired"}
Live == {t \in Transactions : phase[t] \in {"active", "queued", "executed", "synced", "ready"}}
Leases == callers \union requests
Init == /\ phase = [t \in Transactions |-> "idle"]
        /\ callers = {} /\ requests = {} /\ callbacks = {}
        /\ cancelled = {} /\ durable = {}
Admit(t) ==
    /\ phase[t] = "idle" /\ Cardinality(Leases) < Capacity
    /\ phase' = [phase EXCEPT ![t] = "active"]
    /\ callers' = callers \union {t}
    /\ UNCHANGED <<requests, callbacks, cancelled, durable>>
Enqueue(t) ==
    /\ phase[t] = "active"
    /\ phase' = [phase EXCEPT ![t] = "queued"]
    /\ requests' = IF RetainRequest THEN requests \union {t} ELSE requests
    /\ callbacks' = callbacks \union {t}
    /\ UNCHANGED <<callers, cancelled, durable>>
DropCaller(t) ==
    /\ t \in callers
    /\ callers' = callers \ {t}
    /\ phase' = IF phase[t] = "active" THEN [phase EXCEPT ![t] = "retired"] ELSE phase
    /\ UNCHANGED <<requests, callbacks, cancelled, durable>>
Cancel(t) ==
    /\ phase[t] \in {"active", "queued"} /\ t \notin cancelled
    /\ cancelled' = cancelled \union {t}
    /\ UNCHANGED <<phase, callers, requests, callbacks, durable>>
Execute(t) ==
    /\ phase[t] = "queued"
    /\ phase' = [phase EXCEPT ![t] = IF t \in cancelled THEN "ready" ELSE "executed"]
    /\ callbacks' = callbacks \ {t}
    /\ UNCHANGED <<callers, requests, cancelled, durable>>
Sync(t) ==
    /\ phase[t] = "executed"
    /\ phase' = [phase EXCEPT ![t] = "synced"]
    /\ durable' = durable \union {t}
    /\ UNCHANGED <<callers, requests, callbacks, cancelled>>
Complete(t) ==
    /\ phase[t] \in {"executed", "synced"}
    \* Executed -> ready abstracts rejection/sync failure, without success.
    /\ phase' = [phase EXCEPT ![t] = "ready"]
    /\ UNCHANGED <<callers, requests, callbacks, cancelled, durable>>
Retire(t) ==
    /\ phase[t] = "ready"
    /\ phase' = [phase EXCEPT ![t] = "retired"]
    /\ callers' = callers \ {t} /\ requests' = requests \ {t}
    /\ UNCHANGED <<callbacks, cancelled, durable>>
Next == \E t \in Transactions : Admit(t) \/ Enqueue(t) \/ DropCaller(t) \/ Cancel(t) \/ Execute(t) \/ Sync(t) \/ Complete(t) \/ Retire(t)
TypeInvariant ==
    /\ phase \in [Transactions -> Phases]
    /\ callers \subseteq Transactions /\ requests \subseteq Transactions
    /\ callbacks \subseteq Transactions /\ cancelled \subseteq Transactions
    /\ durable \subseteq Transactions
ProtectedUntilRetirement == Live \subseteq Leases
BoundedAdmission == Cardinality(Leases) <= Capacity
CancelledBeforeExecutionNotDurable == cancelled \intersect durable = {}
NoCallbackAfterExecution == \A t \in Transactions : phase[t] \in {"executed", "synced", "ready", "retired"} => t \notin callbacks
=============================================================================
