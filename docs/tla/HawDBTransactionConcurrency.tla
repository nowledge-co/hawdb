-------------------- MODULE HawDBTransactionConcurrency --------------------
EXTENDS FiniteSets, Integers, Naturals, Sequences

CONSTANT Transactions, Readers, Keys, LockTokens, MaxEpoch

VARIABLES publishedEpoch,
          durableEpoch,
          txPhase,
          txMode,
          baseEpoch,
          commitEpoch,
          publisher,
          heldShared,
          heldExclusive,
          lockTokens,
          savedShared,
          savedExclusive,
          savedLockTokens,
          statementOpen,
          waitFor,
          readerEpoch

vars == <<publishedEpoch,
          durableEpoch,
          txPhase,
          txMode,
          baseEpoch,
          commitEpoch,
          publisher,
          heldShared,
          heldExclusive,
          lockTokens,
          savedShared,
          savedExclusive,
          savedLockTokens,
          statementOpen,
          waitFor,
          readerEpoch>>

TxPhases == {"idle", "active", "prepared", "durable", "committed", "conflict", "deadlock", "aborted"}
TxModes == {"none", "optimistic", "pessimistic"}
LockModes == {"shared", "exclusive"}
LockSpans == (SUBSET Keys) \ {{}}

OtherTransactions(tx) == Transactions \ {tx}

ExclusiveBlockers(tx, span) ==
    {owner \in OtherTransactions(tx) : heldExclusive[owner] \intersect span # {}}

AllBlockers(tx, span) ==
    {owner \in OtherTransactions(tx) :
        (heldShared[owner] \union heldExclusive[owner]) \intersect span # {}}

Blockers(tx, span, lockMode) ==
    IF lockMode = "shared"
      THEN ExclusiveBlockers(tx, span)
      ELSE AllBlockers(tx, span)

WaitPaths ==
    UNION {[1..length -> Transactions] :
            length \in 2..(Cardinality(Transactions) + 1)}

HasWaitPath(from, to) ==
    \E path \in WaitPaths:
        /\ path[1] = from
        /\ path[Len(path)] = to
        /\ \A index \in 1..(Len(path) - 1):
            path[index + 1] \in waitFor[path[index]]

WouldDeadlock(waiter, owners) ==
    waiter \in owners \/ \E owner \in owners: HasWaitPath(owner, waiter)

RemoveDependency(graph, transaction) ==
    [tx \in Transactions |->
        IF tx = transaction
          THEN {}
          ELSE graph[tx] \ {transaction}]

Init ==
    /\ publishedEpoch = 0
    /\ durableEpoch = 0
    /\ txPhase = [tx \in Transactions |-> "idle"]
    /\ txMode = [tx \in Transactions |-> "none"]
    /\ baseEpoch = [tx \in Transactions |-> -1]
    /\ commitEpoch = [tx \in Transactions |-> -1]
    /\ publisher = "none"
    /\ heldShared = [tx \in Transactions |-> {}]
    /\ heldExclusive = [tx \in Transactions |-> {}]
    /\ lockTokens = [tx \in Transactions |-> {}]
    /\ savedShared = [tx \in Transactions |-> {}]
    /\ savedExclusive = [tx \in Transactions |-> {}]
    /\ savedLockTokens = [tx \in Transactions |-> {}]
    /\ statementOpen = [tx \in Transactions |-> FALSE]
    /\ waitFor = [tx \in Transactions |-> {}]
    /\ readerEpoch = [reader \in Readers |-> -1]

Begin(tx, mode) ==
    /\ txPhase[tx] = "idle"
    /\ mode \in {"optimistic", "pessimistic"}
    /\ publisher = "none"
    /\ publishedEpoch < MaxEpoch
    /\ txPhase' = [txPhase EXCEPT ![tx] = "active"]
    /\ txMode' = [txMode EXCEPT ![tx] = mode]
    /\ baseEpoch' = [baseEpoch EXCEPT ![tx] = publishedEpoch]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, commitEpoch, publisher,
                    heldShared, heldExclusive, lockTokens, savedShared,
                    savedExclusive, savedLockTokens, statementOpen, waitFor,
                    readerEpoch>>

BeginStatement(tx) ==
    /\ txPhase[tx] = "active"
    /\ txMode[tx] = "pessimistic"
    /\ ~statementOpen[tx]
    /\ statementOpen' = [statementOpen EXCEPT ![tx] = TRUE]
    /\ savedShared' = [savedShared EXCEPT ![tx] = heldShared[tx]]
    /\ savedExclusive' = [savedExclusive EXCEPT ![tx] = heldExclusive[tx]]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![tx] = lockTokens[tx]]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, heldShared, heldExclusive,
                    lockTokens, waitFor, readerEpoch>>

FinishStatement(tx) ==
    /\ txPhase[tx] = "active"
    /\ statementOpen[tx]
    /\ statementOpen' = [statementOpen EXCEPT ![tx] = FALSE]
    /\ savedShared' = [savedShared EXCEPT ![tx] = {}]
    /\ savedExclusive' = [savedExclusive EXCEPT ![tx] = {}]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![tx] = {}]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, heldShared, heldExclusive,
                    lockTokens, waitFor, readerEpoch>>

RollbackStatement(tx) ==
    /\ txPhase[tx] = "active"
    /\ statementOpen[tx]
    /\ heldShared' = [heldShared EXCEPT ![tx] = savedShared[tx]]
    /\ heldExclusive' = [heldExclusive EXCEPT ![tx] = savedExclusive[tx]]
    /\ lockTokens' = [lockTokens EXCEPT ![tx] = savedLockTokens[tx]]
    /\ statementOpen' = [statementOpen EXCEPT ![tx] = FALSE]
    /\ savedShared' = [savedShared EXCEPT ![tx] = {}]
    /\ savedExclusive' = [savedExclusive EXCEPT ![tx] = {}]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![tx] = {}]
    /\ waitFor' = [waitFor EXCEPT ![tx] = {}]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, readerEpoch>>

AcquireLock(tx, span, lockMode) ==
    LET usedTokens == UNION {lockTokens[owner] : owner \in Transactions}
        freeTokens == LockTokens \ usedTokens IN
    /\ txPhase[tx] = "active"
    /\ span \in LockSpans
    /\ lockMode \in LockModes
    /\ waitFor[tx] = {}
    /\ Blockers(tx, span, lockMode) = {}
    /\ freeTokens # {}
    /\ IF lockMode = "shared"
          THEN ~(span \subseteq (heldShared[tx] \union heldExclusive[tx]))
          ELSE ~(span \subseteq heldExclusive[tx])
    /\ IF lockMode = "shared"
          THEN /\ heldShared' = [heldShared EXCEPT ![tx] = @ \union span]
               /\ UNCHANGED heldExclusive
          ELSE /\ heldExclusive' = [heldExclusive EXCEPT ![tx] = @ \union span]
               /\ UNCHANGED heldShared
    /\ lockTokens' =
        [lockTokens EXCEPT ![tx] = @ \union {CHOOSE token \in freeTokens : TRUE}]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, savedShared, savedExclusive,
                    savedLockTokens, statementOpen, waitFor, readerEpoch>>

EscalateLock(tx, lockMode) ==
    LET spans == heldShared[tx] \union heldExclusive[tx] IN
    /\ txPhase[tx] = "active"
    /\ lockTokens[tx] # {}
    /\ spans # {}
    /\ spans # Keys
    /\ lockMode \in LockModes
    /\ IF lockMode = "shared"
          THEN /\ heldExclusive[tx] = {}
               /\ ExclusiveBlockers(tx, Keys) = {}
               /\ heldShared' = [heldShared EXCEPT ![tx] = Keys]
               /\ UNCHANGED heldExclusive
          ELSE /\ AllBlockers(tx, Keys) = {}
               /\ heldShared' = [heldShared EXCEPT ![tx] = {}]
               /\ heldExclusive' = [heldExclusive EXCEPT ![tx] = Keys]
    /\ lockTokens' =
        [lockTokens EXCEPT ![tx] = {CHOOSE token \in @ : TRUE}]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, savedShared, savedExclusive,
                    savedLockTokens, statementOpen, waitFor, readerEpoch>>

RejectLockBudget(tx) ==
    LET usedTokens == UNION {lockTokens[owner] : owner \in Transactions} IN
    /\ txPhase[tx] = "active"
    /\ usedTokens = LockTokens
    /\ txPhase' = [txPhase EXCEPT ![tx] = "aborted"]
    /\ heldShared' = [heldShared EXCEPT ![tx] = {}]
    /\ heldExclusive' = [heldExclusive EXCEPT ![tx] = {}]
    /\ lockTokens' = [lockTokens EXCEPT ![tx] = {}]
    /\ savedShared' = [savedShared EXCEPT ![tx] = {}]
    /\ savedExclusive' = [savedExclusive EXCEPT ![tx] = {}]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![tx] = {}]
    /\ statementOpen' = [statementOpen EXCEPT ![tx] = FALSE]
    /\ waitFor' = RemoveDependency(waitFor, tx)
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txMode, baseEpoch,
                    commitEpoch, publisher, readerEpoch>>

RegisterWait(waiter, span, lockMode) ==
    LET owners == Blockers(waiter, span, lockMode) IN
    /\ txPhase[waiter] = "active"
    /\ span \in LockSpans
    /\ lockMode \in LockModes
    /\ owners # {}
    /\ waitFor[waiter] = {}
    /\ ~WouldDeadlock(waiter, owners)
    /\ waitFor' = [waitFor EXCEPT ![waiter] = owners]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, heldShared, heldExclusive, lockTokens,
                    savedShared, savedExclusive, savedLockTokens, statementOpen,
                    readerEpoch>>

ReleaseWait(waiter) ==
    /\ waitFor[waiter] # {}
    /\ waitFor' = [waitFor EXCEPT ![waiter] = {}]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, heldShared, heldExclusive, lockTokens,
                    savedShared, savedExclusive, savedLockTokens, statementOpen,
                    readerEpoch>>

RejectDeadlock(waiter, span, lockMode) ==
    LET owners == Blockers(waiter, span, lockMode) IN
    /\ txPhase[waiter] = "active"
    /\ span \in LockSpans
    /\ lockMode \in LockModes
    /\ owners # {}
    /\ waitFor[waiter] = {}
    /\ WouldDeadlock(waiter, owners)
    /\ txPhase' = [txPhase EXCEPT ![waiter] = "deadlock"]
    /\ heldShared' = [heldShared EXCEPT ![waiter] = {}]
    /\ heldExclusive' = [heldExclusive EXCEPT ![waiter] = {}]
    /\ lockTokens' = [lockTokens EXCEPT ![waiter] = {}]
    /\ savedShared' = [savedShared EXCEPT ![waiter] = {}]
    /\ savedExclusive' = [savedExclusive EXCEPT ![waiter] = {}]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![waiter] = {}]
    /\ statementOpen' = [statementOpen EXCEPT ![waiter] = FALSE]
    /\ waitFor' = RemoveDependency(waitFor, waiter)
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txMode, baseEpoch,
                    commitEpoch, publisher, readerEpoch>>

PrepareCommit(tx) ==
    /\ txPhase[tx] = "active"
    /\ publisher = "none"
    /\ waitFor[tx] = {}
    /\ ~statementOpen[tx]
    /\ IF txMode[tx] = "optimistic"
          THEN /\ baseEpoch[tx] = publishedEpoch
               /\ heldExclusive[tx] = Keys
          ELSE TRUE
    /\ txPhase' = [txPhase EXCEPT ![tx] = "prepared"]
    /\ publisher' = tx
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txMode, baseEpoch,
                    commitEpoch, heldShared, heldExclusive, lockTokens,
                    savedShared, savedExclusive, savedLockTokens, statementOpen,
                    waitFor, readerEpoch>>

RejectOptimisticConflict(tx) ==
    /\ txPhase[tx] = "active"
    /\ txMode[tx] = "optimistic"
    /\ baseEpoch[tx] # publishedEpoch
    /\ waitFor[tx] = {}
    /\ txPhase' = [txPhase EXCEPT ![tx] = "conflict"]
    /\ heldShared' = [heldShared EXCEPT ![tx] = {}]
    /\ heldExclusive' = [heldExclusive EXCEPT ![tx] = {}]
    /\ lockTokens' = [lockTokens EXCEPT ![tx] = {}]
    /\ savedShared' = [savedShared EXCEPT ![tx] = {}]
    /\ savedExclusive' = [savedExclusive EXCEPT ![tx] = {}]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![tx] = {}]
    /\ statementOpen' = [statementOpen EXCEPT ![tx] = FALSE]
    /\ waitFor' = RemoveDependency(waitFor, tx)
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txMode, baseEpoch,
                    commitEpoch, publisher, readerEpoch>>

MakeDurable(tx) ==
    /\ publisher = tx
    /\ txPhase[tx] = "prepared"
    /\ durableEpoch = publishedEpoch
    /\ durableEpoch' = publishedEpoch + 1
    /\ txPhase' = [txPhase EXCEPT ![tx] = "durable"]
    /\ UNCHANGED <<publishedEpoch, txMode, baseEpoch, commitEpoch, publisher,
                    heldShared, heldExclusive, lockTokens, savedShared,
                    savedExclusive, savedLockTokens, statementOpen, waitFor,
                    readerEpoch>>

Publish(tx) ==
    /\ publisher = tx
    /\ txPhase[tx] = "durable"
    /\ durableEpoch = publishedEpoch + 1
    /\ publishedEpoch' = durableEpoch
    /\ txPhase' = [txPhase EXCEPT ![tx] = "committed"]
    /\ commitEpoch' = [commitEpoch EXCEPT ![tx] = durableEpoch]
    /\ publisher' = "none"
    /\ heldShared' = [heldShared EXCEPT ![tx] = {}]
    /\ heldExclusive' = [heldExclusive EXCEPT ![tx] = {}]
    /\ lockTokens' = [lockTokens EXCEPT ![tx] = {}]
    /\ savedShared' = [savedShared EXCEPT ![tx] = {}]
    /\ savedExclusive' = [savedExclusive EXCEPT ![tx] = {}]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![tx] = {}]
    /\ statementOpen' = [statementOpen EXCEPT ![tx] = FALSE]
    /\ waitFor' = RemoveDependency(waitFor, tx)
    /\ UNCHANGED <<durableEpoch, txMode, baseEpoch, readerEpoch>>

Rollback(tx) ==
    /\ txPhase[tx] = "active"
    /\ txPhase' = [txPhase EXCEPT ![tx] = "aborted"]
    /\ heldShared' = [heldShared EXCEPT ![tx] = {}]
    /\ heldExclusive' = [heldExclusive EXCEPT ![tx] = {}]
    /\ lockTokens' = [lockTokens EXCEPT ![tx] = {}]
    /\ savedShared' = [savedShared EXCEPT ![tx] = {}]
    /\ savedExclusive' = [savedExclusive EXCEPT ![tx] = {}]
    /\ savedLockTokens' = [savedLockTokens EXCEPT ![tx] = {}]
    /\ statementOpen' = [statementOpen EXCEPT ![tx] = FALSE]
    /\ waitFor' = RemoveDependency(waitFor, tx)
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txMode, baseEpoch,
                    commitEpoch, publisher, readerEpoch>>

BeginRead(reader) ==
    /\ readerEpoch[reader] = -1
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = publishedEpoch]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, heldShared, heldExclusive, lockTokens,
                    savedShared, savedExclusive, savedLockTokens, statementOpen,
                    waitFor>>

EndRead(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = -1]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, txPhase, txMode, baseEpoch,
                    commitEpoch, publisher, heldShared, heldExclusive, lockTokens,
                    savedShared, savedExclusive, savedLockTokens, statementOpen,
                    waitFor>>

Crash ==
    /\ publishedEpoch' = durableEpoch
    /\ txPhase' =
        [tx \in Transactions |->
            IF txPhase[tx] = "durable"
              THEN "committed"
              ELSE IF txPhase[tx] \in {"active", "prepared"}
                THEN "aborted"
                ELSE txPhase[tx]]
    /\ commitEpoch' =
        [tx \in Transactions |->
            IF txPhase[tx] = "durable" THEN durableEpoch ELSE commitEpoch[tx]]
    /\ publisher' = "none"
    /\ heldShared' = [tx \in Transactions |-> {}]
    /\ heldExclusive' = [tx \in Transactions |-> {}]
    /\ lockTokens' = [tx \in Transactions |-> {}]
    /\ savedShared' = [tx \in Transactions |-> {}]
    /\ savedExclusive' = [tx \in Transactions |-> {}]
    /\ savedLockTokens' = [tx \in Transactions |-> {}]
    /\ statementOpen' = [tx \in Transactions |-> FALSE]
    /\ waitFor' = [tx \in Transactions |-> {}]
    /\ readerEpoch' = [reader \in Readers |-> -1]
    /\ UNCHANGED <<durableEpoch, txMode, baseEpoch>>

Next ==
    \/ \E tx \in Transactions, mode \in {"optimistic", "pessimistic"}: Begin(tx, mode)
    \/ \E tx \in Transactions: BeginStatement(tx)
    \/ \E tx \in Transactions: FinishStatement(tx)
    \/ \E tx \in Transactions: RollbackStatement(tx)
    \/ \E tx \in Transactions, span \in LockSpans, lockMode \in LockModes:
        AcquireLock(tx, span, lockMode)
    \/ \E tx \in Transactions, lockMode \in LockModes: EscalateLock(tx, lockMode)
    \/ \E tx \in Transactions: RejectLockBudget(tx)
    \/ \E waiter \in Transactions, span \in LockSpans, lockMode \in LockModes:
        RegisterWait(waiter, span, lockMode)
    \/ \E waiter \in Transactions: ReleaseWait(waiter)
    \/ \E waiter \in Transactions, span \in LockSpans, lockMode \in LockModes:
        RejectDeadlock(waiter, span, lockMode)
    \/ \E tx \in Transactions: PrepareCommit(tx)
    \/ \E tx \in Transactions: RejectOptimisticConflict(tx)
    \/ \E tx \in Transactions: MakeDurable(tx)
    \/ \E tx \in Transactions: Publish(tx)
    \/ \E tx \in Transactions: Rollback(tx)
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Crash

TypeInvariant ==
    /\ publishedEpoch \in 0..MaxEpoch
    /\ durableEpoch \in 0..MaxEpoch
    /\ txPhase \in [Transactions -> TxPhases]
    /\ txMode \in [Transactions -> TxModes]
    /\ baseEpoch \in [Transactions -> -1..MaxEpoch]
    /\ commitEpoch \in [Transactions -> -1..MaxEpoch]
    /\ publisher \in Transactions \union {"none"}
    /\ heldShared \in [Transactions -> SUBSET Keys]
    /\ heldExclusive \in [Transactions -> SUBSET Keys]
    /\ lockTokens \in [Transactions -> SUBSET LockTokens]
    /\ savedShared \in [Transactions -> SUBSET Keys]
    /\ savedExclusive \in [Transactions -> SUBSET Keys]
    /\ savedLockTokens \in [Transactions -> SUBSET LockTokens]
    /\ statementOpen \in [Transactions -> BOOLEAN]
    /\ waitFor \in [Transactions -> SUBSET Transactions]
    /\ readerEpoch \in [Readers -> -1..MaxEpoch]

DurableBeforePublish ==
    /\ publishedEpoch <= durableEpoch
    /\ durableEpoch <= publishedEpoch + 1

SinglePublisher ==
    (publisher = "none") <=>
        (\A tx \in Transactions: txPhase[tx] \notin {"prepared", "durable"})

PublisherOwnsCommitPipeline ==
    publisher # "none" => txPhase[publisher] \in {"prepared", "durable"}

LockCompatibility ==
    \A left, right \in Transactions:
        left # right =>
            heldExclusive[left] \intersect
                (heldShared[right] \union heldExclusive[right]) = {}

LockTokensAreExclusive ==
    \A left, right \in Transactions:
        left # right => lockTokens[left] \intersect lockTokens[right] = {}

LockOwnershipHasOneOrMoreBudgetTokens ==
    \A tx \in Transactions:
        (heldShared[tx] \union heldExclusive[tx] = {}) <=> (lockTokens[tx] = {})

LockTableIsBounded ==
    Cardinality(UNION {lockTokens[tx] : tx \in Transactions}) <= Cardinality(LockTokens)

StatementSavepointWasPreviouslyHeld ==
    \A tx \in Transactions:
        statementOpen[tx] =>
            /\ savedShared[tx] \subseteq (heldShared[tx] \union heldExclusive[tx])
            /\ savedExclusive[tx] \subseteq heldExclusive[tx]
            /\ savedLockTokens[tx] = {} \/ lockTokens[tx] # {}

OptimisticPublisherOwnsDatabaseLock ==
    \A tx \in Transactions:
        /\ txMode[tx] = "optimistic"
        /\ txPhase[tx] \in {"prepared", "durable"}
        => heldExclusive[tx] = Keys

OptimisticFirstCommitterWins ==
    \A tx \in Transactions:
        /\ txMode[tx] = "optimistic"
        /\ txPhase[tx] = "committed"
        => commitEpoch[tx] = baseEpoch[tx] + 1

ConflictRequiresStaleSnapshot ==
    \A tx \in Transactions:
        txPhase[tx] = "conflict" =>
            /\ txMode[tx] = "optimistic"
            /\ baseEpoch[tx] < publishedEpoch

CommitEpochsAreUnique ==
    \A left, right \in Transactions:
        /\ left # right
        /\ txPhase[left] = "committed"
        /\ txPhase[right] = "committed"
        => commitEpoch[left] # commitEpoch[right]

ReadersSeeOnlyPublishedSnapshots ==
    \A reader \in Readers:
        readerEpoch[reader] = -1 \/ readerEpoch[reader] <= publishedEpoch

DurableCommitIsRecoverable ==
    durableEpoch > publishedEpoch =>
        /\ publisher # "none"
        /\ txPhase[publisher] = "durable"

WaitForGraphIsAcyclic ==
    ~\E tx \in Transactions: HasWaitPath(tx, tx)

DeadlockVictimReleasesDependencies ==
    \A victim \in Transactions:
        txPhase[victim] = "deadlock" =>
            /\ waitFor[victim] = {}
            /\ heldShared[victim] = {}
            /\ heldExclusive[victim] = {}
            /\ \A waiter \in Transactions: victim \notin waitFor[waiter]

Spec == Init /\ [][Next]_vars

=============================================================================
