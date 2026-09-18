--------------------- MODULE HawDBConcurrentSnapshots ---------------------
EXTENDS Integers, Naturals, Sequences

CONSTANT Readers, MaxEpoch

VARIABLES publishedEpoch, durableEpoch, writer, stagedEpoch, readerEpoch

vars == <<publishedEpoch, durableEpoch, writer, stagedEpoch, readerEpoch>>

Init ==
    /\ publishedEpoch = 0
    /\ durableEpoch = 0
    /\ writer = "none"
    /\ stagedEpoch = 0
    /\ readerEpoch = [reader \in Readers |-> -1]

BeginWrite ==
    /\ writer = "none"
    /\ publishedEpoch < MaxEpoch
    /\ writer' = "active"
    /\ stagedEpoch' = publishedEpoch + 1
    /\ UNCHANGED <<publishedEpoch, durableEpoch, readerEpoch>>

MakeDurable ==
    /\ writer = "active"
    /\ stagedEpoch = publishedEpoch + 1
    /\ durableEpoch' = stagedEpoch
    /\ UNCHANGED <<publishedEpoch, writer, stagedEpoch, readerEpoch>>

Publish ==
    /\ writer = "active"
    /\ durableEpoch >= stagedEpoch
    /\ publishedEpoch' = stagedEpoch
    /\ writer' = "none"
    /\ stagedEpoch' = 0
    /\ UNCHANGED <<durableEpoch, readerEpoch>>

BeginRead(reader) ==
    /\ readerEpoch[reader] = -1
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = publishedEpoch]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, writer, stagedEpoch>>

EndRead(reader) ==
    /\ readerEpoch[reader] >= 0
    /\ readerEpoch' = [readerEpoch EXCEPT ![reader] = -1]
    /\ UNCHANGED <<publishedEpoch, durableEpoch, writer, stagedEpoch>>

Crash ==
    /\ writer' = "none"
    /\ stagedEpoch' = 0
    /\ readerEpoch' = [reader \in Readers |-> -1]
    /\ UNCHANGED <<publishedEpoch, durableEpoch>>

Next ==
    \/ BeginWrite
    \/ MakeDurable
    \/ Publish
    \/ \E reader \in Readers: BeginRead(reader)
    \/ \E reader \in Readers: EndRead(reader)
    \/ Crash

DurableBeforePublish == publishedEpoch <= durableEpoch
WriterOwnsNextEpoch == writer = "active" => stagedEpoch = publishedEpoch + 1
ReadersSeePublishedSnapshots ==
    \A reader \in Readers:
        readerEpoch[reader] = -1 \/ readerEpoch[reader] <= publishedEpoch

CrashLeavesRecoverablePublication == publishedEpoch <= durableEpoch

Spec == Init /\ [][Next]_vars

=============================================================================
