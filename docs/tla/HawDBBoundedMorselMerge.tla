-------------------- MODULE HawDBBoundedMorselMerge --------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* Workers may complete morsels out of order, but issuance stays inside one *)
(* capacity-sized window ahead of the coordinator's consumed prefix.        *)
(* Cancellation or a worker panic clears every admitted intermediate.       *)
(***************************************************************************)

CONSTANT TotalMorsels, Capacity

ASSUME /\ TotalMorsels \in Nat \ {0}
       /\ Capacity \in Nat \ {0}

Ordinals == 0..(TotalMorsels - 1)
Statuses == {"running", "succeeded", "failed", "cancelled"}

Prefix(count) == IF count = 0 THEN {} ELSE 0..(count - 1)

VARIABLES nextIssue, expected, active, channel, reorder, emitted, status

vars == <<nextIssue, expected, active, channel, reorder, emitted, status>>

Init ==
    /\ nextIssue = 0
    /\ expected = 0
    /\ active = {}
    /\ channel = {}
    /\ reorder = {}
    /\ emitted = {}
    /\ status = "running"

Issue ==
    /\ status = "running"
    /\ nextIssue < TotalMorsels
    /\ nextIssue < expected + Capacity
    /\ active' = active \cup {nextIssue}
    /\ nextIssue' = nextIssue + 1
    /\ UNCHANGED <<expected, channel, reorder, emitted, status>>

WorkerCompletes(ordinal) ==
    /\ status = "running"
    /\ ordinal \in active
    /\ Cardinality(channel) < Capacity
    /\ active' = active \ {ordinal}
    /\ channel' = channel \cup {ordinal}
    /\ UNCHANGED <<nextIssue, expected, reorder, emitted, status>>

CoordinatorReceives(ordinal) ==
    /\ status = "running"
    /\ ordinal \in channel
    /\ channel' = channel \ {ordinal}
    /\ reorder' = reorder \cup {ordinal}
    /\ UNCHANGED <<nextIssue, expected, active, emitted, status>>

EmitExpected ==
    /\ status = "running"
    /\ expected \in reorder
    /\ reorder' = reorder \ {expected}
    /\ emitted' = emitted \cup {expected}
    /\ expected' = expected + 1
    /\ UNCHANGED <<nextIssue, active, channel, status>>

Finish ==
    /\ status = "running"
    /\ expected = TotalMorsels
    /\ active = {}
    /\ channel = {}
    /\ reorder = {}
    /\ status' = "succeeded"
    /\ UNCHANGED <<nextIssue, expected, active, channel, reorder, emitted>>

WorkerPanics ==
    /\ status = "running"
    /\ active # {}
    /\ active' = {}
    /\ channel' = {}
    /\ reorder' = {}
    /\ status' = "failed"
    /\ UNCHANGED <<nextIssue, expected, emitted>>

Cancel ==
    /\ status = "running"
    /\ active' = {}
    /\ channel' = {}
    /\ reorder' = {}
    /\ status' = "cancelled"
    /\ UNCHANGED <<nextIssue, expected, emitted>>

Terminal ==
    /\ status # "running"
    /\ UNCHANGED vars

Next ==
    \/ Issue
    \/ \E ordinal \in Ordinals: WorkerCompletes(ordinal)
    \/ \E ordinal \in Ordinals: CoordinatorReceives(ordinal)
    \/ EmitExpected
    \/ Finish
    \/ WorkerPanics
    \/ Cancel
    \/ Terminal

TypeOK ==
    /\ nextIssue \in 0..TotalMorsels
    /\ expected \in 0..TotalMorsels
    /\ active \subseteq Ordinals
    /\ channel \subseteq Ordinals
    /\ reorder \subseteq Ordinals
    /\ emitted \subseteq Ordinals
    /\ status \in Statuses

OrderedPrefix == emitted = Prefix(expected)

IssuedPartition ==
    /\ status = "running" =>
        /\ Prefix(nextIssue) = emitted \cup active \cup channel \cup reorder
        /\ active \cap channel = {}
        /\ active \cap reorder = {}
        /\ active \cap emitted = {}
        /\ channel \cap reorder = {}
        /\ channel \cap emitted = {}
        /\ reorder \cap emitted = {}
    /\ status = "succeeded" => emitted = Ordinals

AdmissionWindowBounded == nextIssue <= expected + Capacity

ChannelBounded == Cardinality(channel) <= Capacity

ReorderBounded == Cardinality(reorder) <= Capacity

TerminalHasNoBufferedOutput ==
    status \in {"succeeded", "failed", "cancelled"} =>
        /\ active = {}
        /\ channel = {}
        /\ reorder = {}

EventuallyTerminates == <>(status # "running")

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

=============================================================================
