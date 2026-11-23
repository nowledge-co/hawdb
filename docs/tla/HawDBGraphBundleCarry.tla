-------------------- MODULE HawDBGraphBundleCarry --------------------
EXTENDS Naturals, FiniteSets

(* Physical checkpoint C, graph bundle G and logical source epoch E are
   independent. Byte validation and completed filesystem barriers are abstract
   preconditions; HawDBAutomaticCheckpoint qualifies those separately. *)
CONSTANTS MaxGeneration, MaxCommit, ReuseDirtyGraph,
          ForgetCurrentGraph, ForgetReaderGraph
VARIABLES commit, graphEpoch, current, previous, candidate, reader,
          graphs, rows, graphData, nextGeneration, synchronized
vars == <<commit, graphEpoch, current, previous, candidate, reader,
          graphs, rows, graphData, nextGeneration, synchronized>>

Root(c, g, e, s, v) == [c |-> c, g |-> g, e |-> e, s |-> s, v |-> v]
Empty == Root(0, 0, 0, 0, 0)
Roots == {current, previous, candidate, reader}
GraphRoots == {r.g : r \in Roots}
RowRoots == {r.c : r \in Roots}

Init ==
    /\ commit = 0 /\ graphEpoch = 0
    /\ current = Empty /\ previous = Empty
    /\ candidate = Empty /\ reader = Empty
    /\ graphs = {0} /\ rows = {0}
    /\ graphData = [g \in 0..MaxGeneration |-> 0]
    /\ nextGeneration = 1 /\ synchronized = FALSE

Write(isGraph) ==
    /\ commit < MaxCommit
    /\ commit' = commit + 1
    /\ graphEpoch' = IF isGraph THEN commit + 1 ELSE graphEpoch
    /\ UNCHANGED <<current, previous, candidate, reader, graphs, rows,
                    graphData, nextGeneration, synchronized>>

Prepare ==
    /\ candidate = Empty /\ nextGeneration <= MaxGeneration
    /\ LET reuse == ReuseDirtyGraph \/ graphEpoch <= current.s
           g == IF reuse THEN current.g ELSE nextGeneration
       IN /\ candidate' = Root(nextGeneration, g,
                                  IF reuse THEN current.e ELSE commit,
                                  commit, graphEpoch)
          /\ graphs' = graphs \cup {g}
          /\ graphData' = IF reuse THEN graphData
                          ELSE [graphData EXCEPT ![g] = graphEpoch]
    /\ rows' = rows \cup {nextGeneration}
    /\ nextGeneration' = nextGeneration + 1
    /\ synchronized' = FALSE
    /\ UNCHANGED <<commit, graphEpoch, current, previous, reader>>

Sync ==
    /\ candidate # Empty /\ ~synchronized
    /\ synchronized' = TRUE
    /\ UNCHANGED <<commit, graphEpoch, current, previous, candidate, reader,
                    graphs, rows, graphData, nextGeneration>>

Select ==
    /\ candidate # Empty /\ synchronized /\ candidate.s = commit
    /\ previous' = current /\ current' = candidate /\ candidate' = Empty
    /\ synchronized' = FALSE
    /\ UNCHANGED <<commit, graphEpoch, reader, graphs, rows,
                    graphData, nextGeneration>>

Discard ==
    /\ candidate # Empty /\ candidate' = Empty /\ synchronized' = FALSE
    /\ UNCHANGED <<commit, graphEpoch, current, previous, reader,
                    graphs, rows, graphData, nextGeneration>>

Pin ==
    /\ reader = Empty /\ current # Empty /\ reader' = current
    /\ UNCHANGED <<commit, graphEpoch, current, previous, candidate,
                    graphs, rows, graphData, nextGeneration, synchronized>>
Release ==
    /\ reader # Empty /\ reader' = Empty
    /\ UNCHANGED <<commit, graphEpoch, current, previous, candidate,
                    graphs, rows, graphData, nextGeneration, synchronized>>

RetainedGraphs ==
    {0, previous.g, candidate.g}
    \cup (IF ForgetCurrentGraph THEN {} ELSE {current.g})
    \cup (IF ForgetReaderGraph THEN {reader.c} ELSE {reader.g})
ReclaimGraph(g) ==
    /\ g \in graphs \ RetainedGraphs /\ graphs' = graphs \ {g}
    /\ UNCHANGED <<commit, graphEpoch, current, previous, candidate, reader,
                    rows, graphData, nextGeneration, synchronized>>
ReclaimRow(c) ==
    /\ c \in rows \ (RowRoots \cup {0}) /\ rows' = rows \ {c}
    /\ UNCHANGED <<commit, graphEpoch, current, previous, candidate, reader,
                    graphs, graphData, nextGeneration, synchronized>>

Next == Write(TRUE) \/ Write(FALSE) \/ Prepare \/ Sync \/ Select \/ Discard
        \/ Pin \/ Release
        \/ (\E g \in 0..MaxGeneration: ReclaimGraph(g))
        \/ (\E c \in 0..MaxGeneration: ReclaimRow(c))
Spec == Init /\ [][Next]_vars

TypeOK == /\ commit \in 0..MaxCommit /\ graphEpoch \in 0..commit
          /\ graphs \subseteq 0..MaxGeneration
          /\ rows \subseteq 0..MaxGeneration
          /\ nextGeneration \in 1..(MaxGeneration + 1)
          /\ synchronized \in BOOLEAN
GraphRootsRetained == GraphRoots \subseteq graphs
RowRootsRetained == RowRoots \subseteq rows
GraphMatchesCapturedSource == \A r \in Roots: graphData[r.g] = r.v
BindingsOrdered == \A r \in Roots: r.g <= r.c /\ r.v <= r.e /\ r.e <= r.s
=====================================================================
