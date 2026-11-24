-------------------- MODULE HawDBGraphBundleCarry --------------------
EXTENDS Naturals, FiniteSets

(* C/G/E describe physical base identity. Logical tips may advance beyond
   that base through a complete WAL suffix. Bytes, transaction framing and
   completed filesystem barriers are assumptions supplied by the companion
   models and executable qualification, not proved by this model. *)
CONSTANTS MaxGeneration, MaxCommit, ReuseDirtyGraph,
          ForgetCurrentGraph, ForgetReaderGraph, DropGraphSuffix,
          LoseGraphFence, ForgetReaderDelta
VARIABLES commit, graphEpoch, graphFence, history, liveGraph,
          current, previous, candidate, candidateTip, candidateGraph,
          reader, readerTip, readerGraph, graphs, rows, graphData,
          nextGeneration, synchronized, selectedCarriedSuffix
vars == <<commit, graphEpoch, graphFence, history, liveGraph,
          current, previous, candidate, candidateTip, candidateGraph,
          reader, readerTip, readerGraph, graphs, rows, graphData,
          nextGeneration, synchronized, selectedCarriedSuffix>>

Root(c, g, e, s, v) == [c |-> c, g |-> g, e |-> e, s |-> s, v |-> v]
Empty == Root(0, 0, 0, 0, 0)
Roots == {current, previous, candidate, reader}
GraphRoots == {r.g : r \in Roots}
RowRoots == {r.c : r \in Roots}

Init ==
    /\ commit = 0 /\ graphEpoch = 0 /\ graphFence = 0 /\ liveGraph = 0
    /\ history = [s \in 0..MaxCommit |-> 0]
    /\ current = Empty /\ previous = Empty /\ candidate = Empty
    /\ candidateTip = 0 /\ candidateGraph = 0
    /\ reader = Empty /\ readerTip = 0 /\ readerGraph = 0
    /\ graphs = {0} /\ rows = {0}
    /\ graphData = [g \in 0..MaxGeneration |-> 0]
    /\ nextGeneration = 1 /\ synchronized = FALSE
    /\ selectedCarriedSuffix = FALSE

Write(isGraph) ==
    /\ commit < MaxCommit
    /\ commit' = commit + 1
    /\ graphEpoch' = IF isGraph THEN commit + 1 ELSE graphEpoch
    /\ graphFence' = IF isGraph THEN commit + 1 ELSE graphFence
    /\ history' = [history EXCEPT ![commit + 1] = graphEpoch']
    /\ liveGraph' = graphEpoch'
    /\ UNCHANGED <<current, previous, candidate, candidateTip, candidateGraph,
                    reader, readerTip, readerGraph, graphs, rows, graphData,
                    nextGeneration, synchronized, selectedCarriedSuffix>>

Prepare ==
    /\ candidate = Empty /\ nextGeneration <= MaxGeneration
    /\ LET reuse == current.c > 0 /\ (ReuseDirtyGraph \/ graphFence <= current.s)
           g == IF reuse THEN current.g ELSE nextGeneration
       IN /\ candidate' = Root(nextGeneration, g,
                                  IF reuse THEN current.e ELSE commit,
                                  commit, graphEpoch)
          /\ graphs' = graphs \cup {g}
          /\ graphData' = IF reuse THEN graphData
                          ELSE [graphData EXCEPT ![g] = graphEpoch]
    /\ candidateTip' = commit /\ candidateGraph' = liveGraph
    /\ rows' = rows \cup {nextGeneration}
    /\ nextGeneration' = nextGeneration + 1
    /\ synchronized' = FALSE
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, reader, readerTip, readerGraph,
                    selectedCarriedSuffix>>

CatchUp ==
    /\ candidate # Empty /\ candidateTip < commit
    /\ candidateTip' = commit
    /\ candidateGraph' = IF DropGraphSuffix /\ candidate.g < candidate.c /\ candidate.v > 0
                          THEN candidateGraph ELSE history[commit]
    /\ synchronized' = FALSE
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, candidate, reader, readerTip, readerGraph,
                    graphs, rows, graphData, nextGeneration, selectedCarriedSuffix>>

Sync ==
    /\ candidate # Empty /\ ~synchronized /\ synchronized' = TRUE
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, candidate, candidateTip, candidateGraph,
                    reader, readerTip, readerGraph, graphs, rows, graphData,
                    nextGeneration, selectedCarriedSuffix>>

Select ==
    /\ candidate # Empty /\ synchronized /\ candidateTip = commit
    /\ previous' = current /\ current' = candidate /\ candidate' = Empty
    /\ liveGraph' = candidateGraph
    /\ graphFence' = IF LoseGraphFence /\ candidate.g < candidate.c
                         /\ candidate.v > 0 /\ candidateGraph > candidate.v
                     THEN candidate.s ELSE graphFence
    /\ candidateTip' = 0 /\ candidateGraph' = 0 /\ synchronized' = FALSE
    /\ selectedCarriedSuffix' = (selectedCarriedSuffix
          \/ (candidate.g < candidate.c /\ candidate.s < candidateTip
              /\ candidate.v > 0 /\ history[candidateTip] > candidate.v))
    /\ UNCHANGED <<commit, graphEpoch, history, reader, readerTip, readerGraph,
                    graphs, rows, graphData, nextGeneration>>

Discard ==
    /\ candidate # Empty /\ candidate' = Empty
    /\ candidateTip' = 0 /\ candidateGraph' = 0 /\ synchronized' = FALSE
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, reader, readerTip, readerGraph,
                    graphs, rows, graphData, nextGeneration, selectedCarriedSuffix>>

Pin ==
    /\ reader = Empty /\ current # Empty
    /\ reader' = current /\ readerTip' = commit
    /\ readerGraph' = IF ForgetReaderDelta /\ current.g < current.c /\ current.v > 0
                     THEN graphData[current.g] ELSE liveGraph
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, candidate, candidateTip, candidateGraph,
                    graphs, rows, graphData, nextGeneration, synchronized,
                    selectedCarriedSuffix>>
Release ==
    /\ reader # Empty /\ reader' = Empty /\ readerTip' = 0 /\ readerGraph' = 0
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, candidate, candidateTip, candidateGraph,
                    graphs, rows, graphData, nextGeneration, synchronized,
                    selectedCarriedSuffix>>

(* Reopening may conservatively raise the classification fence to the
   checkpoint base coverage, even when physical graph E is older. WAL replay
   restores later graph changes. Actual crash/loss choices belong to the
   companion automatic-checkpoint model. *)
RecoverFence ==
    /\ candidate = Empty /\ reader = Empty
    /\ graphFence' = IF graphEpoch > current.s THEN graphEpoch ELSE current.s
    /\ UNCHANGED <<commit, graphEpoch, history, liveGraph, current, previous,
                    candidate, candidateTip, candidateGraph, reader, readerTip,
                    readerGraph, graphs, rows, graphData, nextGeneration,
                    synchronized, selectedCarriedSuffix>>

RetainedGraphs ==
    {0, previous.g, candidate.g}
    \cup (IF ForgetCurrentGraph THEN {} ELSE {current.g})
    \cup (IF ForgetReaderGraph THEN {reader.c} ELSE {reader.g})
ReclaimGraph(g) ==
    /\ g \in graphs \ RetainedGraphs /\ graphs' = graphs \ {g}
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, candidate, candidateTip, candidateGraph,
                    reader, readerTip, readerGraph, rows, graphData,
                    nextGeneration, synchronized, selectedCarriedSuffix>>
ReclaimRow(c) ==
    /\ c \in rows \ (RowRoots \cup {0}) /\ rows' = rows \ {c}
    /\ UNCHANGED <<commit, graphEpoch, graphFence, history, liveGraph,
                    current, previous, candidate, candidateTip, candidateGraph,
                    reader, readerTip, readerGraph, graphs, graphData,
                    nextGeneration, synchronized, selectedCarriedSuffix>>

Next == Write(TRUE) \/ Write(FALSE) \/ Prepare \/ CatchUp \/ Sync \/ Select
        \/ Discard \/ Pin \/ Release \/ RecoverFence
        \/ (\E g \in 0..MaxGeneration: ReclaimGraph(g))
        \/ (\E c \in 0..MaxGeneration: ReclaimRow(c))
Spec == Init /\ [][Next]_vars

TypeOK == /\ commit \in 0..MaxCommit /\ graphEpoch \in 0..commit
          /\ graphFence \in 0..commit /\ liveGraph \in 0..commit
          /\ history \in [0..MaxCommit -> 0..MaxCommit]
          /\ graphs \subseteq 0..MaxGeneration /\ rows \subseteq 0..MaxGeneration
          /\ graphData \in [0..MaxGeneration -> 0..MaxCommit]
          /\ candidateTip \in 0..commit /\ readerTip \in 0..commit
          /\ candidateGraph \in 0..commit /\ readerGraph \in 0..commit
          /\ nextGeneration \in 1..(MaxGeneration + 1)
          /\ synchronized \in BOOLEAN /\ selectedCarriedSuffix \in BOOLEAN
GraphRootsRetained == GraphRoots \subseteq graphs
RowRootsRetained == RowRoots \subseteq rows
GraphMatchesCapturedSource ==
    \A r \in Roots: graphData[r.g] = r.v /\ r.v = history[r.s]
BindingsOrdered ==
    \A r \in Roots: r.g <= r.c /\ r.v <= r.e /\ r.e <= r.s /\ r.s <= commit
FreshRootsHaveGraph == \A r \in Roots: r.c > 0 => r.g > 0
GraphFenceCoversGraphChanges == graphEpoch <= graphFence
CandidateTipCoversBase == candidate # Empty => candidate.s <= candidateTip
CandidatePrefixComplete == candidate # Empty => candidateGraph = history[candidateTip]
ServingSnapshotMatchesTip == liveGraph = history[commit]
ReaderTipCoversBase == reader # Empty => reader.s <= readerTip
ReaderSnapshotMatchesTip == reader # Empty => readerGraph = history[readerTip]
NoCarriedSuffixSelection == ~selectedCarriedSuffix
=====================================================================
