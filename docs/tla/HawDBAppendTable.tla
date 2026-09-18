-------------------------- MODULE HawDBAppendTable --------------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A strict append table admits a bounded non-empty row batch only when     *)
(* every order key is above the visible watermark of its partition. The    *)
(* complete batch is written to the WAL, made durable, and then published  *)
(* atomically. Recovery exposes an exact durable batch prefix. Rejected     *)
(* duplicate or out-of-order batches do not change canonical state.        *)
(*                                                                         *)
(* The mutation constants are FALSE in the target configuration. Separate  *)
(* negative-control configurations turn one on and must violate the named  *)
(* invariant, proving that the model checker exercises each safety edge.   *)
(***************************************************************************)

CONSTANTS
    Partitions,
    MaxOrder,
    MaxBatchRows,
    MutatePublishBeforeSync,
    MutateSkipWatermark,
    MutatePartialRecovery

ASSUME /\ Partitions # {}
       /\ MaxOrder \in Nat \ {0}
       /\ MaxBatchRows \in Nat \ {0}
       /\ MutatePublishBeforeSync \in BOOLEAN
       /\ MutateSkipWatermark \in BOOLEAN
       /\ MutatePartialRecovery \in BOOLEAN

Orders == 1..MaxOrder
Phases == {"idle", "staged", "durable"}
BatchType == [partition : Partitions, rows : SUBSET Orders]

EmptyRows == [partition \in Partitions |-> {}]

MaxValue(values) ==
    IF values = {}
    THEN 0
    ELSE CHOOSE candidate \in values:
        \A value \in values: value <= candidate

HistoryRows(history, count) ==
    [partition \in Partitions |->
        UNION {
            history[index].rows :
                index \in {
                    candidate \in 1..count :
                        history[candidate].partition = partition
                }
        }]

AppendRows(rows, partition, batchRows) ==
    [rows EXCEPT ![partition] = @ \cup batchRows]

HistoryIsStrict(history) ==
    \A earlier, later \in DOMAIN history:
        earlier < later
        /\ history[earlier].partition = history[later].partition
        => \A old \in history[earlier].rows:
            \A new \in history[later].rows: old < new

VARIABLES
    phase,
    stagedPartition,
    stagedRows,
    durableBatches,
    durableRows,
    visibleBatchCount,
    visibleRows,
    lastRejected

vars == <<
    phase,
    stagedPartition,
    stagedRows,
    durableBatches,
    durableRows,
    visibleBatchCount,
    visibleRows,
    lastRejected
>>

Init ==
    /\ phase = "idle"
    /\ stagedPartition \in Partitions
    /\ stagedRows = {}
    /\ durableBatches = <<>>
    /\ durableRows = EmptyRows
    /\ visibleBatchCount = 0
    /\ visibleRows = EmptyRows
    /\ lastRejected = FALSE

StageAppend(partition, batchRows) ==
    /\ phase = "idle"
    /\ partition \in Partitions
    /\ batchRows \subseteq Orders
    /\ batchRows # {}
    /\ Cardinality(batchRows) <= MaxBatchRows
    /\ (MutateSkipWatermark
        \/ \A row \in batchRows: row > MaxValue(visibleRows[partition]))
    /\ phase' = "staged"
    /\ stagedPartition' = partition
    /\ stagedRows' = batchRows
    /\ lastRejected' = FALSE
    /\ UNCHANGED <<
        durableBatches,
        durableRows,
        visibleBatchCount,
        visibleRows
        >>

RejectAppend(partition, batchRows) ==
    /\ phase = "idle"
    /\ partition \in Partitions
    /\ batchRows \subseteq Orders
    /\ batchRows # {}
    /\ Cardinality(batchRows) <= MaxBatchRows
    /\ \E row \in batchRows: row <= MaxValue(visibleRows[partition])
    /\ lastRejected' = TRUE
    /\ UNCHANGED <<
        phase,
        stagedPartition,
        stagedRows,
        durableBatches,
        durableRows,
        visibleBatchCount,
        visibleRows
        >>

SyncWal ==
    /\ phase = "staged"
    /\ LET batch == [partition |-> stagedPartition, rows |-> stagedRows]
       IN /\ durableBatches' = Append(durableBatches, batch)
          /\ durableRows' = AppendRows(durableRows, stagedPartition, stagedRows)
    /\ phase' = "durable"
    /\ UNCHANGED <<
        stagedPartition,
        stagedRows,
        visibleBatchCount,
        visibleRows,
        lastRejected
        >>

PublishDurableBatch ==
    /\ phase = "durable"
    /\ visibleBatchCount' = Len(durableBatches)
    /\ visibleRows' = durableRows
    /\ phase' = "idle"
    /\ stagedRows' = {}
    /\ lastRejected' = FALSE
    /\ UNCHANGED <<stagedPartition, durableBatches, durableRows>>

PublishUnsyncedBatch ==
    /\ MutatePublishBeforeSync
    /\ phase = "staged"
    /\ visibleRows' = AppendRows(visibleRows, stagedPartition, stagedRows)
    /\ visibleBatchCount' = visibleBatchCount
    /\ phase' = "idle"
    /\ stagedRows' = {}
    /\ lastRejected' = FALSE
    /\ UNCHANGED <<stagedPartition, durableBatches, durableRows>>

ExactCrashRecover ==
    /\ phase \in Phases
    /\ phase' = "idle"
    /\ stagedRows' = {}
    /\ visibleBatchCount' = Len(durableBatches)
    /\ visibleRows' = durableRows
    /\ lastRejected' = FALSE
    /\ UNCHANGED <<stagedPartition, durableBatches, durableRows>>

PartialCrashRecover ==
    /\ MutatePartialRecovery
    /\ phase \in Phases
    /\ \E partition \in Partitions:
        \E row \in durableRows[partition]:
            /\ phase' = "idle"
            /\ stagedRows' = {}
            /\ visibleBatchCount' = Len(durableBatches)
            /\ visibleRows' =
                [durableRows EXCEPT ![partition] = @ \ {row}]
            /\ lastRejected' = FALSE
            /\ UNCHANGED <<stagedPartition, durableBatches, durableRows>>

CrashRecover == ExactCrashRecover \/ PartialCrashRecover

Next ==
    \/ \E partition \in Partitions:
        \E batchRows \in SUBSET Orders: StageAppend(partition, batchRows)
    \/ \E partition \in Partitions:
        \E batchRows \in SUBSET Orders: RejectAppend(partition, batchRows)
    \/ SyncWal
    \/ PublishDurableBatch
    \/ PublishUnsyncedBatch
    \/ CrashRecover

TypeOK ==
    /\ phase \in Phases
    /\ stagedPartition \in Partitions
    /\ stagedRows \subseteq Orders
    /\ durableBatches \in Seq(BatchType)
    /\ durableRows \in [Partitions -> SUBSET Orders]
    /\ visibleBatchCount \in 0..Len(durableBatches)
    /\ visibleRows \in [Partitions -> SUBSET Orders]
    /\ lastRejected \in BOOLEAN

DurableRowsMatchHistory ==
    durableRows = HistoryRows(durableBatches, Len(durableBatches))

VisibleRowsAreDurablePrefix ==
    visibleRows = HistoryRows(durableBatches, visibleBatchCount)

DurableHistoryIsStrict == HistoryIsStrict(durableBatches)

VisibleDoesNotLeadDurability == visibleBatchCount <= Len(durableBatches)

Spec == Init /\ [][Next]_vars

=============================================================================
