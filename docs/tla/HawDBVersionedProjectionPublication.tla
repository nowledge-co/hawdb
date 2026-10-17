---------------- MODULE HawDBVersionedProjectionPublication ----------------
EXTENDS Integers, FiniteSets

CONSTANTS MaxEpoch, SkipInstance, IgnoreBindingIncarnation, OmitScope,
          SkipBarrier, DropFence, SkipSourceDurability, PublishEarly,
          ReclaimStaged, CancelCommitted, PrunePinned, SkipArtifactDurability,
          EarlyWatermark

(* One prepared full projection, three row identities, and two artifact parts.
   a/b belong to the projected domain; u is unrelated. b starts absent.
   Atomic artifact/selector sync actions abstract successful file/directory
   durability barriers. Graph writes may be acknowledged without syncing.
   This finite protocol model does not model filesystem bytes or query plans. *)
Rows == {"a", "b", "u"}
Barriers == {"database", "schema"}
Keys == Rows \cup Barriers \cup {"scope"}
Parts == {1, 2}
ZeroStamps == [k \in Keys |-> 0]
InitialRows == [k \in Rows |-> IF k = "b" THEN -1 ELSE 0]
OpenPhases == {"captured", "building", "staged", "validated", "installing", "committed"}

VARIABLES db, job, pin, leased, fence, durableParts, selectorWritten,
          selectorDurable, visible, watermark, acknowledged, outcome,
          stalePublished, crashes, unrelatedDuringPrepare, disjointPublished
vars == <<db, job, pin, leased, fence, durableParts, selectorWritten,
          selectorDurable, visible, watermark, acknowledged, outcome,
          stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

Projection(rows, query) ==
    {<<k, rows[k]>> : k \in (IF query = "point" THEN {"a"}
                             ELSE {r \in {"a", "b"}: rows[r] # -1})}
ReadKeys == IF job.query = "point" THEN {"a"}
            ELSE {r \in {"a", "b"}: job.rows[r] # -1}
DependencyKeys == ReadKeys \cup
    (IF job.query = "scan" /\ ~OmitScope THEN {"scope"} ELSE {}) \cup
    (IF SkipBarrier THEN {} ELSE Barriers)
IdentityValid ==
    /\ SkipInstance \/ job.instance = db.instance
    /\ IF IgnoreBindingIncarnation
          THEN job.binding % 2 = db.binding % 2
          ELSE job.binding = db.binding
IndexValid == IdentityValid /\ \A k \in DependencyKeys: db.stamps[k] <= job.epoch
SourceCovered == SkipSourceDurability \/ job.epoch <= db.durableEpoch
(* Independent full-history stamps are never pruned and do not omit scans. *)
OracleValid ==
    /\ job.instance = db.instance
    /\ job.binding = db.binding
    /\ \A k \in (ReadKeys \cup Barriers \cup
                    (IF job.query = "scan" THEN {"scope"} ELSE {})):
          db.oracle[k] <= job.epoch
    /\ Projection(job.rows, job.query) = Projection(db.rows, job.query)

Init ==
    /\ db = [epoch |-> 0, rows |-> InitialRows, stamps |-> ZeroStamps,
              oracle |-> ZeroStamps, durableEpoch |-> 0,
              durableRows |-> InitialRows, instance |-> 0, binding |-> 0]
    /\ job = [phase |-> "idle", epoch |-> 0, rows |-> InitialRows,
               instance |-> 0, binding |-> 0, query |-> "point",
               parts |-> {}, flushed |-> {}]
    /\ pin = FALSE /\ leased = FALSE /\ fence = FALSE
    /\ durableParts = {} /\ selectorWritten = FALSE /\ selectorDurable = FALSE
    /\ visible = FALSE /\ watermark = 0 /\ acknowledged = FALSE
    /\ outcome = "none" /\ stalePublished = FALSE /\ crashes = 0
    /\ unrelatedDuringPrepare = FALSE /\ disjointPublished = FALSE

Begin(query) ==
    /\ job.phase = "idle" /\ crashes = 0
    /\ job' = [job EXCEPT !.phase = "captured", !.epoch = db.epoch,
                 !.rows = db.rows, !.instance = db.instance,
                 !.binding = db.binding, !.query = query]
    /\ pin' = TRUE /\ leased' = TRUE
    /\ UNCHANGED <<db, fence, durableParts, selectorWritten, selectorDurable,
                    visible, watermark, acknowledged, outcome, stalePublished,
                    crashes, unrelatedDuringPrepare, disjointPublished>>

GraphWrite(key, value, sync, broad) ==
    /\ ~fence \/ DropFence
    /\ db.epoch < MaxEpoch /\ crashes = 0
    /\ key \in Rows /\ value \in {-1, 0, 1} /\ db.rows[key] # value
    /\ key # "b" => value # -1
    /\ broad => key = "a"
    /\ LET next == db.epoch + 1
           rows == [db.rows EXCEPT ![key] = value]
           membership == key = "b" /\ ((db.rows[key] = -1) # (value = -1))
           keys == (IF broad THEN {"database"} ELSE {key}) \cup
                   (IF membership THEN {"scope"} ELSE {})
           stamp(s) == [k \in Keys |-> IF k \in keys THEN next ELSE s[k]]
       IN db' = [db EXCEPT !.epoch = next, !.rows = rows,
                  !.stamps = stamp(db.stamps), !.oracle = stamp(db.oracle),
                  !.durableEpoch = IF sync THEN next ELSE @,
                  !.durableRows = IF sync THEN rows ELSE @]
    /\ unrelatedDuringPrepare' = (unrelatedDuringPrepare \/
          (key = "u" /\ job.phase \in {"captured", "building", "staged"}))
    /\ UNCHANGED <<job, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, disjointPublished>>

SchemaWrite(sync) ==
    /\ (~fence \/ DropFence) /\ db.epoch < MaxEpoch /\ crashes = 0
    /\ LET next == db.epoch + 1
       IN db' = [db EXCEPT !.epoch = next, !.stamps["schema"] = next,
                  !.oracle["schema"] = next,
                  !.durableEpoch = IF sync THEN next ELSE @,
                  !.durableRows = IF sync THEN db.rows ELSE @]
    /\ UNCHANGED <<job, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

ReplaceInstance ==
    /\ (~fence \/ DropFence) /\ db.instance = 0 /\ crashes = 0
    /\ db' = [db EXCEPT !.instance = 1]
    /\ UNCHANGED <<job, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

Rebind ==
    /\ (~fence \/ DropFence) /\ db.binding < 2 /\ crashes = 0
    /\ db' = [db EXCEPT !.binding = @ + 1]
    /\ UNCHANGED <<job, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

SyncSource ==
    /\ db.durableEpoch < db.epoch /\ crashes = 0
    /\ db' = [db EXCEPT !.durableEpoch = db.epoch, !.durableRows = db.rows]
    /\ UNCHANGED <<job, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

Prune(key) ==
    /\ db.stamps[key] > 0 /\ crashes = 0
    /\ PrunePinned \/ ~pin \/ db.stamps[key] < job.epoch
    /\ db' = [db EXCEPT !.stamps[key] = 0]
    /\ UNCHANGED <<job, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

BuildPart(part) ==
    /\ job.phase \in {"captured", "building"} /\ part \notin job.parts
    /\ pin /\ leased
    /\ job' = [job EXCEPT !.parts = @ \cup {part}, !.phase = "building"]
    /\ UNCHANGED <<db, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

FlushPart(part) ==
    /\ leased /\ part \in job.parts \ durableParts
    /\ durableParts' = durableParts \cup {part}
    /\ job' = [job EXCEPT !.flushed = @ \cup {part}]
    /\ UNCHANGED <<db, pin, leased, fence, selectorWritten, selectorDurable,
                    visible, watermark, acknowledged, outcome, stalePublished,
                    crashes, unrelatedDuringPrepare, disjointPublished>>

FinishStage ==
    /\ job.phase = "building" /\ job.parts = Parts
    /\ job' = [job EXCEPT !.phase = "staged"]
    /\ UNCHANGED <<db, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

Validate ==
    /\ job.phase = "staged" /\ IndexValid /\ SourceCovered
    /\ job' = [job EXCEPT !.phase = "validated"] /\ fence' = TRUE
    /\ UNCHANGED <<db, pin, leased, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, acknowledged, outcome,
                    stalePublished, crashes, unrelatedDuringPrepare, disjointPublished>>

Reject ==
    /\ job.phase = "staged" /\ ~(IndexValid /\ SourceCovered)
    /\ job' = [job EXCEPT !.phase = "rejected"]
    /\ pin' = FALSE /\ leased' = FALSE /\ outcome' = "rejected"
    /\ UNCHANGED <<db, fence, durableParts, selectorWritten, selectorDurable,
                    visible, watermark, acknowledged, stalePublished, crashes,
                    unrelatedDuringPrepare, disjointPublished>>

WriteSelector ==
    /\ job.phase = "validated" /\ fence
    /\ job' = [job EXCEPT !.phase = "installing"]
    /\ selectorWritten' = TRUE
    /\ visible' = IF PublishEarly THEN TRUE ELSE visible
    /\ watermark' = IF EarlyWatermark THEN job.epoch ELSE watermark
    /\ UNCHANGED <<db, pin, leased, fence, durableParts, selectorDurable,
                    acknowledged, outcome, stalePublished, crashes,
                    unrelatedDuringPrepare, disjointPublished>>

SyncSelector ==
    /\ job.phase = "installing" /\ selectorWritten /\ fence
    /\ SkipArtifactDurability \/ durableParts = Parts
    /\ selectorDurable' = TRUE /\ watermark' = job.epoch
    /\ stalePublished' = (stalePublished \/ ~OracleValid)
    /\ disjointPublished' = (unrelatedDuringPrepare /\ OracleValid)
    /\ job' = [job EXCEPT !.phase = "committed"]
    /\ UNCHANGED <<db, pin, leased, fence, durableParts, selectorWritten,
                    visible, acknowledged, outcome, crashes, unrelatedDuringPrepare>>

MakeVisible ==
    /\ job.phase = "committed" /\ selectorDurable
    /\ visible' = TRUE /\ fence' = FALSE /\ pin' = FALSE /\ leased' = FALSE
    /\ job' = [job EXCEPT !.phase = "published"]
    /\ UNCHANGED <<db, durableParts, selectorWritten, selectorDurable,
                    watermark, acknowledged, outcome, stalePublished, crashes,
                    unrelatedDuringPrepare, disjointPublished>>

Respond ==
    /\ job.phase = "published" /\ outcome = "none"
    /\ acknowledged' = TRUE /\ outcome' = "ok"
    /\ job' = [job EXCEPT !.phase = "done"]
    /\ UNCHANGED <<db, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, stalePublished, crashes,
                    unrelatedDuringPrepare, disjointPublished>>

Cancel ==
    /\ job.phase \in OpenPhases /\ (~selectorDurable \/ CancelCommitted)
    /\ job' = [job EXCEPT !.phase = "cancelled"] /\ outcome' = "cancelled"
    /\ pin' = FALSE /\ leased' = FALSE /\ fence' = FALSE
    /\ selectorWritten' = selectorDurable
    /\ UNCHANGED <<db, durableParts, selectorDurable, visible, watermark,
                    acknowledged, stalePublished, crashes,
                    unrelatedDuringPrepare, disjointPublished>>

Crash ==
    /\ crashes = 0
    /\ db' = [db EXCEPT !.epoch = db.durableEpoch, !.rows = db.durableRows,
                !.stamps = ZeroStamps, !.oracle = ZeroStamps, !.instance = @ + 1]
    /\ crashes' = 1 /\ visible' = selectorDurable
    /\ selectorWritten' = selectorDurable
    /\ job' = [job EXCEPT !.phase = "crashed"]
    /\ pin' = FALSE /\ leased' = FALSE /\ fence' = FALSE
    /\ outcome' = IF selectorDurable /\ ~acknowledged THEN "lost" ELSE outcome
    /\ watermark' = IF selectorDurable THEN watermark ELSE 0
    /\ UNCHANGED <<durableParts, selectorDurable, acknowledged, stalePublished,
                    unrelatedDuringPrepare, disjointPublished>>

RetryLostResponse ==
    /\ crashes = 1 /\ selectorDurable /\ outcome = "lost"
    /\ outcome' = "ok" /\ acknowledged' = TRUE
    /\ job' = [job EXCEPT !.phase = "done"]
    /\ UNCHANGED <<db, pin, leased, fence, durableParts, selectorWritten,
                    selectorDurable, visible, watermark, stalePublished, crashes,
                    unrelatedDuringPrepare, disjointPublished>>

Reclaim(part) ==
    /\ part \in durableParts /\ ~selectorDurable
    /\ ~leased \/ ReclaimStaged
    /\ durableParts' = durableParts \ {part}
    /\ UNCHANGED <<db, job, pin, leased, fence, selectorWritten, selectorDurable,
                    visible, watermark, acknowledged, outcome, stalePublished,
                    crashes, unrelatedDuringPrepare, disjointPublished>>

Next == (\E q \in {"point", "scan"}: Begin(q))
        \/ (\E k \in Rows, v \in {-1, 0, 1}, s \in BOOLEAN, b \in BOOLEAN:
              GraphWrite(k, v, s, b))
        \/ (\E s \in BOOLEAN: SchemaWrite(s))
        \/ ReplaceInstance \/ Rebind \/ SyncSource
        \/ (\E k \in Keys: Prune(k))
        \/ (\E p \in Parts: BuildPart(p) \/ FlushPart(p) \/ Reclaim(p))
        \/ FinishStage \/ Validate \/ Reject \/ WriteSelector \/ SyncSelector
        \/ MakeVisible \/ Respond \/ Cancel \/ Crash \/ RetryLostResponse

TypeOK ==
    /\ db.epoch \in 0..MaxEpoch /\ db.durableEpoch \in 0..db.epoch
    /\ db.rows \in [Rows -> {-1, 0, 1}] /\ db.durableRows \in [Rows -> {-1, 0, 1}]
    /\ db.stamps \in [Keys -> 0..MaxEpoch] /\ db.oracle \in [Keys -> 0..MaxEpoch]
    /\ db.instance \in 0..2 /\ db.binding \in 0..2
    /\ job.epoch \in 0..MaxEpoch /\ job.rows \in [Rows -> {-1, 0, 1}]
    /\ job.instance \in 0..1 /\ job.binding \in 0..2
    /\ job.query \in {"point", "scan"} /\ job.parts \subseteq Parts
    /\ job.flushed \subseteq job.parts /\ durableParts \subseteq Parts
    /\ {pin, leased, fence, selectorWritten, selectorDurable, visible,
          acknowledged, stalePublished, unrelatedDuringPrepare, disjointPublished} \subseteq BOOLEAN
    /\ watermark \in 0..MaxEpoch /\ crashes \in 0..1
    /\ outcome \in {"none", "ok", "cancelled", "lost", "rejected"}
NoStalePublication == ~stalePublished
ArtifactsBeforeSelector == selectorDurable => durableParts = Parts
DurableBeforeVisible == (visible \/ acknowledged) => selectorDurable
DurableSourceBeforeProjection == selectorDurable => job.epoch <= db.durableEpoch
ReceiptHasCompleteGeneration == watermark > 0 => selectorDurable /\ durableParts = Parts
StagingRetained == leased => job.flushed \subseteq durableParts
ResourcesUntilDecision == job.phase \in OpenPhases => pin /\ leased
NoCommittedCancellation == selectorDurable => outcome # "cancelled"
NoDisjointPublication == ~disjointPublished
NoRecoveredLostResponse == ~(crashes = 1 /\ acknowledged /\ selectorDurable /\ job.phase = "done")
=============================================================================
