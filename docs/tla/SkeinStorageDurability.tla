---------------------- MODULE SkeinStorageDurability ----------------------
EXTENDS Integers, Naturals, FiniteSets

(***************************************************************************)
(* This model covers the default SyncOnEveryWrite durable storage path.    *)
(* One logical WAL record represents one atomic mutation batch.             *)
(* A durable WAL decision may be recovered even when the caller did not     *)
(* observe its acknowledgement.                                             *)
(*                                                                          *)
(* The binary WAL frames each record as a fragment chain (FULL, or          *)
(* FIRST..MIDDLE*..LAST) inside fixed-size blocks; every fragment carries   *)
(* a type-masked, generation-bound checksum. The durable tail is one of:    *)
(* tornTail, an incomplete fragment chain at end of file and the only       *)
(* doctor-repairable state; diskStatus "corrupt", a checksum- or            *)
(* sequence-invalid complete chain that fails closed wherever it sits;      *)
(* or staleTail, a fragment carrying a stale WAL generation past the        *)
(* logical tail, which recyclable-log discipline reads as end of log,       *)
(* equivalent to clean EOF. Block-aligned resynchronization locates         *)
(* damage but never skips it.                                               *)
(***************************************************************************)

CONSTANT Owners, MaxCommit, MaxGeneration

ASSUME /\ Owners # {}
       /\ MaxCommit \in Nat
       /\ MaxGeneration \in Nat

Modes == {"closed", "running", "poisoned", "crashed", "failed"}
PendingPhases == {"none", "appended", "durable", "applied"}
CheckpointPhases == {"idle", "building", "checkpoint_durable", "wal_durable"}
Generations == 0..MaxGeneration
Epochs == 0..MaxCommit
CommitEpochs == 1..MaxCommit
WalRecords == [generation : Generations, epoch : CommitEpochs]

VARIABLES
    mode,
    leases,
    volatileEpoch,
    durableEpoch,
    acknowledgedEpoch,
    activeGeneration,
    manifestCheckpointEpoch,
    manifestReplayLsn,
    checkpointByGeneration,
    walStartByGeneration,
    walRecords,
    tornTail,
    staleTail,
    diskStatus,
    pendingPhase,
    pendingEpoch,
    checkpointPhase,
    buildGeneration,
    buildEpoch

vars == <<
    mode,
    leases,
    volatileEpoch,
    durableEpoch,
    acknowledgedEpoch,
    activeGeneration,
    manifestCheckpointEpoch,
    manifestReplayLsn,
    checkpointByGeneration,
    walStartByGeneration,
    walRecords,
    tornTail,
    staleTail,
    diskStatus,
    pendingPhase,
    pendingEpoch,
    checkpointPhase,
    buildGeneration,
    buildEpoch
>>

WalRecord(generation, epoch) == [generation |-> generation, epoch |-> epoch]

ActiveWalEpochs ==
    {epoch \in CommitEpochs:
        WalRecord(activeGeneration, epoch) \in walRecords}

Init ==
    /\ mode = "closed"
    /\ leases = {}
    /\ volatileEpoch = 0
    /\ durableEpoch = 0
    /\ acknowledgedEpoch = 0
    /\ activeGeneration = 0
    /\ manifestCheckpointEpoch = 0
    /\ manifestReplayLsn = 1
    /\ checkpointByGeneration =
        [generation \in Generations |-> IF generation = 0 THEN 0 ELSE -1]
    /\ walStartByGeneration =
        [generation \in Generations |-> IF generation = 0 THEN 1 ELSE -1]
    /\ walRecords = {}
    /\ tornTail = {}
    /\ staleTail = FALSE
    /\ diskStatus = "valid"
    /\ pendingPhase = "none"
    /\ pendingEpoch = 0
    /\ checkpointPhase = "idle"
    /\ buildGeneration = 0
    /\ buildEpoch = 0

Open(owner) ==
    /\ mode = "closed"
    /\ leases = {}
    /\ diskStatus = "valid"
    /\ tornTail = {}
    /\ leases' = {owner}
    /\ mode' = "running"
    /\ volatileEpoch' = durableEpoch
    /\ tornTail' = {}
    /\ UNCHANGED <<
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

Close ==
    /\ mode = "running"
    /\ pendingPhase = "none"
    /\ checkpointPhase = "idle"
    /\ mode' = "closed"
    /\ leases' = {}
    /\ UNCHANGED <<
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

BeginCommit ==
    /\ mode = "running"
    /\ pendingPhase = "none"
    /\ checkpointPhase = "idle"
    /\ volatileEpoch < MaxCommit
    /\ pendingPhase' = "appended"
    /\ pendingEpoch' = volatileEpoch + 1
    /\ tornTail' = {WalRecord(activeGeneration, volatileEpoch + 1)}
    /\ staleTail' = FALSE
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        diskStatus,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

SyncWal ==
    /\ mode = "running"
    /\ pendingPhase = "appended"
    /\ pendingEpoch = durableEpoch + 1
    /\ tornTail = {WalRecord(activeGeneration, pendingEpoch)}
    /\ walRecords' = walRecords \cup {WalRecord(activeGeneration, pendingEpoch)}
    /\ durableEpoch' = pendingEpoch
    /\ tornTail' = {}
    /\ pendingPhase' = "durable"
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        staleTail,
        diskStatus,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

ApplyWal ==
    /\ mode = "running"
    /\ pendingPhase = "durable"
    /\ pendingEpoch = durableEpoch
    /\ volatileEpoch' = pendingEpoch
    /\ pendingPhase' = "applied"
    /\ UNCHANGED <<
        mode,
        leases,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

ApplyWalFails ==
    /\ mode = "running"
    /\ pendingPhase = "durable"
    /\ mode' = "poisoned"
    /\ pendingPhase' = "none"
    /\ pendingEpoch' = 0
    /\ UNCHANGED <<
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

AcknowledgeCommit ==
    /\ mode = "running"
    /\ pendingPhase = "applied"
    /\ acknowledgedEpoch' = pendingEpoch
    /\ pendingPhase' = "none"
    /\ pendingEpoch' = 0
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

BeginCheckpoint ==
    /\ mode = "running"
    /\ pendingPhase = "none"
    /\ checkpointPhase = "idle"
    /\ activeGeneration < MaxGeneration
    /\ checkpointPhase' = "building"
    /\ buildGeneration' = activeGeneration + 1
    /\ buildEpoch' = volatileEpoch
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch
        >>

PersistCheckpoint ==
    /\ mode = "running"
    /\ checkpointPhase = "building"
    /\ checkpointByGeneration' =
        [checkpointByGeneration EXCEPT ![buildGeneration] = buildEpoch]
    /\ checkpointPhase' = "checkpoint_durable"
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        buildGeneration,
        buildEpoch
        >>

PrepareWalGeneration ==
    /\ mode = "running"
    /\ checkpointPhase = "checkpoint_durable"
    /\ checkpointByGeneration[buildGeneration] = buildEpoch
    /\ walStartByGeneration' =
        [walStartByGeneration EXCEPT ![buildGeneration] = buildEpoch + 1]
    /\ checkpointPhase' = "wal_durable"
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        buildGeneration,
        buildEpoch
        >>

PublishManifest ==
    /\ mode = "running"
    /\ checkpointPhase = "wal_durable"
    /\ checkpointByGeneration[buildGeneration] = buildEpoch
    /\ walStartByGeneration[buildGeneration] = buildEpoch + 1
    /\ activeGeneration' = buildGeneration
    /\ manifestCheckpointEpoch' = buildEpoch
    /\ manifestReplayLsn' = buildEpoch + 1
    /\ checkpointPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch
        >>

Crash ==
    /\ mode \in {"running", "poisoned"}
    /\ mode' = "crashed"
    /\ leases' = {}
    /\ volatileEpoch' = 0
    /\ pendingPhase' = "none"
    /\ pendingEpoch' = 0
    /\ checkpointPhase' = "idle"
    /\ buildGeneration' = 0
    /\ buildEpoch' = 0
    /\ UNCHANGED <<
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        diskStatus
        >>

Recover(owner) ==
    /\ mode = "crashed"
    /\ leases = {}
    /\ diskStatus = "valid"
    /\ tornTail = {}
    /\ mode' = "running"
    /\ leases' = {owner}
    /\ volatileEpoch' = durableEpoch
    /\ tornTail' = {}
    /\ UNCHANGED <<
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

DoctorRepairTornTail ==
    /\ mode \in {"closed", "crashed"}
    /\ leases = {}
    /\ diskStatus = "valid"
    /\ tornTail # {}
    /\ tornTail' = {}
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

ExposeStaleGenerationFragment ==
    /\ mode \in {"closed", "crashed"}
    /\ diskStatus = "valid"
    /\ tornTail = {}
    /\ ~staleTail
    /\ activeGeneration > 0
    /\ staleTail' = TRUE
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

InjectCompleteChainCorruption ==
    /\ mode \in {"closed", "crashed"}
    /\ diskStatus = "valid"
    /\ durableEpoch > manifestCheckpointEpoch
    /\ diskStatus' = "corrupt"
    /\ UNCHANGED <<
        mode,
        leases,
        volatileEpoch,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        tornTail,
        staleTail,
        pendingPhase,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

RejectCorruptOpen ==
    /\ mode \in {"closed", "crashed"}
    /\ leases = {}
    /\ diskStatus = "corrupt"
    /\ mode' = "failed"
    /\ volatileEpoch' = 0
    /\ tornTail' = {}
    /\ UNCHANGED <<
        leases,
        durableEpoch,
        acknowledgedEpoch,
        activeGeneration,
        manifestCheckpointEpoch,
        manifestReplayLsn,
        checkpointByGeneration,
        walStartByGeneration,
        walRecords,
        staleTail,
        diskStatus,
        pendingPhase,
        pendingEpoch,
        checkpointPhase,
        buildGeneration,
        buildEpoch
        >>

Next ==
    \/ \E owner \in Owners: Open(owner)
    \/ Close
    \/ BeginCommit
    \/ SyncWal
    \/ ApplyWal
    \/ ApplyWalFails
    \/ AcknowledgeCommit
    \/ BeginCheckpoint
    \/ PersistCheckpoint
    \/ PrepareWalGeneration
    \/ PublishManifest
    \/ Crash
    \/ \E owner \in Owners: Recover(owner)
    \/ DoctorRepairTornTail
    \/ ExposeStaleGenerationFragment
    \/ InjectCompleteChainCorruption
    \/ RejectCorruptOpen

TypeOK ==
    /\ mode \in Modes
    /\ leases \subseteq Owners
    /\ volatileEpoch \in Epochs
    /\ durableEpoch \in Epochs
    /\ acknowledgedEpoch \in Epochs
    /\ activeGeneration \in Generations
    /\ manifestCheckpointEpoch \in Epochs
    /\ manifestReplayLsn \in 1..(MaxCommit + 1)
    /\ checkpointByGeneration \in [Generations -> (-1)..MaxCommit]
    /\ walStartByGeneration \in [Generations -> (-1)..(MaxCommit + 1)]
    /\ walRecords \subseteq WalRecords
    /\ tornTail \subseteq WalRecords
    /\ Cardinality(tornTail) <= 1
    /\ staleTail \in BOOLEAN
    /\ diskStatus \in {"valid", "corrupt"}
    /\ pendingPhase \in PendingPhases
    /\ pendingEpoch \in Epochs
    /\ checkpointPhase \in CheckpointPhases
    /\ buildGeneration \in Generations
    /\ buildEpoch \in Epochs

SingleDirectoryOwner == Cardinality(leases) <= 1

LeaseMatchesHandle ==
    /\ (mode \in {"running", "poisoned"} => Cardinality(leases) = 1)
    /\ (mode \in {"closed", "crashed", "failed"} => leases = {})

ManifestReferencesDurableGeneration ==
    /\ checkpointByGeneration[activeGeneration] = manifestCheckpointEpoch
    /\ walStartByGeneration[activeGeneration] = manifestReplayLsn
    /\ manifestReplayLsn = manifestCheckpointEpoch + 1

ActiveWalIsContiguous ==
    ActiveWalEpochs = (manifestCheckpointEpoch + 1)..durableEpoch

DurablePrefixReachable ==
    \A epoch \in 1..durableEpoch:
        \/ epoch <= manifestCheckpointEpoch
        \/ WalRecord(activeGeneration, epoch) \in walRecords

AcknowledgedNeverLost ==
    /\ acknowledgedEpoch <= durableEpoch
    /\ (mode = "running" => acknowledgedEpoch <= volatileEpoch)

VisibleStateIsDurable ==
    mode = "running" => volatileEpoch <= durableEpoch

QuiescentHandleMatchesDurableState ==
    mode = "running" /\ pendingPhase = "none" =>
        volatileEpoch = durableEpoch

PoisonedHandleRequiresReopen ==
    mode = "poisoned" => pendingPhase = "none"

CorruptionFailsClosed ==
    mode = "failed" => /\ leases = {}
                       /\ volatileEpoch = 0

TornTailIsUnsyncedActiveAppend ==
    \A record \in tornTail:
        /\ record.generation = activeGeneration
        /\ record.epoch = durableEpoch + 1
        /\ record \notin walRecords

StaleFragmentReadsAsEndOfLog ==
    staleTail => tornTail = {}

CorruptCompleteChainNeverServes ==
    diskStatus = "corrupt" => mode \in {"closed", "crashed", "failed"}

Spec == Init /\ [][Next]_vars

=============================================================================
