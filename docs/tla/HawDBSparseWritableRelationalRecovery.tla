--------------- MODULE HawDBSparseWritableRelationalRecovery ---------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* Writable relational recovery hydrates exactly one WAL record's bounded  *)
(* authenticated access set. A newer staged delta wins over the immutable  *)
(* checkpoint, missing rows remain explicit, replay changes only the local  *)
(* workspace, exact logical counts advance atomically, and the workspace is *)
(* discarded before the next record. Invalid or oversized hydration rejects *)
(* without ever materializing the complete checkpoint.                      *)
(***************************************************************************)

Keys == {"k1", "k2"}
Operations == {"insert", "delete", "noop"}
MaxWorkspaceEntries == 1

VisibleRows(base, present, deleted) == (base \ deleted) \cup present
DeltaReadKeys(present, deleted, access) ==
    access \intersect (present \cup deleted)
CheckpointReadKeys(present, deleted, access) ==
    access \ (present \cup deleted)

VARIABLES
    phase,
    baseRows,
    deltaPresent,
    deltaDeleted,
    authenticatedAccess,
    suppliedAccess,
    suppliedRows,
    workspaceRows,
    logicalRows,
    operation,
    target,
    logicalRowCount,
    fullCheckpointMaterialized

vars == <<
    phase,
    baseRows,
    deltaPresent,
    deltaDeleted,
    authenticatedAccess,
    suppliedAccess,
    suppliedRows,
    workspaceRows,
    logicalRows,
    operation,
    target,
    logicalRowCount,
    fullCheckpointMaterialized
>>

Init ==
    /\ phase = "idle"
    /\ baseRows = {}
    /\ deltaPresent = {}
    /\ deltaDeleted = {}
    /\ authenticatedAccess = {}
    /\ suppliedAccess = {}
    /\ suppliedRows = {}
    /\ workspaceRows = {}
    /\ logicalRows = {}
    /\ operation = "noop"
    /\ target = "k1"
    /\ logicalRowCount = 0
    /\ fullCheckpointMaterialized = FALSE

HydrationIsAdmissible(base, present, deleted, access, suppliedKeys, rows) ==
    /\ Cardinality(access) <= MaxWorkspaceEntries
    /\ suppliedKeys = access
    /\ rows = access \intersect VisibleRows(base, present, deleted)

BeginRecord(base, present, deleted, access, suppliedKeys, rows, op, key) ==
    /\ phase = "idle"
    /\ base \subseteq Keys
    /\ present \subseteq Keys
    /\ deleted \subseteq Keys
    /\ present \intersect deleted = {}
    /\ access \subseteq Keys
    /\ access # {}
    /\ suppliedKeys \subseteq Keys
    /\ rows \subseteq Keys
    /\ op \in Operations
    /\ key \in access
    /\ baseRows' = base
    /\ deltaPresent' = present
    /\ deltaDeleted' = deleted
    /\ authenticatedAccess' = access
    /\ suppliedAccess' = suppliedKeys
    /\ suppliedRows' = rows
    /\ logicalRows' = VisibleRows(base, present, deleted)
    /\ operation' = op
    /\ target' = key
    /\ logicalRowCount' = 0
    /\ fullCheckpointMaterialized' = FALSE
    /\ IF HydrationIsAdmissible(base, present, deleted, access, suppliedKeys, rows)
          THEN /\ phase' = "hydrated"
               /\ workspaceRows' = rows
          ELSE /\ phase' = "rejected"
               /\ workspaceRows' = {}

ReplayRecord ==
    /\ phase = "hydrated"
    /\ phase' = "replayed"
    /\ workspaceRows' =
          CASE operation = "insert" -> workspaceRows \cup {target}
            [] operation = "delete" -> workspaceRows \ {target}
            [] OTHER -> workspaceRows
    /\ logicalRows' =
          CASE operation = "insert" -> logicalRows \cup {target}
            [] operation = "delete" -> logicalRows \ {target}
            [] OTHER -> logicalRows
    /\ UNCHANGED <<
        baseRows, deltaPresent, deltaDeleted, authenticatedAccess,
        suppliedAccess, suppliedRows, operation, target, logicalRowCount,
        fullCheckpointMaterialized
        >>

CommitRecord ==
    /\ phase = "replayed"
    /\ phase' = "committed"
    /\ logicalRowCount' = Cardinality(logicalRows)
    /\ workspaceRows' = {}
    /\ UNCHANGED <<
        baseRows, deltaPresent, deltaDeleted, authenticatedAccess,
        suppliedAccess, suppliedRows, logicalRows, operation, target,
        fullCheckpointMaterialized
        >>

Next ==
    \/ \E base \in SUBSET Keys,
          present \in SUBSET Keys,
          deleted \in SUBSET Keys,
          access \in SUBSET Keys,
          suppliedKeys \in SUBSET Keys,
          rows \in SUBSET Keys,
          op \in Operations,
          key \in Keys:
          BeginRecord(base, present, deleted, access, suppliedKeys, rows, op, key)
    \/ ReplayRecord
    \/ CommitRecord

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"idle", "hydrated", "replayed", "committed", "rejected"}
    /\ baseRows \subseteq Keys
    /\ deltaPresent \subseteq Keys
    /\ deltaDeleted \subseteq Keys
    /\ authenticatedAccess \subseteq Keys
    /\ suppliedAccess \subseteq Keys
    /\ suppliedRows \subseteq Keys
    /\ workspaceRows \subseteq Keys
    /\ logicalRows \subseteq Keys
    /\ operation \in Operations
    /\ target \in Keys
    /\ logicalRowCount \in 0..Cardinality(Keys)
    /\ fullCheckpointMaterialized \in BOOLEAN

NeverMaterializesFullCheckpoint ==
    fullCheckpointMaterialized = FALSE

AcceptedHydrationIsExact ==
    phase = "hydrated" =>
        /\ suppliedAccess = authenticatedAccess
        /\ workspaceRows = authenticatedAccess \intersect
             VisibleRows(baseRows, deltaPresent, deltaDeleted)

WorkspaceIsBounded ==
    phase \in {"hydrated", "replayed"} =>
        Cardinality(authenticatedAccess) <= MaxWorkspaceEntries

HydrationSourcesPartitionAccess ==
    phase = "hydrated" =>
        /\ DeltaReadKeys(deltaPresent, deltaDeleted, authenticatedAccess)
             \intersect
             CheckpointReadKeys(deltaPresent, deltaDeleted, authenticatedAccess) = {}
        /\ DeltaReadKeys(deltaPresent, deltaDeleted, authenticatedAccess)
             \cup
             CheckpointReadKeys(deltaPresent, deltaDeleted, authenticatedAccess)
             = authenticatedAccess

StagedDeltaPrecedesCheckpoint ==
    phase = "hydrated" =>
        workspaceRows = authenticatedAccess \intersect
            ((baseRows \ deltaDeleted) \cup deltaPresent)

StagedTombstonesMaskCheckpoint ==
    phase = "hydrated" =>
        workspaceRows \intersect deltaDeleted = {}

StagedPresentRowsPrecedeCheckpoint ==
    phase = "hydrated" =>
        authenticatedAccess \intersect deltaPresent \subseteq workspaceRows

CommittedCountIsExact ==
    phase = "committed" => logicalRowCount = Cardinality(logicalRows)

CommittedWorkspaceIsDiscarded ==
    phase = "committed" => workspaceRows = {}

RejectedHydrationNeverCommits ==
    phase = "rejected" => logicalRowCount = 0

=============================================================================
