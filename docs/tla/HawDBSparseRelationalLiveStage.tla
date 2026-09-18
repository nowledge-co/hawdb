-------------------- MODULE HawDBSparseRelationalLiveStage --------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************)
(* A metadata-only live transaction executes in one bounded materialized   *)
(* workspace. The supplied keys contain every actual replay-access key and *)
(* may additionally contain unchanged rows needed for authoritative unique *)
(* or foreign-key validation. Those support rows do not affect the logical *)
(* count delta, and the complete workspace is discarded after commit.      *)
(***************************************************************************)

Keys == {"k1", "k2"}
Operations == {"insert", "delete", "noop"}
MaxWorkspaceEntries == 2

Apply(rows, operation, target) ==
    CASE operation = "insert" -> rows \cup {target}
      [] operation = "delete" -> rows \ {target}
      [] OTHER -> rows

VARIABLES
    phase,
    baseRows,
    suppliedAccess,
    suppliedRows,
    replayAccess,
    supportAccess,
    workspaceBefore,
    workspaceAfter,
    logicalRows,
    operation,
    target,
    logicalRowCount,
    fullCheckpointMaterialized

vars == <<
    phase,
    baseRows,
    suppliedAccess,
    suppliedRows,
    replayAccess,
    supportAccess,
    workspaceBefore,
    workspaceAfter,
    logicalRows,
    operation,
    target,
    logicalRowCount,
    fullCheckpointMaterialized
>>

Init ==
    /\ phase = "idle"
    /\ baseRows = {}
    /\ suppliedAccess = {}
    /\ suppliedRows = {}
    /\ replayAccess = {}
    /\ supportAccess = {}
    /\ workspaceBefore = {}
    /\ workspaceAfter = {}
    /\ logicalRows = {}
    /\ operation = "noop"
    /\ target = "k1"
    /\ logicalRowCount = 0
    /\ fullCheckpointMaterialized = FALSE

HydrationIsAdmissible(base, supplied, rows, replay) ==
    /\ replay # {}
    /\ replay \subseteq supplied
    /\ Cardinality(supplied) <= MaxWorkspaceEntries
    /\ rows = base \intersect supplied

Begin(base, supplied, rows, replay, op, key) ==
    /\ phase = "idle"
    /\ base \subseteq Keys
    /\ supplied \subseteq Keys
    /\ rows \subseteq Keys
    /\ replay \subseteq Keys
    /\ op \in Operations
    /\ key \in replay
    /\ baseRows' = base
    /\ suppliedAccess' = supplied
    /\ suppliedRows' = rows
    /\ replayAccess' = replay
    /\ supportAccess' = supplied \ replay
    /\ logicalRows' = base
    /\ operation' = op
    /\ target' = key
    /\ logicalRowCount' = 0
    /\ fullCheckpointMaterialized' = FALSE
    /\ IF HydrationIsAdmissible(base, supplied, rows, replay)
          THEN /\ phase' = "hydrated"
               /\ workspaceBefore' = rows
               /\ workspaceAfter' = rows
          ELSE /\ phase' = "rejected"
               /\ workspaceBefore' = {}
               /\ workspaceAfter' = {}

Stage ==
    /\ phase = "hydrated"
    /\ phase' = "staged"
    /\ workspaceAfter' = Apply(workspaceBefore, operation, target)
    /\ logicalRows' = Apply(logicalRows, operation, target)
    /\ UNCHANGED <<
        baseRows, suppliedAccess, suppliedRows, replayAccess, supportAccess,
        workspaceBefore, operation, target, logicalRowCount,
        fullCheckpointMaterialized
        >>

Commit ==
    /\ phase = "staged"
    /\ phase' = "committed"
    /\ logicalRowCount' = Cardinality(logicalRows)
    /\ workspaceBefore' = {}
    /\ workspaceAfter' = {}
    /\ UNCHANGED <<
        baseRows, suppliedAccess, suppliedRows, replayAccess, supportAccess,
        logicalRows, operation, target, fullCheckpointMaterialized
        >>

Next ==
    \/ \E base \in SUBSET Keys,
          supplied \in SUBSET Keys,
          rows \in SUBSET Keys,
          replay \in SUBSET Keys,
          op \in Operations,
          key \in Keys:
          Begin(base, supplied, rows, replay, op, key)
    \/ Stage
    \/ Commit

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"idle", "hydrated", "staged", "committed", "rejected"}
    /\ baseRows \subseteq Keys
    /\ suppliedAccess \subseteq Keys
    /\ suppliedRows \subseteq Keys
    /\ replayAccess \subseteq Keys
    /\ supportAccess \subseteq Keys
    /\ workspaceBefore \subseteq Keys
    /\ workspaceAfter \subseteq Keys
    /\ logicalRows \subseteq Keys
    /\ operation \in Operations
    /\ target \in Keys
    /\ logicalRowCount \in 0..Cardinality(Keys)
    /\ fullCheckpointMaterialized \in BOOLEAN

NeverMaterializesFullCheckpoint ==
    fullCheckpointMaterialized = FALSE

AcceptedReplayIsCovered ==
    phase \in {"hydrated", "staged"} => replayAccess \subseteq suppliedAccess

AcceptedHydrationIsExact ==
    phase \in {"hydrated", "staged"} =>
        suppliedRows = baseRows \intersect suppliedAccess

WorkspaceIsBounded ==
    phase \in {"hydrated", "staged"} =>
        Cardinality(suppliedAccess) <= MaxWorkspaceEntries

SupportRowsAreUnchanged ==
    phase = "staged" =>
        workspaceAfter \intersect supportAccess =
            workspaceBefore \intersect supportAccess

CommittedCountIsExact ==
    phase = "committed" => logicalRowCount = Cardinality(logicalRows)

CommittedWorkspaceIsDiscarded ==
    phase = "committed" =>
        /\ workspaceBefore = {}
        /\ workspaceAfter = {}

RejectedHydrationNeverCommits ==
    phase = "rejected" => logicalRowCount = 0

=============================================================================
