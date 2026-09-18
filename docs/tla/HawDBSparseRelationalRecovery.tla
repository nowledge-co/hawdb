------------------- MODULE HawDBSparseRelationalRecovery -------------------
EXTENDS Naturals, Sequences, FiniteSets

(***************************************************************************)
(* A read-only out-of-core authoritative open mounts only canonical schema  *)
(* and row-count metadata, validates the complete WAL source, and then      *)
(* serves only when both pre-published row and index recovery artifacts bind*)
(* that exact source and recovered epoch. Schema-changing WAL, missing or   *)
(* mismatched artifacts, and count drift fail closed. No transition         *)
(* materializes checkpoint rows or publishes a derived artifact.            *)
(***************************************************************************)

RecordIds == 1..2
Sources == {<<1>>, <<2>>, <<1, 2>>, <<2, 1>>}
MaybeSources == Sources \cup {<<>>}
Counts == 0..3
Epochs == 1..2

VARIABLES
    phase,
    walSource,
    rowArtifactSource,
    indexArtifactSource,
    rowArtifactAvailable,
    indexArtifactAvailable,
    schemaStable,
    artifactEpoch,
    artifactRowCount,
    walValidated,
    metadataOnly,
    materializedRowCount,
    logicalRowCount,
    visibleEpoch,
    publishedArtifacts

vars == <<
    phase,
    walSource,
    rowArtifactSource,
    indexArtifactSource,
    rowArtifactAvailable,
    indexArtifactAvailable,
    schemaStable,
    artifactEpoch,
    artifactRowCount,
    walValidated,
    metadataOnly,
    materializedRowCount,
    logicalRowCount,
    visibleEpoch,
    publishedArtifacts
>>

Init ==
    /\ phase = "idle"
    /\ walSource = <<>>
    /\ rowArtifactSource = <<>>
    /\ indexArtifactSource = <<>>
    /\ rowArtifactAvailable = FALSE
    /\ indexArtifactAvailable = FALSE
    /\ schemaStable = TRUE
    /\ artifactEpoch = 1
    /\ artifactRowCount = 0
    /\ walValidated = FALSE
    /\ metadataOnly = FALSE
    /\ materializedRowCount = 0
    /\ logicalRowCount = 0
    /\ visibleEpoch = 0
    /\ publishedArtifacts = FALSE

MountMetadataOnly(wal, rowSource, indexSource, rowAvailable, indexAvailable,
                  stableSchema, recoveredEpoch, recoveredRows) ==
    /\ phase = "idle"
    /\ wal \in Sources
    /\ rowSource \in MaybeSources
    /\ indexSource \in MaybeSources
    /\ rowAvailable \in BOOLEAN
    /\ indexAvailable \in BOOLEAN
    /\ stableSchema \in BOOLEAN
    /\ recoveredEpoch \in Epochs
    /\ recoveredRows \in Counts
    /\ phase' = "mounted"
    /\ walSource' = wal
    /\ rowArtifactSource' = rowSource
    /\ indexArtifactSource' = indexSource
    /\ rowArtifactAvailable' = rowAvailable
    /\ indexArtifactAvailable' = indexAvailable
    /\ schemaStable' = stableSchema
    /\ artifactEpoch' = recoveredEpoch
    /\ artifactRowCount' = recoveredRows
    /\ walValidated' = FALSE
    /\ metadataOnly' = TRUE
    /\ materializedRowCount' = 0
    /\ logicalRowCount' = 0
    /\ visibleEpoch' = 0
    /\ publishedArtifacts' = FALSE

ValidateWal ==
    /\ phase = "mounted"
    /\ phase' = "validated"
    /\ walValidated' = TRUE
    /\ UNCHANGED <<
        walSource, rowArtifactSource, indexArtifactSource,
        rowArtifactAvailable, indexArtifactAvailable, schemaStable,
        artifactEpoch, artifactRowCount, metadataOnly, materializedRowCount,
        logicalRowCount, visibleEpoch, publishedArtifacts
        >>

ExactArtifacts ==
    /\ schemaStable
    /\ rowArtifactAvailable
    /\ indexArtifactAvailable
    /\ rowArtifactSource = walSource
    /\ indexArtifactSource = walSource
    /\ artifactEpoch = Len(walSource)

ActivateExactArtifacts ==
    /\ phase = "validated"
    /\ walValidated
    /\ ExactArtifacts
    /\ phase' = "serving"
    /\ logicalRowCount' = artifactRowCount
    /\ visibleEpoch' = artifactEpoch
    /\ UNCHANGED <<
        walSource, rowArtifactSource, indexArtifactSource,
        rowArtifactAvailable, indexArtifactAvailable, schemaStable,
        artifactEpoch, artifactRowCount, walValidated, metadataOnly,
        materializedRowCount, publishedArtifacts
        >>

RejectIncompleteRecovery ==
    /\ phase = "validated"
    /\ walValidated
    /\ ~ExactArtifacts
    /\ phase' = "rejected"
    /\ UNCHANGED <<
        walSource, rowArtifactSource, indexArtifactSource,
        rowArtifactAvailable, indexArtifactAvailable, schemaStable,
        artifactEpoch, artifactRowCount, walValidated, metadataOnly,
        materializedRowCount, logicalRowCount, visibleEpoch,
        publishedArtifacts
        >>

Next ==
    \/ \E wal \in Sources,
          rowSource \in MaybeSources,
          indexSource \in MaybeSources,
          rowAvailable \in BOOLEAN,
          indexAvailable \in BOOLEAN,
          stableSchema \in BOOLEAN,
          recoveredEpoch \in Epochs,
          recoveredRows \in Counts:
          MountMetadataOnly(
              wal,
              rowSource,
              indexSource,
              rowAvailable,
              indexAvailable,
              stableSchema,
              recoveredEpoch,
              recoveredRows
          )
    \/ ValidateWal
    \/ ActivateExactArtifacts
    \/ RejectIncompleteRecovery

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ phase \in {"idle", "mounted", "validated", "serving", "rejected"}
    /\ walSource \in MaybeSources
    /\ rowArtifactSource \in MaybeSources
    /\ indexArtifactSource \in MaybeSources
    /\ rowArtifactAvailable \in BOOLEAN
    /\ indexArtifactAvailable \in BOOLEAN
    /\ schemaStable \in BOOLEAN
    /\ artifactEpoch \in Epochs
    /\ artifactRowCount \in Counts
    /\ walValidated \in BOOLEAN
    /\ metadataOnly \in BOOLEAN
    /\ materializedRowCount \in Counts
    /\ logicalRowCount \in Counts
    /\ visibleEpoch \in 0..2
    /\ publishedArtifacts \in BOOLEAN

SparseOpenNeverMaterializes ==
    metadataOnly => materializedRowCount = 0

ServingRequiresValidatedExactArtifacts ==
    phase = "serving" =>
        /\ walValidated
        /\ metadataOnly
        /\ ExactArtifacts

ServingCountComesFromRowArtifact ==
    phase = "serving" =>
        /\ logicalRowCount = artifactRowCount
        /\ visibleEpoch = artifactEpoch

RejectedRecoveryNeverServes ==
    phase = "rejected" => visibleEpoch = 0

ReadOnlyRecoveryNeverPublishes ==
    publishedArtifacts = FALSE

SchemaChangingWalCannotServe ==
    ~schemaStable => phase # "serving"

=============================================================================
