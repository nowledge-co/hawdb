---------------------- MODULE SkeinSystemSchemaUpgrade ----------------------
EXTENDS Integers, Naturals

(***************************************************************************)
(* Ordered system-schema upgrades publish schema objects and their registry  *)
(* version in one durable WAL decision. No database handle is returned while *)
(* an upgrade is staged or durable-but-not-yet-published.                    *)
(***************************************************************************)

CONSTANT MaxVersion

ASSUME MaxVersion \in Nat \ {0}

Versions == 0..(MaxVersion + 1)
MaybeVersions == {-1} \cup Versions
SchemaPhases == {"checking", "staged", "ready", "synced", "open", "rejected"}
DdlStates == {"none", "pending", "succeeded", "failed"}
OpenResults == {"none", "accepted", "rejected"}
ValidationStates == {"valid", "drift"}
TargetKinds == {"empty", "engineOnly", "nonEmpty"}
StreamRegistryStates == {"valid", "missing", "drift"}
ImportPhases == {"checking", "staged", "synced", "recovering", "open", "rejected"}
ExportRegistryStates == {"none", "valid"}

VARIABLES durableSchemaVersion,
          durableRegistryVersion,
          baseVersion,
          checksumState,
          registryShape,
          readOnly,
          schemaPhase,
          ddlState,
          stagedFrom,
          stagedTo,
          visibleSchemaVersion,
          visibleRegistryVersion,
          openResult,
          targetKind,
          streamRegistry,
          importPhase,
          importDurable,
          importVisible,
          sourceEpoch,
          exported,
          exportEpoch,
          exportRegistry

vars == <<durableSchemaVersion,
          durableRegistryVersion,
          baseVersion,
          checksumState,
          registryShape,
          readOnly,
          schemaPhase,
          ddlState,
          stagedFrom,
          stagedTo,
          visibleSchemaVersion,
          visibleRegistryVersion,
          openResult,
          targetKind,
          streamRegistry,
          importPhase,
          importDurable,
          importVisible,
          sourceEpoch,
          exported,
          exportEpoch,
          exportRegistry>>

SchemaVars == <<durableSchemaVersion,
                durableRegistryVersion,
                baseVersion,
                checksumState,
                registryShape,
                readOnly,
                schemaPhase,
                ddlState,
                stagedFrom,
                stagedTo,
                visibleSchemaVersion,
                visibleRegistryVersion,
                openResult>>

ImportVars == <<targetKind,
                streamRegistry,
                importPhase,
                importDurable,
                importVisible>>

ExportVars == <<sourceEpoch,
                exported,
                exportEpoch,
                exportRegistry>>

SchemaInputValid ==
    /\ checksumState = "valid"
    /\ registryShape = "valid"
    /\ durableRegistryVersion <= MaxVersion

ImportInputValid == streamRegistry = "valid"
ImportTargetAllowed == targetKind \in {"empty", "engineOnly"}

Init ==
    /\ durableSchemaVersion \in Versions
    /\ durableRegistryVersion = durableSchemaVersion
    /\ baseVersion = durableSchemaVersion
    /\ checksumState \in ValidationStates
    /\ registryShape \in ValidationStates
    /\ readOnly \in BOOLEAN
    /\ schemaPhase = "checking"
    /\ ddlState = "none"
    /\ stagedFrom = 0
    /\ stagedTo = 0
    /\ visibleSchemaVersion = -1
    /\ visibleRegistryVersion = -1
    /\ openResult = "none"
    /\ targetKind \in TargetKinds
    /\ streamRegistry \in StreamRegistryStates
    /\ importPhase = "checking"
    /\ importDurable = FALSE
    /\ importVisible = FALSE
    /\ sourceEpoch \in 0..2
    /\ exported = FALSE
    /\ exportEpoch = -1
    /\ exportRegistry = "none"

RejectInvalidSchema ==
    /\ schemaPhase = "checking"
    /\ ~SchemaInputValid
    /\ schemaPhase' = "rejected"
    /\ openResult' = "rejected"
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly, ddlState,
                    stagedFrom, stagedTo, visibleSchemaVersion,
                    visibleRegistryVersion>>

OpenCurrentSchema ==
    /\ schemaPhase = "checking"
    /\ SchemaInputValid
    /\ durableRegistryVersion = MaxVersion
    /\ schemaPhase' = "open"
    /\ visibleSchemaVersion' = durableSchemaVersion
    /\ visibleRegistryVersion' = durableRegistryVersion
    /\ openResult' = "accepted"
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly, ddlState,
                    stagedFrom, stagedTo>>

RejectReadOnlyUpgrade ==
    /\ schemaPhase = "checking"
    /\ SchemaInputValid
    /\ durableRegistryVersion < MaxVersion
    /\ readOnly
    /\ schemaPhase' = "rejected"
    /\ openResult' = "rejected"
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly, ddlState,
                    stagedFrom, stagedTo, visibleSchemaVersion,
                    visibleRegistryVersion>>

StageOrderedUpgrade ==
    /\ schemaPhase = "checking"
    /\ SchemaInputValid
    /\ durableRegistryVersion < MaxVersion
    /\ ~readOnly
    /\ schemaPhase' = "staged"
    /\ ddlState' = "pending"
    /\ stagedFrom' = durableRegistryVersion + 1
    /\ stagedTo' = MaxVersion
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly,
                    visibleSchemaVersion, visibleRegistryVersion, openResult>>

FailDdl ==
    /\ schemaPhase = "staged"
    /\ ddlState = "pending"
    /\ schemaPhase' = "rejected"
    /\ ddlState' = "failed"
    /\ openResult' = "rejected"
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly, stagedFrom,
                    stagedTo, visibleSchemaVersion, visibleRegistryVersion>>

FinishDdl ==
    /\ schemaPhase = "staged"
    /\ ddlState = "pending"
    /\ schemaPhase' = "ready"
    /\ ddlState' = "succeeded"
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly, stagedFrom,
                    stagedTo, visibleSchemaVersion, visibleRegistryVersion,
                    openResult>>

SyncUpgradeBatch ==
    /\ schemaPhase = "ready"
    /\ ddlState = "succeeded"
    /\ schemaPhase' = "synced"
    /\ durableSchemaVersion' = MaxVersion
    /\ durableRegistryVersion' = MaxVersion
    /\ UNCHANGED <<baseVersion, checksumState, registryShape, readOnly,
                    ddlState, stagedFrom, stagedTo, visibleSchemaVersion,
                    visibleRegistryVersion, openResult>>

PublishUpgradedHandle ==
    /\ schemaPhase = "synced"
    /\ durableSchemaVersion = MaxVersion
    /\ durableRegistryVersion = MaxVersion
    /\ schemaPhase' = "open"
    /\ visibleSchemaVersion' = durableSchemaVersion
    /\ visibleRegistryVersion' = durableRegistryVersion
    /\ openResult' = "accepted"
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly, ddlState,
                    stagedFrom, stagedTo>>

CrashSchemaOpen ==
    /\ schemaPhase \in {"staged", "ready", "synced", "open"}
    /\ schemaPhase' = "checking"
    /\ ddlState' = "none"
    /\ stagedFrom' = 0
    /\ stagedTo' = 0
    /\ visibleSchemaVersion' = -1
    /\ visibleRegistryVersion' = -1
    /\ openResult' = "none"
    /\ UNCHANGED <<durableSchemaVersion, durableRegistryVersion, baseVersion,
                    checksumState, registryShape, readOnly>>

RejectInvalidImport ==
    /\ importPhase = "checking"
    /\ (~ImportInputValid \/ ~ImportTargetAllowed)
    /\ importPhase' = "rejected"
    /\ UNCHANGED <<targetKind, streamRegistry, importDurable, importVisible>>

StageImport ==
    /\ importPhase = "checking"
    /\ ImportInputValid
    /\ ImportTargetAllowed
    /\ importPhase' = "staged"
    /\ UNCHANGED <<targetKind, streamRegistry, importDurable, importVisible>>

SyncImport ==
    /\ importPhase = "staged"
    /\ importPhase' = "synced"
    /\ importDurable' = TRUE
    /\ UNCHANGED <<targetKind, streamRegistry, importVisible>>

PublishImport ==
    /\ importPhase = "synced"
    /\ importDurable
    /\ importPhase' = "open"
    /\ importVisible' = TRUE
    /\ UNCHANGED <<targetKind, streamRegistry, importDurable>>

CrashImport ==
    /\ importPhase \in {"staged", "synced", "open"}
    /\ importPhase' = IF importDurable THEN "recovering" ELSE "checking"
    /\ importVisible' = FALSE
    /\ UNCHANGED <<targetKind, streamRegistry, importDurable>>

RecoverDurableImport ==
    /\ importPhase = "recovering"
    /\ importDurable
    /\ importPhase' = "open"
    /\ importVisible' = TRUE
    /\ UNCHANGED <<targetKind, streamRegistry, importDurable>>

DeriveSkeinLightningExport ==
    /\ ~exported
    /\ exported' = TRUE
    /\ exportEpoch' = sourceEpoch
    /\ exportRegistry' = "valid"
    /\ UNCHANGED sourceEpoch

SchemaNext ==
    \/ RejectInvalidSchema
    \/ OpenCurrentSchema
    \/ RejectReadOnlyUpgrade
    \/ StageOrderedUpgrade
    \/ FailDdl
    \/ FinishDdl
    \/ SyncUpgradeBatch
    \/ PublishUpgradedHandle
    \/ CrashSchemaOpen

ImportNext ==
    \/ RejectInvalidImport
    \/ StageImport
    \/ SyncImport
    \/ PublishImport
    \/ CrashImport
    \/ RecoverDurableImport

Next ==
    \/ /\ SchemaNext
       /\ UNCHANGED <<ImportVars, ExportVars>>
    \/ /\ ImportNext
       /\ UNCHANGED <<SchemaVars, ExportVars>>
    \/ /\ DeriveSkeinLightningExport
       /\ UNCHANGED <<SchemaVars, ImportVars>>

TypeOK ==
    /\ durableSchemaVersion \in Versions
    /\ durableRegistryVersion \in Versions
    /\ baseVersion \in Versions
    /\ checksumState \in ValidationStates
    /\ registryShape \in ValidationStates
    /\ readOnly \in BOOLEAN
    /\ schemaPhase \in SchemaPhases
    /\ ddlState \in DdlStates
    /\ stagedFrom \in Versions
    /\ stagedTo \in Versions
    /\ visibleSchemaVersion \in MaybeVersions
    /\ visibleRegistryVersion \in MaybeVersions
    /\ openResult \in OpenResults
    /\ targetKind \in TargetKinds
    /\ streamRegistry \in StreamRegistryStates
    /\ importPhase \in ImportPhases
    /\ importDurable \in BOOLEAN
    /\ importVisible \in BOOLEAN
    /\ sourceEpoch \in 0..2
    /\ exported \in BOOLEAN
    /\ exportEpoch \in {-1} \cup 0..2
    /\ exportRegistry \in ExportRegistryStates

DurableSchemaAndRegistryAreAtomic ==
    durableSchemaVersion = durableRegistryVersion

VisibleSchemaAndRegistryAreAtomic ==
    visibleSchemaVersion = visibleRegistryVersion

OnlyValidatedSchemaIsServed ==
    openResult = "accepted" =>
        /\ schemaPhase = "open"
        /\ SchemaInputValid
        /\ visibleSchemaVersion = durableSchemaVersion
        /\ visibleRegistryVersion = durableRegistryVersion
        /\ durableRegistryVersion = MaxVersion

PendingSchemaIsNotServed ==
    schemaPhase # "open" =>
        /\ visibleSchemaVersion = -1
        /\ visibleRegistryVersion = -1

UpgradeStagesAContiguousSuffix ==
    schemaPhase \in {"staged", "ready"} =>
        /\ stagedFrom = durableRegistryVersion + 1
        /\ stagedTo = MaxVersion

FailedDdlDoesNotAdvanceDurableState ==
    ddlState = "failed" =>
        /\ durableSchemaVersion = baseVersion
        /\ durableRegistryVersion = baseVersion

ReadOnlyNeverStagesUpgrade ==
    readOnly => schemaPhase \notin {"staged", "ready", "synced"}

InvalidOrFutureSchemaNeverOpens ==
    (~SchemaInputValid) => openResult # "accepted"

VisibleImportIsDurable == importVisible => importDurable

OnlyValidatedImportBecomesDurable ==
    importDurable =>
        /\ ImportInputValid
        /\ ImportTargetAllowed

InvalidImportNeverBecomesVisible ==
    (~ImportInputValid \/ ~ImportTargetAllowed) => ~importVisible

ImportVisibilityIsAtomic ==
    (importPhase = "open") = importVisible

ExportIncludesEngineRegistryWithoutEpochAdvance ==
    exported =>
        /\ exportRegistry = "valid"
        /\ exportEpoch = sourceEpoch

Spec == Init /\ [][Next]_vars

=============================================================================
