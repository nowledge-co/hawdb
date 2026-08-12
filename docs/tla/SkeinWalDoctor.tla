-------------------------- MODULE SkeinWalDoctor ---------------------------
EXTENDS Integers, Naturals

(***************************************************************************)
(* This model covers the destructive doctor repair of a torn WAL tail: a   *)
(* record whose fragment chain (FULL, or FIRST..MIDDLE*..LAST) is          *)
(* incomplete at end of file. walState "torn" is exactly that state, the   *)
(* only doctor-repairable one; "retained" means every remaining record is  *)
(* a complete, checksum-valid fragment chain. A checksum- or               *)
(* sequence-invalid complete chain fails closed outside this protocol,     *)
(* and a fragment carrying a stale WAL generation reads as end of log      *)
(* (clean EOF), so neither ever enters planning.                           *)
(***************************************************************************)

CONSTANT MaxIdentity

ASSUME MaxIdentity \in Nat \ {0}

VARIABLES directoryLease,
          walState,
          sourceIdentity,
          planState,
          planIdentity,
          dataLossAcknowledged,
          quarantineDurable,
          pendingAudit,
          appliedAudit,
          repairPhase,
          repairResult,
          openResult

vars == <<directoryLease,
          walState,
          sourceIdentity,
          planState,
          planIdentity,
          dataLossAcknowledged,
          quarantineDurable,
          pendingAudit,
          appliedAudit,
          repairPhase,
          repairResult,
          openResult>>

DirectoryLeases == {"none", "doctor", "database"}
WalStates == {"torn", "retained"}
PlanStates == {"none", "published"}
RepairPhases == {"idle", "planning", "verifying", "quarantined",
                 "prepared", "truncated", "applied", "finishing"}
RepairResults == {"none", "rejected", "succeeded"}
OpenResults == {"none", "rejected", "accepted"}

Init ==
    /\ directoryLease = "none"
    /\ walState = "torn"
    /\ sourceIdentity = 0
    /\ planState = "none"
    /\ planIdentity = 0
    /\ dataLossAcknowledged = FALSE
    /\ quarantineDurable = FALSE
    /\ pendingAudit = FALSE
    /\ appliedAudit = FALSE
    /\ repairPhase = "idle"
    /\ repairResult = "none"
    /\ openResult = "none"

StartPlanning ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ walState = "torn"
    /\ ~pendingAudit
    /\ directoryLease' = "doctor"
    /\ repairPhase' = "planning"
    /\ repairResult' = "none"
    /\ openResult' = "none"
    /\ UNCHANGED <<walState, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit>>

PublishPlan ==
    /\ directoryLease = "doctor"
    /\ repairPhase = "planning"
    /\ planState' = "published"
    /\ planIdentity' = sourceIdentity
    /\ dataLossAcknowledged' = FALSE
    /\ directoryLease' = "none"
    /\ repairPhase' = "idle"
    /\ UNCHANGED <<walState, sourceIdentity, quarantineDurable, pendingAudit,
                    appliedAudit, repairResult, openResult>>

AcknowledgeExactPlan ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ planState = "published"
    /\ planIdentity = sourceIdentity
    /\ dataLossAcknowledged' = TRUE
    /\ UNCHANGED <<directoryLease, walState, sourceIdentity, planState,
                    planIdentity, quarantineDurable, pendingAudit, appliedAudit,
                    repairPhase, repairResult, openResult>>

ExternalSourceChange ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ walState = "torn"
    /\ ~pendingAudit
    /\ sourceIdentity < MaxIdentity
    /\ sourceIdentity' = sourceIdentity + 1
    /\ openResult' = "none"
    /\ UNCHANGED <<directoryLease, walState, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit, repairPhase, repairResult>>

RejectUnacknowledgedApply ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ planState = "published"
    /\ ~dataLossAcknowledged
    /\ repairResult # "rejected"
    /\ repairResult' = "rejected"
    /\ UNCHANGED <<directoryLease, walState, sourceIdentity, planState,
                    planIdentity, dataLossAcknowledged, quarantineDurable,
                    pendingAudit, appliedAudit, repairPhase, openResult>>

RejectStaleApply ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ planState = "published"
    /\ dataLossAcknowledged
    /\ planIdentity # sourceIdentity
    /\ repairResult # "rejected"
    /\ repairResult' = "rejected"
    /\ UNCHANGED <<directoryLease, walState, sourceIdentity, planState,
                    planIdentity, dataLossAcknowledged, quarantineDurable,
                    pendingAudit, appliedAudit, repairPhase, openResult>>

StartApply ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ walState = "torn"
    /\ planState = "published"
    /\ dataLossAcknowledged
    /\ planIdentity = sourceIdentity
    /\ ~pendingAudit
    /\ directoryLease' = "doctor"
    /\ repairPhase' = "verifying"
    /\ repairResult' = "none"
    /\ openResult' = "none"
    /\ UNCHANGED <<walState, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit>>

PersistQuarantine ==
    /\ directoryLease = "doctor"
    /\ repairPhase = "verifying"
    /\ quarantineDurable' = TRUE
    /\ repairPhase' = "quarantined"
    /\ UNCHANGED <<directoryLease, walState, sourceIdentity, planState,
                    planIdentity, dataLossAcknowledged, pendingAudit,
                    appliedAudit, repairResult, openResult>>

PublishPreparedAudit ==
    /\ directoryLease = "doctor"
    /\ repairPhase = "quarantined"
    /\ quarantineDurable
    /\ pendingAudit' = TRUE
    /\ repairPhase' = "prepared"
    /\ UNCHANGED <<directoryLease, walState, sourceIdentity, planState,
                    planIdentity, dataLossAcknowledged, quarantineDurable,
                    appliedAudit, repairResult, openResult>>

TruncateWal ==
    /\ directoryLease = "doctor"
    /\ repairPhase = "prepared"
    /\ pendingAudit
    /\ quarantineDurable
    /\ walState' = "retained"
    /\ repairPhase' = "truncated"
    /\ UNCHANGED <<directoryLease, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit, repairResult, openResult>>

PublishAppliedAudit ==
    /\ directoryLease = "doctor"
    /\ repairPhase = "truncated"
    /\ walState = "retained"
    /\ pendingAudit
    /\ appliedAudit' = TRUE
    /\ repairPhase' = "applied"
    /\ UNCHANGED <<directoryLease, walState, sourceIdentity, planState,
                    planIdentity, dataLossAcknowledged, quarantineDurable,
                    pendingAudit, repairResult, openResult>>

RemovePendingAudit ==
    /\ directoryLease = "doctor"
    /\ repairPhase = "applied"
    /\ appliedAudit
    /\ pendingAudit
    /\ pendingAudit' = FALSE
    /\ repairPhase' = "finishing"
    /\ UNCHANGED <<directoryLease, walState, sourceIdentity, planState,
                    planIdentity, dataLossAcknowledged, quarantineDurable,
                    appliedAudit, repairResult, openResult>>

FinishApply ==
    /\ directoryLease = "doctor"
    /\ repairPhase = "finishing"
    /\ walState = "retained"
    /\ appliedAudit
    /\ ~pendingAudit
    /\ directoryLease' = "none"
    /\ repairPhase' = "idle"
    /\ repairResult' = "succeeded"
    /\ UNCHANGED <<walState, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit, openResult>>

StartResume ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ pendingAudit
    /\ planState = "published"
    /\ planIdentity = sourceIdentity
    /\ dataLossAcknowledged
    /\ quarantineDurable
    /\ directoryLease' = "doctor"
    /\ repairPhase' =
        IF walState = "torn"
          THEN "prepared"
          ELSE IF appliedAudit THEN "applied" ELSE "truncated"
    /\ repairResult' = "none"
    /\ openResult' = "none"
    /\ UNCHANGED <<walState, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit>>

CrashDoctor ==
    /\ directoryLease = "doctor"
    /\ directoryLease' = "none"
    /\ repairPhase' = "idle"
    /\ repairResult' = "none"
    /\ openResult' = "none"
    /\ UNCHANGED <<walState, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit>>

AttemptOrdinaryOpen ==
    /\ directoryLease = "none"
    /\ repairPhase = "idle"
    /\ openResult = "none"
    /\ IF walState = "retained" /\ ~pendingAudit
          THEN /\ directoryLease' = "database"
               /\ openResult' = "accepted"
          ELSE /\ UNCHANGED directoryLease
               /\ openResult' = "rejected"
    /\ UNCHANGED <<walState, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit, repairPhase, repairResult>>

CloseDatabase ==
    /\ directoryLease = "database"
    /\ directoryLease' = "none"
    /\ openResult' = "none"
    /\ UNCHANGED <<walState, sourceIdentity, planState, planIdentity,
                    dataLossAcknowledged, quarantineDurable, pendingAudit,
                    appliedAudit, repairPhase, repairResult>>

Next ==
    \/ StartPlanning
    \/ PublishPlan
    \/ AcknowledgeExactPlan
    \/ ExternalSourceChange
    \/ RejectUnacknowledgedApply
    \/ RejectStaleApply
    \/ StartApply
    \/ PersistQuarantine
    \/ PublishPreparedAudit
    \/ TruncateWal
    \/ PublishAppliedAudit
    \/ RemovePendingAudit
    \/ FinishApply
    \/ StartResume
    \/ CrashDoctor
    \/ AttemptOrdinaryOpen
    \/ CloseDatabase

TypeOK ==
    /\ directoryLease \in DirectoryLeases
    /\ walState \in WalStates
    /\ sourceIdentity \in 0..MaxIdentity
    /\ planState \in PlanStates
    /\ planIdentity \in 0..MaxIdentity
    /\ dataLossAcknowledged \in BOOLEAN
    /\ quarantineDurable \in BOOLEAN
    /\ pendingAudit \in BOOLEAN
    /\ appliedAudit \in BOOLEAN
    /\ repairPhase \in RepairPhases
    /\ repairResult \in RepairResults
    /\ openResult \in OpenResults

DoctorPhaseOwnsExclusiveLease ==
    /\ (repairPhase = "idle" => directoryLease # "doctor")
    /\ (repairPhase # "idle" => directoryLease = "doctor")

PreparedAuditHasDurableQuarantine ==
    pendingAudit =>
        /\ quarantineDurable
        /\ planState = "published"
        /\ dataLossAcknowledged

TruncationRequiresPreparedAudit ==
    walState = "retained" =>
        /\ quarantineDurable
        /\ (pendingAudit \/ appliedAudit)

AppliedAuditFollowsTruncation ==
    appliedAudit => walState = "retained" /\ quarantineDurable

AcceptedOpenIsSafe ==
    directoryLease = "database" =>
        /\ walState = "retained"
        /\ ~pendingAudit

RejectedApplyDoesNotMutateWal ==
    repairResult = "rejected" =>
        /\ walState = "torn"
        /\ ~pendingAudit
        /\ ~appliedAudit

SuccessfulRepairIsAudited ==
    repairResult = "succeeded" =>
        /\ walState = "retained"
        /\ quarantineDurable
        /\ appliedAudit
        /\ ~pendingAudit

RepairMutationUsesExactAcknowledgedPlan ==
    repairPhase \in {"verifying", "quarantined", "prepared", "truncated",
                      "applied", "finishing"} =>
        /\ planState = "published"
        /\ planIdentity = sourceIdentity
        /\ dataLossAcknowledged

Fairness ==
    /\ SF_vars(StartResume)
    /\ SF_vars(TruncateWal)
    /\ SF_vars(PublishAppliedAudit)
    /\ SF_vars(RemovePendingAudit)

Spec == Init /\ [][Next]_vars /\ Fairness

PendingRepairEventuallySettles ==
    pendingAudit ~> appliedAudit /\ ~pendingAudit

=============================================================================
