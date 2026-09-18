-------------------- MODULE HawDBMemoryTierRelease --------------------
EXTENDS FiniteSets

(***************************************************************************)
(* The release gate keeps the desktop policy, explicit 512 MiB capability  *)
(* policy, representative production read, constrained capability read, and *)
(* both desktop and capability overflow-compaction runs as six independent  *)
(* obligations. Every obligation is bound to the exact release identity.     *)
(* Advancing the identity makes retained evidence stale; stale or incomplete *)
(* evidence can never publish a ready release.                               *)
(***************************************************************************)

CONSTANT Identities

ASSUME Identities # {}

NoEvidence == "none"
EvidenceIdentities == Identities \cup {NoEvidence}

VARIABLES
    currentIdentity,
    desktopPolicyIdentity,
    capabilityPolicyIdentity,
    productionReadIdentity,
    capabilityReadIdentity,
    desktopOverflowIdentity,
    capabilityOverflowIdentity,
    releaseReady

vars == <<
    currentIdentity,
    desktopPolicyIdentity,
    capabilityPolicyIdentity,
    productionReadIdentity,
    capabilityReadIdentity,
    desktopOverflowIdentity,
    capabilityOverflowIdentity,
    releaseReady
>>

Init ==
    /\ currentIdentity \in Identities
    /\ desktopPolicyIdentity = NoEvidence
    /\ capabilityPolicyIdentity = NoEvidence
    /\ productionReadIdentity = NoEvidence
    /\ capabilityReadIdentity = NoEvidence
    /\ desktopOverflowIdentity = NoEvidence
    /\ capabilityOverflowIdentity = NoEvidence
    /\ releaseReady = FALSE

RecordDesktopPolicy ==
    /\ desktopPolicyIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        desktopOverflowIdentity,
        capabilityOverflowIdentity,
        releaseReady
       >>

RecordCapabilityPolicy ==
    /\ capabilityPolicyIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        desktopOverflowIdentity,
        capabilityOverflowIdentity,
        releaseReady
       >>

RecordProductionRead ==
    /\ productionReadIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        capabilityReadIdentity,
        desktopOverflowIdentity,
        capabilityOverflowIdentity,
        releaseReady
       >>

RecordCapabilityRead ==
    /\ capabilityReadIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        desktopOverflowIdentity,
        capabilityOverflowIdentity,
        releaseReady
       >>

RecordDesktopOverflow ==
    /\ desktopOverflowIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        capabilityOverflowIdentity,
        releaseReady
       >>

RecordCapabilityOverflow ==
    /\ capabilityOverflowIdentity' = currentIdentity
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        desktopOverflowIdentity,
        releaseReady
       >>

AdvanceIdentity ==
    /\ \E nextIdentity \in Identities \ {currentIdentity}:
          currentIdentity' = nextIdentity
    /\ releaseReady' = FALSE
    /\ UNCHANGED <<
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        desktopOverflowIdentity,
        capabilityOverflowIdentity
       >>

AllEvidenceCurrent ==
    /\ desktopPolicyIdentity = currentIdentity
    /\ capabilityPolicyIdentity = currentIdentity
    /\ productionReadIdentity = currentIdentity
    /\ capabilityReadIdentity = currentIdentity
    /\ desktopOverflowIdentity = currentIdentity
    /\ capabilityOverflowIdentity = currentIdentity

AdmitRelease ==
    /\ ~releaseReady
    /\ AllEvidenceCurrent
    /\ releaseReady' = TRUE
    /\ UNCHANGED <<
        currentIdentity,
        desktopPolicyIdentity,
        capabilityPolicyIdentity,
        productionReadIdentity,
        capabilityReadIdentity,
        desktopOverflowIdentity,
        capabilityOverflowIdentity
       >>

Next ==
    \/ RecordDesktopPolicy
    \/ RecordCapabilityPolicy
    \/ RecordProductionRead
    \/ RecordCapabilityRead
    \/ RecordDesktopOverflow
    \/ RecordCapabilityOverflow
    \/ AdvanceIdentity
    \/ AdmitRelease

TypeOK ==
    /\ currentIdentity \in Identities
    /\ desktopPolicyIdentity \in EvidenceIdentities
    /\ capabilityPolicyIdentity \in EvidenceIdentities
    /\ productionReadIdentity \in EvidenceIdentities
    /\ capabilityReadIdentity \in EvidenceIdentities
    /\ desktopOverflowIdentity \in EvidenceIdentities
    /\ capabilityOverflowIdentity \in EvidenceIdentities
    /\ releaseReady \in BOOLEAN

ReadyRequiresBothPolicies ==
    releaseReady =>
        /\ desktopPolicyIdentity = currentIdentity
        /\ capabilityPolicyIdentity = currentIdentity

ReadyRequiresBothWorkloads ==
    releaseReady =>
        /\ productionReadIdentity = currentIdentity
        /\ capabilityReadIdentity = currentIdentity

ReadyRequiresBothOverflowProfiles ==
    releaseReady =>
        /\ desktopOverflowIdentity = currentIdentity
        /\ capabilityOverflowIdentity = currentIdentity

Spec == Init /\ [][Next]_vars

=============================================================================
