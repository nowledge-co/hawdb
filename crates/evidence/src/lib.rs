//! Stable production qualification and crash-recovery evidence contracts.

mod crash_recovery;
mod production;

pub use crash_recovery::{
    StorageCrashCaseEvidence, StorageCrashPoint, StorageCrashRecoveryEvidence,
    STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL,
};
pub use production::{
    ProductionEvidenceBinding, ProductionQualificationIdentity,
    PRODUCTION_QUALIFICATION_POLICY_VERSION,
};

/// Returns stable blocker codes without exposing the embedded facade's
/// qualification implementation details.
pub fn production_evidence_blocker_codes(
    binding: &ProductionEvidenceBinding,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    binding.blocker_codes_for(expected)
}
