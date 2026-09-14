//! Production qualification, recovery, and redacted diagnostic evidence.

#[doc(hidden)]
pub mod background_maintenance_evidence;
pub mod blackbox;
mod crash_recovery;
#[doc(hidden)]
pub mod inventory;
#[doc(hidden)]
pub mod json_access;
mod production;
#[doc(hidden)]
pub mod query_family_evidence;
#[doc(hidden)]
pub mod query_inventory;
#[doc(hidden)]
pub mod replacement_contract;
#[doc(hidden)]
pub mod storage_recovery_evidence;

pub use crash_recovery::{
    StorageCrashCaseEvidence, StorageCrashPoint, StorageCrashRecoveryEvidence,
    STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL,
};
pub use production::{
    ProductionEvidenceBinding, ProductionQualificationIdentity,
    PRODUCTION_QUALIFICATION_POLICY_VERSION,
};

#[doc(hidden)]
pub mod resource_profile;

/// Returns stable blocker codes without exposing the embedded facade's
/// qualification implementation details.
pub fn production_evidence_blocker_codes(
    binding: &ProductionEvidenceBinding,
    expected: &ProductionQualificationIdentity,
) -> Vec<String> {
    binding.blocker_codes_for(expected)
}
