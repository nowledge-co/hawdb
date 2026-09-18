// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Production qualification, recovery, and redacted diagnostic evidence.

mod access_control;
#[doc(hidden)]
pub mod background_maintenance_evidence;
pub mod blackbox;
mod crash_recovery;
#[doc(hidden)]
pub mod fixture_contract_check;
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

pub use access_control::{access_control_policy_readiness, AccessControlPolicyReadiness};
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
