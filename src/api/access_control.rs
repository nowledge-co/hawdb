use super::{Database, QueryAccessControlContext};
use skein_core::RuntimeCapability;
use skein_evidence::{access_control_policy_readiness, AccessControlPolicyReadiness};

impl Database {
    pub fn access_control_policy_readiness(
        &self,
        required_policy_epoch: u64,
        observed_policy: Option<&QueryAccessControlContext>,
    ) -> AccessControlPolicyReadiness {
        let access_control_capability_enabled = self
            .config
            .runtime_capabilities
            .is_enabled(RuntimeCapability::AccessControl);
        access_control_policy_readiness(
            access_control_capability_enabled,
            required_policy_epoch,
            observed_policy,
        )
    }
}
