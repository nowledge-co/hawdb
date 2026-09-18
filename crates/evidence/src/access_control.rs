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

use hawdb_core::QueryAccessControlContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessControlPolicyReadiness {
    pub ready: bool,
    pub access_control_capability_enabled: bool,
    pub required_policy_epoch: u64,
    pub observed_policy_epoch: Option<u64>,
    pub stale_policy_state: bool,
    pub blocker_codes: Vec<String>,
}

pub fn access_control_policy_readiness(
    access_control_capability_enabled: bool,
    required_policy_epoch: u64,
    observed_policy: Option<&QueryAccessControlContext>,
) -> AccessControlPolicyReadiness {
    let observed_policy_epoch = observed_policy.map(QueryAccessControlContext::policy_epoch);
    let mut blocker_codes = Vec::new();

    if !access_control_capability_enabled {
        blocker_codes.push("access_control_capability_disabled".to_string());
    }
    if required_policy_epoch == 0 {
        blocker_codes.push("access_control_required_policy_epoch_missing".to_string());
    }

    match observed_policy {
        Some(policy) => {
            if policy.validate().is_err() {
                blocker_codes.push("access_control_policy_invalid".to_string());
            }
            if required_policy_epoch != 0 && policy.policy_epoch() < required_policy_epoch {
                blocker_codes.push("access_control_policy_stale".to_string());
            }
        }
        None => blocker_codes.push("access_control_policy_missing".to_string()),
    }

    let stale_policy_state = blocker_codes
        .iter()
        .any(|code| code == "access_control_policy_stale");

    AccessControlPolicyReadiness {
        ready: blocker_codes.is_empty(),
        access_control_capability_enabled,
        required_policy_epoch,
        observed_policy_epoch,
        stale_policy_state,
        blocker_codes,
    }
}

#[cfg(test)]
mod tests {
    use super::access_control_policy_readiness;
    use hawdb_core::QueryAccessControlContext;

    #[test]
    fn missing_and_stale_policies_fail_closed() {
        let missing = access_control_policy_readiness(true, 10, None);
        assert!(!missing.ready);
        assert_eq!(missing.observed_policy_epoch, None);
        assert_eq!(missing.blocker_codes, ["access_control_policy_missing"]);

        let stale_policy = QueryAccessControlContext::visibility_scope(9, "space_id", "team-a");
        let stale = access_control_policy_readiness(true, 10, Some(&stale_policy));
        assert!(!stale.ready);
        assert!(stale.stale_policy_state);
        assert_eq!(stale.observed_policy_epoch, Some(9));
        assert_eq!(stale.blocker_codes, ["access_control_policy_stale"]);
    }

    #[test]
    fn current_valid_policy_is_ready() {
        let policy = QueryAccessControlContext::visibility_scope(10, "space_id", "team-a");
        let readiness = access_control_policy_readiness(true, 10, Some(&policy));

        assert!(readiness.ready);
        assert!(!readiness.stale_policy_state);
        assert!(readiness.blocker_codes.is_empty());
    }

    #[test]
    fn disabled_capability_is_reported_without_scope_values() {
        let policy =
            QueryAccessControlContext::visibility_scope(10, "secret_space_id", "secret_space");
        let readiness = access_control_policy_readiness(false, 10, Some(&policy));

        assert!(!readiness.ready);
        assert!(!readiness.access_control_capability_enabled);
        assert_eq!(
            readiness.blocker_codes,
            ["access_control_capability_disabled"]
        );
        assert!(!readiness
            .blocker_codes
            .iter()
            .any(|code| code.contains("secret")));
    }
}
