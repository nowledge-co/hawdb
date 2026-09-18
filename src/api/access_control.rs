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

use super::{Database, QueryAccessControlContext};
use hawdb_core::RuntimeCapability;
use hawdb_evidence::{access_control_policy_readiness, AccessControlPolicyReadiness};

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
