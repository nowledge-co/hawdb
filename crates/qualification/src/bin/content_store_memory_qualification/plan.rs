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

use super::qualification_input::{
    read_bounded_json, EvidenceBindingInput, ProductionIdentityInput,
};
use hawdb_qualification::ProductionContentStoreMemoryQualificationConfig;
use serde::Deserialize;
use std::path::{Path, PathBuf};

pub(crate) const CONTENT_STORE_MEMORY_QUALIFICATION_PLAN_PROTOCOL: &str =
    "hawdb-production-content-store-memory-plan-v1";

pub(crate) fn read_plan(path: &Path) -> Result<ContentStoreMemoryQualificationPlan, String> {
    read_bounded_json(path, "Content Store memory qualification plan")
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContentStoreMemoryQualificationPlan {
    protocol: String,
    evidence_binding: EvidenceBindingInput,
    expected_identity: ProductionIdentityInput,
}

impl ContentStoreMemoryQualificationPlan {
    pub(crate) fn into_config(
        self,
        storage_path: PathBuf,
    ) -> Result<ProductionContentStoreMemoryQualificationConfig, String> {
        if self.protocol != CONTENT_STORE_MEMORY_QUALIFICATION_PLAN_PROTOCOL {
            return Err(format!(
                "Content Store memory qualification plan protocol must be {CONTENT_STORE_MEMORY_QUALIFICATION_PLAN_PROTOCOL}"
            ));
        }
        if !storage_path.exists() {
            return Err(
                "Content Store memory qualification requires an existing storage path".to_string(),
            );
        }
        Ok(ProductionContentStoreMemoryQualificationConfig {
            evidence_binding: self.evidence_binding.into(),
            expected_identity: self.expected_identity.into(),
            storage_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_example_plan_builds_bound_detected_config() {
        let plan: ContentStoreMemoryQualificationPlan = serde_json::from_str(include_str!(
            "../../../fixtures/nowledge_content_store/production_memory_plan_example_v1.json"
        ))
        .unwrap();
        let config = plan.into_config(std::env::temp_dir()).unwrap();

        assert_eq!(config.evidence_binding.identity, config.expected_identity);
        assert!(config.evidence_binding.generated_at_unix_seconds > 0);
        assert!(config.storage_path.exists());
    }

    #[test]
    fn plan_rejects_unknown_protocol_and_missing_storage_path() {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../../../fixtures/nowledge_content_store/production_memory_plan_example_v1.json"
        ))
        .unwrap();
        value["protocol"] = serde_json::json!("unknown");
        let plan: ContentStoreMemoryQualificationPlan = serde_json::from_value(value).unwrap();
        assert!(plan
            .into_config(std::env::temp_dir())
            .unwrap_err()
            .contains("protocol"));

        let plan: ContentStoreMemoryQualificationPlan = serde_json::from_str(include_str!(
            "../../../fixtures/nowledge_content_store/production_memory_plan_example_v1.json"
        ))
        .unwrap();
        let missing = std::env::temp_dir().join("hawdb-memory-qualification-missing-path");
        assert!(plan
            .into_config(missing)
            .unwrap_err()
            .contains("existing storage path"));
    }
}
