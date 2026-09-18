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

use crate::{HawDBError, Result};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryAccessControlContext {
    policy_epoch: u64,
    visibility_property: String,
    allowed_visibility_values: BTreeSet<String>,
}

impl QueryAccessControlContext {
    pub fn visibility_scope(
        policy_epoch: u64,
        visibility_property: impl Into<String>,
        allowed_visibility_value: impl Into<String>,
    ) -> Self {
        Self::visibility_scopes(
            policy_epoch,
            visibility_property,
            std::iter::once(allowed_visibility_value),
        )
    }

    pub fn visibility_scopes(
        policy_epoch: u64,
        visibility_property: impl Into<String>,
        allowed_visibility_values: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            policy_epoch,
            visibility_property: visibility_property.into(),
            allowed_visibility_values: allowed_visibility_values
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }

    pub const fn policy_epoch(&self) -> u64 {
        self.policy_epoch
    }

    pub fn visibility_property(&self) -> &str {
        &self.visibility_property
    }

    pub fn allowed_visibility_values(&self) -> &BTreeSet<String> {
        &self.allowed_visibility_values
    }

    pub fn validate(&self) -> Result<()> {
        if self.policy_epoch == 0 {
            return Err(HawDBError::Semantic(
                "access control policy epoch must be non-zero".to_string(),
            ));
        }
        if self.visibility_property.trim().is_empty() {
            return Err(HawDBError::Semantic(
                "access control visibility property must be non-empty".to_string(),
            ));
        }
        if self.allowed_visibility_values.is_empty() {
            return Err(HawDBError::Semantic(
                "access control visibility scope must not be empty".to_string(),
            ));
        }
        if self
            .allowed_visibility_values
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(HawDBError::Semantic(
                "access control visibility scope values must be non-empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::QueryAccessControlContext;

    #[test]
    fn visibility_scopes_normalize_duplicate_values_and_expose_stable_order() {
        let context = QueryAccessControlContext::visibility_scopes(
            7,
            "space_id",
            ["team-b", "team-a", "team-b"],
        );

        assert_eq!(context.policy_epoch(), 7);
        assert_eq!(context.visibility_property(), "space_id");
        assert_eq!(
            context
                .allowed_visibility_values()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            ["team-a", "team-b"]
        );
        assert!(context.validate().is_ok());
    }

    #[test]
    fn validation_rejects_invalid_policy_inputs() {
        let invalid_contexts = [
            QueryAccessControlContext::visibility_scope(0, "space_id", "team-a"),
            QueryAccessControlContext::visibility_scope(7, "  ", "team-a"),
            QueryAccessControlContext::visibility_scopes(7, "space_id", Vec::<String>::new()),
            QueryAccessControlContext::visibility_scope(7, "space_id", "  "),
        ];

        for context in invalid_contexts {
            assert!(context.validate().is_err());
        }
    }
}
