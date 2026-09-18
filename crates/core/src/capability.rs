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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeCapability {
    AccessControl,
    FullTextSearch,
    VectorSearch,
    GraphAnalytics,
    BackgroundMaintenance,
}

impl RuntimeCapability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccessControl => "access_control",
            Self::FullTextSearch => "full_text_search",
            Self::VectorSearch => "vector_search",
            Self::GraphAnalytics => "graph_analytics",
            Self::BackgroundMaintenance => "background_maintenance",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeCapabilities {
    pub access_control: bool,
    pub full_text_search: bool,
    pub vector_search: bool,
    pub graph_analytics: bool,
    pub background_maintenance: bool,
}

impl RuntimeCapabilities {
    pub const fn shared_host() -> Self {
        Self {
            access_control: false,
            full_text_search: true,
            vector_search: true,
            graph_analytics: true,
            background_maintenance: true,
        }
    }

    pub const fn mobile_embedded() -> Self {
        Self {
            access_control: false,
            full_text_search: true,
            vector_search: true,
            graph_analytics: false,
            background_maintenance: false,
        }
    }

    pub const fn is_enabled(self, capability: RuntimeCapability) -> bool {
        match capability {
            RuntimeCapability::AccessControl => self.access_control,
            RuntimeCapability::FullTextSearch => self.full_text_search,
            RuntimeCapability::VectorSearch => self.vector_search,
            RuntimeCapability::GraphAnalytics => self.graph_analytics,
            RuntimeCapability::BackgroundMaintenance => self.background_maintenance,
        }
    }

    pub fn require(self, capability: RuntimeCapability) -> Result<()> {
        if self.is_enabled(capability) {
            return Ok(());
        }
        Err(HawDBError::CapabilityUnavailable { capability })
    }

    pub const fn with(mut self, capability: RuntimeCapability, enabled: bool) -> Self {
        match capability {
            RuntimeCapability::AccessControl => self.access_control = enabled,
            RuntimeCapability::FullTextSearch => self.full_text_search = enabled,
            RuntimeCapability::VectorSearch => self.vector_search = enabled,
            RuntimeCapability::GraphAnalytics => self.graph_analytics = enabled,
            RuntimeCapability::BackgroundMaintenance => self.background_maintenance = enabled,
        }
        self
    }

    pub const fn intersection(self, available: Self) -> Self {
        Self {
            access_control: self.access_control && available.access_control,
            full_text_search: self.full_text_search && available.full_text_search,
            vector_search: self.vector_search && available.vector_search,
            graph_analytics: self.graph_analytics && available.graph_analytics,
            background_maintenance: self.background_maintenance && available.background_maintenance,
        }
    }
}

impl Default for RuntimeCapabilities {
    fn default() -> Self {
        Self::shared_host()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mobile_disables_only_optional_heavy_runtime_capabilities() {
        let capabilities = RuntimeCapabilities::mobile_embedded();

        assert!(!capabilities.is_enabled(RuntimeCapability::AccessControl));
        assert!(capabilities.is_enabled(RuntimeCapability::FullTextSearch));
        assert!(capabilities.is_enabled(RuntimeCapability::VectorSearch));
        assert!(!capabilities.is_enabled(RuntimeCapability::GraphAnalytics));
        assert!(!capabilities.is_enabled(RuntimeCapability::BackgroundMaintenance));
    }

    #[test]
    fn disabled_capability_returns_typed_error() {
        let error = RuntimeCapabilities::mobile_embedded()
            .require(RuntimeCapability::GraphAnalytics)
            .unwrap_err();

        assert_eq!(
            error,
            HawDBError::CapabilityUnavailable {
                capability: RuntimeCapability::GraphAnalytics
            }
        );
        assert_eq!(error.to_string(), "capability unavailable: graph_analytics");
    }

    #[test]
    fn requested_capabilities_are_bounded_by_compiled_availability() {
        let available = RuntimeCapabilities::shared_host()
            .with(RuntimeCapability::AccessControl, true)
            .with(RuntimeCapability::GraphAnalytics, false)
            .with(RuntimeCapability::BackgroundMaintenance, false);
        let effective = RuntimeCapabilities::shared_host().intersection(available);

        assert!(!effective.access_control);
        assert!(effective.full_text_search);
        assert!(effective.vector_search);
        assert!(!effective.graph_analytics);
        assert!(!effective.background_maintenance);
    }
}
