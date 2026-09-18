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

pub const NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL: &str =
    "hawdb-nowledge-mem-serving-path-readiness-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NowledgeMemServingEntrypoint {
    EmbeddedStoreHandle,
    RawDatabase,
}

impl NowledgeMemServingEntrypoint {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EmbeddedStoreHandle => "nowledge_mem_embedded_store_handle",
            Self::RawDatabase => "raw_database",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemServingPathReadiness {
    pub protocol: String,
    pub entrypoint: NowledgeMemServingEntrypoint,
    pub host_runtime_binding: Option<String>,
    pub shared_runtime_governor: bool,
    pub foreground_parameterized_cypher_admitted: bool,
    pub bounded_streaming_read_admitted: bool,
    pub typed_mutation_admitted: bool,
    pub typed_analytics_admitted: bool,
    pub typed_maintenance_admitted: bool,
    pub direct_database_access: bool,
}

impl NowledgeMemServingPathReadiness {
    #[doc(hidden)]
    pub fn embedded_store_handle() -> Self {
        Self {
            protocol: NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL.to_string(),
            entrypoint: NowledgeMemServingEntrypoint::EmbeddedStoreHandle,
            host_runtime_binding: None,
            shared_runtime_governor: true,
            foreground_parameterized_cypher_admitted: true,
            bounded_streaming_read_admitted: true,
            typed_mutation_admitted: true,
            typed_analytics_admitted: true,
            typed_maintenance_admitted: true,
            direct_database_access: false,
        }
    }

    pub fn raw_database() -> Self {
        Self {
            protocol: NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL.to_string(),
            entrypoint: NowledgeMemServingEntrypoint::RawDatabase,
            host_runtime_binding: None,
            shared_runtime_governor: false,
            foreground_parameterized_cypher_admitted: false,
            bounded_streaming_read_admitted: false,
            typed_mutation_admitted: false,
            typed_analytics_admitted: false,
            typed_maintenance_admitted: false,
            direct_database_access: true,
        }
    }

    /// Binds structural facade readiness to the host runtime that owns the
    /// long-lived handle. Empty identities remain unbound and fail closed.
    pub fn bind_host_runtime(mut self, identity: impl Into<String>) -> Self {
        let identity = identity.into();
        let identity = identity.trim();
        self.host_runtime_binding = (!identity.is_empty()).then(|| identity.to_string());
        self
    }

    pub fn admission_safe(&self) -> bool {
        self.entrypoint == NowledgeMemServingEntrypoint::EmbeddedStoreHandle
            && self.shared_runtime_governor
            && self.foreground_parameterized_cypher_admitted
            && self.bounded_streaming_read_admitted
            && self.typed_mutation_admitted
            && self.typed_analytics_admitted
            && self.typed_maintenance_admitted
            && !self.direct_database_access
    }

    pub fn ready(&self) -> bool {
        self.protocol == NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL
            && self.admission_safe()
            && self.host_runtime_binding.is_some()
    }

    pub fn blocker_codes(&self) -> Vec<&'static str> {
        let mut blockers = Vec::new();
        if self.protocol != NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL {
            blockers.push("serving_path_protocol_mismatch");
        }
        if self.entrypoint != NowledgeMemServingEntrypoint::EmbeddedStoreHandle {
            blockers.push("admitted_store_handle_not_used");
        }
        if self.host_runtime_binding.is_none() {
            blockers.push("host_runtime_binding_missing");
        }
        if !self.shared_runtime_governor {
            blockers.push("shared_runtime_governor_not_proven");
        }
        if !self.foreground_parameterized_cypher_admitted {
            blockers.push("foreground_parameterized_cypher_not_admitted");
        }
        if !self.bounded_streaming_read_admitted {
            blockers.push("bounded_streaming_read_not_admitted");
        }
        if !self.typed_mutation_admitted {
            blockers.push("typed_mutation_not_admitted");
        }
        if !self.typed_analytics_admitted {
            blockers.push("typed_analytics_not_admitted");
        }
        if !self.typed_maintenance_admitted {
            blockers.push("typed_maintenance_not_admitted");
        }
        if self.direct_database_access {
            blockers.push("direct_database_access_not_production_safe");
        }
        blockers
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "entrypoint": self.entrypoint.as_str(),
            "host_runtime_binding": self.host_runtime_binding,
            "shared_runtime_governor": self.shared_runtime_governor,
            "foreground_parameterized_cypher_admitted": self.foreground_parameterized_cypher_admitted,
            "bounded_streaming_read_admitted": self.bounded_streaming_read_admitted,
            "typed_mutation_admitted": self.typed_mutation_admitted,
            "typed_analytics_admitted": self.typed_analytics_admitted,
            "typed_maintenance_admitted": self.typed_maintenance_admitted,
            "direct_database_access": self.direct_database_access,
            "admission_safe": self.admission_safe(),
            "ready": self.ready(),
            "blocker_codes": self.blocker_codes(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_handle_requires_an_explicit_host_runtime_binding() {
        let unbound = NowledgeMemServingPathReadiness::embedded_store_handle();

        assert!(unbound.admission_safe());
        assert!(!unbound.ready());
        assert_eq!(
            unbound.blocker_codes(),
            vec!["host_runtime_binding_missing"]
        );

        let bound = unbound.bind_host_runtime("nmem_graph_hawdb_embedded_runtime");
        assert!(bound.ready());
        assert!(bound.blocker_codes().is_empty());
        assert_eq!(
            bound.json(),
            serde_json::json!({
                "protocol": NOWLEDGE_MEM_SERVING_PATH_READINESS_PROTOCOL,
                "entrypoint": "nowledge_mem_embedded_store_handle",
                "host_runtime_binding": "nmem_graph_hawdb_embedded_runtime",
                "shared_runtime_governor": true,
                "foreground_parameterized_cypher_admitted": true,
                "bounded_streaming_read_admitted": true,
                "typed_mutation_admitted": true,
                "typed_analytics_admitted": true,
                "typed_maintenance_admitted": true,
                "direct_database_access": false,
                "admission_safe": true,
                "ready": true,
                "blocker_codes": [],
            })
        );
    }

    #[test]
    fn raw_database_cannot_satisfy_serving_path_readiness() {
        let raw =
            NowledgeMemServingPathReadiness::raw_database().bind_host_runtime("controlled_host");

        assert!(!raw.admission_safe());
        assert!(!raw.ready());
        assert!(raw
            .blocker_codes()
            .contains(&"direct_database_access_not_production_safe"));
        assert!(raw
            .blocker_codes()
            .contains(&"shared_runtime_governor_not_proven"));
    }
}
