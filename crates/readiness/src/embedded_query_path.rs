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

//! Readiness contract for embedded query facades.

/// Stable protocol identifier for embedded query-path readiness reports.
pub const EMBEDDED_QUERY_PATH_READINESS_PROTOCOL: &str = "hawdb-embedded-query-path-readiness-v1";

/// One public query entrypoint exposed by an embedded HawDB facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddedQueryEntrypoint {
    AdmittedSync,
    AdmittedTokio,
    RawDatabase,
}

impl EmbeddedQueryEntrypoint {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AdmittedSync => "hawdb_embedded_admitted",
            Self::AdmittedTokio => "hawdb_tokio_embedded_admitted",
            Self::RawDatabase => "raw_database",
        }
    }
}

/// Readiness evidence for one embedded query entrypoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddedQueryPathReadiness {
    pub protocol: &'static str,
    pub entrypoint: EmbeddedQueryEntrypoint,
    pub governor_enforced: bool,
    pub result_budget_enforced: bool,
    pub cancellation_enforced: bool,
    pub mutation_serialized: bool,
    pub admission_safe: bool,
    pub blockers: Vec<&'static str>,
}

impl EmbeddedQueryPathReadiness {
    #[doc(hidden)]
    pub fn admitted(entrypoint: EmbeddedQueryEntrypoint) -> Self {
        Self {
            protocol: EMBEDDED_QUERY_PATH_READINESS_PROTOCOL,
            entrypoint,
            governor_enforced: true,
            result_budget_enforced: true,
            cancellation_enforced: true,
            mutation_serialized: true,
            admission_safe: true,
            blockers: Vec::new(),
        }
    }

    #[doc(hidden)]
    pub fn raw_database() -> Self {
        Self {
            protocol: EMBEDDED_QUERY_PATH_READINESS_PROTOCOL,
            entrypoint: EmbeddedQueryEntrypoint::RawDatabase,
            governor_enforced: false,
            result_budget_enforced: false,
            cancellation_enforced: false,
            mutation_serialized: true,
            admission_safe: false,
            blockers: vec![
                "runtime_governor_not_enforced",
                "host_equivalent_governor_not_proven",
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EmbeddedQueryEntrypoint, EmbeddedQueryPathReadiness, EMBEDDED_QUERY_PATH_READINESS_PROTOCOL,
    };

    #[test]
    fn admitted_readiness_carries_all_runtime_guarantees() {
        let readiness =
            EmbeddedQueryPathReadiness::admitted(EmbeddedQueryEntrypoint::AdmittedTokio);

        assert_eq!(readiness.protocol, EMBEDDED_QUERY_PATH_READINESS_PROTOCOL);
        assert_eq!(readiness.entrypoint, EmbeddedQueryEntrypoint::AdmittedTokio);
        assert!(readiness.governor_enforced);
        assert!(readiness.result_budget_enforced);
        assert!(readiness.cancellation_enforced);
        assert!(readiness.mutation_serialized);
        assert!(readiness.admission_safe);
        assert!(readiness.blockers.is_empty());
    }

    #[test]
    fn raw_database_readiness_remains_blocked() {
        let readiness = EmbeddedQueryPathReadiness::raw_database();

        assert_eq!(readiness.protocol, EMBEDDED_QUERY_PATH_READINESS_PROTOCOL);
        assert_eq!(readiness.entrypoint, EmbeddedQueryEntrypoint::RawDatabase);
        assert!(readiness.mutation_serialized);
        assert!(!readiness.governor_enforced);
        assert!(!readiness.result_budget_enforced);
        assert!(!readiness.cancellation_enforced);
        assert!(!readiness.admission_safe);
        assert_eq!(
            readiness.blockers,
            vec![
                "runtime_governor_not_enforced",
                "host_equivalent_governor_not_proven",
            ]
        );
    }
}
