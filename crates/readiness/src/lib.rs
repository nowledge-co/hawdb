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

#[doc(hidden)]
pub mod evidence_json;

#[doc(hidden)]
pub mod embedded_query_path;

#[doc(hidden)]
pub mod bounded_read_evidence;

#[doc(hidden)]
pub mod bounded_read_evidence_cli;

#[doc(hidden)]
pub mod graph_summary;

#[doc(hidden)]
pub mod graph_route_catalog;

#[doc(hidden)]
pub mod integration_bundle;

#[doc(hidden)]
pub mod integration_bundle_cli;

#[doc(hidden)]
pub mod integration_readiness;

#[doc(hidden)]
pub mod json_parse;

#[doc(hidden)]
pub mod library_readiness;

#[doc(hidden)]
pub mod library_readiness_cli;

#[doc(hidden)]
pub mod serving_path;

#[doc(hidden)]
pub mod runtime_status;

#[doc(hidden)]
pub mod embedded_store_reports;

#[doc(hidden)]
pub mod cutover;

#[doc(hidden)]
pub mod embedded_query_report;

#[doc(hidden)]
pub mod previous_wrapper_preflight;

#[doc(hidden)]
pub mod query_runtime_preflight;

#[doc(hidden)]
pub mod query_runtime_preflight_cli;

#[doc(hidden)]
pub mod query_runtime_preflight_report;

#[doc(hidden)]
pub mod graph_route;

#[doc(hidden)]
pub mod graph_route_cli;

#[doc(hidden)]
pub mod graph_route_evidence_cli;

#[doc(hidden)]
pub mod replacement_summary;

#[doc(hidden)]
pub mod slow_query;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessAreaMap {
    pub graph: ReadinessAreaSummary,
    pub query: ReadinessAreaSummary,
    pub query_family: ReadinessAreaSummary,
    pub graph_route: ReadinessAreaSummary,
    pub search_route_ownership: ReadinessAreaSummary,
    pub storage: ReadinessAreaSummary,
    pub search_projection: ReadinessAreaSummary,
    pub search_projection_shadow: ReadinessAreaSummary,
    pub search_candidate_shadow: ReadinessAreaSummary,
    pub workload_fixture: ReadinessAreaSummary,
    pub background: ReadinessAreaSummary,
}

impl ReadinessAreaMap {
    pub fn areas(&self) -> Vec<ReadinessAreaSummary> {
        vec![
            self.graph.clone(),
            self.query.clone(),
            self.query_family.clone(),
            self.graph_route.clone(),
            self.search_route_ownership.clone(),
            self.storage.clone(),
            self.search_projection.clone(),
            self.search_projection_shadow.clone(),
            self.search_candidate_shadow.clone(),
            self.workload_fixture.clone(),
            self.background.clone(),
        ]
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "graph": self.graph.state_json(),
            "query": self.query.state_json(),
            "storage": self.storage.state_json(),
            "background": self.background.state_json(),
            "query_family": self.query_family.state_json(),
            "graph_route": self.graph_route.state_json(),
            "search_route_ownership": self.search_route_ownership.state_json(),
            "search_projection": self.search_projection.state_json(),
            "search_projection_shadow": self.search_projection_shadow.state_json(),
            "search_candidate_shadow": self.search_candidate_shadow.state_json(),
            "workload_fixture": self.workload_fixture.state_json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadinessAreaSummary {
    pub name: String,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
}

impl ReadinessAreaSummary {
    pub fn new(
        name: impl Into<String>,
        ready: bool,
        blocker_codes: impl Into<Vec<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            ready,
            blocker_codes: blocker_codes.into(),
        }
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
        })
    }

    pub fn state_json(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.ready,
            "blocker_codes": self.blocker_codes,
        })
    }
}

#[doc(hidden)]
pub mod source_mutation;

#[cfg(test)]
mod tests {
    use super::{ReadinessAreaMap, ReadinessAreaSummary};

    #[test]
    fn area_map_preserves_stable_json_contract() {
        let map = ReadinessAreaMap {
            graph: ReadinessAreaSummary::new("graph", true, Vec::new()),
            query: ReadinessAreaSummary::new("query", false, vec!["query_blocked".to_string()]),
            query_family: ReadinessAreaSummary::new("query_family", true, Vec::new()),
            graph_route: ReadinessAreaSummary::new("graph_route", true, Vec::new()),
            search_route_ownership: ReadinessAreaSummary::new(
                "search_route_ownership",
                true,
                Vec::new(),
            ),
            storage: ReadinessAreaSummary::new("storage", true, Vec::new()),
            search_projection: ReadinessAreaSummary::new("search_projection", true, Vec::new()),
            search_projection_shadow: ReadinessAreaSummary::new(
                "search_projection_shadow",
                true,
                Vec::new(),
            ),
            search_candidate_shadow: ReadinessAreaSummary::new(
                "search_candidate_shadow",
                true,
                Vec::new(),
            ),
            workload_fixture: ReadinessAreaSummary::new("workload_fixture", true, Vec::new()),
            background: ReadinessAreaSummary::new("background", true, Vec::new()),
        };

        assert_eq!(map.areas().len(), 11);
        assert_eq!(
            map.json()["query"],
            serde_json::json!({
                "ready": false,
                "blocker_codes": ["query_blocked"],
            })
        );
    }
}
