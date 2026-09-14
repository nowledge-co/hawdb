#[doc(hidden)]
pub mod evidence_json;

#[doc(hidden)]
pub mod graph_summary;

#[doc(hidden)]
pub mod integration_bundle;

#[doc(hidden)]
pub mod integration_readiness;

#[doc(hidden)]
pub mod previous_wrapper_preflight;

#[doc(hidden)]
pub mod graph_route;

#[doc(hidden)]
pub mod replacement_summary;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemReadinessAreaMap {
    pub graph: NowledgeMemReadinessAreaSummary,
    pub query: NowledgeMemReadinessAreaSummary,
    pub query_family: NowledgeMemReadinessAreaSummary,
    pub graph_route: NowledgeMemReadinessAreaSummary,
    pub search_route_ownership: NowledgeMemReadinessAreaSummary,
    pub storage: NowledgeMemReadinessAreaSummary,
    pub search_projection: NowledgeMemReadinessAreaSummary,
    pub search_projection_shadow: NowledgeMemReadinessAreaSummary,
    pub search_candidate_shadow: NowledgeMemReadinessAreaSummary,
    pub workload_fixture: NowledgeMemReadinessAreaSummary,
    pub background: NowledgeMemReadinessAreaSummary,
}

impl NowledgeMemReadinessAreaMap {
    pub fn areas(&self) -> Vec<NowledgeMemReadinessAreaSummary> {
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
pub struct NowledgeMemReadinessAreaSummary {
    pub name: String,
    pub ready: bool,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemReadinessAreaSummary {
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
    use super::{NowledgeMemReadinessAreaMap, NowledgeMemReadinessAreaSummary};

    #[test]
    fn area_map_preserves_stable_json_contract() {
        let map = NowledgeMemReadinessAreaMap {
            graph: NowledgeMemReadinessAreaSummary::new("graph", true, Vec::new()),
            query: NowledgeMemReadinessAreaSummary::new(
                "query",
                false,
                vec!["query_blocked".to_string()],
            ),
            query_family: NowledgeMemReadinessAreaSummary::new("query_family", true, Vec::new()),
            graph_route: NowledgeMemReadinessAreaSummary::new("graph_route", true, Vec::new()),
            search_route_ownership: NowledgeMemReadinessAreaSummary::new(
                "search_route_ownership",
                true,
                Vec::new(),
            ),
            storage: NowledgeMemReadinessAreaSummary::new("storage", true, Vec::new()),
            search_projection: NowledgeMemReadinessAreaSummary::new(
                "search_projection",
                true,
                Vec::new(),
            ),
            search_projection_shadow: NowledgeMemReadinessAreaSummary::new(
                "search_projection_shadow",
                true,
                Vec::new(),
            ),
            search_candidate_shadow: NowledgeMemReadinessAreaSummary::new(
                "search_candidate_shadow",
                true,
                Vec::new(),
            ),
            workload_fixture: NowledgeMemReadinessAreaSummary::new(
                "workload_fixture",
                true,
                Vec::new(),
            ),
            background: NowledgeMemReadinessAreaSummary::new("background", true, Vec::new()),
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
