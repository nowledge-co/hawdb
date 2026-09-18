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

//! Storage-independent graph route ownership and readiness contracts.
//!
//! The embedded facade owns probe execution and activation. These checks only
//! interpret the caller's complete ownership inventory and readiness evidence.

use std::collections::{BTreeMap, BTreeSet};

mod catalog;

#[cfg(test)]
mod differential;

pub use catalog::{
    nowledge_mem_graph_read_route_catalog_digest, nowledge_mem_graph_read_route_spec,
    nowledge_mem_graph_read_route_spec_json, nowledge_mem_graph_read_route_specs_json,
    nowledge_mem_required_query_families_for_route, NowledgeMemGraphReadRouteEvidenceKind,
    NowledgeMemGraphReadRouteOwner, NowledgeMemGraphReadRouteSpec,
    NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION, NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS,
    NOWLEDGE_MEM_SEARCH_ROUTE, REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRouteReadinessSummary {
    pub route_primary_ready: bool,
    pub primary_ready_routes: Vec<String>,
    pub route_query_plan_evidence_ready: bool,
    pub route_query_profile_evidence_ready: bool,
    pub route_query_api_behavior_evidence_ready: bool,
    pub relationship_property_pruning_required_count: u64,
    pub relationship_property_pruning_report_count: u64,
    pub route_relationship_property_pruning_evidence_ready: bool,
}

pub const NOWLEDGE_MEM_ROUTE_OWNERSHIP_PROTOCOL: &str = "hawdb-nowledge-mem-route-ownership-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NowledgeMemRouteOwnershipPolicy {
    pub require_all_hawdb: bool,
}

impl NowledgeMemRouteOwnershipPolicy {
    pub const fn migration() -> Self {
        Self {
            require_all_hawdb: false,
        }
    }

    pub const fn production_cutover() -> Self {
        Self {
            require_all_hawdb: true,
        }
    }
}

impl Default for NowledgeMemRouteOwnershipPolicy {
    fn default() -> Self {
        Self::migration()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NowledgeMemRouteReadEngine {
    Legacy,
    HawDB,
}

impl NowledgeMemRouteReadEngine {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::HawDB => "hawdb",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRouteOwnership {
    pub route: String,
    pub read_engine: NowledgeMemRouteReadEngine,
}

impl NowledgeMemRouteOwnership {
    pub fn new(route: impl Into<String>, read_engine: NowledgeMemRouteReadEngine) -> Self {
        Self {
            route: route.into(),
            read_engine,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NowledgeMemRouteOwnershipReadinessReport {
    pub protocol: String,
    pub ready: bool,
    pub production_cutover_ready: bool,
    pub require_all_hawdb: bool,
    pub required_route_count: usize,
    pub explicit_route_count: usize,
    pub hawdb_route_count: usize,
    pub legacy_route_count: usize,
    pub routes: Vec<NowledgeMemRouteOwnership>,
    pub hawdb_routes: Vec<String>,
    pub legacy_routes: Vec<String>,
    pub missing_required_routes: Vec<String>,
    pub unknown_routes: Vec<String>,
    pub duplicate_routes: Vec<String>,
    pub conflicting_routes: Vec<String>,
    pub hawdb_not_ready_routes: Vec<String>,
    pub route_readiness_present: bool,
    pub route_readiness_ready: bool,
    pub route_catalog_version: String,
    pub route_catalog_digest: String,
    pub blocker_codes: Vec<String>,
}

impl NowledgeMemRouteOwnershipReadinessReport {
    pub fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "protocol": self.protocol,
            "ready": self.ready,
            "production_cutover_ready": self.production_cutover_ready,
            "require_all_hawdb": self.require_all_hawdb,
            "required_route_count": self.required_route_count,
            "explicit_route_count": self.explicit_route_count,
            "hawdb_route_count": self.hawdb_route_count,
            "legacy_route_count": self.legacy_route_count,
            "routes": self.routes.iter().map(route_ownership_json).collect::<Vec<_>>(),
            "hawdb_routes": self.hawdb_routes,
            "legacy_routes": self.legacy_routes,
            "missing_required_routes": self.missing_required_routes,
            "unknown_routes": self.unknown_routes,
            "duplicate_routes": self.duplicate_routes,
            "conflicting_routes": self.conflicting_routes,
            "hawdb_not_ready_routes": self.hawdb_not_ready_routes,
            "route_readiness_present": self.route_readiness_present,
            "route_readiness_ready": self.route_readiness_ready,
            "route_catalog_version": self.route_catalog_version,
            "route_catalog_digest": self.route_catalog_digest,
            "blocker_codes": self.blocker_codes,
        })
    }
}

pub fn nowledge_mem_route_ownership_all_legacy() -> Vec<NowledgeMemRouteOwnership> {
    nowledge_mem_route_ownership_for_engine(NowledgeMemRouteReadEngine::Legacy)
}

pub fn nowledge_mem_route_ownership_all_hawdb() -> Vec<NowledgeMemRouteOwnership> {
    nowledge_mem_route_ownership_for_engine(NowledgeMemRouteReadEngine::HawDB)
}

pub fn nowledge_mem_route_ownership_for_engine(
    read_engine: NowledgeMemRouteReadEngine,
) -> Vec<NowledgeMemRouteOwnership> {
    REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .map(|route| NowledgeMemRouteOwnership::new(*route, read_engine))
        .collect()
}

pub fn nowledge_mem_route_ownership_readiness(
    routes: &[NowledgeMemRouteOwnership],
    route_readiness: Option<&NowledgeMemRouteReadinessSummary>,
    policy: NowledgeMemRouteOwnershipPolicy,
) -> NowledgeMemRouteOwnershipReadinessReport {
    let required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut route_counts = BTreeMap::<&str, usize>::new();
    let mut route_engines = BTreeMap::<&str, BTreeSet<NowledgeMemRouteReadEngine>>::new();
    for route in routes {
        *route_counts.entry(route.route.as_str()).or_default() += 1;
        route_engines
            .entry(route.route.as_str())
            .or_default()
            .insert(route.read_engine);
    }

    let explicit_required_routes = route_counts
        .keys()
        .copied()
        .filter(|route| required_routes.contains(route))
        .collect::<BTreeSet<_>>();
    let missing_required_routes = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
        .iter()
        .copied()
        .filter(|route| !explicit_required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let unknown_routes = route_counts
        .keys()
        .copied()
        .filter(|route| !required_routes.contains(route))
        .map(str::to_string)
        .collect::<Vec<_>>();
    let duplicate_routes = route_counts
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();
    let conflicting_routes = route_engines
        .iter()
        .filter(|(_, engines)| engines.len() > 1)
        .map(|(route, _)| (*route).to_string())
        .collect::<Vec<_>>();

    let hawdb_routes = routes_by_engine(routes, NowledgeMemRouteReadEngine::HawDB);
    let legacy_routes = routes_by_engine(routes, NowledgeMemRouteReadEngine::Legacy);
    let primary_ready_routes = route_readiness
        .map(|summary| {
            summary
                .primary_ready_routes
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    let route_readiness_ready = route_readiness.is_some_and(route_readiness_summary_ready);
    let hawdb_not_ready_routes = hawdb_routes
        .iter()
        .filter(|route| !route_readiness_ready || !primary_ready_routes.contains(route.as_str()))
        .cloned()
        .collect::<Vec<_>>();

    let mut blocker_codes = Vec::new();
    if !missing_required_routes.is_empty() {
        blocker_codes.push("route_ownership_missing_required_routes".to_string());
    }
    if !unknown_routes.is_empty() {
        blocker_codes.push("route_ownership_unknown_routes".to_string());
    }
    if !duplicate_routes.is_empty() {
        blocker_codes.push("route_ownership_duplicate_routes".to_string());
    }
    if !conflicting_routes.is_empty() {
        blocker_codes.push("route_ownership_conflicting_routes".to_string());
    }
    if !hawdb_routes.is_empty() && route_readiness.is_none() {
        blocker_codes.push("route_ownership_route_readiness_missing".to_string());
    }
    if !hawdb_not_ready_routes.is_empty() {
        blocker_codes.push("route_ownership_hawdb_routes_not_ready".to_string());
    }
    if policy.require_all_hawdb && !legacy_routes.is_empty() {
        blocker_codes.push("route_ownership_legacy_routes_remaining".to_string());
    }

    let ready = blocker_codes.is_empty();
    let production_cutover_ready = ready && legacy_routes.is_empty();

    NowledgeMemRouteOwnershipReadinessReport {
        protocol: NOWLEDGE_MEM_ROUTE_OWNERSHIP_PROTOCOL.to_string(),
        ready,
        production_cutover_ready,
        require_all_hawdb: policy.require_all_hawdb,
        required_route_count: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len(),
        explicit_route_count: explicit_required_routes.len(),
        hawdb_route_count: hawdb_routes.len(),
        legacy_route_count: legacy_routes.len(),
        routes: routes.to_vec(),
        hawdb_routes,
        legacy_routes,
        missing_required_routes,
        unknown_routes,
        duplicate_routes,
        conflicting_routes,
        hawdb_not_ready_routes,
        route_readiness_present: route_readiness.is_some(),
        route_readiness_ready,
        route_catalog_version: NOWLEDGE_MEM_GRAPH_READ_ROUTE_CATALOG_VERSION.to_string(),
        route_catalog_digest: nowledge_mem_graph_read_route_catalog_digest(),
        blocker_codes,
    }
}

fn route_ownership_json(route: &NowledgeMemRouteOwnership) -> serde_json::Value {
    serde_json::json!({
        "route": route.route,
        "read_engine": route.read_engine.as_str(),
    })
}

fn routes_by_engine(
    routes: &[NowledgeMemRouteOwnership],
    read_engine: NowledgeMemRouteReadEngine,
) -> Vec<String> {
    routes
        .iter()
        .filter(|route| route.read_engine == read_engine)
        .map(|route| route.route.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn route_readiness_summary_ready(summary: &NowledgeMemRouteReadinessSummary) -> bool {
    summary.route_primary_ready
        && summary.route_query_plan_evidence_ready
        && summary.route_query_profile_evidence_ready
        && summary.route_query_api_behavior_evidence_ready
        && summary.route_relationship_property_pruning_evidence_ready
        && summary.relationship_property_pruning_required_count
            == summary.relationship_property_pruning_report_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_readiness_allows_explicit_legacy_routes() {
        let report = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_legacy(),
            None,
            NowledgeMemRouteOwnershipPolicy::migration(),
        );

        assert!(report.ready);
        assert!(!report.production_cutover_ready);
        assert_eq!(
            report.legacy_route_count,
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert!(report.blocker_codes.is_empty());
    }

    #[test]
    fn production_cutover_rejects_legacy_routes() {
        let report = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_legacy(),
            None,
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert!(!report.production_cutover_ready);
        assert_eq!(
            report.blocker_codes,
            vec!["route_ownership_legacy_routes_remaining".to_string()]
        );
    }

    #[test]
    fn hawdb_routes_require_primary_ready_evidence() {
        let report = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_hawdb(),
            None,
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert!(!report.production_cutover_ready);
        assert!(report
            .blocker_codes
            .contains(&"route_ownership_route_readiness_missing".to_string()));
        assert!(report
            .blocker_codes
            .contains(&"route_ownership_hawdb_routes_not_ready".to_string()));
        assert_eq!(
            report.hawdb_not_ready_routes.len(),
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
    }

    #[test]
    fn production_cutover_accepts_all_hawdb_with_ready_evidence() {
        let readiness = ready_route_readiness();
        let report = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_hawdb(),
            Some(&readiness),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );

        assert!(report.ready);
        assert!(report.production_cutover_ready);
        assert_eq!(
            report.hawdb_route_count,
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert!(report.legacy_routes.is_empty());
        assert!(report.hawdb_not_ready_routes.is_empty());
    }

    #[test]
    fn hawdb_routes_require_api_behavior_evidence() {
        let mut readiness = ready_route_readiness();
        readiness.route_query_api_behavior_evidence_ready = false;

        let report = nowledge_mem_route_ownership_readiness(
            &nowledge_mem_route_ownership_all_hawdb(),
            Some(&readiness),
            NowledgeMemRouteOwnershipPolicy::production_cutover(),
        );

        assert!(!report.ready);
        assert!(!report.production_cutover_ready);
        assert_eq!(
            report.hawdb_not_ready_routes.len(),
            REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len()
        );
        assert!(report
            .blocker_codes
            .contains(&"route_ownership_hawdb_routes_not_ready".to_string()));
    }

    #[test]
    fn ownership_fails_closed_on_missing_unknown_duplicate_or_conflicting_routes() {
        let readiness = ready_route_readiness();
        let first_route = REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES[0];
        let mut routes = nowledge_mem_route_ownership_all_hawdb();
        routes.pop();
        routes.push(NowledgeMemRouteOwnership::new(
            "/unknown",
            NowledgeMemRouteReadEngine::HawDB,
        ));
        routes.push(NowledgeMemRouteOwnership::new(
            first_route,
            NowledgeMemRouteReadEngine::Legacy,
        ));

        let report = nowledge_mem_route_ownership_readiness(
            &routes,
            Some(&readiness),
            NowledgeMemRouteOwnershipPolicy::migration(),
        );

        assert!(!report.ready);
        assert!(!report.production_cutover_ready);
        assert_eq!(
            report.missing_required_routes,
            vec![REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                [REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES.len() - 1]
                .to_string()]
        );
        assert_eq!(report.unknown_routes, vec!["/unknown".to_string()]);
        assert_eq!(report.duplicate_routes, vec![first_route.to_string()]);
        assert_eq!(report.conflicting_routes, vec![first_route.to_string()]);
    }

    fn ready_route_readiness() -> NowledgeMemRouteReadinessSummary {
        NowledgeMemRouteReadinessSummary {
            route_primary_ready: true,
            primary_ready_routes: REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
                .iter()
                .map(|route| (*route).to_string())
                .collect(),
            route_query_plan_evidence_ready: true,
            route_query_profile_evidence_ready: true,
            route_query_api_behavior_evidence_ready: true,
            relationship_property_pruning_required_count: 1,
            relationship_property_pruning_report_count: 1,
            route_relationship_property_pruning_evidence_ready: true,
        }
    }
}
