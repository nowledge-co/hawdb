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

//! Embedded database adapters for the internal compatibility harness.

use crate::api::{Database, DatabaseSession, QueryOutput};
use crate::{Result, Value};
use hawdb_analytics::ProjectedGraph;
use hawdb_compat::{CompatibilityPrimaryEngine, CompatibilityPrimarySession};
use std::collections::BTreeMap;

pub use hawdb_compat::{
    add_shadow_ready_report, add_shadow_run_report, add_shadow_trace_report,
    assess_compatibility_cutover, assess_compatibility_cypher_migration_gate_bundle,
    assess_compatibility_cypher_migration_gate_bundle_with_rollback,
    assess_compatibility_migration_gate, assess_compatibility_migration_gate_bundle,
    assess_compatibility_migration_gate_with_rollback, assess_external_shadow_cutover_evidence,
    assess_query_inventory_coverage, assess_query_inventory_cypher_coverage,
    assess_query_inventory_gate, build_compatibility_query_inventory,
    build_compatibility_query_inventory_from_json,
    build_compatibility_query_inventory_from_json_str, compatibility_cutover_report_to_json,
    compatibility_inventory_coverage_report_to_json, compatibility_inventory_gate_report_to_json,
    compatibility_migration_gate_bundle_to_json, compatibility_migration_gate_report_to_json,
    compatibility_query_inventory_to_json, cutover_evidence_is_eligible,
    enforce_external_shadow_adapter_smoke_requirements, external_shadow_adapter_smoke_fixture,
    external_shadow_adapter_smoke_report_json, external_shadow_json_from_value,
    external_shadow_ready_missing_capabilities, external_shadow_trace_health_from_bundle,
    external_shadow_trace_report_json, external_shadow_value_from_json, is_self_shadow_command,
    nowledge_memory_core_fixture, nowledge_memory_core_inventory, should_run_shadow_ready,
    CompatibilityCheck, CompatibilityCheckReport, CompatibilityCutoverDecision,
    CompatibilityCutoverPolicy, CompatibilityCutoverReport, CompatibilityFixture,
    CompatibilityInventoryCoveragePolicy, CompatibilityInventoryCoverageReport,
    CompatibilityInventoryGateReport, CompatibilityMigrationGateBundle,
    CompatibilityMigrationGateReport, CompatibilityQueryCallSite, CompatibilityQueryInventory,
    CompatibilityQueryInventoryItem, CompatibilityReport, CompatibilityRollbackEvidence,
    CompatibilityShadowCheckReport, CompatibilityShadowEngine, CompatibilityShadowReport,
    CompatibilityShadowStatus, CompatibilityTolerance, CypherExecutionMode, CypherFixtureCheck,
    CypherFixtureStatement, ExpectedErrorClass, ExpectedRows, ExternalShadowCommand,
    ExternalShadowCutoverEvidence, ExternalShadowProjectGraphReply,
    ExternalShadowProjectGraphRequest, ExternalShadowProtocolBackend, ExternalShadowProtocolServer,
    ExternalShadowReady, ExternalShadowStatementRequest, ExternalShadowTraceHealth,
    ExternalShadowTraceSummary, ProjectedGraphFixtureCheck, ProjectedGraphShadowOutput,
    ProjectedGraphShadowResult, ShadowRequestContext, ShadowRequestPhase,
    EXTERNAL_SHADOW_PROTOCOL_VERSION, REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
};

pub fn run_compatibility_fixture(
    db: &mut Database,
    fixture: &CompatibilityFixture,
) -> Result<CompatibilityReport> {
    hawdb_compat::run_compatibility_fixture(db, fixture)
}

pub fn run_compatibility_fixture_with_shadow(
    db: &mut Database,
    fixture: &CompatibilityFixture,
    shadow: &mut impl CompatibilityShadowEngine,
) -> Result<CompatibilityShadowReport> {
    hawdb_compat::run_compatibility_fixture_with_shadow(db, fixture, shadow)
}

impl CompatibilityPrimaryEngine for Database {
    type Session<'a> = DatabaseSession<'a>;

    fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        Database::query_with_params(self, cypher, parameters)
    }

    fn explain_plan_with_params(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<String> {
        Ok(
            Database::explain_query_with_params(self, cypher, parameters)?
                .physical_plan
                .explain(0),
        )
    }

    fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph {
        Database::project_graph(self, rel_type)
    }

    fn session(&mut self) -> Self::Session<'_> {
        Database::session(self)
    }
}

impl CompatibilityPrimarySession for DatabaseSession<'_> {
    fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput> {
        DatabaseSession::query_with_params(self, cypher, parameters)
    }
}

#[cfg(test)]
use hawdb_compat::projected_graph_shadow_output;
#[cfg(test)]
mod facade_tests;
#[cfg(test)]
#[path = "../crates/compat/src/test_support.rs"]
mod test_support;
#[cfg(test)]
mod tests;
