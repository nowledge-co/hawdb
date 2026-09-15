//! Internal compatibility fixtures, comparison, and developer shadow protocols.
//!
//! Hosts continue to use the embedded `skein` facade. Database execution is
//! supplied by the facade; this crate cannot open or activate a database.

use skein_analytics::ProjectedGraph;
use skein_core::{Result, SkeinError, Value};
use skein_executor::{QueryOutput, Row};
use std::collections::BTreeMap;

mod primary;
#[doc(hidden)]
pub use primary::{CompatibilityPrimaryEngine, CompatibilityPrimarySession};

mod external_shadow;
mod inventory_gate;
mod nowledge_fixture;
#[doc(hidden)]
pub mod nowledge_inventory;

#[cfg(test)]
use external_shadow::{
    decode_external_projected_graph_response, decode_external_query_response,
    decode_external_ready_response, decode_external_session_response,
};
pub use external_shadow::{
    external_shadow_json_from_value, external_shadow_ready_missing_capabilities,
    external_shadow_trace_health_from_bundle, external_shadow_trace_report_json,
    external_shadow_value_from_json, ExternalShadowCommand, ExternalShadowProjectGraphReply,
    ExternalShadowProjectGraphRequest, ExternalShadowProtocolBackend, ExternalShadowProtocolServer,
    ExternalShadowReady, ExternalShadowStatementRequest, ExternalShadowTraceHealth,
    ExternalShadowTraceSummary, REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
};
pub use inventory_gate::{
    assess_compatibility_cypher_migration_gate_bundle,
    assess_compatibility_cypher_migration_gate_bundle_with_rollback,
    assess_compatibility_migration_gate, assess_compatibility_migration_gate_bundle,
    assess_compatibility_migration_gate_with_rollback, assess_query_inventory_coverage,
    assess_query_inventory_cypher_coverage, assess_query_inventory_gate,
    build_compatibility_query_inventory, build_compatibility_query_inventory_from_json,
    build_compatibility_query_inventory_from_json_str, compatibility_cutover_report_to_json,
    compatibility_inventory_coverage_report_to_json, compatibility_inventory_gate_report_to_json,
    compatibility_migration_gate_bundle_to_json, compatibility_migration_gate_report_to_json,
    compatibility_query_inventory_to_json, CompatibilityCutoverDecision,
    CompatibilityInventoryCoveragePolicy, CompatibilityInventoryCoverageReport,
    CompatibilityInventoryGateReport, CompatibilityMigrationGateBundle,
    CompatibilityMigrationGateReport, CompatibilityQueryCallSite, CompatibilityQueryInventory,
    CompatibilityQueryInventoryItem, CompatibilityRollbackEvidence,
};
pub use nowledge_fixture::{nowledge_memory_core_fixture, nowledge_memory_core_inventory};
pub use nowledge_inventory::{
    scan_nowledge_query_inventory_cypher_coverage_detail_to_json,
    scan_nowledge_query_inventory_cypher_coverage_to_json, NowledgeCypherMigrationGateJsonOptions,
};

const DEFAULT_FLOAT_ABS_TOLERANCE: f64 = 1.0e-9;
pub const EXTERNAL_SHADOW_PROTOCOL_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq)]
pub struct CompatibilityFixture {
    pub name: String,
    pub setup: Vec<CypherFixtureStatement>,
    pub checks: Vec<CompatibilityCheck>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CypherFixtureStatement {
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityCheck {
    Cypher(CypherFixtureCheck),
    ProjectedGraph(ProjectedGraphFixtureCheck),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CypherFixtureCheck {
    pub name: String,
    pub setup_queries: Vec<CypherFixtureStatement>,
    pub statement: CypherFixtureStatement,
    pub expected_rows: ExpectedRows,
    pub expected_error: Option<ExpectedErrorClass>,
    pub effect_query: Option<CypherFixtureStatement>,
    pub effect_expected_rows: Option<ExpectedRows>,
    pub expected_plan_contains: Vec<String>,
    pub tolerance: CompatibilityTolerance,
    pub execution_mode: CypherExecutionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedRows {
    Exact(Vec<Row>),
    Unordered(Vec<Row>),
    RowCount(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedErrorClass {
    Parse,
    Semantic,
    Storage,
    Execution,
    CapabilityUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CypherExecutionMode {
    Database,
    Session,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompatibilityTolerance {
    pub float_abs: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedGraphFixtureCheck {
    pub name: String,
    pub rel_type: Option<String>,
    pub expected_node_count: usize,
    pub expected_edge_count: usize,
    pub expected_incoming: Vec<(u64, Vec<u64>)>,
    pub expected_communities: Vec<(u64, u64)>,
    pub expected_hierarchical_communities: Vec<(usize, u64, u64)>,
    pub expected_page_rank_scores: Vec<(u64, f64)>,
    pub page_rank_top_node: Option<u64>,
    pub tolerance: CompatibilityTolerance,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub fixture: String,
    pub checks: Vec<CompatibilityCheckReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityCheckReport {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityShadowReport {
    pub fixture: String,
    pub shadow_engine: String,
    pub primary_checks: Vec<CompatibilityCheckReport>,
    pub shadow_checks: Vec<CompatibilityShadowCheckReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityShadowCheckReport {
    pub name: String,
    pub status: CompatibilityShadowStatus,
    pub primary_only_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompatibilityShadowStatus {
    Matched,
    PrimaryOnly,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectedGraphShadowResult {
    Output(ProjectedGraphShadowOutput),
    PrimaryOnly { reason: Option<String> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompatibilityCutoverPolicy {
    pub require_shadow_for_all_checks: bool,
    pub min_matched_checks: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityCutoverReport {
    pub fixture: String,
    pub shadow_engine: String,
    pub decision: CompatibilityCutoverDecision,
    pub primary_check_count: usize,
    pub total_checks: usize,
    pub matched_checks: usize,
    pub primary_only_checks: Vec<String>,
    pub primary_only_reasons: BTreeMap<String, String>,
    pub blockers: Vec<String>,
}

pub trait CompatibilityShadowEngine {
    fn name(&self) -> &str;
    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput>;
    fn execute_with_context(
        &mut self,
        statement: &CypherFixtureStatement,
        _context: ShadowRequestContext,
    ) -> Result<QueryOutput> {
        self.execute(statement)
    }

    fn execute_session(
        &mut self,
        statements: &[CypherFixtureStatement],
    ) -> Result<Vec<QueryOutput>> {
        statements
            .iter()
            .map(|statement| self.execute(statement))
            .collect()
    }
    fn execute_session_with_context(
        &mut self,
        statements: &[CypherFixtureStatement],
        _context: ShadowRequestContext,
    ) -> Result<Vec<QueryOutput>> {
        self.execute_session(statements)
    }

    fn project_graph(
        &mut self,
        _check: &ProjectedGraphFixtureCheck,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        Ok(None)
    }
    fn project_graph_with_context(
        &mut self,
        check: &ProjectedGraphFixtureCheck,
        _context: ShadowRequestContext,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        self.project_graph(check)
    }
    fn project_graph_result_with_context(
        &mut self,
        check: &ProjectedGraphFixtureCheck,
        context: ShadowRequestContext,
    ) -> Result<ProjectedGraphShadowResult> {
        match self.project_graph_with_context(check, context)? {
            Some(output) => Ok(ProjectedGraphShadowResult::Output(output)),
            None => Ok(ProjectedGraphShadowResult::PrimaryOnly { reason: None }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShadowRequestContext<'a> {
    pub fixture: &'a str,
    pub check: Option<&'a str>,
    pub phase: ShadowRequestPhase,
    pub statement_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowRequestPhase {
    FixtureSetup,
    CheckSetup,
    Statement,
    Session,
    Effect,
    ProjectGraph,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedGraphShadowOutput {
    pub node_count: usize,
    pub edge_count: usize,
    pub incoming: Vec<(u64, Vec<u64>)>,
    pub communities: Vec<(u64, u64)>,
    pub hierarchical_communities: Vec<(usize, u64, u64)>,
    pub page_rank_scores: Vec<(u64, f64)>,
    pub page_rank_top_node: Option<u64>,
}

impl Default for CompatibilityTolerance {
    fn default() -> Self {
        Self {
            float_abs: DEFAULT_FLOAT_ABS_TOLERANCE,
        }
    }
}

impl Default for CompatibilityCutoverPolicy {
    fn default() -> Self {
        Self {
            require_shadow_for_all_checks: true,
            min_matched_checks: 1,
        }
    }
}
fn is_mutation_statement(cypher: &str) -> bool {
    let upper = cypher.to_ascii_uppercase();
    upper.contains(" CREATE ")
        || upper.starts_with("CREATE ")
        || upper.contains(" MERGE ")
        || upper.starts_with("MERGE ")
        || upper.contains(" SET ")
        || upper.starts_with("SET ")
        || upper.contains(" DELETE ")
        || upper.starts_with("DELETE ")
        || upper.contains(" DETACH DELETE ")
        || upper.contains(" ON CREATE SET ")
        || upper.contains(" ON MATCH SET ")
        || upper.contains("DROP ")
}

impl CypherFixtureStatement {
    pub fn new(cypher: impl Into<String>) -> Self {
        Self {
            cypher: cypher.into(),
            parameters: BTreeMap::new(),
        }
    }

    pub fn with_parameters(cypher: impl Into<String>, parameters: BTreeMap<String, Value>) -> Self {
        Self {
            cypher: cypher.into(),
            parameters,
        }
    }
}

impl CypherFixtureCheck {
    pub fn expect_rows(
        name: impl Into<String>,
        statement: CypherFixtureStatement,
        expected_rows: ExpectedRows,
    ) -> Self {
        Self {
            name: name.into(),
            setup_queries: Vec::new(),
            statement,
            expected_rows,
            expected_error: None,
            effect_query: None,
            effect_expected_rows: None,
            expected_plan_contains: Vec::new(),
            tolerance: CompatibilityTolerance::default(),
            execution_mode: CypherExecutionMode::Database,
        }
    }

    pub fn expect_error(
        name: impl Into<String>,
        statement: CypherFixtureStatement,
        expected_error: ExpectedErrorClass,
    ) -> Self {
        Self {
            name: name.into(),
            setup_queries: Vec::new(),
            statement,
            expected_rows: ExpectedRows::RowCount(0),
            expected_error: Some(expected_error),
            effect_query: None,
            effect_expected_rows: None,
            expected_plan_contains: Vec::new(),
            tolerance: CompatibilityTolerance::default(),
            execution_mode: CypherExecutionMode::Database,
        }
    }

    pub fn with_plan_contains(mut self, expected_plan_contains: Vec<String>) -> Self {
        self.expected_plan_contains = expected_plan_contains;
        self
    }

    pub fn with_tolerance(mut self, tolerance: CompatibilityTolerance) -> Self {
        self.tolerance = tolerance;
        self
    }

    pub fn with_session_execution(mut self) -> Self {
        self.execution_mode = CypherExecutionMode::Session;
        self
    }

    pub fn with_setup_query(mut self, setup_query: CypherFixtureStatement) -> Self {
        self.setup_queries.push(setup_query);
        self
    }

    pub fn with_effect_query(
        mut self,
        effect_query: CypherFixtureStatement,
        effect_expected_rows: ExpectedRows,
    ) -> Self {
        self.effect_query = Some(effect_query);
        self.effect_expected_rows = Some(effect_expected_rows);
        self
    }
}

impl ExpectedErrorClass {
    fn from_error(error: &SkeinError) -> Self {
        match error {
            SkeinError::Parse(_) => Self::Parse,
            SkeinError::Semantic(_) => Self::Semantic,
            SkeinError::Storage(_)
            | SkeinError::StorageIntegrity(_)
            | SkeinError::AppendSequenceExhausted { .. } => Self::Storage,
            SkeinError::Execution(_) => Self::Execution,
            SkeinError::CapabilityUnavailable { .. } => Self::CapabilityUnavailable,
        }
    }
}

pub fn run_compatibility_fixture(
    db: &mut impl CompatibilityPrimaryEngine,
    fixture: &CompatibilityFixture,
) -> Result<CompatibilityReport> {
    run_primary_setup(db, fixture)?;

    Ok(CompatibilityReport {
        fixture: fixture.name.clone(),
        checks: run_primary_checks(db, fixture)?
            .into_iter()
            .map(|check| check.report)
            .collect(),
    })
}

pub fn run_compatibility_fixture_with_shadow(
    db: &mut impl CompatibilityPrimaryEngine,
    fixture: &CompatibilityFixture,
    shadow: &mut impl CompatibilityShadowEngine,
) -> Result<CompatibilityShadowReport> {
    run_primary_setup(db, fixture)?;
    run_shadow_setup(fixture, shadow)?;

    let mut primary_checks = Vec::new();
    let mut shadow_checks = Vec::new();
    for check in &fixture.checks {
        match check {
            CompatibilityCheck::Cypher(check) => {
                let primary = run_cypher_check(db, fixture, check)?;
                primary_checks.push(CompatibilityCheckReport {
                    name: check.name.clone(),
                });
                run_shadow_cypher_check(fixture, check, shadow, &primary)?;
                shadow_checks.push(CompatibilityShadowCheckReport {
                    name: check.name.clone(),
                    status: CompatibilityShadowStatus::Matched,
                    primary_only_reason: None,
                });
            }
            CompatibilityCheck::ProjectedGraph(check) => {
                let primary = run_projected_graph_check(db, fixture, check)?;
                primary_checks.push(CompatibilityCheckReport {
                    name: check.name.clone(),
                });
                let (status, primary_only_reason) = match shadow.project_graph_result_with_context(
                    check,
                    ShadowRequestContext {
                        fixture: &fixture.name,
                        check: Some(&check.name),
                        phase: ShadowRequestPhase::ProjectGraph,
                        statement_index: None,
                    },
                )? {
                    ProjectedGraphShadowResult::Output(shadow_output) => {
                        compare_projected_graph_shadow(
                            fixture,
                            check,
                            shadow.name(),
                            &primary,
                            &shadow_output,
                        )?;
                        (CompatibilityShadowStatus::Matched, None)
                    }
                    ProjectedGraphShadowResult::PrimaryOnly { reason } => {
                        (CompatibilityShadowStatus::PrimaryOnly, reason)
                    }
                };
                shadow_checks.push(CompatibilityShadowCheckReport {
                    name: check.name.clone(),
                    status,
                    primary_only_reason,
                });
            }
        }
    }

    Ok(CompatibilityShadowReport {
        fixture: fixture.name.clone(),
        shadow_engine: shadow.name().to_string(),
        primary_checks,
        shadow_checks,
    })
}

pub fn assess_compatibility_cutover(
    report: &CompatibilityShadowReport,
    policy: CompatibilityCutoverPolicy,
) -> CompatibilityCutoverReport {
    let total_checks = report.shadow_checks.len();
    let matched_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::Matched)
        .count();
    let primary_only_checks = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
        .map(|check| check.name.clone())
        .collect::<Vec<_>>();
    let primary_only_reasons = report
        .shadow_checks
        .iter()
        .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
        .filter_map(|check| {
            check
                .primary_only_reason
                .as_ref()
                .map(|reason| (check.name.clone(), reason.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut blockers = Vec::new();

    if total_checks == 0 {
        blockers.push("no compatibility checks were executed".to_string());
    }
    if report.primary_checks.len() != total_checks {
        blockers.push(format!(
            "primary check count {} does not match shadow check count {}",
            report.primary_checks.len(),
            total_checks
        ));
    }
    if matched_checks < policy.min_matched_checks {
        blockers.push(format!(
            "matched check count {} is below required minimum {}",
            matched_checks, policy.min_matched_checks
        ));
    }
    if policy.require_shadow_for_all_checks && !primary_only_checks.is_empty() {
        let primary_only_descriptions = report
            .shadow_checks
            .iter()
            .filter(|check| check.status == CompatibilityShadowStatus::PrimaryOnly)
            .map(|check| match &check.primary_only_reason {
                Some(reason) => format!("{} ({})", check.name, reason),
                None => check.name.clone(),
            })
            .collect::<Vec<_>>();
        blockers.push(format!(
            "shadow engine '{}' did not cover checks: {}",
            report.shadow_engine,
            primary_only_descriptions.join(", ")
        ));
    }

    CompatibilityCutoverReport {
        fixture: report.fixture.clone(),
        shadow_engine: report.shadow_engine.clone(),
        decision: if blockers.is_empty() {
            CompatibilityCutoverDecision::Ready
        } else {
            CompatibilityCutoverDecision::Blocked
        },
        primary_check_count: report.primary_checks.len(),
        total_checks,
        matched_checks,
        primary_only_checks,
        primary_only_reasons,
        blockers,
    }
}
fn run_primary_setup(
    db: &mut impl CompatibilityPrimaryEngine,
    fixture: &CompatibilityFixture,
) -> Result<()> {
    for statement in &fixture.setup {
        db.query_with_params(&statement.cypher, &statement.parameters)
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' setup failed for '{}': {error}",
                    fixture.name, statement.cypher
                ))
            })?;
    }
    Ok(())
}

fn run_shadow_setup(
    fixture: &CompatibilityFixture,
    shadow: &mut impl CompatibilityShadowEngine,
) -> Result<()> {
    for statement in &fixture.setup {
        shadow
            .execute_with_context(
                statement,
                ShadowRequestContext {
                    fixture: &fixture.name,
                    check: None,
                    phase: ShadowRequestPhase::FixtureSetup,
                    statement_index: None,
                },
            )
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' shadow engine '{}' setup failed for '{}': {error}",
                    fixture.name,
                    shadow.name(),
                    statement.cypher
                ))
            })?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrimaryCheckOutput {
    report: CompatibilityCheckReport,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CypherCheckOutcome {
    Rows {
        output: QueryOutput,
        effect: Option<QueryOutput>,
    },
    Error(ExpectedErrorClass),
}

fn run_primary_checks(
    db: &mut impl CompatibilityPrimaryEngine,
    fixture: &CompatibilityFixture,
) -> Result<Vec<PrimaryCheckOutput>> {
    let mut reports = Vec::new();
    for check in &fixture.checks {
        match check {
            CompatibilityCheck::Cypher(check) => {
                run_cypher_check(db, fixture, check)?;
                reports.push(PrimaryCheckOutput {
                    report: CompatibilityCheckReport {
                        name: check.name.clone(),
                    },
                });
            }
            CompatibilityCheck::ProjectedGraph(check) => {
                run_projected_graph_check(db, fixture, check)?;
                reports.push(PrimaryCheckOutput {
                    report: CompatibilityCheckReport {
                        name: check.name.clone(),
                    },
                });
            }
        }
    }
    Ok(reports)
}

fn run_shadow_cypher_check(
    fixture: &CompatibilityFixture,
    check: &CypherFixtureCheck,
    shadow: &mut impl CompatibilityShadowEngine,
    primary: &CypherCheckOutcome,
) -> Result<()> {
    if check.execution_mode == CypherExecutionMode::Session {
        return run_shadow_cypher_session_check(fixture, check, shadow, primary);
    }
    for setup_query in &check.setup_queries {
        shadow
            .execute_with_context(
                setup_query,
                ShadowRequestContext {
                    fixture: &fixture.name,
                    check: Some(&check.name),
                    phase: ShadowRequestPhase::CheckSetup,
                    statement_index: None,
                },
            )
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' check '{}' shadow engine '{}' setup failed for '{}': {error}",
                    fixture.name,
                    check.name,
                    shadow.name(),
                    setup_query.cypher
                ))
            })?;
    }
    let shadow_output = shadow.execute_with_context(
        &check.statement,
        ShadowRequestContext {
            fixture: &fixture.name,
            check: Some(&check.name),
            phase: ShadowRequestPhase::Statement,
            statement_index: None,
        },
    );
    match (primary, check.expected_error) {
        (CypherCheckOutcome::Error(expected), Some(_)) => {
            let Err(error) = shadow_output else {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' shadow engine '{}' expected {:?} error for '{}', got success",
                    fixture.name,
                    check.name,
                    shadow.name(),
                    expected,
                    check.statement.cypher
                )));
            };
            let actual = ExpectedErrorClass::from_error(&error);
            if actual != *expected {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' shadow engine '{}' expected {:?} error for '{}', got {:?}: {error}",
                    fixture.name,
                    check.name,
                    shadow.name(),
                    expected,
                    check.statement.cypher,
                    actual
                )));
            }
        }
        (
            CypherCheckOutcome::Rows {
                output: primary_output,
                effect: primary_effect,
            },
            None,
        ) => {
            let shadow_output = shadow_output.map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' check '{}' shadow engine '{}' failed for '{}': {error}",
                    fixture.name,
                    check.name,
                    shadow.name(),
                    check.statement.cypher
                ))
            })?;
            check
                .expected_rows
                .assert_matches(
                    &fixture.name,
                    &format!("{} shadow {}", check.name, shadow.name()),
                    &check.statement.cypher,
                    &shadow_output,
                    check.tolerance,
                )
                .map_err(|error| {
                    SkeinError::Execution(format!(
                        "fixture '{}' check '{}' shadow engine '{}' row validation failed: {error}",
                        fixture.name,
                        check.name,
                        shadow.name()
                    ))
                })?;
            check.expected_rows.assert_shadow_matches_primary(
                &fixture.name,
                &check.name,
                shadow.name(),
                primary_output,
                &shadow_output,
                check.tolerance,
            )?;
            if let Some(effect_query) = &check.effect_query {
                let Some(primary_effect) = primary_effect else {
                    return Err(SkeinError::Execution(format!(
                        "fixture '{}' check '{}' missing primary effect output",
                        fixture.name, check.name
                    )));
                };
                let shadow_effect = shadow
                    .execute_with_context(
                        effect_query,
                        ShadowRequestContext {
                            fixture: &fixture.name,
                            check: Some(&check.name),
                            phase: ShadowRequestPhase::Effect,
                            statement_index: None,
                        },
                    )
                    .map_err(|error| {
                    SkeinError::Execution(format!(
                        "fixture '{}' check '{}' shadow engine '{}' failed effect query '{}': {error}",
                        fixture.name,
                        check.name,
                        shadow.name(),
                        effect_query.cypher
                    ))
                })?;
                let expected = check.effect_expected_rows.as_ref().ok_or_else(|| {
                    SkeinError::Execution(format!(
                        "fixture '{}' check '{}' effect query is missing expected rows",
                        fixture.name, check.name
                    ))
                })?;
                expected.assert_matches(
                    &fixture.name,
                    &format!("{} effect shadow {}", check.name, shadow.name()),
                    &effect_query.cypher,
                    &shadow_effect,
                    check.tolerance,
                )?;
                expected.assert_shadow_matches_primary(
                    &fixture.name,
                    &format!("{} effect", check.name),
                    shadow.name(),
                    primary_effect,
                    &shadow_effect,
                    check.tolerance,
                )?;
            }
        }
        _ => {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' has inconsistent primary outcome and expectation",
                fixture.name, check.name
            )));
        }
    }
    Ok(())
}

fn run_shadow_cypher_session_check(
    fixture: &CompatibilityFixture,
    check: &CypherFixtureCheck,
    shadow: &mut impl CompatibilityShadowEngine,
    primary: &CypherCheckOutcome,
) -> Result<()> {
    let mut statements = check.setup_queries.clone();
    statements.push(check.statement.clone());
    if let Some(effect_query) = &check.effect_query {
        statements.push(effect_query.clone());
    }
    let outputs = shadow
        .execute_session_with_context(
            &statements,
            ShadowRequestContext {
                fixture: &fixture.name,
                check: Some(&check.name),
                phase: ShadowRequestPhase::Session,
                statement_index: None,
            },
        )
        .map_err(|error| {
            SkeinError::Execution(format!(
                "fixture '{}' check '{}' shadow engine '{}' session failed: {error}",
                fixture.name,
                check.name,
                shadow.name()
            ))
        })?;
    let statement_index = check.setup_queries.len();
    let shadow_output = outputs.get(statement_index).ok_or_else(|| {
        SkeinError::Execution(format!(
            "fixture '{}' check '{}' shadow engine '{}' session returned no statement output",
            fixture.name,
            check.name,
            shadow.name()
        ))
    })?;

    let CypherCheckOutcome::Rows {
        output: primary_output,
        effect: primary_effect,
    } = primary
    else {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' has inconsistent session primary outcome",
            fixture.name, check.name
        )));
    };

    check.expected_rows.assert_matches(
        &fixture.name,
        &format!("{} shadow {}", check.name, shadow.name()),
        &check.statement.cypher,
        shadow_output,
        check.tolerance,
    )?;
    check.expected_rows.assert_shadow_matches_primary(
        &fixture.name,
        &check.name,
        shadow.name(),
        primary_output,
        shadow_output,
        check.tolerance,
    )?;

    if let Some(effect_query) = &check.effect_query {
        let Some(primary_effect) = primary_effect else {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' missing primary effect output",
                fixture.name, check.name
            )));
        };
        let effect_index = statements.len() - 1;
        let shadow_effect = outputs.get(effect_index).ok_or_else(|| {
            SkeinError::Execution(format!(
                "fixture '{}' check '{}' shadow engine '{}' session returned no effect output",
                fixture.name,
                check.name,
                shadow.name()
            ))
        })?;
        let expected = check.effect_expected_rows.as_ref().ok_or_else(|| {
            SkeinError::Execution(format!(
                "fixture '{}' check '{}' effect query is missing expected rows",
                fixture.name, check.name
            ))
        })?;
        expected.assert_matches(
            &fixture.name,
            &format!("{} effect shadow {}", check.name, shadow.name()),
            &effect_query.cypher,
            shadow_effect,
            check.tolerance,
        )?;
        expected.assert_shadow_matches_primary(
            &fixture.name,
            &format!("{} effect", check.name),
            shadow.name(),
            primary_effect,
            shadow_effect,
            check.tolerance,
        )?;
    }

    Ok(())
}

fn run_cypher_check(
    db: &mut impl CompatibilityPrimaryEngine,
    fixture: &CompatibilityFixture,
    check: &CypherFixtureCheck,
) -> Result<CypherCheckOutcome> {
    if check.execution_mode == CypherExecutionMode::Session {
        return run_cypher_session_check(db, fixture, check);
    }
    for setup_query in &check.setup_queries {
        db.query_with_params(&setup_query.cypher, &setup_query.parameters)
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' check '{}' setup failed for '{}': {error}",
                    fixture.name, check.name, setup_query.cypher
                ))
            })?;
    }
    let output = db.query_with_params(&check.statement.cypher, &check.statement.parameters);
    if let Some(expected_error) = check.expected_error {
        let error = match output {
            Ok(output) => {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected {:?} error for '{}', got rows {:?}",
                    fixture.name, check.name, expected_error, check.statement.cypher, output.rows
                )));
            }
            Err(error) => error,
        };
        let actual = ExpectedErrorClass::from_error(&error);
        if actual != expected_error {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' expected {:?} error for '{}', got {:?}: {error}",
                fixture.name, check.name, expected_error, check.statement.cypher, actual
            )));
        }
        return Ok(CypherCheckOutcome::Error(expected_error));
    }

    let output = output?;
    check.expected_rows.assert_matches(
        &fixture.name,
        &check.name,
        &check.statement.cypher,
        &output,
        check.tolerance,
    )?;

    if !check.expected_plan_contains.is_empty() {
        let plan =
            db.explain_plan_with_params(&check.statement.cypher, &check.statement.parameters)?;
        for needle in &check.expected_plan_contains {
            if !plan.contains(needle) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected plan to contain '{}', got:\n{}",
                    fixture.name, check.name, needle, plan
                )));
            }
        }
    }

    let effect = if let Some(effect_query) = &check.effect_query {
        let effect = db.query_with_params(&effect_query.cypher, &effect_query.parameters)?;
        let expected = check.effect_expected_rows.as_ref().ok_or_else(|| {
            SkeinError::Execution(format!(
                "fixture '{}' check '{}' effect query is missing expected rows",
                fixture.name, check.name
            ))
        })?;
        expected.assert_matches(
            &fixture.name,
            &format!("{} effect", check.name),
            &effect_query.cypher,
            &effect,
            check.tolerance,
        )?;
        Some(effect)
    } else {
        None
    };

    Ok(CypherCheckOutcome::Rows { output, effect })
}

fn run_cypher_session_check(
    db: &mut impl CompatibilityPrimaryEngine,
    fixture: &CompatibilityFixture,
    check: &CypherFixtureCheck,
) -> Result<CypherCheckOutcome> {
    let mut session = db.session();
    for setup_query in &check.setup_queries {
        session
            .query_with_params(&setup_query.cypher, &setup_query.parameters)
            .map_err(|error| {
                SkeinError::Execution(format!(
                    "fixture '{}' check '{}' session setup failed for '{}': {error}",
                    fixture.name, check.name, setup_query.cypher
                ))
            })?;
    }

    let output = session.query_with_params(&check.statement.cypher, &check.statement.parameters)?;
    check.expected_rows.assert_matches(
        &fixture.name,
        &check.name,
        &check.statement.cypher,
        &output,
        check.tolerance,
    )?;

    let effect = if let Some(effect_query) = &check.effect_query {
        let effect = session.query_with_params(&effect_query.cypher, &effect_query.parameters)?;
        let expected = check.effect_expected_rows.as_ref().ok_or_else(|| {
            SkeinError::Execution(format!(
                "fixture '{}' check '{}' effect query is missing expected rows",
                fixture.name, check.name
            ))
        })?;
        expected.assert_matches(
            &fixture.name,
            &format!("{} effect", check.name),
            &effect_query.cypher,
            &effect,
            check.tolerance,
        )?;
        Some(effect)
    } else {
        None
    };

    Ok(CypherCheckOutcome::Rows { output, effect })
}

fn run_projected_graph_check(
    db: &impl CompatibilityPrimaryEngine,
    fixture: &CompatibilityFixture,
    check: &ProjectedGraphFixtureCheck,
) -> Result<ProjectedGraphShadowOutput> {
    let graph = db.project_graph(check.rel_type.as_deref());
    let output = projected_graph_shadow_output(&graph, check);
    assert_projected_graph_matches_fixture(fixture, check, &output)?;
    Ok(output)
}

#[doc(hidden)]
pub fn projected_graph_shadow_output(
    graph: &ProjectedGraph,
    check: &ProjectedGraphFixtureCheck,
) -> ProjectedGraphShadowOutput {
    let page_rank_scores = graph
        .page_rank(Default::default())
        .into_iter()
        .map(|score| (score.node.0, score.score))
        .collect::<Vec<_>>();
    ProjectedGraphShadowOutput {
        node_count: graph.node_count(),
        edge_count: graph.edge_count(),
        incoming: check
            .expected_incoming
            .iter()
            .map(|(node, _)| {
                let sources = graph
                    .incoming_sources(skein_storage::NodeId(*node))
                    .map(|sources| sources.map(|source| source.0).collect::<Vec<_>>())
                    .unwrap_or_default();
                (*node, sources)
            })
            .collect(),
        communities: if check.expected_communities.is_empty() {
            Vec::new()
        } else {
            graph
                .louvain_communities(Default::default())
                .into_iter()
                .map(|assignment| (assignment.node.0, assignment.community.0))
                .collect()
        },
        hierarchical_communities: if check.expected_hierarchical_communities.is_empty() {
            Vec::new()
        } else {
            graph
                .hierarchical_louvain_communities(Default::default())
                .into_iter()
                .map(|assignment| (assignment.level, assignment.node.0, assignment.community.0))
                .collect()
        },
        page_rank_top_node: page_rank_scores.first().map(|(node, _)| *node),
        page_rank_scores,
    }
}

fn assert_projected_graph_matches_fixture(
    fixture: &CompatibilityFixture,
    check: &ProjectedGraphFixtureCheck,
    output: &ProjectedGraphShadowOutput,
) -> Result<()> {
    if output.node_count != check.expected_node_count {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' expected {} projected nodes, got {}",
            fixture.name, check.name, check.expected_node_count, output.node_count
        )));
    }
    if output.edge_count != check.expected_edge_count {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' expected {} projected edges, got {}",
            fixture.name, check.name, check.expected_edge_count, output.edge_count
        )));
    }
    for (node, expected_sources) in &check.expected_incoming {
        let actual = output
            .incoming
            .iter()
            .find(|(candidate, _)| candidate == node)
            .map(|(_, sources)| sources.clone())
            .unwrap_or_default();
        if actual != *expected_sources {
            return Err(SkeinError::Execution(format!(
                "fixture '{}' check '{}' expected incoming sources {:?} for node {}, got {:?}",
                fixture.name, check.name, expected_sources, node, actual
            )));
        }
    }

    if let Some(expected_node) = check.page_rank_top_node
        && output.page_rank_top_node != Some(expected_node)
    {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' expected PageRank top node {}, got {:?}",
            fixture.name, check.name, expected_node, output.page_rank_top_node
        )));
    }
    if !check.expected_communities.is_empty() {
        let communities = output
            .communities
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>();
        for (node, expected_community) in &check.expected_communities {
            let actual = communities.get(node).copied();
            if actual != Some(*expected_community) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected community {} for node {}, got {:?}",
                    fixture.name, check.name, expected_community, node, actual
                )));
            }
        }
    }
    if !check.expected_hierarchical_communities.is_empty() {
        let communities = output
            .hierarchical_communities
            .iter()
            .copied()
            .map(|(level, node, community)| ((level, node), community))
            .collect::<BTreeMap<_, _>>();
        for (level, node, expected_community) in &check.expected_hierarchical_communities {
            let actual = communities.get(&(*level, *node)).copied();
            if actual != Some(*expected_community) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected level {} community {} for node {}, got {:?}",
                    fixture.name, check.name, level, expected_community, node, actual
                )));
            }
        }
    }
    if !check.expected_page_rank_scores.is_empty() {
        let scores = output
            .page_rank_scores
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>();
        for (node, expected_score) in &check.expected_page_rank_scores {
            let actual = scores.get(node).copied();
            let Some(actual_score) = actual else {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected PageRank score for node {}, got none",
                    fixture.name, check.name, node
                )));
            };
            if !float_matches(actual_score, *expected_score, check.tolerance.float_abs) {
                return Err(SkeinError::Execution(format!(
                    "fixture '{}' check '{}' expected PageRank score {} for node {}, got {}",
                    fixture.name, check.name, expected_score, node, actual_score
                )));
            }
        }
    }

    Ok(())
}

fn compare_projected_graph_shadow(
    fixture: &CompatibilityFixture,
    check: &ProjectedGraphFixtureCheck,
    shadow_engine: &str,
    primary: &ProjectedGraphShadowOutput,
    shadow: &ProjectedGraphShadowOutput,
) -> Result<()> {
    assert_projected_graph_matches_fixture(fixture, check, shadow).map_err(|error| {
        SkeinError::Execution(format!(
            "fixture '{}' check '{}' shadow engine '{}' projected graph validation failed: {error}",
            fixture.name, check.name, shadow_engine
        ))
    })?;
    if !projected_graph_outputs_match(primary, shadow, check.tolerance) {
        return Err(SkeinError::Execution(format!(
            "fixture '{}' check '{}' shadow engine '{}' projected graph mismatch: primary {:?}, shadow {:?}",
            fixture.name, check.name, shadow_engine, primary, shadow
        )));
    }
    Ok(())
}

impl ExpectedRows {
    fn assert_matches(
        &self,
        fixture_name: &str,
        check_name: &str,
        cypher: &str,
        output: &QueryOutput,
        tolerance: CompatibilityTolerance,
    ) -> Result<()> {
        match self {
            ExpectedRows::Exact(expected) => {
                let actual = output.rows.clone().into_rows();
                if !rows_match_ordered(expected, &actual, tolerance) {
                    return Err(row_mismatch_error(
                        fixture_name,
                        check_name,
                        cypher,
                        expected,
                        &actual,
                    ));
                }
            }
            ExpectedRows::Unordered(expected) => {
                let mut actual = output.rows.clone().into_rows();
                let expected_matches = rows_match_unordered(expected, &actual, tolerance);
                if !expected_matches {
                    let mut expected = expected.clone();
                    expected.sort();
                    actual.sort();
                    return Err(row_mismatch_error(
                        fixture_name,
                        check_name,
                        cypher,
                        &expected,
                        &actual,
                    ));
                }
            }
            ExpectedRows::RowCount(expected) => {
                if output.rows.len() != *expected {
                    return Err(SkeinError::Execution(format!(
                        "fixture '{}' check '{}' expected {} rows for '{}', got {}",
                        fixture_name,
                        check_name,
                        expected,
                        cypher,
                        output.rows.len()
                    )));
                }
            }
        }
        Ok(())
    }

    fn assert_shadow_matches_primary(
        &self,
        fixture_name: &str,
        check_name: &str,
        shadow_engine: &str,
        primary: &QueryOutput,
        shadow: &QueryOutput,
        tolerance: CompatibilityTolerance,
    ) -> Result<()> {
        match self {
            ExpectedRows::Exact(_) => {
                let primary_rows = primary.rows.clone().into_rows();
                let shadow_rows = shadow.rows.clone().into_rows();
                if !rows_match_ordered(&primary_rows, &shadow_rows, tolerance) {
                    return Err(shadow_mismatch_error(
                        fixture_name,
                        check_name,
                        shadow_engine,
                        &primary_rows,
                        &shadow_rows,
                    ));
                }
            }
            ExpectedRows::Unordered(_) => {
                let mut primary_rows = primary.rows.clone().into_rows();
                let mut shadow_rows = shadow.rows.clone().into_rows();
                if !rows_match_unordered(&primary_rows, &shadow_rows, tolerance) {
                    primary_rows.sort();
                    shadow_rows.sort();
                    return Err(shadow_mismatch_error(
                        fixture_name,
                        check_name,
                        shadow_engine,
                        &primary_rows,
                        &shadow_rows,
                    ));
                }
            }
            ExpectedRows::RowCount(_) => {
                if primary.rows.len() != shadow.rows.len() {
                    return Err(SkeinError::Execution(format!(
                        "fixture '{}' check '{}' shadow engine '{}' row count mismatch: primary {}, shadow {}",
                        fixture_name,
                        check_name,
                        shadow_engine,
                        primary.rows.len(),
                        shadow.rows.len()
                    )));
                }
            }
        }
        Ok(())
    }
}

fn row_mismatch_error(
    fixture_name: &str,
    check_name: &str,
    cypher: &str,
    expected: &[Row],
    actual: &[Row],
) -> SkeinError {
    SkeinError::Execution(format!(
        "fixture '{}' check '{}' row mismatch for '{}': expected {:?}, got {:?}",
        fixture_name, check_name, cypher, expected, actual
    ))
}

fn rows_match_ordered(expected: &[Row], actual: &[Row], tolerance: CompatibilityTolerance) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| rows_match(expected, actual, tolerance))
}

fn rows_match_unordered(
    expected: &[Row],
    actual: &[Row],
    tolerance: CompatibilityTolerance,
) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let mut used = vec![false; actual.len()];
    for expected_row in expected {
        let Some(index) = actual.iter().enumerate().position(|(index, actual_row)| {
            !used[index] && rows_match(expected_row, actual_row, tolerance)
        }) else {
            return false;
        };
        used[index] = true;
    }
    true
}

fn rows_match(expected: &Row, actual: &Row, tolerance: CompatibilityTolerance) -> bool {
    expected.len() == actual.len()
        && expected.iter().all(|(key, expected_value)| {
            actual
                .get(key)
                .is_some_and(|actual_value| values_match(expected_value, actual_value, tolerance))
        })
}

fn values_match(expected: &Value, actual: &Value, tolerance: CompatibilityTolerance) -> bool {
    match (expected, actual) {
        (Value::Float(expected), Value::Float(actual)) => {
            float_matches(*expected, *actual, tolerance.float_abs)
        }
        (Value::List(expected), Value::List(actual)) => {
            expected.len() == actual.len()
                && expected
                    .iter()
                    .zip(actual)
                    .all(|(expected, actual)| values_match(expected, actual, tolerance))
        }
        (Value::Map(expected), Value::Map(actual)) => {
            expected.len() == actual.len()
                && expected.iter().all(|(key, expected)| {
                    actual
                        .get(key)
                        .is_some_and(|actual| values_match(expected, actual, tolerance))
                })
        }
        _ => expected == actual,
    }
}

fn float_matches(left: f64, right: f64, tolerance: f64) -> bool {
    if left == right {
        return true;
    }
    left.is_finite() && right.is_finite() && (left - right).abs() <= tolerance
}

fn projected_graph_outputs_match(
    primary: &ProjectedGraphShadowOutput,
    shadow: &ProjectedGraphShadowOutput,
    tolerance: CompatibilityTolerance,
) -> bool {
    primary.node_count == shadow.node_count
        && primary.edge_count == shadow.edge_count
        && primary.incoming == shadow.incoming
        && primary.communities == shadow.communities
        && primary.hierarchical_communities == shadow.hierarchical_communities
        && primary.page_rank_top_node == shadow.page_rank_top_node
        && page_rank_scores_match(
            &primary.page_rank_scores,
            &shadow.page_rank_scores,
            tolerance,
        )
}

fn page_rank_scores_match(
    primary: &[(u64, f64)],
    shadow: &[(u64, f64)],
    tolerance: CompatibilityTolerance,
) -> bool {
    primary.len() == shadow.len()
        && primary.iter().zip(shadow).all(
            |((primary_node, primary_score), (shadow_node, shadow_score))| {
                primary_node == shadow_node
                    && float_matches(*primary_score, *shadow_score, tolerance.float_abs)
            },
        )
}

fn shadow_mismatch_error(
    fixture_name: &str,
    check_name: &str,
    shadow_engine: &str,
    primary: &[Row],
    shadow: &[Row],
) -> SkeinError {
    SkeinError::Execution(format!(
        "fixture '{}' check '{}' shadow engine '{}' row mismatch: primary {:?}, shadow {:?}",
        fixture_name, check_name, shadow_engine, primary, shadow
    ))
}

#[cfg(test)]
mod differential;
#[cfg(test)]
mod primary_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
