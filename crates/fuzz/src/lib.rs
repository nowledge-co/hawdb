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

use hawdb::api::{Database, DatabaseReadTransaction};
use hawdb::executor::Row;
use hawdb::optimizer::OptimizerSearchDirective;
use hawdb::{HawDBError, Value};
use serde_json::{json, Map as JsonMap, Value as JsonValue};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{Display, Formatter};

mod append_oracle;
mod coverage;
mod generator;
mod output;
mod parser_oracle;
mod predicate_rewrite;
mod query_ast;
mod row_page_oracle;
mod sql_oracle;
mod wal_tail_oracle;

use coverage::PlanCoverageTracker;
use generator::StateAwareCaseGenerator;
use predicate_rewrite::PREDICATE_REWRITE_SHAPES;
use query_ast::QueryAst;

pub use append_oracle::{run_append_state_machine_case, APPEND_STATE_MACHINE_PROTOCOL};
pub use coverage::PlanCoverageReport;
pub use hawdb_fuzz_contracts::{
    NowledgeQueryFuzzCaseReport, NowledgeQueryFuzzHarnessOptions, NowledgeQueryFuzzHarnessReport,
    NOWLEDGE_QUERY_FUZZ_HARNESS_PROTOCOL,
};
pub use output::{
    emit_fuzz_report, fuzz_current_report_path, read_fuzz_current_report,
    write_fuzz_current_report, FuzzReportPaths, DEFAULT_FUZZ_LOG_DIRECTORY,
    DEFAULT_FUZZ_PROGRESS_INTERVAL,
};
pub use parser_oracle::{
    generate_parser_fuzz_case, parser_fuzz_seed_count, parser_input_fingerprint,
    run_parser_fuzz_case, ParserFuzzCase, ParserFuzzObservation, PARSER_FUZZ_PROTOCOL,
};
pub use row_page_oracle::{run_row_page_compaction_case, ROW_PAGE_COMPACTION_PROTOCOL};
pub use sql_oracle::{
    SqlCaseReport, SqlExecutionObservation, SqlFailureReport, SqlJoinGeneratorProfile,
    SqlJoinRewriteCase, SqlJoinRewriteEvidence, SqlJoinRewriteFailureReport, SqlMutation,
    SqlPredicateRewriteCase, SqlPredicateRewriteEvidence, SqlPredicateRewriteFailureReport,
    SqlQueryInvocation, SqlReductionReport, SqlReplayBundle, SqlTlpCase, SqlTlpEvidence,
    SQL_JOIN_REWRITE_PROTOCOL, SQL_PREDICATE_REWRITE_PROTOCOL, SQL_REPLAY_PROTOCOL,
    SQL_TLP_AGGREGATE_PROTOCOL, SQL_TLP_PROTOCOL,
};
pub use wal_tail_oracle::{run_wal_tail_recovery_case, WAL_TAIL_RECOVERY_PROTOCOL};

pub fn compiled_capabilities_json() -> JsonValue {
    let capabilities = hawdb::compiled_runtime_capabilities();
    json!({
        "full_text_search": capabilities.is_enabled(hawdb::RuntimeCapability::FullTextSearch),
        "vector_search": capabilities.is_enabled(hawdb::RuntimeCapability::VectorSearch),
        "graph_analytics": capabilities.is_enabled(hawdb::RuntimeCapability::GraphAnalytics),
        "background_maintenance": capabilities
            .is_enabled(hawdb::RuntimeCapability::BackgroundMaintenance),
        "access_control": capabilities.is_enabled(hawdb::RuntimeCapability::AccessControl),
    })
}

#[cfg(test)]
mod capability_parity_tests {
    fn assert_parity(capability: hawdb::RuntimeCapability, forwarded: bool) {
        assert_eq!(
            hawdb::compiled_runtime_capabilities().is_enabled(capability),
            forwarded,
            "compiled capability {} diverged from the forwarded fuzz feature",
            capability.as_str(),
        );
    }

    #[test]
    fn forwarded_features_match_compiled_hawdb_capabilities() {
        assert_parity(
            hawdb::RuntimeCapability::FullTextSearch,
            cfg!(feature = "full-text-search"),
        );
        assert_parity(
            hawdb::RuntimeCapability::VectorSearch,
            cfg!(feature = "vector-search"),
        );
        assert_parity(
            hawdb::RuntimeCapability::GraphAnalytics,
            cfg!(feature = "graph-analytics"),
        );
        assert_parity(
            hawdb::RuntimeCapability::BackgroundMaintenance,
            cfg!(feature = "background-maintenance"),
        );
        assert_parity(
            hawdb::RuntimeCapability::AccessControl,
            cfg!(feature = "acl"),
        );
    }
}

pub const CAMPAIGN_PROTOCOL: &str = "hawdb-multi-oracle-fuzz-v1";
pub const GRAPH_PREDICATE_REWRITE_PROTOCOL: &str = "hawdb-graph-predicate-rewrite-fuzz-v1";
pub const GRAPH_TLP_AGGREGATE_PROTOCOL: &str = "hawdb-graph-tlp-aggregate-fuzz-v1";
pub const GRAPH_TLP_PROTOCOL: &str = "hawdb-graph-tlp-fuzz-v1";
pub const METAMORPHIC_PROTOCOL: &str = "hawdb-graph-metamorphic-fuzz-v1";
pub const PLAN_DIFFERENTIAL_PROTOCOL: &str = "hawdb-plan-differential-fuzz-v1";
pub const REPLAY_BUNDLE_PROTOCOL: &str = "hawdb-multi-oracle-replay-v1";
pub(crate) const QUERY_SHAPE_COUNT: usize = 12;
const DEFAULT_CASE_COUNT: usize = 128;
const MAX_CASE_COUNT: usize = 10_000;
const MAX_REDUCTION_ATTEMPTS: usize = 64;

pub type Parameters = BTreeMap<String, Value>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultSemantics {
    Ordered,
    Bag,
}

impl ResultSemantics {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Bag => "bag",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutation {
    pub cypher: String,
    pub parameters: Parameters,
}

impl Mutation {
    pub fn new(cypher: impl Into<String>) -> Self {
        Self {
            cypher: cypher.into(),
            parameters: Parameters::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzCase {
    pub seed: u64,
    pub shape: String,
    pub mutations: Vec<Mutation>,
    pub query: QueryInvocation,
    pub(crate) query_ast: QueryAst,
    pub graph_tlp: GraphTlpCase,
    pub graph_tlp_aggregate: GraphTlpCase,
    pub graph_predicate_rewrite: GraphPredicateRewriteCase,
    pub metamorphic: MetamorphicCase,
    pub index_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryInvocation {
    pub cypher: String,
    pub parameters: Parameters,
    pub result_semantics: ResultSemantics,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTlpCase {
    pub name: String,
    pub original: QueryInvocation,
    pub predicate_true: QueryInvocation,
    pub predicate_false: QueryInvocation,
    pub predicate_null: QueryInvocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPredicateRewriteCase {
    pub name: String,
    pub original: QueryInvocation,
    pub rewritten: QueryInvocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetamorphicCase {
    pub graph_isomorphism: MetamorphicRelation,
    pub direction_reversal: Option<MetamorphicRelation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetamorphicRelation {
    pub name: &'static str,
    pub applicability_guard: &'static str,
    pub mutations: Vec<Mutation>,
    pub query: QueryInvocation,
    pub identifier_prefix_to_strip: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityProfile {
    pub shapes: Vec<&'static str>,
    pub compares_duplicates: bool,
    pub compares_missing_and_null: bool,
    pub compares_float_bit_patterns: bool,
    pub compares_path_values: bool,
}

impl CapabilityProfile {
    pub fn plan_differential_v1() -> Self {
        Self {
            shapes: vec![
                "node_scan",
                "equality_filter",
                "in_filter",
                "range_filter",
                "one_hop_expand",
                "self_loop",
                "parallel_edges",
                "cartesian_product",
                "distinct_projection",
                "aggregate",
                "top_n",
                "missing_or_null",
            ],
            compares_duplicates: true,
            compares_missing_and_null: true,
            compares_float_bit_patterns: true,
            compares_path_values: false,
        }
    }

    pub fn graph_tlp_v1() -> Self {
        Self {
            shapes: vec!["nullable_node_property", "node_range", "relationship_range"],
            compares_duplicates: true,
            compares_missing_and_null: true,
            compares_float_bit_patterns: true,
            compares_path_values: false,
        }
    }

    pub fn graph_tlp_aggregate_v1() -> Self {
        Self {
            shapes: vec![
                "nullable_node_property_count",
                "node_range_count",
                "relationship_range_count",
            ],
            compares_duplicates: true,
            compares_missing_and_null: true,
            compares_float_bit_patterns: false,
            compares_path_values: false,
        }
    }

    pub fn graph_metamorphic_v1() -> Self {
        Self {
            shapes: vec!["graph_isomorphism", "direction_reversal"],
            compares_duplicates: true,
            compares_missing_and_null: true,
            compares_float_bit_patterns: true,
            compares_path_values: false,
        }
    }

    pub fn graph_predicate_rewrite_v1() -> Self {
        Self {
            shapes: PREDICATE_REWRITE_SHAPES.to_vec(),
            compares_duplicates: true,
            compares_missing_and_null: true,
            compares_float_bit_patterns: true,
            compares_path_values: false,
        }
    }
}

pub trait Oracle {
    type Output;

    fn capability_profile(&self) -> CapabilityProfile;
    fn evaluate(&self, case: &FuzzCase) -> Self::Output;
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum OracleResult {
    Equivalent(DifferentialEvidence),
    Failure(FailureReport),
}

impl OracleResult {
    pub const fn is_equivalent(&self) -> bool {
        matches!(self, Self::Equivalent(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DifferentialEvidence {
    pub memo: ExecutionObservation,
    pub direct_fallback: ExecutionObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureReport {
    pub signature: String,
    pub reason: String,
    pub replay: ReplayBundle,
    pub reduction: ReductionReport,
    pub memo: ExecutionObservation,
    pub direct_fallback: ExecutionObservation,
}

impl FailureReport {
    pub fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "reason": self.reason,
            "replay": self.replay.json(),
            "reduction": self.reduction.json(),
            "memo": self.memo.json(),
            "direct_fallback": self.direct_fallback.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum GraphTlpOracleResult {
    Equivalent(GraphTlpEvidence),
    Failure(GraphTlpFailureReport),
}

impl GraphTlpOracleResult {
    pub const fn is_equivalent(&self) -> bool {
        matches!(self, Self::Equivalent(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTlpEvidence {
    pub original: ExecutionObservation,
    pub predicate_true: ExecutionObservation,
    pub predicate_false: ExecutionObservation,
    pub predicate_null: ExecutionObservation,
}

impl GraphTlpEvidence {
    fn observations(&self) -> [(&'static str, &ExecutionObservation); 4] {
        [
            ("original", &self.original),
            ("predicate_true", &self.predicate_true),
            ("predicate_false", &self.predicate_false),
            ("predicate_null", &self.predicate_null),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphTlpFailureReport {
    pub signature: String,
    pub reason: String,
    pub replay: ReplayBundle,
    pub reduction: ReductionReport,
    pub evidence: GraphTlpEvidence,
}

pub type GraphTlpAggregateOracleResult = GraphTlpOracleResult;
pub type GraphTlpAggregateFailureReport = GraphTlpFailureReport;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum GraphPredicateRewriteOracleResult {
    Equivalent(GraphPredicateRewriteEvidence),
    Failure(GraphPredicateRewriteFailureReport),
}

impl GraphPredicateRewriteOracleResult {
    pub const fn is_equivalent(&self) -> bool {
        matches!(self, Self::Equivalent(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPredicateRewriteEvidence {
    pub original: ExecutionObservation,
    pub rewritten: ExecutionObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphPredicateRewriteFailureReport {
    pub signature: String,
    pub reason: String,
    pub replay: ReplayBundle,
    pub reduction: ReductionReport,
    pub evidence: GraphPredicateRewriteEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum MetamorphicOracleResult {
    Equivalent(MetamorphicEvidence),
    Failure(MetamorphicFailureReport),
}

impl MetamorphicOracleResult {
    pub const fn is_equivalent(&self) -> bool {
        matches!(self, Self::Equivalent(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetamorphicEvidence {
    pub original: ExecutionObservation,
    pub graph_isomorphism: ExecutionObservation,
    pub direction_reversal: Option<ExecutionObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetamorphicFailureReport {
    pub signature: &'static str,
    pub relation: &'static str,
    pub reason: String,
    pub replay: ReplayBundle,
    pub evidence: MetamorphicEvidence,
}

impl MetamorphicFailureReport {
    pub fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "relation": self.relation,
            "reason": self.reason,
            "replay": self.replay.json(),
            "original": self.evidence.original.json(),
            "graph_isomorphism": self.evidence.graph_isomorphism.json(),
            "direction_reversal": self.evidence.direction_reversal.as_ref().map(ExecutionObservation::json),
        })
    }
}

impl GraphTlpFailureReport {
    pub fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "reason": self.reason,
            "replay": self.replay.json(),
            "reduction": self.reduction.json(),
            "original": self.evidence.original.json(),
            "predicate_true": self.evidence.predicate_true.json(),
            "predicate_false": self.evidence.predicate_false.json(),
            "predicate_null": self.evidence.predicate_null.json(),
        })
    }
}

impl GraphPredicateRewriteFailureReport {
    pub fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "reason": self.reason,
            "replay": self.replay.json(),
            "reduction": self.reduction.json(),
            "original": self.evidence.original.json(),
            "rewritten": self.evidence.rewritten.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayBundle {
    pub seed: u64,
    pub shape: String,
    pub mutations: Vec<Mutation>,
    pub query: QueryInvocation,
    pub(crate) query_ast: QueryAst,
    pub graph_tlp: GraphTlpCase,
    pub graph_tlp_aggregate: GraphTlpCase,
    pub graph_predicate_rewrite: GraphPredicateRewriteCase,
    pub metamorphic: MetamorphicCase,
    pub index_enabled: bool,
}

impl ReplayBundle {
    pub fn from_case(case: &FuzzCase) -> Self {
        Self {
            seed: case.seed,
            shape: case.shape.clone(),
            mutations: case.mutations.clone(),
            query: case.query.clone(),
            query_ast: case.query_ast.clone(),
            graph_tlp: case.graph_tlp.clone(),
            graph_tlp_aggregate: case.graph_tlp_aggregate.clone(),
            graph_predicate_rewrite: case.graph_predicate_rewrite.clone(),
            metamorphic: case.metamorphic.clone(),
            index_enabled: case.index_enabled,
        }
    }

    pub fn json(&self) -> JsonValue {
        json!({
            "protocol": REPLAY_BUNDLE_PROTOCOL,
            "seed": self.seed,
            "shape": self.shape,
            "index_enabled": self.index_enabled,
            "optimizer_search_variants": ["memo", "direct_fallback"],
            "mutations": self.mutations.iter().map(mutation_json).collect::<Vec<_>>(),
            "query": query_invocation_json(&self.query),
            "query_ast": self.query_ast.json(),
            "graph_tlp": graph_tlp_case_json(&self.graph_tlp),
            "graph_tlp_aggregate": graph_tlp_case_json(&self.graph_tlp_aggregate),
            "graph_predicate_rewrite": graph_predicate_rewrite_case_json(&self.graph_predicate_rewrite),
            "metamorphic": metamorphic_case_json(&self.metamorphic),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReductionReport {
    pub oracle: &'static str,
    pub original_mutation_count: usize,
    pub reduced_mutation_count: usize,
    pub original_query_node_count: usize,
    pub reduced_query_node_count: usize,
    pub attempts: usize,
    pub replay: ReplayBundle,
}

impl ReductionReport {
    fn json(&self) -> JsonValue {
        json!({
            "oracle": self.oracle,
            "original_mutation_count": self.original_mutation_count,
            "reduced_mutation_count": self.reduced_mutation_count,
            "original_query_node_count": self.original_query_node_count,
            "reduced_query_node_count": self.reduced_query_node_count,
            "attempts": self.attempts,
            "replay": self.replay.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionObservation {
    pub snapshot_epoch: Option<u64>,
    pub optimizer_search: Option<String>,
    pub search_mode: Option<String>,
    pub plan_fingerprint: Option<String>,
    pub optimizer_stages: Vec<String>,
    pub outcome: ExecutionOutcome,
}

impl ExecutionObservation {
    fn json(&self) -> JsonValue {
        json!({
            "snapshot_epoch": self.snapshot_epoch,
            "optimizer_search": self.optimizer_search,
            "search_mode": self.search_mode,
            "plan_fingerprint": self.plan_fingerprint,
            "optimizer_stages": self.optimizer_stages,
            "outcome": self.outcome.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionOutcome {
    Rows(Vec<Row>),
    Error {
        phase: &'static str,
        class: &'static str,
        message: String,
    },
}

impl ExecutionOutcome {
    fn json(&self) -> JsonValue {
        match self {
            Self::Rows(rows) => json!({
                "status": "rows",
                "row_count": rows.len(),
                "rows": rows.iter().map(row_json).collect::<Vec<_>>(),
            }),
            Self::Error {
                phase,
                class,
                message,
            } => json!({
                "status": "error",
                "phase": phase,
                "class": class,
                "message": message,
            }),
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct PlanDifferentialOracle;

impl Oracle for PlanDifferentialOracle {
    type Output = OracleResult;

    fn capability_profile(&self) -> CapabilityProfile {
        CapabilityProfile::plan_differential_v1()
    }

    fn evaluate(&self, case: &FuzzCase) -> OracleResult {
        let (memo, direct_fallback) = execute_case(case);
        let failure = classify_plan_failure(
            &memo.outcome,
            &direct_fallback.outcome,
            case.query.result_semantics,
        )
        .or_else(|| classify_search_mode_failure(&memo, &direct_fallback));

        if let Some(failure) = failure {
            OracleResult::Failure(FailureReport {
                signature: failure.signature.code(),
                reason: failure.reason,
                replay: ReplayBundle::from_case(case),
                reduction: reduce_failure(case, OracleKind::PlanDifferential, &failure.signature),
                memo,
                direct_fallback,
            })
        } else {
            OracleResult::Equivalent(DifferentialEvidence {
                memo,
                direct_fallback,
            })
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GraphTlpOracle;

impl Oracle for GraphTlpOracle {
    type Output = GraphTlpOracleResult;

    fn capability_profile(&self) -> CapabilityProfile {
        CapabilityProfile::graph_tlp_v1()
    }

    fn evaluate(&self, case: &FuzzCase) -> GraphTlpOracleResult {
        let evidence = execute_graph_tlp_case(case);
        let failure = classify_graph_tlp_failure(&evidence);

        if let Some(failure) = failure {
            GraphTlpOracleResult::Failure(GraphTlpFailureReport {
                signature: failure.signature.code(),
                reason: failure.reason,
                replay: ReplayBundle::from_case(case),
                reduction: reduce_failure(case, OracleKind::GraphTlp, &failure.signature),
                evidence,
            })
        } else {
            GraphTlpOracleResult::Equivalent(evidence)
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GraphTlpAggregateOracle;

impl Oracle for GraphTlpAggregateOracle {
    type Output = GraphTlpAggregateOracleResult;

    fn capability_profile(&self) -> CapabilityProfile {
        CapabilityProfile::graph_tlp_aggregate_v1()
    }

    fn evaluate(&self, case: &FuzzCase) -> GraphTlpAggregateOracleResult {
        let evidence = execute_graph_tlp_queries(case, &case.graph_tlp_aggregate);
        let failure = classify_graph_tlp_aggregate_failure(&evidence);

        if let Some(failure) = failure {
            GraphTlpOracleResult::Failure(GraphTlpFailureReport {
                signature: failure.signature.code(),
                reason: failure.reason,
                replay: ReplayBundle::from_case(case),
                reduction: reduce_failure(case, OracleKind::GraphTlpAggregate, &failure.signature),
                evidence,
            })
        } else {
            GraphTlpOracleResult::Equivalent(evidence)
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GraphPredicateRewriteOracle;

impl Oracle for GraphPredicateRewriteOracle {
    type Output = GraphPredicateRewriteOracleResult;

    fn capability_profile(&self) -> CapabilityProfile {
        CapabilityProfile::graph_predicate_rewrite_v1()
    }

    fn evaluate(&self, case: &FuzzCase) -> GraphPredicateRewriteOracleResult {
        let evidence = execute_graph_predicate_rewrite_case(case);
        let failure = classify_graph_predicate_rewrite_failure(&evidence);

        if let Some(failure) = failure {
            GraphPredicateRewriteOracleResult::Failure(GraphPredicateRewriteFailureReport {
                signature: failure.signature.code(),
                reason: failure.reason,
                replay: ReplayBundle::from_case(case),
                reduction: reduce_failure(
                    case,
                    OracleKind::GraphPredicateRewrite,
                    &failure.signature,
                ),
                evidence,
            })
        } else {
            GraphPredicateRewriteOracleResult::Equivalent(evidence)
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GraphMetamorphicOracle;

impl Oracle for GraphMetamorphicOracle {
    type Output = MetamorphicOracleResult;

    fn capability_profile(&self) -> CapabilityProfile {
        CapabilityProfile::graph_metamorphic_v1()
    }

    fn evaluate(&self, case: &FuzzCase) -> MetamorphicOracleResult {
        let original = execute_mutations_query(&case.mutations, &case.query);
        let graph_isomorphism = normalized_observation(
            execute_mutations_query(
                &case.metamorphic.graph_isomorphism.mutations,
                &case.metamorphic.graph_isomorphism.query,
            ),
            case.metamorphic
                .graph_isomorphism
                .identifier_prefix_to_strip
                .as_deref(),
        );
        let direction_reversal = case
            .metamorphic
            .direction_reversal
            .as_ref()
            .map(|relation| execute_mutations_query(&relation.mutations, &relation.query));
        let evidence = MetamorphicEvidence {
            original,
            graph_isomorphism,
            direction_reversal,
        };
        if let Some((signature, relation, reason)) =
            classify_metamorphic_failure(&evidence, case.query.result_semantics)
        {
            MetamorphicOracleResult::Failure(MetamorphicFailureReport {
                signature,
                relation,
                reason,
                replay: ReplayBundle::from_case(case),
                evidence,
            })
        } else {
            MetamorphicOracleResult::Equivalent(evidence)
        }
    }
}

fn execute_mutations_query(
    mutations: &[Mutation],
    query: &QueryInvocation,
) -> ExecutionObservation {
    let mut db = Database::new();
    for mutation in mutations {
        if let Err(error) = db.query_with_params(&mutation.cypher, &mutation.parameters) {
            return error_observation("mutation", error);
        }
    }
    let snapshot_epoch = db.commit_epoch();
    execute_snapshot_case(
        &mut db.begin_read_transaction(),
        snapshot_epoch,
        query,
        OptimizerSearchDirective::Memo,
    )
}

fn normalized_observation(
    mut observation: ExecutionObservation,
    identifier_prefix_to_strip: Option<&str>,
) -> ExecutionObservation {
    let Some(prefix) = identifier_prefix_to_strip else {
        return observation;
    };
    if let ExecutionOutcome::Rows(rows) = &mut observation.outcome {
        for row in rows {
            for value in row.values_mut() {
                normalize_identifier_value(value, prefix);
            }
        }
    }
    observation
}

fn normalize_identifier_value(value: &mut Value, prefix: &str) {
    match value {
        Value::String(identifier)
            if identifier
                .strip_prefix(prefix)
                .is_some_and(|id| id.starts_with("mem-") || id.starts_with("entity-")) =>
        {
            identifier.drain(..prefix.len());
        }
        Value::List(values) => {
            for value in values {
                normalize_identifier_value(value, prefix);
            }
        }
        Value::Map(values) => {
            for value in values.values_mut() {
                normalize_identifier_value(value, prefix);
            }
        }
        _ => {}
    }
}

fn classify_metamorphic_failure(
    evidence: &MetamorphicEvidence,
    semantics: ResultSemantics,
) -> Option<(&'static str, &'static str, String)> {
    if let ExecutionOutcome::Error { phase, class, .. } = &evidence.original.outcome {
        return Some((
            "metamorphic_original_errored",
            "original",
            format!("metamorphic original query failed in {phase}/{class}"),
        ));
    }
    if let ExecutionOutcome::Error { phase, class, .. } = &evidence.graph_isomorphism.outcome {
        return Some((
            "graph_isomorphism_transformed_errored",
            "graph_isomorphism",
            format!("graph-isomorphism transformed query failed in {phase}/{class}"),
        ));
    }
    let ExecutionOutcome::Rows(original_rows) = &evidence.original.outcome else {
        unreachable!("original error was classified above")
    };
    let ExecutionOutcome::Rows(isomorphic_rows) = &evidence.graph_isomorphism.outcome else {
        unreachable!("isomorphism error was classified above")
    };
    if let Err(reason) = compare_rows(original_rows, isomorphic_rows, semantics) {
        return Some((
            "graph_isomorphism_result_mismatch",
            "graph_isomorphism",
            format!("graph-isomorphism result mismatch: {reason}"),
        ));
    }
    if let Some(direction_reversal) = &evidence.direction_reversal {
        if let ExecutionOutcome::Error { phase, class, .. } = &direction_reversal.outcome {
            return Some((
                "direction_reversal_transformed_errored",
                "direction_reversal",
                format!("direction-reversal transformed query failed in {phase}/{class}"),
            ));
        }
        let ExecutionOutcome::Rows(reversed_rows) = &direction_reversal.outcome else {
            unreachable!("direction-reversal error was classified above")
        };
        if let Err(reason) = compare_rows(original_rows, reversed_rows, semantics) {
            return Some((
                "direction_reversal_result_mismatch",
                "direction_reversal",
                format!("direction-reversal result mismatch: {reason}"),
            ));
        }
    }
    None
}

fn execute_case(case: &FuzzCase) -> (ExecutionObservation, ExecutionObservation) {
    let (mut snapshot, snapshot_epoch) = match prepare_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => {
            let observation = *observation;
            return (observation.clone(), observation);
        }
    };
    let memo = execute_snapshot_case(
        &mut snapshot,
        snapshot_epoch,
        &case.query,
        OptimizerSearchDirective::Memo,
    );
    let direct_fallback = execute_snapshot_case(
        &mut snapshot,
        snapshot_epoch,
        &case.query,
        OptimizerSearchDirective::DirectFallback,
    );
    (memo, direct_fallback)
}

fn prepare_case(
    case: &FuzzCase,
) -> Result<(DatabaseReadTransaction, u64), Box<ExecutionObservation>> {
    let mut db = Database::new();

    for mutation in &case.mutations {
        if let Err(error) = db.query_with_params(&mutation.cypher, &mutation.parameters) {
            return Err(Box::new(error_observation("mutation", error)));
        }
    }

    let snapshot_epoch = db.commit_epoch();
    Ok((db.begin_read_transaction(), snapshot_epoch))
}

fn execute_snapshot_case(
    snapshot: &mut DatabaseReadTransaction,
    snapshot_epoch: u64,
    query: &QueryInvocation,
    optimizer_search: OptimizerSearchDirective,
) -> ExecutionObservation {
    let snapshot_epoch = Some(snapshot_epoch);
    let optimizer_search_name = optimizer_search.as_str();
    let hinted_cypher = format!(
        "CYPHER system.optimizer_search = '{optimizer_search_name}' {}",
        query.cypher
    );

    let explain = match snapshot.explain_query_with_params(&hinted_cypher, &query.parameters) {
        Ok(explain) => explain,
        Err(error) => {
            return snapshot_error_observation(
                snapshot_epoch,
                Some(optimizer_search_name.to_string()),
                "explain",
                error,
            );
        }
    };
    let search_mode = Some(explain.trace.search_mode.as_str().to_string());
    let plan_fingerprint = Some(explain.trace.selected_plan_fingerprint.clone());
    let optimizer_stages = explain
        .trace
        .stage_events
        .iter()
        .map(|stage| {
            let stats = stage.stats();
            format!(
                "{}:{}:{}:{}:{}:{}",
                stage.name(),
                stage.apply_order().as_str(),
                stats.input_count,
                stats.output_count,
                stats.applied_rules,
                stats.skipped_rules,
            )
        })
        .collect();

    match snapshot.query_with_params(&hinted_cypher, &query.parameters) {
        Ok(output) => ExecutionObservation {
            snapshot_epoch,
            optimizer_search: Some(optimizer_search_name.to_string()),
            search_mode,
            plan_fingerprint,
            optimizer_stages,
            outcome: ExecutionOutcome::Rows(output.rows.into_rows()),
        },
        Err(error) => ExecutionObservation {
            snapshot_epoch,
            optimizer_search: Some(optimizer_search_name.to_string()),
            search_mode,
            plan_fingerprint,
            optimizer_stages,
            outcome: error_outcome("execute", error),
        },
    }
}

fn execute_graph_tlp_case(case: &FuzzCase) -> GraphTlpEvidence {
    execute_graph_tlp_queries(case, &case.graph_tlp)
}

fn execute_graph_tlp_queries(case: &FuzzCase, queries: &GraphTlpCase) -> GraphTlpEvidence {
    let (mut snapshot, snapshot_epoch) = match prepare_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => {
            let observation = *observation;
            return GraphTlpEvidence {
                original: observation.clone(),
                predicate_true: observation.clone(),
                predicate_false: observation.clone(),
                predicate_null: observation,
            };
        }
    };
    let mut execute = |query: &QueryInvocation| {
        execute_snapshot_case(
            &mut snapshot,
            snapshot_epoch,
            query,
            OptimizerSearchDirective::Memo,
        )
    };

    GraphTlpEvidence {
        original: execute(&queries.original),
        predicate_true: execute(&queries.predicate_true),
        predicate_false: execute(&queries.predicate_false),
        predicate_null: execute(&queries.predicate_null),
    }
}

fn execute_graph_predicate_rewrite_case(case: &FuzzCase) -> GraphPredicateRewriteEvidence {
    let (mut snapshot, snapshot_epoch) = match prepare_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => {
            let observation = *observation;
            return GraphPredicateRewriteEvidence {
                original: observation.clone(),
                rewritten: observation,
            };
        }
    };
    let mut execute = |query: &QueryInvocation| {
        execute_snapshot_case(
            &mut snapshot,
            snapshot_epoch,
            query,
            OptimizerSearchDirective::Memo,
        )
    };

    GraphPredicateRewriteEvidence {
        original: execute(&case.graph_predicate_rewrite.original),
        rewritten: execute(&case.graph_predicate_rewrite.rewritten),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DetectedFailure {
    signature: FailureSignature,
    reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FailureSignature {
    PlanResultMismatch,
    PlanBothErrored {
        memo_phase: &'static str,
        memo_class: &'static str,
        direct_phase: &'static str,
        direct_class: &'static str,
    },
    PlanMemoErrored {
        phase: &'static str,
        class: &'static str,
    },
    PlanDirectFallbackErrored {
        phase: &'static str,
        class: &'static str,
    },
    PlanSnapshotMismatch,
    PlanDirectiveMismatch {
        path: &'static str,
    },
    PlanSearchModeMismatch {
        path: &'static str,
    },
    GraphTlpErrored {
        variant: &'static str,
        phase: &'static str,
        class: &'static str,
    },
    GraphTlpSnapshotMismatch,
    GraphTlpPartitionMismatch,
    GraphTlpAggregateErrored {
        variant: &'static str,
        phase: &'static str,
        class: &'static str,
    },
    GraphTlpAggregateSnapshotMismatch,
    GraphTlpAggregateInvalidResult {
        variant: &'static str,
    },
    GraphTlpAggregateOverflow,
    GraphTlpAggregateMismatch,
    GraphPredicateRewriteErrored {
        variant: &'static str,
        phase: &'static str,
        class: &'static str,
    },
    GraphPredicateRewriteSnapshotMismatch,
    GraphPredicateRewriteMismatch,
}

impl FailureSignature {
    fn code(&self) -> String {
        match self {
            Self::PlanResultMismatch => "plan_differential_result_mismatch".to_string(),
            Self::PlanBothErrored {
                memo_phase,
                memo_class,
                direct_phase,
                direct_class,
            } => format!(
                "plan_differential_both_errored_memo_{memo_phase}_{memo_class}_direct_{direct_phase}_{direct_class}"
            ),
            Self::PlanMemoErrored { phase, class } => {
                format!("plan_differential_memo_{phase}_{class}")
            }
            Self::PlanDirectFallbackErrored { phase, class } => {
                format!("plan_differential_direct_fallback_{phase}_{class}")
            }
            Self::PlanSnapshotMismatch => "plan_differential_snapshot_mismatch".to_string(),
            Self::PlanDirectiveMismatch { path } => {
                format!("plan_differential_{path}_directive_mismatch")
            }
            Self::PlanSearchModeMismatch { path } => {
                format!("plan_differential_{path}_search_mode_mismatch")
            }
            Self::GraphTlpErrored {
                variant,
                phase,
                class,
            } => format!("graph_tlp_{variant}_{phase}_{class}"),
            Self::GraphTlpSnapshotMismatch => "graph_tlp_snapshot_mismatch".to_string(),
            Self::GraphTlpPartitionMismatch => "graph_tlp_partition_mismatch".to_string(),
            Self::GraphTlpAggregateErrored {
                variant,
                phase,
                class,
            } => format!("graph_tlp_aggregate_{variant}_{phase}_{class}"),
            Self::GraphTlpAggregateSnapshotMismatch => {
                "graph_tlp_aggregate_snapshot_mismatch".to_string()
            }
            Self::GraphTlpAggregateInvalidResult { variant } => {
                format!("graph_tlp_aggregate_{variant}_invalid_result")
            }
            Self::GraphTlpAggregateOverflow => "graph_tlp_aggregate_overflow".to_string(),
            Self::GraphTlpAggregateMismatch => "graph_tlp_aggregate_mismatch".to_string(),
            Self::GraphPredicateRewriteErrored {
                variant,
                phase,
                class,
            } => format!("graph_predicate_rewrite_{variant}_{phase}_{class}"),
            Self::GraphPredicateRewriteSnapshotMismatch => {
                "graph_predicate_rewrite_snapshot_mismatch".to_string()
            }
            Self::GraphPredicateRewriteMismatch => {
                "graph_predicate_rewrite_mismatch".to_string()
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OracleKind {
    PlanDifferential,
    GraphTlp,
    GraphTlpAggregate,
    GraphPredicateRewrite,
}

impl OracleKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PlanDifferential => "plan_differential",
            Self::GraphTlp => "graph_tlp",
            Self::GraphTlpAggregate => "graph_tlp_aggregate",
            Self::GraphPredicateRewrite => "graph_predicate_rewrite",
        }
    }
}

fn classify_graph_predicate_rewrite_failure(
    evidence: &GraphPredicateRewriteEvidence,
) -> Option<DetectedFailure> {
    for (variant, observation) in [
        ("original", &evidence.original),
        ("rewritten", &evidence.rewritten),
    ] {
        if let ExecutionOutcome::Error { phase, class, .. } = &observation.outcome {
            let phase = *phase;
            let class = *class;
            return Some(DetectedFailure {
                signature: FailureSignature::GraphPredicateRewriteErrored {
                    variant,
                    phase,
                    class,
                },
                reason: format!("graph predicate rewrite {variant} failed in {phase}/{class}"),
            });
        }
    }

    if evidence.original.snapshot_epoch.is_none()
        || evidence.original.snapshot_epoch != evidence.rewritten.snapshot_epoch
    {
        return Some(DetectedFailure {
            signature: FailureSignature::GraphPredicateRewriteSnapshotMismatch,
            reason: "graph predicate rewrite variants did not execute on one pinned snapshot"
                .to_string(),
        });
    }

    let ExecutionOutcome::Rows(original) = &evidence.original.outcome else {
        unreachable!("graph predicate rewrite errors were classified above")
    };
    let ExecutionOutcome::Rows(rewritten) = &evidence.rewritten.outcome else {
        unreachable!("graph predicate rewrite errors were classified above")
    };
    compare_rows(original, rewritten, ResultSemantics::Bag)
        .err()
        .map(|reason| DetectedFailure {
            signature: FailureSignature::GraphPredicateRewriteMismatch,
            reason: format!("graph predicate rewrite mismatch: {reason}"),
        })
}

fn classify_graph_tlp_failure(evidence: &GraphTlpEvidence) -> Option<DetectedFailure> {
    let observations = evidence.observations();
    for (variant, observation) in observations {
        if let ExecutionOutcome::Error { phase, class, .. } = &observation.outcome {
            let phase = *phase;
            let class = *class;
            return Some(DetectedFailure {
                signature: FailureSignature::GraphTlpErrored {
                    variant,
                    phase,
                    class,
                },
                reason: format!("graph TLP {variant} failed in {phase}/{class}"),
            });
        }
    }

    let snapshot_epoch = evidence.original.snapshot_epoch;
    if snapshot_epoch.is_none()
        || observations
            .iter()
            .any(|(_, observation)| observation.snapshot_epoch != snapshot_epoch)
    {
        return Some(DetectedFailure {
            signature: FailureSignature::GraphTlpSnapshotMismatch,
            reason: "graph TLP variants did not execute on one pinned snapshot".to_string(),
        });
    }

    let ExecutionOutcome::Rows(original) = &evidence.original.outcome else {
        unreachable!("error outcomes were classified above")
    };
    let mut partitioned = Vec::new();
    for (_, observation) in observations.into_iter().skip(1) {
        let ExecutionOutcome::Rows(rows) = &observation.outcome else {
            unreachable!("error outcomes were classified above")
        };
        partitioned.extend_from_slice(rows);
    }

    compare_rows(original, &partitioned, ResultSemantics::Bag)
        .err()
        .map(|reason| DetectedFailure {
            signature: FailureSignature::GraphTlpPartitionMismatch,
            reason: format!("graph TLP partition mismatch: {reason}"),
        })
}

fn classify_graph_tlp_aggregate_failure(evidence: &GraphTlpEvidence) -> Option<DetectedFailure> {
    let observations = evidence.observations();
    for (variant, observation) in observations {
        if let ExecutionOutcome::Error { phase, class, .. } = &observation.outcome {
            let phase = *phase;
            let class = *class;
            return Some(DetectedFailure {
                signature: FailureSignature::GraphTlpAggregateErrored {
                    variant,
                    phase,
                    class,
                },
                reason: format!("graph TLP aggregate {variant} failed in {phase}/{class}"),
            });
        }
    }

    let snapshot_epoch = evidence.original.snapshot_epoch;
    if snapshot_epoch.is_none()
        || observations
            .iter()
            .any(|(_, observation)| observation.snapshot_epoch != snapshot_epoch)
    {
        return Some(DetectedFailure {
            signature: FailureSignature::GraphTlpAggregateSnapshotMismatch,
            reason: "graph TLP aggregate variants did not execute on one pinned snapshot"
                .to_string(),
        });
    }

    let mut counts = [0_u64; 4];
    for (index, (variant, observation)) in observations.into_iter().enumerate() {
        let Some(count) = observation_count(observation) else {
            return Some(DetectedFailure {
                signature: FailureSignature::GraphTlpAggregateInvalidResult { variant },
                reason: format!(
                    "graph TLP aggregate {variant} must return one non-negative integer count"
                ),
            });
        };
        counts[index] = count;
    }

    let Some(partition_count) = counts[1]
        .checked_add(counts[2])
        .and_then(|count| count.checked_add(counts[3]))
    else {
        return Some(DetectedFailure {
            signature: FailureSignature::GraphTlpAggregateOverflow,
            reason: "graph TLP aggregate partition count overflowed u64".to_string(),
        });
    };

    (counts[0] != partition_count).then(|| DetectedFailure {
        signature: FailureSignature::GraphTlpAggregateMismatch,
        reason: format!(
            "graph TLP aggregate mismatch: original_count={} partition_count={partition_count}",
            counts[0]
        ),
    })
}

fn observation_count(observation: &ExecutionObservation) -> Option<u64> {
    let ExecutionOutcome::Rows(rows) = &observation.outcome else {
        return None;
    };
    let [row] = rows.as_slice() else {
        return None;
    };
    if row.len() != 1 {
        return None;
    }
    let Value::Int(count) = row.get("count")? else {
        return None;
    };
    u64::try_from(*count).ok()
}

fn error_observation(phase: &'static str, error: HawDBError) -> ExecutionObservation {
    snapshot_error_observation(None, None, phase, error)
}

fn snapshot_error_observation(
    snapshot_epoch: Option<u64>,
    optimizer_search: Option<String>,
    phase: &'static str,
    error: HawDBError,
) -> ExecutionObservation {
    ExecutionObservation {
        snapshot_epoch,
        optimizer_search,
        search_mode: None,
        plan_fingerprint: None,
        optimizer_stages: Vec::new(),
        outcome: error_outcome(phase, error),
    }
}

fn error_outcome(phase: &'static str, error: HawDBError) -> ExecutionOutcome {
    ExecutionOutcome::Error {
        phase,
        class: error_class(&error),
        message: error.to_string(),
    }
}

fn classify_search_mode_failure(
    memo: &ExecutionObservation,
    direct_fallback: &ExecutionObservation,
) -> Option<DetectedFailure> {
    if memo.snapshot_epoch.is_none() || memo.snapshot_epoch != direct_fallback.snapshot_epoch {
        return Some(DetectedFailure {
            signature: FailureSignature::PlanSnapshotMismatch,
            reason: format!(
                "optimizer paths did not execute on the same pinned snapshot: memo={:?} direct_fallback={:?}",
                memo.snapshot_epoch, direct_fallback.snapshot_epoch
            ),
        });
    }
    if memo.optimizer_search.as_deref() != Some("memo") {
        return Some(DetectedFailure {
            signature: FailureSignature::PlanDirectiveMismatch { path: "memo" },
            reason: format!(
                "memo execution recorded unexpected optimizer hint {:?}",
                memo.optimizer_search
            ),
        });
    }
    if direct_fallback.optimizer_search.as_deref() != Some("direct_fallback") {
        return Some(DetectedFailure {
            signature: FailureSignature::PlanDirectiveMismatch {
                path: "direct_fallback",
            },
            reason: format!(
                "direct fallback execution recorded unexpected optimizer hint {:?}",
                direct_fallback.optimizer_search
            ),
        });
    }
    if memo.search_mode.as_deref() != Some("memo") {
        return Some(DetectedFailure {
            signature: FailureSignature::PlanSearchModeMismatch { path: "memo" },
            reason: format!(
                "memo configuration selected unexpected search mode {:?}",
                memo.search_mode
            ),
        });
    }
    if direct_fallback.search_mode.as_deref() != Some("direct_fallback") {
        return Some(DetectedFailure {
            signature: FailureSignature::PlanSearchModeMismatch {
                path: "direct_fallback",
            },
            reason: format!(
                "direct fallback configuration selected unexpected search mode {:?}",
                direct_fallback.search_mode
            ),
        });
    }
    None
}

fn classify_plan_failure(
    memo: &ExecutionOutcome,
    direct_fallback: &ExecutionOutcome,
    semantics: ResultSemantics,
) -> Option<DetectedFailure> {
    match (memo, direct_fallback) {
        (ExecutionOutcome::Rows(memo), ExecutionOutcome::Rows(direct_fallback)) => {
            compare_rows(memo, direct_fallback, semantics)
                .err()
                .map(|reason| DetectedFailure {
                    signature: FailureSignature::PlanResultMismatch,
                    reason: format!("memo/direct_fallback result mismatch: {reason}"),
                })
        }
        (
            ExecutionOutcome::Error {
                phase: memo_phase,
                class: memo_class,
                ..
            },
            ExecutionOutcome::Error {
                phase: direct_phase,
                class: direct_class,
                ..
            },
        ) => Some(DetectedFailure {
            signature: FailureSignature::PlanBothErrored {
                memo_phase,
                memo_class,
                direct_phase,
                direct_class,
            },
            reason: format!(
                "generated case failed in both oracle paths: memo={memo_phase}/{memo_class} direct_fallback={direct_phase}/{direct_class}"
            ),
        }),
        (ExecutionOutcome::Error { phase, class, .. }, ExecutionOutcome::Rows(_)) => {
            Some(DetectedFailure {
                signature: FailureSignature::PlanMemoErrored { phase, class },
                reason: "memo failed while direct_fallback returned rows".to_string(),
            })
        }
        (ExecutionOutcome::Rows(_), ExecutionOutcome::Error { phase, class, .. }) => {
            Some(DetectedFailure {
                signature: FailureSignature::PlanDirectFallbackErrored { phase, class },
                reason: "direct_fallback failed while memo returned rows".to_string(),
            })
        }
    }
}

fn reduce_failure(
    case: &FuzzCase,
    oracle: OracleKind,
    expected: &FailureSignature,
) -> ReductionReport {
    let original_mutation_count = case.mutations.len();
    let original_query_node_count = case.query_ast.node_count();
    let mut reduced = case.clone();
    let mut attempts = 0;
    let mut granularity = 2;

    while !reduced.mutations.is_empty() && attempts < MAX_REDUCTION_ATTEMPTS {
        let mutation_count = reduced.mutations.len();
        let chunk_size = mutation_count.div_ceil(granularity);
        let mut start = 0;
        let mut removed_chunk = false;

        while start < mutation_count && attempts < MAX_REDUCTION_ATTEMPTS {
            let end = (start + chunk_size).min(mutation_count);
            let mut candidate = reduced.clone();
            candidate.mutations.drain(start..end);
            candidate.index_enabled = has_index_mutation(&candidate.mutations);
            attempts += 1;

            if failure_signature(&candidate, oracle).as_ref() == Some(expected) {
                reduced = candidate;
                granularity = granularity.saturating_sub(1).max(2);
                removed_chunk = true;
                break;
            }
            start = end;
        }

        if !removed_chunk {
            if granularity >= mutation_count {
                break;
            }
            granularity = (granularity * 2).min(mutation_count);
        }
    }

    if oracle == OracleKind::PlanDifferential {
        loop {
            let mut accepted = None;
            for query_ast in reduced.query_ast.reduction_candidates() {
                if attempts >= MAX_REDUCTION_ATTEMPTS {
                    break;
                }
                let mut candidate = reduced.clone();
                candidate.query = query_ast.invocation();
                candidate.query_ast = query_ast;
                attempts += 1;
                if failure_signature(&candidate, oracle).as_ref() == Some(expected) {
                    accepted = Some(candidate);
                    break;
                }
            }
            let Some(candidate) = accepted else {
                break;
            };
            reduced = candidate;
        }
    }

    ReductionReport {
        oracle: oracle.as_str(),
        original_mutation_count,
        reduced_mutation_count: reduced.mutations.len(),
        original_query_node_count,
        reduced_query_node_count: reduced.query_ast.node_count(),
        attempts,
        replay: ReplayBundle::from_case(&reduced),
    }
}

fn failure_signature(case: &FuzzCase, oracle: OracleKind) -> Option<FailureSignature> {
    match oracle {
        OracleKind::PlanDifferential => {
            let (memo, direct_fallback) = execute_case(case);
            classify_plan_failure(
                &memo.outcome,
                &direct_fallback.outcome,
                case.query.result_semantics,
            )
            .or_else(|| classify_search_mode_failure(&memo, &direct_fallback))
            .map(|failure| failure.signature)
        }
        OracleKind::GraphTlp => classify_graph_tlp_failure(&execute_graph_tlp_case(case))
            .map(|failure| failure.signature),
        OracleKind::GraphTlpAggregate => classify_graph_tlp_aggregate_failure(
            &execute_graph_tlp_queries(case, &case.graph_tlp_aggregate),
        )
        .map(|failure| failure.signature),
        OracleKind::GraphPredicateRewrite => {
            classify_graph_predicate_rewrite_failure(&execute_graph_predicate_rewrite_case(case))
                .map(|failure| failure.signature)
        }
    }
}

fn has_index_mutation(mutations: &[Mutation]) -> bool {
    mutations.iter().any(|mutation| {
        mutation.cypher.starts_with("CREATE INDEX")
            || mutation.cypher.starts_with("CREATE RANGE INDEX")
    })
}

pub fn compare_rows(left: &[Row], right: &[Row], semantics: ResultSemantics) -> Result<(), String> {
    match semantics {
        ResultSemantics::Ordered => {
            if left == right {
                Ok(())
            } else {
                Err(format!(
                    "ordered rows differ: left_rows={} right_rows={}",
                    left.len(),
                    right.len()
                ))
            }
        }
        ResultSemantics::Bag => {
            let mut left = left.to_vec();
            let mut right = right.to_vec();
            left.sort_by(compare_row);
            right.sort_by(compare_row);
            if left == right {
                Ok(())
            } else {
                Err(format!(
                    "bag rows differ: left_rows={} right_rows={}",
                    left.len(),
                    right.len()
                ))
            }
        }
    }
}

fn compare_row(left: &Row, right: &Row) -> Ordering {
    left.iter().cmp(right.iter())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CampaignOptions {
    pub seed: u64,
    pub case_count: usize,
    pub case_index: Option<usize>,
}

impl Default for CampaignOptions {
    fn default() -> Self {
        Self {
            seed: 0x9e37_79b9_7f4a_7c15,
            case_count: DEFAULT_CASE_COUNT,
            case_index: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CampaignExecutionOptions {
    pub shard_index: usize,
    pub shard_count: usize,
    pub resume_after_case: Option<usize>,
}

impl Default for CampaignExecutionOptions {
    fn default() -> Self {
        Self {
            shard_index: 0,
            shard_count: 1,
            resume_after_case: None,
        }
    }
}

impl CampaignExecutionOptions {
    pub fn validate(self, campaign: CampaignOptions) -> Result<(), FuzzError> {
        if self.shard_count == 0 {
            return Err(FuzzError::new("shard_count must be greater than zero"));
        }
        if self.shard_index >= self.shard_count {
            return Err(FuzzError::new("shard_index must be less than shard_count"));
        }
        if campaign.case_index.is_some()
            && (self.shard_count != 1 || self.resume_after_case.is_some())
        {
            return Err(FuzzError::new(
                "exact case replay cannot be combined with sharding or resume",
            ));
        }
        Ok(())
    }

    pub fn selected_case_count(self, campaign: CampaignOptions) -> usize {
        campaign.case_index.map_or_else(
            || {
                (0..campaign.case_count)
                    .filter(|index| index % self.shard_count == self.shard_index)
                    .count()
            },
            |_| 1,
        )
    }

    fn selects(self, index: usize) -> bool {
        index % self.shard_count == self.shard_index
            && self
                .resume_after_case
                .is_none_or(|completed| index > completed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignCaseReport {
    pub index: usize,
    pub seed: u64,
    pub shape: String,
    pub graph_tlp_shape: String,
    pub graph_tlp_aggregate_shape: String,
    pub graph_predicate_rewrite_shape: String,
    pub index_enabled: bool,
    pub success: bool,
    pub plan_differential_success: bool,
    pub graph_tlp_success: bool,
    pub graph_tlp_aggregate_success: bool,
    pub graph_predicate_rewrite_success: bool,
    pub metamorphic_success: bool,
    pub direction_reversal_applicable: bool,
    pub reproduction_command: Option<String>,
    pub memo_plan_fingerprint: Option<String>,
    pub direct_fallback_plan_fingerprint: Option<String>,
    pub optimizer_stages: Vec<String>,
    pub plan_coverage_novel: bool,
    pub sql: SqlCaseReport,
    pub failure: Option<FailureReport>,
    pub graph_tlp_failure: Option<GraphTlpFailureReport>,
    pub graph_tlp_aggregate_failure: Option<GraphTlpAggregateFailureReport>,
    pub graph_predicate_rewrite_failure: Option<GraphPredicateRewriteFailureReport>,
    pub metamorphic_failure: Option<MetamorphicFailureReport>,
}

impl CampaignCaseReport {
    pub fn json(&self) -> JsonValue {
        json!({
            "index": self.index,
            "seed": self.seed,
            "shape": self.shape,
            "graph_tlp_shape": self.graph_tlp_shape,
            "graph_tlp_aggregate_shape": self.graph_tlp_aggregate_shape,
            "graph_predicate_rewrite_shape": self.graph_predicate_rewrite_shape,
            "index_enabled": self.index_enabled,
            "success": self.success,
            "plan_differential_success": self.plan_differential_success,
            "graph_tlp_success": self.graph_tlp_success,
            "graph_tlp_aggregate_success": self.graph_tlp_aggregate_success,
            "graph_predicate_rewrite_success": self.graph_predicate_rewrite_success,
            "metamorphic_success": self.metamorphic_success,
            "direction_reversal_applicable": self.direction_reversal_applicable,
            "reproduction_command": self.reproduction_command,
            "memo_plan_fingerprint": self.memo_plan_fingerprint,
            "direct_fallback_plan_fingerprint": self.direct_fallback_plan_fingerprint,
            "optimizer_stages": self.optimizer_stages,
            "plan_coverage_novel": self.plan_coverage_novel,
            "sql": self.sql.json(),
            "failure": self.failure.as_ref().map(FailureReport::json),
            "graph_tlp_failure": self.graph_tlp_failure.as_ref().map(GraphTlpFailureReport::json),
            "graph_tlp_aggregate_failure": self.graph_tlp_aggregate_failure.as_ref().map(GraphTlpAggregateFailureReport::json),
            "graph_predicate_rewrite_failure": self.graph_predicate_rewrite_failure.as_ref().map(GraphPredicateRewriteFailureReport::json),
            "metamorphic_failure": self.metamorphic_failure.as_ref().map(MetamorphicFailureReport::json),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CampaignReport {
    pub seed: u64,
    pub requested_case_count: usize,
    pub executed_case_count: usize,
    pub passed_case_count: usize,
    pub failed_case_count: usize,
    pub complete_shape_coverage: bool,
    pub plan_coverage: PlanCoverageReport,
    pub cases: Vec<CampaignCaseReport>,
}

impl CampaignReport {
    pub const fn success(&self) -> bool {
        self.failed_case_count == 0 && self.executed_case_count > 0
    }

    pub fn json(&self) -> JsonValue {
        let plan_profile = CapabilityProfile::plan_differential_v1();
        let tlp_profile = CapabilityProfile::graph_tlp_v1();
        let tlp_aggregate_profile = CapabilityProfile::graph_tlp_aggregate_v1();
        let predicate_rewrite_profile = CapabilityProfile::graph_predicate_rewrite_v1();
        let metamorphic_profile = CapabilityProfile::graph_metamorphic_v1();
        json!({
            "protocol": CAMPAIGN_PROTOCOL,
            "success": self.success(),
            "seed": self.seed,
            "requested_case_count": self.requested_case_count,
            "executed_case_count": self.executed_case_count,
            "passed_case_count": self.passed_case_count,
            "failed_case_count": self.failed_case_count,
            "complete_shape_coverage": self.complete_shape_coverage,
            "plan_coverage": self.plan_coverage.json(),
            "oracles": ["plan_differential", "graph_tlp", "graph_tlp_aggregate", "graph_predicate_rewrite", "graph_metamorphic", "sql_tlp", "sql_tlp_aggregate", "sql_predicate_rewrite", "sql_join_rewrite"],
            "oracle_protocols": {
                "plan_differential": PLAN_DIFFERENTIAL_PROTOCOL,
                "graph_tlp": GRAPH_TLP_PROTOCOL,
                "graph_tlp_aggregate": GRAPH_TLP_AGGREGATE_PROTOCOL,
                "graph_predicate_rewrite": GRAPH_PREDICATE_REWRITE_PROTOCOL,
                "graph_metamorphic": METAMORPHIC_PROTOCOL,
                "sql_tlp": SQL_TLP_PROTOCOL,
                "sql_tlp_aggregate": SQL_TLP_AGGREGATE_PROTOCOL,
                "sql_predicate_rewrite": SQL_PREDICATE_REWRITE_PROTOCOL,
                "sql_join_rewrite": SQL_JOIN_REWRITE_PROTOCOL,
            },
            "capability_profiles": {
                "plan_differential": capability_profile_json(&plan_profile),
                "graph_tlp": capability_profile_json(&tlp_profile),
                "graph_tlp_aggregate": capability_profile_json(&tlp_aggregate_profile),
                "graph_predicate_rewrite": capability_profile_json(&predicate_rewrite_profile),
                "graph_metamorphic": capability_profile_json(&metamorphic_profile),
                "sql_tlp": sql_oracle::sql_capability_profile_json(false),
                "sql_tlp_aggregate": sql_oracle::sql_capability_profile_json(true),
                "sql_predicate_rewrite": sql_oracle::sql_predicate_rewrite_capability_profile_json(),
                "sql_join_rewrite": sql_oracle::sql_join_rewrite_capability_profile_json(),
            },
            "cases": self.cases.iter().map(CampaignCaseReport::json).collect::<Vec<_>>(),
        })
    }
}

pub fn run_campaign(options: CampaignOptions) -> Result<CampaignReport, FuzzError> {
    run_campaign_with_case_observer(options, CampaignExecutionOptions::default(), |_| Ok(()))
}

pub fn run_campaign_with_case_observer(
    options: CampaignOptions,
    execution: CampaignExecutionOptions,
    mut observer: impl FnMut(&CampaignCaseReport) -> Result<(), FuzzError>,
) -> Result<CampaignReport, FuzzError> {
    if options.case_index.is_none() && options.case_count == 0 {
        return Err(FuzzError::new("case_count must be greater than zero"));
    }
    if options.case_count > MAX_CASE_COUNT {
        return Err(FuzzError::new(format!(
            "case_count exceeds the safety limit {MAX_CASE_COUNT}"
        )));
    }
    if options
        .case_index
        .is_some_and(|index| index >= MAX_CASE_COUNT)
    {
        return Err(FuzzError::new(format!(
            "case_index exceeds the safety limit {}",
            MAX_CASE_COUNT - 1
        )));
    }
    execution.validate(options)?;
    let selected_case_count = execution.selected_case_count(options);
    if selected_case_count == 0 {
        return Err(FuzzError::new("campaign shard selects no cases"));
    }

    let plan_oracle = PlanDifferentialOracle;
    let graph_tlp_oracle = GraphTlpOracle;
    let graph_tlp_aggregate_oracle = GraphTlpAggregateOracle;
    let graph_predicate_rewrite_oracle = GraphPredicateRewriteOracle;
    let metamorphic_oracle = GraphMetamorphicOracle;
    let mut generator = StateAwareCaseGenerator::new(options.seed);
    let generation_case_count = options
        .case_index
        .map_or(options.case_count, |index| index + 1);
    let requested_case_count = selected_case_count;
    let mut cases = Vec::with_capacity(requested_case_count);
    let mut plan_coverage = PlanCoverageTracker::default();
    for index in 0..generation_case_count {
        let case = generator.case(index);
        if options.case_index.is_some_and(|target| target != index) {
            continue;
        }
        if options.case_index.is_none() && !execution.selects(index) {
            continue;
        }
        let (
            plan_differential_success,
            memo_plan_fingerprint,
            direct_fallback_plan_fingerprint,
            optimizer_stages,
            failure,
        ) = match plan_oracle.evaluate(&case) {
            OracleResult::Equivalent(evidence) => {
                let mut stages = evidence.memo.optimizer_stages;
                stages.extend(evidence.direct_fallback.optimizer_stages);
                (
                    true,
                    evidence.memo.plan_fingerprint,
                    evidence.direct_fallback.plan_fingerprint,
                    stages,
                    None,
                )
            }
            OracleResult::Failure(failure) => {
                let mut stages = failure.memo.optimizer_stages.clone();
                stages.extend(failure.direct_fallback.optimizer_stages.iter().cloned());
                (
                    false,
                    failure.memo.plan_fingerprint.clone(),
                    failure.direct_fallback.plan_fingerprint.clone(),
                    stages,
                    Some(failure),
                )
            }
        };
        let (graph_tlp_success, graph_tlp_failure) = match graph_tlp_oracle.evaluate(&case) {
            GraphTlpOracleResult::Equivalent(_) => (true, None),
            GraphTlpOracleResult::Failure(failure) => (false, Some(failure)),
        };
        let (graph_tlp_aggregate_success, graph_tlp_aggregate_failure) =
            match graph_tlp_aggregate_oracle.evaluate(&case) {
                GraphTlpOracleResult::Equivalent(_) => (true, None),
                GraphTlpOracleResult::Failure(failure) => (false, Some(failure)),
            };
        let (graph_predicate_rewrite_success, graph_predicate_rewrite_failure) =
            match graph_predicate_rewrite_oracle.evaluate(&case) {
                GraphPredicateRewriteOracleResult::Equivalent(_) => (true, None),
                GraphPredicateRewriteOracleResult::Failure(failure) => (false, Some(failure)),
            };
        let (metamorphic_success, metamorphic_failure) = match metamorphic_oracle.evaluate(&case) {
            MetamorphicOracleResult::Equivalent(_) => (true, None),
            MetamorphicOracleResult::Failure(failure) => (false, Some(failure)),
        };
        let sql = sql_oracle::evaluate_sql_case(case.seed, index, case.index_enabled);
        let success = plan_differential_success
            && graph_tlp_success
            && graph_tlp_aggregate_success
            && graph_predicate_rewrite_success
            && metamorphic_success
            && sql.success;
        let plan_coverage_novel = plan_coverage.observe(
            memo_plan_fingerprint.as_deref(),
            direct_fallback_plan_fingerprint.as_deref(),
        );
        plan_coverage.observe_optimizer_stages(&optimizer_stages);
        let direction_reversal_applicable = case.metamorphic.direction_reversal.is_some();
        cases.push(CampaignCaseReport {
            index,
            seed: case.seed,
            shape: case.shape,
            graph_tlp_shape: case.graph_tlp.name,
            graph_tlp_aggregate_shape: case.graph_tlp_aggregate.name,
            graph_predicate_rewrite_shape: case.graph_predicate_rewrite.name,
            index_enabled: case.index_enabled,
            success,
            plan_differential_success,
            graph_tlp_success,
            graph_tlp_aggregate_success,
            graph_predicate_rewrite_success,
            metamorphic_success,
            direction_reversal_applicable,
            reproduction_command: (!success).then(|| {
                format!(
                    "bazel run //crates/fuzz:hawdb_optimizer_fuzz -- --seed {} --case-index {index}",
                    options.seed
                )
            }),
            memo_plan_fingerprint,
            direct_fallback_plan_fingerprint,
            optimizer_stages,
            plan_coverage_novel,
            sql,
            failure,
            graph_tlp_failure,
            graph_tlp_aggregate_failure,
            graph_predicate_rewrite_failure,
            metamorphic_failure,
        });
        observer(cases.last().expect("campaign case was just appended"))?;
    }

    let failed_case_count = cases.iter().filter(|case| !case.success).count();
    Ok(CampaignReport {
        seed: options.seed,
        requested_case_count,
        executed_case_count: cases.len(),
        passed_case_count: cases.len().saturating_sub(failed_case_count),
        failed_case_count,
        complete_shape_coverage: options.case_index.is_none()
            && has_complete_shape_coverage(&cases),
        plan_coverage: plan_coverage.report(),
        cases,
    })
}

pub fn campaign_progress_json(
    campaign: CampaignOptions,
    execution: CampaignExecutionOptions,
    cases: &[JsonValue],
) -> JsonValue {
    let failed_case_count = cases.iter().filter(|case| case["success"] == false).count();
    json!({
        "protocol": CAMPAIGN_PROTOCOL,
        "complete": false,
        "success": false,
        "seed": campaign.seed,
        "configured_case_count": campaign.case_count,
        "requested_case_count": execution.selected_case_count(campaign),
        "executed_case_count": cases.len(),
        "passed_case_count": cases.len().saturating_sub(failed_case_count),
        "failed_case_count": failed_case_count,
        "current_case_index": cases.last().and_then(|case| case["index"].as_u64()),
        "shard_index": execution.shard_index,
        "shard_count": execution.shard_count,
        "cases": cases,
    })
}

pub fn merge_campaign_report_json(
    report: &CampaignReport,
    prior_cases: &[JsonValue],
    campaign: CampaignOptions,
    execution: CampaignExecutionOptions,
) -> JsonValue {
    let mut indexed = BTreeMap::<usize, JsonValue>::new();
    for case in prior_cases
        .iter()
        .cloned()
        .chain(report.cases.iter().map(CampaignCaseReport::json))
    {
        if let Some(index) = case["index"]
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
        {
            indexed.insert(index, case);
        }
    }

    let mut plan_coverage = PlanCoverageTracker::default();
    for case in indexed.values_mut() {
        let novel = plan_coverage.observe(
            case["memo_plan_fingerprint"].as_str(),
            case["direct_fallback_plan_fingerprint"].as_str(),
        );
        case["plan_coverage_novel"] = JsonValue::Bool(novel);
        if let Some(stages) = case["optimizer_stages"].as_array() {
            let stages = stages
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>();
            plan_coverage.observe_optimizer_stages(&stages);
        }
    }
    let cases = indexed.into_values().collect::<Vec<_>>();
    let failed_case_count = cases.iter().filter(|case| case["success"] == false).count();
    let requested_case_count = execution.selected_case_count(campaign);
    let complete = cases.len() == requested_case_count;
    let mut json = report.json();
    json["complete"] = JsonValue::Bool(complete);
    json["success"] = JsonValue::Bool(complete && failed_case_count == 0 && !cases.is_empty());
    json["configured_case_count"] = JsonValue::from(campaign.case_count);
    json["requested_case_count"] = JsonValue::from(requested_case_count);
    json["executed_case_count"] = JsonValue::from(cases.len());
    json["passed_case_count"] = JsonValue::from(cases.len().saturating_sub(failed_case_count));
    json["failed_case_count"] = JsonValue::from(failed_case_count);
    json["current_case_index"] = cases
        .last()
        .and_then(|case| case["index"].as_u64())
        .map_or(JsonValue::Null, JsonValue::from);
    json["shard_index"] = JsonValue::from(execution.shard_index);
    json["shard_count"] = JsonValue::from(execution.shard_count);
    json["resumed_case_count"] = JsonValue::from(prior_cases.len());
    json["complete_shape_coverage"] = JsonValue::Bool(
        complete && campaign.case_index.is_none() && has_complete_shape_coverage_json(&cases),
    );
    json["plan_coverage"] = plan_coverage.report().json();
    json["cases"] = JsonValue::Array(cases);
    json
}

fn has_complete_shape_coverage_json(cases: &[JsonValue]) -> bool {
    json_observes_all(
        cases,
        &["shape"],
        &CapabilityProfile::plan_differential_v1().shapes,
    ) && json_observes_all(
        cases,
        &["graph_tlp_shape"],
        &CapabilityProfile::graph_tlp_v1().shapes,
    ) && json_observes_all(
        cases,
        &["graph_tlp_aggregate_shape"],
        &CapabilityProfile::graph_tlp_aggregate_v1().shapes,
    ) && json_observes_all(
        cases,
        &["graph_predicate_rewrite_shape"],
        &CapabilityProfile::graph_predicate_rewrite_v1().shapes,
    ) && json_observes_all(cases, &["sql", "shape"], &sql_oracle::SQL_QUERY_SHAPES)
        && json_observes_all(
            cases,
            &["sql", "predicate_rewrite_shape"],
            &PREDICATE_REWRITE_SHAPES,
        )
        && json_observes_all(
            cases,
            &["sql", "join_rewrite_shape"],
            &sql_oracle::SQL_JOIN_REWRITE_SHAPES,
        )
        && cases
            .iter()
            .any(|case| case["direction_reversal_applicable"] == true)
}

fn json_observes_all(cases: &[JsonValue], path: &[&str], expected: &[&str]) -> bool {
    let observed = cases
        .iter()
        .filter_map(|case| {
            path.iter()
                .try_fold(case, |value, key| value.get(*key))
                .and_then(JsonValue::as_str)
        })
        .collect::<BTreeSet<_>>();
    expected.iter().all(|shape| observed.contains(shape))
}

fn has_complete_shape_coverage(cases: &[CampaignCaseReport]) -> bool {
    let plan_profile = CapabilityProfile::plan_differential_v1();
    let graph_tlp_profile = CapabilityProfile::graph_tlp_v1();
    let graph_tlp_aggregate_profile = CapabilityProfile::graph_tlp_aggregate_v1();
    let graph_predicate_rewrite_profile = CapabilityProfile::graph_predicate_rewrite_v1();

    observes_all_shapes(
        cases.iter().map(|case| case.shape.as_str()),
        &plan_profile.shapes,
    ) && observes_all_shapes(
        cases.iter().map(|case| case.graph_tlp_shape.as_str()),
        &graph_tlp_profile.shapes,
    ) && observes_all_shapes(
        cases
            .iter()
            .map(|case| case.graph_tlp_aggregate_shape.as_str()),
        &graph_tlp_aggregate_profile.shapes,
    ) && observes_all_shapes(
        cases
            .iter()
            .map(|case| case.graph_predicate_rewrite_shape.as_str()),
        &graph_predicate_rewrite_profile.shapes,
    ) && observes_all_shapes(
        cases.iter().map(|case| case.sql.shape.as_str()),
        &sql_oracle::SQL_QUERY_SHAPES,
    ) && observes_all_shapes(
        cases
            .iter()
            .map(|case| case.sql.predicate_rewrite_shape.as_str()),
        &PREDICATE_REWRITE_SHAPES,
    ) && observes_all_shapes(
        cases
            .iter()
            .map(|case| case.sql.join_rewrite_shape.as_str()),
        &sql_oracle::SQL_JOIN_REWRITE_SHAPES,
    ) && !cases.is_empty()
        && cases.iter().any(|case| case.direction_reversal_applicable)
}

fn observes_all_shapes<'a>(observed: impl Iterator<Item = &'a str>, expected: &[&str]) -> bool {
    let observed = observed.collect::<BTreeSet<_>>();
    expected.iter().all(|shape| observed.contains(shape))
}

fn error_class(error: &HawDBError) -> &'static str {
    match error {
        HawDBError::Parse(_) => "parse",
        HawDBError::Semantic(_) => "semantic",
        HawDBError::Execution(_) | HawDBError::TransactionConflict { .. } => "execution",
        HawDBError::Storage(_)
        | HawDBError::StorageIntegrity(_)
        | HawDBError::AppendSequenceExhausted { .. } => "storage",
        HawDBError::CapabilityUnavailable { .. } => "capability_unavailable",
    }
}

fn mutation_json(mutation: &Mutation) -> JsonValue {
    json!({
        "cypher": mutation.cypher,
        "parameters": parameters_json(&mutation.parameters),
    })
}

fn query_invocation_json(query: &QueryInvocation) -> JsonValue {
    json!({
        "cypher": query.cypher,
        "parameters": parameters_json(&query.parameters),
        "result_semantics": query.result_semantics.as_str(),
    })
}

fn graph_tlp_case_json(case: &GraphTlpCase) -> JsonValue {
    json!({
        "name": case.name,
        "original": query_invocation_json(&case.original),
        "predicate_true": query_invocation_json(&case.predicate_true),
        "predicate_false": query_invocation_json(&case.predicate_false),
        "predicate_null": query_invocation_json(&case.predicate_null),
    })
}

fn graph_predicate_rewrite_case_json(case: &GraphPredicateRewriteCase) -> JsonValue {
    json!({
        "name": case.name,
        "original": query_invocation_json(&case.original),
        "rewritten": query_invocation_json(&case.rewritten),
    })
}

fn metamorphic_case_json(case: &MetamorphicCase) -> JsonValue {
    let relation_json = |relation: &MetamorphicRelation| {
        json!({
            "name": relation.name,
            "applicability_guard": relation.applicability_guard,
            "mutations": relation.mutations.iter().map(mutation_json).collect::<Vec<_>>(),
            "query": query_invocation_json(&relation.query),
            "identifier_prefix_to_strip": relation.identifier_prefix_to_strip,
        })
    };
    json!({
        "graph_isomorphism": relation_json(&case.graph_isomorphism),
        "direction_reversal": case.direction_reversal.as_ref().map(relation_json),
    })
}

fn capability_profile_json(profile: &CapabilityProfile) -> JsonValue {
    json!({
        "shapes": profile.shapes,
        "compares_duplicates": profile.compares_duplicates,
        "compares_missing_and_null": profile.compares_missing_and_null,
        "compares_float_bit_patterns": profile.compares_float_bit_patterns,
        "compares_path_values": profile.compares_path_values,
    })
}

fn parameters_json(parameters: &Parameters) -> JsonValue {
    JsonValue::Object(
        parameters
            .iter()
            .map(|(key, value)| (key.clone(), typed_value_json(value)))
            .collect(),
    )
}

fn row_json(row: &Row) -> JsonValue {
    JsonValue::Object(
        row.iter()
            .map(|(key, value)| (key.clone(), typed_value_json(value)))
            .collect(),
    )
}

fn typed_value_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => json!({"type": "null"}),
        Value::Bool(value) => json!({"type": "bool", "value": value}),
        Value::Int(value) => json!({"type": "int", "value": value}),
        Value::Float(value) => json!({
            "type": "float",
            "bits": format!("{:016x}", value.to_bits()),
        }),
        Value::String(value) => json!({"type": "string", "value": value}),
        Value::Uuid(value) => json!({"type": "uuid", "value": value.to_string()}),
        Value::Binary(value) => json!({
            "type": "binary",
            "hex": value.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        }),
        Value::List(values) => json!({
            "type": "list",
            "values": values.iter().map(typed_value_json).collect::<Vec<_>>(),
        }),
        Value::Map(values) => {
            let values = values
                .iter()
                .map(|(key, value)| (key.clone(), typed_value_json(value)))
                .collect::<JsonMap<_, _>>();
            json!({"type": "map", "values": values})
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzError {
    message: String,
}

impl FuzzError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for FuzzError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for FuzzError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Row {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    #[test]
    fn bag_comparison_preserves_duplicate_multiplicity() {
        let one = row([("id", Value::Int(1))]);
        let two = row([("id", Value::Int(2))]);

        assert!(compare_rows(
            &[one.clone(), two.clone(), one.clone()],
            &[two, one.clone(), one],
            ResultSemantics::Bag,
        )
        .is_ok());
        assert!(compare_rows(
            &[row([("id", Value::Int(1))]), row([("id", Value::Int(1))])],
            &[row([("id", Value::Int(1))])],
            ResultSemantics::Bag,
        )
        .is_err());
    }

    #[test]
    fn typed_comparison_distinguishes_missing_null_and_float_bits() {
        assert!(compare_rows(
            &[Row::new()],
            &[row([("value", Value::Null)])],
            ResultSemantics::Ordered,
        )
        .is_err());
        assert!(compare_rows(
            &[row([("value", Value::Float(0.0))])],
            &[row([("value", Value::Float(-0.0))])],
            ResultSemantics::Ordered,
        )
        .is_err());
        let first_nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0001));
        let second_nan = Value::Float(f64::from_bits(0x7ff8_0000_0000_0002));
        assert!(compare_rows(
            &[row([("value", first_nan)])],
            &[row([("value", second_nan)])],
            ResultSemantics::Ordered,
        )
        .is_err());
    }

    #[test]
    fn typed_value_json_preserves_uuid_values() {
        let value = hawdb::Uuid::parse_str("018f4e6a-7c1b-7cc8-8f4d-1234567890ab")
            .expect("parse UUID fixture");
        assert_eq!(
            typed_value_json(&Value::Uuid(value)),
            json!({"type": "uuid", "value": value.to_string()})
        );
    }

    #[test]
    fn campaign_covers_every_query_shape_and_all_oracles() {
        let report = run_campaign(CampaignOptions {
            seed: 7,
            case_count: QUERY_SHAPE_COUNT,
            case_index: None,
        })
        .unwrap();

        assert!(report.success(), "{}", report.json());
        assert!(report.complete_shape_coverage);
        assert_eq!(report.executed_case_count, QUERY_SHAPE_COUNT);
        assert_eq!(report.failed_case_count, 0);
        assert!(report.plan_coverage.unique_memo_plans > 0);
        assert!(report.plan_coverage.unique_direct_fallback_plans > 0);
        assert!(report.plan_coverage.unique_plan_pairs > 0);
        assert!(report
            .cases
            .iter()
            .all(|case| case.memo_plan_fingerprint.is_some()));
        assert!(report
            .cases
            .iter()
            .all(|case| case.direct_fallback_plan_fingerprint.is_some()));
        assert!(report.cases.iter().all(|case| case.graph_tlp_success));
        assert!(report
            .cases
            .iter()
            .all(|case| case.graph_tlp_aggregate_success));
        assert!(report
            .cases
            .iter()
            .all(|case| case.graph_predicate_rewrite_success));
        assert!(report.cases.iter().all(|case| case.metamorphic_success));
        assert!(report.cases.iter().all(|case| case.sql.success));
        assert!(report.cases.iter().all(|case| case.sql.row_tlp_success));
        assert!(report
            .cases
            .iter()
            .all(|case| case.sql.aggregate_tlp_success));
        assert!(report
            .cases
            .iter()
            .all(|case| case.sql.predicate_rewrite_success));
        assert!(report
            .cases
            .iter()
            .all(|case| case.sql.join_rewrite_success));
        assert!(report
            .cases
            .iter()
            .any(|case| case.direction_reversal_applicable));
        assert!(report.cases.iter().any(|case| case.index_enabled));
        assert!(report.cases.iter().any(|case| !case.index_enabled));
        let json = report.json();
        assert_eq!(json["protocol"], CAMPAIGN_PROTOCOL);
        assert_eq!(json["oracles"][0], "plan_differential");
        assert_eq!(json["oracles"][1], "graph_tlp");
        assert_eq!(json["oracles"][2], "graph_tlp_aggregate");
        assert_eq!(json["oracles"][3], "graph_predicate_rewrite");
        assert_eq!(json["oracles"][4], "graph_metamorphic");
        assert_eq!(json["oracles"][5], "sql_tlp");
        assert_eq!(json["oracles"][6], "sql_tlp_aggregate");
        assert_eq!(json["oracles"][7], "sql_predicate_rewrite");
        assert_eq!(json["oracles"][8], "sql_join_rewrite");
        assert_eq!(
            json["oracle_protocols"]["plan_differential"],
            PLAN_DIFFERENTIAL_PROTOCOL
        );
        assert_eq!(json["oracle_protocols"]["graph_tlp"], GRAPH_TLP_PROTOCOL);
        assert_eq!(
            json["oracle_protocols"]["graph_tlp_aggregate"],
            GRAPH_TLP_AGGREGATE_PROTOCOL
        );
        assert_eq!(
            json["oracle_protocols"]["graph_metamorphic"],
            METAMORPHIC_PROTOCOL
        );
        assert_eq!(json["oracle_protocols"]["sql_tlp"], SQL_TLP_PROTOCOL);
        assert_eq!(
            json["oracle_protocols"]["sql_tlp_aggregate"],
            SQL_TLP_AGGREGATE_PROTOCOL
        );
        assert_eq!(
            json["oracle_protocols"]["graph_predicate_rewrite"],
            GRAPH_PREDICATE_REWRITE_PROTOCOL
        );
        assert_eq!(
            json["oracle_protocols"]["sql_predicate_rewrite"],
            SQL_PREDICATE_REWRITE_PROTOCOL
        );
        assert_eq!(
            json["oracle_protocols"]["sql_join_rewrite"],
            SQL_JOIN_REWRITE_PROTOCOL
        );
    }

    #[test]
    fn oracle_runs_both_hints_on_one_pinned_snapshot() {
        let mut generator = StateAwareCaseGenerator::new(7);
        let case = generator.case(0);

        let OracleResult::Equivalent(evidence) = PlanDifferentialOracle.evaluate(&case) else {
            panic!("generated case should be equivalent");
        };
        assert!(evidence.memo.snapshot_epoch.is_some());
        assert_eq!(
            evidence.memo.snapshot_epoch,
            evidence.direct_fallback.snapshot_epoch
        );
        assert_eq!(evidence.memo.optimizer_search.as_deref(), Some("memo"));
        assert_eq!(evidence.memo.search_mode.as_deref(), Some("memo"));
        assert_eq!(
            evidence.direct_fallback.optimizer_search.as_deref(),
            Some("direct_fallback")
        );
        assert_eq!(
            evidence.direct_fallback.search_mode.as_deref(),
            Some("direct_fallback")
        );
    }

    #[test]
    fn state_aware_generator_emits_parseable_queries_for_all_oracles() {
        let mut generator = StateAwareCaseGenerator::new(7);
        let mut rewrite_pairs = BTreeSet::new();

        for index in 0..QUERY_SHAPE_COUNT * PREDICATE_REWRITE_SHAPES.len() {
            let case = generator.case(index);
            rewrite_pairs.insert((
                case.shape.clone(),
                case.graph_predicate_rewrite.name.clone(),
            ));
            let mut queries = vec![
                &case.query,
                &case.graph_tlp.original,
                &case.graph_tlp.predicate_true,
                &case.graph_tlp.predicate_false,
                &case.graph_tlp.predicate_null,
                &case.graph_tlp_aggregate.original,
                &case.graph_tlp_aggregate.predicate_true,
                &case.graph_tlp_aggregate.predicate_false,
                &case.graph_tlp_aggregate.predicate_null,
                &case.graph_predicate_rewrite.original,
                &case.graph_predicate_rewrite.rewritten,
                &case.metamorphic.graph_isomorphism.query,
            ];
            if let Some(direction_reversal) = &case.metamorphic.direction_reversal {
                queries.push(&direction_reversal.query);
            }
            for query in queries {
                hawdb::cypher::parse(&query.cypher).unwrap_or_else(|error| {
                    panic!("generated query failed to parse: {}: {error}", query.cypher)
                });
            }
        }
        assert_eq!(
            rewrite_pairs.len(),
            QUERY_SHAPE_COUNT * PREDICATE_REWRITE_SHAPES.len()
        );
    }

    #[test]
    fn graph_tlp_detects_an_invalid_partition_relation() {
        let mut generator = StateAwareCaseGenerator::new(7);
        let mut case = generator.case(0);
        case.graph_tlp.predicate_false = case.graph_tlp.predicate_true.clone();

        assert!(PlanDifferentialOracle.evaluate(&case).is_equivalent());
        let GraphTlpOracleResult::Failure(failure) = GraphTlpOracle.evaluate(&case) else {
            panic!("graph TLP must reject overlapping partitions");
        };
        assert!(failure.reason.contains("graph TLP partition mismatch"));
        assert_eq!(failure.signature, "graph_tlp_partition_mismatch");
        assert_eq!(
            failure.replay.graph_tlp.predicate_false,
            failure.replay.graph_tlp.predicate_true
        );
        assert_eq!(failure.reduction.oracle, "graph_tlp");
        assert!(
            failure.reduction.reduced_mutation_count < failure.reduction.original_mutation_count
        );
    }

    #[test]
    fn graph_predicate_rewrite_detects_and_reduces_invalid_relation() {
        let mut generator = StateAwareCaseGenerator::new(7);
        let mut case = generator.case(0);
        case.graph_predicate_rewrite.rewritten = case.graph_tlp.original.clone();

        let GraphPredicateRewriteOracleResult::Failure(failure) =
            GraphPredicateRewriteOracle.evaluate(&case)
        else {
            panic!("graph predicate rewrite must reject a non-equivalent query");
        };
        assert!(failure.reason.contains("graph predicate rewrite mismatch"));
        assert_eq!(failure.signature, "graph_predicate_rewrite_mismatch");
        assert_eq!(failure.reduction.oracle, "graph_predicate_rewrite");
        assert!(
            failure.reduction.reduced_mutation_count < failure.reduction.original_mutation_count
        );
    }

    #[test]
    fn graph_tlp_covers_nullable_and_relationship_predicates() {
        let mut generator = StateAwareCaseGenerator::new(7);
        let mut observed_nullable = false;
        let mut observed_relationship = false;

        for index in 0..32 {
            let case = generator.case(index);
            observed_nullable |= case.graph_tlp.name == "nullable_node_property";
            observed_relationship |= case.graph_tlp.name == "relationship_range";
            let GraphTlpOracleResult::Equivalent(evidence) = GraphTlpOracle.evaluate(&case) else {
                panic!("generated graph TLP relation must be equivalent");
            };
            let ExecutionOutcome::Rows(null_rows) = evidence.predicate_null.outcome else {
                panic!("generated null partition must execute successfully");
            };
            assert!(!null_rows.is_empty());
        }

        assert!(observed_nullable);
        assert!(observed_relationship);
    }

    #[test]
    fn graph_tlp_aggregate_recombines_partition_counts() {
        let mut generator = StateAwareCaseGenerator::new(7);

        for index in 0..32 {
            let case = generator.case(index);
            let GraphTlpAggregateOracleResult::Equivalent(evidence) =
                GraphTlpAggregateOracle.evaluate(&case)
            else {
                panic!("generated graph TLP aggregate relation must be equivalent");
            };
            let snapshot_epoch = evidence.original.snapshot_epoch;
            assert!(snapshot_epoch.is_some());
            assert!(evidence
                .observations()
                .iter()
                .all(|(_, observation)| observation.snapshot_epoch == snapshot_epoch));

            let counts = evidence
                .observations()
                .map(|(_, observation)| observation_count(observation).unwrap());
            assert_eq!(counts[0], counts[1] + counts[2] + counts[3]);
        }
    }

    #[test]
    fn graph_tlp_aggregate_detects_an_invalid_partition_relation() {
        let mut generator = StateAwareCaseGenerator::new(7);
        let mut case = generator.case(0);
        case.graph_tlp_aggregate.predicate_null = case.graph_tlp_aggregate.original.clone();

        let GraphTlpAggregateOracleResult::Failure(failure) =
            GraphTlpAggregateOracle.evaluate(&case)
        else {
            panic!("graph TLP aggregate must reject an invalid count partition");
        };
        assert!(failure.reason.contains("graph TLP aggregate mismatch"));
        assert_eq!(failure.reduction.oracle, "graph_tlp_aggregate");
        assert_eq!(
            failure.replay.graph_tlp_aggregate.predicate_null,
            failure.replay.graph_tlp_aggregate.original
        );
    }

    #[test]
    fn replay_bundle_retains_typed_parameters() {
        let mut generator = StateAwareCaseGenerator::new(9);
        let case = generator.case(2);
        let replay = ReplayBundle::from_case(&case).json();

        assert_eq!(replay["protocol"], REPLAY_BUNDLE_PROTOCOL);
        assert_eq!(replay["shape"], "in_filter");
        assert_eq!(replay["optimizer_search_variants"][0], "memo");
        assert_eq!(replay["optimizer_search_variants"][1], "direct_fallback");
        assert_eq!(replay["query"]["parameters"]["ids"]["type"], "list");
        assert!(replay["query_ast"]["node_count"].as_u64().unwrap() > 0);
        assert_eq!(
            replay["metamorphic"]["graph_isomorphism"]["name"],
            "graph_isomorphism"
        );
        assert!(replay["graph_tlp"]["predicate_true"]["cypher"]
            .as_str()
            .unwrap()
            .contains("WHERE"));
        assert!(replay["graph_tlp_aggregate"]["original"]["cypher"]
            .as_str()
            .unwrap()
            .contains("count("));
        assert_eq!(
            replay["graph_predicate_rewrite"]["name"],
            case.graph_predicate_rewrite.name
        );
    }

    #[test]
    fn invalid_generated_case_fails_closed_with_replay() {
        let mut generator = StateAwareCaseGenerator::new(11);
        let mut case = generator.case(0);
        case.mutations.push(Mutation::new("CREATE invalid"));

        let OracleResult::Failure(failure) = PlanDifferentialOracle.evaluate(&case) else {
            panic!("invalid generated case must fail closed");
        };
        assert!(failure.reason.contains("memo=mutation/parse"));
        assert!(failure
            .signature
            .starts_with("plan_differential_both_errored_"));
        assert_eq!(failure.replay.seed, case.seed);
        assert_eq!(
            failure.replay.mutations.last().unwrap().cypher,
            "CREATE invalid"
        );
        assert_eq!(failure.reduction.oracle, "plan_differential");
        assert_eq!(failure.reduction.reduced_mutation_count, 1);
        assert_eq!(
            failure.reduction.replay.mutations[0].cypher,
            "CREATE invalid"
        );
        assert!(
            failure.reduction.reduced_query_node_count
                < failure.reduction.original_query_node_count
        );
    }

    #[test]
    fn metamorphic_oracle_has_independent_direction_failure_signature() {
        let mut generator = StateAwareCaseGenerator::new(7);
        let mut case = (0..QUERY_SHAPE_COUNT)
            .map(|index| generator.case(index))
            .find(|case| case.metamorphic.direction_reversal.is_some())
            .unwrap();
        case.metamorphic.direction_reversal.as_mut().unwrap().query = case.query.clone();

        let MetamorphicOracleResult::Failure(failure) = GraphMetamorphicOracle.evaluate(&case)
        else {
            panic!("invalid direction transform must fail");
        };
        assert_eq!(failure.signature, "direction_reversal_result_mismatch");
        assert_eq!(failure.relation, "direction_reversal");
        assert!(PlanDifferentialOracle.evaluate(&case).is_equivalent());
        assert!(GraphTlpOracle.evaluate(&case).is_equivalent());
    }

    #[test]
    fn semantic_mismatch_signature_cannot_reduce_to_execution_error() {
        let rows = ExecutionOutcome::Rows(vec![row([("id", Value::Int(1))])]);
        let different_rows = ExecutionOutcome::Rows(vec![row([("id", Value::Int(2))])]);
        let semantic = classify_plan_failure(&rows, &different_rows, ResultSemantics::Bag)
            .unwrap()
            .signature;
        let execution_error = ExecutionOutcome::Error {
            phase: "execute",
            class: "semantic",
            message: "invalid reduced query".to_string(),
        };
        let reduced = classify_plan_failure(&execution_error, &rows, ResultSemantics::Bag)
            .unwrap()
            .signature;

        assert_eq!(semantic, FailureSignature::PlanResultMismatch);
        assert_ne!(semantic, reduced);
    }

    #[test]
    fn campaign_is_reproducible_from_seed() {
        let options = CampaignOptions {
            seed: 17,
            case_count: 3,
            case_index: None,
        };

        assert_eq!(
            run_campaign(options).unwrap(),
            run_campaign(options).unwrap()
        );
    }

    #[test]
    fn sharded_campaign_resumes_without_reexecuting_completed_cases() {
        let campaign = CampaignOptions {
            seed: 17,
            case_count: 12,
            case_index: None,
        };
        let execution = CampaignExecutionOptions {
            shard_index: 1,
            shard_count: 3,
            resume_after_case: None,
        };
        let full = run_campaign_with_case_observer(campaign, execution, |_| Ok(())).unwrap();
        assert_eq!(
            full.cases.iter().map(|case| case.index).collect::<Vec<_>>(),
            [1, 4, 7, 10]
        );

        let prior_cases = full.cases[..2]
            .iter()
            .map(CampaignCaseReport::json)
            .collect::<Vec<_>>();
        let resumed_execution = CampaignExecutionOptions {
            resume_after_case: Some(4),
            ..execution
        };
        let mut observed = Vec::new();
        let resumed = run_campaign_with_case_observer(campaign, resumed_execution, |case| {
            observed.push(case.index);
            Ok(())
        })
        .unwrap();
        assert_eq!(observed, [7, 10]);

        let merged =
            merge_campaign_report_json(&resumed, &prior_cases, campaign, resumed_execution);
        assert_eq!(merged["complete"], true);
        assert_eq!(merged["success"], true);
        assert_eq!(merged["requested_case_count"], 4);
        assert_eq!(merged["executed_case_count"], 4);
        assert_eq!(merged["current_case_index"], 10);
        assert_eq!(
            merged["cases"]
                .as_array()
                .unwrap()
                .iter()
                .map(|case| case["index"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [1, 4, 7, 10]
        );
    }

    #[test]
    fn exact_case_replay_preserves_generated_case_and_oracle_results() {
        let full = run_campaign(CampaignOptions {
            seed: 17,
            case_count: 6,
            case_index: None,
        })
        .unwrap();
        let exact = run_campaign(CampaignOptions {
            seed: 17,
            case_count: DEFAULT_CASE_COUNT,
            case_index: Some(5),
        })
        .unwrap();

        assert_eq!(exact.requested_case_count, 1);
        assert_eq!(exact.executed_case_count, 1);
        assert!(!exact.complete_shape_coverage);
        assert_eq!(exact.cases[0].index, 5);
        assert_eq!(exact.cases[0].seed, full.cases[5].seed);
        assert_eq!(exact.cases[0].shape, full.cases[5].shape);
        assert_eq!(
            exact.cases[0].memo_plan_fingerprint,
            full.cases[5].memo_plan_fingerprint
        );
        assert_eq!(exact.cases[0].success, full.cases[5].success);
        assert_eq!(exact.cases[0].sql, full.cases[5].sql);
    }

    #[test]
    fn campaign_rejects_unbounded_or_empty_runs() {
        assert_eq!(
            run_campaign(CampaignOptions {
                seed: 1,
                case_count: 0,
                case_index: None,
            })
            .unwrap_err()
            .to_string(),
            "case_count must be greater than zero"
        );
        assert!(run_campaign(CampaignOptions {
            seed: 1,
            case_count: MAX_CASE_COUNT + 1,
            case_index: None,
        })
        .is_err());
        assert!(run_campaign(CampaignOptions {
            seed: 1,
            case_count: DEFAULT_CASE_COUNT,
            case_index: Some(MAX_CASE_COUNT),
        })
        .is_err());
        assert!(run_campaign_with_case_observer(
            CampaignOptions {
                seed: 1,
                case_count: 4,
                case_index: None,
            },
            CampaignExecutionOptions {
                shard_index: 2,
                shard_count: 2,
                resume_after_case: None,
            },
            |_| Ok(()),
        )
        .is_err());
    }
}
