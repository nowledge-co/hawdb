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

use crate::{
    compare_rows, error_class, row_json, typed_value_json, ExecutionOutcome, ResultSemantics,
};
#[cfg(test)]
use hawdb::api::DatabaseConfig;
use hawdb::api::{Database, DatabaseReadTransaction};
use hawdb::{
    HawDBError, QueryStreamOptions, RelationalJoinPlanningDirective, RelationalJoinPlanningOutcome,
    RelationalJoinPlanningReason, RelationalJoinPlanningStatus, RelationalJoinPlanningStrategy,
    Value,
};
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;

mod generator;

use generator::generate_sql_case;

pub const SQL_TLP_PROTOCOL: &str = "hawdb-sql-tlp-fuzz-v1";
pub const SQL_TLP_AGGREGATE_PROTOCOL: &str = "hawdb-sql-tlp-aggregate-fuzz-v1";
pub const SQL_PREDICATE_REWRITE_PROTOCOL: &str = "hawdb-sql-predicate-rewrite-fuzz-v1";
pub const SQL_JOIN_REWRITE_PROTOCOL: &str = "hawdb-sql-join-rewrite-fuzz-v1";
pub const SQL_REPLAY_PROTOCOL: &str = "hawdb-sql-fuzz-replay-v1";

const MAX_SQL_REDUCTION_ATTEMPTS: usize = 64;
pub(crate) const SQL_QUERY_SHAPES: [&str; 8] = [
    "nullable_score_range",
    "nullable_tag_equality",
    "inner_join_nullable_priority",
    "left_join_nullable_priority",
    "nullable_score_in_list",
    "nullable_column_comparison",
    "nullable_conjunction",
    "nullable_disjunction",
];
const SQL_QUERY_SHAPE_COUNT: usize = SQL_QUERY_SHAPES.len();
pub(crate) const SQL_JOIN_REWRITE_SHAPES: [&str; 12] = [
    "three_inner_chain",
    "three_inner_reverse_chain",
    "four_inner_chain",
    "four_inner_star",
    "four_inner_cycle",
    "five_inner_tree",
    "five_inner_cycle",
    "five_mixed_left_preserved",
    "six_inner_tree",
    "six_inner_cycle",
    "six_mixed_left_preserved",
    "six_mixed_left_null_rejected",
];
const SQL_JOIN_REWRITE_SHAPE_COUNT: usize = SQL_JOIN_REWRITE_SHAPES.len();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlMutation {
    pub sql: String,
    pub parameters: Vec<Value>,
    reducible: bool,
}

impl SqlMutation {
    fn required(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
            reducible: false,
        }
    }

    fn data(sql: impl Into<String>, parameters: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            parameters,
            reducible: true,
        }
    }

    fn index(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
            reducible: true,
        }
    }

    fn json(&self) -> JsonValue {
        json!({
            "sql": self.sql,
            "parameters": values_json(&self.parameters),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlQueryInvocation {
    pub sql: String,
    pub parameters: Vec<Value>,
    pub result_semantics: ResultSemantics,
}

impl SqlQueryInvocation {
    fn json(&self) -> JsonValue {
        json!({
            "sql": self.sql,
            "parameters": values_json(&self.parameters),
            "result_semantics": self.result_semantics.as_str(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTlpCase {
    pub name: String,
    pub original: SqlQueryInvocation,
    pub predicate_true: SqlQueryInvocation,
    pub predicate_false: SqlQueryInvocation,
    pub predicate_null: SqlQueryInvocation,
}

impl SqlTlpCase {
    fn json(&self) -> JsonValue {
        json!({
            "name": self.name,
            "original": self.original.json(),
            "predicate_true": self.predicate_true.json(),
            "predicate_false": self.predicate_false.json(),
            "predicate_null": self.predicate_null.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPredicateRewriteCase {
    pub name: String,
    pub original: SqlQueryInvocation,
    pub rewritten: SqlQueryInvocation,
}

impl SqlPredicateRewriteCase {
    fn json(&self) -> JsonValue {
        json!({
            "name": self.name,
            "original": self.original.json(),
            "rewritten": self.rewritten.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJoinRewriteCase {
    pub name: String,
    pub optimized: SqlQueryInvocation,
    pub syntax_reference: SqlQueryInvocation,
    pub expected_strategy: RelationalJoinPlanningStrategy,
    pub generator_profile: SqlJoinGeneratorProfile,
}

impl SqlJoinRewriteCase {
    fn json(&self) -> JsonValue {
        json!({
            "name": self.name,
            "optimized": self.optimized.json(),
            "syntax_reference": self.syntax_reference.json(),
            "expected_strategy": self.expected_strategy.as_str(),
            "generator_profile": self.generator_profile.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJoinGeneratorProfile {
    pub relation_count: usize,
    pub join_graph: String,
    pub null_rejection: String,
    pub selectivity: String,
    pub index_profile: String,
    pub statistics_profile: String,
    pub table_cardinalities: BTreeMap<String, usize>,
    pub dimension_distinct_count: usize,
    pub skewed_join_keys: bool,
}

impl SqlJoinGeneratorProfile {
    fn json(&self) -> JsonValue {
        json!({
            "relation_count": self.relation_count,
            "join_graph": self.join_graph,
            "null_rejection": self.null_rejection,
            "selectivity": self.selectivity,
            "index_profile": self.index_profile,
            "statistics_profile": self.statistics_profile,
            "table_cardinalities": self.table_cardinalities,
            "dimension_distinct_count": self.dimension_distinct_count,
            "skewed_join_keys": self.skewed_join_keys,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlFuzzCase {
    seed: u64,
    shape: String,
    setup: Vec<SqlMutation>,
    row_tlp: SqlTlpCase,
    aggregate_tlp: SqlTlpCase,
    predicate_rewrite: SqlPredicateRewriteCase,
    join_rewrite: SqlJoinRewriteCase,
    index_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlReplayBundle {
    pub seed: u64,
    pub shape: String,
    pub setup: Vec<SqlMutation>,
    pub row_tlp: SqlTlpCase,
    pub aggregate_tlp: SqlTlpCase,
    pub predicate_rewrite: SqlPredicateRewriteCase,
    pub join_rewrite: SqlJoinRewriteCase,
    pub index_enabled: bool,
}

impl SqlReplayBundle {
    fn from_case(case: &SqlFuzzCase) -> Self {
        Self {
            seed: case.seed,
            shape: case.shape.clone(),
            setup: case.setup.clone(),
            row_tlp: case.row_tlp.clone(),
            aggregate_tlp: case.aggregate_tlp.clone(),
            predicate_rewrite: case.predicate_rewrite.clone(),
            join_rewrite: case.join_rewrite.clone(),
            index_enabled: case.index_enabled,
        }
    }

    pub fn json(&self) -> JsonValue {
        json!({
            "protocol": SQL_REPLAY_PROTOCOL,
            "seed": self.seed,
            "shape": self.shape,
            "index_enabled": self.index_enabled,
            "setup": self.setup.iter().map(SqlMutation::json).collect::<Vec<_>>(),
            "row_tlp": self.row_tlp.json(),
            "aggregate_tlp": self.aggregate_tlp.json(),
            "predicate_rewrite": self.predicate_rewrite.json(),
            "join_rewrite": self.join_rewrite.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExecutionObservation {
    pub snapshot_epoch: Option<u64>,
    pub plan: Option<Vec<hawdb::executor::Row>>,
    pub join_planning: Option<Box<RelationalJoinPlanningOutcome>>,
    pub outcome: ExecutionOutcome,
}

impl SqlExecutionObservation {
    fn json(&self) -> JsonValue {
        json!({
            "snapshot_epoch": self.snapshot_epoch,
            "plan": self.plan.as_ref().map(|rows| rows.iter().map(row_json).collect::<Vec<_>>()),
            "join_planning": self.join_planning.as_deref().map(join_planning_json),
            "outcome": self.outcome.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTlpEvidence {
    pub original: SqlExecutionObservation,
    pub predicate_true: SqlExecutionObservation,
    pub predicate_false: SqlExecutionObservation,
    pub predicate_null: SqlExecutionObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPredicateRewriteEvidence {
    pub original: SqlExecutionObservation,
    pub rewritten: SqlExecutionObservation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJoinRewriteEvidence {
    pub optimized: SqlExecutionObservation,
    pub syntax_reference: SqlExecutionObservation,
}

impl SqlJoinRewriteEvidence {
    fn json(&self) -> JsonValue {
        json!({
            "optimized": self.optimized.json(),
            "syntax_reference": self.syntax_reference.json(),
        })
    }
}

impl SqlPredicateRewriteEvidence {
    fn json(&self) -> JsonValue {
        json!({
            "original": self.original.json(),
            "rewritten": self.rewritten.json(),
        })
    }
}

impl SqlTlpEvidence {
    fn observations(&self) -> [(&'static str, &SqlExecutionObservation); 4] {
        [
            ("original", &self.original),
            ("predicate_true", &self.predicate_true),
            ("predicate_false", &self.predicate_false),
            ("predicate_null", &self.predicate_null),
        ]
    }

    fn json(&self) -> JsonValue {
        json!({
            "original": self.original.json(),
            "predicate_true": self.predicate_true.json(),
            "predicate_false": self.predicate_false.json(),
            "predicate_null": self.predicate_null.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlReductionReport {
    pub oracle: &'static str,
    pub original_setup_count: usize,
    pub reduced_setup_count: usize,
    pub attempts: usize,
    pub replay: SqlReplayBundle,
}

impl SqlReductionReport {
    fn json(&self) -> JsonValue {
        json!({
            "oracle": self.oracle,
            "original_setup_count": self.original_setup_count,
            "reduced_setup_count": self.reduced_setup_count,
            "attempts": self.attempts,
            "replay": self.replay.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFailureReport {
    pub signature: String,
    pub reason: String,
    pub replay: SqlReplayBundle,
    pub reduction: SqlReductionReport,
    pub evidence: SqlTlpEvidence,
}

impl SqlFailureReport {
    fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "reason": self.reason,
            "replay": self.replay.json(),
            "reduction": self.reduction.json(),
            "evidence": self.evidence.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlPredicateRewriteFailureReport {
    pub signature: String,
    pub reason: String,
    pub replay: SqlReplayBundle,
    pub reduction: SqlReductionReport,
    pub evidence: SqlPredicateRewriteEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJoinRewriteFailureReport {
    pub signature: String,
    pub reason: String,
    pub replay: SqlReplayBundle,
    pub reduction: SqlReductionReport,
    pub evidence: SqlJoinRewriteEvidence,
}

impl SqlJoinRewriteFailureReport {
    fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "reason": self.reason,
            "replay": self.replay.json(),
            "reduction": self.reduction.json(),
            "evidence": self.evidence.json(),
        })
    }
}

impl SqlPredicateRewriteFailureReport {
    fn json(&self) -> JsonValue {
        json!({
            "signature": self.signature,
            "reason": self.reason,
            "replay": self.replay.json(),
            "reduction": self.reduction.json(),
            "evidence": self.evidence.json(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCaseReport {
    pub shape: String,
    pub predicate_rewrite_shape: String,
    pub join_rewrite_shape: String,
    pub index_enabled: bool,
    pub success: bool,
    pub row_tlp_success: bool,
    pub aggregate_tlp_success: bool,
    pub predicate_rewrite_success: bool,
    pub join_rewrite_success: bool,
    pub join_rewrite_profile: SqlJoinGeneratorProfile,
    pub join_rewrite_plan_signature: Option<String>,
    pub join_rewrite_planning: Option<Box<RelationalJoinPlanningOutcome>>,
    pub row_tlp_failure: Option<SqlFailureReport>,
    pub aggregate_tlp_failure: Option<SqlFailureReport>,
    pub predicate_rewrite_failure: Option<SqlPredicateRewriteFailureReport>,
    pub join_rewrite_failure: Option<SqlJoinRewriteFailureReport>,
}

pub(crate) fn sql_capability_profile_json(aggregate: bool) -> JsonValue {
    json!({
        "shapes": SQL_QUERY_SHAPES,
        "aggregate": aggregate,
        "parameterized": true,
        "nullable_predicates": true,
        "in_list": true,
        "column_comparison": true,
        "boolean_composition": true,
        "inner_join": true,
        "left_join": true,
        "optional_indexes": true,
        "result_semantics": "bag",
    })
}

pub(crate) fn sql_predicate_rewrite_capability_profile_json() -> JsonValue {
    json!({
        "shapes": crate::predicate_rewrite::PREDICATE_REWRITE_SHAPES,
        "parameterized": true,
        "three_valued_logic": true,
        "pinned_snapshot": true,
        "result_semantics": "bag",
    })
}

pub(crate) fn sql_join_rewrite_capability_profile_json() -> JsonValue {
    json!({
        "shapes": SQL_JOIN_REWRITE_SHAPES,
        "relation_counts": [3, 4, 5, 6],
        "duplicate_values": true,
        "null_values": true,
        "inner_join": true,
        "left_join": true,
        "null_rejection": true,
        "pinned_snapshot": true,
        "optimized_strategy": ["csg_cmp_memo"],
        "join_graphs": ["chain", "reverse_chain", "star", "tree", "cycle"],
        "selectivity_profiles": ["rare", "medium", "broad"],
        "index_profiles": ["none", "join_keys", "filter", "composite"],
        "statistics_profiles": ["compact", "skewed", "wide"],
        "reference_strategy": "syntax_order",
        "result_semantics": "bag",
    })
}

impl SqlCaseReport {
    pub(crate) fn json(&self) -> JsonValue {
        json!({
            "shape": self.shape,
            "predicate_rewrite_shape": self.predicate_rewrite_shape,
            "join_rewrite_shape": self.join_rewrite_shape,
            "index_enabled": self.index_enabled,
            "success": self.success,
            "row_tlp_success": self.row_tlp_success,
            "aggregate_tlp_success": self.aggregate_tlp_success,
            "predicate_rewrite_success": self.predicate_rewrite_success,
            "join_rewrite_success": self.join_rewrite_success,
            "join_rewrite_profile": self.join_rewrite_profile.json(),
            "join_rewrite_plan_signature": self.join_rewrite_plan_signature,
            "join_rewrite_planning": self.join_rewrite_planning.as_deref().map(join_planning_json),
            "row_tlp_failure": self.row_tlp_failure.as_ref().map(SqlFailureReport::json),
            "aggregate_tlp_failure": self.aggregate_tlp_failure.as_ref().map(SqlFailureReport::json),
            "predicate_rewrite_failure": self.predicate_rewrite_failure.as_ref().map(SqlPredicateRewriteFailureReport::json),
            "join_rewrite_failure": self.join_rewrite_failure.as_ref().map(SqlJoinRewriteFailureReport::json),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlOracleKind {
    RowTlp,
    AggregateTlp,
    PredicateRewrite,
    JoinRewrite,
}

impl SqlOracleKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::RowTlp => "sql_tlp",
            Self::AggregateTlp => "sql_tlp_aggregate",
            Self::PredicateRewrite => "sql_predicate_rewrite",
            Self::JoinRewrite => "sql_join_rewrite",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlFailureSignature {
    Errored {
        oracle: SqlOracleKind,
        variant: &'static str,
        phase: &'static str,
        class: &'static str,
    },
    SnapshotMismatch {
        oracle: SqlOracleKind,
    },
    RowPartitionMismatch,
    AggregateInvalidResult {
        variant: &'static str,
    },
    AggregateOverflow,
    AggregatePartitionMismatch,
    PredicateRewriteMismatch,
    JoinRewritePlanningMismatch {
        variant: &'static str,
    },
    JoinRewriteResultMismatch,
}

impl SqlFailureSignature {
    fn code(&self) -> String {
        match self {
            Self::Errored {
                oracle,
                variant,
                phase,
                class,
            } => format!("{}_{variant}_{phase}_{class}", oracle.as_str()),
            Self::SnapshotMismatch { oracle } => format!("{}_snapshot_mismatch", oracle.as_str()),
            Self::RowPartitionMismatch => "sql_tlp_partition_mismatch".to_string(),
            Self::AggregateInvalidResult { variant } => {
                format!("sql_tlp_aggregate_{variant}_invalid_result")
            }
            Self::AggregateOverflow => "sql_tlp_aggregate_overflow".to_string(),
            Self::AggregatePartitionMismatch => "sql_tlp_aggregate_partition_mismatch".to_string(),
            Self::PredicateRewriteMismatch => "sql_predicate_rewrite_mismatch".to_string(),
            Self::JoinRewritePlanningMismatch { variant } => {
                format!("sql_join_rewrite_{variant}_planning_mismatch")
            }
            Self::JoinRewriteResultMismatch => "sql_join_rewrite_result_mismatch".to_string(),
        }
    }
}

#[derive(Debug)]
struct DetectedSqlFailure {
    signature: SqlFailureSignature,
    reason: String,
}

pub(crate) fn evaluate_sql_case(seed: u64, index: usize, index_enabled: bool) -> SqlCaseReport {
    let case = generate_sql_case(seed, index, index_enabled);
    let (row_evidence, aggregate_evidence, predicate_rewrite_evidence, join_rewrite_evidence) =
        execute_sql_case(&case);
    let row_failure = classify_sql_row_tlp_failure(&row_evidence);
    let aggregate_failure = classify_sql_aggregate_tlp_failure(&aggregate_evidence);
    let predicate_rewrite_failure =
        classify_sql_predicate_rewrite_failure(&predicate_rewrite_evidence);
    let join_rewrite_failure =
        classify_sql_join_rewrite_failure(&case.join_rewrite, &join_rewrite_evidence);
    let row_tlp_success = row_failure.is_none();
    let aggregate_tlp_success = aggregate_failure.is_none();
    let predicate_rewrite_success = predicate_rewrite_failure.is_none();
    let join_rewrite_success = join_rewrite_failure.is_none();
    let join_rewrite_profile = case.join_rewrite.generator_profile.clone();
    let join_rewrite_plan_signature = join_rewrite_evidence
        .optimized
        .plan
        .as_deref()
        .map(sql_plan_signature);
    let join_rewrite_planning = join_rewrite_evidence.optimized.join_planning.clone();

    SqlCaseReport {
        shape: case.shape.clone(),
        predicate_rewrite_shape: case.predicate_rewrite.name.clone(),
        join_rewrite_shape: case.join_rewrite.name.clone(),
        index_enabled,
        success: row_tlp_success
            && aggregate_tlp_success
            && predicate_rewrite_success
            && join_rewrite_success,
        row_tlp_success,
        aggregate_tlp_success,
        predicate_rewrite_success,
        join_rewrite_success,
        join_rewrite_profile,
        join_rewrite_plan_signature,
        join_rewrite_planning,
        row_tlp_failure: row_failure
            .map(|failure| failure_report(&case, SqlOracleKind::RowTlp, failure, row_evidence)),
        aggregate_tlp_failure: aggregate_failure.map(|failure| {
            failure_report(
                &case,
                SqlOracleKind::AggregateTlp,
                failure,
                aggregate_evidence,
            )
        }),
        predicate_rewrite_failure: predicate_rewrite_failure.map(|failure| {
            predicate_rewrite_failure_report(&case, failure, predicate_rewrite_evidence)
        }),
        join_rewrite_failure: join_rewrite_failure
            .map(|failure| join_rewrite_failure_report(&case, failure, join_rewrite_evidence)),
    }
}

fn failure_report(
    case: &SqlFuzzCase,
    oracle: SqlOracleKind,
    failure: DetectedSqlFailure,
    evidence: SqlTlpEvidence,
) -> SqlFailureReport {
    SqlFailureReport {
        signature: failure.signature.code(),
        reason: failure.reason,
        replay: SqlReplayBundle::from_case(case),
        reduction: reduce_sql_failure(case, oracle, &failure.signature),
        evidence,
    }
}

fn predicate_rewrite_failure_report(
    case: &SqlFuzzCase,
    failure: DetectedSqlFailure,
    evidence: SqlPredicateRewriteEvidence,
) -> SqlPredicateRewriteFailureReport {
    SqlPredicateRewriteFailureReport {
        signature: failure.signature.code(),
        reason: failure.reason,
        replay: SqlReplayBundle::from_case(case),
        reduction: reduce_sql_failure(case, SqlOracleKind::PredicateRewrite, &failure.signature),
        evidence,
    }
}

fn join_rewrite_failure_report(
    case: &SqlFuzzCase,
    failure: DetectedSqlFailure,
    evidence: SqlJoinRewriteEvidence,
) -> SqlJoinRewriteFailureReport {
    SqlJoinRewriteFailureReport {
        signature: failure.signature.code(),
        reason: failure.reason,
        replay: SqlReplayBundle::from_case(case),
        reduction: reduce_sql_failure(case, SqlOracleKind::JoinRewrite, &failure.signature),
        evidence,
    }
}

fn execute_sql_case(
    case: &SqlFuzzCase,
) -> (
    SqlTlpEvidence,
    SqlTlpEvidence,
    SqlPredicateRewriteEvidence,
    SqlJoinRewriteEvidence,
) {
    let (snapshot, snapshot_epoch) = match prepare_sql_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => {
            let evidence = repeated_evidence(observation);
            let predicate_rewrite = repeated_predicate_rewrite_evidence(evidence.original.clone());
            let join_rewrite = repeated_join_rewrite_evidence(evidence.original.clone());
            return (evidence.clone(), evidence, predicate_rewrite, join_rewrite);
        }
    };
    (
        execute_sql_tlp_queries(&snapshot, snapshot_epoch, &case.row_tlp),
        execute_sql_tlp_queries(&snapshot, snapshot_epoch, &case.aggregate_tlp),
        execute_sql_predicate_rewrite_queries(&snapshot, snapshot_epoch, &case.predicate_rewrite),
        execute_sql_join_rewrite_queries(&snapshot, snapshot_epoch, &case.join_rewrite),
    )
}

fn execute_sql_oracle(case: &SqlFuzzCase, oracle: SqlOracleKind) -> SqlTlpEvidence {
    let (snapshot, snapshot_epoch) = match prepare_sql_case(case) {
        Ok(prepared) => prepared,
        Err(observation) => return repeated_evidence(observation),
    };
    let queries = match oracle {
        SqlOracleKind::RowTlp => &case.row_tlp,
        SqlOracleKind::AggregateTlp => &case.aggregate_tlp,
        SqlOracleKind::PredicateRewrite => {
            unreachable!("predicate rewrite uses its two-observation executor")
        }
        SqlOracleKind::JoinRewrite => {
            unreachable!("join rewrite uses its two-observation executor")
        }
    };
    execute_sql_tlp_queries(&snapshot, snapshot_epoch, queries)
}

fn prepare_sql_case(
    case: &SqlFuzzCase,
) -> Result<(DatabaseReadTransaction, u64), SqlExecutionObservation> {
    let mut database = Database::new();
    for mutation in &case.setup {
        if let Err(error) = database.query_sql_with_params(&mutation.sql, &mutation.parameters) {
            return Err(sql_error_observation("setup", error));
        }
    }
    let snapshot_epoch = database.commit_epoch();
    Ok((database.begin_read_transaction(), snapshot_epoch))
}

fn execute_sql_tlp_queries(
    snapshot: &DatabaseReadTransaction,
    snapshot_epoch: u64,
    queries: &SqlTlpCase,
) -> SqlTlpEvidence {
    SqlTlpEvidence {
        original: execute_sql_query(snapshot, snapshot_epoch, &queries.original),
        predicate_true: execute_sql_query(snapshot, snapshot_epoch, &queries.predicate_true),
        predicate_false: execute_sql_query(snapshot, snapshot_epoch, &queries.predicate_false),
        predicate_null: execute_sql_query(snapshot, snapshot_epoch, &queries.predicate_null),
    }
}

fn execute_sql_predicate_rewrite_queries(
    snapshot: &DatabaseReadTransaction,
    snapshot_epoch: u64,
    queries: &SqlPredicateRewriteCase,
) -> SqlPredicateRewriteEvidence {
    SqlPredicateRewriteEvidence {
        original: execute_sql_query(snapshot, snapshot_epoch, &queries.original),
        rewritten: execute_sql_query(snapshot, snapshot_epoch, &queries.rewritten),
    }
}

fn execute_sql_join_rewrite_queries(
    snapshot: &DatabaseReadTransaction,
    snapshot_epoch: u64,
    queries: &SqlJoinRewriteCase,
) -> SqlJoinRewriteEvidence {
    SqlJoinRewriteEvidence {
        optimized: execute_sql_query(snapshot, snapshot_epoch, &queries.optimized),
        syntax_reference: execute_sql_query_with_join_planning(
            snapshot,
            snapshot_epoch,
            &queries.syntax_reference,
            RelationalJoinPlanningDirective::SyntaxOrder,
        ),
    }
}

fn execute_sql_query(
    snapshot: &DatabaseReadTransaction,
    snapshot_epoch: u64,
    query: &SqlQueryInvocation,
) -> SqlExecutionObservation {
    execute_sql_query_with_join_planning(
        snapshot,
        snapshot_epoch,
        query,
        RelationalJoinPlanningDirective::Auto,
    )
}

fn execute_sql_query_with_join_planning(
    snapshot: &DatabaseReadTransaction,
    snapshot_epoch: u64,
    query: &SqlQueryInvocation,
    join_planning: RelationalJoinPlanningDirective,
) -> SqlExecutionObservation {
    let explain_sql = format!("EXPLAIN {}", query.sql);
    let plan = match snapshot.query_sql_with_params_options_with_join_planning(
        &explain_sql,
        &query.parameters,
        QueryStreamOptions::default(),
        join_planning,
    ) {
        Ok(output) => Some(output.rows),
        Err(error) => {
            return SqlExecutionObservation {
                snapshot_epoch: Some(snapshot_epoch),
                plan: None,
                join_planning: None,
                outcome: sql_error_outcome("explain", error),
            };
        }
    };
    match snapshot.query_sql_with_params_options_profiled_with_join_planning(
        &query.sql,
        &query.parameters,
        QueryStreamOptions::default(),
        join_planning,
    ) {
        Ok(profiled) => SqlExecutionObservation {
            snapshot_epoch: Some(snapshot_epoch),
            plan: plan.map(|rows| rows.into_rows()),
            join_planning: Some(Box::new(profiled.profile.join_planning)),
            outcome: ExecutionOutcome::Rows(profiled.output.rows.into_rows()),
        },
        Err(error) => SqlExecutionObservation {
            snapshot_epoch: Some(snapshot_epoch),
            plan: plan.map(|rows| rows.into_rows()),
            join_planning: None,
            outcome: sql_error_outcome("execute", error),
        },
    }
}

fn repeated_evidence(observation: SqlExecutionObservation) -> SqlTlpEvidence {
    SqlTlpEvidence {
        original: observation.clone(),
        predicate_true: observation.clone(),
        predicate_false: observation.clone(),
        predicate_null: observation,
    }
}

fn repeated_predicate_rewrite_evidence(
    observation: SqlExecutionObservation,
) -> SqlPredicateRewriteEvidence {
    SqlPredicateRewriteEvidence {
        original: observation.clone(),
        rewritten: observation,
    }
}

fn repeated_join_rewrite_evidence(observation: SqlExecutionObservation) -> SqlJoinRewriteEvidence {
    SqlJoinRewriteEvidence {
        optimized: observation.clone(),
        syntax_reference: observation,
    }
}

fn classify_sql_row_tlp_failure(evidence: &SqlTlpEvidence) -> Option<DetectedSqlFailure> {
    if let Some(failure) = classify_sql_common_failure(evidence, SqlOracleKind::RowTlp) {
        return Some(failure);
    }
    let ExecutionOutcome::Rows(original) = &evidence.original.outcome else {
        unreachable!("SQL row TLP errors were classified above")
    };
    let mut partitioned = Vec::new();
    for (_, observation) in evidence.observations().into_iter().skip(1) {
        let ExecutionOutcome::Rows(rows) = &observation.outcome else {
            unreachable!("SQL row TLP errors were classified above")
        };
        partitioned.extend_from_slice(rows);
    }
    compare_rows(original, &partitioned, ResultSemantics::Bag)
        .err()
        .map(|reason| DetectedSqlFailure {
            signature: SqlFailureSignature::RowPartitionMismatch,
            reason: format!("SQL TLP partition mismatch: {reason}"),
        })
}

fn classify_sql_aggregate_tlp_failure(evidence: &SqlTlpEvidence) -> Option<DetectedSqlFailure> {
    if let Some(failure) = classify_sql_common_failure(evidence, SqlOracleKind::AggregateTlp) {
        return Some(failure);
    }
    let mut counts = [0_u64; 4];
    for (index, (variant, observation)) in evidence.observations().into_iter().enumerate() {
        let Some(count) = sql_observation_count(observation) else {
            return Some(DetectedSqlFailure {
                signature: SqlFailureSignature::AggregateInvalidResult { variant },
                reason: format!(
                    "SQL TLP aggregate {variant} must return one non-negative integer count"
                ),
            });
        };
        counts[index] = count;
    }
    let Some(partition_count) = counts[1]
        .checked_add(counts[2])
        .and_then(|count| count.checked_add(counts[3]))
    else {
        return Some(DetectedSqlFailure {
            signature: SqlFailureSignature::AggregateOverflow,
            reason: "SQL TLP aggregate partition count overflowed u64".to_string(),
        });
    };
    (counts[0] != partition_count).then(|| DetectedSqlFailure {
        signature: SqlFailureSignature::AggregatePartitionMismatch,
        reason: format!(
            "SQL TLP aggregate mismatch: original_count={} partition_count={partition_count}",
            counts[0]
        ),
    })
}

fn classify_sql_predicate_rewrite_failure(
    evidence: &SqlPredicateRewriteEvidence,
) -> Option<DetectedSqlFailure> {
    if let Some(failure) = classify_sql_pair_common_failure(
        [
            ("original", &evidence.original),
            ("rewritten", &evidence.rewritten),
        ],
        SqlOracleKind::PredicateRewrite,
    ) {
        return Some(failure);
    }
    let ExecutionOutcome::Rows(original) = &evidence.original.outcome else {
        unreachable!("SQL predicate rewrite errors were classified above")
    };
    let ExecutionOutcome::Rows(rewritten) = &evidence.rewritten.outcome else {
        unreachable!("SQL predicate rewrite errors were classified above")
    };
    compare_rows(original, rewritten, ResultSemantics::Bag)
        .err()
        .map(|reason| DetectedSqlFailure {
            signature: SqlFailureSignature::PredicateRewriteMismatch,
            reason: format!("SQL predicate rewrite mismatch: {reason}"),
        })
}

fn classify_sql_join_rewrite_failure(
    case: &SqlJoinRewriteCase,
    evidence: &SqlJoinRewriteEvidence,
) -> Option<DetectedSqlFailure> {
    if let Some(failure) = classify_sql_pair_common_failure(
        [
            ("optimized", &evidence.optimized),
            ("syntax_reference", &evidence.syntax_reference),
        ],
        SqlOracleKind::JoinRewrite,
    ) {
        return Some(failure);
    }
    let optimized = evidence.optimized.join_planning.as_deref();
    if !optimized.is_some_and(|outcome| {
        outcome.strategy == case.expected_strategy
            && outcome.status == RelationalJoinPlanningStatus::Selected
            && outcome.memo_groups.is_some()
            && outcome.memo_expressions.is_some()
            && outcome.cost.is_some()
    }) {
        return Some(join_planning_failure(
            "optimized",
            format!(
                "expected selected {} plan with memo and cost evidence, got {optimized:?}",
                case.expected_strategy.as_str()
            ),
        ));
    }
    let syntax_reference = evidence.syntax_reference.join_planning.as_deref();
    if !syntax_reference.is_some_and(|outcome| {
        outcome.strategy == RelationalJoinPlanningStrategy::SyntaxOrder
            && outcome.status == RelationalJoinPlanningStatus::Selected
            && outcome.reason == RelationalJoinPlanningReason::ExplicitSyntaxOrder
            && outcome.cost.is_none()
            && matches!(
                outcome.attempts.as_slice(),
                [attempt]
                    if attempt.strategy == RelationalJoinPlanningStrategy::SyntaxOrder
                        && attempt.status == RelationalJoinPlanningStatus::Selected
                        && attempt.reason == RelationalJoinPlanningReason::ExplicitSyntaxOrder
                        && attempt.cost.is_none()
            )
    }) {
        return Some(join_planning_failure(
            "syntax_reference",
            format!("expected explicit syntax-order reference, got {syntax_reference:?}"),
        ));
    }
    let ExecutionOutcome::Rows(optimized) = &evidence.optimized.outcome else {
        unreachable!("SQL join rewrite errors were classified above")
    };
    let ExecutionOutcome::Rows(syntax_reference) = &evidence.syntax_reference.outcome else {
        unreachable!("SQL join rewrite errors were classified above")
    };
    compare_rows(optimized, syntax_reference, ResultSemantics::Bag)
        .err()
        .map(|reason| DetectedSqlFailure {
            signature: SqlFailureSignature::JoinRewriteResultMismatch,
            reason: format!("SQL join rewrite result mismatch: {reason}"),
        })
}

fn join_planning_failure(variant: &'static str, reason: String) -> DetectedSqlFailure {
    DetectedSqlFailure {
        signature: SqlFailureSignature::JoinRewritePlanningMismatch { variant },
        reason: format!("SQL join rewrite {variant} planning mismatch: {reason}"),
    }
}

fn classify_sql_pair_common_failure(
    observations: [(&'static str, &SqlExecutionObservation); 2],
    oracle: SqlOracleKind,
) -> Option<DetectedSqlFailure> {
    for (variant, observation) in observations {
        if let ExecutionOutcome::Error { phase, class, .. } = &observation.outcome {
            return Some(DetectedSqlFailure {
                signature: SqlFailureSignature::Errored {
                    oracle,
                    variant,
                    phase,
                    class,
                },
                reason: format!("{} {variant} failed in {phase}/{class}", oracle.as_str()),
            });
        }
    }
    if observations[0].1.snapshot_epoch.is_none()
        || observations[0].1.snapshot_epoch != observations[1].1.snapshot_epoch
    {
        return Some(DetectedSqlFailure {
            signature: SqlFailureSignature::SnapshotMismatch { oracle },
            reason: format!(
                "{} variants did not use one pinned snapshot",
                oracle.as_str()
            ),
        });
    }
    None
}

fn classify_sql_common_failure(
    evidence: &SqlTlpEvidence,
    oracle: SqlOracleKind,
) -> Option<DetectedSqlFailure> {
    let observations = evidence.observations();
    for (variant, observation) in observations {
        if let ExecutionOutcome::Error { phase, class, .. } = &observation.outcome {
            let phase = *phase;
            let class = *class;
            return Some(DetectedSqlFailure {
                signature: SqlFailureSignature::Errored {
                    oracle,
                    variant,
                    phase,
                    class,
                },
                reason: format!("{} {variant} failed in {phase}/{class}", oracle.as_str()),
            });
        }
    }
    let snapshot_epoch = evidence.original.snapshot_epoch;
    if snapshot_epoch.is_none()
        || observations
            .iter()
            .any(|(_, observation)| observation.snapshot_epoch != snapshot_epoch)
    {
        return Some(DetectedSqlFailure {
            signature: SqlFailureSignature::SnapshotMismatch { oracle },
            reason: format!(
                "{} variants did not use one pinned snapshot",
                oracle.as_str()
            ),
        });
    }
    None
}

fn sql_observation_count(observation: &SqlExecutionObservation) -> Option<u64> {
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

fn reduce_sql_failure(
    case: &SqlFuzzCase,
    oracle: SqlOracleKind,
    expected: &SqlFailureSignature,
) -> SqlReductionReport {
    let original_setup_count = case.setup.len();
    let mut reduced = case.clone();
    let mut attempts = 0;

    loop {
        let mut accepted = None;
        for index in 0..reduced.setup.len() {
            if attempts >= MAX_SQL_REDUCTION_ATTEMPTS {
                break;
            }
            if !reduced.setup[index].reducible {
                continue;
            }
            let mut candidate = reduced.clone();
            candidate.setup.remove(index);
            candidate.index_enabled = candidate
                .setup
                .iter()
                .any(|mutation| mutation.sql.starts_with("CREATE INDEX"));
            attempts += 1;
            if sql_failure_signature(&candidate, oracle).as_ref() == Some(expected) {
                accepted = Some(candidate);
                break;
            }
        }
        let Some(candidate) = accepted else {
            break;
        };
        reduced = candidate;
    }

    SqlReductionReport {
        oracle: oracle.as_str(),
        original_setup_count,
        reduced_setup_count: reduced.setup.len(),
        attempts,
        replay: SqlReplayBundle::from_case(&reduced),
    }
}

fn sql_failure_signature(case: &SqlFuzzCase, oracle: SqlOracleKind) -> Option<SqlFailureSignature> {
    match oracle {
        SqlOracleKind::RowTlp => classify_sql_row_tlp_failure(&execute_sql_oracle(case, oracle)),
        SqlOracleKind::AggregateTlp => {
            classify_sql_aggregate_tlp_failure(&execute_sql_oracle(case, oracle))
        }
        SqlOracleKind::PredicateRewrite => {
            let evidence = match prepare_sql_case(case) {
                Ok((snapshot, snapshot_epoch)) => execute_sql_predicate_rewrite_queries(
                    &snapshot,
                    snapshot_epoch,
                    &case.predicate_rewrite,
                ),
                Err(observation) => repeated_predicate_rewrite_evidence(observation),
            };
            classify_sql_predicate_rewrite_failure(&evidence)
        }
        SqlOracleKind::JoinRewrite => {
            let evidence = match prepare_sql_case(case) {
                Ok((snapshot, snapshot_epoch)) => {
                    execute_sql_join_rewrite_queries(&snapshot, snapshot_epoch, &case.join_rewrite)
                }
                Err(observation) => repeated_join_rewrite_evidence(observation),
            };
            classify_sql_join_rewrite_failure(&case.join_rewrite, &evidence)
        }
    }
    .map(|failure| failure.signature)
}

fn sql_error_observation(phase: &'static str, error: HawDBError) -> SqlExecutionObservation {
    SqlExecutionObservation {
        snapshot_epoch: None,
        plan: None,
        join_planning: None,
        outcome: sql_error_outcome(phase, error),
    }
}

fn sql_error_outcome(phase: &'static str, error: HawDBError) -> ExecutionOutcome {
    ExecutionOutcome::Error {
        phase,
        class: error_class(&error),
        message: error.to_string(),
    }
}

fn values_json(values: &[Value]) -> Vec<JsonValue> {
    values.iter().map(typed_value_json).collect()
}

fn sql_plan_signature(rows: &[hawdb::executor::Row]) -> String {
    let mut rows = rows.iter().map(row_json).collect::<Vec<_>>();
    for row in &mut rows {
        let Some(JsonValue::String(operator_info)) = row
            .get_mut("operator info")
            .and_then(JsonValue::as_object_mut)
            .and_then(|value| value.get_mut("value"))
        else {
            continue;
        };
        *operator_info = normalize_sql_plan_operator_info(operator_info);
    }

    serde_json::to_string(&rows).expect("SQL EXPLAIN rows contain JSON-serializable values")
}

fn normalize_sql_plan_operator_info(operator_info: &str) -> String {
    const VOLATILE_FIELDS: [&str; 4] = ["parse_nanos", "bind_nanos", "plan_nanos", "execute_nanos"];

    operator_info
        .split(", ")
        .map(|field| {
            VOLATILE_FIELDS
                .iter()
                .find_map(|name| {
                    field
                        .strip_prefix(name)
                        .and_then(|value| value.strip_prefix('='))
                        .map(|_| format!("{name}=<volatile>"))
                })
                .unwrap_or_else(|| field.to_string())
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn join_planning_json(outcome: &RelationalJoinPlanningOutcome) -> JsonValue {
    json!({
        "strategy": outcome.strategy.as_str(),
        "status": outcome.status.as_str(),
        "reason": outcome.reason.as_str(),
        "memo_groups": outcome.memo_groups,
        "memo_expressions": outcome.memo_expressions,
        "budget": {
            "max_groups": outcome.budget.max_groups,
            "max_expressions": outcome.budget.max_expressions,
        },
        "selected_order": outcome.selected_order,
        "attempts": outcome.attempts.iter().map(|attempt| json!({
            "strategy": attempt.strategy.as_str(),
            "status": attempt.status.as_str(),
            "reason": attempt.reason.as_str(),
            "fallback_class": attempt.fallback_class.map(|class| class.as_str()),
            "memo_groups": attempt.memo_groups,
            "memo_expressions": attempt.memo_expressions,
            "cost": attempt.cost.map(|cost| json!({
                "estimated_rows": cost.estimated_rows,
                "cost": cost.cost,
                "cpu": cost.cpu,
                "random_io": cost.random_io,
                "sequential_io": cost.sequential_io,
                "output_rows": cost.output_rows,
            })),
        })).collect::<Vec<_>>(),
        "cost": outcome.cost.map(|cost| json!({
            "estimated_rows": cost.estimated_rows,
            "cost": cost.cost,
            "cpu": cost.cpu,
            "random_io": cost.random_io,
            "sequential_io": cost.sequential_io,
            "output_rows": cost.output_rows,
        })),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_plan_signature_normalizes_stage_timings() {
        let normalized = normalize_sql_plan_operator_info(
            "planning_strategy=csg_cmp_memo, parse_nanos=123, bind_nanos=456, \
             plan_nanos=789, execute_nanos=10, memo_groups=6",
        );

        assert_eq!(
            normalized,
            "planning_strategy=csg_cmp_memo, parse_nanos=<volatile>, \
             bind_nanos=<volatile>, plan_nanos=<volatile>, execute_nanos=<volatile>, \
             memo_groups=6"
        );
    }

    #[test]
    fn generated_sql_cases_cover_all_shapes_and_oracles() {
        let mut observed = std::collections::BTreeSet::new();
        let mut observed_rewrite_pairs = std::collections::BTreeSet::new();
        let mut observed_join_rewrites = std::collections::BTreeSet::new();
        for index in
            0..SQL_QUERY_SHAPE_COUNT * crate::predicate_rewrite::PREDICATE_REWRITE_SHAPES.len()
        {
            let case = generate_sql_case(17 + index as u64, index, index.is_multiple_of(2));
            observed.insert(case.shape.clone());
            observed_rewrite_pairs
                .insert((case.shape.clone(), case.predicate_rewrite.name.clone()));
            observed_join_rewrites.insert(case.join_rewrite.name.clone());
            let (row, aggregate, predicate_rewrite, join_rewrite) = execute_sql_case(&case);
            assert!(
                classify_sql_row_tlp_failure(&row).is_none(),
                "{}",
                SqlReplayBundle::from_case(&case).json()
            );
            assert!(
                classify_sql_aggregate_tlp_failure(&aggregate).is_none(),
                "{}",
                SqlReplayBundle::from_case(&case).json()
            );
            assert!(
                classify_sql_predicate_rewrite_failure(&predicate_rewrite).is_none(),
                "{}",
                SqlReplayBundle::from_case(&case).json()
            );
            assert!(row
                .observations()
                .iter()
                .all(|(_, observation)| observation.plan.is_some()));
            assert!(aggregate
                .observations()
                .iter()
                .all(|(_, observation)| observation.plan.is_some()));
            assert!(predicate_rewrite.original.plan.is_some());
            assert!(predicate_rewrite.rewritten.plan.is_some());
            let join_failure = classify_sql_join_rewrite_failure(&case.join_rewrite, &join_rewrite);
            assert!(
                join_failure.is_none(),
                "{join_failure:?}\n{}",
                SqlReplayBundle::from_case(&case).json()
            );
            assert!(join_rewrite.optimized.plan.is_some());
            assert!(join_rewrite.syntax_reference.plan.is_some());
            let ExecutionOutcome::Rows(null_rows) = &row.predicate_null.outcome else {
                panic!("{} null partition did not return rows", case.shape);
            };
            assert!(
                !null_rows.is_empty(),
                "{} null partition was empty",
                case.shape
            );
            assert!(
                sql_observation_count(&aggregate.predicate_null).is_some_and(|count| count > 0),
                "{} aggregate null partition was empty",
                case.shape
            );
        }
        assert_eq!(observed.len(), SQL_QUERY_SHAPE_COUNT);
        assert_eq!(
            observed_rewrite_pairs.len(),
            SQL_QUERY_SHAPE_COUNT * crate::predicate_rewrite::PREDICATE_REWRITE_SHAPES.len()
        );
        assert_eq!(observed_join_rewrites.len(), SQL_JOIN_REWRITE_SHAPE_COUNT);
    }

    #[test]
    fn join_generator_covers_costing_dimensions() {
        let mut relation_counts = std::collections::BTreeSet::new();
        let mut join_graphs = std::collections::BTreeSet::new();
        let mut null_rejections = std::collections::BTreeSet::new();
        let mut selectivities = std::collections::BTreeSet::new();
        let mut index_profiles = std::collections::BTreeSet::new();
        let mut statistics_profiles = std::collections::BTreeSet::new();

        for index in 0..SQL_JOIN_REWRITE_SHAPE_COUNT * 8 {
            let case = generate_sql_case(211 + index as u64, index, index.is_multiple_of(2));
            let profile = case.join_rewrite.generator_profile;
            relation_counts.insert(profile.relation_count);
            join_graphs.insert(profile.join_graph);
            null_rejections.insert(profile.null_rejection);
            selectivities.insert(profile.selectivity);
            index_profiles.insert(profile.index_profile);
            statistics_profiles.insert(profile.statistics_profile);
        }

        assert_eq!(relation_counts, [3, 4, 5, 6].into_iter().collect());
        assert_eq!(
            join_graphs,
            ["chain", "cycle", "reverse_chain", "star", "tree"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert_eq!(
            null_rejections,
            ["none", "preserved", "rejected"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert_eq!(
            selectivities,
            ["broad", "medium", "rare"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert_eq!(
            index_profiles,
            ["composite", "filter", "join_keys", "none"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert_eq!(
            statistics_profiles,
            ["compact", "skewed", "wide"]
                .into_iter()
                .map(str::to_string)
                .collect()
        );
    }

    #[test]
    fn relational_join_rewrite_differential_campaign_covers_every_shape() {
        let mut observed = std::collections::BTreeSet::new();
        let mut selected_orders = std::collections::BTreeSet::new();
        let mut plan_signatures = std::collections::BTreeSet::new();
        let mut memo_profiles = std::collections::BTreeSet::new();
        let mut reordered = false;
        for index in 0..SQL_JOIN_REWRITE_SHAPE_COUNT * 8 {
            let case = generate_sql_case(101 + index as u64, index, index.is_multiple_of(2));
            let (snapshot, snapshot_epoch) = prepare_sql_case(&case).unwrap();
            let evidence =
                execute_sql_join_rewrite_queries(&snapshot, snapshot_epoch, &case.join_rewrite);
            let failure = classify_sql_join_rewrite_failure(&case.join_rewrite, &evidence);
            assert!(
                failure.is_none(),
                "{failure:?}\n{}",
                SqlReplayBundle::from_case(&case).json()
            );
            observed.insert(case.join_rewrite.name);
            let planning = evidence.optimized.join_planning.as_deref().unwrap();
            selected_orders.insert(planning.selected_order.clone());
            memo_profiles.insert((planning.memo_groups, planning.memo_expressions));
            reordered |= planning.join_order_reordered();
            plan_signatures.insert(sql_plan_signature(
                evidence.optimized.plan.as_deref().unwrap(),
            ));
        }
        assert_eq!(
            observed,
            SQL_JOIN_REWRITE_SHAPES
                .into_iter()
                .map(str::to_string)
                .collect()
        );
        assert!(reordered);
        assert!(selected_orders.len() >= 6, "{selected_orders:?}");
        assert!(memo_profiles.len() >= 4, "{memo_profiles:?}");
        assert!(plan_signatures.len() >= 8, "{plan_signatures:?}");
    }

    #[test]
    fn grace_hash_join_matches_the_in_memory_reference_across_skewed_cases() {
        const QUERY: &str = "SELECT l.id AS left_id, r.id AS right_id \
            FROM fuzz_hash_left AS l \
            LEFT JOIN fuzz_hash_right AS r \
            ON r.join_key = l.join_key AND r.tag = l.tag AND r.keep = true \
            WHERE l.tenant = 1";

        for case_index in 0..16u64 {
            let mut config = DatabaseConfig::default();
            let spill_directory = std::env::temp_dir().join(format!(
                "hawdb-fuzz-grace-hash-{}-{}-{}",
                std::process::id(),
                case_index,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_nanos()
            ));
            config.execution_memory = hawdb::executor::ExecutionMemoryConfig {
                blocking_operator_bytes: std::num::NonZeroUsize::new(512)
                    .expect("non-zero blocking budget"),
                max_spill_bytes: std::num::NonZeroU64::new(2 * 1024 * 1024)
                    .expect("non-zero spill budget"),
                max_spill_runs: std::num::NonZeroUsize::new(4).expect("non-zero spill run budget"),
                min_spill_free_bytes: std::num::NonZeroU64::MIN,
                spill_directory: spill_directory.clone(),
                ..hawdb::executor::ExecutionMemoryConfig::default()
            };
            let mut in_memory = Database::new();
            let mut grace = Database::new_with_config(config);
            for database in [&mut in_memory, &mut grace] {
                for statement in [
                    "CREATE TABLE fuzz_hash_left (id BIGINT PRIMARY KEY, tenant BIGINT NOT NULL, join_key BIGINT, tag BIGINT NOT NULL)",
                    "CREATE TABLE fuzz_hash_right (id BIGINT PRIMARY KEY, join_key BIGINT, tag BIGINT NOT NULL, keep BOOLEAN NOT NULL)",
                    "CREATE INDEX fuzz_hash_left_tenant ON fuzz_hash_left (tenant)",
                ] {
                    database.query_sql(statement).expect("create hash fuzz fixture");
                }
                let mut seed = 0x9e37_79b9_7f4a_7c15u64 ^ case_index;
                let next = |seed: &mut u64| {
                    *seed ^= *seed << 13;
                    *seed ^= *seed >> 7;
                    *seed ^= *seed << 17;
                    *seed
                };
                let left_rows = (0..24u64)
                    .map(|id| {
                        let join_key = if next(&mut seed).is_multiple_of(7) {
                            "NULL".to_string()
                        } else {
                            ((next(&mut seed) % 5) as i64).to_string()
                        };
                        format!(
                            "({id}, {}, {join_key}, {})",
                            if id.is_multiple_of(3) { 1 } else { 2 },
                            next(&mut seed) % 3
                        )
                    })
                    .collect::<Vec<_>>();
                database
                    .query_sql(&format!(
                        "INSERT INTO fuzz_hash_left (id, tenant, join_key, tag) VALUES {}",
                        left_rows.join(", ")
                    ))
                    .expect("insert left hash fuzz rows");
                let right_rows = (0..48u64)
                    .map(|id| {
                        let join_key = if id.is_multiple_of(13) {
                            "NULL".to_string()
                        } else if !id.is_multiple_of(5) {
                            "1".to_string()
                        } else {
                            ((next(&mut seed) % 5) as i64).to_string()
                        };
                        format!(
                            "({id}, {join_key}, {}, {})",
                            next(&mut seed) % 3,
                            if next(&mut seed).is_multiple_of(4) {
                                "false"
                            } else {
                                "true"
                            }
                        )
                    })
                    .collect::<Vec<_>>();
                database
                    .query_sql(&format!(
                        "INSERT INTO fuzz_hash_right (id, join_key, tag, keep) VALUES {}",
                        right_rows.join(", ")
                    ))
                    .expect("insert right hash fuzz rows");
            }

            let normalize = |database: &mut Database| {
                let mut rows = database
                    .query_sql(QUERY)
                    .expect("execute hash join fuzz query")
                    .rows
                    .iter()
                    .map(|row| (row["left_id"].clone(), row["right_id"].clone()))
                    .collect::<Vec<_>>();
                rows.sort();
                rows
            };
            assert_eq!(
                normalize(&mut grace),
                normalize(&mut in_memory),
                "case {case_index}"
            );
            drop(grace);
            let _ = std::fs::remove_dir_all(spill_directory);
        }
    }

    #[test]
    fn sql_join_rewrite_detects_and_reduces_invalid_reference() {
        let mut case = generate_sql_case(41, 0, true);
        case.join_rewrite
            .syntax_reference
            .sql
            .push_str(" AND r.id = -1");
        let (snapshot, snapshot_epoch) = prepare_sql_case(&case).unwrap();
        let evidence =
            execute_sql_join_rewrite_queries(&snapshot, snapshot_epoch, &case.join_rewrite);
        let failure = classify_sql_join_rewrite_failure(&case.join_rewrite, &evidence).unwrap();

        assert_eq!(
            failure.signature,
            SqlFailureSignature::JoinRewriteResultMismatch
        );
        let reduction = reduce_sql_failure(&case, SqlOracleKind::JoinRewrite, &failure.signature);
        assert!(reduction.reduced_setup_count < reduction.original_setup_count);
        assert_eq!(reduction.oracle, "sql_join_rewrite");
    }

    #[test]
    fn sql_predicate_rewrite_detects_and_reduces_invalid_relation() {
        let mut case = generate_sql_case(29, 0, true);
        case.predicate_rewrite.rewritten = case.row_tlp.original.clone();
        let (snapshot, snapshot_epoch) = prepare_sql_case(&case).unwrap();
        let evidence = execute_sql_predicate_rewrite_queries(
            &snapshot,
            snapshot_epoch,
            &case.predicate_rewrite,
        );
        let failure = classify_sql_predicate_rewrite_failure(&evidence).unwrap();

        assert_eq!(
            failure.signature,
            SqlFailureSignature::PredicateRewriteMismatch
        );
        let reduction =
            reduce_sql_failure(&case, SqlOracleKind::PredicateRewrite, &failure.signature);
        assert!(reduction.reduced_setup_count < reduction.original_setup_count);
        assert_eq!(reduction.oracle, "sql_predicate_rewrite");
    }

    #[test]
    fn sql_tlp_detects_and_reduces_invalid_partition() {
        let mut case = generate_sql_case(17, 0, true);
        case.row_tlp.predicate_null = case.row_tlp.original.clone();
        let evidence = execute_sql_oracle(&case, SqlOracleKind::RowTlp);
        let failure = classify_sql_row_tlp_failure(&evidence).unwrap();

        assert_eq!(failure.signature, SqlFailureSignature::RowPartitionMismatch);
        let reduction = reduce_sql_failure(&case, SqlOracleKind::RowTlp, &failure.signature);
        assert!(reduction.reduced_setup_count < reduction.original_setup_count);
        assert_eq!(reduction.oracle, "sql_tlp");
    }

    #[test]
    fn sql_aggregate_tlp_detects_and_reduces_invalid_partition() {
        let mut case = generate_sql_case(19, 3, false);
        case.aggregate_tlp.predicate_null = case.aggregate_tlp.original.clone();
        let evidence = execute_sql_oracle(&case, SqlOracleKind::AggregateTlp);
        let failure = classify_sql_aggregate_tlp_failure(&evidence).unwrap();

        assert_eq!(
            failure.signature,
            SqlFailureSignature::AggregatePartitionMismatch
        );
        let reduction = reduce_sql_failure(&case, SqlOracleKind::AggregateTlp, &failure.signature);
        assert!(reduction.reduced_setup_count < reduction.original_setup_count);
        assert_eq!(reduction.oracle, "sql_tlp_aggregate");
    }

    #[test]
    fn sql_setup_errors_fail_closed_with_replay() {
        let mut case = generate_sql_case(23, 1, false);
        case.setup
            .push(SqlMutation::data("INSERT invalid", Vec::new()));
        let evidence = execute_sql_oracle(&case, SqlOracleKind::RowTlp);
        let failure = classify_sql_row_tlp_failure(&evidence).unwrap();
        assert!(matches!(
            failure.signature,
            SqlFailureSignature::Errored {
                phase: "setup",
                class: "parse",
                ..
            }
        ));
        let report = failure_report(&case, SqlOracleKind::RowTlp, failure, evidence);
        assert_eq!(report.replay.setup.last().unwrap().sql, "INSERT invalid");
        assert_eq!(
            report.reduction.replay.setup.last().unwrap().sql,
            "INSERT invalid"
        );
    }
}
