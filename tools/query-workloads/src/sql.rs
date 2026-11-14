// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::{typed_value_json, ResultSemantics};
use hawdb_core::Value;
use serde_json::{json, Value as JsonValue};
use std::collections::BTreeMap;
mod generator;
pub use generator::generate_sql_case;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedJoinStrategy {
    CsgCmpMemo,
}
impl ExpectedJoinStrategy {
    pub const fn as_str(self) -> &'static str {
        "csg_cmp_memo"
    }
}

pub const SQL_QUERY_SHAPES: [&str; 8] = [
    "nullable_score_range",
    "nullable_tag_equality",
    "inner_join_nullable_priority",
    "left_join_nullable_priority",
    "nullable_score_in_list",
    "nullable_column_comparison",
    "nullable_conjunction",
    "nullable_disjunction",
];
pub const SQL_QUERY_SHAPE_COUNT: usize = SQL_QUERY_SHAPES.len();
pub const SQL_JOIN_REWRITE_SHAPES: [&str; 12] = [
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
pub const SQL_JOIN_REWRITE_SHAPE_COUNT: usize = SQL_JOIN_REWRITE_SHAPES.len();

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlMutation {
    pub sql: String,
    pub parameters: Vec<Value>,
    pub reducible: bool,
}

impl SqlMutation {
    pub fn required(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
            reducible: false,
        }
    }

    pub fn data(sql: impl Into<String>, parameters: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            parameters,
            reducible: true,
        }
    }

    pub fn index(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            parameters: Vec::new(),
            reducible: true,
        }
    }

    pub fn json(&self) -> JsonValue {
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
    pub fn json(&self) -> JsonValue {
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
    pub fn json(&self) -> JsonValue {
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
    pub fn json(&self) -> JsonValue {
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
    pub expected_strategy: ExpectedJoinStrategy,
    pub generator_profile: SqlJoinGeneratorProfile,
}

impl SqlJoinRewriteCase {
    pub fn json(&self) -> JsonValue {
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
    pub fn json(&self) -> JsonValue {
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
pub struct SqlFuzzCase {
    pub seed: u64,
    pub shape: String,
    pub setup: Vec<SqlMutation>,
    pub row_tlp: SqlTlpCase,
    pub aggregate_tlp: SqlTlpCase,
    pub predicate_rewrite: SqlPredicateRewriteCase,
    pub join_rewrite: SqlJoinRewriteCase,
    pub norec: SqlPredicateRewriteCase,
    pub index_enabled: bool,
}

fn values_json(values: &[Value]) -> Vec<JsonValue> {
    values.iter().map(typed_value_json).collect()
}
