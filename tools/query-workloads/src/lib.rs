// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Deterministic developer workloads with no dependency on database execution.

use hawdb_core::Value;
use query_ast::QueryAst;
use std::collections::BTreeMap;
pub mod cardinality;
pub mod compressed_statistics;
pub mod generator;
pub mod predicate_rewrite;
pub mod query_ast;
pub mod sql;
mod value;
pub use value::typed_value_json;

pub const QUERY_SHAPE_COUNT: usize = 13;

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
    pub query_ast: QueryAst,
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

pub mod smith;
