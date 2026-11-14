// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Bounded, typed SQL generation for fixed-fixture experiments. Generation is
//! independent of the engine, execution results, and cost coefficients.

use crate::generator::DeterministicRng;
use hawdb_core::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const PROTOCOL: &str = "hawdb-query-workloads-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Template,
    Recursive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_nodes: usize,
    pub max_depth: usize,
    pub max_relations: usize,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_nodes: 96,
            max_depth: 6,
            max_relations: 3,
            max_output_rows: 64,
            max_output_bytes: 262_144,
        }
    }
}
impl Limits {
    pub fn cost_baseline() -> Self {
        Self {
            max_nodes: 32,
            max_depth: 4,
            max_relations: 2,
            max_output_rows: 16,
            max_output_bytes: 65_536,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !(24..=512).contains(&self.max_nodes)
            || !(3..=8).contains(&self.max_depth)
            || !(1..=4).contains(&self.max_relations)
            || !(1..=1024).contains(&self.max_output_rows)
            || !(1024..=4_194_304).contains(&self.max_output_bytes)
        {
            return Err("generation limits outside the supported bounded domain".into());
        }
        Ok(())
    }
}

/// The supported scalar subset has an exact, tagged replay representation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Scalar {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
}
impl Scalar {
    pub fn value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Int(v) => Value::Int(*v),
            Self::Text(v) => Value::String(v.clone()),
            Self::Bool(v) => Value::Bool(*v),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Type {
    Int,
    Text,
    Bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum Expr {
    Column {
        relation: usize,
        name: String,
        ty: Type,
    },
    Parameter {
        value: Scalar,
        ty: Type,
    },
    Binary {
        op: String,
        left: Box<Expr>,
        right: Box<Expr>,
        ty: Type,
    },
    IsNull(Box<Expr>),
    Not(Box<Expr>),
}
impl Expr {
    fn column(relation: usize, name: &str, ty: Type) -> Self {
        Self::Column {
            relation,
            name: name.into(),
            ty,
        }
    }
    fn parameter(value: Scalar, ty: Type) -> Self {
        Self::Parameter { value, ty }
    }
    fn binary(op: &str, left: Self, right: Self, ty: Type) -> Self {
        Self::Binary {
            op: op.into(),
            left: Box::new(left),
            right: Box::new(right),
            ty,
        }
    }
    fn metrics(&self) -> (usize, usize) {
        match self {
            Self::Column { .. } | Self::Parameter { .. } => (1, 1),
            Self::Binary { left, right, .. } => {
                let (ln, ld) = left.metrics();
                let (rn, rd) = right.metrics();
                (1 + ln + rn, 1 + ld.max(rd))
            }
            Self::IsNull(e) | Self::Not(e) => {
                let (n, d) = e.metrics();
                (n + 1, d + 1)
            }
        }
    }
    fn render(&self, params: &mut Vec<Scalar>, features: &mut BTreeSet<String>) -> String {
        match self {
            Self::Column { relation, name, .. } => format!("t{relation}.{name}"),
            Self::Parameter { value, .. } => {
                params.push(value.clone());
                format!("${}", params.len())
            }
            Self::Binary {
                op, left, right, ..
            } => {
                features.insert(format!("expr:{op}"));
                format!(
                    "({} {op} {})",
                    left.render(params, features),
                    right.render(params, features)
                )
            }
            Self::IsNull(e) => {
                features.insert("expr:is_null".into());
                format!("({} IS NULL)", e.render(params, features))
            }
            Self::Not(e) => {
                features.insert("expr:not".into());
                format!("(NOT {})", e.render(params, features))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    pub sql: String,
    pub parameters: Vec<Scalar>,
}
impl Statement {
    pub fn values(&self) -> Vec<Value> {
        self.parameters.iter().map(Scalar::value).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub index: usize,
    pub template: String,
    pub mode: Mode,
    pub statement: Statement,
    pub ordered: bool,
    pub nodes: usize,
    pub depth: usize,
    pub relations: usize,
    pub features: BTreeSet<String>,
    pub limit: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    pub rows: usize,
    pub setup: Vec<Statement>,
}

/// Both generation modes use exactly these bytes and values. Joins are bounded
/// many-to-one lookups, while NULLs and duplicate non-key values remain present.
pub fn fixture(rows: usize) -> Result<Fixture, String> {
    if !(8..=100_000).contains(&rows) {
        return Err("fixture rows must be in 8..=100000".into());
    }
    let plain = |sql: &str| Statement {
        sql: sql.into(),
        parameters: vec![],
    };
    let mut setup = vec![
        plain("CREATE TABLE work_items (id BIGINT PRIMARY KEY, bucket BIGINT NOT NULL, score BIGINT, label TEXT)"),
        plain("CREATE TABLE work_dims (id BIGINT PRIMARY KEY, score BIGINT, label TEXT)"),
    ];
    // Bucket seven has no dimension, exercising NULL extension in LEFT JOIN.
    for id in 0..7i64 {
        setup.push(Statement {
            sql: "INSERT INTO work_dims VALUES ($1, $2, $3)".into(),
            parameters: vec![
                Scalar::Int(id),
                Scalar::Int(id - 3),
                Scalar::Text(format!("label-{}", id % 3)),
            ],
        });
    }
    for id in 0..rows as i64 {
        setup.push(Statement {
            sql: "INSERT INTO work_items VALUES ($1, $2, $3, $4)".into(),
            parameters: vec![
                Scalar::Int(id),
                Scalar::Int(id % 8),
                if id % 5 == 0 {
                    Scalar::Null
                } else {
                    Scalar::Int(id % 31 - 15)
                },
                if id % 7 == 0 {
                    Scalar::Null
                } else {
                    Scalar::Text(format!("label-{}", id % 3))
                },
            ],
        });
    }
    setup.push(plain("CREATE INDEX work_score ON work_items (score)"));
    setup.push(plain("CREATE INDEX work_bucket ON work_items (bucket, id)"));
    Ok(Fixture { rows, setup })
}

struct Generator {
    rng: DeterministicRng,
    relations: usize,
}
impl Generator {
    fn choose(&mut self, n: usize) -> usize {
        (self.rng.next_u64() % n as u64) as usize
    }
    fn scalar(&mut self, ty: Type, nodes: usize, depth: usize) -> Expr {
        if ty == Type::Int && nodes >= 3 && depth > 1 && self.choose(3) == 0 {
            let left = self.scalar(ty, (nodes - 1) / 2, depth - 1);
            let right = self.scalar(ty, nodes / 2, depth - 1);
            return Expr::binary(if self.choose(2) == 0 { "+" } else { "-" }, left, right, ty);
        }
        if self.choose(3) != 0 {
            let relation = self.choose(self.relations);
            Expr::column(
                relation,
                if ty == Type::Int { "score" } else { "label" },
                ty,
            )
        } else {
            Expr::parameter(
                match ty {
                    Type::Int => Scalar::Int(self.choose(31) as i64 - 15),
                    Type::Text => Scalar::Text(format!("label-{}", self.choose(3))),
                    Type::Bool => Scalar::Bool(self.choose(2) == 0),
                },
                ty,
            )
        }
    }
    fn predicate(&mut self, nodes: usize, depth: usize) -> Expr {
        if nodes >= 7 && depth >= 3 {
            match self.choose(4) {
                0 | 1 => {
                    let left = self.predicate((nodes - 1) / 2, depth - 1);
                    let right = self.predicate(nodes / 2, depth - 1);
                    return Expr::binary(
                        if self.choose(2) == 0 { "AND" } else { "OR" },
                        left,
                        right,
                        Type::Bool,
                    );
                }
                2 => return Expr::Not(Box::new(self.predicate(nodes - 1, depth - 1))),
                _ => {}
            }
        }
        let ty = if self.choose(3) == 0 {
            Type::Text
        } else {
            Type::Int
        };
        if self.choose(5) == 0 {
            return Expr::IsNull(Box::new(self.scalar(ty, nodes - 1, depth - 1)));
        }
        let left = self.scalar(ty, (nodes - 1) / 2, depth - 1);
        let right = self.scalar(ty, nodes / 2, depth - 1);
        Expr::binary(
            if ty == Type::Int {
                ["=", "<>", ">=", "<"][self.choose(4)]
            } else {
                ["=", "<>"][self.choose(2)]
            },
            left,
            right,
            Type::Bool,
        )
    }
}

pub fn generate(seed: u64, index: usize, mode: Mode, limits: &Limits) -> Result<Case, String> {
    limits.validate()?;
    if index >= 10_000 {
        return Err("case index exceeds 9999".into());
    }
    let mut generator = Generator {
        rng: DeterministicRng::new(seed ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)),
        relations: 1,
    };
    let aggregate = index % 6 == 3 || index % 6 == 4;
    let joined =
        matches!(index % 6, 2 | 4 | 5) && limits.max_relations > 1 && limits.max_depth >= 4;
    generator.relations = if joined {
        2 + generator.choose(limits.max_relations - 1)
    } else {
        1
    };
    // Count the query root, FROM relations, join predicates, projection/group,
    // ORDER BY and LIMIT/OFFSET nodes, as well as every expression node.
    let fixed_nodes =
        1 + generator.relations + (generator.relations - 1) * 4 + if aggregate { 7 } else { 4 } + 3;
    let available = limits
        .max_nodes
        .checked_sub(fixed_nodes)
        .filter(|n| *n >= 3)
        .ok_or("node budget cannot represent the requested query shape")?;
    let predicate = match mode {
        Mode::Template => Expr::binary(
            ">=",
            Expr::column(0, "score", Type::Int),
            Expr::parameter(Scalar::Int(generator.choose(31) as i64 - 15), Type::Int),
            Type::Bool,
        ),
        Mode::Recursive => generator.predicate(available, limits.max_depth - 1),
    };
    let (expr_nodes, expr_depth) = predicate.metrics();
    let mut features = BTreeSet::from([
        "select".into(),
        "where".into(),
        "order_by".into(),
        "limit".into(),
    ]);
    let mut parameters = vec![];
    let filter = predicate.render(&mut parameters, &mut features);
    let mut from = "work_items AS t0".to_string();
    for alias in 1..generator.relations {
        let kind = if index % 6 == 5 {
            "LEFT JOIN"
        } else {
            "INNER JOIN"
        };
        features.insert(kind.to_ascii_lowercase());
        from.push_str(&format!(
            " {kind} work_dims AS t{alias} ON t0.bucket = t{alias}.id"
        ));
    }
    let limit = 1 + generator.choose(limits.max_output_rows);
    let offset = generator.choose(4);
    let (projection, suffix, limit) = if aggregate {
        features.insert("aggregate".into());
        features.remove("order_by");
        features.remove("limit");
        if limits.max_output_rows >= 8 {
            features.insert("group_by".into());
            (
                "t0.bucket AS group_key, COUNT(*) AS count_value, SUM(t0.score) AS sum_value",
                " GROUP BY t0.bucket".to_string(),
                8,
            )
        } else {
            (
                "COUNT(*) AS count_value, SUM(t0.score) AS sum_value",
                String::new(),
                1,
            )
        }
    } else {
        (
            "t0.id AS id, t0.score AS score, t0.label AS label",
            format!(" ORDER BY score, id LIMIT {limit} OFFSET {offset}"),
            limit,
        )
    };
    let case = Case {
        index,
        template: format!("sqlsmith-{}", index % 6),
        mode,
        statement: Statement {
            sql: format!("SELECT {projection} FROM {from} WHERE {filter}{suffix}"),
            parameters,
        },
        ordered: !aggregate,
        nodes: fixed_nodes + expr_nodes,
        depth: (expr_depth + 1).max(if joined { 4 } else { 3 }),
        relations: generator.relations,
        features,
        limit,
    };
    if case.nodes > limits.max_nodes || case.depth > limits.max_depth {
        return Err("generated AST exceeds its declared bounds".into());
    }
    Ok(case)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_replay_and_feature_inventory() {
        let mut features = BTreeSet::new();
        for max_depth in [3, 6, 8] {
            for max_nodes in [48, 96, 512] {
                let limits = Limits {
                    max_depth,
                    max_nodes,
                    ..Limits::default()
                };
                for index in 0..144 {
                    let case = generate(7, index, Mode::Recursive, &limits).unwrap();
                    assert_eq!(case, generate(7, index, Mode::Recursive, &limits).unwrap());
                    assert!(
                        case.nodes <= max_nodes
                            && case.depth <= max_depth
                            && case.relations <= limits.max_relations
                            && case.limit <= limits.max_output_rows
                    );
                    features.extend(case.features);
                }
            }
        }
        for expected in [
            "expr:AND",
            "expr:OR",
            "expr:not",
            "expr:is_null",
            "inner join",
            "left join",
            "aggregate",
            "group_by",
            "order_by",
            "limit",
        ] {
            assert!(features.contains(expected), "{expected}");
        }
        assert!(generate(
            7,
            0,
            Mode::Recursive,
            &Limits {
                max_nodes: 1,
                ..Limits::default()
            }
        )
        .is_err());
    }
}
