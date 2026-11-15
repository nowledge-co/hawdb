// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Small, exhaustively counted populations for cardinality diagnostics.
//! Expected counts describe query results, never intermediate operator rows.

use crate::smith::{Case, Fixture, Mode, Scalar, Statement};
use serde::{Deserialize, Serialize};

pub const PROTOCOL: &str = "hawdb-cardinality-workloads-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseMetadata {
    pub id: String,
    pub cohort: String,
    pub family: String,
    pub expected_result_rows: usize,
}

pub struct Workload {
    pub fixture: Fixture,
    pub cases: Vec<(Case, CaseMetadata)>,
}

#[derive(Clone, Copy)]
struct Row {
    id: i64,
    a: Option<i64>,
    b: Option<i64>,
}

fn statement(sql: impl Into<String>, parameters: Vec<Scalar>) -> Statement {
    Statement {
        sql: sql.into(),
        parameters,
    }
}

fn scalar(value: Option<i64>) -> Scalar {
    value.map_or(Scalar::Null, Scalar::Int)
}

fn add_case(
    cases: &mut Vec<(Case, CaseMetadata)>,
    cohort: &str,
    family: &str,
    query: Statement,
    expected_result_rows: usize,
    relations: usize,
) {
    let id = format!("{cohort}/{family}");
    cases.push((
        Case {
            index: cases.len(),
            template: family.into(),
            mode: Mode::Template,
            statement: query,
            ordered: true,
            // Conservative upper bounds for these fixed SELECT skeletons.
            nodes: 32,
            depth: 4,
            relations,
            features: [format!("cohort:{cohort}"), format!("family:{family}")].into(),
            limit: 1024,
        },
        CaseMetadata {
            id,
            cohort: cohort.into(),
            family: family.into(),
            expected_result_rows,
        },
    ));
}

pub fn generate(seed: u64, rows: usize) -> Result<Workload, String> {
    if ![64, 128].contains(&rows) {
        return Err("cardinality fixture rows must be 64 or 128 per populated table".into());
    }
    let shift = (seed % 8) as i64;
    let populations = [
        "uniform",
        "skewed",
        "correlated",
        "independent",
        "null_heavy",
        "empty",
    ]
    .into_iter()
    .map(|name| {
        let population = (0..if name == "empty" { 0 } else { rows })
            .map(|id| {
                let i = id as i64;
                let a = (i + shift) % 8;
                let b = (i / 8 + shift) % 8;
                let (a, b) = match name {
                    "skewed" => (Some(if id < rows * 7 / 8 { 0 } else { a }), Some(b)),
                    "correlated" => (Some(a), Some(a)),
                    "null_heavy" => ((id % 4 == 0).then_some(a), (id % 3 == 0).then_some(b)),
                    // Uniform has a different marginal NDV from the paired
                    // independent/correlated populations (both NDV eight).
                    "uniform" => (Some((i + shift) % 16), Some(b)),
                    _ => (Some(a), Some(b)),
                };
                Row { id: i, a, b }
            })
            .collect::<Vec<_>>();
        (name, population)
    })
    .collect::<Vec<_>>();
    let mut setup = vec![];
    let mut cases = vec![];
    for (name, population) in &populations {
        let table = format!("ce_{name}");
        setup.push(statement(
            format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT)"),
            vec![],
        ));
        for row in population {
            setup.push(statement(
                format!("INSERT INTO {table} VALUES ($1, $2, $3)"),
                vec![Scalar::Int(row.id), scalar(row.a), scalar(row.b)],
            ));
        }
        setup.push(statement(
            format!("CREATE INDEX {table}_a ON {table} (a)"),
            vec![],
        ));
        setup.push(statement(
            format!("CREATE INDEX {table}_ab ON {table} (a, b)"),
            vec![],
        ));
        // The oracle counts the original population in Rust; it never consults
        // optimizer statistics, plans, or the result under test.
        type Predicate = fn(&Row) -> bool;
        let probes: [(&str, &str, Vec<Scalar>, Predicate); 12] = [
            ("full_scan", "", vec![], |_| true),
            ("point_hot", "a = $1", vec![Scalar::Int(0)], |r| {
                r.a == Some(0)
            }),
            ("point_rare", "a = $1", vec![Scalar::Int(7)], |r| {
                r.a == Some(7)
            }),
            ("point_absent", "a = $1", vec![Scalar::Int(99)], |r| {
                r.a == Some(99)
            }),
            (
                "range",
                "a >= $1 AND a < $2",
                vec![Scalar::Int(2), Scalar::Int(5)],
                |r| r.a.is_some_and(|a| (2..5).contains(&a)),
            ),
            ("range_outside", "a > $1", vec![Scalar::Int(99)], |r| {
                r.a.is_some_and(|a| a > 99)
            }),
            (
                "joint_equal",
                "a = $1 AND b = $2",
                vec![Scalar::Int(3), Scalar::Int(3)],
                |r| r.a == Some(3) && r.b == Some(3),
            ),
            (
                "joint_different",
                "a = $1 AND b = $2",
                vec![Scalar::Int(3), Scalar::Int(7)],
                |r| r.a == Some(3) && r.b == Some(7),
            ),
            (
                "duplicate_predicate",
                "a = $1 AND a = $1",
                vec![Scalar::Int(3)],
                |r| r.a == Some(3),
            ),
            ("is_null", "a IS NULL", vec![], |r| r.a.is_none()),
            ("is_not_null", "a IS NOT NULL", vec![], |r| r.a.is_some()),
            ("residual_filter", "b = $1", vec![Scalar::Int(3)], |r| {
                r.b == Some(3)
            }),
        ];
        for (family, predicate, parameters, matches) in probes {
            add_case(
                &mut cases,
                name,
                family,
                statement(
                    if predicate.is_empty() {
                        format!("SELECT id FROM {table} ORDER BY id")
                    } else {
                        format!("SELECT id FROM {table} WHERE {predicate} ORDER BY id")
                    },
                    parameters,
                ),
                population.iter().filter(|r| matches(r)).count(),
                1,
            );
        }
    }
    // Two repeated dimension keys per bucket, with bucket seven absent.
    let dimensions = (0..14i64)
        .map(|id| Row {
            id,
            a: Some(id / 2),
            b: None,
        })
        .collect::<Vec<_>>();
    setup.push(statement(
        "CREATE TABLE ce_dims (id BIGINT PRIMARY KEY, a BIGINT)",
        vec![],
    ));
    for row in &dimensions {
        setup.push(statement(
            "INSERT INTO ce_dims VALUES ($1, $2)",
            vec![Scalar::Int(row.id), scalar(row.a)],
        ));
    }
    setup.push(statement("CREATE INDEX ce_dims_a ON ce_dims (a)", vec![]));
    let left = &populations
        .iter()
        .find(|(n, _)| *n == "independent")
        .unwrap()
        .1;
    let correlated = &populations
        .iter()
        .find(|(n, _)| *n == "correlated")
        .unwrap()
        .1;
    for (family, right_table, right, outer, restricted) in [
        ("one_to_many", "ce_dims", &dimensions, false, false),
        ("left_unmatched", "ce_dims", &dimensions, true, false),
        ("many_to_many", "ce_correlated", correlated, false, true),
        ("no_match", "ce_empty", &populations[5].1, false, false),
        ("left_empty", "ce_empty", &populations[5].1, true, false),
    ] {
        let expected = left
            .iter()
            .filter(|r| !restricted || r.a == Some(3))
            .map(|l| {
                let matches = right.iter().filter(|r| l.a.is_some() && l.a == r.a).count();
                if outer {
                    matches.max(1)
                } else {
                    matches
                }
            })
            .sum();
        let join = if outer { "LEFT JOIN" } else { "JOIN" };
        let predicate = if restricted { " WHERE l.a = $1" } else { "" };
        add_case(&mut cases, "join", family, statement(
            format!("SELECT l.id AS left_id, r.id AS right_id FROM ce_independent AS l {join} {right_table} AS r ON l.a = r.a{predicate} ORDER BY l.id, r.id"),
            if restricted { vec![Scalar::Int(3)] } else { vec![] }), expected, 2);
    }
    Ok(Workload {
        fixture: Fixture {
            rows: rows * 5 + dimensions.len(),
            setup,
        },
        cases,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_populations_expose_correlation_nulls_and_join_multiplicity() {
        let w = generate(7, 128).unwrap();
        let count = |id: &str| {
            w.cases
                .iter()
                .find(|(_, m)| m.id == id)
                .unwrap()
                .1
                .expected_result_rows
        };
        assert_eq!(count("independent/joint_equal"), 2);
        assert_eq!(count("correlated/joint_equal"), 16);
        assert_eq!(count("correlated/joint_different"), 0);
        assert_eq!(count("null_heavy/is_null"), 96);
        assert_eq!(count("join/one_to_many"), 224);
        assert_eq!(count("join/left_unmatched"), 240);
        assert_eq!(count("join/many_to_many"), 256);
        assert_eq!(count("join/no_match"), 0);
        assert_eq!(count("join/left_empty"), 128);
        assert!(w
            .cases
            .iter()
            .all(|(c, m)| m.expected_result_rows <= c.limit && !c.statement.sql.contains("LIMIT")));
        assert!(generate(7, 65).is_err());
    }
}
