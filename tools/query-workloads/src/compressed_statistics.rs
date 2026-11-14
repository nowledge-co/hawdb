// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Directed, completely consumed graph queries for distribution/cost diagnosis.
//! SQL-named legacy statement fields retain the frozen tool wire contract.

use crate::cardinality::{CaseMetadata, Workload};
use crate::smith::{Case, Fixture, Mode, Scalar, Statement};

pub const PROTOCOL: &str = "hawdb-graph-statistics-workloads-v1";
const LABELS: [&str; 2] = ["StatsMemory", "IndexedStatsMemory"];
const CONDITIONS: [&str; 5] = [
    "m.space_id = $p0 AND m.score >= 0",
    "m.score >= $p0",
    "m.optional IS NULL",
    "m.space_id IN [$p0, $p1]",
    "m.space_id <> $p0",
];

fn query(label: &str, condition: &str) -> String {
    format!("MATCH (m:{label}) WHERE {condition} RETURN m.id AS id ORDER BY id")
}

/// Only these no-LIMIT/no-existence skeletons establish complete child reads.
pub fn complete_query(sql: &str) -> bool {
    LABELS.iter().any(|label| {
        CONDITIONS
            .iter()
            .any(|condition| query(label, condition) == sql)
    })
}

pub fn generate(seed: u64, rows: usize) -> Result<Workload, String> {
    if !(64..=1024).contains(&rows) {
        return Err("graph statistics rows must be in 64..=1024".into());
    }
    let shift = (seed % 20) as usize;
    let cold = |id: usize| (id + shift).is_multiple_of(20);
    let score = |id: usize| if id.is_multiple_of(2) { 1000 } else { id % 100 };
    let mut setup = vec![Statement {
        sql: "CREATE INDEX ON :IndexedStatsMemory(space_id)".into(),
        parameters: vec![],
    }];
    for label in LABELS {
        for id in 0..rows {
            let space = if cold(id) { "cold" } else { "hot" };
            let optional = if id.is_multiple_of(8) {
                String::new()
            } else {
                format!(", optional: {}", id % 3)
            };
            setup.push(Statement {
                sql: format!(
                    "CREATE (:{label} {{id: {id}, space_id: '{space}', score: {}{optional}}})",
                    score(id)
                ),
                parameters: vec![],
            });
        }
    }
    let mut cases = Vec::new();
    for label in LABELS {
        let mut add = |family: &str,
                       cohort: &str,
                       condition: &str,
                       parameters: Vec<Scalar>,
                       expected_result_rows: usize| {
            let id = format!("{label}/{family}/{cohort}");
            cases.push((
                Case {
                    index: cases.len(),
                    template: family.into(),
                    mode: Mode::Template,
                    statement: Statement {
                        sql: query(label, condition),
                        parameters,
                    },
                    ordered: true,
                    nodes: 32,
                    depth: 4,
                    relations: 1,
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
        };
        for (space, is_cold) in [("hot", false), ("cold", true)] {
            add(
                "equality",
                space,
                CONDITIONS[0],
                vec![Scalar::Text(space.into())],
                (0..rows).filter(|id| cold(*id) == is_cold).count(),
            );
        }
        for bound in [0, 50, 1000, 1001] {
            add(
                "range",
                &format!("ge_{bound}"),
                CONDITIONS[1],
                vec![Scalar::Int(bound as i64)],
                (0..rows).filter(|id| score(*id) >= bound).count(),
            );
        }
        add(
            "null",
            "missing",
            CONDITIONS[2],
            vec![],
            (0..rows).filter(|id| id.is_multiple_of(8)).count(),
        );
        add(
            "in",
            "all",
            CONDITIONS[3],
            vec![Scalar::Text("hot".into()), Scalar::Text("cold".into())],
            rows,
        );
        add(
            "in",
            "duplicate",
            CONDITIONS[3],
            vec![Scalar::Text("cold".into()), Scalar::Text("cold".into())],
            (0..rows).filter(|id| cold(*id)).count(),
        );
        add(
            "not_equal",
            "cold",
            CONDITIONS[4],
            vec![Scalar::Text("hot".into())],
            (0..rows).filter(|id| cold(*id)).count(),
        );
    }
    Ok(Workload {
        fixture: Fixture { rows, setup },
        cases,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_directed_inventory_keeps_zero_and_complete_read_controls() {
        let workload = generate(42, 128).unwrap();
        assert_eq!(workload.cases.len(), 20);
        assert!(workload
            .cases
            .iter()
            .all(|(case, _)| complete_query(&case.statement.sql)));
        assert!(workload
            .cases
            .iter()
            .any(|(_, meta)| meta.expected_result_rows == 0));
        assert!(!complete_query("MATCH (m:StatsMemory) RETURN m.id LIMIT 1"));
        assert!(generate(42, 4096).is_err());
    }
}
