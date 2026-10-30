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

//! Capture the public projection before optimizations can erase empty inputs.

use super::*;
use hawdb_plan_cypher::{GraphAlgorithmIdentity, GraphAlgorithmKind, SetNodePropertiesReturnMode};

pub(super) fn logical_output_columns(
    mut plan: &LogicalPlan,
    scoring: Option<BoundScoringRequest<'_>>,
) -> Option<Vec<String>> {
    let columns = loop {
        match plan {
            LogicalPlan::Project { items, .. } => {
                break items
                    .iter()
                    .map(|item| item.name.clone())
                    .collect::<Vec<_>>();
            }
            LogicalPlan::Aggregate {
                group_keys, items, ..
            } => {
                break group_keys
                    .iter()
                    .map(|item| item.name.clone())
                    .chain(items.iter().map(|item| item.name.clone()))
                    .collect();
            }
            LogicalPlan::ShortestPath { returns, .. } => {
                break returns.iter().map(|item| item.name.clone()).collect();
            }
            LogicalPlan::OptionalRelationshipCountSum { output, .. } => {
                break vec![output.clone()];
            }
            LogicalPlan::SetNodePropertiesReturn { returns, .. } => {
                break match returns {
                    SetNodePropertiesReturnMode::Project(items) => {
                        items.iter().map(|item| item.name.clone()).collect()
                    }
                    SetNodePropertiesReturnMode::Count { name } => vec![name.clone()],
                };
            }
            LogicalPlan::GraphAlgorithm {
                algorithm,
                score_column,
                return_node_identity,
                ..
            } => {
                break graph_algorithm_columns(
                    *algorithm,
                    GraphAlgorithmIdentity::legacy(*return_node_identity),
                    score_column,
                )
            }
            LogicalPlan::GraphAlgorithmOutput(output) => {
                break graph_algorithm_columns(
                    output.algorithm,
                    output.identity,
                    &output.score_column,
                )
            }
            LogicalPlan::VectorSeed {
                output_external_id: false,
                ..
            }
            | LogicalPlan::TextSeed {
                output_external_id: false,
                ..
            } => {
                break vec!["id".into(), "score".into()];
            }
            LogicalPlan::ThreadRepairStats { .. } => {
                break hawdb_plan_cypher::THREAD_REPAIR_STATS_COLUMNS
                    .into_iter()
                    .map(str::to_owned)
                    .collect();
            }
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. } => plan = input,
            _ => return None,
        }
    };
    // Match the existing executor suffix rule, including unaliased repeated
    // function calls. Declaring the schema must not change accepted queries.
    let mut columns = hawdb_plan_cypher::unique_projection_names(columns);
    if scoring.is_some()
        && !columns
            .iter()
            .any(|name| name == hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN)
    {
        columns.push(hawdb_plan_cypher::SCORING_RERANK_SCORE_COLUMN.into());
    }
    Some(columns)
}

fn graph_algorithm_columns(
    algorithm: GraphAlgorithmKind,
    identity: GraphAlgorithmIdentity,
    score_column: &str,
) -> Vec<String> {
    let mut columns = vec!["node".into()];
    columns.extend(identity.columns().map(str::to_owned));
    if algorithm == GraphAlgorithmKind::Louvain {
        columns.push("level".into());
    }
    columns.push(score_column.to_owned());
    columns
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_procedure_projections_always_declare_output_columns() {
        let parameters = BTreeMap::from([
            (
                "embedding".into(),
                Value::List(vec![Value::Float(1.0), Value::Float(0.0)]),
            ),
            (
                "vector".into(),
                Value::Vector(hawdb_core::Vector::new(&[1.0, 0.0]).unwrap()),
            ),
            ("text".into(), Value::String("memory".into())),
        ]);
        let cases: &[(&str, &[&str])] = &[
            ("CALL vector_search($embedding) RETURN id, score", &["id", "score"]),
            ("CALL vector_search($embedding) YIELD id, score MATCH (n:Memory) RETURN score, id, n.id", &["score", "id", "n.id"]),
            ("CALL text_search($text) RETURN id, score", &["id", "score"]),
            ("CALL text_search($text) YIELD id AS hit, score AS rank MATCH (n:Memory) RETURN rank, hit, n.id", &["rank", "hit", "n.id"]),
            ("CALL graph_seed_search($text, label := 'Memory') YIELD node, score RETURN score, node.id", &["score", "node.id"]),
            ("CALL vector_index_search('docs', $vector, candidates := 8, topK := 2) YIELD node, distance RETURN distance, node.id", &["distance", "node.id"]),
            ("CALL page_rank('graph') RETURN node, pagerank_score", &["node", "pagerank_score"]),
            ("CALL louvain('graph') RETURN node, level, louvain_id", &["node", "level", "louvain_id"]),
        ];
        for &(query, expected) in cases {
            let statement = cypher::parse(query).unwrap_or_else(|error| panic!("{query}: {error}"));
            let plan = planner::plan_with_params(statement_body(&statement), &parameters)
                .unwrap_or_else(|error| panic!("{query}: {error}"));
            // Legacy CALL syntax does not accept LIMIT. Exercise the schema
            // wrapper directly without broadening those parser contracts.
            for plan in [
                plan.clone(),
                LogicalPlan::Limit {
                    offset: 0,
                    limit: Some(0),
                    input: Box::new(plan),
                },
            ] {
                assert_eq!(
                    logical_output_columns(&plan, None).as_deref(),
                    Some(
                        expected
                            .iter()
                            .map(|name| (*name).to_owned())
                            .collect::<Vec<_>>()
                            .as_slice()
                    ),
                    "{query}: {plan:?}",
                );
            }
        }
    }
}
