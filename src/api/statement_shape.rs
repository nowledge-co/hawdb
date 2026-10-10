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

use crate::cypher::{
    AggregateExpression, ArithmeticOp, ClauseKind, PathSearch, ProcedureCallKind, QueryPipeline,
    ReturnExpressionKind,
};

/// Keep query-report categories independent of the parser's AST representation.
pub(super) fn pipeline_statement_kind(query: &QueryPipeline) -> &'static str {
    let with_count = query
        .clauses
        .iter()
        .filter(|clause| matches!(clause.kind, ClauseKind::With(_)))
        .count();
    // Multi-stage queries were already reported as pipelines before the default
    // MATCH migration. Preserve that existing category across representations.
    if with_count >= 2 {
        return "pipeline";
    }
    match query.clauses.first().map(|clause| &clause.kind) {
        Some(ClauseKind::Unwind { .. }) => return "unwind_mutation",
        Some(ClauseKind::Call { procedure, .. }) => {
            return match procedure.kind {
                ProcedureCallKind::VectorSearch(_)
                    if query
                        .clauses
                        .iter()
                        .any(|clause| matches!(clause.kind, ClauseKind::Match { .. })) =>
                {
                    "vector_graph_search"
                }
                ProcedureCallKind::VectorSearch(_) => "vector_search",
                ProcedureCallKind::TextSearch(_)
                    if query
                        .clauses
                        .iter()
                        .any(|clause| matches!(clause.kind, ClauseKind::Match { .. })) =>
                {
                    "text_graph_search"
                }
                ProcedureCallKind::TextSearch(_) => "text_search",
                ProcedureCallKind::GraphSeedSearch(_) => "graph_seed_search",
                ProcedureCallKind::GraphAlgorithm { .. } => "graph_algorithm",
                ProcedureCallKind::ProjectGraph { .. } => "project_graph",
            }
        }
        Some(ClauseKind::Create(patterns)) => {
            return if patterns
                .first()
                .is_some_and(|pattern| pattern.steps.is_empty())
            {
                "create_node"
            } else {
                "create_relationship"
            }
        }
        Some(ClauseKind::Merge { pattern, .. }) => {
            return if pattern.steps.is_empty() {
                "merge_node"
            } else {
                "merge_relationship"
            }
        }
        Some(ClauseKind::Match {
            optional: false, ..
        }) => {}
        _ => return "pipeline",
    }
    let mut patterns = 0;
    let mut has_expansion = false;
    let mut optional_matches = 0;
    let mut shortest_path = false;
    for clause in &query.clauses {
        if let ClauseKind::Match {
            optional,
            patterns: matched,
            ..
        } = &clause.kind
        {
            optional_matches += usize::from(*optional);
            if !optional {
                patterns += matched.len();
                has_expansion |= matched.iter().any(|pattern| !pattern.steps.is_empty());
            }
            shortest_path |= matched.iter().any(|pattern| {
                pattern
                    .steps
                    .iter()
                    .any(|step| step.relationship.search == PathSearch::AllShortest)
            });
        }
    }
    let has_return = query
        .clauses
        .iter()
        .any(|clause| matches!(clause.kind, ClauseKind::Return(_)));
    for clause in &query.clauses {
        match clause.kind {
            ClauseKind::Set(_) => {
                return if has_return {
                    "match_set_return"
                } else {
                    "match_set"
                }
            }
            ClauseKind::Delete { .. } => return "match_delete",
            ClauseKind::Create(_) => return "match_create_relationship",
            ClauseKind::Merge { .. } => {
                return match (has_expansion, patterns > 1) {
                    (true, true) => "match_expand_match_merge_relationship",
                    (true, false) => "match_expand_merge_relationship",
                    (false, _) => "match_merge_relationship",
                }
            }
            _ => {}
        }
    }
    if shortest_path {
        return "shortest_path_return";
    }
    if optional_matches == 2
        && with_count == 0
        && let Some(clause) = query.clauses.last()
        && let ClauseKind::Return(projection) = &clause.kind
        && let [item] = projection.items.as_slice()
        && let ReturnExpressionKind::Arithmetic { first, rest } = &item.expression.kind
        && matches!(
            first.kind,
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable { .. })
        )
        && let [(ArithmeticOp::Add, second)] = rest.as_slice()
        && matches!(
            second.kind,
            ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable { .. })
        )
    {
        return "match_optional_relationship_count_sum";
    }
    if patterns == 2 && !has_expansion && optional_matches == 0 && with_count == 0 {
        "match_nodes_return"
    } else {
        "match_return"
    }
}

#[cfg(test)]
mod tests {
    use super::super::statement_kind;
    use crate::cypher::{self, Statement};

    #[test]
    fn pipeline_reporting_preserves_statement_kinds() {
        for (source, expected) in [
            ("MATCH (m:Memory) RETURN m.id", "match_return"),
            ("MATCH (a:Memory {id: $a}), (b:Memory {id: $b}) RETURN a.id, b.id", "match_nodes_return"),
            ("MATCH (m:Memory) SET m.title = $title", "match_set"),
            ("MATCH (m:Memory) SET m.title = $title RETURN m.id", "match_set_return"),
            ("MATCH (m:Memory) DETACH DELETE m", "match_delete"),
            ("MATCH (a:Memory), (b:Memory) CREATE (a)-[:EVOLVES]->(b)", "match_create_relationship"),
            ("MATCH (a:Memory), (b:Memory) MERGE (a)-[:EVOLVES]->(b)", "match_merge_relationship"),
            ("MATCH (a:Memory)-[:EVOLVES]->(b:Memory) MERGE (a)-[:RELATED]->(b)", "match_expand_merge_relationship"),
            ("MATCH (a:Memory)-[:EVOLVES]->(b:Memory), (c:Memory) MERGE (c)-[:RELATED]->(b)", "match_expand_match_merge_relationship"),
            ("MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN length(p) AS hops", "shortest_path_return"),
            ("MATCH (e:Entity {id: $eid}) OPTIONAL MATCH (e)-[r1:RELATES_TO]-() OPTIONAL MATCH ()-[r2:RELATES_TO]->(e) RETURN (count(r1) + count(r2))", "match_optional_relationship_count_sum"),
            ("MATCH (m:Memory) WITH DISTINCT m.id AS id RETURN id", "match_return"),
            ("MATCH (m:Memory) WITH m WITH m RETURN m.id", "pipeline"),
            ("CALL vector_search($embedding, topK := 4) YIELD id, score MATCH (m:Memory) WHERE m.space_id = $space_id RETURN m.id, score", "vector_graph_search"),
            ("CALL vector_search($embedding, topK := 4) RETURN id, score", "vector_search"),
            ("CALL page_rank('g') RETURN node, rank", "graph_algorithm"),
            ("CALL project_graph('g', ['Memory'], ['EVOLVES'])", "project_graph"),
            ("CREATE (:Memory {id: $id})", "create_node"),
            ("CREATE (:Memory {id: $a})-[:EVOLVES]->(:Memory {id: $b})", "create_relationship"),
            ("MERGE (:Memory {id: $id})", "merge_node"),
            ("MERGE (:Memory {id: $a})-[:EVOLVES]->(:Memory {id: $b})", "merge_relationship"),
            ("UNWIND $rows AS row CREATE (:Memory {id: row.id})", "unwind_mutation"),
        ] {
            let public = cypher::parse(source).unwrap();
            assert_eq!(statement_kind(&public), expected, "public: {source}");
            let pipeline = Statement::Pipeline(Box::new(cypher::parse_pipeline(source).unwrap()));
            assert_eq!(statement_kind(&pipeline), expected, "pipeline: {source}");
            let wrapped = Statement::CypherQuery(Box::new(cypher::CypherQuery {
                system_variables: Vec::new(),
                statement: pipeline,
            }));
            assert_eq!(statement_kind(&wrapped), expected, "wrapped: {source}");
        }
    }
}
