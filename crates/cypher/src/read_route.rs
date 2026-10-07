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

use crate::{ClauseKind, PathSearch, QueryPipeline, Statement};

/// Syntactic properties of a Cypher read route used by host-level reporting.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CypherReadRouteShape {
    pub fast_path_reason: Option<&'static str>,
    pub has_ordering: bool,
    pub has_pagination: bool,
}

impl CypherReadRouteShape {
    pub const fn is_fast_path(self) -> bool {
        self.fast_path_reason.is_some()
    }
}

/// Returns the executable statement nested below a `CYPHER` wrapper.
#[doc(hidden)]
pub fn query_statement_body(statement: &Statement) -> &Statement {
    match statement {
        Statement::CypherQuery(query) => &query.statement,
        _ => statement,
    }
}

/// Classifies read-route syntax without making an execution-policy decision.
#[doc(hidden)]
pub fn classify_read_route_shape(statement: &Statement) -> CypherReadRouteShape {
    let body = query_statement_body(statement);
    if let Statement::Pipeline(query) = body {
        return classify_pipeline_read_route(query);
    }
    CypherReadRouteShape {
        fast_path_reason: None,
        has_ordering: false,
        has_pagination: false,
    }
}

fn classify_pipeline_read_route(query: &QueryPipeline) -> CypherReadRouteShape {
    let projections = query
        .clauses
        .iter()
        .filter_map(|clause| match &clause.kind {
            ClauseKind::With(projection) | ClauseKind::Return(projection) => Some(projection),
            _ => None,
        });
    let mut shape = CypherReadRouteShape {
        fast_path_reason: None,
        has_ordering: false,
        has_pagination: false,
    };
    for projection in projections {
        shape.has_ordering |= !projection.order_by.is_empty();
        shape.has_pagination |= projection.offset.is_some() || projection.limit.is_some();
    }

    let Some((returned, matched)) = query.clauses.split_last() else {
        return shape;
    };
    let ClauseKind::Return(projection) = &returned.kind else {
        return shape;
    };
    // The legacy seeded read shared MATCH's syntactic lookup/expansion report.
    // Keep procedure capability and plan-cache eligibility as separate policies.
    let vector_seeded = matched.first().is_some_and(|clause| {
        matches!(&clause.kind,
        ClauseKind::Call { procedure, yields }
        if matches!(procedure.kind, crate::ProcedureCallKind::VectorSearch(_))
            && yields.len() == 2
            && yields[0].name.eq_ignore_ascii_case("id") && yields[0].alias.is_none()
            && yields[1].name.eq_ignore_ascii_case("score") && yields[1].alias.is_none())
    });
    let matched = if vector_seeded {
        &matched[1..]
    } else {
        matched
    };
    if projection.distinct
        || projection.predicate.is_some()
        || shape.has_ordering
        || projection.offset.is_some()
    {
        return shape;
    }

    if let [clause] = matched
        && let ClauseKind::Match {
            optional: false,
            patterns,
            predicate,
        } = &clause.kind
        && let [pattern] = patterns.as_slice()
    {
        if let [step] = pattern.steps.as_slice()
            && !vector_seeded
            && pattern.variable.is_some()
            && step.relationship.search == PathSearch::AllShortest
            && projection.limit.is_none()
        {
            shape.fast_path_reason = Some("bounded_shortest_path");
        } else if !pattern.first.properties.is_empty() && predicate.is_none() {
            shape.fast_path_reason = match pattern.steps.as_slice() {
                [] => Some("simple_node_lookup"),
                [step]
                    if step.relationship.min_hops == 1
                        && step.relationship.max_hops == 1
                        && step.relationship.search == PathSearch::All =>
                {
                    Some("simple_one_hop_expand")
                }
                _ => None,
            };
        }
        return shape;
    }

    if vector_seeded {
        return shape;
    }

    // The two independent nodes may occur in one MATCH or two consecutive MATCHes.
    // Any filter, expansion or intervening clause requires the general read route.
    let mut nodes = 0;
    for clause in matched {
        let ClauseKind::Match {
            optional: false,
            patterns,
            predicate: None,
        } = &clause.kind
        else {
            return shape;
        };
        for pattern in patterns {
            if !pattern.steps.is_empty() || pattern.first.properties.is_empty() {
                return shape;
            }
            nodes += 1;
        }
    }
    if nodes == 2 {
        shape.fast_path_reason = Some("simple_two_node_lookup");
    }
    shape
}

#[cfg(test)]
mod tests {
    use super::{classify_read_route_shape, CypherReadRouteShape};

    #[test]
    fn pipeline_read_routes_preserve_fast_paths_and_window_reports() {
        let cases = [
            ("MATCH (m:Memory {id: $id}) RETURN m.title AS title", Some("simple_node_lookup"), false, false),
            ("MATCH (m:Memory {id: $id}) RETURN m.title AS title LIMIT 1", Some("simple_node_lookup"), false, true),
            ("MATCH (m:Memory {id: $id})-[:MENTIONS]->(e:Entity) RETURN e.id", Some("simple_one_hop_expand"), false, false),
            ("MATCH (a:Memory {id: $a}), (b:Memory {id: $b}) RETURN a.id, b.id LIMIT 1", Some("simple_two_node_lookup"), false, true),
            ("MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN length(p) AS hops", Some("bounded_shortest_path"), false, false),
            ("MATCH (m:Memory {id: $id}) RETURN m.title AS title ORDER BY title LIMIT 1", None, true, true),
            ("MATCH (m:Memory {id: $id}) RETURN m.title AS title SKIP 1", None, false, true),
            ("MATCH (m:Memory) WITH m, COUNT(*) AS total ORDER BY total LIMIT 1 RETURN m.id, total", None, true, true),
            ("MATCH (m:Memory) WHERE m.id = $id RETURN m.title", None, false, false),
            ("MATCH (m:Memory {id: $id}) SET m.title = $title RETURN m.title", None, false, false),
            ("CALL vector_search($embedding, topK := 20) YIELD id, score MATCH (m:Memory {id: $id}) RETURN m.title, score", Some("simple_node_lookup"), false, false),
            ("CALL vector_search($embedding, topK := 20) YIELD id, score MATCH (m:Memory {id: $id}) RETURN m.title, score LIMIT 1", Some("simple_node_lookup"), false, true),
            ("CALL vector_search($embedding, topK := 20) YIELD id, score MATCH (m:Memory {id: $id})-[:MENTIONS]->(e:Entity) RETURN e.id, score", Some("simple_one_hop_expand"), false, false),
            ("CALL vector_search($embedding, topK := 20) YIELD id, score MATCH (m:Memory {id: $id}) RETURN m.title, score ORDER BY score DESC LIMIT 1", None, true, true),
        ];
        for (source, fast_path_reason, has_ordering, has_pagination) in cases {
            let expected = CypherReadRouteShape {
                fast_path_reason,
                has_ordering,
                has_pagination,
            };
            let parsed = crate::parse(source).unwrap();
            assert_eq!(
                classify_read_route_shape(&parsed),
                expected,
                "default parser: {source}"
            );
            let statement =
                crate::Statement::Pipeline(Box::new(crate::parse_pipeline(source).unwrap()));
            assert_eq!(
                classify_read_route_shape(&statement),
                expected,
                "pipeline: {source}"
            );
            let wrapped = crate::Statement::CypherQuery(Box::new(crate::CypherQuery {
                system_variables: Vec::new(),
                statement,
            }));
            assert_eq!(
                classify_read_route_shape(&wrapped),
                expected,
                "wrapped: {source}"
            );
        }
    }

    #[test]
    fn classifies_read_route_shape_from_ast_not_query_text() {
        let compact = crate::parse("MATCH (m:Memory {id: $id}) RETURN m.title AS title").unwrap();
        let spaced = crate::parse(
            "  match   ( m : Memory   { id : $id } )   return   m.title   as   title  ",
        )
        .unwrap();
        let ordered = crate::parse(
            "MATCH (m:Memory {id: $id}) RETURN m.title AS title ORDER BY title LIMIT 1",
        )
        .unwrap();

        let compact_shape = classify_read_route_shape(&compact);
        let spaced_shape = classify_read_route_shape(&spaced);
        let ordered_shape = classify_read_route_shape(&ordered);

        assert!(compact_shape.is_fast_path());
        assert_eq!(compact_shape.fast_path_reason, Some("simple_node_lookup"));
        assert!(!compact_shape.has_ordering);
        assert!(!compact_shape.has_pagination);
        assert_eq!(compact_shape, spaced_shape);
        assert!(!ordered_shape.is_fast_path());
        assert!(ordered_shape.has_ordering);
        assert!(ordered_shape.has_pagination);
    }
}
