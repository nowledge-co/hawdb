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

use crate::{MatchNodesReturn, MatchReturn, Statement};

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
    let fast_path_reason = match body {
        Statement::MatchReturn(query) if is_simple_node_lookup(query) => Some("simple_node_lookup"),
        Statement::MatchReturn(query) if is_simple_one_hop_expand(query) => {
            Some("simple_one_hop_expand")
        }
        Statement::MatchNodesReturn(query) if is_simple_two_node_lookup(query) => {
            Some("simple_two_node_lookup")
        }
        Statement::ShortestPathReturn(_) => Some("bounded_shortest_path"),
        _ => None,
    };
    CypherReadRouteShape {
        fast_path_reason,
        has_ordering: statement_has_ordering(body),
        has_pagination: statement_has_pagination(body),
    }
}

fn statement_has_ordering(statement: &Statement) -> bool {
    match statement {
        Statement::MatchReturn(query) => {
            !query.order_by.is_empty() || !query.with_order_by.is_empty()
        }
        _ => false,
    }
}

fn statement_has_pagination(statement: &Statement) -> bool {
    match statement {
        Statement::MatchReturn(query) => {
            query.offset.is_some()
                || query.limit.is_some()
                || query.with_offset.is_some()
                || query.with_limit.is_some()
        }
        Statement::MatchNodesReturn(query) => query.limit.is_some(),
        _ => false,
    }
}

fn is_simple_node_lookup(query: &MatchReturn) -> bool {
    !query.properties.is_empty()
        && query.expand.is_none()
        && query.post_match_expand.is_none()
        && query.optional_expand.is_none()
        && query.optional_with.is_none()
        && query.collect_with.is_none()
        && query.distinct_with.is_none()
        && query.with_projection.is_none()
        && query.with_order_by.is_empty()
        && query.with_offset.is_none()
        && query.with_limit.is_none()
        && query.aggregate_with.is_none()
        && query.aggregate_with_filter.is_none()
        && query.post_with_match.is_none()
        && query.predicate.is_none()
        && !query.distinct
        && query.order_by.is_empty()
        && query.offset.is_none()
}

fn is_simple_one_hop_expand(query: &MatchReturn) -> bool {
    query.expand.as_ref().is_some_and(|expand| {
        expand.min_hops == 1
            && expand.max_hops == 1
            && !query.properties.is_empty()
            && query.post_match_expand.is_none()
            && query.optional_expand.is_none()
            && query.optional_with.is_none()
            && query.collect_with.is_none()
            && query.distinct_with.is_none()
            && query.with_projection.is_none()
            && query.with_order_by.is_empty()
            && query.with_offset.is_none()
            && query.with_limit.is_none()
            && query.aggregate_with.is_none()
            && query.aggregate_with_filter.is_none()
            && query.post_with_match.is_none()
            && query.predicate.is_none()
            && !query.distinct
            && query.order_by.is_empty()
            && query.offset.is_none()
    })
}

fn is_simple_two_node_lookup(query: &MatchNodesReturn) -> bool {
    !query.left_properties.is_empty()
        && !query.right_properties.is_empty()
        && query.predicate.is_none()
}

#[cfg(test)]
mod tests {
    use super::classify_read_route_shape;

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
