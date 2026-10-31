// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::MAX_CYPHER_PARSER_DEPTH;
use crate::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_clauses_keep_independent_projection_scopes_and_source_ranges() {
        let query = " \u{2003}MATCH (a:Item {id: $id}), (b:Other) MATCH (a)-[r:LINK*1..2]->(c:Item) WHERE c.id = b.id WITH a, lower(c.name) AS name ORDER BY name DESC LIMIT 3 WITH name RETURN name; ";
        let parsed = parse_pipeline(query).unwrap();
        assert_eq!(parsed.clauses.len(), 5);
        assert_eq!(
            parsed.span.unwrap().text(query),
            Some(query.trim().trim_end_matches(';'))
        );
        let ClauseKind::Match { patterns, .. } = &parsed.clauses[0].kind else {
            panic!("expected MATCH");
        };
        assert_eq!(patterns.len(), 2);
        assert_eq!(
            patterns[0].first.span.unwrap().text(query),
            Some("(a:Item {id: $id})")
        );
        assert_eq!(
            patterns[0].first.properties["id"].span.unwrap().text(query),
            Some("$id")
        );
        let ClauseKind::Match {
            patterns,
            predicate,
            ..
        } = &parsed.clauses[1].kind
        else {
            panic!("expected MATCH");
        };
        let step = &patterns[0].steps[0];
        assert_eq!(
            step.span.unwrap().text(query),
            Some("-[r:LINK*1..2]->(c:Item)")
        );
        assert_eq!(
            step.relationship.span.unwrap().text(query),
            Some("-[r:LINK*1..2]->")
        );
        assert_eq!(step.target.span.unwrap().text(query), Some("(c:Item)"));
        assert_eq!(
            predicate.as_ref().unwrap().span.unwrap().text(query),
            Some("c.id = b.id")
        );
        assert!(matches!(
            step.relationship.direction,
            RelationshipDirection::Outgoing
        ));
        let ClauseKind::With(projection) = &parsed.clauses[2].kind else {
            panic!("expected WITH");
        };
        assert_eq!(projection.items.len(), 2);
        assert_eq!(projection.order_by.len(), 1);
        assert!(projection.limit.is_some());
        assert_eq!(
            parsed.clauses[3].span.unwrap().text(query),
            Some("WITH name")
        );
        assert!(matches!(parsed.clauses[4].kind, ClauseKind::Return(_)));
    }

    #[test]
    fn correlated_optional_aggregates_do_not_require_application_aliases() {
        for (first, second) in [
            ("identity_refs", "legacy_messages"),
            ("linked_identifiers", "raw_events"),
        ] {
            let query = format!("MATCH (root:Record) OPTIONAL MATCH (identity:Identifier) WHERE identity.owner = root.key WITH root, COUNT(identity) AS {first} OPTIONAL MATCH (root)-[:OWNS]->(event:Event) WITH root, {first}, COUNT(event) AS {second} OPTIONAL MATCH (root)-[:SUMMARIZED_BY]->(summary:Summary) RETURN root.key, {first}, {second}, COUNT(summary) ORDER BY root.key ASC");
            let parsed = parse_pipeline(&query).unwrap();
            assert_eq!(parsed.clauses.len(), 7);
            for index in [1, 3, 5] {
                assert!(matches!(
                    parsed.clauses[index].kind,
                    ClauseKind::Match { optional: true, .. }
                ));
            }
            let ClauseKind::With(projection) = &parsed.clauses[2].kind else {
                panic!("expected WITH");
            };
            assert_eq!(projection.items[1].alias.as_deref(), Some(first));
            let ClauseKind::With(projection) = &parsed.clauses[4].kind else {
                panic!("expected WITH");
            };
            assert_eq!(projection.items[2].alias.as_deref(), Some(second));
        }
    }

    #[test]
    fn mutations_are_ordered_clauses_over_the_same_patterns() {
        let query = "MATCH (a:Item), (b:Item) MERGE (a)-[r:LINK {id: $id}]->(b) ON CREATE SET r.created = true ON MATCH SET r.updated = true SET a.flag = $flag RETURN a.id";
        let parsed = parse_pipeline(query).unwrap();
        assert_eq!(parsed.clauses.len(), 4);
        let ClauseKind::Merge {
            pattern,
            on_create,
            on_match,
        } = &parsed.clauses[1].kind
        else {
            panic!("expected MERGE");
        };
        assert_eq!(pattern.first.variable, "a");
        assert_eq!(pattern.steps[0].target.variable, "b");
        assert_eq!(on_create.len(), 1);
        assert_eq!(on_match.len(), 1);
        assert!(matches!(parsed.clauses[2].kind, ClauseKind::Set(_)));
        assert!(matches!(parsed.clauses[3].kind, ClauseKind::Return(_)));
        let parsed = parse_pipeline("MATCH (a:Item) DETACH DELETE a").unwrap();
        assert!(matches!(
            parsed.clauses[1].kind,
            ClauseKind::Delete { detach: true, .. }
        ));
    }

    #[test]
    fn unwind_is_a_bounded_row_source_clause() {
        let scalar = parse_pipeline("MATCH (s:Skill) UNWIND $ids AS key MATCH (m:Memory {id: key}) MERGE (s)-[:EVIDENCE]->(m) RETURN m.id").unwrap();
        let ClauseKind::Match { patterns, .. } = &scalar.clauses[2].kind else {
            panic!("MATCH")
        };
        assert!(matches!(&patterns[0].first.properties["id"].kind,
            ValueExpressionKind::Binding(variable) if variable == "key"));
        let query = "UNWIND $rows AS row MERGE (entity:Entity {id: row.id}) ON CREATE SET entity.name = row.name";
        let parsed = parse_pipeline(query).unwrap();
        assert_eq!(parsed.clauses.len(), 2);
        let ClauseKind::Unwind { source, variable } = &parsed.clauses[0].kind else {
            panic!("expected UNWIND")
        };
        assert_eq!(variable, "row");
        assert!(matches!(
            source.kind,
            ValueExpressionKind::Parameter(ref parameter) if parameter == "rows"
        ));
        let ClauseKind::Merge {
            pattern, on_create, ..
        } = &parsed.clauses[1].kind
        else {
            panic!("expected MERGE")
        };
        assert!(matches!(
            pattern.first.properties["id"].kind,
            ValueExpressionKind::BindingProperty {
                ref variable,
                ref property,
            } if variable == "row" && property == "id"
        ));
        assert!(matches!(
            on_create[0].value,
            SetValueExpression::Property {
                ref variable,
                ref property,
            } if variable == "row" && property == "name"
        ));

        for query in [
            "UNWIND range(1, 2) AS row CREATE (n:Node)",
            "UNWIND row.id AS value CREATE (n:Node)",
        ] {
            assert!(parse_pipeline(query).is_err(), "{query}");
        }
    }

    #[test]
    fn column_predicates_and_procedure_yields_preserve_composition() {
        let query = "CALL vector_search($embedding, topK := $k) YIELD id AS key, score MATCH (n:Item) WHERE n.id = key WITH n, score AS rank WHERE rank >= $min AND rank <> 0 RETURN n.id, rank";
        let parsed = parse_pipeline(query).unwrap();
        assert_eq!(parsed.clauses.len(), 4);
        let ClauseKind::Call { procedure, yields } = &parsed.clauses[0].kind else {
            panic!("expected CALL")
        };
        assert_eq!(
            procedure.span.unwrap().text(query),
            Some("vector_search($embedding, topK := $k)")
        );
        assert_eq!(yields[0].span.unwrap().text(query), Some("id AS key"));
        assert_eq!(yields[0].alias.as_deref(), Some("key"));
        let ClauseKind::With(projection) = &parsed.clauses[2].kind else {
            panic!("expected WITH")
        };
        assert_eq!(
            projection
                .predicate
                .as_ref()
                .unwrap()
                .span
                .unwrap()
                .text(query),
            Some("rank >= $min AND rank <> 0")
        );
    }

    #[test]
    fn arithmetic_has_precedence_aggregation_and_original_child_ranges() {
        let query = "MATCH (n:Item) RETURN (COUNT(n) + COUNT(DISTINCT n) * 2 - $offset) AS total";
        let parsed = parse_pipeline(query).unwrap();
        let ClauseKind::Return(projection) = &parsed.clauses[1].kind else {
            panic!("expected RETURN")
        };
        let expression = &projection.items[0].expression;
        assert_eq!(
            expression.span.unwrap().text(query),
            Some("(COUNT(n) + COUNT(DISTINCT n) * 2 - $offset)")
        );
        let ReturnExpressionKind::Arithmetic { first, rest } = &expression.kind else {
            panic!("expected arithmetic")
        };
        assert_eq!(first.span.unwrap().text(query), Some("COUNT(n)"));
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].0, ArithmeticOp::Add);
        assert_eq!(rest[1].0, ArithmeticOp::Subtract);
        let ReturnExpressionKind::Arithmetic { first, rest } = &rest[0].1.kind else {
            panic!("expected product")
        };
        assert_eq!(first.span.unwrap().text(query), Some("COUNT(DISTINCT n)"));
        assert_eq!(rest[0].0, ArithmeticOp::Multiply);
        assert_eq!(rest[0].1.span.unwrap().text(query), Some("2"));
        let long = format!(
            "RETURN {}",
            std::iter::repeat_n("1", MAX_CYPHER_PARSER_DEPTH + 1)
                .collect::<Vec<_>>()
                .join(" + ")
        );
        assert!(parse_pipeline(&long)
            .unwrap_err()
            .to_string()
            .contains("maximum nesting depth"));
        let deep = format!(
            "RETURN {}1{}",
            "(".repeat(MAX_CYPHER_PARSER_DEPTH),
            ")".repeat(MAX_CYPHER_PARSER_DEPTH)
        );
        assert!(parse_pipeline(&deep).is_err());
        assert!(parse_pipeline("MATCH (n:Item) RETURN lower(COUNT(n))").is_err());
    }

    #[test]
    fn path_function_names_remain_legal_column_names() {
        let query = "MATCH (n:Item) WHERE n \u{2003}. id = $id WITH n.id AS length, n.name AS properties RETURN length, properties";
        let parsed = parse_pipeline(query).unwrap();
        let ClauseKind::Return(projection) = &parsed.clauses[2].kind else {
            panic!("expected RETURN")
        };
        for (item, name) in projection.items.iter().zip(["length", "properties"]) {
            let ReturnExpressionKind::Value(expression) = &item.expression.kind else {
                panic!("expected column")
            };
            assert_eq!(
                expression.kind,
                ScalarExpressionKind::Variable(name.to_string())
            );
            assert_eq!(expression.span.unwrap().text(query), Some(name));
        }
        let query = "MATCH route \u{2003}= \u{2003}(a)-[:LINK* ALL SHORTEST 1..2]->(b) RETURN length(route) AS hops";
        let parsed = parse_pipeline(query).unwrap();
        let ClauseKind::Match { patterns, .. } = &parsed.clauses[0].kind else {
            panic!("expected MATCH")
        };
        assert_eq!(patterns[0].variable.as_deref(), Some("route"));
        assert!(patterns[0]
            .span
            .unwrap()
            .text(query)
            .unwrap()
            .starts_with("route \u{2003}= \u{2003}(a)"));
    }

    #[test]
    fn aggregate_function_names_remain_legal_column_aliases() {
        let query = "MATCH (n:Item) WITH COUNT(n) AS count, MIN(n.id) AS min, MAX(n.id) AS max, AVG(n.id) AS avg, COLLECT(n.id) AS collect RETURN count, min, max, avg, collect";
        let parsed = parse_pipeline(query).unwrap();
        let ClauseKind::Return(projection) = &parsed.clauses[2].kind else {
            panic!("expected RETURN")
        };
        for (item, name) in projection
            .items
            .iter()
            .zip(["count", "min", "max", "avg", "collect"])
        {
            let ReturnExpressionKind::Value(expression) = &item.expression.kind else {
                panic!("expected column")
            };
            assert_eq!(
                expression.kind,
                ScalarExpressionKind::Variable(name.to_string())
            );
            assert_eq!(expression.span.unwrap().text(query), Some(name));
        }
    }

    #[test]
    fn shortest_path_functions_parse_as_search_modes_with_optional_upper_bounds() {
        let query = "MATCH (p1:Person {id: $a}), (p2:Person {id: $b}) OPTIONAL MATCH path = shortestPath((p1)-[:KNOWS*]-(p2)) RETURN CASE path IS NULL WHEN true THEN -1 ELSE length(path) END AS len, path IS NOT NULL AS found";
        for parsed in [parse_pipeline(query).unwrap(), {
            let crate::Statement::Pipeline(parsed) = crate::parse(query).unwrap() else {
                panic!("public MATCH parses as a pipeline");
            };
            *parsed
        }] {
            let ClauseKind::Match {
                optional: true,
                patterns,
                ..
            } = &parsed.clauses[1].kind
            else {
                panic!("expected OPTIONAL MATCH");
            };
            assert_eq!(patterns[0].variable.as_deref(), Some("path"));
            let relationship = &patterns[0].steps[0].relationship;
            assert_eq!(relationship.search, PathSearch::AnyShortest);
            assert_eq!(relationship.direction, RelationshipDirection::Undirected);
            assert_eq!(
                (relationship.min_hops, relationship.max_hops),
                (1, UNBOUNDED_MAX_HOPS)
            );
            let ClauseKind::Return(projection) = &parsed.clauses[2].kind else {
                panic!("expected RETURN");
            };
            let ReturnExpressionKind::Value(case) = &projection.items[0].expression.kind else {
                panic!("expected CASE");
            };
            let ScalarExpressionKind::Case {
                operand: Some(operand),
                otherwise: Some(otherwise),
                ..
            } = &case.kind
            else {
                panic!("expected simple CASE: {case:?}");
            };
            assert!(matches!(
                operand.kind,
                ScalarExpressionKind::IsNull { negated: false, .. }
            ));
            assert!(matches!(
                &otherwise.kind,
                ScalarExpressionKind::Path(ShortestPathReturnExpression::Length { path_variable })
                    if path_variable == "path"
            ));
            let ReturnExpressionKind::Value(found) = &projection.items[1].expression.kind else {
                panic!("expected IS NOT NULL");
            };
            assert!(matches!(
                found.kind,
                ScalarExpressionKind::IsNull { negated: true, .. }
            ));
        }
        for (query, search, direction, hops) in [
            (
                "MATCH p = allShortestPaths((a)<-[:LINK*0..4]-(b)) RETURN nodes(p), relationships(p)",
                PathSearch::AllShortest,
                RelationshipDirection::Incoming,
                (0, 4),
            ),
            (
                "MATCH shortestPath((a)-[*..3]->(b)) RETURN a",
                PathSearch::AnyShortest,
                RelationshipDirection::Outgoing,
                (1, 3),
            ),
            (
                "MATCH p = (a)-[:LINK* ANY SHORTEST 0..]->(b) RETURN length(p)",
                PathSearch::AnyShortest,
                RelationshipDirection::Outgoing,
                (0, UNBOUNDED_MAX_HOPS),
            ),
        ] {
            let parsed = parse_pipeline(query).unwrap();
            let ClauseKind::Match { patterns, .. } = &parsed.clauses[0].kind else {
                panic!("expected MATCH");
            };
            let relationship = &patterns[0].steps[0].relationship;
            assert_eq!(relationship.search, search, "{query}");
            assert_eq!(relationship.direction, direction, "{query}");
            assert_eq!(
                (relationship.min_hops, relationship.max_hops),
                hops,
                "{query}"
            );
        }
        for query in [
            "MATCH (a)-[:LINK*]->(b) RETURN a",
            "MATCH p = shortestPath((a)-[:LINK* ALL SHORTEST]->(b)) RETURN a",
            "MATCH p = shortestPath((a)) RETURN a",
            "MATCH p = shortestPath((a)-[:LINK*]->(b) RETURN a",
        ] {
            assert!(parse_pipeline(query).is_err(), "{query}");
        }
    }

    #[test]
    fn shortest_paths_are_pattern_search_modes_with_regular_return_clauses() {
        let query = "MATCH route = (a:Item)-[links:LINK* ALL SHORTEST 1..4]->(b:Item) WHERE a.id = $id RETURN properties(nodes(route), 'id') AS ids, length(route) AS hops";
        let parsed = parse_pipeline(query).unwrap();
        let ClauseKind::Match { patterns, .. } = &parsed.clauses[0].kind else {
            panic!("expected MATCH")
        };
        assert_eq!(patterns[0].variable.as_deref(), Some("route"));
        let relationship = &patterns[0].steps[0].relationship;
        assert_eq!(relationship.search, PathSearch::AllShortest);
        assert_eq!(
            relationship.span.unwrap().text(query),
            Some("-[links:LINK* ALL SHORTEST 1..4]->")
        );
        let ClauseKind::Return(projection) = &parsed.clauses[1].kind else {
            panic!("expected RETURN")
        };
        assert_eq!(projection.items.len(), 2);
        assert_eq!(
            projection.items[1].expression.span.unwrap().text(query),
            Some("length(route)")
        );
    }

    #[test]
    fn pipeline_rejects_incomplete_queries_and_clauses_after_return() {
        for query in [
            "",
            "MATCH (n:Item)",
            "MATCH (n:Item) WITH n",
            "MATCH (n:Item) RETURN n MATCH (m:Item)",
            "MATCH (n:Item)-[:LINK*]->(m:Item) RETURN m",
            "MATCH (n:Item) RETURN n LIMIT",
        ] {
            assert!(parse_pipeline(query).is_err(), "{query}");
        }
    }
}

#[cfg(test)]
mod migration_audit {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn pipeline_covers_every_existing_query_family() {
        let mut totals = BTreeMap::<&str, usize>::new();
        let mut missing = BTreeMap::<&str, Vec<(String, String)>>::new();
        for line in include_str!("../../fixtures/migration_corpus_v1.jsonl").lines() {
            let case: serde_json::Value = serde_json::from_str(line).unwrap();
            if case["parse"] != "accepted" {
                continue;
            }
            let query = case["query"].as_str().unwrap();
            let statement =
                crate::parse(query).unwrap_or_else(|error| panic!("{}: {error}", case["id"]));
            let family = match statement {
                Statement::Pipeline(_) => "pipeline",
                Statement::CreateNode(_) | Statement::CreateRelationship(_) => "create",
                Statement::MergeNode(_) | Statement::MergeRelationship(_) => "merge",
                Statement::VectorSearch(_)
                | Statement::GraphAlgorithm(_)
                | Statement::ProjectGraph(_) => "procedure",
                _ => continue,
            };
            *totals.entry(family).or_default() += 1;
            if let Err(error) = parse_pipeline(query) {
                missing
                    .entry(family)
                    .or_default()
                    .push((case["id"].as_str().unwrap().to_string(), error.to_string()));
            }
        }
        assert_eq!(
            totals.values().sum::<usize>(),
            1327,
            "core query coverage must not shrink"
        );
        for (family, total) in &totals {
            let failures = missing.get(family).map(Vec::as_slice).unwrap_or_default();
            eprintln!(
                "{}",
                serde_json::json!({"family":family,"total":total,"missing":failures.len(),"examples":failures})
            );
        }
        assert!(
            missing.is_empty(),
            "clause migration has uncovered query families"
        );
    }
}
