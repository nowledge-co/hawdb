use super::super::ast::*;
use super::{Parser, MAX_CYPHER_INPUT_BYTES, MAX_CYPHER_PARSER_DEPTH};
use hawdb_core::{HawDBError, Result};

/// Parses ordered query clauses independently of statement-template lowering.
#[doc(hidden)]
pub fn parse_pipeline(input: &str) -> Result<QueryPipeline> {
    if input.len() > MAX_CYPHER_INPUT_BYTES {
        return Err(HawDBError::Parse(format!(
            "Cypher input exceeds maximum length of {MAX_CYPHER_INPUT_BYTES} bytes"
        )));
    }
    let mut parser = Parser::new(input);
    let pipeline = parser.parse_query_pipeline()?;
    parser.consume_char(';');
    parser.expect_eof()?;
    Ok(pipeline)
}

impl Parser<'_> {
    pub(super) fn parse_query_pipeline(&mut self) -> Result<QueryPipeline> {
        self.skip_ws();
        let start = self.pos;
        let mut clauses = Vec::new();
        let mut terminal = false;
        while !terminal {
            self.skip_ws();
            let clause_start = self.pos;
            let kind = if self.consume_keyword("UNWIND") {
                self.parse_pipeline_unwind()?
            } else if self.consume_keyword("MATCH") {
                self.parse_pipeline_match(false)?
            } else if self.consume_keyword("OPTIONAL") {
                self.expect_keyword("MATCH")?;
                self.parse_pipeline_match(true)?
            } else if self.consume_keyword("WITH") {
                ClauseKind::With(self.parse_pipeline_projection(true)?)
            } else if self.consume_keyword("RETURN") {
                terminal = true;
                ClauseKind::Return(self.parse_pipeline_projection(false)?)
            } else if self.consume_keyword("CALL") {
                self.parse_pipeline_call()?
            } else if self.consume_keyword("CREATE") {
                ClauseKind::Create(self.parse_pipeline_patterns()?)
            } else if self.consume_keyword("MERGE") {
                let pattern = self.parse_pipeline_pattern()?;
                let mut on_create = Vec::new();
                let mut on_match = Vec::new();
                while self.consume_keyword("ON") {
                    let sets = if self.consume_keyword("CREATE") {
                        &mut on_create
                    } else {
                        self.expect_keyword("MATCH")?;
                        &mut on_match
                    };
                    if !sets.is_empty() {
                        return Err(self.error("duplicate MERGE action"));
                    }
                    self.expect_keyword("SET")?;
                    *sets = self.parse_set_properties()?;
                }
                ClauseKind::Merge {
                    pattern,
                    on_create,
                    on_match,
                }
            } else if self.consume_keyword("SET") {
                ClauseKind::Set(self.parse_set_properties()?)
            } else if self.next_keyword_is("DETACH") || self.next_keyword_is("DELETE") {
                let detach = self.consume_keyword("DETACH");
                self.expect_keyword("DELETE")?;
                let mut variables = vec![self.parse_ident()?];
                while self.consume_char(',') {
                    variables.push(self.parse_ident()?);
                }
                ClauseKind::Delete { detach, variables }
            } else if !clauses.is_empty() && matches!(self.peek_char(), None | Some(';')) {
                break;
            } else {
                return Err(self.error("expected a query clause"));
            };
            clauses.push(self.source_node(kind, clause_start));
        }
        if !matches!(
            clauses.last().map(|clause| &clause.kind),
            Some(
                ClauseKind::Return(_)
                    | ClauseKind::Call { .. }
                    | ClauseKind::Create(_)
                    | ClauseKind::Merge { .. }
                    | ClauseKind::Set(_)
                    | ClauseKind::Delete { .. }
            )
        ) {
            return Err(self.error("query requires RETURN or a concluding mutation"));
        }
        Ok(self.source_node(QueryPipelineKind { clauses }, start))
    }

    fn parse_pipeline_unwind(&mut self) -> Result<ClauseKind> {
        let source = self.parse_value()?;
        if !matches!(
            source.kind,
            ValueExpressionKind::Parameter(_) | ValueExpressionKind::List(_)
        ) {
            return Err(self.error("UNWIND accepts only a parameter or list literal source"));
        }
        self.expect_keyword("AS")?;
        let variable = self.parse_ident()?;
        Ok(ClauseKind::Unwind { source, variable })
    }

    fn parse_pipeline_call(&mut self) -> Result<ClauseKind> {
        self.skip_ws();
        let start = self.pos;
        let name = self.parse_ident()?;
        self.expect_char('(')?;
        let kind = match name.to_ascii_lowercase().as_str() {
            "vector_search" => {
                ProcedureCallKind::VectorSearch(self.parse_vector_search_arguments()?)
            }
            "project_graph" => {
                let name = self.parse_string()?;
                self.expect_char(',')?;
                let node_labels = self.parse_string_list()?;
                self.expect_char(',')?;
                let rel_types = self.parse_project_graph_rel_types()?;
                self.skip_procedure_args_tail()?;
                ProcedureCallKind::ProjectGraph {
                    name,
                    node_labels,
                    rel_types,
                }
            }
            "page_rank" | "pagerank" | "louvain" => {
                let algorithm = if name.eq_ignore_ascii_case("louvain") {
                    GraphAlgorithmKind::Louvain
                } else {
                    GraphAlgorithmKind::PageRank
                };
                let graph_name = self.parse_string()?;
                let options = self.parse_graph_algorithm_options()?;
                ProcedureCallKind::GraphAlgorithm {
                    algorithm,
                    graph_name,
                    options,
                }
            }
            _ => return Err(self.error("unsupported procedure")),
        };
        let procedure = self.source_node(kind, start);
        let mut yields = Vec::new();
        if self.consume_keyword("YIELD") {
            loop {
                self.skip_ws();
                let start = self.pos;
                let name = self.parse_ident()?;
                let alias = if self.consume_keyword("AS") {
                    Some(self.parse_ident()?)
                } else {
                    None
                };
                yields.push(self.source_node(YieldItemKind { name, alias }, start));
                if !self.consume_char(',') {
                    break;
                }
            }
        }
        Ok(ClauseKind::Call { procedure, yields })
    }

    fn parse_pipeline_match(&mut self, optional: bool) -> Result<ClauseKind> {
        let patterns = self.parse_pipeline_patterns()?;
        let predicate = self.parse_pipeline_where()?;
        Ok(ClauseKind::Match {
            optional,
            patterns,
            predicate,
        })
    }

    fn parse_pipeline_where(&mut self) -> Result<Option<PredicateExpression>> {
        self.skip_ws();
        if !self.consume_keyword("WHERE") {
            return Ok(None);
        }
        self.skip_ws();
        let start = self.pos;
        let predicate = self.parse_predicate(true)?;
        Ok(Some(self.source_node(predicate, start)))
    }

    fn parse_pipeline_projection(&mut self, with: bool) -> Result<ProjectionClause> {
        let distinct = self.consume_keyword("DISTINCT");
        let items = self.parse_pipeline_return_items()?;
        let predicate = if with {
            self.parse_pipeline_where()?
        } else {
            None
        };
        let order_by = if self.consume_keyword("ORDER") {
            self.expect_keyword("BY")?;
            self.parse_order_items()?
        } else {
            Vec::new()
        };
        let offset = if self.consume_keyword("SKIP") || self.consume_keyword("OFFSET") {
            Some(self.parse_value()?)
        } else {
            None
        };
        let limit = if self.consume_keyword("LIMIT") {
            Some(self.parse_value()?)
        } else {
            None
        };
        Ok(ProjectionClause {
            distinct,
            items,
            predicate,
            order_by,
            offset,
            limit,
        })
    }

    fn parse_pipeline_return_items(&mut self) -> Result<Vec<ReturnItem>> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            let start = self.pos;
            let expression = self.parse_pipeline_arithmetic(false)?;
            if arithmetic_depth(&expression) > MAX_CYPHER_PARSER_DEPTH {
                return Err(self.error("arithmetic expression exceeds maximum nesting depth"));
            }
            let alias = if self.consume_keyword("AS") {
                Some(self.parse_ident()?)
            } else {
                None
            };
            items.push(self.source_node(ReturnItemKind { expression, alias }, start));
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_pipeline_arithmetic(&mut self, product: bool) -> Result<ReturnExpression> {
        self.skip_ws();
        let start = self.pos;
        let mut first = if product {
            self.parse_pipeline_return_atom()?
        } else {
            self.parse_pipeline_arithmetic(true)?
        };
        let mut rest = Vec::new();
        loop {
            let op = if product && self.consume_char('*') {
                ArithmeticOp::Multiply
            } else if product && self.consume_char('/') {
                ArithmeticOp::Divide
            } else if product && self.consume_char('%') {
                ArithmeticOp::Remainder
            } else if !product && self.consume_char('+') {
                ArithmeticOp::Add
            } else if !product && self.consume_char('-') {
                ArithmeticOp::Subtract
            } else {
                break;
            };
            let right = if product {
                self.parse_pipeline_return_atom()?
            } else {
                self.parse_pipeline_arithmetic(true)?
            };
            rest.push((op, right));
        }
        if !rest.is_empty() {
            first = self.source_node(
                ReturnExpressionKind::Arithmetic {
                    first: Box::new(first),
                    rest,
                },
                start,
            );
        }
        Ok(first)
    }

    fn parse_pipeline_return_atom(&mut self) -> Result<ReturnExpression> {
        self.with_recursion(|parser| {
            parser.skip_ws();
            let start = parser.pos;
            if parser.consume_char('(') {
                let expression = parser.parse_pipeline_arithmetic(false)?;
                parser.expect_char(')')?;
                return Ok(parser.source_node(expression.kind, start));
            }
            let kind = if parser.consume_pipeline_function("PROPERTIES") {
                parser.expect_keyword("NODES")?;
                parser.expect_char('(')?;
                let path_variable = parser.parse_ident()?;
                parser.expect_char(')')?;
                parser.expect_char(',')?;
                let property = match parser.parse_value()?.kind {
                    ValueExpressionKind::Literal(hawdb_core::Value::String(property)) => property,
                    _ => return Err(parser.error("node property projection requires a string key")),
                };
                parser.expect_char(')')?;
                ReturnExpressionKind::Path(ShortestPathReturnExpression::NodePropertyList {
                    path_variable,
                    property,
                })
            } else if parser.consume_pipeline_function("LENGTH") {
                let path_variable = parser.parse_ident()?;
                parser.expect_char(')')?;
                ReturnExpressionKind::Path(ShortestPathReturnExpression::Length { path_variable })
            } else {
                parser.parse_return_atom()?
            };
            Ok(parser.source_node(kind, start))
        })
    }

    fn consume_pipeline_function(&mut self, name: &str) -> bool {
        let checkpoint = self.checkpoint();
        if self.consume_keyword(name) && self.consume_char('(') {
            return true;
        }
        self.restore(checkpoint);
        false
    }

    fn parse_pipeline_patterns(&mut self) -> Result<Vec<MatchPattern>> {
        let mut patterns = vec![self.parse_pipeline_pattern()?];
        while self.consume_char(',') {
            patterns.push(self.parse_pipeline_pattern()?);
        }
        Ok(patterns)
    }

    fn parse_pipeline_pattern(&mut self) -> Result<MatchPattern> {
        self.skip_ws();
        let start = self.pos;
        let variable = if self.peek_char() == Some('(') {
            None
        } else {
            let variable = self.parse_ident()?;
            self.expect_char('=')?;
            Some(variable)
        };
        let first = self.parse_pipeline_node()?;
        let mut steps = Vec::new();
        loop {
            self.skip_ws();
            let relationship_start = self.pos;
            let incoming = self.consume_char('<');
            if !incoming && !self.consume_char('-') {
                break;
            }
            if incoming {
                self.expect_char('-')?;
            }
            let ((variable, rel_type, properties, min_hops, max_hops), search) =
                self.parse_match_relationship_pattern_with_search(true)?;
            self.expect_char('-')?;
            let direction = if incoming {
                RelationshipDirection::Incoming
            } else if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let relationship = self.source_node(
                RelationshipPatternKind {
                    variable,
                    rel_type,
                    properties,
                    direction,
                    min_hops,
                    max_hops,
                    search,
                },
                relationship_start,
            );
            let target = self.parse_pipeline_node()?;
            steps.push(self.source_node(
                PatternStepKind {
                    relationship,
                    target,
                },
                relationship_start,
            ));
        }
        Ok(self.source_node(
            MatchPatternKind {
                variable,
                first,
                steps,
            },
            start,
        ))
    }

    fn parse_pipeline_node(&mut self) -> Result<NodePattern> {
        self.skip_ws();
        let start = self.pos;
        let previous_anonymous_id = self.anonymous_variable_id;
        let (variable, label, properties) = self.parse_match_node_pattern()?;
        Ok(self.source_node(
            NodePatternKind {
                variable,
                anonymous: self.anonymous_variable_id != previous_anonymous_id,
                label,
                properties,
            },
            start,
        ))
    }
}

fn arithmetic_depth(expression: &ReturnExpression) -> usize {
    match &expression.kind {
        ReturnExpressionKind::Arithmetic { first, rest } => rest
            .iter()
            .fold(arithmetic_depth(first), |depth, (_, right)| {
                depth.max(arithmetic_depth(right)) + 1
            }),
        _ => 1,
    }
}

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
            let query = case["query"].as_str().unwrap();
            let Ok(statement) = crate::parse(query) else {
                continue;
            };
            let family = match statement {
                Statement::MatchReturn(_) => "match_return",
                Statement::MatchNodesReturn(_) => "match_nodes_return",
                Statement::Pipeline(_) => "pipeline",
                Statement::MatchOptionalRelationshipCountSum(_) => "optional_count_sum",
                Statement::ShortestPathReturn(_) => "shortest_path",
                Statement::MatchSet(_) | Statement::MatchSetReturn(_) => "match_set",
                Statement::MatchDelete(_) => "match_delete",
                Statement::MatchCreateRelationship(_) => "match_create_relationship",
                Statement::MatchMergeRelationship(_) => "match_merge_relationship",
                Statement::MatchExpandMergeRelationship(_)
                | Statement::MatchExpandMatchMergeRelationship(_) => "match_expand_merge",
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
