use std::collections::BTreeSet;

use skein_core::Result;

use super::super::ast::*;
use super::Parser;

struct ParsedWithClause {
    optional_with: Option<OptionalWithAggregate>,
    collect_with: Option<WithCollect>,
    distinct_with: Option<WithDistinctProjection>,
    with_projection: Option<WithProjection>,
    aggregate_with: Option<WithAggregateProjection>,
}

struct ParsedOptionalCountTerm {
    variable: String,
    distinct: bool,
}

impl ParsedOptionalCountTerm {
    fn display_variable(&self) -> String {
        if self.distinct {
            format!("DISTINCT {}", self.variable)
        } else {
            self.variable.clone()
        }
    }
}

impl Parser<'_> {
    pub(super) fn parse_match_statement(&mut self) -> Result<Statement> {
        let path_variable = self.consume_match_path_binding_prefix();
        let (variable, label, properties) = self.parse_match_node_pattern()?;
        if let Some(path_variable) = path_variable.as_ref()
            && self.next_relationship_pattern_is_all_shortest()
        {
            return self.parse_shortest_path_return(
                path_variable.clone(),
                variable,
                label,
                properties,
            );
        }
        if self.consume_char(',') || self.consume_keyword("MATCH") {
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            let predicate = if self.consume_keyword("WHERE") {
                Some(self.parse_property_predicate()?)
            } else {
                None
            };
            if self.consume_keyword("RETURN") {
                let returns = self.parse_return_items()?;
                let limit = if self.consume_keyword("LIMIT") {
                    Some(self.parse_value()?)
                } else {
                    None
                };
                return Ok(Statement::MatchNodesReturn(MatchNodesReturn {
                    left_variable: variable,
                    left_label: label,
                    left_properties: properties,
                    right_variable: target_variable,
                    right_label: target_label,
                    right_properties: target_properties,
                    predicate,
                    returns,
                    limit,
                }));
            }
            if self.consume_keyword("MERGE") {
                let merge_pattern = self.parse_bound_relationship_merge_pattern()?;
                let on_create_sets = if self.consume_keyword("ON") {
                    self.expect_keyword("CREATE")?;
                    self.expect_keyword("SET")?;
                    self.parse_set_properties()?
                } else {
                    Vec::new()
                };
                return Ok(Statement::MatchMergeRelationship(MatchMergeRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    target_variable,
                    target_label,
                    target_properties,
                    predicate,
                    merge_source_variable: merge_pattern.source_variable,
                    rel_variable: merge_pattern.rel_variable,
                    rel_type: merge_pattern.rel_type,
                    rel_properties: merge_pattern.rel_properties,
                    merge_target_variable: merge_pattern.target_variable,
                    on_create_sets,
                }));
            }
            self.expect_keyword("CREATE")?;
            let (create_source_variable, rel_type, rel_properties, create_target_variable) =
                self.parse_bound_relationship_create_pattern()?;
            return Ok(Statement::MatchCreateRelationship(
                MatchCreateRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    target_variable,
                    target_label,
                    target_properties,
                    predicate,
                    create_source_variable,
                    rel_type,
                    rel_properties,
                    create_target_variable,
                },
            ));
        }
        let expand = if self.consume_char('<') {
            self.expect_char('-')?;
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            self.expect_char('-')?;
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            Some(RelationshipExpand {
                variable: rel_variable,
                rel_type,
                properties,
                direction: RelationshipDirection::Incoming,
                target_variable,
                target_label,
                target_properties,
                min_hops,
                max_hops,
            })
        } else if self.consume_char('-') {
            let (rel_variable, rel_type, properties, min_hops, max_hops) =
                self.parse_match_relationship_pattern()?;
            self.expect_char('-')?;
            let direction = if self.consume_char('>') {
                RelationshipDirection::Outgoing
            } else {
                RelationshipDirection::Undirected
            };
            let (target_variable, target_label, target_properties) =
                self.parse_match_node_pattern()?;
            Some(RelationshipExpand {
                variable: rel_variable,
                rel_type,
                properties,
                direction,
                target_variable,
                target_label,
                target_properties,
                min_hops,
                max_hops,
            })
        } else {
            None
        };
        let inline_post_match_expand = if let Some(expand) = expand.as_ref() {
            self.parse_post_match_relationship_expand(
                &expand.target_variable,
                &expand.target_label,
                &expand.target_properties,
            )?
        } else {
            None
        };
        let mut predicate = if self.consume_keyword("WHERE") {
            Some(self.parse_property_predicate()?)
        } else {
            None
        };
        if self.consume_keyword("MERGE") {
            let Some(expand) = expand else {
                return Err(self.error("MATCH MERGE requires a bound relationship pattern"));
            };
            let merge_pattern = self.parse_bound_relationship_merge_pattern()?;
            let on_create_sets = if self.consume_keyword("ON") {
                self.expect_keyword("CREATE")?;
                self.expect_keyword("SET")?;
                self.parse_set_properties()?
            } else {
                Vec::new()
            };
            return Ok(Statement::MatchExpandMergeRelationship(
                MatchExpandMergeRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    expand,
                    predicate,
                    merge_source_variable: merge_pattern.source_variable,
                    rel_variable: merge_pattern.rel_variable,
                    rel_type: merge_pattern.rel_type,
                    rel_properties: merge_pattern.rel_properties,
                    merge_target_variable: merge_pattern.target_variable,
                    on_create_sets,
                },
            ));
        }
        if self.consume_char(',') || self.consume_keyword("MATCH") {
            let (matched_target_variable, matched_target_label, matched_target_properties) =
                self.parse_match_node_pattern()?;
            let post_match_expand = self.parse_post_match_relationship_expand(
                &matched_target_variable,
                &matched_target_label,
                &matched_target_properties,
            )?;
            if let Some(post_match_expand) = post_match_expand {
                if self.consume_keyword("WHERE") {
                    predicate = Some(combine_match_predicates(
                        predicate,
                        self.parse_property_predicate()?,
                    ));
                }
                if !self.consume_keyword("RETURN") {
                    return Err(self.error("expected RETURN"));
                }
                let distinct = self.consume_keyword("DISTINCT");
                let returns = self.parse_return_items()?;
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
                return Ok(Statement::MatchReturn(Box::new(MatchReturn {
                    vector_seed: None,
                    variable,
                    label,
                    properties,
                    expand,
                    post_match_expand: Some(post_match_expand),
                    optional_expand: None,
                    optional_with: None,
                    collect_with: None,
                    distinct_with: None,
                    with_projection: None,
                    with_order_by: Vec::new(),
                    with_offset: None,
                    with_limit: None,
                    aggregate_with: None,
                    aggregate_with_filter: None,
                    post_with_match: None,
                    predicate,
                    distinct,
                    returns,
                    order_by,
                    offset,
                    limit,
                })));
            }
            if self.consume_keyword("WHERE") {
                predicate = Some(combine_match_predicates(
                    predicate,
                    self.parse_property_predicate()?,
                ));
            }
            self.expect_keyword("MERGE")?;
            let Some(expand) = expand else {
                return Err(self.error("MATCH MERGE requires a bound relationship pattern"));
            };
            let merge_pattern = self.parse_bound_relationship_merge_pattern()?;
            let on_create_sets = if self.consume_keyword("ON") {
                self.expect_keyword("CREATE")?;
                self.expect_keyword("SET")?;
                self.parse_set_properties()?
            } else {
                Vec::new()
            };
            return Ok(Statement::MatchExpandMatchMergeRelationship(
                MatchExpandMatchMergeRelationship {
                    source_variable: variable,
                    source_label: label,
                    source_properties: properties,
                    expand,
                    matched_target_variable,
                    matched_target_label,
                    matched_target_properties,
                    predicate,
                    merge_source_variable: merge_pattern.source_variable,
                    rel_variable: merge_pattern.rel_variable,
                    rel_type: merge_pattern.rel_type,
                    rel_properties: merge_pattern.rel_properties,
                    merge_target_variable: merge_pattern.target_variable,
                    on_create_sets,
                },
            ));
        }
        let optional_expand = if self.consume_keyword("OPTIONAL") {
            self.expect_keyword("MATCH")?;
            let thread_repair_start = self.checkpoint();
            if let Ok(statement) =
                self.parse_thread_repair_stats_after_optional_match(&variable, &label)
            {
                return Ok(statement);
            }
            self.restore(thread_repair_start);
            let mut scope = BTreeSet::from([variable.clone()]);
            if let Some(expand) = &expand {
                scope.insert(expand.target_variable.clone());
                if let Some(rel_variable) = &expand.variable {
                    scope.insert(rel_variable.clone());
                }
            }
            Some(self.parse_optional_relationship_expand(&scope)?)
        } else {
            None
        };
        if let Some(first_optional) = &optional_expand
            && (self.next_keyword_is("WHERE") || self.next_keyword_is("OPTIONAL"))
        {
            let first_filter = if self.consume_keyword("WHERE") {
                Some(self.parse_optional_relationship_count_filter(first_optional)?)
            } else {
                None
            };
            if self.consume_keyword("OPTIONAL") {
                self.expect_keyword("MATCH")?;
                let second_optional =
                    self.parse_optional_relationship_expand(&BTreeSet::from([variable.clone()]))?;
                let second_filter = if self.consume_keyword("WHERE") {
                    Some(self.parse_optional_relationship_count_filter(&second_optional)?)
                } else {
                    None
                };
                self.expect_keyword("RETURN")?;
                let (first_count, second_count, output) =
                    self.parse_optional_relationship_count_sum_return()?;
                let first_leg = self.optional_relationship_count_leg(
                    first_optional,
                    first_filter,
                    &first_count,
                )?;
                let second_leg = self.optional_relationship_count_leg(
                    &second_optional,
                    second_filter,
                    &second_count,
                )?;
                return Ok(Statement::MatchOptionalRelationshipCountSum(
                    MatchOptionalRelationshipCountSum {
                        variable,
                        label,
                        properties,
                        legs: vec![first_leg, second_leg],
                        output,
                    },
                ));
            }
        }
        let with_clause = if self.consume_keyword("WITH") {
            self.parse_with_clause()?
        } else {
            ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                with_projection: None,
                aggregate_with: None,
            }
        };
        let aggregate_with_filter = if (with_clause.aggregate_with.is_some()
            || with_clause.optional_with.is_some()
            || with_clause.with_projection.is_some())
            && self.consume_keyword("WHERE")
        {
            Some(self.parse_with_alias_filter()?)
        } else {
            None
        };
        let mut with_order_by = Vec::new();
        let mut with_offset = None;
        let mut with_limit = None;
        if with_clause.aggregate_with.is_some()
            || with_clause.optional_with.is_some()
            || with_clause.with_projection.is_some()
        {
            if self.consume_keyword("ORDER") {
                self.expect_keyword("BY")?;
                with_order_by = self.parse_order_items()?;
            }
            if self.consume_keyword("SKIP") || self.consume_keyword("OFFSET") {
                with_offset = Some(self.parse_value()?);
            }
            if self.consume_keyword("LIMIT") {
                with_limit = Some(self.parse_value()?);
            }
        }
        let post_with_match = if with_clause.aggregate_with.is_some() {
            if self.consume_keyword("OPTIONAL") {
                self.expect_keyword("MATCH")?;
                Some(self.parse_post_with_node_lookup(true)?)
            } else if self.consume_keyword("MATCH") {
                Some(self.parse_post_with_node_lookup(false)?)
            } else {
                None
            }
        } else {
            None
        };
        if self.consume_keyword("SET") {
            let update = MatchSet {
                variable,
                label,
                properties,
                expand,
                predicate,
                sets: self.parse_set_properties()?,
            };
            if self.consume_keyword("RETURN") {
                return Ok(Statement::MatchSetReturn(MatchSetReturn {
                    update,
                    returns: self.parse_return_items()?,
                }));
            }
            return Ok(Statement::MatchSet(update));
        }
        let detach = self.consume_keyword("DETACH");
        if detach || self.next_keyword_is("DELETE") {
            if !self.consume_keyword("DELETE") {
                return Err(self.error("expected DELETE"));
            }
            let delete_variable = self.parse_ident()?;
            return Ok(Statement::MatchDelete(MatchDelete {
                variable,
                label,
                properties,
                expand,
                predicate,
                delete_variable,
                detach,
            }));
        }
        if !self.consume_keyword("RETURN") {
            return Err(self.error("expected RETURN"));
        }
        let distinct = self.consume_keyword("DISTINCT");
        let returns = self.parse_return_items()?;
        let with_projection_has_window = with_clause.with_projection.is_some();
        let order_by = if self.consume_keyword("ORDER") {
            if !with_order_by.is_empty() {
                return Err(self.error("ORDER BY is already attached to WITH"));
            }
            self.expect_keyword("BY")?;
            self.parse_order_items()?
        } else if with_projection_has_window {
            Vec::new()
        } else {
            with_order_by.clone()
        };
        let offset = if self.consume_keyword("SKIP") || self.consume_keyword("OFFSET") {
            if with_offset.is_some() {
                return Err(self.error("offset is already attached to WITH"));
            }
            Some(self.parse_value()?)
        } else if with_projection_has_window {
            None
        } else {
            with_offset.clone()
        };
        let limit = if self.consume_keyword("LIMIT") {
            if with_limit.is_some() {
                return Err(self.error("LIMIT is already attached to WITH"));
            }
            Some(self.parse_value()?)
        } else if with_projection_has_window {
            None
        } else {
            with_limit.clone()
        };
        let with_order_by = if with_projection_has_window {
            with_order_by
        } else {
            Vec::new()
        };
        let with_offset = if with_projection_has_window {
            with_offset
        } else {
            None
        };
        let with_limit = if with_projection_has_window {
            with_limit
        } else {
            None
        };
        Ok(Statement::MatchReturn(Box::new(MatchReturn {
            vector_seed: None,
            variable,
            label,
            properties,
            expand,
            post_match_expand: inline_post_match_expand,
            optional_expand,
            optional_with: with_clause.optional_with,
            collect_with: with_clause.collect_with,
            distinct_with: with_clause.distinct_with,
            with_projection: with_clause.with_projection,
            with_order_by,
            with_offset,
            with_limit,
            aggregate_with: with_clause.aggregate_with,
            aggregate_with_filter,
            post_with_match,
            predicate,
            distinct,
            returns,
            order_by,
            offset,
            limit,
        })))
    }

    fn parse_shortest_path_return(
        &mut self,
        path_variable: String,
        source_variable: String,
        source_label: String,
        source_properties: std::collections::BTreeMap<String, ValueExpression>,
    ) -> Result<Statement> {
        self.expect_char('-')?;
        let (rel_variable, rel_type, min_hops, max_hops) =
            self.parse_all_shortest_relationship_pattern()?;
        self.expect_char('-')?;
        let direction = if self.consume_char('>') {
            RelationshipDirection::Outgoing
        } else {
            RelationshipDirection::Undirected
        };
        let (target_variable, target_label, target_properties) = self.parse_match_node_pattern()?;
        let predicate = if self.consume_keyword("WHERE") {
            Some(self.parse_property_predicate()?)
        } else {
            None
        };
        self.expect_keyword("RETURN")?;
        Ok(Statement::ShortestPathReturn(Box::new(
            ShortestPathReturn {
                path_variable: path_variable.clone(),
                source_variable,
                source_label,
                source_properties,
                rel_variable,
                rel_type,
                direction,
                target_variable,
                target_label,
                target_properties,
                min_hops,
                max_hops,
                predicate,
                returns: self.parse_shortest_path_return_items(&path_variable)?,
            },
        )))
    }

    fn parse_all_shortest_relationship_pattern(
        &mut self,
    ) -> Result<(Option<String>, String, usize, usize)> {
        self.expect_char('[')?;
        let (variable, rel_type) = if self.peek_char() == Some('*') {
            (None, String::new())
        } else if self.consume_char(':') {
            (None, self.parse_ident()?)
        } else {
            let variable = self.parse_ident()?;
            let rel_type = if self.consume_char(':') {
                self.parse_ident()?
            } else {
                String::new()
            };
            (Some(variable), rel_type)
        };
        self.expect_char('*')?;
        self.expect_keyword("ALL")?;
        self.expect_keyword("SHORTEST")?;
        let (min_hops, max_hops) = self.parse_bounded_hops()?;
        self.expect_char(']')?;
        Ok((variable, rel_type, min_hops, max_hops))
    }

    fn parse_shortest_path_return_items(
        &mut self,
        path_variable: &str,
    ) -> Result<Vec<ShortestPathReturnItem>> {
        let mut items = Vec::new();
        loop {
            let expression = if self.consume_keyword("PROPERTIES") {
                self.expect_char('(')?;
                self.expect_keyword("NODES")?;
                self.expect_char('(')?;
                let nodes_path_variable = self.parse_ident()?;
                self.expect_char(')')?;
                self.expect_char(',')?;
                let property = match self.parse_value()?.kind {
                    ValueExpressionKind::Literal(skein_core::Value::String(property)) => property,
                    _ => return Err(self.error("node property projection requires a string key")),
                };
                self.expect_char(')')?;
                ShortestPathReturnExpression::NodePropertyList {
                    path_variable: nodes_path_variable,
                    property,
                }
            } else if self.consume_keyword("LENGTH") {
                self.expect_char('(')?;
                let length_path_variable = self.parse_ident()?;
                self.expect_char(')')?;
                ShortestPathReturnExpression::Length {
                    path_variable: length_path_variable,
                }
            } else {
                return Err(self.error("expected shortest path return expression"));
            };
            self.expect_keyword("AS")?;
            let alias = self.parse_ident()?;
            match &expression {
                ShortestPathReturnExpression::NodePropertyList {
                    path_variable: expression_path,
                    ..
                }
                | ShortestPathReturnExpression::Length {
                    path_variable: expression_path,
                } if expression_path != path_variable => {
                    return Err(self.error("shortest path return references a different path"));
                }
                _ => {}
            }
            items.push(ShortestPathReturnItem { expression, alias });
            if !self.consume_char(',') {
                break;
            }
        }
        Ok(items)
    }

    fn parse_post_with_node_lookup(&mut self, optional: bool) -> Result<PostWithNodeLookup> {
        let (variable, label, properties) = self.parse_match_node_pattern()?;
        if !properties.is_empty() {
            return Err(self.error("post-WITH MATCH lookup does not support node properties"));
        }
        self.expect_keyword("WHERE")?;
        let predicate_variable = self.parse_ident()?;
        if predicate_variable != variable {
            return Err(self.error("post-WITH MATCH lookup variable mismatch"));
        }
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_char('=')?;
        let column = self.parse_ident()?;
        Ok(PostWithNodeLookup {
            variable,
            label,
            property,
            column,
            optional,
        })
    }

    fn parse_with_clause(&mut self) -> Result<ParsedWithClause> {
        if self.consume_keyword("DISTINCT") {
            let items = self.parse_return_items()?;
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: Some(WithDistinctProjection { items }),
                with_projection: None,
                aggregate_with: None,
            });
        }
        if self.next_keyword_is("date_part") {
            let items = self.parse_return_items()?;
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                with_projection: None,
                aggregate_with: Some(WithAggregateProjection { items }),
            });
        }
        if self.next_keyword_is("CASE") {
            let items = self.parse_return_items()?;
            let has_aggregate = items
                .iter()
                .any(|item| matches!(item.expression.kind, ReturnExpressionKind::Aggregate(_)));
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                with_projection: (!has_aggregate).then_some(WithProjection {
                    items: items.clone(),
                }),
                aggregate_with: has_aggregate.then_some(WithAggregateProjection { items }),
            });
        }
        self.skip_ws();
        let group_start = self.pos;
        let group_variable = self.parse_ident()?;
        let group_span = self.source_span(group_start);
        if self.consume_char('.') {
            let property = self.parse_ident()?;
            let value = self.source_node(
                ScalarExpressionKind::Property {
                    variable: group_variable,
                    property,
                },
                group_start,
            );
            let expression = self.source_node(ReturnExpressionKind::Value(value), group_start);
            let alias = if self.consume_keyword("AS") {
                Some(self.parse_ident()?)
            } else {
                None
            };
            let first_item = self.source_node(ReturnItemKind { expression, alias }, group_start);
            let mut items = vec![first_item];
            while self.consume_char(',') {
                let mut next = self.parse_return_items()?;
                items.append(&mut next);
            }
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                with_projection: None,
                aggregate_with: Some(WithAggregateProjection { items }),
            });
        }
        self.expect_char(',')?;
        self.skip_ws();
        let aggregate_start = self.pos;
        if self.consume_keyword("COUNT") {
            let first_count = self.parse_count_return_item_after_count_keyword(aggregate_start)?;
            let is_simple_optional_count = matches!(
                first_count.expression.kind,
                ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable { .. })
            ) && !self.peek_next_with_item_separator();
            if is_simple_optional_count {
                let ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                    variable,
                    distinct,
                }) = first_count.kind.expression.kind
                else {
                    unreachable!("simple optional count shape checked above");
                };
                let alias = first_count
                    .kind
                    .alias
                    .expect("count return items always require an alias in WITH");
                return Ok(ParsedWithClause {
                    optional_with: Some(OptionalWithAggregate {
                        group_variable,
                        count_variable: variable,
                        distinct,
                        alias,
                    }),
                    collect_with: None,
                    distinct_with: None,
                    with_projection: None,
                    aggregate_with: None,
                });
            }
            let mut items = vec![
                variable_return_item(group_variable, group_span),
                first_count,
            ];
            while self.consume_char(',') {
                let mut next = self.parse_return_items()?;
                items.append(&mut next);
            }
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                with_projection: None,
                aggregate_with: Some(WithAggregateProjection { items }),
            });
        }
        if self.consume_keyword("COLLECT") {
            self.expect_char('(')?;
            let distinct = self.consume_keyword("DISTINCT");
            let collect_variable = self.parse_ident()?;
            let collect_property = if self.consume_char('.') {
                Some(self.parse_ident()?)
            } else {
                None
            };
            self.expect_char(')')?;
            let expression_span = self.source_span(aggregate_start);
            self.expect_keyword("AS")?;
            let alias = self.parse_ident()?;
            let first_collect = self.source_node(
                ReturnItemKind {
                    expression: AstNode::from_source(
                        if let Some(property) = collect_property.clone() {
                            ReturnExpressionKind::Aggregate(AggregateExpression::CollectProperty {
                                variable: collect_variable.clone(),
                                property,
                                distinct,
                            })
                        } else {
                            ReturnExpressionKind::Aggregate(AggregateExpression::CollectVariable {
                                variable: collect_variable.clone(),
                                distinct,
                            })
                        },
                        expression_span,
                    ),
                    alias: Some(alias.clone()),
                },
                aggregate_start,
            );
            if self.consume_char(',') {
                let mut items = vec![
                    variable_return_item(group_variable, group_span),
                    first_collect,
                ];
                let mut next = self.parse_return_items()?;
                items.append(&mut next);
                return Ok(ParsedWithClause {
                    optional_with: None,
                    collect_with: None,
                    distinct_with: None,
                    with_projection: None,
                    aggregate_with: Some(WithAggregateProjection { items }),
                });
            }
            let Some(collect_property) = collect_property else {
                return Ok(ParsedWithClause {
                    optional_with: None,
                    collect_with: None,
                    distinct_with: None,
                    with_projection: None,
                    aggregate_with: Some(WithAggregateProjection {
                        items: vec![
                            variable_return_item(group_variable, group_span),
                            first_collect,
                        ],
                    }),
                });
            };
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: Some(WithCollect {
                    group_variable,
                    collect_variable,
                    collect_property,
                    distinct,
                    alias,
                }),
                distinct_with: None,
                with_projection: None,
                aggregate_with: None,
            });
        }
        if self.next_keyword_is("CASE") {
            let mut items = vec![variable_return_item(group_variable, group_span)];
            let mut projections = self.parse_return_items()?;
            items.append(&mut projections);
            return Ok(ParsedWithClause {
                optional_with: None,
                collect_with: None,
                distinct_with: None,
                with_projection: Some(WithProjection { items }),
                aggregate_with: None,
            });
        }
        Err(self.error("expected COUNT or COLLECT"))
    }

    fn parse_count_return_item_after_count_keyword(&mut self, start: usize) -> Result<ReturnItem> {
        self.expect_char('(')?;
        let distinct = self.consume_keyword("DISTINCT");
        let expression = if self.consume_char('*') {
            if distinct {
                return Err(self.error("COUNT(DISTINCT *) is not supported"));
            }
            ReturnExpressionKind::Aggregate(AggregateExpression::CountAll)
        } else {
            let variable = self.parse_ident()?;
            if self.consume_char('.') {
                ReturnExpressionKind::Aggregate(AggregateExpression::CountProperty {
                    variable,
                    property: self.parse_ident()?,
                    distinct,
                })
            } else {
                ReturnExpressionKind::Aggregate(AggregateExpression::CountVariable {
                    variable,
                    distinct,
                })
            }
        };
        self.expect_char(')')?;
        let expression = self.source_node(expression, start);
        self.expect_keyword("AS")?;
        let alias = self.parse_ident()?;
        Ok(self.source_node(
            ReturnItemKind {
                expression,
                alias: Some(alias),
            },
            start,
        ))
    }

    fn parse_thread_repair_stats_after_optional_match(
        &mut self,
        variable: &str,
        label: &str,
    ) -> Result<Statement> {
        let (identity_variable, identity_label, identity_properties) =
            self.parse_match_node_pattern()?;
        if !identity_properties.is_empty() {
            return Err(self.error("thread repair identity optional match cannot use properties"));
        }
        self.expect_keyword("WHERE")?;
        let where_identity_variable = self.parse_ident()?;
        if where_identity_variable != identity_variable {
            return Err(self.error("thread repair identity WHERE must use the identity variable"));
        }
        self.expect_char('.')?;
        let identity_ref_property = self.parse_ident()?;
        self.expect_char('=')?;
        let where_thread_variable = self.parse_ident()?;
        if where_thread_variable != variable {
            return Err(
                self.error("thread repair identity WHERE must reference the thread variable")
            );
        }
        self.expect_char('.')?;
        let thread_id_property = self.parse_ident()?;

        self.expect_keyword("WITH")?;
        let with_thread_variable = self.parse_ident()?;
        if with_thread_variable != variable {
            return Err(self.error("thread repair WITH must keep the thread variable"));
        }
        self.expect_char(',')?;
        let (identity_count_variable, identity_refs_alias) = self.parse_count_alias()?;
        if identity_count_variable != identity_variable || identity_refs_alias != "identity_refs" {
            return Err(self.error("thread repair WITH must count identity_refs"));
        }

        self.expect_keyword("OPTIONAL")?;
        self.expect_keyword("MATCH")?;
        let message_optional =
            self.parse_optional_relationship_expand(&BTreeSet::from([variable.to_string()]))?;
        if message_optional.source_variable != variable {
            return Err(self.error("thread repair message optional must start from thread"));
        }

        self.expect_keyword("WITH")?;
        let second_with_thread_variable = self.parse_ident()?;
        if second_with_thread_variable != variable {
            return Err(self.error("thread repair second WITH must keep the thread variable"));
        }
        self.expect_char(',')?;
        let identity_refs_column = self.parse_ident()?;
        if identity_refs_column != "identity_refs" {
            return Err(self.error("thread repair second WITH must keep identity_refs"));
        }
        self.expect_char(',')?;
        let (message_count_variable, legacy_messages_alias) = self.parse_count_alias()?;
        if message_count_variable != message_optional.expand.target_variable
            || legacy_messages_alias != "legacy_messages"
        {
            return Err(self.error("thread repair second WITH must count legacy_messages"));
        }

        self.expect_keyword("OPTIONAL")?;
        self.expect_keyword("MATCH")?;
        let memory_optional =
            self.parse_optional_relationship_expand(&BTreeSet::from([variable.to_string()]))?;
        if memory_optional.source_variable != variable {
            return Err(self.error("thread repair memory optional must start from thread"));
        }

        self.expect_keyword("RETURN")?;
        self.expect_thread_repair_return(variable, &memory_optional.expand.target_variable)?;
        self.expect_keyword("ORDER")?;
        self.expect_keyword("BY")?;
        let order_variable = self.parse_ident()?;
        if order_variable != variable {
            return Err(self.error("thread repair ORDER BY must use the thread variable"));
        }
        self.expect_char('.')?;
        let order_property = self.parse_ident()?;
        if order_property != "id" {
            return Err(self.error("thread repair ORDER BY must use thread id"));
        }
        self.expect_keyword("ASC")?;

        Ok(Statement::MatchThreadRepairStats(MatchThreadRepairStats {
            variable: variable.to_string(),
            label: label.to_string(),
            identity_variable,
            identity_label,
            identity_ref_property,
            thread_id_property,
            message_rel_type: message_optional.expand.rel_type,
            message_label: message_optional.expand.target_label,
            memory_rel_type: memory_optional.expand.rel_type,
            memory_label: memory_optional.expand.target_label,
        }))
    }

    fn parse_count_alias(&mut self) -> Result<(String, String)> {
        self.expect_keyword("COUNT")?;
        self.expect_char('(')?;
        let variable = self.parse_ident()?;
        self.expect_char(')')?;
        self.expect_keyword("AS")?;
        let alias = self.parse_ident()?;
        Ok((variable, alias))
    }

    fn expect_thread_repair_return(&mut self, variable: &str, memory_variable: &str) -> Result<()> {
        self.expect_property_return(variable, "id")?;
        self.expect_char(',')?;
        self.expect_property_return(variable, "thread_id")?;
        self.expect_char(',')?;
        let expression = self.parse_scalar_expression()?;
        let space_id_matches = matches!(
            expression.kind,
            ScalarExpressionKind::DefaultIfNullOrEq {
                variable: ref expression_variable,
                ref property,
                ..
            } if expression_variable == variable && property == "space_id"
        );
        if !space_id_matches {
            return Err(self.error("thread repair RETURN must normalize thread space_id"));
        }
        self.expect_char(',')?;
        let expression = self.parse_scalar_expression()?;
        if !matches!(expression.kind, ScalarExpressionKind::Coalesce(_)) {
            return Err(self.error("thread repair RETURN must coalesce message_count"));
        }
        self.expect_char(',')?;
        let identity_refs = self.parse_ident()?;
        if identity_refs != "identity_refs" {
            return Err(self.error("thread repair RETURN must include identity_refs"));
        }
        self.expect_char(',')?;
        let legacy_messages = self.parse_ident()?;
        if legacy_messages != "legacy_messages" {
            return Err(self.error("thread repair RETURN must include legacy_messages"));
        }
        self.expect_char(',')?;
        self.expect_keyword("COUNT")?;
        self.expect_char('(')?;
        let counted_memory = self.parse_ident()?;
        if counted_memory != memory_variable {
            return Err(self.error("thread repair RETURN must count compacted memories"));
        }
        self.expect_char(')')?;
        Ok(())
    }

    fn expect_property_return(&mut self, variable: &str, property: &str) -> Result<()> {
        let parsed_variable = self.parse_ident()?;
        if parsed_variable != variable {
            return Err(self.error("thread repair RETURN property has the wrong variable"));
        }
        self.expect_char('.')?;
        let parsed_property = self.parse_ident()?;
        if parsed_property != property {
            return Err(self.error("thread repair RETURN property has the wrong property"));
        }
        Ok(())
    }

    fn parse_optional_relationship_count_filter(
        &mut self,
        optional: &OptionalRelationshipExpand,
    ) -> Result<OptionalRelationshipCountFilter> {
        let Some(relationship_variable) = optional.expand.variable.as_deref() else {
            return Err(
                self.error("OPTIONAL relationship count filter requires a relationship variable")
            );
        };
        let variable = self.parse_ident()?;
        if variable != relationship_variable {
            return Err(self.error(
                "OPTIONAL relationship count filter must use the counted relationship variable",
            ));
        }
        self.expect_char('.')?;
        let property = self.parse_ident()?;
        self.expect_token("<>")?;
        let value = self.parse_value()?;
        self.expect_keyword("OR")?;
        let null_variable = self.parse_ident()?;
        if null_variable != relationship_variable {
            return Err(self.error(
                "OPTIONAL relationship count filter must repeat the relationship variable",
            ));
        }
        self.expect_char('.')?;
        let null_property = self.parse_ident()?;
        if null_property != property {
            return Err(
                self.error("OPTIONAL relationship count filter must repeat the filtered property")
            );
        }
        self.expect_keyword("IS")?;
        self.expect_keyword("NULL")?;
        self.expect_keyword("OR")?;
        let empty_variable = self.parse_ident()?;
        if empty_variable != relationship_variable {
            return Err(self.error(
                "OPTIONAL relationship count filter must repeat the relationship variable",
            ));
        }
        self.expect_char('.')?;
        let empty_property = self.parse_ident()?;
        if empty_property != property {
            return Err(
                self.error("OPTIONAL relationship count filter must repeat the filtered property")
            );
        }
        self.expect_char('=')?;
        let empty = self.parse_value()?;
        if empty.kind != ValueExpressionKind::Literal(skein_core::Value::String(String::new())) {
            return Err(self.error(
                "OPTIONAL relationship count filter only supports an empty-string fallback",
            ));
        }
        Ok(OptionalRelationshipCountFilter::PropertyNotEqOrEmpty { property, value })
    }

    fn parse_optional_relationship_count_sum_return(
        &mut self,
    ) -> Result<(ParsedOptionalCountTerm, ParsedOptionalCountTerm, String)> {
        let parenthesized = self.consume_char('(');
        let first = self.parse_optional_count_term()?;
        self.expect_char('+')?;
        let second = self.parse_optional_count_term()?;
        if parenthesized {
            self.expect_char(')')?;
        }
        let output = format!(
            "(count({}) + count({}))",
            first.display_variable(),
            second.display_variable()
        );
        Ok((first, second, output))
    }

    fn parse_optional_count_term(&mut self) -> Result<ParsedOptionalCountTerm> {
        self.expect_keyword("COUNT")?;
        self.expect_char('(')?;
        let distinct = self.consume_keyword("DISTINCT");
        let variable = self.parse_ident()?;
        self.expect_char(')')?;
        Ok(ParsedOptionalCountTerm { variable, distinct })
    }

    fn optional_relationship_count_leg(
        &self,
        optional: &OptionalRelationshipExpand,
        filter: Option<OptionalRelationshipCountFilter>,
        count: &ParsedOptionalCountTerm,
    ) -> Result<OptionalRelationshipCountLeg> {
        let Some(relationship_variable) = optional.expand.variable.clone() else {
            return Err(
                self.error("OPTIONAL relationship count sum requires relationship variables")
            );
        };
        if relationship_variable != count.variable {
            return Err(self.error(
                "OPTIONAL relationship count sum must count the optional relationship variable",
            ));
        }
        Ok(OptionalRelationshipCountLeg {
            relationship_variable,
            rel_type: optional.expand.rel_type.clone(),
            direction: optional.expand.direction,
            distinct: count.distinct,
            filter,
        })
    }

    fn peek_next_with_item_separator(&mut self) -> bool {
        self.skip_ws();
        self.peek_char() == Some(',')
    }

    fn parse_with_alias_filter(&mut self) -> Result<WithAliasFilter> {
        self.parse_with_alias_filter_disjunction()
    }

    fn parse_with_alias_filter_disjunction(&mut self) -> Result<WithAliasFilter> {
        let mut filters = vec![self.parse_with_alias_filter_conjunction()?];
        while self.consume_keyword("OR") {
            filters.push(self.parse_with_alias_filter_conjunction()?);
        }
        if filters.len() == 1 {
            Ok(filters.remove(0))
        } else {
            Ok(WithAliasFilter::Or(filters))
        }
    }

    fn parse_with_alias_filter_conjunction(&mut self) -> Result<WithAliasFilter> {
        let mut filters = vec![self.parse_with_alias_filter_atom()?];
        while self.consume_keyword("AND") {
            filters.push(self.parse_with_alias_filter_atom()?);
        }
        if filters.len() == 1 {
            Ok(filters.remove(0))
        } else {
            Ok(WithAliasFilter::And(filters))
        }
    }

    fn parse_with_alias_filter_atom(&mut self) -> Result<WithAliasFilter> {
        self.skip_ws();
        if self.consume_char('(') {
            let filter = self.parse_with_alias_filter_disjunction()?;
            self.expect_char(')')?;
            return Ok(filter);
        }
        let left = self.parse_with_alias_filter_expression()?;
        self.skip_ws();
        let op = if self.consume_token("<>") || self.consume_token("!=") {
            WithAliasFilterOp::Ne
        } else if self.consume_token("<=") {
            WithAliasFilterOp::Lte
        } else if self.consume_token(">=") {
            WithAliasFilterOp::Gte
        } else if self.consume_char('=') {
            WithAliasFilterOp::Eq
        } else if self.consume_char('<') {
            WithAliasFilterOp::Lt
        } else if self.consume_char('>') {
            WithAliasFilterOp::Gt
        } else if self.consume_keyword("CONTAINS") {
            WithAliasFilterOp::Contains
        } else {
            return Err(self.error("expected WITH alias comparison operator"));
        };
        Ok(WithAliasFilter::Comparison {
            left,
            op,
            right: self.parse_with_alias_filter_expression()?,
        })
    }

    fn parse_with_alias_filter_expression(&mut self) -> Result<WithAliasFilterExpression> {
        self.skip_ws();
        if self.peek_char() == Some('$')
            || self.peek_char() == Some('\'')
            || self.peek_char() == Some('"')
            || self.peek_char() == Some('-')
            || self.peek_char().is_some_and(|ch| ch.is_ascii_digit())
            || self.next_keyword_is("TRUE")
            || self.next_keyword_is("FALSE")
            || self.next_keyword_is("NULL")
        {
            return Ok(WithAliasFilterExpression::Value(self.parse_value()?));
        }
        let variable = self.parse_ident()?;
        if self.consume_char('.') {
            Ok(WithAliasFilterExpression::Property {
                variable,
                property: self.parse_ident()?,
            })
        } else {
            Ok(WithAliasFilterExpression::Column(variable))
        }
    }
}

fn combine_match_predicates(
    left: Option<PropertyPredicate>,
    right: PropertyPredicate,
) -> PropertyPredicate {
    match left {
        Some(PropertyPredicate::And(mut predicates)) => {
            predicates.push(right);
            PropertyPredicate::And(predicates)
        }
        Some(left) => PropertyPredicate::And(vec![left, right]),
        None => right,
    }
}

fn variable_return_item(variable: String, span: SourceSpan) -> ReturnItem {
    let value = AstNode::from_source(ScalarExpressionKind::Variable(variable), span);
    let expression = AstNode::from_source(ReturnExpressionKind::Value(value), span);
    AstNode::from_source(
        ReturnItemKind {
            expression,
            alias: None,
        },
        span,
    )
}
