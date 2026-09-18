use super::*;
use skein_cypher::{ClauseKind, PathSearch, ProjectionClause, QueryPipeline};

mod imports;
mod mutation;
mod normalize;
mod path;
mod procedure;
#[cfg(test)]
mod tests;

/// Migration entrypoint for the ordered-clause binder.
#[doc(hidden)]
pub fn plan_pipeline_query(
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    bind_pipeline(&skein_cypher::parse_pipeline(query)?, parameters)
}

/// Migration entrypoint with structural read-plan normalization enabled.
#[doc(hidden)]
pub fn plan_normalized_pipeline_query(
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    plan_pipeline_query(query, parameters).map(normalize::normalize)
}

#[derive(Clone)]
enum BindingType {
    Graph {
        kind: GraphEntityKind,
        column: Option<String>,
    },
    Scalar,
    UnmaterializedPath,
}

#[derive(Clone, Default)]
struct Scope(BTreeMap<String, BindingType>);

impl Scope {
    fn graph_names(&self) -> BTreeSet<String> {
        self.0
            .iter()
            .filter_map(|(name, kind)| {
                matches!(kind, BindingType::Graph { .. }).then_some(name.clone())
            })
            .collect()
    }

    fn columns(&self) -> BTreeSet<String> {
        self.0
            .iter()
            .filter_map(|(name, kind)| {
                matches!(
                    kind,
                    BindingType::Scalar
                        | BindingType::Graph {
                            column: Some(_),
                            ..
                        }
                )
                .then_some(name.clone())
            })
            .collect()
    }

    fn imports(&self) -> Vec<GraphBindingImport> {
        self.0
            .iter()
            .filter_map(|(variable, binding)| match binding {
                BindingType::Graph {
                    kind,
                    column: Some(column),
                } => Some(GraphBindingImport {
                    variable: variable.clone(),
                    column: column.clone(),
                    kind: *kind,
                }),
                _ => None,
            })
            .collect()
    }

    fn bind_graph(
        &mut self,
        name: &str,
        kind: GraphEntityKind,
        introduced: &mut Vec<String>,
    ) -> Result<()> {
        match self.0.get(name) {
            Some(BindingType::Graph { kind: previous, .. }) if *previous == kind => Ok(()),
            Some(_) => Err(SkeinError::Semantic(format!(
                "variable '{name}' has an incompatible MATCH type"
            ))),
            None => {
                self.0
                    .insert(name.to_string(), BindingType::Graph { kind, column: None });
                introduced.push(name.to_string());
                Ok(())
            }
        }
    }

    fn materialized(&mut self) {
        for binding in self.0.values_mut() {
            if let BindingType::Graph { column, .. } = binding {
                *column = None;
            }
        }
    }
}

fn bind_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    if query.clauses.len() > 32 {
        return Err(SkeinError::Semantic(
            "query exceeds maximum clause depth".to_string(),
        ));
    }
    if query.clauses.iter().any(|clause| {
        matches!(
            clause.kind,
            ClauseKind::Create(_)
                | ClauseKind::Merge { .. }
                | ClauseKind::Set(_)
                | ClauseKind::Delete { .. }
        )
    }) {
        return mutation::bind_mutation_pipeline(query, parameters);
    }
    if matches!(
        query.clauses.first().map(|clause| &clause.kind),
        Some(ClauseKind::Call { .. })
    ) {
        return procedure::bind_procedure_pipeline(query, parameters);
    }
    if query.clauses.iter().any(|clause| {
        matches!(&clause.kind,
        ClauseKind::Match { patterns, .. } if patterns.iter().any(|pattern|
            pattern.steps.iter().any(|step| step.relationship.search == PathSearch::AllShortest)))
    }) {
        return path::bind_shortest_path_pipeline(query, parameters);
    }
    bind_read_clauses(&query.clauses, None, Scope::default(), parameters)
}

fn bind_read_clauses(
    clauses: &[skein_cypher::Clause],
    mut input: Option<LogicalPlan>,
    mut scope: Scope,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    for clause in clauses {
        match &clause.kind {
            ClauseKind::Match {
                optional,
                patterns,
                predicate,
            } => {
                let imports = scope.imports();
                let mut introduced = Vec::new();
                let mut steps = Vec::new();
                for pattern in patterns {
                    if let Some(variable) = &pattern.variable {
                        if scope.0.contains_key(variable) {
                            return Err(SkeinError::Semantic(format!(
                                "path variable '{variable}' is already bound"
                            )));
                        }
                        // Bounded expands may have an unused path alias, but it must not
                        // accidentally become a node binding in a later MATCH.
                        scope
                            .0
                            .insert(variable.clone(), BindingType::UnmaterializedPath);
                    }
                    if pattern
                        .steps
                        .iter()
                        .any(|step| step.relationship.search == PathSearch::AllShortest)
                    {
                        return Err(SkeinError::Semantic(
                            "path binding requires shortest-path lowering".to_string(),
                        ));
                    }
                    scope.bind_graph(
                        &pattern.first.variable,
                        GraphEntityKind::Node,
                        &mut introduced,
                    )?;
                    steps.push(GraphMatchStep::Node(bind_node(&pattern.first, parameters)?));
                    let mut source = pattern.first.variable.clone();
                    for step in &pattern.steps {
                        let relationship = &step.relationship;
                        if let Some(variable) = &relationship.variable {
                            scope.bind_graph(
                                variable,
                                GraphEntityKind::Relationship,
                                &mut introduced,
                            )?;
                        }
                        scope.bind_graph(
                            &step.target.variable,
                            GraphEntityKind::Node,
                            &mut introduced,
                        )?;
                        steps.push(GraphMatchStep::Expand {
                            source,
                            relationship: relationship.variable.clone(),
                            rel_type: relationship.rel_type.clone(),
                            properties: bind_properties(&relationship.properties, parameters)?,
                            direction: relationship.direction,
                            min_hops: relationship.min_hops,
                            max_hops: relationship.max_hops,
                            target: bind_node(&step.target, parameters)?,
                        });
                        source = step.target.variable.clone();
                    }
                }
                if steps.len() > 32 {
                    return Err(SkeinError::Semantic(
                        "MATCH exceeds maximum pattern depth".to_string(),
                    ));
                }
                scope.materialized();
                let predicate = predicate
                    .as_ref()
                    .map(|predicate| bind_predicate(&predicate.kind, &scope, parameters))
                    .transpose()?;
                input = Some(LogicalPlan::GraphMatch {
                    program: GraphMatchProgram {
                        imports,
                        introduced,
                        steps,
                        predicate,
                        optional: *optional,
                    },
                    input: input.map(Box::new),
                });
            }
            ClauseKind::With(projection) | ClauseKind::Return(projection) => {
                let current = input
                    .take()
                    .unwrap_or_else(|| restore_bindings(None, &mut Scope::default()));
                let (next, next_scope) = bind_projection(current, &scope, projection, parameters)?;
                input = Some(next);
                scope = next_scope;
            }
            _ => {
                return Err(SkeinError::Semantic(
                    "clause requires mutation or procedure pipeline lowering".to_string(),
                ))
            }
        }
    }
    input.ok_or_else(|| SkeinError::Semantic("empty query pipeline".to_string()))
}

fn bind_node(
    node: &skein_cypher::NodePattern,
    parameters: &BTreeMap<String, Value>,
) -> Result<GraphMatchNode> {
    Ok(GraphMatchNode {
        variable: node.variable.clone(),
        label: node.label.clone(),
        properties: bind_properties(&node.properties, parameters)?,
    })
}

fn restore_bindings(input: Option<LogicalPlan>, scope: &mut Scope) -> LogicalPlan {
    let imports = scope.imports();
    if imports.is_empty()
        && let Some(input) = input
    {
        return input;
    }
    scope.materialized();
    LogicalPlan::GraphMatch {
        program: GraphMatchProgram {
            imports,
            introduced: Vec::new(),
            steps: Vec::new(),
            predicate: None,
            optional: false,
        },
        input: input.map(Box::new),
    }
}

fn bind_projection(
    mut input: LogicalPlan,
    scope: &Scope,
    projection: &ProjectionClause,
    parameters: &BTreeMap<String, Value>,
) -> Result<(LogicalPlan, Scope)> {
    let graph = scope.graph_names();
    let columns = scope.columns();
    let mut planned = if projection
        .items
        .iter()
        .any(|item| super::arithmetic::contains_aggregate(&item.expression))
    {
        super::arithmetic::plan_composed_returns(&graph, &columns, &projection.items, parameters)?
    } else {
        plan_return_items_with_columns(&graph, &columns, &projection.items, parameters)?
    };
    let names = unique_projection_names(planned.names());
    let mut output_scope = Scope::default();
    for (item, name) in projection.items.iter().zip(&names) {
        let kind = match &item.expression.kind {
            ReturnExpressionKind::Value(AstNode {
                kind: ScalarExpressionKind::Variable(variable),
                ..
            }) => match scope.0.get(variable) {
                Some(BindingType::Graph { kind, .. }) => BindingType::Graph {
                    kind: *kind,
                    column: Some(name.clone()),
                },
                _ => BindingType::Scalar,
            },
            _ => BindingType::Scalar,
        };
        output_scope.0.insert(name.clone(), kind);
    }
    let projected_names = names.iter().cloned().collect();
    let projected_graph = output_scope.graph_names();
    let mut sort_items = Vec::new();
    let mut hidden_order_keys = false;
    for (index, item) in projection.order_by.iter().enumerate() {
        match plan_sort_items(
            &projected_graph,
            &projected_names,
            std::slice::from_ref(item),
            parameters,
        ) {
            Ok(mut items) => sort_items.append(&mut items),
            Err(error) if projection.distinct => return Err(error),
            Err(_) => {
                let mut items =
                    plan_sort_items(&graph, &columns, std::slice::from_ref(item), parameters)?;
                let item = items.pop().expect("one ORDER BY expression");
                let expression = match item.key {
                    SortKey::Column(column) => ProjectionExpression::Column(column),
                    SortKey::Property { variable, property } => {
                        ProjectionExpression::Property { variable, property }
                    }
                    SortKey::Id { variable } => ProjectionExpression::Id { variable },
                    SortKey::Expression(expression) => expression,
                };
                let name = format!("\0order.{index}");
                append_order_projection(
                    &mut planned,
                    Projection {
                        expression,
                        name: name.clone(),
                    },
                );
                sort_items.push(SortItem {
                    key: SortKey::Column(name),
                    direction: item.direction,
                });
                hidden_order_keys = true;
            }
        }
    }
    let needed = imports::for_returns(&mut planned, scope)?;
    input = imports::restore(input, scope, &needed);
    let mut output = planned.into_logical(input);
    if projection.distinct {
        output = LogicalPlan::Distinct {
            input: Box::new(output),
        };
    }
    if let Some(predicate) = &projection.predicate {
        let mut predicate_scope = output_scope.clone();
        predicate_scope.materialized();
        let predicate = bind_predicate(&predicate.kind, &predicate_scope, parameters)?;
        output = imports::restore(output, &output_scope, &imports::for_predicate(&predicate));
        output = LogicalPlan::Filter {
            predicate,
            input: Box::new(output),
        };
    }
    if !sort_items.is_empty() {
        let needed = imports::for_sort(&mut sort_items, &output_scope)?;
        output = imports::restore(output, &output_scope, &needed);
        output = LogicalPlan::Sort {
            items: sort_items,
            input: Box::new(output),
        };
    }
    let offset = projection
        .offset
        .as_ref()
        .map(|value| bind_pagination_value(value, parameters, "offset"))
        .transpose()?
        .unwrap_or(0);
    let limit = projection
        .limit
        .as_ref()
        .map(|value| bind_pagination_value(value, parameters, "limit"))
        .transpose()?;
    if offset != 0 || limit.is_some() {
        output = LogicalPlan::Limit {
            offset,
            limit,
            input: Box::new(output),
        };
    }
    if hidden_order_keys {
        output = LogicalPlan::Project {
            items: names
                .into_iter()
                .map(|name| Projection {
                    expression: ProjectionExpression::Column(name.clone()),
                    name,
                })
                .collect(),
            input: Box::new(output),
        };
    }
    Ok((output, output_scope))
}

fn unique_projection_names(names: Vec<String>) -> Vec<String> {
    let mut used = BTreeSet::new();
    names
        .into_iter()
        .map(|name| {
            let mut candidate = name.clone();
            let mut suffix = 2;
            while !used.insert(candidate.clone()) {
                candidate = format!("{name}#{suffix}");
                suffix += 1;
            }
            candidate
        })
        .collect()
}

fn append_order_projection(planned: &mut PlannedReturns, projection: Projection) {
    match planned {
        PlannedReturns::Projections(items) => items.push(projection),
        PlannedReturns::Aggregations { group_keys, .. } => group_keys.push(projection),
        PlannedReturns::AggregateProjection {
            group_keys,
            projections,
            ..
        } => {
            projections.push(Projection {
                expression: ProjectionExpression::Column(projection.name.clone()),
                name: projection.name.clone(),
            });
            group_keys.push(projection);
        }
    }
}

fn bind_predicate(
    predicate: &PropertyPredicate,
    scope: &Scope,
    parameters: &BTreeMap<String, Value>,
) -> Result<Predicate> {
    let graph = scope.graph_names();
    let columns = scope.columns();
    let all = graph.union(&columns).cloned().collect();
    validate_predicate(&all, predicate)?;
    let bind =
        |expression| plan_scalar_expression_with_columns(&graph, &columns, expression, parameters);
    Ok(match predicate {
        PropertyPredicate::And(children) => Predicate::And(
            children
                .iter()
                .map(|child| bind_predicate(child, scope, parameters))
                .collect::<Result<_>>()?,
        ),
        PropertyPredicate::Or(children) => Predicate::Or(
            children
                .iter()
                .map(|child| bind_predicate(child, scope, parameters))
                .collect::<Result<_>>()?,
        ),
        PropertyPredicate::Not(child) => {
            Predicate::Not(Box::new(bind_predicate(child, scope, parameters)?))
        }
        PropertyPredicate::ExpressionEq { expression, value } => Predicate::ExpressionEq {
            expression: bind(expression)?,
            value: bind(value)?,
        },
        PropertyPredicate::ExpressionNotEq { expression, value } => Predicate::ExpressionNotEq {
            expression: bind(expression)?,
            value: bind(value)?,
        },
        PropertyPredicate::ExpressionCompare {
            expression,
            op,
            value,
        } => Predicate::ExpressionCompare {
            expression: bind(expression)?,
            op: plan_comparison_op(*op),
            value: bind(value)?,
        },
        PropertyPredicate::ExpressionContains { expression, value } => {
            Predicate::ExpressionContains {
                expression: bind(expression)?,
                value: bind(value)?,
            }
        }
        PropertyPredicate::Eq {
            variable,
            property,
            value,
        } if columns.contains(variable) => Predicate::ExpressionEq {
            expression: column_property(variable, property),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::NotEq {
            variable,
            property,
            value,
        } if columns.contains(variable) => Predicate::ExpressionNotEq {
            expression: column_property(variable, property),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::Compare {
            variable,
            property,
            op,
            value,
        } if columns.contains(variable) => Predicate::ExpressionCompare {
            expression: column_property(variable, property),
            op: plan_comparison_op(*op),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::Contains {
            variable,
            property,
            value,
        } if columns.contains(variable) => Predicate::ExpressionContains {
            expression: column_property(variable, property),
            value: ProjectionExpression::Literal(bind_value(value, parameters)?),
        },
        PropertyPredicate::IsNull { variable, property }
        | PropertyPredicate::IsNotNull { variable, property }
            if columns.contains(variable) =>
        {
            Predicate::ExpressionEq {
                expression: ProjectionExpression::IsNull {
                    expression: Box::new(column_property(variable, property)),
                    negated: matches!(predicate, PropertyPredicate::IsNotNull { .. }),
                },
                value: ProjectionExpression::Literal(Value::Bool(true)),
            }
        }
        _ => {
            let mut variables = BTreeSet::new();
            collect_predicate_variables(predicate, &mut variables);
            if variables.iter().any(|variable| columns.contains(variable)) {
                return Err(SkeinError::Semantic(
                    "predicate requires a graph binding for this operation".to_string(),
                ));
            }
            plan_predicate(predicate, &graph, parameters)?
        }
    })
}

fn column_property(column: &str, property: &str) -> ProjectionExpression {
    ProjectionExpression::ColumnProperty {
        column: column.to_string(),
        property: property.to_string(),
    }
}
