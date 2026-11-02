// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn bind_shortest_path_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let [matched, returned] = query.clauses.as_slice() else {
        return Err(unsupported(
            "ALL SHORTEST requires a MATCH followed by RETURN",
        ));
    };
    let ClauseKind::Match {
        optional: false,
        patterns,
        predicate,
    } = &matched.kind
    else {
        return Err(unsupported("ALL SHORTEST requires a non-optional MATCH"));
    };
    let ClauseKind::Return(projection) = &returned.kind else {
        return Err(unsupported("ALL SHORTEST requires a RETURN projection"));
    };
    if projection.distinct
        || projection.predicate.is_some()
        || !projection.order_by.is_empty()
        || projection.offset.is_some()
        || projection.limit.is_some()
    {
        return Err(unsupported(
            "ALL SHORTEST does not support projection modifiers",
        ));
    }
    let [pattern] = patterns.as_slice() else {
        return Err(unsupported("ALL SHORTEST requires one path pattern"));
    };
    let Some(path_variable) = &pattern.variable else {
        return Err(unsupported("ALL SHORTEST requires a path variable"));
    };
    let [step] = pattern.steps.as_slice() else {
        return Err(unsupported(
            "ALL SHORTEST requires one bounded relationship pattern",
        ));
    };
    let source = &pattern.first;
    let target = &step.target;
    let relationship = &step.relationship;
    if relationship.search != PathSearch::AllShortest {
        return Err(unsupported("path projection requires ALL SHORTEST"));
    }
    if relationship.direction == RelationshipDirection::Incoming {
        return Err(unsupported(
            "ALL SHORTEST path reads do not support incoming-only patterns",
        ));
    }
    if !source.properties.is_empty() || !target.properties.is_empty() {
        return Err(unsupported(
            "ALL SHORTEST path reads require endpoint ids in WHERE predicates",
        ));
    }
    if relationship.min_hops == 0
        || relationship.max_hops == 0
        || relationship.max_hops == hawdb_cypher::UNBOUNDED_MAX_HOPS
        || relationship.min_hops > relationship.max_hops
    {
        return Err(unsupported(
            "ALL SHORTEST path reads require a finite positive hop range",
        ));
    }
    if relationship.variable.is_some() && !relationship.rel_type.is_empty() {
        return Err(unsupported(
            "ALL SHORTEST path reads do not bind relationship variables",
        ));
    }
    if !relationship.properties.is_empty() {
        return Err(unsupported(
            "ALL SHORTEST does not support relationship property filters",
        ));
    }
    let scope = BTreeSet::from([source.variable.clone(), target.variable.clone()]);
    let predicate = predicate.as_ref().map(|predicate| &predicate.kind);
    if let Some(predicate) = predicate {
        validate_predicate(&scope, predicate)?;
    }
    let source_id = endpoint_id_value(predicate, &source.variable, parameters, "source")?;
    let target_id = endpoint_id_value(predicate, &target.variable, parameters, "target")?;
    // Endpoint visibility predicates also constrain intermediate nodes in the executor.
    // Do not repurpose them for ordinary WHERE conditions or silently discard conditions.
    if let Some(predicate) = predicate {
        validate_endpoint_predicates(
            predicate,
            &source.variable,
            &target.variable,
            &mut BTreeSet::new(),
        )?;
    }
    let returns = projection
        .items
        .iter()
        .map(|item| {
            let (referenced_path, expression) = match &item.expression.kind {
                ReturnExpressionKind::Path(ShortestPathReturnExpression::NodePropertyList {
                    path_variable,
                    property,
                }) => (
                    path_variable,
                    ShortestPathProjectionExpression::NodePropertyList {
                        property: property.clone(),
                    },
                ),
                ReturnExpressionKind::Path(ShortestPathReturnExpression::Length {
                    path_variable,
                }) => (path_variable, ShortestPathProjectionExpression::Length),
                _ => return Err(unsupported("ALL SHORTEST requires a path projection")),
            };
            if referenced_path != path_variable {
                return Err(unsupported(
                    "shortest path projection references an unknown path",
                ));
            }
            let name = item
                .alias
                .clone()
                .ok_or_else(|| unsupported("shortest path projection requires an alias"))?;
            Ok(ShortestPathProjection { expression, name })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::ShortestPath {
        source_variable: source.variable.clone(),
        source_label: source.label.clone(),
        source_id,
        source_visibility_predicate: None,
        rel_type: relationship.rel_type.clone(),
        direction: relationship.direction,
        target_variable: target.variable.clone(),
        target_label: target.label.clone(),
        target_id,
        target_visibility_predicate: None,
        min_hops: relationship.min_hops,
        max_hops: relationship.max_hops,
        returns,
    })
}

fn validate_endpoint_predicates<'a>(
    predicate: &'a PropertyPredicate,
    source: &str,
    target: &str,
    seen: &mut BTreeSet<&'a str>,
) -> Result<()> {
    match predicate {
        PropertyPredicate::And(children) => {
            for child in children {
                validate_endpoint_predicates(child, source, target, seen)?;
            }
            Ok(())
        }
        PropertyPredicate::Eq {
            variable, property, ..
        } if (variable == source || variable == target) && property == "id" => {
            if !seen.insert(variable) {
                return Err(unsupported(
                    "ALL SHORTEST requires one id equality per endpoint",
                ));
            }
            Ok(())
        }
        _ => Err(unsupported(
            "ALL SHORTEST supports only endpoint id equalities",
        )),
    }
}

fn unsupported(message: &str) -> HawDBError {
    HawDBError::Semantic(message.to_string())
}

pub(super) fn endpoint_id_value(
    predicate: Option<&PropertyPredicate>,
    variable: &str,
    parameters: &BTreeMap<String, Value>,
    endpoint_name: &str,
) -> Result<Value> {
    let Some(predicate) = predicate else {
        return Err(HawDBError::Semantic(format!(
            "ALL SHORTEST path reads require {endpoint_name} id predicate"
        )));
    };
    find_endpoint_id_value(predicate, variable, parameters)?.ok_or_else(|| {
        HawDBError::Semantic(format!(
            "ALL SHORTEST path reads require {endpoint_name} id equality on '{variable}.id'"
        ))
    })
}

pub(super) fn find_endpoint_id_value(
    predicate: &PropertyPredicate,
    variable: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<Option<Value>> {
    match predicate {
        PropertyPredicate::And(predicates) => {
            for predicate in predicates {
                if let Some(value) = find_endpoint_id_value(predicate, variable, parameters)? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
        PropertyPredicate::Eq {
            variable: predicate_variable,
            property,
            value,
        } if predicate_variable == variable && property == "id" => {
            Ok(Some(bind_value(value, parameters)?))
        }
        _ => Ok(None),
    }
}

/// Clause-level input of [`bind_shortest_path_match`].
pub(super) struct ShortestPathMatch<'a> {
    pub(super) optional: bool,
    pub(super) patterns: &'a [hawdb_cypher::MatchPattern],
    pub(super) predicate: Option<&'a hawdb_cypher::PredicateExpression>,
}

/// Refuses WHERE conjuncts that read a shortest-path variable of the same
/// MATCH clause, before either shortest-path binder runs, so the refusal is
/// reported for every syntax form. Neo4j applies such predicates during the
/// search; evaluating them after selection would silently differ.
pub(super) fn reject_shortest_path_predicates(clauses: &[hawdb_cypher::Clause]) -> Result<()> {
    for clause in clauses {
        let hawdb_cypher::ClauseKind::Match {
            patterns,
            predicate: Some(predicate),
            ..
        } = &clause.kind
        else {
            continue;
        };
        let paths = patterns
            .iter()
            .filter(|pattern| is_shortest_path_pattern(pattern))
            .filter_map(|pattern| pattern.variable.clone())
            .collect::<BTreeSet<_>>();
        if paths.is_empty() {
            continue;
        }
        let mut variables = BTreeSet::new();
        collect_predicate_variables(&predicate.kind, &mut variables);
        if let Some(path) = variables.intersection(&paths).next() {
            return Err(HawDBError::Semantic(format!(
                "WHERE predicates on shortest-path variable '{path}' are not supported in the same MATCH clause; filter after the search with `WITH {path} WHERE ...`"
            )));
        }
    }
    Ok(())
}

pub(super) fn is_shortest_path_pattern(pattern: &hawdb_cypher::MatchPattern) -> bool {
    pattern
        .steps
        .iter()
        .any(|step| step.relationship.search.is_shortest())
}

/// Binds a MATCH or OPTIONAL MATCH clause containing `shortestPath(...)` or
/// `allShortestPaths(...)` patterns.
///
/// Each shortest-path pattern becomes endpoint `Node` steps followed by a
/// `ShortestPath` step, so both endpoints are bound before the search. A
/// required MATCH whose other patterns are single nodes first binds every
/// endpoint under the WHERE conjuncts that do not read a path variable, so
/// endpoint predicates are applied (and may use indexes) before any search.
/// OPTIONAL MATCH keeps the whole clause in one program because its NULL row
/// belongs to the complete pattern, and so do clauses with relationship
/// patterns, whose relationships the searches must not reuse.
pub(super) fn bind_shortest_path_match(
    clause: ShortestPathMatch<'_>,
    input: Option<LogicalPlan>,
    scope: &mut Scope,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let ShortestPathMatch {
        optional,
        patterns,
        predicate,
    } = clause;
    let imports = scope.imports();
    let mut constrained = scope.graph_names();
    if let Some(predicate) = predicate {
        collect_predicate_variables(&predicate.kind, &mut constrained);
    }
    for pattern in patterns
        .iter()
        .filter(|pattern| !is_shortest_path_pattern(pattern))
    {
        constrained.insert(pattern.first.variable.clone());
        constrained.extend(
            pattern
                .steps
                .iter()
                .map(|step| step.target.variable.clone()),
        );
    }
    let mut introduced = Vec::new();
    let mut steps = Vec::new();
    let mut path_steps = Vec::new();
    let mut paths = Vec::new();
    for (index, pattern) in patterns.iter().enumerate() {
        if !is_shortest_path_pattern(pattern) {
            bind_ordinary_pattern(pattern, scope, &mut introduced, &mut steps, parameters)?;
            continue;
        }
        let [step] = pattern.steps.as_slice() else {
            return Err(unsupported(
                "shortestPath requires exactly one relationship pattern",
            ));
        };
        let relationship = &step.relationship;
        if relationship.variable.is_some() {
            return Err(unsupported(
                "shortestPath does not bind relationship variables; use relationships(path)",
            ));
        }
        if !relationship.properties.is_empty() {
            return Err(unsupported(
                "shortestPath does not support relationship property patterns",
            ));
        }
        if relationship.min_hops > 1 {
            return Err(unsupported(
                "shortestPath supports a minimum of 0 or 1 hops",
            ));
        }
        let bounded = relationship.max_hops != hawdb_cypher::UNBOUNDED_MAX_HOPS;
        if bounded && relationship.max_hops > MAX_SHORTEST_PATH_HOPS {
            return Err(HawDBError::Semantic(format!(
                "shortestPath maximum of {} hops exceeds the limit of {MAX_SHORTEST_PATH_HOPS}",
                relationship.max_hops
            )));
        }
        let path = pattern
            .variable
            .clone()
            .unwrap_or_else(|| format!("\0shortest_path.{index}"));
        if scope.0.contains_key(&path) {
            return Err(HawDBError::Semantic(format!(
                "path variable '{path}' is already bound"
            )));
        }
        // Bind the path first so an endpoint cannot reuse its name.
        scope.0.insert(path.clone(), BindingType::Path);
        for endpoint in [&pattern.first, &step.target] {
            if endpoint.anonymous
                || (endpoint.properties.is_empty() && !constrained.contains(&endpoint.variable))
            {
                return Err(HawDBError::Semantic(format!(
                    "shortestPath endpoint '{}' must be bound by an earlier clause, an inline property map or a WHERE predicate",
                    if endpoint.anonymous {
                        "()"
                    } else {
                        endpoint.variable.as_str()
                    }
                )));
            }
            scope.bind_graph(&endpoint.variable, GraphEntityKind::Node, &mut introduced)?;
            let node = bind_node(endpoint, parameters)?;
            push_endpoint_node(&mut steps, node.clone());
            path_steps.push(GraphMatchStep::Node(node));
        }
        path_steps.push(GraphMatchStep::ShortestPath(Box::new(
            GraphMatchShortestPath {
                path: path.clone(),
                source: pattern.first.variable.clone(),
                target: step.target.variable.clone(),
                rel_type: relationship.rel_type.clone(),
                direction: relationship.direction,
                min_hops: relationship.min_hops,
                max_hops: if bounded {
                    relationship.max_hops
                } else {
                    MAX_SHORTEST_PATH_HOPS
                },
                bounded,
                selection: if relationship.search == PathSearch::AllShortest {
                    ShortestPathSelection::All
                } else {
                    ShortestPathSelection::Any
                },
            },
        )));
        paths.push(path);
    }
    if steps.len().saturating_add(path_steps.len()) > 32 {
        return Err(HawDBError::Semantic(
            "MATCH exceeds maximum pattern depth".to_string(),
        ));
    }
    scope.materialized();
    // Relationship uniqueness spans the clause, so ordinary relationship
    // patterns stay in the same program as the searches they constrain.
    let split = !optional
        && patterns
            .iter()
            .all(|pattern| is_shortest_path_pattern(pattern) || pattern.steps.is_empty());
    let path_names = paths.iter().cloned().collect::<BTreeSet<_>>();
    let mut endpoint_conjuncts = Vec::new();
    let mut path_conjuncts = Vec::new();
    if let Some(predicate) = predicate {
        let conjuncts = match &predicate.kind {
            PropertyPredicate::And(children) => children.iter().collect(),
            predicate => vec![predicate],
        };
        for conjunct in conjuncts {
            let mut variables = BTreeSet::new();
            collect_predicate_variables(conjunct, &mut variables);
            // Neo4j applies WHERE predicates on a shortest-path variable while
            // searching (the shortest path that satisfies them). Evaluating
            // them after selection would silently differ, so require the
            // explicit post-filter form instead.
            if let Some(path) = variables.intersection(&path_names).next() {
                return Err(HawDBError::Semantic(format!(
                    "WHERE predicates on shortest-path variable '{path}' are not supported in the same MATCH clause; filter after the search with `WITH {path} WHERE ...`"
                )));
            }
            let bound = bind_predicate(conjunct, scope, parameters)?;
            if !split {
                path_conjuncts.push(bound);
            } else {
                endpoint_conjuncts.push(bound);
            }
        }
    }
    let combine = |mut predicates: Vec<Predicate>| match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(Predicate::And(predicates)),
    };
    if !split {
        introduced.extend(paths);
        steps.extend(path_steps);
        return Ok(LogicalPlan::GraphMatch {
            program: GraphMatchProgram {
                imports,
                introduced,
                projected_nodes: BTreeMap::new(),
                steps,
                predicate: combine(path_conjuncts),
                predicate_steps: Vec::new(),
                optional,
            },
            input: input.map(Box::new),
        });
    }
    let endpoint_predicate = combine(endpoint_conjuncts);
    // Endpoints that were all bound earlier need no separate binding program.
    let (input, imports) = if introduced.is_empty()
        && endpoint_predicate.is_none()
        && patterns.iter().all(is_shortest_path_pattern)
    {
        (input, imports)
    } else {
        let endpoints = LogicalPlan::GraphMatch {
            program: GraphMatchProgram {
                imports,
                introduced,
                projected_nodes: BTreeMap::new(),
                steps,
                predicate: endpoint_predicate,
                predicate_steps: Vec::new(),
                optional: false,
            },
            input: input.map(Box::new),
        };
        (Some(endpoints), Vec::new())
    };
    Ok(LogicalPlan::GraphMatch {
        program: GraphMatchProgram {
            imports,
            introduced: paths,
            projected_nodes: BTreeMap::new(),
            steps: path_steps,
            predicate: combine(path_conjuncts),
            predicate_steps: Vec::new(),
            optional: false,
        },
        input: input.map(Box::new),
    })
}

/// Adds a shortest-path endpoint to the binding steps without repeating a
/// node the clause already binds.
///
/// `MATCH (a:Person {id: 1}), (b:Person {id: 2}), p = shortestPath((a)-..-(b))`
/// names each endpoint twice. A repeated `Node` step only re-checks its label
/// and properties against the bound node, so folding the endpoint's
/// constraints into the earlier step keeps the same rows, and leaves one step
/// per variable, which normalization can lower to independent node access
/// (and so to index seeks) instead of a generic nested scan. Folding is
/// limited to constraints that combine without comparing values: a label
/// only into an unlabeled or identically labeled step, and properties only
/// when at most one side has any, so the plan shape never depends on
/// parameter values. Anything else keeps the explicit re-check step.
fn push_endpoint_node(steps: &mut Vec<GraphMatchStep>, node: GraphMatchNode) {
    let existing = steps.iter_mut().find_map(|step| match step {
        GraphMatchStep::Node(existing) if existing.variable == node.variable => Some(existing),
        _ => None,
    });
    let Some(existing) = existing else {
        steps.push(GraphMatchStep::Node(node));
        return;
    };
    let labels_fold =
        node.label.is_empty() || existing.label.is_empty() || node.label == existing.label;
    let properties_fold = node.properties.is_empty() || existing.properties.is_empty();
    if !(labels_fold && properties_fold) {
        steps.push(GraphMatchStep::Node(node));
        return;
    }
    if existing.label.is_empty() {
        existing.label = node.label;
    }
    if existing.properties.is_empty() {
        existing.properties = node.properties;
    }
}

/// The ordinary-pattern half of the generic MATCH binder, shared with
/// clauses that also contain shortest-path patterns.
fn bind_ordinary_pattern(
    pattern: &hawdb_cypher::MatchPattern,
    scope: &mut Scope,
    introduced: &mut Vec<String>,
    steps: &mut Vec<GraphMatchStep>,
    parameters: &BTreeMap<String, Value>,
) -> Result<()> {
    if let Some(variable) = &pattern.variable {
        if scope.0.contains_key(variable) {
            return Err(HawDBError::Semantic(format!(
                "path variable '{variable}' is already bound"
            )));
        }
        scope
            .0
            .insert(variable.clone(), BindingType::UnmaterializedPath);
    }
    scope.bind_graph(&pattern.first.variable, GraphEntityKind::Node, introduced)?;
    steps.push(GraphMatchStep::Node(bind_node(&pattern.first, parameters)?));
    let mut source = pattern.first.variable.clone();
    for step in &pattern.steps {
        let relationship = &step.relationship;
        validate_bounded_relationship(relationship)?;
        if let Some(variable) = &relationship.variable {
            scope.bind_graph(variable, GraphEntityKind::Relationship, introduced)?;
        }
        scope.bind_graph(&step.target.variable, GraphEntityKind::Node, introduced)?;
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
    Ok(())
}
