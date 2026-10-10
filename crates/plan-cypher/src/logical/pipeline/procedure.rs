use super::*;
use hawdb_cypher::{ProcedureCallKind, YieldItem};

pub(super) fn bind_procedure_pipeline(
    query: &QueryPipeline,
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let ClauseKind::Call { procedure, yields } = &query.clauses[0].kind else {
        unreachable!()
    };
    let mut tail = &query.clauses[1..];
    let has_graph_read = tail
        .iter()
        .any(|clause| matches!(clause.kind, ClauseKind::Match { .. }));
    let (mut input, columns) = match &procedure.kind {
        ProcedureCallKind::ProjectGraph {
            name,
            node_labels,
            rel_types,
            relationship_predicates,
        } => {
            if !yields.is_empty() || !tail.is_empty() {
                return Err(unsupported("project_graph does not yield query rows"));
            }
            return Ok(LogicalPlan::ProjectGraph {
                name: name.clone(),
                node_labels: node_labels.clone(),
                rel_types: rel_types.clone(),
                relationship_predicates: bind_projected_relationship_predicates(
                    relationship_predicates,
                )?,
            });
        }
        ProcedureCallKind::GraphAlgorithm {
            algorithm,
            graph_name,
            options,
        } => {
            let default_score = match algorithm {
                CypherGraphAlgorithmKind::PageRank => "pagerank_score",
                CypherGraphAlgorithmKind::Louvain => "louvain_id",
            };
            let mut score = default_score.to_string();
            let yield_has_node_id = yields
                .iter()
                .any(|item| item.name.eq_ignore_ascii_case("node_id"));
            let yield_has_node_label = yields
                .iter()
                .any(|item| item.name.eq_ignore_ascii_case("node_label"));
            if yield_has_node_id != yield_has_node_label {
                return Err(unsupported(
                    "graph algorithm identity requires both node_id and node_label",
                ));
            }
            let mut return_node_identity = yield_has_node_id;
            // PageRank historically exposes the requested rank spelling directly.
            if yields.is_empty()
                && let [clause] = tail
                && let ClauseKind::Return(projection) = &clause.kind
                && let Some(names) = identity_columns(projection)
                && names
                    .first()
                    .is_some_and(|name| name.eq_ignore_ascii_case("node"))
                && let Some((identity, score_name)) =
                    graph_algorithm_return_shape(*algorithm, &names)
                && (score_name.eq_ignore_ascii_case(default_score)
                    || (*algorithm == CypherGraphAlgorithmKind::PageRank
                        && score_name.eq_ignore_ascii_case("rank")))
            {
                return_node_identity = identity;
                score = score_name.to_string();
                tail = &[];
            }
            let mut columns = vec!["node".to_string()];
            if return_node_identity {
                columns.extend(["node_id".to_string(), "node_label".to_string()]);
            }
            if *algorithm == CypherGraphAlgorithmKind::Louvain {
                columns.push("level".to_string());
            }
            columns.push(score.clone());
            (
                LogicalPlan::GraphAlgorithm {
                    algorithm: plan_graph_algorithm_kind(*algorithm),
                    graph_name: graph_name.clone(),
                    options: bind_graph_algorithm_options(options, parameters)?,
                    score_column: score,
                    return_node_identity,
                    node_visibility_predicate: None,
                },
                columns,
            )
        }
        ProcedureCallKind::GraphSeedSearch(search) => {
            return bind_graph_seed_pipeline(search, yields, tail, parameters);
        }
        ProcedureCallKind::TextSearch(search) => {
            if yields.is_empty() && has_graph_read {
                return Err(unsupported("text search requires YIELD before MATCH"));
            }
            let seeded_match = !yields.is_empty() && has_graph_read;
            let input = bind_text_seed(search, parameters, seeded_match)?;
            (input, vec!["id".to_string(), "score".to_string()])
        }
        ProcedureCallKind::VectorSearch(search) => {
            let input = bind_vector_seed(search, parameters, !yields.is_empty())?;
            if yields.is_empty() && has_graph_read {
                return Err(unsupported("vector search requires YIELD before MATCH"));
            }
            if yields.is_empty()
                && let [clause] = tail
                && let ClauseKind::Return(projection) = &clause.kind
                && identity_columns(projection).is_some_and(|names| {
                    names.len() == 2
                        && names[0].eq_ignore_ascii_case("id")
                        && names[1].eq_ignore_ascii_case("score")
                })
            {
                tail = &[];
            }
            (input, vec!["id".to_string(), "score".to_string()])
        }
    };
    let seeded_match = !yields.is_empty()
        && (matches!(procedure.kind, ProcedureCallKind::VectorSearch(_))
            || (matches!(procedure.kind, ProcedureCallKind::TextSearch(_)) && has_graph_read));
    let producer = if matches!(procedure.kind, ProcedureCallKind::TextSearch(_)) {
        "text"
    } else {
        "vector"
    };
    let (next, mut scope) = bind_yields(input, &columns, yields, seeded_match)?;
    input = next;
    if seeded_match {
        let Some((clause, rest)) = tail.split_first() else {
            return Err(unsupported(&format!(
                "{producer} search YIELD must feed a MATCH read query"
            )));
        };
        let ClauseKind::Match {
            optional: false,
            patterns,
            predicate,
        } = &clause.kind
        else {
            return Err(unsupported(&format!(
                "{producer} search YIELD must feed a MATCH read query"
            )));
        };
        let [pattern] = patterns.as_slice() else {
            return Err(unsupported(&format!(
                "{producer} search requires one seeded MATCH pattern"
            )));
        };
        let hops = tail.iter().fold(0usize, |hops, clause| match &clause.kind {
            ClauseKind::Match { patterns, .. } => patterns.iter().fold(hops, |hops, pattern| {
                pattern.steps.iter().fold(hops, |hops, step| {
                    hops.saturating_add(step.relationship.max_hops)
                })
            }),
            _ => hops,
        });
        if hops > MAX_RETRIEVER_SEEDED_GRAPH_HOPS {
            return Err(unsupported(&format!("{producer}-seeded graph expansion supports at most {MAX_RETRIEVER_SEEDED_GRAPH_HOPS} hops")));
        }
        scope.bind_graph(
            &pattern.first.variable,
            GraphEntityKind::Node,
            &mut Vec::new(),
        )?;
        input = LogicalPlan::NodeColumnLookup {
            variable: pattern.first.variable.clone(),
            label: pattern.first.label.clone(),
            property: "id".to_string(),
            column: "external_id".to_string(),
            optional: false,
            node_visibility_predicate: None,
            input: Box::new(input),
        };
        if pattern.first.properties.is_empty() && pattern.steps.is_empty() {
            if let Some(predicate) = predicate {
                input = LogicalPlan::Filter {
                    predicate: bind_predicate(&predicate.kind, &scope, parameters)?,
                    input: Box::new(input),
                };
            }
            tail = rest;
        }
    }
    bind_read_clauses(tail, Some(input), scope, parameters)
}

fn identity_columns(projection: &ProjectionClause) -> Option<Vec<String>> {
    if projection.distinct
        || projection.predicate.is_some()
        || !projection.order_by.is_empty()
        || projection.offset.is_some()
        || projection.limit.is_some()
    {
        return None;
    }
    projection
        .items
        .iter()
        .map(|item| match &item.expression.kind {
            ReturnExpressionKind::Value(AstNode {
                kind: ScalarExpressionKind::Variable(name),
                ..
            }) if item.alias.is_none() => Some(name.clone()),
            _ => None,
        })
        .collect()
}

fn graph_algorithm_return_shape(
    algorithm: CypherGraphAlgorithmKind,
    names: &[String],
) -> Option<(bool, &str)> {
    let mut index = 1;
    let identity = names
        .get(index)
        .is_some_and(|name| name.eq_ignore_ascii_case("node_id"));
    if identity {
        if !names
            .get(index + 1)
            .is_some_and(|name| name.eq_ignore_ascii_case("node_label"))
        {
            return None;
        }
        index += 2;
    } else if names
        .get(index)
        .is_some_and(|name| name.eq_ignore_ascii_case("node_label"))
    {
        return None;
    }
    if algorithm == CypherGraphAlgorithmKind::Louvain
        && names
            .get(index)
            .is_some_and(|name| name.eq_ignore_ascii_case("level"))
    {
        index += 1;
    }
    (index + 1 == names.len()).then(|| (identity, names[index].as_str()))
}

fn bind_yields(
    input: LogicalPlan,
    columns: &[String],
    yields: &[YieldItem],
    preserve_external_id: bool,
) -> Result<(LogicalPlan, Scope)> {
    let mut scope = Scope::default();
    if yields.is_empty() {
        scope.0.extend(
            columns
                .iter()
                .map(|name| (name.clone(), BindingType::Scalar)),
        );
        return Ok((input, scope));
    }
    let mut items = Vec::new();
    let mut identity = yields.len() == columns.len();
    for (index, item) in yields.iter().enumerate() {
        let Some(column) = columns
            .iter()
            .find(|column| column.eq_ignore_ascii_case(&item.name))
        else {
            return Err(unsupported("YIELD references an unknown procedure column"));
        };
        let name = item.alias.as_ref().unwrap_or(column);
        if scope.0.insert(name.clone(), BindingType::Scalar).is_some()
            || (preserve_external_id && name == "external_id")
        {
            return Err(unsupported(
                "YIELD aliases must be distinct procedure columns",
            ));
        }
        identity &= columns.get(index) == Some(name) && name == column;
        items.push(Projection {
            expression: ProjectionExpression::Column(column.clone()),
            name: name.clone(),
        });
    }
    if identity {
        return Ok((input, scope));
    }
    if preserve_external_id {
        items.push(Projection {
            expression: ProjectionExpression::Column("external_id".to_string()),
            name: "external_id".to_string(),
        });
    }
    Ok((
        LogicalPlan::Project {
            items,
            input: Box::new(input),
        },
        scope,
    ))
}

fn unsupported(message: &str) -> HawDBError {
    HawDBError::Semantic(message.to_string())
}

fn bind_graph_seed_pipeline(
    search: &hawdb_cypher::GraphSeedSearch,
    yields: &[YieldItem],
    tail: &[Clause],
    parameters: &BTreeMap<String, Value>,
) -> Result<LogicalPlan> {
    let ValueExpressionKind::Parameter(query_parameter) = &search.query.kind else {
        return Err(unsupported("graph seed query must be a string parameter"));
    };
    if !matches!(parameters.get(query_parameter), Some(Value::String(_))) {
        return Err(unsupported("graph seed query must be a string parameter"));
    }
    let Value::String(label) = bind_value(&search.label, parameters)? else {
        return Err(unsupported("graph seed label must be a nonempty string"));
    };
    if label.is_empty() || label.contains('\0') {
        return Err(unsupported(
            "graph seed label must be a nonempty string without NUL",
        ));
    }
    let top_k = search
        .top_k
        .as_ref()
        .map(|value| bind_non_negative_usize(value, parameters, "topK"))
        .transpose()?
        .unwrap_or(10);
    let mut scope = Scope::default();
    let mut variable = "node".to_string();
    let mut score_column = "score".to_string();
    let mut seen = BTreeSet::new();
    for item in yields {
        let column = item.name.to_ascii_lowercase();
        if !seen.insert(column.clone()) {
            return Err(unsupported("graph seed YIELD columns must be distinct"));
        }
        let alias = item.alias.as_ref().unwrap_or(&column);
        if alias.contains('\0') || scope.0.contains_key(alias) {
            return Err(unsupported(
                "graph seed YIELD aliases must be distinct and public",
            ));
        }
        match column.as_str() {
            "node" => {
                variable = alias.clone();
                scope.bind_graph(alias, GraphEntityKind::Node, &mut Vec::new())?;
            }
            "score" => {
                score_column = alias.clone();
                scope.0.insert(alias.clone(), BindingType::Scalar);
            }
            _ => return Err(unsupported("unknown graph seed YIELD column")),
        }
    }
    if yields.is_empty() {
        scope.bind_graph(&variable, GraphEntityKind::Node, &mut Vec::new())?;
        scope.0.insert(score_column.clone(), BindingType::Scalar);
    }
    let hops = tail.iter().fold(0usize, |hops, clause| match &clause.kind {
        ClauseKind::Match { patterns, .. } => patterns.iter().fold(hops, |hops, pattern| {
            pattern.steps.iter().fold(hops, |hops, step| {
                hops.saturating_add(step.relationship.max_hops)
            })
        }),
        _ => hops,
    });
    if hops > MAX_RETRIEVER_SEEDED_GRAPH_HOPS {
        return Err(unsupported(
            "graph-seeded expansion supports at most two hops",
        ));
    }
    let input = LogicalPlan::GraphSeed {
        query_parameter: query_parameter.clone(),
        label,
        variable,
        score_column,
        top_k,
        node_visibility_predicate: None,
    };
    if tail.is_empty() {
        let items = scope
            .0
            .iter()
            .map(|(name, kind)| Projection {
                name: name.clone(),
                expression: match kind {
                    BindingType::Graph { .. } => ProjectionExpression::Variable {
                        variable: name.clone(),
                    },
                    _ => ProjectionExpression::Column(name.clone()),
                },
            })
            .collect();
        return Ok(LogicalPlan::Project {
            items,
            input: Box::new(input),
        });
    }
    bind_read_clauses(tail, Some(input), scope, parameters)
}
