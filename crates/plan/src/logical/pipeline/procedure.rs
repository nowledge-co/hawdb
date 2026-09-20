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
    let (mut input, columns) = match &procedure.kind {
        ProcedureCallKind::ProjectGraph {
            name,
            node_labels,
            rel_types,
        } => {
            if !yields.is_empty() || !tail.is_empty() {
                return Err(unsupported("project_graph does not yield query rows"));
            }
            return Ok(LogicalPlan::ProjectGraph {
                name: name.clone(),
                node_labels: node_labels.clone(),
                rel_types: rel_types.clone(),
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
            let mut columns = vec!["node".to_string()];
            if *algorithm == CypherGraphAlgorithmKind::Louvain {
                columns.push("level".to_string());
            }
            // PageRank historically exposes the requested rank spelling directly.
            if yields.is_empty()
                && let [clause] = tail
                && let ClauseKind::Return(projection) = &clause.kind
                && let Some(names) = identity_columns(projection)
                && names
                    .first()
                    .is_some_and(|name| name.eq_ignore_ascii_case("node"))
                && let Some(last) = names.last()
                && (last.eq_ignore_ascii_case(default_score)
                    || (*algorithm == CypherGraphAlgorithmKind::PageRank
                        && last.eq_ignore_ascii_case("rank")))
                && (names.len() == 2
                    || (*algorithm == CypherGraphAlgorithmKind::Louvain
                        && names.len() == 3
                        && names[1].eq_ignore_ascii_case("level")))
            {
                score = last.clone();
                tail = &[];
            }
            columns.push(score.clone());
            (
                LogicalPlan::GraphAlgorithm {
                    algorithm: plan_graph_algorithm_kind(*algorithm),
                    graph_name: graph_name.clone(),
                    options: bind_graph_algorithm_options(options, parameters)?,
                    score_column: score,
                    node_visibility_predicate: None,
                },
                columns,
            )
        }
        ProcedureCallKind::VectorSearch(search) => {
            let input = bind_vector_seed(search, parameters, !yields.is_empty())?;
            if yields.is_empty()
                && tail
                    .iter()
                    .any(|clause| matches!(clause.kind, ClauseKind::Match { .. }))
            {
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
    let vector_match =
        matches!(procedure.kind, ProcedureCallKind::VectorSearch(_)) && !yields.is_empty();
    let (next, mut scope) = bind_yields(input, &columns, yields, vector_match)?;
    input = next;
    if vector_match {
        let Some((clause, rest)) = tail.split_first() else {
            return Err(unsupported(
                "vector search YIELD must feed a MATCH read query",
            ));
        };
        let ClauseKind::Match {
            optional: false,
            patterns,
            predicate,
        } = &clause.kind
        else {
            return Err(unsupported(
                "vector search YIELD must feed a MATCH read query",
            ));
        };
        let [pattern] = patterns.as_slice() else {
            return Err(unsupported(
                "vector search requires one seeded MATCH pattern",
            ));
        };
        let hops = tail.iter().fold(0usize, |hops, clause| match &clause.kind {
            ClauseKind::Match { patterns, .. } => patterns.iter().fold(hops, |hops, pattern| {
                pattern.steps.iter().fold(hops, |hops, step| {
                    hops.saturating_add(step.relationship.max_hops)
                })
            }),
            _ => hops,
        });
        if hops > MAX_VECTOR_SEEDED_GRAPH_HOPS {
            return Err(unsupported(&format!("vector-seeded graph expansion supports at most {MAX_VECTOR_SEEDED_GRAPH_HOPS} hops")));
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
