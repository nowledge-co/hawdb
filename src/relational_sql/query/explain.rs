use super::{
    bind_bound, predicate_is_covered_by_access, projection_contains_aggregate,
    push_relational_output, single_count_distinct_column, RelationalAccessPathDescriptor,
    RelationalAccessPathKind, RelationalIndexExecutionEvidence, RelationalJoinPlanningOutcome,
    RelationalJoinPlanningStatus, RelationalOperatorCardinalityProfile, RelationalOperatorId,
    RelationalQueryLimits, RelationalQueryOutput, RelationalRowExecutionEvidence,
    RelationalSqlStageTimings, Result, Row, SelectProjection, SelectStatement, SqlColumnRef,
    SqlComparisonOp, SqlExpression, SqlFunctionArgument, SqlLikeEscape, SqlOrderDirection,
    SqlPredicate, SqlValue, Value,
};

#[derive(Debug)]
pub(super) struct RelationalExplainNode {
    pub(super) operator: &'static str,
    pub(super) identity: RelationalExplainNodeIdentity,
    pub(super) estimated_rows: Option<usize>,
    pub(super) access_object: String,
    pub(super) operator_info: String,
    pub(super) report_operator: Option<&'static str>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RelationalExplainNodeIdentity {
    Profile(RelationalOperatorId),
    Blocking(&'static str),
    Logical(&'static str),
}

impl RelationalExplainNodeIdentity {
    pub(super) fn profile_id(self) -> Option<RelationalOperatorId> {
        match self {
            Self::Profile(operator_id) => Some(operator_id),
            Self::Blocking(_) | Self::Logical(_) => None,
        }
    }

    pub(super) fn display_id(self) -> String {
        match self {
            Self::Profile(operator_id) => operator_id.get().to_string(),
            Self::Blocking(name) => format!("blocking_{name}"),
            Self::Logical(name) => format!("logical_{name}"),
        }
    }
}

pub(super) fn format_relational_explain(
    select: &SelectStatement,
    parameters: &[Value],
    mut output: RelationalQueryOutput,
    analyze: bool,
    limits: RelationalQueryLimits,
) -> Result<RelationalQueryOutput> {
    let actual_output_rows = output.rows.len();
    let mut nodes = Vec::new();
    let bound_limit = bind_bound(select.limit, parameters, "LIMIT")?;
    let bound_offset = bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0);
    if select.limit.is_some() || select.offset.is_some() {
        nodes.push(RelationalExplainNode {
            operator: "LimitExec",
            identity: RelationalExplainNodeIdentity::Logical("limit"),
            estimated_rows: bound_limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
            access_object: String::new(),
            operator_info: format!(
                "implementation=fused, offset={}, count={}",
                bound_offset,
                bound_limit
                    .map(|limit| limit.to_string())
                    .unwrap_or_else(|| "unbounded".to_string())
            ),
            report_operator: None,
        });
    }
    if !select.order_by.is_empty() && output.access_path.order_prefix_len != select.order_by.len() {
        nodes.push(RelationalExplainNode {
            operator: "TopNExec",
            identity: RelationalExplainNodeIdentity::Blocking("top_n"),
            estimated_rows: bound_limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
            access_object: String::new(),
            operator_info: format!(
                "order_by={}, offset={}",
                explain_order_by(&select.order_by),
                bound_offset
            ),
            report_operator: Some("TopNExec"),
        });
    }
    let has_aggregate = select.projection.iter().any(projection_contains_aggregate);
    if has_aggregate || !select.group_by.is_empty() {
        nodes.push(RelationalExplainNode {
            operator: "RelationalAggregateExec",
            identity: RelationalExplainNodeIdentity::Blocking("aggregate"),
            estimated_rows: (!select.group_by.is_empty()).then_some(
                output
                    .access_path
                    .estimated_rows
                    .min(limits.max_intermediate_rows),
            ),
            access_object: String::new(),
            operator_info: if select.group_by.is_empty() {
                format!(
                    "group_by=[], aggregates=[{}]",
                    explain_aggregate_projections(&select.projection)
                )
            } else {
                format!(
                    "group_by=[{}], aggregates=[{}]",
                    explain_columns(&select.group_by),
                    explain_aggregate_projections(&select.projection)
                )
            },
            report_operator: Some("RelationalAggregateExec"),
        });
    }
    if !select.group_by.is_empty() {
        nodes.push(RelationalExplainNode {
            operator: "SortExec",
            identity: RelationalExplainNodeIdentity::Blocking("group_sort"),
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: format!("group_keys=[{}]", explain_columns(&select.group_by)),
            report_operator: Some("SortExec"),
        });
    }
    if select.distinct || single_count_distinct_column(select).is_some() {
        nodes.push(RelationalExplainNode {
            operator: "DistinctExec",
            identity: RelationalExplainNodeIdentity::Blocking("distinct"),
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: if select.distinct {
                "scope=statement".to_string()
            } else {
                "scope=aggregate_argument".to_string()
            },
            report_operator: Some("DistinctExec"),
        });
    }
    nodes.push(RelationalExplainNode {
        operator: "ProjectionExec",
        identity: RelationalExplainNodeIdentity::Logical("projection"),
        estimated_rows: Some(output.access_path.estimated_rows),
        access_object: String::new(),
        operator_info: format!("implementation=fused, columns={}", select.projection.len()),
        report_operator: None,
    });
    if let Some(selection) = &select.selection
        && !predicate_is_covered_by_access(
            Some(selection),
            &output.access_path,
            &select.order_by,
            &select.from.name,
            select.from_alias.as_deref().unwrap_or(&select.from.name),
        )
    {
        nodes.push(RelationalExplainNode {
            operator: "SelectionExec",
            identity: RelationalExplainNodeIdentity::Logical("selection"),
            estimated_rows: Some(output.access_path.estimated_rows),
            access_object: String::new(),
            operator_info: format!(
                "implementation=fused, residual_predicate={}",
                explain_predicate(selection)
            ),
            report_operator: None,
        });
    }
    for cardinality in output.operator_cardinality_profiles.iter().skip(1) {
        let operator_id = cardinality.operator_id;
        let table = &cardinality.table;
        let descriptor = &cardinality.access_path;
        let access_path = explain_access_path(
            descriptor,
            relational_index_evidence(&output, table, descriptor),
            &output.row_execution_evidence,
        );
        nodes.push(RelationalExplainNode {
            operator: cardinality.operator.as_str(),
            identity: RelationalExplainNodeIdentity::Profile(operator_id),
            estimated_rows: Some(cardinality.estimated_rows),
            access_object: explain_access_object(table, descriptor),
            operator_info: format!(
                "{}, {access_path}",
                explain_join_planning(&output.join_planning, output.stage_timings)
            ),
            report_operator: None,
        });
    }
    let base_operator_id = RelationalOperatorId::from_plan_index(0);
    let base_cardinality = relational_operator_cardinality_profile(&output, base_operator_id);
    let base_table =
        base_cardinality.map_or(select.from.name.as_str(), |profile| profile.table.as_str());
    let base_access_path =
        base_cardinality.map_or(&output.access_path, |profile| &profile.access_path);
    nodes.push(RelationalExplainNode {
        operator: base_cardinality.map_or_else(
            || match base_access_path.kind {
                RelationalAccessPathKind::FullScan => "TableFullScanExec",
                RelationalAccessPathKind::PrimaryKey => "TablePointGetExec",
                RelationalAccessPathKind::Index => "IndexRangeScanExec",
            },
            |profile| profile.operator.as_str(),
        ),
        identity: RelationalExplainNodeIdentity::Profile(base_operator_id),
        estimated_rows: base_cardinality.map(|profile| profile.estimated_rows),
        access_object: explain_access_object(base_table, base_access_path),
        operator_info: explain_access_path(
            base_access_path,
            relational_index_evidence(&output, base_table, base_access_path),
            &output.row_execution_evidence,
        ),
        report_operator: None,
    });

    let mut rows = Vec::with_capacity(nodes.len());
    let mut payload_bytes = 0usize;
    for (index, node) in nodes.iter().enumerate() {
        let report = node.report_operator.and_then(|operator| {
            output
                .blocking_operator_memory_reports
                .iter()
                .find(|report| report.operator == operator)
        });
        let cardinality = node
            .identity
            .profile_id()
            .and_then(|operator_id| relational_operator_cardinality_profile(&output, operator_id));
        let display_id = node.identity.display_id();
        let mut row = Row::from([
            (
                "id".to_string(),
                Value::String(explain_tree_id(node.operator, index, &display_id)),
            ),
            (
                "estRows".to_string(),
                optional_estimated_rows_explain_value(node.estimated_rows),
            ),
            ("task".to_string(), Value::String("root".to_string())),
            (
                "access object".to_string(),
                Value::String(node.access_object.clone()),
            ),
            (
                "operator info".to_string(),
                Value::String(node.operator_info.clone()),
            ),
        ]);
        if analyze {
            row.insert(
                "actRows".to_string(),
                cardinality.map_or(Value::Null, |cardinality| {
                    optional_usize_explain_value(cardinality.actual_rows)
                }),
            );
            row.insert(
                "execution info".to_string(),
                if let Some(cardinality) = cardinality {
                    Value::String(format!(
                        "operator_id={}, fully_consumed={}",
                        cardinality.operator_id.get(),
                        cardinality.fully_consumed
                    ))
                } else if index == 0 {
                    Value::String(format!(
                        "statement_output_rows={actual_output_rows}, intermediate_rows={}, hydrated_rows={}, compressed_bytes={}, decompressed_bytes={}",
                        output.intermediate_rows,
                        output.hydration.hydrated_rows,
                        output.hydration.compressed_bytes,
                        output.hydration.decompressed_bytes,
                    ))
                } else {
                    report
                        .map(|report| {
                            Value::String(format!("input_rows={}", report.input_rows))
                        })
                        .unwrap_or(Value::Null)
                },
            );
            row.insert(
                "memory".to_string(),
                report
                    .map(|report| {
                        Value::String(format!(
                            "peak={}/budget={}",
                            report.peak_tracked_bytes, report.budget_bytes
                        ))
                    })
                    .unwrap_or(Value::Null),
            );
            row.insert(
                "disk".to_string(),
                report
                    .filter(|report| report.spill_run_count != 0)
                    .map(|report| {
                        Value::String(format!(
                            "runs={}, rows={}, bytes={}",
                            report.spill_run_count, report.spilled_rows, report.spilled_bytes
                        ))
                    })
                    .unwrap_or(Value::Null),
            );
        }
        push_relational_output(row, &mut rows, &mut payload_bytes, limits)?;
    }
    output.rows = rows.into();
    Ok(output)
}

pub(super) fn explain_join_planning(
    outcome: &RelationalJoinPlanningOutcome,
    stage_timings: RelationalSqlStageTimings,
) -> String {
    let join_order = if outcome.join_order_reordered() {
        "cost_reordered"
    } else if outcome.status == RelationalJoinPlanningStatus::Fallback {
        "syntax_fallback"
    } else {
        "syntax"
    };
    let memo_groups = outcome
        .memo_groups
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    let memo_expressions = outcome
        .memo_expressions
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unavailable".to_string());
    let selected_order = outcome.selected_order.join(",");
    let cost = outcome.cost.map_or_else(
        || "plan_cost=unavailable".to_string(),
        |cost| {
            format!(
                "estimated_rows={}, plan_cost={}, cpu={}, random_io={}, sequential_io={}, output_rows={}",
                cost.estimated_rows,
                cost.cost,
                cost.cpu,
                cost.random_io,
                cost.sequential_io,
                cost.output_rows
            )
        },
    );
    let attempts = outcome
        .attempts
        .iter()
        .enumerate()
        .map(|(index, attempt)| {
            let fallback_class = attempt
                .fallback_class
                .map(|class| class.as_str())
                .unwrap_or("none");
            let memo_groups = attempt
                .memo_groups
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_string());
            let memo_expressions = attempt
                .memo_expressions
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unavailable".to_string());
            let cost = attempt
                .cost
                .map(|cost| cost.cost.to_string())
                .unwrap_or_else(|| "unavailable".to_string());
            format!(
                "{index}:{}:{}:{}:fallback_class={fallback_class}:memo_groups={memo_groups}:memo_expressions={memo_expressions}:cost={cost}",
                attempt.strategy.as_str(),
                attempt.status.as_str(),
                attempt.reason.as_str(),
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "join_order={join_order}, planning_strategy={}, planning_status={}, planning_reason={}, memo_groups={memo_groups}, memo_expressions={memo_expressions}, max_groups={}, max_expressions={}, selected_order=[{selected_order}], attempts=[{attempts}], parse_nanos={}, bind_nanos={}, plan_nanos={}, execute_nanos={}, {cost}",
        outcome.strategy.as_str(),
        outcome.status.as_str(),
        outcome.reason.as_str(),
        outcome.budget.max_groups,
        outcome.budget.max_expressions,
        stage_timings.parse_nanos,
        stage_timings.bind_nanos,
        stage_timings.plan_nanos,
        stage_timings.execute_nanos,
    )
}

pub(super) fn explain_tree_id(operator: &str, index: usize, display_id: &str) -> String {
    if index == 0 {
        format!("{operator}_{display_id}")
    } else {
        format!("{}└─{operator}_{display_id}", "  ".repeat(index - 1))
    }
}

pub(super) fn optional_usize_explain_value(value: Option<usize>) -> Value {
    value
        .map(|value| Value::Int(i64::try_from(value).unwrap_or(i64::MAX)))
        .unwrap_or(Value::Null)
}

pub(super) fn optional_estimated_rows_explain_value(value: Option<usize>) -> Value {
    optional_usize_explain_value(value.map(|value| value.max(1)))
}

pub(super) fn explain_access_object(
    table: &str,
    descriptor: &RelationalAccessPathDescriptor,
) -> String {
    match descriptor.kind {
        RelationalAccessPathKind::FullScan => format!("table:{table}"),
        RelationalAccessPathKind::PrimaryKey => format!("table:{table}, primary_key"),
        RelationalAccessPathKind::Index => {
            format!("table:{table}, index:{}", descriptor.name)
        }
    }
}

pub(super) fn relational_operator_cardinality_profile(
    output: &RelationalQueryOutput,
    operator_id: RelationalOperatorId,
) -> Option<&RelationalOperatorCardinalityProfile> {
    output
        .operator_cardinality_profiles
        .iter()
        .find(|profile| profile.operator_id == operator_id)
}

pub(super) fn relational_index_evidence<'a>(
    output: &'a RelationalQueryOutput,
    table: &str,
    descriptor: &RelationalAccessPathDescriptor,
) -> Option<&'a RelationalIndexExecutionEvidence> {
    let physical_index = match descriptor.kind {
        RelationalAccessPathKind::PrimaryKey => skein_storage::RELATIONAL_PRIMARY_INDEX_NAME,
        RelationalAccessPathKind::Index => descriptor.name.as_str(),
        RelationalAccessPathKind::FullScan => return None,
    };
    output
        .index_execution_evidence
        .iter()
        .find(|evidence| evidence.table == table && evidence.index == physical_index)
}

pub(super) fn explain_access_path(
    descriptor: &RelationalAccessPathDescriptor,
    evidence: Option<&RelationalIndexExecutionEvidence>,
    row_evidence: &RelationalRowExecutionEvidence,
) -> String {
    let planned = format!(
        "equality_prefix={}, order_prefix={}, exclusive_seek={}, direction={}, unique_point={}, covering={}, row_fetch={}",
        descriptor.equality_prefix_len,
        descriptor.order_prefix_len,
        descriptor.exclusive_range,
        if descriptor.reverse_order {
            "backward"
        } else {
            "forward"
        },
        descriptor.unique_point,
        descriptor.covering,
        descriptor.requires_row_fetch
    );
    let row = format!(
        "row_runtime_path={}, row_projection_generation={}, row_projection_source_watermark={}, row_projection_version={}, row_projection_publication_epoch={}, row_base_generation={}, row_delta_generation={}, row_base_epoch={}, row_visible_epoch={}, row_root_set_digest={}, row_descriptor_reads={}, row_logical_pages={}, row_logical_bytes={}, row_physical_pages={}, row_physical_bytes={}, row_cache_hits={}, row_cache_misses={}, row_cache_admission_rejections={}, row_overlay_entries={}, row_overlay_bytes={}, row_rows={}, row_borrowed_rows={}, row_owned_rows={}, row_index_covered_rows={}",
        row_evidence.runtime_path,
        row_evidence
            .projection_generation
            .as_deref()
            .unwrap_or("none"),
        optional_u64_text(row_evidence.projection_source_watermark),
        optional_u64_text(row_evidence.projection_version),
        optional_u64_text(row_evidence.projection_publication_commit_epoch),
        optional_u64_text(row_evidence.base_generation),
        optional_u64_text(row_evidence.delta_generation),
        optional_u64_text(row_evidence.base_commit_epoch),
        optional_u64_text(row_evidence.visible_commit_epoch),
        row_evidence.root_set_digest.as_deref().unwrap_or("none"),
        row_evidence.descriptor_reads,
        row_evidence.logical_pages,
        row_evidence.logical_bytes,
        row_evidence.file_pages,
        row_evidence.file_bytes,
        row_evidence.cache_hits,
        row_evidence.cache_misses,
        row_evidence.cache_admission_rejections,
        row_evidence.overlay_entries,
        row_evidence.overlay_resident_bytes,
        row_evidence.rows_visited,
        row_evidence.borrowed_rows_visited,
        row_evidence.owned_rows_visited,
        row_evidence.index_covered_rows,
    );
    let Some(evidence) = evidence else {
        return format!("{planned}, {row}");
    };
    let fallback_reasons = if evidence.fallback_reasons.is_empty() {
        "none".to_string()
    } else {
        evidence
            .fallback_reasons
            .iter()
            .copied()
            .collect::<Vec<_>>()
            .join("|")
    };
    format!(
        "{planned}, runtime_path={}, lookups={}, range_lookups={}, exclusive_seek_lookups={}, backward_lookups={}, early_stop_lookups={}, demand_paged={}, authoritative={}, transaction_workspace={}, canonical_fallback={}, fallback_reasons={}, base_generation={}, delta_generation={}, base_epoch={}, visible_epoch={}, root_set_digest={}, logical_pages={}, logical_bytes={}, physical_pages={}, physical_bytes={}, cache_hits={}, cache_misses={}, cache_admission_rejections={}, delta_pages_skipped={}, delta_entries={}, live_batches={}, live_entries={}, live_matches={}, live_bytes={}, index_rows={}, {row}",
        evidence.runtime_path(),
        evidence.lookups,
        evidence.range_lookups,
        evidence.exclusive_seek_lookups,
        evidence.backward_lookups,
        evidence.early_stop_lookups,
        evidence.demand_paged_lookups,
        evidence.authoritative_lookups,
        evidence.transaction_workspace_lookups,
        evidence.canonical_fallback_lookups,
        fallback_reasons,
        optional_u64_text(evidence.base_generation),
        optional_u64_text(evidence.delta_generation),
        optional_u64_text(evidence.base_commit_epoch),
        optional_u64_text(evidence.visible_commit_epoch),
        evidence.root_set_digest.as_deref().unwrap_or("none"),
        evidence.logical_pages,
        evidence.logical_bytes,
        evidence.file_pages,
        evidence.file_bytes,
        evidence.cache_hits,
        evidence.cache_misses,
        evidence.cache_admission_rejections,
        evidence.delta_pages_skipped,
        evidence.delta_entries_visited,
        evidence.live_batches_visited,
        evidence.live_entries_visited,
        evidence.live_entries_matched,
        evidence.live_bytes_visited,
        evidence.rows_visited,
    )
}

pub(super) fn optional_u64_text(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

pub(super) fn explain_order_by(order_by: &[crate::sql::SqlOrderItem]) -> String {
    order_by
        .iter()
        .map(|item| {
            format!(
                "{} {}",
                explain_column(&item.column),
                match item.direction {
                    SqlOrderDirection::Asc => "ASC",
                    SqlOrderDirection::Desc => "DESC",
                }
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn explain_columns(columns: &[SqlColumnRef]) -> String {
    columns
        .iter()
        .map(explain_column)
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn explain_column(column: &SqlColumnRef) -> String {
    column
        .qualifier
        .as_ref()
        .map(|qualifier| format!("{qualifier}.{}", column.name))
        .unwrap_or_else(|| column.name.clone())
}

pub(super) fn explain_aggregate_projections(projections: &[SelectProjection]) -> String {
    projections
        .iter()
        .filter_map(|projection| match projection {
            SelectProjection::Expression { expression, .. } => {
                explain_aggregate_expression(expression)
            }
            SelectProjection::Wildcard | SelectProjection::Column { .. } => None,
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(super) fn explain_aggregate_expression(expression: &SqlExpression) -> Option<String> {
    let SqlExpression::Function {
        name,
        arguments,
        filter,
        ..
    } = expression
    else {
        return None;
    };
    if name == "coalesce" {
        let aggregates = arguments
            .iter()
            .filter_map(|argument| match argument {
                SqlFunctionArgument::Expression(expression) => {
                    explain_aggregate_expression(expression)
                }
                SqlFunctionArgument::Wildcard => None,
            })
            .collect::<Vec<_>>();
        return (!aggregates.is_empty()).then(|| format!("coalesce({})", aggregates.join(", ")));
    }
    if !matches!(name.as_str(), "count" | "sum" | "max") {
        return None;
    }
    let arguments = arguments
        .iter()
        .map(|argument| match argument {
            SqlFunctionArgument::Wildcard => "*".to_string(),
            SqlFunctionArgument::Expression(expression) => explain_expression(expression),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut explanation = format!("{name}({arguments})");
    if let Some(filter) = filter {
        explanation.push_str(" FILTER (");
        explanation.push_str(&explain_predicate(filter));
        explanation.push(')');
    }
    Some(explanation)
}

pub(super) fn explain_expression(expression: &SqlExpression) -> String {
    match expression {
        SqlExpression::Column(column) => explain_column(column),
        SqlExpression::Value(value) => explain_sql_value(value),
        SqlExpression::Function {
            name, arguments, ..
        } => format!(
            "{name}({})",
            arguments
                .iter()
                .map(|argument| match argument {
                    SqlFunctionArgument::Wildcard => "*".to_string(),
                    SqlFunctionArgument::Expression(expression) => explain_expression(expression),
                })
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

pub(super) fn explain_predicate(predicate: &SqlPredicate) -> String {
    match predicate {
        SqlPredicate::And(left, right) => {
            format!(
                "({} AND {})",
                explain_predicate(left),
                explain_predicate(right)
            )
        }
        SqlPredicate::Or(left, right) => {
            format!(
                "({} OR {})",
                explain_predicate(left),
                explain_predicate(right)
            )
        }
        SqlPredicate::Not(predicate) => format!("NOT ({})", explain_predicate(predicate)),
        SqlPredicate::Compare { left, op, right } => format!(
            "{} {} {}",
            explain_column(left),
            explain_comparison_operator(*op),
            explain_sql_value(right)
        ),
        SqlPredicate::CompareColumns { left, op, right } => format!(
            "{} {} {}",
            explain_column(left),
            explain_comparison_operator(*op),
            explain_column(right)
        ),
        SqlPredicate::InList {
            left,
            values,
            negated,
        } => format!(
            "{} {}IN ({})",
            explain_column(left),
            if *negated { "NOT " } else { "" },
            values
                .iter()
                .map(explain_sql_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlPredicate::Like {
            left,
            pattern,
            case_insensitive,
            negated,
            escape,
        } => {
            let mut explanation = format!(
                "{} {}{} {}",
                explain_column(left),
                if *negated { "NOT " } else { "" },
                if *case_insensitive { "ILIKE" } else { "LIKE" },
                explain_sql_value(pattern)
            );
            match escape {
                SqlLikeEscape::Character('\\') => {}
                SqlLikeEscape::Character(character) => {
                    explanation.push_str(&format!(" ESCAPE '{character}'"));
                }
                SqlLikeEscape::Disabled => explanation.push_str(" ESCAPE ''"),
            }
            explanation
        }
        SqlPredicate::IsNull { column, negated } => format!(
            "{} IS {}NULL",
            explain_column(column),
            if *negated { "NOT " } else { "" }
        ),
    }
}

pub(super) fn explain_comparison_operator(operator: SqlComparisonOp) -> &'static str {
    match operator {
        SqlComparisonOp::Eq => "=",
        SqlComparisonOp::NotEq => "!=",
        SqlComparisonOp::Lt => "<",
        SqlComparisonOp::Lte => "<=",
        SqlComparisonOp::Gt => ">",
        SqlComparisonOp::Gte => ">=",
    }
}

pub(super) fn explain_sql_value(value: &SqlValue) -> String {
    match value {
        SqlValue::Literal(value) => value.to_string(),
        SqlValue::Parameter(position) => format!("${position}"),
    }
}
