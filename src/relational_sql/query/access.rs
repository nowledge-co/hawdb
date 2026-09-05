use super::{
    bind_sql_value, relational_unique_index_name, resolve_column, select_relational_access_path,
    value_to_relational_as, BTreeMap, BTreeSet, BoundRow, PlannedJoin, RelationalAccessCandidate,
    RelationalAccessPathDescriptor, RelationalAccessPathKind, RelationalBaseAccess,
    RelationalIndexRangeScan, RelationalIndexReadMode, RelationalIndexRuntime,
    RelationalIndexScanDirection, RelationalJoinAccess, RelationalJoinAccessCandidate,
    RelationalKey, RelationalReadRow, RelationalRowReadMode, RelationalRowRuntime, RelationalState,
    RelationalTableSchema, RelationalValue, Result, SkeinError, SqlColumnRef, SqlComparisonOp,
    SqlNullOrder, SqlOrderDirection, SqlPredicate, SqlValue, Value,
};

pub(super) struct RelationalBaseAccessPlanning<'a> {
    pub(super) predicate: Option<&'a SqlPredicate>,
    pub(super) order_by: &'a [crate::sql::SqlOrderItem],
    pub(super) prefer_ordered_access: bool,
    pub(super) parameters: &'a [Value],
    pub(super) state: &'a RelationalState,
    pub(super) schema: &'a RelationalTableSchema,
    pub(super) table: &'a str,
    pub(super) qualifier: &'a str,
    pub(super) cardinality_limit: usize,
    pub(super) projection: RelationalProjectionAccessPlanning,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RelationalProjectionAccessPlanning {
    pub(super) force_full_scan: bool,
    pub(super) row_count_override: Option<usize>,
}

pub(super) fn projection_access_planning(
    row_read_mode: RelationalRowReadMode<'_>,
    table: &str,
) -> RelationalProjectionAccessPlanning {
    RelationalProjectionAccessPlanning {
        force_full_scan: row_read_mode.is_projection_table(table),
        row_count_override: row_read_mode.projection_estimated_rows(table),
    }
}

pub(super) fn choose_base_access(
    planning: RelationalBaseAccessPlanning<'_>,
) -> Result<RelationalAccessCandidate> {
    let RelationalBaseAccessPlanning {
        predicate,
        order_by,
        prefer_ordered_access,
        parameters,
        state,
        schema,
        table,
        qualifier,
        cardinality_limit,
        projection,
    } = planning;
    let mut equalities = Vec::new();
    if let Some(predicate) = predicate {
        collect_conjunctive_equalities(predicate, &mut equalities);
    }
    let mut bound = BTreeMap::<String, RelationalValue>::new();
    for (column, value) in equalities {
        if column
            .qualifier
            .as_deref()
            .is_some_and(|candidate| candidate != table && candidate != qualifier)
        {
            continue;
        }
        let Some(position) = schema.column_position(&column.name) else {
            continue;
        };
        let value = value_to_relational_as(
            bind_sql_value(value, parameters)?,
            schema.columns[position].scalar_type,
        )?;
        if matches!(value, RelationalValue::Null) {
            continue;
        }
        if value.scalar_type() != Some(schema.columns[position].scalar_type) {
            return Err(SkeinError::Semantic(format!(
                "relational comparison on {} has an incompatible scalar type",
                column.name
            )));
        }
        match bound.entry(column.name.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(value);
            }
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &value => {}
            std::collections::btree_map::Entry::Occupied(_) => {
                // The residual predicate will reject the contradictory equality.
            }
        }
    }

    let row_count = projection
        .row_count_override
        .unwrap_or_else(|| state.row_count(table));
    let mut candidates = vec![RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count.min(cardinality_limit).max(1),
        },
        access: RelationalBaseAccess::FullScan,
    }];

    if !projection.force_full_scan {
        if let Some(key) = complete_key(&schema.primary_key, &bound) {
            let estimated_rows = usize::from(state.row(table, &key).is_some()).max(1);
            candidates.push(RelationalAccessCandidate {
                descriptor: RelationalAccessPathDescriptor {
                    kind: RelationalAccessPathKind::PrimaryKey,
                    name: "__primary_key".to_string(),
                    index_columns: schema.primary_key.clone(),
                    access_columns: schema.primary_key.iter().cloned().collect(),
                    equality_prefix_len: schema.primary_key.len(),
                    order_prefix_len: 0,
                    exclusive_range: false,
                    reverse_order: false,
                    unique_point: true,
                    covering: false,
                    requires_row_fetch: false,
                    estimated_rows,
                },
                access: RelationalBaseAccess::PrimaryKey(key),
            });
        }
        for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
            if let Some(candidate) = index_access_candidate(
                &planning,
                relational_unique_index_name(ordinal),
                columns,
                true,
                &bound,
            )? {
                candidates.push(candidate);
            }
        }
        for index in &schema.indexes {
            if let Some(candidate) = index_access_candidate(
                &planning,
                index.name.clone(),
                &index.columns,
                index.unique,
                &bound,
            )? {
                candidates.push(candidate);
            }
        }
    }

    let ordered_candidates = candidates
        .iter()
        .filter(|candidate| {
            prefer_ordered_access
                && !order_by.is_empty()
                && candidate.descriptor.order_prefix_len == order_by.len()
                && predicate_is_covered_by_access(
                    predicate,
                    &candidate.descriptor,
                    order_by,
                    table,
                    qualifier,
                )
        })
        .map(|candidate| candidate.descriptor.clone())
        .collect::<Vec<_>>();
    let descriptors = if ordered_candidates.is_empty() {
        candidates
            .iter()
            .map(|candidate| candidate.descriptor.clone())
            .collect()
    } else {
        ordered_candidates
    };
    let selected = select_relational_access_path(descriptors)
        .map_err(|error| SkeinError::Execution(format!("invalid relational access path: {error}")))?
        .expect("full scan is always an access-path candidate");
    let position = candidates
        .iter()
        .position(|candidate| candidate.descriptor == selected)
        .expect("selected relational access path came from the candidate set");
    Ok(candidates.swap_remove(position))
}

pub(super) fn complete_key(
    columns: &[String],
    bound: &BTreeMap<String, RelationalValue>,
) -> Option<RelationalKey> {
    columns
        .iter()
        .map(|column| bound.get(column).cloned())
        .collect::<Option<Vec<_>>>()
        .map(RelationalKey)
}

pub(super) fn index_access_candidate(
    planning: &RelationalBaseAccessPlanning<'_>,
    name: String,
    columns: &[String],
    unique: bool,
    bound: &BTreeMap<String, RelationalValue>,
) -> Result<Option<RelationalAccessCandidate>> {
    let RelationalBaseAccessPlanning {
        order_by,
        state,
        schema,
        table,
        qualifier,
        cardinality_limit,
        ..
    } = planning;
    let prefix = columns
        .iter()
        .map_while(|column| bound.get(column).cloned())
        .collect::<Vec<_>>();
    if prefix.is_empty() {
        return Ok(None);
    }
    let prefix_len = prefix.len();
    let (mut order_prefix_len, mut direction) =
        index_order_prefix(order_by, columns, prefix_len, schema, table, qualifier);
    let key = RelationalKey(prefix);
    let access_columns = columns[..prefix_len]
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let equality_covered =
        predicate_is_covered_by_equalities(planning.predicate, &access_columns, table, qualifier);
    let exclusive_bound = if equality_covered {
        None
    } else if order_prefix_len == order_by.len()
        && prefix_len.saturating_add(order_by.len()) == columns.len()
    {
        bind_canonical_keyset_bound(planning, &key, &access_columns)?
    } else {
        None
    };
    if !equality_covered && exclusive_bound.is_none() {
        order_prefix_len = 0;
        direction = RelationalIndexScanDirection::Forward;
    }
    let estimated_rows =
        match state.index_prefix_cardinality_at_most(table, &name, &key, *cardinality_limit) {
            Some(rows) => rows,
            None if !state.materialized_index_postings_resident() => {
                if unique && prefix_len == columns.len() {
                    usize::from(state.row_count(table) != 0)
                } else {
                    state.row_count(table).min(*cardinality_limit)
                }
            }
            None => {
                return Err(SkeinError::Execution(format!(
                    "relational index {name} on table {table} is not materialized"
                )));
            }
        }
        .max(1);
    Ok(Some(RelationalAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns,
            equality_prefix_len: prefix_len,
            order_prefix_len,
            exclusive_range: exclusive_bound.is_some(),
            reverse_order: direction == RelationalIndexScanDirection::Backward,
            unique_point: unique && prefix_len == columns.len(),
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        },
        access: RelationalBaseAccess::Index {
            name,
            scan: RelationalIndexRangeScan {
                prefix: key,
                exclusive_bound,
                direction,
            },
        },
    }))
}

pub(super) fn index_order_prefix(
    order_by: &[crate::sql::SqlOrderItem],
    index_columns: &[String],
    equality_prefix_len: usize,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
) -> (usize, RelationalIndexScanDirection) {
    if order_by.is_empty()
        || equality_prefix_len.saturating_add(order_by.len()) > index_columns.len()
    {
        return (0, RelationalIndexScanDirection::Forward);
    }
    let direction = match order_by[0].direction {
        SqlOrderDirection::Asc => RelationalIndexScanDirection::Forward,
        SqlOrderDirection::Desc => RelationalIndexScanDirection::Backward,
    };
    for (ordinal, item) in order_by.iter().enumerate() {
        if item.direction != order_by[0].direction
            || item
                .column
                .qualifier
                .as_deref()
                .is_some_and(|candidate| candidate != table && candidate != qualifier)
            || item.column.name != index_columns[equality_prefix_len + ordinal]
        {
            return (0, RelationalIndexScanDirection::Forward);
        }
        let Some(position) = schema.column_position(&item.column.name) else {
            return (0, RelationalIndexScanDirection::Forward);
        };
        if schema.columns[position].nullable
            && (direction == RelationalIndexScanDirection::Backward
                || !matches!(item.nulls, SqlNullOrder::First))
        {
            return (0, RelationalIndexScanDirection::Forward);
        }
    }
    (order_by.len(), direction)
}

pub(super) fn predicate_is_covered_by_access(
    predicate: Option<&SqlPredicate>,
    access: &RelationalAccessPathDescriptor,
    order_by: &[crate::sql::SqlOrderItem],
    table: &str,
    qualifier: &str,
) -> bool {
    predicate_is_covered_by_equalities(predicate, &access.access_columns, table, qualifier)
        || (access.order_prefix_len == order_by.len()
            && canonical_keyset_values(
                predicate,
                &access.access_columns,
                order_by,
                table,
                qualifier,
            )
            .is_some())
}

pub(super) fn predicate_is_covered_by_equalities(
    predicate: Option<&SqlPredicate>,
    access_columns: &BTreeSet<String>,
    table: &str,
    qualifier: &str,
) -> bool {
    fn covered(
        predicate: &SqlPredicate,
        access_columns: &BTreeSet<String>,
        table: &str,
        qualifier: &str,
        columns: &mut BTreeSet<String>,
    ) -> bool {
        match predicate {
            SqlPredicate::And(left, right) => {
                covered(left, access_columns, table, qualifier, columns)
                    && covered(right, access_columns, table, qualifier, columns)
            }
            SqlPredicate::Compare {
                left,
                op: SqlComparisonOp::Eq,
                ..
            } => {
                column_matches(left, table, qualifier)
                    && access_columns.contains(&left.name)
                    && columns.insert(left.name.clone())
            }
            _ => false,
        }
    }

    predicate.is_none_or(|predicate| {
        let mut columns = BTreeSet::new();
        covered(predicate, access_columns, table, qualifier, &mut columns)
            && columns == *access_columns
    })
}

pub(super) fn bind_canonical_keyset_bound(
    planning: &RelationalBaseAccessPlanning<'_>,
    prefix: &RelationalKey,
    access_columns: &BTreeSet<String>,
) -> Result<Option<RelationalKey>> {
    let RelationalBaseAccessPlanning {
        predicate,
        order_by,
        schema,
        table,
        qualifier,
        parameters,
        ..
    } = planning;
    let Some((first, second)) =
        canonical_keyset_values(*predicate, access_columns, order_by, table, qualifier)
    else {
        return Ok(None);
    };
    let mut bound = prefix.0.clone();
    for (value, item) in [(first, &order_by[0]), (second, &order_by[1])] {
        let Some(position) = schema.column_position(&item.column.name) else {
            return Ok(None);
        };
        let value = value_to_relational_as(
            bind_sql_value(value, parameters)?,
            schema.columns[position].scalar_type,
        )?;
        if schema.columns[position].nullable
            || matches!(value, RelationalValue::Null)
            || value.scalar_type() != Some(schema.columns[position].scalar_type)
        {
            return Ok(None);
        }
        bound.push(value);
    }
    Ok(Some(RelationalKey(bound)))
}

pub(super) fn canonical_keyset_values<'a>(
    predicate: Option<&'a SqlPredicate>,
    access_columns: &BTreeSet<String>,
    order_by: &[crate::sql::SqlOrderItem],
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let predicate = predicate?;
    if order_by.len() != 2 || order_by[0].direction != order_by[1].direction {
        return None;
    }
    let expected = match order_by[0].direction {
        SqlOrderDirection::Asc => SqlComparisonOp::Gt,
        SqlOrderDirection::Desc => SqlComparisonOp::Lt,
    };
    let mut terms = Vec::new();
    collect_conjuncts(predicate, &mut terms);
    let mut equality_columns = BTreeSet::new();
    let mut cursor = None;
    for term in terms {
        if let SqlPredicate::Compare {
            left,
            op: SqlComparisonOp::Eq,
            ..
        } = term
            && column_matches(left, table, qualifier)
            && access_columns.contains(&left.name)
        {
            if !equality_columns.insert(left.name.clone()) {
                return None;
            }
            continue;
        }
        if cursor.is_some() {
            return None;
        }
        cursor = match_keyset_or(
            term,
            &order_by[0].column,
            &order_by[1].column,
            expected,
            table,
            qualifier,
        );
        cursor?;
    }
    if equality_columns != *access_columns {
        return None;
    }
    cursor
}

pub(super) fn collect_conjuncts<'a>(
    predicate: &'a SqlPredicate,
    output: &mut Vec<&'a SqlPredicate>,
) {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_conjuncts(left, output);
            collect_conjuncts(right, output);
        }
        predicate => output.push(predicate),
    }
}

pub(super) fn match_keyset_or<'a>(
    predicate: &'a SqlPredicate,
    first_column: &SqlColumnRef,
    second_column: &SqlColumnRef,
    comparison: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let SqlPredicate::Or(left, right) = predicate else {
        return None;
    };
    match_keyset_branches(
        left,
        right,
        first_column,
        second_column,
        comparison,
        table,
        qualifier,
    )
    .or_else(|| {
        match_keyset_branches(
            right,
            left,
            first_column,
            second_column,
            comparison,
            table,
            qualifier,
        )
    })
}

pub(super) fn match_keyset_branches<'a>(
    first_branch: &'a SqlPredicate,
    tie_branch: &'a SqlPredicate,
    first_column: &SqlColumnRef,
    second_column: &SqlColumnRef,
    comparison: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<(&'a SqlValue, &'a SqlValue)> {
    let first = match_column_comparison(first_branch, first_column, comparison, table, qualifier)?;
    let SqlPredicate::And(left, right) = tie_branch else {
        return None;
    };
    let tie = match_column_comparison(left, first_column, SqlComparisonOp::Eq, table, qualifier)
        .zip(match_column_comparison(
            right,
            second_column,
            comparison,
            table,
            qualifier,
        ))
        .or_else(|| {
            match_column_comparison(right, first_column, SqlComparisonOp::Eq, table, qualifier).zip(
                match_column_comparison(left, second_column, comparison, table, qualifier),
            )
        })?;
    (first == tie.0).then_some((first, tie.1))
}

pub(super) fn match_column_comparison<'a>(
    predicate: &'a SqlPredicate,
    expected_column: &SqlColumnRef,
    expected_op: SqlComparisonOp,
    table: &str,
    qualifier: &str,
) -> Option<&'a SqlValue> {
    let SqlPredicate::Compare { left, op, right } = predicate else {
        return None;
    };
    (*op == expected_op
        && left.name == expected_column.name
        && column_matches(left, table, qualifier))
    .then_some(right)
}

pub(super) fn column_matches(column: &SqlColumnRef, table: &str, qualifier: &str) -> bool {
    column
        .qualifier
        .as_deref()
        .is_none_or(|candidate| candidate == table || candidate == qualifier)
}

pub(super) fn collect_conjunctive_equalities<'a>(
    predicate: &'a SqlPredicate,
    output: &mut Vec<(&'a SqlColumnRef, &'a SqlValue)>,
) {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_conjunctive_equalities(left, output);
            collect_conjunctive_equalities(right, output);
        }
        SqlPredicate::Compare {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => output.push((left, right)),
        _ => {}
    }
}

pub(super) fn choose_join_access(
    predicate: &SqlPredicate,
    state: &RelationalState,
    schema: &RelationalTableSchema,
    table: &str,
    qualifier: &str,
    index_read_mode: RelationalIndexReadMode<'_>,
    projection: RelationalProjectionAccessPlanning,
) -> Result<RelationalJoinAccessCandidate> {
    let mut bound = BTreeMap::<String, SqlColumnRef>::new();
    collect_conjunctive_join_equalities(predicate, table, qualifier, &mut bound);
    let row_count = projection
        .row_count_override
        .unwrap_or_else(|| state.row_count(table));
    let mut candidates = vec![RelationalJoinAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::FullScan,
            name: "__full_scan".to_string(),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 0,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: false,
            estimated_rows: row_count.max(1),
        },
        access: RelationalJoinAccess::FullScan,
    }];

    if !projection.force_full_scan {
        if let Some(columns) = complete_join_columns(&schema.primary_key, &bound) {
            candidates.push(RelationalJoinAccessCandidate {
                descriptor: RelationalAccessPathDescriptor {
                    kind: RelationalAccessPathKind::PrimaryKey,
                    name: "__primary_key".to_string(),
                    index_columns: schema.primary_key.clone(),
                    access_columns: schema.primary_key.iter().cloned().collect(),
                    equality_prefix_len: schema.primary_key.len(),
                    order_prefix_len: 0,
                    exclusive_range: false,
                    reverse_order: false,
                    unique_point: true,
                    covering: false,
                    requires_row_fetch: false,
                    estimated_rows: usize::from(row_count != 0).max(1),
                },
                access: RelationalJoinAccess::PrimaryKey(columns),
            });
        }
        for (ordinal, columns) in schema.unique_constraints.iter().enumerate() {
            if let Some(candidate) = join_index_access_candidate(
                relational_unique_index_name(ordinal),
                columns,
                true,
                &bound,
                row_count,
                table,
                index_read_mode,
            ) {
                candidates.push(candidate);
            }
        }
        for index in &schema.indexes {
            if let Some(candidate) = join_index_access_candidate(
                index.name.clone(),
                &index.columns,
                index.unique,
                &bound,
                row_count,
                table,
                index_read_mode,
            ) {
                candidates.push(candidate);
            }
        }
    }

    let selected = select_relational_access_path(
        candidates
            .iter()
            .map(|candidate| candidate.descriptor.clone()),
    )
    .map_err(|error| SkeinError::Execution(format!("invalid relational join access: {error}")))?
    .expect("full scan is always a join access-path candidate");
    let position = candidates
        .iter()
        .position(|candidate| candidate.descriptor == selected)
        .expect("selected relational join access path came from the candidate set");
    Ok(candidates.swap_remove(position))
}

pub(super) fn collect_conjunctive_join_equalities(
    predicate: &SqlPredicate,
    table: &str,
    qualifier: &str,
    output: &mut BTreeMap<String, SqlColumnRef>,
) {
    match predicate {
        SqlPredicate::And(left, right) => {
            collect_conjunctive_join_equalities(left, table, qualifier, output);
            collect_conjunctive_join_equalities(right, table, qualifier, output);
        }
        SqlPredicate::CompareColumns {
            left,
            op: SqlComparisonOp::Eq,
            right,
        } => {
            let left_is_join = column_targets_join(left, table, qualifier);
            let right_is_join = column_targets_join(right, table, qualifier);
            match (left_is_join, right_is_join) {
                (true, false) => {
                    output
                        .entry(left.name.clone())
                        .or_insert_with(|| right.clone());
                }
                (false, true) => {
                    output
                        .entry(right.name.clone())
                        .or_insert_with(|| left.clone());
                }
                (true, true) | (false, false) => {}
            }
        }
        _ => {}
    }
}

pub(super) fn column_targets_join(column: &SqlColumnRef, table: &str, qualifier: &str) -> bool {
    column
        .qualifier
        .as_deref()
        .is_some_and(|candidate| candidate == table || candidate == qualifier)
}

pub(super) fn complete_join_columns(
    columns: &[String],
    bound: &BTreeMap<String, SqlColumnRef>,
) -> Option<Vec<(String, SqlColumnRef)>> {
    columns
        .iter()
        .map(|column| {
            bound
                .get(column)
                .cloned()
                .map(|outer| (column.clone(), outer))
        })
        .collect()
}

pub(super) fn join_index_access_candidate(
    name: String,
    columns: &[String],
    unique: bool,
    bound: &BTreeMap<String, SqlColumnRef>,
    row_count: usize,
    table: &str,
    index_read_mode: RelationalIndexReadMode<'_>,
) -> Option<RelationalJoinAccessCandidate> {
    let access_columns = columns
        .iter()
        .map_while(|column| {
            bound
                .get(column)
                .cloned()
                .map(|outer| (column.clone(), outer))
        })
        .collect::<Vec<_>>();
    if access_columns.is_empty() {
        return None;
    }
    let equality_prefix_len = access_columns.len();
    let unique_point = unique && equality_prefix_len == columns.len();
    let estimated_rows = if unique_point {
        usize::from(row_count != 0)
    } else {
        index_read_mode
            .probe_statistics(table, &name, equality_prefix_len)
            .map(|statistics| {
                debug_assert!(statistics.distinct_non_null_values <= statistics.non_null_rows);
                debug_assert!(statistics.fanout <= statistics.non_null_rows);
                debug_assert!(statistics.fanout >= statistics.average_fanout());
                usize::try_from(statistics.average_fanout())
                    .unwrap_or(usize::MAX)
                    .min(row_count)
            })
            .unwrap_or(row_count)
    }
    .max(1);
    Some(RelationalJoinAccessCandidate {
        descriptor: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::Index,
            name: name.clone(),
            index_columns: columns.to_vec(),
            access_columns: columns[..equality_prefix_len].iter().cloned().collect(),
            equality_prefix_len,
            order_prefix_len: 0,
            exclusive_range: false,
            reverse_order: false,
            unique_point,
            covering: false,
            requires_row_fetch: true,
            estimated_rows,
        },
        access: RelationalJoinAccess::Index {
            name,
            columns: access_columns,
        },
    })
}

pub(super) fn bound_join_key(
    row: &BoundRow<'_>,
    schema: &RelationalTableSchema,
    columns: &[(String, SqlColumnRef)],
) -> Result<Option<RelationalKey>> {
    let mut values = Vec::with_capacity(columns.len());
    for (join_column, outer_column) in columns {
        let value = resolve_column(row, outer_column)?.clone();
        if matches!(value, RelationalValue::Null) {
            return Ok(None);
        }
        let position = schema.column_position(join_column).ok_or_else(|| {
            SkeinError::Semantic(format!(
                "relational join table {} has no column {join_column}",
                schema.name
            ))
        })?;
        if value.scalar_type() != Some(schema.columns[position].scalar_type) {
            return Err(SkeinError::Semantic(format!(
                "relational join comparison on {join_column} has an incompatible scalar type"
            )));
        }
        values.push(value);
    }
    Ok(Some(RelationalKey(values)))
}

pub(super) fn visit_join_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    planned: &PlannedJoin<'a>,
    row: &BoundRow<'a>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    let table = &planned.join.table.name;
    match &planned.access {
        RelationalJoinAccess::PrimaryKey(columns) => {
            let Some(key) = bound_join_key(row, planned.schema, columns)? else {
                return Ok(true);
            };
            match row_runtime.read_point(table, &key)? {
                Some(row) => visit(row),
                None => Ok(true),
            }
        }
        RelationalJoinAccess::Index { name, columns } => {
            let Some(prefix) = bound_join_key(row, planned.schema, columns)? else {
                return Ok(true);
            };
            index_runtime.visit_prefix(state, table, name, &prefix, |key| {
                match row_runtime.read_point(table, key)? {
                    Some(row) => visit(row),
                    None => Err(SkeinError::StorageIntegrity(format!(
                        "relational index {name} on table {table} points to missing row {key:?}"
                    ))),
                }
            })
        }
        RelationalJoinAccess::FullScan => row_runtime.visit_all(table, visit),
    }
}

pub(super) fn visit_base_entries<'a>(
    state: &'a RelationalState,
    index_runtime: &RelationalIndexRuntime<'_>,
    row_runtime: &RelationalRowRuntime<'a>,
    table: &str,
    access: &RelationalBaseAccess,
    descriptor: Option<&RelationalAccessPathDescriptor>,
    visit: &mut dyn FnMut(RelationalReadRow) -> Result<bool>,
) -> Result<bool> {
    match access {
        RelationalBaseAccess::PrimaryKey(key) => match row_runtime.read_point(table, key)? {
            Some(row) => visit(row),
            None => Ok(true),
        },
        RelationalBaseAccess::Index { name, scan } => {
            let covered_columns = descriptor
                .filter(|descriptor| descriptor.covering)
                .map(|descriptor| descriptor.index_columns.as_slice());
            index_runtime.visit_range_entries(state, table, name, scan, |index_key, key| {
                let row = match covered_columns {
                    Some(index_columns) => row_runtime
                        .read_index_covered(table, index_columns, index_key, key)?,
                    None => row_runtime.read_point(table, key)?,
                };
                match row {
                    Some(row) => visit(row),
                    None => Err(SkeinError::StorageIntegrity(format!(
                        "relational index {name} on table {table} points to missing or non-coverable row {key:?}"
                    ))),
                }
            })
        }
        RelationalBaseAccess::FullScan => row_runtime.visit_all(table, visit),
    }
}
