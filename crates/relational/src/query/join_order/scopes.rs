use super::*;

pub(in crate::query) fn bind_from_scopes(
    select: &mut SelectStatement,
    state: &RelationalState,
) -> Result<()> {
    if select.joins.iter().all(|join| join.on_scope_start == 0) {
        return Ok(());
    }
    let relations = bind_relations(select, state)?;
    let mut qualifiers = BTreeSet::new();
    for relation in &relations {
        if !qualifiers.insert(&relation.qualifier) {
            return Err(SkeinError::Semantic(format!(
                "duplicate relational FROM qualifier {}",
                relation.qualifier
            )));
        }
    }
    for (ordinal, join) in select.joins.iter_mut().enumerate() {
        let scope = relations
            .get(join.on_scope_start..=ordinal + 1)
            .ok_or_else(|| SkeinError::Semantic("invalid relational ON scope boundary".into()))?;
        qualify_expression(&mut join.on, scope)?;
    }
    for projection in &mut select.projection {
        if let SelectProjection::Expression { expression, .. } = projection {
            qualify_expression(expression, &relations)?;
        }
    }
    if let Some(predicate) = &mut select.selection {
        qualify_expression(predicate, &relations)?;
    }
    if let Some(predicate) = &mut select.having {
        qualify_expression(predicate, &relations)?;
    }
    for column in &mut select.group_by {
        *column = qualify_scoped_column(column, &relations)?;
    }
    let order_columns = select
        .order_by
        .iter()
        .map(
            |item| match resolve_relational_order_target(select, item)? {
                RelationalOrderTarget::InputColumn(column) => {
                    qualify_scoped_column(column, &relations).map(Some)
                }
                RelationalOrderTarget::ProjectionColumn { .. }
                | RelationalOrderTarget::ProjectionExpression { .. } => Ok(None),
            },
        )
        .collect::<Result<Vec<_>>>()?;
    for (item, column) in select.order_by.iter_mut().zip(order_columns) {
        if let Some(column) = column {
            item.expression.kind = ExprKind::Column(column);
        }
    }
    Ok(())
}

fn qualify_expression(expression: &mut Expr, relations: &[BoundRelation<'_>]) -> Result<()> {
    expression.try_visit_mut(&mut |node| {
        if let ExprKind::Column(column) = &mut node.kind {
            *column = qualify_scoped_column(column, relations)?;
        }
        Ok(())
    })
}

fn qualify_scoped_column(
    column: &SqlColumnRef,
    relations: &[BoundRelation<'_>],
) -> Result<SqlColumnRef> {
    let mut matches = relations.iter().filter(|relation| {
        column
            .qualifier
            .as_ref()
            .is_none_or(|qualifier| qualifier == &relation.qualifier)
            && relation.schema.column_position(&column.name).is_some()
    });
    let first = matches.next().ok_or_else(|| {
        SkeinError::Semantic(format!(
            "column {} is not visible in this relational FROM scope",
            column.name
        ))
    })?;
    if matches.next().is_some() {
        return Err(SkeinError::Semantic(format!(
            "ambiguous relational column {} in FROM scope",
            column.name
        )));
    }
    Ok(SqlColumnRef {
        qualifier: Some(first.qualifier.clone()),
        name: column.name.clone(),
    })
}
