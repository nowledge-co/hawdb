use super::*;

#[derive(Clone)]
pub(super) struct ScalarState {
    pub(super) state: AggregateExpressionState,
    pub(super) scalar_type: Option<RelationalScalarType>,
    coercible: bool,
}

impl ScalarState {
    pub(super) fn coerce(&mut self, target: Option<RelationalScalarType>) -> Result<()> {
        if let (Some(source), Some(target)) = (self.scalar_type, target)
            && source != target
            && !self.coercible
            && !numeric_pair(source, target)
        {
            return Err(SkeinError::Semantic(
                "HAVING operands have incompatible scalar types".into(),
            ));
        }
        if let (AggregateExpressionState::Constant(value), Some(target)) = (&self.state, target) {
            coerce_value(value.clone(), Some(target))?;
        }
        self.scalar_type = target;
        Ok(())
    }

    pub(super) fn output_memory_bytes(&self) -> usize {
        fn bytes(state: &AggregateExpressionState) -> usize {
            match state {
                AggregateExpressionState::Having(_) => std::mem::size_of::<Value>(),
                AggregateExpressionState::Constant(value) => {
                    skein_executor::binding::value_memory_bytes(value)
                }
                AggregateExpressionState::First { value, .. }
                | AggregateExpressionState::Numeric { value, .. } => value.as_ref().map_or(
                    std::mem::size_of::<Value>(),
                    super::super::aggregate_state::relational_value_memory_bytes,
                ),
                AggregateExpressionState::Count { .. } => std::mem::size_of::<Value>(),
                AggregateExpressionState::Coalesce(states) => states
                    .iter()
                    .map(bytes)
                    .max()
                    .unwrap_or(std::mem::size_of::<Value>()),
            }
        }
        bytes(&self.state)
    }
}

fn numeric_pair(left: RelationalScalarType, right: RelationalScalarType) -> bool {
    matches!(
        (left, right),
        (
            RelationalScalarType::BigInt,
            RelationalScalarType::DoublePrecision
        ) | (
            RelationalScalarType::DoublePrecision,
            RelationalScalarType::BigInt
        )
    )
}

pub(super) fn comparison_type(inputs: &[&ScalarState]) -> Result<Option<RelationalScalarType>> {
    let mut target = inputs
        .iter()
        .filter(|input| !input.coercible)
        .find_map(|input| input.scalar_type)
        .or_else(|| inputs.iter().find_map(|input| input.scalar_type));
    for input in inputs {
        if let (Some(current), Some(candidate)) = (target, input.scalar_type)
            && current != candidate
        {
            if numeric_pair(current, candidate) {
                target = Some(RelationalScalarType::DoublePrecision);
            } else if !input.coercible {
                return Err(SkeinError::Semantic(
                    "HAVING operands have incompatible scalar types".into(),
                ));
            }
        }
    }
    Ok(target)
}

pub(super) fn coerce_value(
    value: Value,
    target: Option<RelationalScalarType>,
) -> Result<RelationalValue> {
    let mut value = value_to_relational(value)?;
    if let Some(target) = target {
        if matches!(target, RelationalScalarType::DoublePrecision)
            && let RelationalValue::BigInt(integer) = value
        {
            value = RelationalValue::DoublePrecision(integer as f64);
        }
        value = super::super::super::coerce_relational_value(value, target)?;
        if value.scalar_type().is_some_and(|actual| actual != target) {
            return Err(SkeinError::Semantic(
                "HAVING operands have incompatible scalar types".into(),
            ));
        }
    }
    Ok(value)
}

pub(super) struct HavingBindings<'a> {
    relations: Vec<(&'a str, &'a RelationalTableSchema)>,
    group_keys: BTreeSet<(usize, usize)>,
    parameters: &'a [Value],
}

impl<'a> HavingBindings<'a> {
    pub(super) fn new(
        select: &'a SelectStatement,
        parameters: &'a [Value],
        state: &'a RelationalState,
    ) -> Result<Self> {
        let relations = std::iter::once((&select.from, &select.from_alias))
            .chain(select.joins.iter().map(|join| (&join.table, &join.alias)))
            .map(|(table, alias)| {
                let schema = state.table_schema(&table.name).ok_or_else(|| {
                    SkeinError::Semantic(format!("unknown relational table {}", table.name))
                })?;
                Ok((alias.as_deref().unwrap_or(&table.name), schema))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut bindings = Self {
            relations,
            group_keys: BTreeSet::new(),
            parameters,
        };
        for column in &select.group_by {
            bindings.group_keys.insert(bindings.column(column)?);
        }
        Ok(bindings)
    }

    fn column(&self, column: &SqlColumnRef) -> Result<(usize, usize)> {
        let mut matches =
            self.relations
                .iter()
                .enumerate()
                .filter_map(|(binding, (qualifier, schema))| {
                    if column
                        .qualifier
                        .as_deref()
                        .is_some_and(|candidate| candidate != *qualifier)
                    {
                        return None;
                    }
                    schema
                        .column_position(&column.name)
                        .map(|position| (binding, position))
                });
        let first = matches.next().ok_or_else(|| {
            SkeinError::Semantic(format!("unknown HAVING/grouped column {}", column.name))
        })?;
        if matches.next().is_some() {
            return Err(SkeinError::Semantic(format!(
                "ambiguous HAVING/grouped column {}",
                column.name
            )));
        }
        Ok(first)
    }

    fn column_type(&self, column: &SqlColumnRef, grouped: bool) -> Result<RelationalScalarType> {
        let (binding, position) = self.column(column)?;
        let schema = self.relations[binding].1;
        let determined_by_key = !schema.primary_key.is_empty()
            && schema.primary_key.iter().all(|name| {
                schema
                    .column_position(name)
                    .is_some_and(|position| self.group_keys.contains(&(binding, position)))
            });
        if grouped && !self.group_keys.contains(&(binding, position)) && !determined_by_key {
            return Err(SkeinError::Semantic(format!(
                "column {} must appear in GROUP BY or an aggregate",
                column.name
            )));
        }
        Ok(schema.columns[position].scalar_type)
    }

    pub(super) fn scalar(&self, expression: &Expr, grouped: bool) -> Result<ScalarState> {
        let scalar_type = self.scalar_type(expression, grouped)?;
        let mut expression = expression.clone();
        expression.try_visit_mut(&mut |node| {
            match &mut node.kind {
                ExprKind::Column(column) => {
                    let (binding, _) = self.column(column)?;
                    column.qualifier = Some(self.relations[binding].0.to_string());
                }
                ExprKind::Value(value @ SqlValue::Parameter(_)) => {
                    *value = SqlValue::Literal(bind_sql_value(value, self.parameters)?);
                }
                _ => {}
            }
            Ok::<_, SkeinError>(())
        })?;
        let coercible = matches!(expression.kind, ExprKind::Value(_));
        Ok(ScalarState {
            state: AggregateExpressionState::new(&expression, self.parameters)?,
            scalar_type,
            coercible,
        })
    }

    fn scalar_type(
        &self,
        expression: &Expr,
        grouped: bool,
    ) -> Result<Option<RelationalScalarType>> {
        match &expression.kind {
            ExprKind::Column(column) => self.column_type(column, grouped).map(Some),
            ExprKind::Value(value) => {
                Ok(value_to_relational(bind_sql_value(value, self.parameters)?)?.scalar_type())
            }
            ExprKind::Function {
                name,
                arguments,
                distinct,
                filter,
            } => {
                if name == "coalesce" {
                    if *distinct || filter.is_some() || arguments.is_empty() {
                        return Err(SkeinError::Semantic(
                            "COALESCE requires arguments without DISTINCT or FILTER".into(),
                        ));
                    }
                    let mut result = None;
                    for argument in arguments {
                        let SqlFunctionArgument::Expression(expression) = argument else {
                            return Err(SkeinError::Semantic(
                                "COALESCE does not accept wildcard".into(),
                            ));
                        };
                        if let Some(candidate) = self.scalar_type(expression, grouped)? {
                            if result.is_some_and(|current| current != candidate) {
                                return Err(SkeinError::Semantic(
                                    "COALESCE arguments have incompatible scalar types".into(),
                                ));
                            }
                            result = Some(candidate);
                        }
                    }
                    return Ok(result);
                }
                if !grouped {
                    return Err(SkeinError::Semantic(
                        "nested aggregates are not supported".into(),
                    ));
                }
                if let Some(filter) = filter {
                    let mut validator = Compiler {
                        bindings: self,
                        slots: Vec::new(),
                        grouped: false,
                    };
                    validator.predicate(filter)?;
                }
                let [argument] = arguments.as_slice() else {
                    return Err(SkeinError::Semantic(
                        "aggregate requires exactly one argument".into(),
                    ));
                };
                match (name.as_str(), argument) {
                    ("count", SqlFunctionArgument::Wildcard) if !distinct => {
                        Ok(Some(RelationalScalarType::BigInt))
                    }
                    (
                        "count",
                        SqlFunctionArgument::Expression(Expr {
                            kind: ExprKind::Column(column),
                            ..
                        }),
                    ) => {
                        self.column_type(column, false)?;
                        Ok(Some(RelationalScalarType::BigInt))
                    }
                    ("sum" | "max", SqlFunctionArgument::Expression(expression)) => {
                        let scalar_type = self.aggregate_input_type(expression)?;
                        if name == "sum"
                            && scalar_type.is_some_and(|scalar_type| {
                                !matches!(
                                    scalar_type,
                                    RelationalScalarType::BigInt
                                        | RelationalScalarType::DoublePrecision
                                )
                            })
                        {
                            return Err(SkeinError::Semantic(
                                "SUM requires BIGINT or DOUBLE PRECISION input".into(),
                            ));
                        }
                        Ok(scalar_type)
                    }
                    _ => Err(SkeinError::Semantic(format!(
                        "unsupported HAVING aggregate {name}"
                    ))),
                }
            }
            _ => Err(SkeinError::Semantic(
                "unsupported HAVING scalar expression".into(),
            )),
        }
    }

    fn aggregate_input_type(&self, expression: &Expr) -> Result<Option<RelationalScalarType>> {
        match &expression.kind {
            ExprKind::Column(column) => self.column_type(column, false).map(Some),
            ExprKind::Value(value) => {
                Ok(value_to_relational(bind_sql_value(value, self.parameters)?)?.scalar_type())
            }
            ExprKind::Function {
                name,
                arguments,
                distinct: false,
                filter: None,
            } if name == "octet_length" => {
                let [SqlFunctionArgument::Expression(Expr {
                    kind: ExprKind::Column(column),
                    ..
                })] = arguments.as_slice()
                else {
                    return Err(SkeinError::Semantic(
                        "OCTET_LENGTH requires exactly one column".into(),
                    ));
                };
                if !matches!(
                    self.column_type(column, false)?,
                    RelationalScalarType::Text | RelationalScalarType::Bytea
                ) {
                    return Err(SkeinError::Semantic(
                        "OCTET_LENGTH requires TEXT or BYTEA input".into(),
                    ));
                }
                Ok(Some(RelationalScalarType::BigInt))
            }
            _ => Err(SkeinError::Semantic(
                "nested or unsupported aggregate input expression".into(),
            )),
        }
    }
}
