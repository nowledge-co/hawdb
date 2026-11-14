// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
            && !source.same_value_domain(Some(target))
            && !self.coercible
            && !numeric_pair(source, target)
        {
            return Err(HawDBError::Semantic(
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
                AggregateExpressionState::Having(_)
                | AggregateExpressionState::Arithmetic { .. }
                | AggregateExpressionState::Negate(_)
                | AggregateExpressionState::Cast { .. } => std::mem::size_of::<Value>(),
                AggregateExpressionState::Constant(value) => {
                    hawdb_executor::binding::value_memory_bytes(value)
                }
                AggregateExpressionState::First { value, .. }
                | AggregateExpressionState::Numeric { value, .. } => value.as_ref().map_or(
                    std::mem::size_of::<Value>(),
                    super::super::state::relational_value_memory_bytes,
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
    left.logical_type().is_numeric()
        && right.logical_type().is_numeric()
        && hawdb_core::numeric::common_numeric_type(
            Some(left.logical_type()),
            Some(right.logical_type()),
        )
        .is_ok()
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
                target = Some(if matches!(current, RelationalScalarType::Decimal(_)) {
                    current
                } else if matches!(candidate, RelationalScalarType::Decimal(_)) {
                    candidate
                } else {
                    RelationalScalarType::DoublePrecision
                });
            } else if !input.coercible {
                return Err(HawDBError::Semantic(
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
    match target {
        Some(target) => crate::query_value::value_to_relational_as(value, target),
        None => value_to_relational(value),
    }
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
        let relations = std::iter::once((select.from_table(), &select.from_alias))
            .chain(select.joins.iter().map(|join| (&join.table, &join.alias)))
            .map(|(table, alias)| {
                let schema = state.table_schema(&table.name).ok_or_else(|| {
                    HawDBError::Semantic(format!("unknown relational table {}", table.name))
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
            HawDBError::Semantic(format!("unknown HAVING/grouped column {}", column.name))
        })?;
        if matches.next().is_some() {
            return Err(HawDBError::Semantic(format!(
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
            return Err(HawDBError::Semantic(format!(
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
            Ok::<_, HawDBError>(())
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
            ExprKind::Arithmetic { .. } | ExprKind::Negate(_) | ExprKind::Cast { .. } => {
                crate::query_value::infer_numeric_type(expression, &|child| {
                    self.scalar_type(child, grouped)
                })
            }
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
                        return Err(HawDBError::Semantic(
                            "COALESCE requires arguments without DISTINCT or FILTER".into(),
                        ));
                    }
                    let mut result = None;
                    for argument in arguments {
                        let SqlFunctionArgument::Expression(expression) = argument else {
                            return Err(HawDBError::Semantic(
                                "COALESCE does not accept wildcard".into(),
                            ));
                        };
                        if let Some(candidate) = self.scalar_type(expression, grouped)? {
                            if result.is_some_and(|current: RelationalScalarType| {
                                !current.same_value_domain(Some(candidate))
                            }) {
                                return Err(HawDBError::Semantic(
                                    "COALESCE arguments have incompatible scalar types".into(),
                                ));
                            }
                            result = Some(candidate);
                        }
                    }
                    return Ok(result);
                }
                if !grouped {
                    return Err(HawDBError::Semantic(
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
                    return Err(HawDBError::Semantic(
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
                    ("sum" | "min" | "max", SqlFunctionArgument::Expression(expression)) => {
                        let scalar_type = self.aggregate_input_type(expression)?;
                        if name == "sum"
                            && scalar_type.is_some_and(|scalar_type| {
                                !matches!(
                                    scalar_type,
                                    RelationalScalarType::BigInt
                                        | RelationalScalarType::DoublePrecision
                                        | RelationalScalarType::Decimal(_)
                                )
                            })
                        {
                            return Err(HawDBError::Semantic("SUM requires numeric input".into()));
                        }
                        Ok(scalar_type)
                    }
                    _ => Err(HawDBError::Semantic(format!(
                        "unsupported HAVING aggregate {name}"
                    ))),
                }
            }
            _ => Err(HawDBError::Semantic(
                "unsupported HAVING scalar expression".into(),
            )),
        }
    }

    fn aggregate_input_type(&self, expression: &Expr) -> Result<Option<RelationalScalarType>> {
        match &expression.kind {
            ExprKind::Arithmetic { .. } | ExprKind::Negate(_) | ExprKind::Cast { .. } => {
                crate::query_value::infer_numeric_type(expression, &|child| {
                    self.aggregate_input_type(child)
                })
            }
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
                    return Err(HawDBError::Semantic(
                        "OCTET_LENGTH requires exactly one column".into(),
                    ));
                };
                if !matches!(
                    self.column_type(column, false)?,
                    RelationalScalarType::Text | RelationalScalarType::Bytea
                ) {
                    return Err(HawDBError::Semantic(
                        "OCTET_LENGTH requires TEXT or BYTEA input".into(),
                    ));
                }
                Ok(Some(RelationalScalarType::BigInt))
            }
            _ => Err(HawDBError::Semantic(
                "nested or unsupported aggregate input expression".into(),
            )),
        }
    }
}
