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

//! Transaction-local incremental maintenance. Definitions and state share the
//! relational WAL/checkpoint boundary; no separately acknowledged refresh exists.

use super::*;

const INTERNAL_PREFIX: &str = "__hawdb_mv_";
const KEY_COLUMN: &str = "__hawdb_mv_key";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewDefinition {
    pub source: String,
    pub columns: Vec<MaterializedViewColumn>,
    pub group_by: Vec<String>,
    pub filter: Option<RelationalPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewColumn {
    pub name: String,
    pub expression: MaterializedViewExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaterializedViewExpression {
    Column(String),
    Count(Option<String>),
    Sum(String),
    Min(String),
    Max(String),
}

impl RelationalTableSchema {
    pub fn visible_columns(&self) -> &[RelationalColumnSchema] {
        let count = self
            .materialized_view
            .as_ref()
            .map_or(self.columns.len(), |view| view.columns.len());
        &self.columns[..count]
    }
}

impl MaterializedViewDefinition {
    fn grouped(&self) -> bool {
        !self.group_by.is_empty()
            || self
                .columns
                .iter()
                .any(|column| !matches!(column.expression, MaterializedViewExpression::Column(_)))
    }

    fn has_extrema(&self) -> bool {
        self.columns.iter().any(|column| {
            matches!(
                column.expression,
                MaterializedViewExpression::Min(_) | MaterializedViewExpression::Max(_)
            )
        })
    }

    pub fn table_schema(
        &self,
        name: String,
        source: &RelationalTableSchema,
    ) -> Result<RelationalTableSchema, RelationalError> {
        if source.materialized_view.is_some() || source.name != self.source || name == self.source {
            return Err(RelationalError::Schema(
                "materialized views require one base RowPage table".into(),
            ));
        }
        if self.columns.is_empty() {
            return Err(RelationalError::Schema(
                "materialized view projection must not be empty".into(),
            ));
        }
        if let Some(filter) = &self.filter {
            validate_predicate(source, filter)?;
        }
        for column in &self.group_by {
            source_column(source, column)?;
        }
        if self.has_extrema() {
            extrema_index(self, source)?;
        }
        let mut names = BTreeSet::new();
        let mut columns = Vec::new();
        for column in &self.columns {
            if column.name.is_empty()
                || column.name.starts_with(INTERNAL_PREFIX)
                || !names.insert(&column.name)
            {
                return Err(RelationalError::Schema(
                    "materialized view output names must be unique and non-reserved".into(),
                ));
            }
            let (scalar_type, nullable) = match &column.expression {
                MaterializedViewExpression::Column(input) => {
                    if self.grouped() && !self.group_by.contains(input) {
                        return Err(RelationalError::Schema(format!(
                            "materialized view column {input} must be in GROUP BY"
                        )));
                    }
                    let input = source_column(source, input)?;
                    (input.scalar_type, input.nullable)
                }
                MaterializedViewExpression::Count(input) => {
                    if let Some(input) = input {
                        source_column(source, input)?;
                    }
                    (RelationalScalarType::BigInt, false)
                }
                MaterializedViewExpression::Min(input) | MaterializedViewExpression::Max(input) => {
                    let scalar_type = source_column(source, input)?.scalar_type;
                    if !matches!(
                        scalar_type,
                        RelationalScalarType::BigInt | RelationalScalarType::Text
                    ) {
                        return Err(RelationalError::Schema(
                            "materialized MIN/MAX requires BIGINT or TEXT input".into(),
                        ));
                    }
                    (scalar_type, true)
                }
                MaterializedViewExpression::Sum(input) => {
                    if source_column(source, input)?.scalar_type != RelationalScalarType::BigInt {
                        return Err(RelationalError::Schema(
                            "materialized SUM requires BIGINT input".into(),
                        ));
                    }
                    (RelationalScalarType::BigInt, true)
                }
            };
            columns.push(RelationalColumnSchema {
                name: column.name.clone(),
                scalar_type,
                nullable,
                default: None,
            });
        }
        columns.push(RelationalColumnSchema {
            name: KEY_COLUMN.into(),
            scalar_type: RelationalScalarType::Bytea,
            nullable: false,
            default: None,
        });
        if self.grouped() {
            // Retraction needs the group's cardinality and each SUM's non-NULL
            // cardinality even when the SELECT does not expose COUNT.
            columns.push(internal_count("__hawdb_mv_rows".into()));
            for (position, column) in self.columns.iter().enumerate() {
                if matches!(column.expression, MaterializedViewExpression::Sum(_)) {
                    columns.push(internal_count(format!("__hawdb_mv_count_{position}")));
                }
            }
        }
        let schema = RelationalTableSchema {
            name,
            columns,
            primary_key: vec![KEY_COLUMN.into()],
            unique_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            materialized_view: Some(Box::new(self.clone())),
        };
        validate_table_schema(&schema)?;
        Ok(schema)
    }
}

pub(super) fn clone_definition(
    view: &MaterializedViewDefinition,
    context: &crate::background::CheckpointDecodeContext,
) -> Result<Box<MaterializedViewDefinition>, RelationalError> {
    context
        .reserve(std::mem::size_of::<MaterializedViewDefinition>())
        .map_err(|error| RelationalError::Admission(error.to_string()))?;
    let name = |value: &str| clone_string_with_allocations(value, Some(context));
    let columns = collect_results_with_allocations(
        view.columns.iter().map(|column| {
            let expression = match &column.expression {
                MaterializedViewExpression::Column(input) => {
                    MaterializedViewExpression::Column(name(input)?)
                }
                MaterializedViewExpression::Count(input) => {
                    MaterializedViewExpression::Count(input.as_deref().map(name).transpose()?)
                }
                MaterializedViewExpression::Sum(input) => {
                    MaterializedViewExpression::Sum(name(input)?)
                }
                MaterializedViewExpression::Min(input) => {
                    MaterializedViewExpression::Min(name(input)?)
                }
                MaterializedViewExpression::Max(input) => {
                    MaterializedViewExpression::Max(name(input)?)
                }
            };
            Ok(MaterializedViewColumn {
                name: name(&column.name)?,
                expression,
            })
        }),
        view.columns.len(),
        Some(context),
    )?;
    Ok(Box::new(MaterializedViewDefinition {
        source: name(&view.source)?,
        columns,
        group_by: clone_schema_strings(&view.group_by, context)?,
        filter: view
            .filter
            .as_ref()
            .map(|filter| clone_filter(filter, context))
            .transpose()?,
    }))
}

fn clone_filter(
    filter: &RelationalPredicate,
    context: &crate::background::CheckpointDecodeContext,
) -> Result<RelationalPredicate, RelationalError> {
    let boxed = |filter: &RelationalPredicate| {
        context
            .reserve(std::mem::size_of::<RelationalPredicate>())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        Ok::<_, RelationalError>(Box::new(clone_filter(filter, context)?))
    };
    Ok(match filter {
        RelationalPredicate::And(left, right) => {
            RelationalPredicate::And(boxed(left)?, boxed(right)?)
        }
        RelationalPredicate::Or(left, right) => {
            RelationalPredicate::Or(boxed(left)?, boxed(right)?)
        }
        RelationalPredicate::Not(inner) => RelationalPredicate::Not(boxed(inner)?),
        RelationalPredicate::Compare { column, op, value } => RelationalPredicate::Compare {
            column: clone_string_with_allocations(column, Some(context))?,
            op: *op,
            value: value.as_ref().to_owned_value_with_allocations(context)?,
        },
        RelationalPredicate::IsNull { column, negated } => RelationalPredicate::IsNull {
            column: clone_string_with_allocations(column, Some(context))?,
            negated: *negated,
        },
    })
}

fn internal_count(name: String) -> RelationalColumnSchema {
    RelationalColumnSchema {
        name,
        scalar_type: RelationalScalarType::BigInt,
        nullable: false,
        default: None,
    }
}

fn source_column<'a>(
    schema: &'a RelationalTableSchema,
    name: &str,
) -> Result<&'a RelationalColumnSchema, RelationalError> {
    schema
        .columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| {
            RelationalError::Schema(format!("unknown materialized view input column {name}"))
        })
}

pub(super) fn validate_definition(
    state: &RelationalState,
    schema: &RelationalTableSchema,
) -> Result<(), RelationalError> {
    if let Some(view) = &schema.materialized_view {
        let source = state.table_schema(&view.source).ok_or_else(|| {
            RelationalError::Schema(format!("unknown materialized view source {}", view.source))
        })?;
        let mut expected = view.table_schema(schema.name.clone(), source)?;
        for index in &schema.indexes {
            validate_view_index(schema, index)?;
        }
        expected.indexes.clone_from(&schema.indexes);
        if expected != *schema {
            return Err(RelationalError::Schema(
                "materialized view schema does not match its definition".into(),
            ));
        }
    }
    for foreign_key in &schema.foreign_keys {
        if state
            .table_schema(&foreign_key.referenced_table)
            .is_some_and(|target| target.materialized_view.is_some())
        {
            return Err(RelationalError::Schema(
                "foreign keys cannot reference materialized views".into(),
            ));
        }
    }
    Ok(())
}

pub(super) fn reject_write(
    state: &RelationalState,
    write: &RelationalWrite,
) -> Result<(), RelationalError> {
    let table = match write {
        RelationalWrite::CreateTable(_) | RelationalWrite::DropMaterializedView { .. } => {
            return Ok(())
        }
        RelationalWrite::CreateIndex { table, index } => {
            if let Some(schema) = state.table_schema(table)
                && schema.materialized_view.is_some()
            {
                validate_view_index(schema, index)?;
            }
            return Ok(());
        }
        RelationalWrite::AddColumn { table, .. }
        | RelationalWrite::Insert { table, .. }
        | RelationalWrite::Upsert { table, .. }
        | RelationalWrite::DeleteByPrimaryKey { table, .. }
        | RelationalWrite::DeleteWhere { table, .. }
        | RelationalWrite::UpdateWhere { table, .. } => table,
    };
    if state
        .table_schema(table)
        .is_some_and(|schema| schema.materialized_view.is_some())
    {
        return Err(RelationalError::Schema(format!(
            "materialized view {table} is read-only"
        )));
    }
    Ok(())
}

fn validate_view_index(
    schema: &RelationalTableSchema,
    index: &RelationalIndexSchema,
) -> Result<(), RelationalError> {
    if index.columns.iter().any(|name| {
        !schema
            .visible_columns()
            .iter()
            .any(|column| column.name == *name)
    }) {
        return Err(RelationalError::Schema(
            "materialized view indexes require visible output columns".into(),
        ));
    }
    Ok(())
}

fn extrema_index<'a>(
    definition: &MaterializedViewDefinition,
    source: &'a RelationalTableSchema,
) -> Result<&'a RelationalIndexSchema, RelationalError> {
    // Unique indexes omit NULL keys. A non-unique prefix is complete even for
    // nullable grouping columns and nullable suffixes.
    source.indexes.iter().find(|index| !index.unique && index.columns.starts_with(&definition.group_by))
        .ok_or_else(|| RelationalError::Schema("materialized MIN/MAX requires a non-unique source index starting with the GROUP BY columns".into()))
}

pub(super) struct MaintenanceOptions<'a> {
    pub limits: RelationalMutationLimits,
    pub overflow_config: RelationalOverflowConfig,
    pub allocations: Option<&'a crate::background::CheckpointDecodeContext>,
}

struct MaintenanceBudget<'a> {
    allocations: Option<&'a crate::background::CheckpointDecodeContext>,
    limits: RelationalMutationLimits,
    rows: usize,
    bytes: usize,
    hydration: RelationalHydrationBudget,
}

impl<'a> MaintenanceBudget<'a> {
    fn new(
        limits: RelationalMutationLimits,
        allocations: Option<&'a crate::background::CheckpointDecodeContext>,
    ) -> Self {
        Self {
            allocations,
            limits,
            rows: 0,
            bytes: 0,
            hydration: RelationalHydrationBudget {
                max_rows: limits.max_rows.get(),
                max_compressed_bytes: limits.max_payload_bytes.get(),
                max_decompressed_bytes: limits.max_payload_bytes.get(),
                max_memory_bytes: limits.max_payload_bytes.get(),
                ..RelationalHydrationBudget::default()
            },
        }
    }

    fn ensure_rows(&self, additional: usize) -> Result<(), RelationalError> {
        if additional > self.limits.max_rows.get().saturating_sub(self.rows) {
            return Err(RelationalError::Admission(
                "materialized view maintenance exceeds the transaction row budget".into(),
            ));
        }
        Ok(())
    }

    fn charge(&mut self, rows: usize, bytes: usize) -> Result<(), RelationalError> {
        self.rows = self.rows.saturating_add(rows);
        self.bytes = self.bytes.saturating_add(bytes);
        if self.rows > self.limits.max_rows.get()
            || self.bytes > self.limits.max_payload_bytes.get()
        {
            return Err(RelationalError::Admission(
                "materialized view maintenance exceeds the transaction row or payload budget"
                    .into(),
            ));
        }
        if let Some(context) = self.allocations {
            context
                .reserve(
                    bytes.saturating_add(rows.saturating_mul(std::mem::size_of::<RelationalRow>())),
                )
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            context
                .checkpoint()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
        }
        Ok(())
    }

    fn read(
        &mut self,
        state: &RelationalState,
        table: &str,
        key: &RelationalKey,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        self.ensure_rows(1)?;
        let row = state.hydrate_row_with_context(
            table,
            key,
            &mut self.hydration,
            self.allocations.map(|context| context.work.task_context()),
        )?;
        if let Some(row) = &row {
            self.charge(1, row.estimated_payload_bytes())?;
        }
        Ok(row)
    }

    fn copy_value(&mut self, value: &RelationalValue) -> Result<RelationalValue, RelationalError> {
        // Repeated projection/group expressions can amplify a single source
        // payload. Admit each copy before allocating it, separately from output.
        self.charge(0, value.estimated_payload_bytes())?;
        match self.allocations {
            Some(context) => value.as_ref().to_owned_value_with_allocations(context),
            None => Ok(value.clone()),
        }
    }
}

pub(super) fn maintain(
    previous: &RelationalState,
    next: &mut RelationalState,
    changed_keys: &mut BTreeMap<String, BTreeSet<RelationalKey>>,
    touched: &mut BTreeSet<String>,
    rebuilt_tables: &BTreeSet<String>,
    options: MaintenanceOptions<'_>,
) -> Result<(), RelationalError> {
    let MaintenanceOptions {
        limits,
        overflow_config,
        allocations,
    } = options;
    let schemas = collect_results_with_allocations(
        next.schemas
            .values()
            .filter(|schema| schema.materialized_view.is_some())
            .map(|schema| Ok(Arc::clone(schema))),
        next.schemas
            .values()
            .filter(|schema| schema.materialized_view.is_some())
            .count(),
        allocations,
    )?;
    let mut budget = MaintenanceBudget::new(limits, allocations);
    for schema in schemas {
        let definition = schema.materialized_view.as_ref().expect("view schema");
        validate_definition(next, &schema)?;
        let rebuild = rebuilt_tables.contains(&schema.name);
        let source = Arc::clone(
            next.schemas
                .get(&definition.source)
                .expect("validated source"),
        );
        let keys = if rebuild {
            let rows = &next.segments[&source.name].rows;
            // Reject known read + output costs before allocating backfill keys.
            // Filtered projections and grouped cardinalities are charged exactly
            // as they are discovered, without a second scan to estimate them.
            let outputs = if !definition.grouped() && definition.filter.is_none() {
                rows.len()
            } else if definition.grouped() && definition.group_by.is_empty() {
                1
            } else {
                usize::from(rows.len() != 0 && definition.filter.is_none())
            };
            budget.ensure_rows(rows.len().saturating_add(outputs))?;
            let mut keys = BTreeSet::new();
            for (key, _) in rows.iter() {
                budget.charge(
                    0,
                    key.0
                        .iter()
                        .map(RelationalValue::estimated_payload_bytes)
                        .sum(),
                )?;
                insert_changed_key_with_allocations(&mut keys, key, allocations)?;
            }
            keys
        } else if let Some(keys) = changed_keys.get(&source.name) {
            let mut copied = BTreeSet::new();
            for key in keys {
                insert_changed_key_with_allocations(&mut copied, key, allocations)?;
            }
            copied
        } else {
            continue;
        };
        let view_keys =
            changed_table_keys_with_allocations(changed_keys, &schema.name, allocations)?;
        if definition.grouped() {
            let mut deltas = BTreeMap::<RelationalKey, GroupDelta>::new();
            if rebuild && definition.group_by.is_empty() {
                budget.charge(0, GroupDelta::estimated_bytes(definition))?;
                deltas.insert(RelationalKey(Vec::new()), GroupDelta::new(definition));
            }
            for key in &keys {
                if !rebuild && let Some(row) = budget.read(previous, &source.name, key)? {
                    add_group_delta(definition, &source, &row, -1, &mut deltas, &mut budget)?;
                }
                if let Some(row) = budget.read(next, &source.name, key)? {
                    add_group_delta(definition, &source, &row, 1, &mut deltas, &mut budget)?;
                }
            }
            for (group, mut delta) in deltas {
                if !rebuild && definition.has_extrema() {
                    merge_unchanged_extrema(
                        previous,
                        definition,
                        &source,
                        &group,
                        &keys,
                        &mut delta,
                        &mut budget,
                    )?;
                }
                let key = encoded_key(&group)?;
                let old = if rebuild {
                    None
                } else {
                    budget.read(previous, &schema.name, &key)?
                };
                let row =
                    apply_group_delta(definition, &group, &key, old.as_ref(), delta, &mut budget)?;
                write_output(
                    next,
                    &schema,
                    key,
                    row,
                    view_keys,
                    overflow_config,
                    &mut budget,
                )?;
            }
        } else {
            for key in keys {
                let output_key = encoded_key(&key)?;
                let row = budget.read(next, &source.name, &key)?;
                // Evaluate errors explicitly; filter NULL is SQL UNKNOWN.
                let row = match row {
                    Some(row) if matches_filter(definition, &source, &row)? => {
                        let mut values = definition
                            .columns
                            .iter()
                            .map(|column| {
                                let MaterializedViewExpression::Column(name) = &column.expression
                                else {
                                    unreachable!("projection view")
                                };
                                budget.copy_value(value(&source, &row, name))
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        values.extend(output_key.0.iter().cloned());
                        Some(RelationalRow::new(values))
                    }
                    _ => None,
                };
                if rebuild && row.is_none() {
                    continue;
                }
                write_output(
                    next,
                    &schema,
                    output_key,
                    row,
                    view_keys,
                    overflow_config,
                    &mut budget,
                )?;
            }
        }
        insert_table_name_with_allocations(touched, &schema.name, allocations)?;
    }
    Ok(())
}

fn write_output(
    next: &mut RelationalState,
    schema: &RelationalTableSchema,
    key: RelationalKey,
    row: Option<RelationalRow>,
    changed_keys: &mut BTreeSet<RelationalKey>,
    overflow_config: RelationalOverflowConfig,
    budget: &mut MaintenanceBudget<'_>,
) -> Result<(), RelationalError> {
    budget.charge(
        1,
        key.0
            .iter()
            .map(RelationalValue::estimated_payload_bytes)
            .sum(),
    )?;
    let row = row
        .map(|mut row| {
            budget.charge(0, row.estimated_payload_bytes())?;
            validate_row(schema, &row)?;
            if let Some(context) = budget.allocations {
                row =
                    RelationalRow::reconstruct_with_allocations(&row, Some(context), |ordinal| {
                        Ok(row.values()[ordinal].as_ref())
                    })?;
            }
            overflow::externalize_row_with_allocations(
                next,
                schema,
                &mut row,
                overflow_config,
                budget.allocations,
            )?;
            Ok::<_, RelationalError>(row)
        })
        .transpose()?;
    let segment = mutable_segment_with_allocations(
        next.segments.get_mut(&schema.name).expect("view segment"),
        budget.allocations,
    )?;
    if let Some(row) = row {
        segment
            .rows
            .insert_with_allocations(key.clone(), row, budget.allocations)?;
    } else {
        if let Some(context) = budget.allocations {
            segment
                .rows
                .prepare_checkpoint_copy_for_key(&key, &context.work)?;
        }
        segment.rows.remove(&key);
    }
    insert_changed_key_with_allocations(changed_keys, &key, budget.allocations)?;
    Ok(())
}

fn value<'a>(
    source: &RelationalTableSchema,
    row: &'a RelationalRow,
    name: &str,
) -> &'a RelationalValue {
    &row.values()[source.column_position(name).expect("validated view input")]
}

fn matches_filter(
    definition: &MaterializedViewDefinition,
    source: &RelationalTableSchema,
    row: &RelationalRow,
) -> Result<bool, RelationalError> {
    definition.filter.as_ref().map_or(Ok(true), |filter| {
        Ok(predicate_truth(source, row, filter)? == Some(true))
    })
}

fn encoded_key(key: &RelationalKey) -> Result<RelationalKey, RelationalError> {
    Ok(RelationalKey(vec![RelationalValue::Bytea(
        codec::encode_relational_row_payload(&RelationalRow::new(key.0.clone()))?,
    )]))
}

struct GroupDelta {
    rows: i128,
    values: Vec<i128>,
    counts: Vec<i128>,
    extrema: Vec<RelationalValue>,
}

impl GroupDelta {
    fn estimated_bytes(definition: &MaterializedViewDefinition) -> usize {
        let extrema_bytes = if definition.has_extrema() {
            std::mem::size_of::<RelationalValue>()
        } else {
            0
        };
        definition
            .columns
            .len()
            .saturating_mul(2 * std::mem::size_of::<i128>() + extrema_bytes)
    }

    fn new(definition: &MaterializedViewDefinition) -> Self {
        let columns = definition.columns.len();
        Self {
            rows: 0,
            values: vec![0; columns],
            counts: vec![0; columns],
            extrema: if definition.has_extrema() {
                vec![RelationalValue::Null; columns]
            } else {
                Vec::new()
            },
        }
    }
}

fn add_group_delta(
    definition: &MaterializedViewDefinition,
    source: &RelationalTableSchema,
    row: &RelationalRow,
    sign: i128,
    deltas: &mut BTreeMap<RelationalKey, GroupDelta>,
    budget: &mut MaintenanceBudget<'_>,
) -> Result<(), RelationalError> {
    if !matches_filter(definition, source, row)? {
        return Ok(());
    }
    let group = RelationalKey(
        definition
            .group_by
            .iter()
            .map(|name| budget.copy_value(value(source, row, name)))
            .collect::<Result<_, _>>()?,
    );
    if !deltas.contains_key(&group) {
        budget.charge(0, GroupDelta::estimated_bytes(definition))?;
    }
    let delta = deltas
        .entry(group)
        .or_insert_with(|| GroupDelta::new(definition));
    delta.rows += sign;
    for (position, column) in definition.columns.iter().enumerate() {
        match &column.expression {
            MaterializedViewExpression::Min(input) | MaterializedViewExpression::Max(input) => {
                if sign > 0 {
                    merge_extremum(
                        &column.expression,
                        value(source, row, input),
                        &mut delta.extrema[position],
                        budget,
                    )?;
                }
            }
            MaterializedViewExpression::Column(_) => {}
            MaterializedViewExpression::Count(input) => {
                if input
                    .as_ref()
                    .is_none_or(|name| !matches!(value(source, row, name), RelationalValue::Null))
                {
                    delta.values[position] += sign;
                }
            }
            MaterializedViewExpression::Sum(input) => {
                if let RelationalValue::BigInt(number) = value(source, row, input) {
                    delta.values[position] += sign * i128::from(*number);
                    delta.counts[position] += sign;
                }
            }
        }
    }
    Ok(())
}

fn merge_extremum(
    expression: &MaterializedViewExpression,
    candidate: &RelationalValue,
    current: &mut RelationalValue,
    budget: &mut MaintenanceBudget<'_>,
) -> Result<(), RelationalError> {
    if !matches!(candidate, RelationalValue::Null)
        && (matches!(current, RelationalValue::Null)
            || if matches!(expression, MaterializedViewExpression::Min(_)) {
                candidate < current
            } else {
                candidate > current
            })
    {
        *current = budget.copy_value(candidate)?;
    }
    Ok(())
}

fn merge_unchanged_extrema(
    previous: &RelationalState,
    definition: &MaterializedViewDefinition,
    source: &RelationalTableSchema,
    group: &RelationalKey,
    changed: &BTreeSet<RelationalKey>,
    delta: &mut GroupDelta,
    budget: &mut MaintenanceBudget<'_>,
) -> Result<(), RelationalError> {
    let index = extrema_index(definition, source)?;
    let mut result = Ok(());
    // Indexes are refreshed after maintenance. Read the committed prefix and
    // exclude every changed identity; new images are already in the delta.
    let visited =
        previous.visit_index_prefix_entries(&source.name, &index.name, group, |_, key| {
            result = (|| {
                if changed.contains(key) {
                    return budget.charge(1, 0);
                }
                if let Some(row) = budget.read(previous, &source.name, key)?
                    && matches_filter(definition, source, &row)?
                {
                    for (position, column) in definition.columns.iter().enumerate() {
                        if let MaterializedViewExpression::Min(input)
                        | MaterializedViewExpression::Max(input) = &column.expression
                        {
                            merge_extremum(
                                &column.expression,
                                value(source, &row, input),
                                &mut delta.extrema[position],
                                budget,
                            )?;
                        }
                    }
                }
                Ok(())
            })();
            result.is_ok()
        });
    if visited.is_none() {
        return Err(RelationalError::Corruption(
            "materialized MIN/MAX source index is missing".into(),
        ));
    }
    result
}

fn number(value: Option<&RelationalValue>) -> i128 {
    match value {
        Some(RelationalValue::BigInt(number)) => i128::from(*number),
        _ => 0,
    }
}

fn bigint(number: i128) -> Result<RelationalValue, RelationalError> {
    i64::try_from(number)
        .map(RelationalValue::BigInt)
        .map_err(|_| RelationalError::Constraint("materialized aggregate BIGINT overflow".into()))
}

fn apply_group_delta(
    definition: &MaterializedViewDefinition,
    group: &RelationalKey,
    key: &RelationalKey,
    old: Option<&RelationalRow>,
    delta: GroupDelta,
    budget: &mut MaintenanceBudget<'_>,
) -> Result<Option<RelationalRow>, RelationalError> {
    let old_value = |position| old.and_then(|row| row.values().get(position));
    let size = definition.columns.len();
    let rows = number(old_value(size + 1)) + delta.rows;
    if rows < 0 {
        return Err(RelationalError::Corruption(
            "negative materialized group cardinality".into(),
        ));
    }
    if rows == 0 && !definition.group_by.is_empty() {
        return Ok(None);
    }
    let mut values = Vec::new();
    let mut counts = Vec::new();
    for (position, column) in definition.columns.iter().enumerate() {
        values.push(match &column.expression {
            MaterializedViewExpression::Column(input) => {
                let position = definition
                    .group_by
                    .iter()
                    .position(|name| name == input)
                    .expect("group column");
                budget.copy_value(&group.0[position])?
            }
            MaterializedViewExpression::Min(_) | MaterializedViewExpression::Max(_) => {
                budget.copy_value(&delta.extrema[position])?
            }
            MaterializedViewExpression::Count(_) => {
                bigint(number(old_value(position)) + delta.values[position])?
            }
            MaterializedViewExpression::Sum(_) => {
                let count = number(old_value(size + 2 + counts.len())) + delta.counts[position];
                if count < 0 || count > rows {
                    return Err(RelationalError::Corruption(
                        "invalid materialized SUM cardinality".into(),
                    ));
                }
                counts.push(bigint(count)?);
                if count == 0 {
                    RelationalValue::Null
                } else {
                    bigint(number(old_value(position)) + delta.values[position])?
                }
            }
        });
    }
    values.extend(key.0.iter().cloned());
    values.push(bigint(rows)?);
    values.extend(counts);
    Ok(Some(RelationalRow::new(values)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> MaintenanceBudget<'static> {
        MaintenanceBudget::new(RelationalMutationLimits::default(), None)
    }

    #[test]
    fn hydration_limits_follow_explicit_maintenance_work_limits() {
        let limits = RelationalMutationLimits {
            max_rows: std::num::NonZeroUsize::new(3).unwrap(),
            max_payload_bytes: std::num::NonZeroUsize::new(4096).unwrap(),
        };
        let budget = MaintenanceBudget::new(limits, None);
        assert_eq!(budget.hydration.max_rows, limits.max_rows.get());
        assert_eq!(
            budget.hydration.max_compressed_bytes,
            limits.max_payload_bytes.get()
        );
        assert_eq!(
            budget.hydration.max_decompressed_bytes,
            limits.max_payload_bytes.get()
        );
        assert_eq!(
            budget.hydration.max_memory_bytes,
            limits.max_payload_bytes.get()
        );
    }

    #[test]
    fn default_row_budget_admits_projection_backfill_and_still_bounds_total_work() {
        let mut budget = budget();
        // Model source reads and projection outputs without allocating a large
        // table merely to cross the former 100,000-unit admission threshold.
        budget.ensure_rows(120_000).unwrap();
        budget.charge(60_000, 0).unwrap();
        budget.charge(60_000, 0).unwrap();
        budget.ensure_rows(130_000).unwrap();
        budget.charge(130_000, 0).unwrap();
        assert!(matches!(
            budget.ensure_rows(1),
            Err(RelationalError::Admission(_))
        ));
        assert!(matches!(
            budget.charge(1, 0),
            Err(RelationalError::Admission(_))
        ));
    }

    #[test]
    fn default_payload_budget_admits_cumulative_copies_and_still_bounds_total_work() {
        let mut budget = budget();
        budget.charge(0, 24 * 1024 * 1024).unwrap();
        budget.charge(0, 24 * 1024 * 1024).unwrap();
        budget.charge(0, 16 * 1024 * 1024).unwrap();
        assert!(matches!(
            budget.charge(0, 1),
            Err(RelationalError::Admission(_))
        ));
    }
}
