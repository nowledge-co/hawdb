use crate::binding::value_payload_bytes;
use crate::columnar::ColumnarRowRef;
use skein_core::{Result, SkeinError, Value, ValueRef};
use skein_plan::{PhysicalOperatorId, PhysicalPlanKind};
use std::collections::BTreeMap;
use std::ops::Index;
use std::sync::{Arc, OnceLock};

pub type Row = BTreeMap<String, Value>;

/// Column names shared by every row of one query result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySchema {
    columns: Arc<[String]>,
}

impl QuerySchema {
    pub fn try_new(columns: impl IntoIterator<Item = String>) -> Result<Self> {
        let columns = columns.into_iter().collect::<Vec<_>>();
        let mut unique = std::collections::BTreeSet::new();
        if let Some(duplicate) = columns
            .iter()
            .find(|column| !unique.insert(column.as_str()))
        {
            return Err(SkeinError::Semantic(format!(
                "query result schema contains duplicate column {duplicate}"
            )));
        }
        Ok(Self {
            columns: Arc::from(columns),
        })
    }

    pub fn empty() -> Self {
        Self {
            columns: Arc::from([]),
        }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column == name)
    }
}

#[derive(Debug)]
struct QueryRowStorage {
    schema: QuerySchema,
    values: Vec<Value>,
    row_count: usize,
}

/// One owned handle into a query's immutable flat value buffer.
#[derive(Clone)]
pub struct QueryRow {
    storage: Arc<QueryRowStorage>,
    row: usize,
}

impl QueryRow {
    pub fn schema(&self) -> &QuerySchema {
        &self.storage.schema
    }

    pub fn len(&self) -> usize {
        self.storage.schema.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, column: usize) -> Option<&Value> {
        row_values(&self.storage, self.row)?.get(column)
    }

    pub fn get(&self, column: &str) -> Option<&Value> {
        self.value(self.storage.schema.position(column)?)
    }

    pub fn contains_key(&self, column: &str) -> bool {
        self.storage.schema.position(column).is_some()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &Value)> {
        self.storage
            .schema
            .columns()
            .iter()
            .map(String::as_str)
            .zip(row_values(&self.storage, self.row).unwrap_or_default())
    }

    pub fn keys(&self) -> impl ExactSizeIterator<Item = &str> {
        self.storage.schema.columns().iter().map(String::as_str)
    }

    pub fn values(&self) -> impl ExactSizeIterator<Item = &Value> {
        row_values(&self.storage, self.row)
            .unwrap_or_default()
            .iter()
    }

    pub fn as_ref(&self) -> QueryRowRef<'_> {
        QueryRowRef {
            schema: &self.storage.schema,
            values: row_values(&self.storage, self.row).unwrap_or_default(),
        }
    }

    pub fn to_owned_row(&self) -> Row {
        self.iter()
            .map(|(name, value)| (name.to_owned(), value.clone()))
            .collect()
    }
}

impl std::fmt::Debug for QueryRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl PartialEq for QueryRow {
    fn eq(&self, other: &Self) -> bool {
        self.schema() == other.schema()
            && row_values(&self.storage, self.row) == row_values(&other.storage, other.row)
    }
}

impl Eq for QueryRow {}

impl PartialEq<Row> for QueryRow {
    fn eq(&self, other: &Row) -> bool {
        row_equals_map(self.as_ref(), other)
    }
}

impl PartialEq<QueryRow> for Row {
    fn eq(&self, other: &QueryRow) -> bool {
        other == self
    }
}

impl Index<&str> for QueryRow {
    type Output = Value;

    fn index(&self, column: &str) -> &Self::Output {
        self.get(column)
            .unwrap_or_else(|| panic!("query result has no column {column}"))
    }
}

/// A borrowed row view over a query's immutable flat value buffer.
#[derive(Debug, Clone, Copy)]
pub struct QueryRowRef<'a> {
    schema: &'a QuerySchema,
    values: &'a [Value],
}

impl<'a> QueryRowRef<'a> {
    pub fn schema(self) -> &'a QuerySchema {
        self.schema
    }

    pub fn len(self) -> usize {
        self.values.len()
    }

    pub fn is_empty(self) -> bool {
        self.values.is_empty()
    }

    pub fn value(self, column: usize) -> Option<&'a Value> {
        self.values.get(column)
    }

    pub fn get(self, column: &str) -> Option<&'a Value> {
        self.value(self.schema.position(column)?)
    }

    pub fn contains_key(self, column: &str) -> bool {
        self.schema.position(column).is_some()
    }

    pub fn iter(self) -> impl ExactSizeIterator<Item = (&'a str, &'a Value)> {
        self.schema
            .columns()
            .iter()
            .map(String::as_str)
            .zip(self.values)
    }

    pub fn keys(self) -> impl ExactSizeIterator<Item = &'a str> {
        self.schema.columns().iter().map(String::as_str)
    }

    pub fn values(self) -> impl ExactSizeIterator<Item = &'a Value> {
        self.values.iter()
    }

    pub fn to_owned_row(self) -> Row {
        self.iter()
            .map(|(name, value)| (name.to_owned(), value.clone()))
            .collect()
    }
}

impl PartialEq<Row> for QueryRowRef<'_> {
    fn eq(&self, other: &Row) -> bool {
        row_equals_map(*self, other)
    }
}

impl PartialEq<QueryRowRef<'_>> for Row {
    fn eq(&self, other: &QueryRowRef<'_>) -> bool {
        other == self
    }
}

impl Index<&str> for QueryRowRef<'_> {
    type Output = Value;

    fn index(&self, column: &str) -> &Self::Output {
        self.get(column)
            .unwrap_or_else(|| panic!("query result has no column {column}"))
    }
}

/// Schema-bearing result rows backed by one flat, immutable value buffer.
pub struct QueryRows {
    storage: Arc<QueryRowStorage>,
    indexed_rows: OnceLock<Vec<QueryRow>>,
}

impl QueryRows {
    pub fn empty() -> Self {
        Self::from_flat_values_unchecked(QuerySchema::empty(), Vec::new(), 0)
    }

    fn from_flat_values_unchecked(
        schema: QuerySchema,
        values: Vec<Value>,
        row_count: usize,
    ) -> Self {
        Self {
            storage: Arc::new(QueryRowStorage {
                schema,
                values,
                row_count,
            }),
            indexed_rows: OnceLock::new(),
        }
    }

    pub fn schema(&self) -> &QuerySchema {
        &self.storage.schema
    }

    pub fn value_rows(&self) -> QueryValueRows<'_> {
        QueryValueRows {
            rows: self,
            next: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.storage.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.storage.row_count == 0
    }

    pub fn value(&self, row: usize, column: usize) -> Option<ValueRef<'_>> {
        row_values(&self.storage, row)?
            .get(column)
            .map(Value::as_ref)
    }

    pub fn get(&self, row: usize, column: &str) -> Option<ValueRef<'_>> {
        self.value(row, self.schema().position(column)?)
    }

    pub fn into_rows(self) -> Vec<Row> {
        (0..self.len())
            .map(|row| {
                self.row(row)
                    .expect("row ordinal is in range")
                    .to_owned_row()
            })
            .collect()
    }

    pub fn row(&self, row: usize) -> Option<QueryRowRef<'_>> {
        if row >= self.len() {
            return None;
        }
        Some(QueryRowRef {
            schema: &self.storage.schema,
            values: row_values(&self.storage, row)?,
        })
    }

    pub fn first(&self) -> Option<QueryRowRef<'_>> {
        self.row(0)
    }

    pub fn last(&self) -> Option<QueryRowRef<'_>> {
        self.len().checked_sub(1).and_then(|row| self.row(row))
    }

    pub fn iter(&self) -> QueryRowsIter<'_> {
        QueryRowsIter {
            rows: self,
            next: 0,
        }
    }

    pub fn retain(&mut self, mut keep: impl FnMut(QueryRowRef<'_>) -> bool) {
        let width = self.schema().len();
        let mut values = Vec::with_capacity(self.storage.values.len());
        let mut row_count = 0usize;
        for row in self.iter() {
            if keep(row) {
                values.extend_from_slice(row.values);
                row_count = row_count.saturating_add(1);
            }
        }
        let retained_row_count = values.len().checked_div(width).unwrap_or(row_count);
        self.storage = Arc::new(QueryRowStorage {
            schema: self.schema().clone(),
            values,
            row_count: retained_row_count,
        });
        self.indexed_rows = OnceLock::new();
    }

    /// Returns stable owned row handles for APIs that require slice pattern
    /// matching or indexing. No map or value is copied.
    pub fn as_slice(&self) -> &[QueryRow] {
        self.indexed_rows()
    }

    pub fn payload_bytes(&self) -> usize {
        let names = self
            .schema()
            .columns()
            .iter()
            .fold(0usize, |total, name| total.saturating_add(name.len()));
        self.storage.values.iter().fold(names, |total, value| {
            total.saturating_add(value_payload_bytes(value))
        })
    }

    fn indexed_rows(&self) -> &[QueryRow] {
        self.indexed_rows.get_or_init(|| {
            (0..self.len())
                .map(|row| QueryRow {
                    storage: Arc::clone(&self.storage),
                    row,
                })
                .collect()
        })
    }
}

impl Default for QueryRows {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<Row>> for QueryRows {
    fn from(rows: Vec<Row>) -> Self {
        let columns = rows
            .iter()
            .flat_map(|row| row.keys().cloned())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let schema = QuerySchema::try_new(columns).expect("BTreeSet columns are unique");
        let mut builder = QueryRowsBuilder::with_schema(schema.clone(), rows.len());
        for mut row in rows {
            builder
                .push_values(
                    schema
                        .columns()
                        .iter()
                        .map(|column| row.remove(column).unwrap_or(Value::Null)),
                )
                .expect("row normalization always matches the inferred schema");
        }
        builder.finish()
    }
}

impl FromIterator<Row> for QueryRows {
    fn from_iter<T: IntoIterator<Item = Row>>(iter: T) -> Self {
        iter.into_iter().collect::<Vec<_>>().into()
    }
}

/// Builds immutable query results without retaining one map or vector per row.
///
/// The first named row binds column names to stable ordinals. Later rows are
/// consumed in the same deterministic `BTreeMap` order and their values move
/// directly into one flat buffer.
pub struct QueryRowsBuilder {
    schema: Option<QuerySchema>,
    values: Vec<Value>,
    row_count: usize,
    row_capacity_hint: usize,
}

impl QueryRowsBuilder {
    pub fn new() -> Self {
        Self::with_row_capacity(0)
    }

    pub fn with_row_capacity(row_capacity: usize) -> Self {
        Self {
            schema: None,
            values: Vec::new(),
            row_count: 0,
            row_capacity_hint: row_capacity,
        }
    }

    pub fn with_schema(schema: QuerySchema, row_capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(row_capacity.saturating_mul(schema.len())),
            schema: Some(schema),
            row_count: 0,
            row_capacity_hint: row_capacity,
        }
    }

    pub fn push_values(&mut self, values: impl IntoIterator<Item = Value>) -> Result<()> {
        self.try_push_values(values.into_iter().map(Ok))
    }

    /// Appends one fallible row directly into the flat value buffer.
    ///
    /// A conversion error or width mismatch rolls the partial row back.
    pub fn try_push_values(
        &mut self,
        values: impl IntoIterator<Item = Result<Value>>,
    ) -> Result<()> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            SkeinError::Execution(
                "query result values require a schema before ordinal insertion".to_string(),
            )
        })?;
        let start = self.values.len();
        for value in values {
            match value {
                Ok(value) => self.values.push(value),
                Err(error) => {
                    self.values.truncate(start);
                    return Err(error);
                }
            }
        }
        let width = self.values.len().saturating_sub(start);
        if width != schema.len() {
            self.values.truncate(start);
            return Err(SkeinError::Execution(format!(
                "query result row {} has width {width}, expected {}",
                self.row_count,
                schema.len()
            )));
        }
        self.row_count = self.row_count.saturating_add(1);
        Ok(())
    }

    pub fn push_named_row(&mut self, row: Row) -> Result<()> {
        if self.schema.is_none() {
            self.schema = Some(QuerySchema::try_new(row.keys().cloned())?);
            self.values.reserve(
                row.len()
                    .saturating_mul(self.row_capacity_hint.max(self.row_count.saturating_add(1))),
            );
        }
        let schema = self.schema.as_ref().expect("query result schema is bound");
        if row.len() != schema.len() {
            return Err(SkeinError::Execution(format!(
                "query result row {} has width {}, expected {}",
                self.row_count,
                row.len(),
                schema.len()
            )));
        }
        if let Some((ordinal, (name, expected))) = row
            .keys()
            .zip(schema.columns())
            .enumerate()
            .find(|(_, (name, expected))| *name != *expected)
        {
            return Err(SkeinError::Execution(format!(
                "query result row {} column {ordinal} is {name}, expected {expected}",
                self.row_count
            )));
        }
        self.values.extend(row.into_values());
        self.row_count = self.row_count.saturating_add(1);
        Ok(())
    }

    pub fn finish(self) -> QueryRows {
        QueryRows::from_flat_values_unchecked(
            self.schema.unwrap_or_else(QuerySchema::empty),
            self.values,
            self.row_count,
        )
    }
}

impl Default for QueryRowsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for QueryRows {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            indexed_rows: OnceLock::new(),
        }
    }
}

impl std::fmt::Debug for QueryRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryRows")
            .field("schema", self.schema())
            .field("values", &self.storage.values)
            .finish()
    }
}

impl PartialEq for QueryRows {
    fn eq(&self, other: &Self) -> bool {
        self.schema() == other.schema()
            && self.len() == other.len()
            && self.storage.values == other.storage.values
    }
}

impl Eq for QueryRows {}

impl PartialEq<Vec<Row>> for QueryRows {
    fn eq(&self, other: &Vec<Row>) -> bool {
        self.len() == other.len() && self.iter().zip(other).all(|(left, right)| left == *right)
    }
}

impl PartialEq<QueryRows> for Vec<Row> {
    fn eq(&self, other: &QueryRows) -> bool {
        other == self
    }
}

impl Index<usize> for QueryRows {
    type Output = QueryRow;

    fn index(&self, row: usize) -> &Self::Output {
        &self.indexed_rows()[row]
    }
}

impl<'a> IntoIterator for &'a QueryRows {
    type Item = QueryRowRef<'a>;
    type IntoIter = QueryRowsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for QueryRows {
    type Item = QueryRow;
    type IntoIter = QueryRowsIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        QueryRowsIntoIter {
            storage: self.storage,
            next: 0,
        }
    }
}

pub struct QueryRowsIter<'a> {
    rows: &'a QueryRows,
    next: usize,
}

impl<'a> Iterator for QueryRowsIter<'a> {
    type Item = QueryRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.rows.row(self.next)?;
        self.next = self.next.saturating_add(1);
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len().saturating_sub(self.next);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for QueryRowsIter<'_> {}

pub struct QueryValueRows<'a> {
    rows: &'a QueryRows,
    next: usize,
}

impl QueryValueRows<'_> {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'a> Iterator for QueryValueRows<'a> {
    type Item = &'a [Value];

    fn next(&mut self) -> Option<Self::Item> {
        let values = self.rows.row(self.next)?.values;
        self.next = self.next.saturating_add(1);
        Some(values)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.rows.len().saturating_sub(self.next);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for QueryValueRows<'_> {}

pub struct QueryRowsIntoIter {
    storage: Arc<QueryRowStorage>,
    next: usize,
}

impl Iterator for QueryRowsIntoIter {
    type Item = QueryRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.storage.row_count {
            return None;
        }
        let row = QueryRow {
            storage: Arc::clone(&self.storage),
            row: self.next,
        };
        self.next = self.next.saturating_add(1);
        Some(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.storage.row_count.saturating_sub(self.next);
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for QueryRowsIntoIter {}

fn row_values(storage: &QueryRowStorage, row: usize) -> Option<&[Value]> {
    if row >= storage.row_count {
        return None;
    }
    let width = storage.schema.len();
    let start = row.checked_mul(width)?;
    let end = start.checked_add(width)?;
    storage.values.get(start..end)
}

fn row_equals_map(row: QueryRowRef<'_>, other: &Row) -> bool {
    row.len() == other.len()
        && row
            .iter()
            .all(|(name, value)| other.get(name).is_some_and(|other| other == value))
}

/// A row view whose values remain valid only for the current consumer call.
///
/// The map representation covers the scalar fallback executor. The columnar
/// representation lets a downstream consumer pull selected values directly
/// from an immutable batch without constructing an intermediate row map.
#[derive(Debug, Clone, Copy)]
pub enum RowRef<'a> {
    Map(&'a Row),
    Columnar(ColumnarRowRef<'a>),
}

impl<'a> RowRef<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Map(row) => row.len(),
            Self::Columnar(_) => self.iter().count(),
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub fn get(self, name: &str) -> Option<ValueRef<'a>> {
        match self {
            Self::Map(row) => row.get(name).map(Value::as_ref),
            Self::Columnar(row) => row.get(name),
        }
    }

    pub fn column(self, index: usize) -> Option<(&'a str, ValueRef<'a>)> {
        match self {
            Self::Map(row) => row
                .iter()
                .nth(index)
                .map(|(name, value)| (name.as_str(), value.as_ref())),
            Self::Columnar(row) => row.column(index),
        }
    }

    pub fn iter(self) -> RowRefIter<'a> {
        match self {
            Self::Map(row) => RowRefIter::Map(row.iter()),
            Self::Columnar(row) => RowRefIter::Columnar { row, index: 0 },
        }
    }

    pub fn to_owned_row(self) -> Row {
        self.iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned_value()))
            .collect()
    }
}

impl<'a> From<&'a Row> for RowRef<'a> {
    fn from(row: &'a Row) -> Self {
        Self::Map(row)
    }
}

impl<'a> From<ColumnarRowRef<'a>> for RowRef<'a> {
    fn from(row: ColumnarRowRef<'a>) -> Self {
        Self::Columnar(row)
    }
}

pub enum RowRefIter<'a> {
    Map(std::collections::btree_map::Iter<'a, String, Value>),
    Columnar {
        row: ColumnarRowRef<'a>,
        index: usize,
    },
}

impl<'a> Iterator for RowRefIter<'a> {
    type Item = (&'a str, ValueRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Map(iter) => iter
                .next()
                .map(|(name, value)| (name.as_str(), value.as_ref())),
            Self::Columnar { row, index } => loop {
                let current = *index;
                *index = index.saturating_add(1);
                if current >= row.schema().len() {
                    return None;
                }
                if let Some(column) = row.column(current) {
                    return Some(column);
                }
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingOperatorMemoryReport {
    pub operator: String,
    pub budget_bytes: usize,
    pub peak_tracked_bytes: usize,
    pub input_rows: usize,
    pub max_spill_bytes: u64,
    pub max_spill_runs: usize,
    pub spilled_bytes: u64,
    pub spill_run_count: usize,
    pub spilled_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PipelineMemoryReport {
    /// Query-owned runtime ledger budget across pipeline and blocking state.
    pub query_memory_budget_bytes: usize,
    /// Highest aggregate tracked resident bytes charged to the query ledger.
    pub query_memory_peak_bytes: usize,
    /// Tracked bytes still owned at the execution completion boundary.
    pub query_memory_completion_bytes: usize,
    /// Number of operator or transfer accounts created by the query.
    pub query_memory_account_count: usize,
    /// Sum of rows emitted at every physical operator boundary.
    pub intermediate_rows: usize,
    /// Sum of retained payload estimates emitted at every physical operator boundary.
    pub intermediate_payload_bytes: usize,
    pub peak_batch_rows: usize,
    pub peak_batch_payload_bytes: usize,
    /// Number of typed columnar batches evaluated by eligible pipeline fragments.
    pub columnar_batches: usize,
    /// Rows loaded into typed columns before selection.
    pub columnar_input_rows: usize,
    /// Rows retained by columnar selections.
    pub columnar_selected_rows: usize,
    /// Morsels consumed by columnar fragments.
    pub morsel_count: usize,
    /// Highest resource-admitted worker count across morsel pipelines.
    pub morsel_max_admitted_workers: usize,
    /// Highest worker count actually used by a morsel scheduler.
    pub morsel_peak_active_workers: usize,
    /// Highest number of completed morsel outputs awaiting or crossing the
    /// coordinator's ordered consumer boundary.
    pub morsel_peak_buffered_outputs: usize,
    /// Highest estimated resident bytes held by those completed outputs.
    pub morsel_peak_buffered_output_bytes: usize,
    /// Highest number of out-of-order outputs held by the ordinal merger.
    pub morsel_peak_reorder_entries: usize,
    pub output_rows: usize,
    pub output_payload_bytes: usize,
    /// Resident memory before execution, when process sampling is supported.
    pub start_resident_bytes: Option<u64>,
    /// Process high-water resident memory before execution.
    pub start_peak_resident_bytes: Option<u64>,
    /// Resident memory after the output rows have been materialized.
    pub steady_resident_bytes: Option<u64>,
    /// Process high-water resident memory at completion.
    pub peak_resident_bytes: Option<u64>,
    /// Positive resident-memory delta retained at completion.
    pub steady_resident_growth_bytes: Option<u64>,
    /// Positive process high-water delta observed during execution.
    pub lifetime_peak_resident_growth_bytes: Option<u64>,
    /// Aggregate page-fault delta when the platform exposes it.
    pub total_page_faults: Option<u64>,
    /// Split page-fault deltas only on platforms that expose this distinction.
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

/// The observed output cardinality for one physical operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorCardinalityProfile {
    pub operator_id: PhysicalOperatorId,
    pub operator: PhysicalPlanKind,
    /// `None` means the operator was not invoked, while `Some(0)` means it ran
    /// and produced no output rows.
    pub actual_rows: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadExecutionProfile<TScanPruningReport> {
    pub max_rows: Option<usize>,
    pub detection_row_cap: Option<usize>,
    pub row_limit_enforced_before_output: bool,
    pub operator_row_cap_enabled: bool,
    pub operator_cardinality_profiles: Vec<OperatorCardinalityProfile>,
    pub blocking_operator_kinds: Vec<String>,
    pub scan_pruning_reports: Vec<TScanPruningReport>,
    pub vector_execution_reports: Vec<crate::VectorExecutionReport>,
    pub graph_expansion_reports: Vec<crate::GraphExpansionExecutionReport>,
    pub blocking_operator_memory_reports: Vec<BlockingOperatorMemoryReport>,
    pub pipeline_memory_report: PipelineMemoryReport,
}

impl<TScanPruningReport> ReadExecutionProfile<TScanPruningReport> {
    pub fn blocking_operator_count(&self) -> usize {
        self.blocking_operator_kinds.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfiledQueryRows<TScanPruningReport> {
    pub rows: QueryRows,
    pub profile: ReadExecutionProfile<TScanPruningReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProfiledQueryStream<TScanPruningReport> {
    /// True when the physical plan emitted batches directly to the consumer.
    /// False means an unsupported operator still materialized bindings before
    /// the bounded consumer boundary.
    pub fully_streamed: bool,
    pub profile: ReadExecutionProfile<TScanPruningReport>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BindingSchema, ColumnVector, ColumnarBatch, SlotDescriptor, SlotId, SlotType, Validity,
    };
    use skein_core::LogicalType;
    use std::sync::Arc;

    #[test]
    fn counts_blocking_operator_kinds() {
        let profile = ReadExecutionProfile::<()> {
            max_rows: Some(10),
            detection_row_cap: Some(11),
            row_limit_enforced_before_output: true,
            operator_row_cap_enabled: true,
            operator_cardinality_profiles: Vec::new(),
            blocking_operator_kinds: vec!["sort".to_string(), "aggregate".to_string()],
            scan_pruning_reports: Vec::new(),
            vector_execution_reports: Vec::new(),
            graph_expansion_reports: Vec::new(),
            blocking_operator_memory_reports: Vec::new(),
            pipeline_memory_report: PipelineMemoryReport::default(),
        };
        assert_eq!(profile.blocking_operator_count(), 2);
    }

    #[test]
    fn borrowed_map_row_materializes_only_on_request() {
        let row = Row::from([
            ("id".to_string(), Value::Int(7)),
            ("content".to_string(), Value::String("payload".into())),
        ]);
        let row_ref = RowRef::from(&row);

        assert_eq!(row_ref.get("id"), Some(ValueRef::Int(7)));
        assert_eq!(row_ref.get("content").unwrap().as_str(), Some("payload"));
        assert_eq!(row_ref.to_owned_row(), row);
    }

    #[test]
    fn schema_bearing_rows_keep_names_once_in_a_flat_value_buffer() {
        let schema = QuerySchema::try_new(["payload".to_string(), "id".to_string()]).unwrap();
        let mut builder = QueryRowsBuilder::with_schema(schema, 2);
        builder
            .push_values([Value::String("one".to_string()), Value::Int(1)])
            .unwrap();
        builder
            .push_values([Value::String("two".to_string()), Value::Int(2)])
            .unwrap();
        let rows = builder.finish();

        assert_eq!(rows.schema().columns(), ["payload", "id"]);
        assert_eq!(rows.get(1, "payload").unwrap().as_str(), Some("two"));
        assert_eq!(rows.value(0, 1), Some(ValueRef::Int(1)));
        assert_eq!(rows.payload_bytes(), "payload".len() + "id".len() + 6 + 16);
        assert_eq!(rows.len(), 2);
        assert!(!rows.is_empty());
        assert_eq!(rows.storage.values.len(), 4);
        assert_eq!(rows[0]["payload"], Value::String("one".to_string()));
        assert_eq!(rows.iter().count(), 2);
        assert_eq!(rows.storage.values.len(), 4);
    }

    #[test]
    fn schema_bearing_rows_reject_duplicate_columns_and_width_mismatch() {
        assert!(QuerySchema::try_new(["id".to_string(), "id".to_string()]).is_err());
        let schema = QuerySchema::try_new(["id".to_string()]).unwrap();
        let mut invalid = QueryRowsBuilder::with_schema(schema, 1);
        assert!(invalid.push_values([]).is_err());

        let mut left = QueryRowsBuilder::with_schema(
            QuerySchema::try_new(["left".to_string(), "right".to_string()]).unwrap(),
            1,
        );
        left.push_values([Value::Int(1), Value::Int(2)]).unwrap();
        let left = left.finish();
        let mut right = QueryRowsBuilder::with_schema(
            QuerySchema::try_new(["right".to_string(), "left".to_string()]).unwrap(),
            1,
        );
        right.push_values([Value::Int(1), Value::Int(2)]).unwrap();
        let right = right.finish();
        assert_ne!(left, right);
    }

    #[test]
    fn fallible_flat_row_append_rolls_back_partial_values() {
        let schema = QuerySchema::try_new(["left".to_string(), "right".to_string()]).unwrap();
        let mut builder = QueryRowsBuilder::with_schema(schema, 1);
        assert!(builder
            .try_push_values([
                Ok(Value::Int(1)),
                Err(SkeinError::Execution("projection failed".to_string())),
            ])
            .is_err());
        builder.push_values([Value::Int(2), Value::Int(3)]).unwrap();
        let rows = builder.finish();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows.value(0, 0), Some(ValueRef::Int(2)));
        assert_eq!(rows.value(0, 1), Some(ValueRef::Int(3)));
        assert_eq!(rows.storage.values.len(), 2);
    }

    #[test]
    fn named_row_builder_binds_schema_once_and_moves_values_by_ordinal() {
        let mut builder = QueryRowsBuilder::with_row_capacity(2);
        builder
            .push_named_row(Row::from([
                ("id".to_string(), Value::Int(1)),
                ("payload".to_string(), Value::String("one".to_string())),
            ]))
            .unwrap();
        builder
            .push_named_row(Row::from([
                ("id".to_string(), Value::Int(2)),
                ("payload".to_string(), Value::String("two".to_string())),
            ]))
            .unwrap();
        let rows = builder.finish();

        assert_eq!(rows.schema().columns(), ["id", "payload"]);
        assert_eq!(rows.value(1, 0), Some(ValueRef::Int(2)));
        assert_eq!(rows.get(0, "payload").unwrap().as_str(), Some("one"));
        assert_eq!(rows.storage.values.len(), 4);
    }

    #[test]
    fn named_row_builder_rejects_schema_drift_without_partial_append() {
        let mut builder = QueryRowsBuilder::new();
        builder
            .push_named_row(Row::from([("id".to_string(), Value::Int(1))]))
            .unwrap();
        assert!(builder
            .push_named_row(Row::from([("name".to_string(), Value::Int(2))]))
            .is_err());
        let rows = builder.finish();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows.get(0, "id"), Some(ValueRef::Int(1)));
    }

    #[test]
    fn borrowed_columnar_row_pulls_values_without_a_row_map() {
        let schema = Arc::new(
            BindingSchema::try_new(vec![
                SlotDescriptor {
                    id: SlotId(0),
                    name: "id".into(),
                    slot_type: SlotType::NodeId,
                },
                SlotDescriptor {
                    id: SlotId(1),
                    name: "content".into(),
                    slot_type: SlotType::logical(LogicalType::Text),
                },
            ])
            .unwrap(),
        );
        let batch = ColumnarBatch::try_new(
            schema,
            vec![
                Arc::new(ColumnVector::node_ids(vec![7])),
                Arc::new(ColumnVector::Utf8 {
                    values: vec!["payload".to_string()].into(),
                    validity: Validity::all(1),
                }),
            ],
        )
        .unwrap();
        let row_ref = RowRef::from(batch.rows().next().unwrap());

        assert_eq!(row_ref.get("id"), Some(ValueRef::Int(7)));
        assert_eq!(row_ref.get("content").unwrap().as_str(), Some("payload"));
        assert_eq!(
            row_ref.to_owned_row(),
            Row::from([
                ("content".into(), Value::String("payload".into())),
                ("id".into(), Value::Int(7)),
            ])
        );
    }
}
