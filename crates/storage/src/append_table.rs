use self::binary::compare_rows;
use crate::{
    RelationalColumnDefault, RelationalColumnSchema, RelationalKey, RelationalRow,
    RelationalScalarType, RelationalValue,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

mod binary;
mod codec;
mod publication;
mod segment;

pub use codec::{
    decode_append_wal_batch, encode_append_wal_batch, AppendDecodeLimits, AppendWalBatch,
};
pub use publication::{
    append_generation_manifest_file, append_segment_file, AppendGenerationArtifacts,
    AppendGenerationManifest, AppendGenerationReader, AppendPublicationConfig,
    AppendPublicationPhase, AppendPublicationReport, AppendPublicationState, AppendPublisher,
    AppendSegmentBinding,
};
pub use segment::{
    AppendSegmentArtifactMetadata, AppendSegmentBlockDescriptor, AppendSegmentConfig,
    AppendSegmentReadOutput, AppendSegmentReadReport, AppendSegmentReader,
    AppendSegmentWriteOutput, AppendSegmentWriter,
};

pub const DEFAULT_MAX_APPEND_MUTATION_ROWS: usize = 100_000;
pub const DEFAULT_MAX_APPEND_MUTATION_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendMutationLimits {
    pub max_rows: NonZeroUsize,
    pub max_payload_bytes: NonZeroUsize,
}

impl Default for AppendMutationLimits {
    fn default() -> Self {
        Self {
            max_rows: NonZeroUsize::new(DEFAULT_MAX_APPEND_MUTATION_ROWS)
                .expect("default append mutation row limit is non-zero"),
            max_payload_bytes: NonZeroUsize::new(DEFAULT_MAX_APPEND_MUTATION_BYTES)
                .expect("default append mutation byte limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendTableError {
    Admission(String),
    Schema(String),
    Constraint(String),
    SequenceExhausted {
        table: String,
        watermark: i64,
        requested: usize,
    },
    Durability(String),
    Corruption(String),
}

impl fmt::Display for AppendTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => write!(formatter, "append admission failed: {message}"),
            Self::Schema(message) => write!(formatter, "append schema error: {message}"),
            Self::Constraint(message) => {
                write!(formatter, "append constraint violation: {message}")
            }
            Self::SequenceExhausted {
                table,
                watermark,
                requested,
            } => write!(
                formatter,
                "append generated order sequence exhausted for table {table}: watermark={watermark}, requested={requested}"
            ),
            Self::Durability(message) => {
                write!(formatter, "append durability error: {message}")
            }
            Self::Corruption(message) => write!(formatter, "append corruption: {message}"),
        }
    }
}

impl std::error::Error for AppendTableError {}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AppendOrderMode {
    #[default]
    CallerProvided,
    CommitSequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendTableSchema {
    pub name: String,
    pub columns: Vec<RelationalColumnSchema>,
    pub partition_key: Vec<String>,
    pub order_key: Vec<String>,
    pub order_mode: AppendOrderMode,
}

impl AppendTableSchema {
    pub fn column_position(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    fn key_positions(&self) -> Result<AppendKeyPositions, AppendTableError> {
        let partition = self
            .partition_key
            .iter()
            .map(|column| {
                self.column_position(column).ok_or_else(|| {
                    AppendTableError::Schema(format!(
                        "partition key references unknown column {column}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        let order = self
            .order_key
            .iter()
            .map(|column| {
                self.column_position(column).ok_or_else(|| {
                    AppendTableError::Schema(format!(
                        "order key references unknown column {column}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(AppendKeyPositions { partition, order })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendTableRow {
    pub table: String,
    pub partition_key: RelationalKey,
    pub order_key: RelationalKey,
    pub row: RelationalRow,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppendStorageResidencyReport {
    pub canonical_segment_count: usize,
    pub canonical_segment_bytes: u64,
    pub resident_segment_payload_bytes: usize,
    pub resident_descriptor_count: usize,
    pub live_rows: usize,
    pub live_payload_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendGeneratedRow {
    /// Values in schema order with the generated order-key column omitted.
    pub values: Vec<RelationalValue>,
}

impl AppendGeneratedRow {
    pub fn new(values: Vec<RelationalValue>) -> Self {
        Self { values }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendWrite {
    CreateTable {
        schema: AppendTableSchema,
    },
    Append {
        table: String,
        rows: Vec<RelationalRow>,
    },
    AppendGenerated {
        table: String,
        rows: Vec<AppendGeneratedRow>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppendTransaction {
    pub writes: Vec<AppendWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendMutationOutcome {
    pub table: String,
    pub appended_rows: usize,
    /// Generated keys in caller input order. Caller-provided writes leave this empty.
    pub generated_order_keys: Vec<RelationalKey>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendCommitPreparation {
    pub state: AppendState,
    pub materialized_transaction: AppendTransaction,
    pub mutation_outcomes: Vec<AppendMutationOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendLiveBatch {
    rows: Arc<[AppendTableRow]>,
    watermarks: Arc<BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>>,
    previous: Option<Arc<Self>>,
    payload_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AppendLiveReadReport {
    pub batches_examined: usize,
    pub batches_pruned: usize,
    pub rows_examined: usize,
    pub rows_returned: usize,
}

impl AppendLiveBatch {
    pub fn rows(&self) -> &[AppendTableRow] {
        &self.rows
    }

    pub fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppendState {
    schemas: Arc<BTreeMap<String, AppendTableSchema>>,
    base_watermarks: Arc<BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>>,
    generated_order_watermarks: Arc<BTreeMap<String, i64>>,
    live_head: Option<Arc<AppendLiveBatch>>,
    live_rows: usize,
    live_payload_bytes: usize,
}

impl AppendState {
    pub fn from_checkpoint(
        schemas: BTreeMap<String, AppendTableSchema>,
        base_watermarks: BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>,
    ) -> Result<Self, AppendTableError> {
        let generated_order_watermarks =
            derive_generated_order_watermarks(&schemas, &base_watermarks)?;
        Self::from_checkpoint_with_generated_order_watermarks(
            schemas,
            base_watermarks,
            generated_order_watermarks,
        )
    }

    pub fn from_checkpoint_with_generated_order_watermarks(
        schemas: BTreeMap<String, AppendTableSchema>,
        base_watermarks: BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>,
        generated_order_watermarks: BTreeMap<String, i64>,
    ) -> Result<Self, AppendTableError> {
        for (name, schema) in &schemas {
            if name != &schema.name {
                return Err(AppendTableError::Corruption(format!(
                    "append checkpoint schema map key {name} differs from schema name {}",
                    schema.name
                )));
            }
            validate_table_schema(schema)?;
        }
        for (table, partitions) in &base_watermarks {
            let schema = schemas.get(table).ok_or_else(|| {
                AppendTableError::Corruption(format!(
                    "append checkpoint watermark references unknown table {table}"
                ))
            })?;
            let positions = schema.key_positions()?;
            for (partition, order) in partitions {
                validate_key_shape(schema, &positions.partition, partition, "partition")?;
                validate_key_shape(schema, &positions.order, order, "order")?;
            }
        }
        validate_generated_order_watermarks(
            &schemas,
            &base_watermarks,
            &generated_order_watermarks,
        )?;
        Ok(Self {
            schemas: Arc::new(schemas),
            base_watermarks: Arc::new(base_watermarks),
            generated_order_watermarks: Arc::new(generated_order_watermarks),
            live_head: None,
            live_rows: 0,
            live_payload_bytes: 0,
        })
    }

    pub fn schema(&self, table: &str) -> Option<&AppendTableSchema> {
        self.schemas.get(table)
    }

    pub fn schemas(&self) -> &BTreeMap<String, AppendTableSchema> {
        &self.schemas
    }

    pub fn generated_order_watermarks(&self) -> &BTreeMap<String, i64> {
        &self.generated_order_watermarks
    }

    pub fn generated_order_watermark(&self, table: &str) -> Option<i64> {
        self.generated_order_watermarks.get(table).copied()
    }

    pub fn live_head(&self) -> Option<&Arc<AppendLiveBatch>> {
        self.live_head.as_ref()
    }

    pub fn live_rows(&self) -> usize {
        self.live_rows
    }

    pub fn live_payload_bytes(&self) -> usize {
        self.live_payload_bytes
    }

    pub fn checkpoint_rows(
        &self,
        max_rows: usize,
    ) -> Result<Vec<AppendTableRow>, AppendTableError> {
        if self.live_rows > max_rows {
            return Err(AppendTableError::Admission(format!(
                "append checkpoint contains {} live rows, exceeding limit {max_rows}",
                self.live_rows
            )));
        }
        let mut rows = Vec::with_capacity(self.live_rows);
        let mut batch = self.live_head.as_deref();
        while let Some(current) = batch {
            rows.extend(current.rows.iter().cloned());
            batch = current.previous.as_deref();
        }
        rows.sort_unstable_by(compare_rows);
        if rows.len() != self.live_rows
            || !rows.windows(2).all(|pair| {
                pair[0].table != pair[1].table
                    || pair[0].partition_key != pair[1].partition_key
                    || pair[0].order_key < pair[1].order_key
            })
        {
            return Err(AppendTableError::Corruption(
                "append live batches contain inconsistent counts or duplicate order keys"
                    .to_string(),
            ));
        }
        Ok(rows)
    }

    pub fn watermark(&self, table: &str, partition: &RelationalKey) -> Option<&RelationalKey> {
        let mut batch = self.live_head.as_deref();
        while let Some(current) = batch {
            if let Some(watermark) = current
                .watermarks
                .get(table)
                .and_then(|partitions| partitions.get(partition))
            {
                return Some(watermark);
            }
            batch = current.previous.as_deref();
        }
        self.base_watermarks
            .get(table)
            .and_then(|partitions| partitions.get(partition))
    }

    pub fn stage_transaction(
        &self,
        transaction: &AppendTransaction,
        limits: AppendMutationLimits,
    ) -> Result<Self, AppendTableError> {
        self.stage_transaction_internal(transaction, limits, AppendStageMode::User)
    }

    pub fn stage_provisional_transaction(
        &self,
        transaction: &AppendTransaction,
        limits: AppendMutationLimits,
    ) -> Result<Self, AppendTableError> {
        self.stage_transaction_internal(transaction, limits, AppendStageMode::Provisional)
    }

    pub fn apply_recovered_transaction(
        &self,
        transaction: &AppendTransaction,
        limits: AppendMutationLimits,
    ) -> Result<Self, AppendTableError> {
        self.stage_transaction_internal(transaction, limits, AppendStageMode::Materialized)
    }

    pub fn prepare_transaction(
        &self,
        transaction: &AppendTransaction,
        limits: AppendMutationLimits,
    ) -> Result<AppendCommitPreparation, AppendTableError> {
        admit_transaction(transaction, limits)?;

        let mut schemas = self.schemas.as_ref().clone();
        let mut generated_order_watermarks = self.generated_order_watermarks.as_ref().clone();
        let mut writes = Vec::with_capacity(transaction.writes.len());
        let mut mutation_outcomes = Vec::new();

        for write in &transaction.writes {
            match write {
                AppendWrite::CreateTable { schema } => {
                    validate_table_schema(schema)?;
                    if schemas.contains_key(&schema.name) {
                        return Err(AppendTableError::Schema(format!(
                            "table {} already exists",
                            schema.name
                        )));
                    }
                    if schema.order_mode == AppendOrderMode::CommitSequence {
                        generated_order_watermarks.insert(schema.name.clone(), 0);
                    }
                    schemas.insert(schema.name.clone(), schema.clone());
                    writes.push(write.clone());
                }
                AppendWrite::Append { table, rows } => {
                    let schema = schemas.get(table).ok_or_else(|| {
                        AppendTableError::Schema(format!("unknown table {table}"))
                    })?;
                    if schema.order_mode != AppendOrderMode::CallerProvided {
                        return Err(AppendTableError::Constraint(format!(
                            "table {table} requires generated commit-sequence order keys"
                        )));
                    }
                    for row in rows {
                        validate_row(schema, row)?;
                    }
                    writes.push(write.clone());
                    mutation_outcomes.push(AppendMutationOutcome {
                        table: table.clone(),
                        appended_rows: rows.len(),
                        generated_order_keys: Vec::new(),
                    });
                }
                AppendWrite::AppendGenerated { table, rows } => {
                    let schema = schemas.get(table).ok_or_else(|| {
                        AppendTableError::Schema(format!("unknown table {table}"))
                    })?;
                    if schema.order_mode != AppendOrderMode::CommitSequence {
                        return Err(AppendTableError::Constraint(format!(
                            "table {table} requires caller-provided order keys"
                        )));
                    }
                    let order_position = generated_order_position(schema)?;
                    for row in rows {
                        validate_generated_row(schema, order_position, row)?;
                    }
                    let watermark =
                        generated_order_watermarks
                            .get(table)
                            .copied()
                            .ok_or_else(|| {
                                AppendTableError::Corruption(format!(
                                    "generated-order table {table} has no durable watermark"
                                ))
                            })?;
                    let requested = rows.len();
                    let requested_i64 = i64::try_from(requested).map_err(|_| {
                        AppendTableError::SequenceExhausted {
                            table: table.clone(),
                            watermark,
                            requested,
                        }
                    })?;
                    let end = watermark.checked_add(requested_i64).ok_or_else(|| {
                        AppendTableError::SequenceExhausted {
                            table: table.clone(),
                            watermark,
                            requested,
                        }
                    })?;
                    let mut materialized_rows = Vec::with_capacity(rows.len());
                    let mut generated_order_keys = Vec::with_capacity(rows.len());
                    for (offset, row) in rows.iter().enumerate() {
                        let value = watermark
                            + i64::try_from(offset + 1)
                                .expect("generated row offset was bounded by i64");
                        materialized_rows.push(materialize_generated_row(
                            order_position,
                            row,
                            value,
                        ));
                        generated_order_keys
                            .push(RelationalKey(vec![RelationalValue::BigInt(value)]));
                    }
                    generated_order_watermarks.insert(table.clone(), end);
                    writes.push(AppendWrite::Append {
                        table: table.clone(),
                        rows: materialized_rows,
                    });
                    mutation_outcomes.push(AppendMutationOutcome {
                        table: table.clone(),
                        appended_rows: rows.len(),
                        generated_order_keys,
                    });
                }
            }
        }

        let materialized_transaction = AppendTransaction { writes };
        let state = self.apply_recovered_transaction(&materialized_transaction, limits)?;
        debug_assert_eq!(
            state.generated_order_watermarks.as_ref(),
            &generated_order_watermarks
        );
        Ok(AppendCommitPreparation {
            state,
            materialized_transaction,
            mutation_outcomes,
        })
    }

    fn stage_transaction_internal(
        &self,
        transaction: &AppendTransaction,
        limits: AppendMutationLimits,
        mode: AppendStageMode,
    ) -> Result<Self, AppendTableError> {
        admit_transaction(transaction, limits)?;

        let mut schemas = None;
        let mut generated_order_watermarks = None;
        let mut staged_rows = Vec::new();
        let mut staged_watermarks: BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>> =
            BTreeMap::new();
        let mut payload_bytes = 0usize;

        for write in &transaction.writes {
            match write {
                AppendWrite::CreateTable { schema } => {
                    validate_table_schema(schema)?;
                    let mutable_schemas =
                        schemas.get_or_insert_with(|| self.schemas.as_ref().clone());
                    if mutable_schemas.contains_key(&schema.name) {
                        return Err(AppendTableError::Schema(format!(
                            "table {} already exists",
                            schema.name
                        )));
                    }
                    mutable_schemas.insert(schema.name.clone(), schema.clone());
                    if schema.order_mode == AppendOrderMode::CommitSequence {
                        generated_order_watermarks
                            .get_or_insert_with(|| self.generated_order_watermarks.as_ref().clone())
                            .insert(schema.name.clone(), 0);
                    }
                }
                AppendWrite::Append { table, rows } => {
                    let schema = schemas
                        .as_ref()
                        .unwrap_or(self.schemas.as_ref())
                        .get(table)
                        .ok_or_else(|| {
                            AppendTableError::Schema(format!("unknown table {table}"))
                        })?;
                    match (schema.order_mode, mode) {
                        (AppendOrderMode::CallerProvided, _)
                        | (AppendOrderMode::CommitSequence, AppendStageMode::Materialized) => {}
                        (AppendOrderMode::CommitSequence, _) => {
                            return Err(AppendTableError::Constraint(format!(
                                "table {table} requires generated commit-sequence order keys"
                            )));
                        }
                    }
                    let positions = schema.key_positions()?;
                    for row in rows {
                        validate_row(schema, row)?;
                        let partition_key = key_from_row(row, &positions.partition)?;
                        let order_key = key_from_row(row, &positions.order)?;
                        if schema.order_mode == AppendOrderMode::CommitSequence {
                            let watermarks = generated_order_watermarks.get_or_insert_with(|| {
                                self.generated_order_watermarks.as_ref().clone()
                            });
                            let watermark = watermarks.get(table).copied().ok_or_else(|| {
                                AppendTableError::Corruption(format!(
                                    "generated-order table {table} has no durable watermark"
                                ))
                            })?;
                            let expected = watermark.checked_add(1).ok_or_else(|| {
                                AppendTableError::Corruption(format!(
                                    "table {table} WAL advances an exhausted generated-order sequence"
                                ))
                            })?;
                            if order_key.0.as_slice()
                                != [RelationalValue::BigInt(expected)].as_slice()
                            {
                                return Err(AppendTableError::Corruption(format!(
                                    "table {table} generated order key is not the next commit sequence value"
                                )));
                            }
                            watermarks.insert(table.clone(), expected);
                        }
                        let prior = staged_watermarks
                            .get(table)
                            .and_then(|partitions| partitions.get(&partition_key))
                            .or_else(|| self.watermark(table, &partition_key));
                        if prior.is_some_and(|watermark| order_key <= *watermark) {
                            return Err(AppendTableError::Constraint(format!(
                                "table {table} order key must increase within its partition"
                            )));
                        }
                        staged_watermarks
                            .entry(table.clone())
                            .or_default()
                            .insert(partition_key.clone(), order_key.clone());
                        payload_bytes = checked_admission_sum(
                            payload_bytes,
                            estimated_row_bytes(row)?,
                            "append transaction payload",
                        )?;
                        staged_rows.push(AppendTableRow {
                            table: table.clone(),
                            partition_key,
                            order_key,
                            row: row.clone(),
                        });
                    }
                }
                AppendWrite::AppendGenerated { table, rows } => {
                    let schema = schemas
                        .as_ref()
                        .unwrap_or(self.schemas.as_ref())
                        .get(table)
                        .ok_or_else(|| {
                            AppendTableError::Schema(format!("unknown table {table}"))
                        })?;
                    if schema.order_mode != AppendOrderMode::CommitSequence {
                        return Err(AppendTableError::Constraint(format!(
                            "table {table} requires caller-provided order keys"
                        )));
                    }
                    let order_position = generated_order_position(schema)?;
                    for row in rows {
                        validate_generated_row(schema, order_position, row)?;
                    }
                    match mode {
                        AppendStageMode::Provisional => {}
                        AppendStageMode::User => {
                            return Err(AppendTableError::Constraint(format!(
                                "table {table} generated rows must be prepared at commit"
                            )));
                        }
                        AppendStageMode::Materialized => {
                            return Err(AppendTableError::Corruption(format!(
                                "WAL transaction contains unresolved generated rows for table {table}"
                            )));
                        }
                    }
                }
            }
        }

        staged_rows.sort_unstable_by(compare_rows);

        let staged_row_count = staged_rows.len();
        let live_rows =
            checked_admission_sum(self.live_rows, staged_row_count, "append live row count")?;
        let live_payload_bytes = checked_admission_sum(
            self.live_payload_bytes,
            payload_bytes,
            "append live payload",
        )?;

        let live_head = if staged_rows.is_empty() {
            self.live_head.clone()
        } else {
            Some(Arc::new(AppendLiveBatch {
                rows: staged_rows.into(),
                watermarks: Arc::new(staged_watermarks),
                previous: self.live_head.clone(),
                payload_bytes,
            }))
        };

        Ok(Self {
            schemas: schemas.map_or_else(|| self.schemas.clone(), Arc::new),
            base_watermarks: self.base_watermarks.clone(),
            generated_order_watermarks: generated_order_watermarks
                .map_or_else(|| self.generated_order_watermarks.clone(), Arc::new),
            live_head,
            live_rows,
            live_payload_bytes,
        })
    }

    pub fn visit_live_rows(
        &self,
        table: &str,
        partition: &RelationalKey,
        after: Option<&RelationalKey>,
        max_rows: usize,
        mut visitor: impl FnMut(&AppendTableRow) -> Result<(), AppendTableError>,
    ) -> Result<AppendLiveReadReport, AppendTableError> {
        if max_rows == 0 {
            return Ok(AppendLiveReadReport::default());
        }
        let mut batches = Vec::new();
        let mut batch = self.live_head.as_deref();
        while let Some(current) = batch {
            batches.push(current);
            batch = current.previous.as_deref();
        }
        let mut report = AppendLiveReadReport::default();
        for batch in batches.into_iter().rev() {
            report.batches_examined = report.batches_examined.saturating_add(1);
            let maximum = batch
                .watermarks
                .get(table)
                .and_then(|partitions| partitions.get(partition));
            if maximum.is_none()
                || maximum
                    .is_some_and(|maximum| after.is_some_and(|watermark| maximum <= watermark))
            {
                report.batches_pruned = report.batches_pruned.saturating_add(1);
                continue;
            }
            let start = lower_bound_live_rows(&batch.rows, table, partition, after);
            for row in batch.rows[start..]
                .iter()
                .take_while(|row| row.table == table && row.partition_key == *partition)
            {
                report.rows_examined = report.rows_examined.saturating_add(1);
                visitor(row)?;
                report.rows_returned = report.rows_returned.saturating_add(1);
                if report.rows_returned == max_rows {
                    return Ok(report);
                }
            }
        }
        Ok(report)
    }
}

fn lower_bound_live_rows(
    rows: &[AppendTableRow],
    table: &str,
    partition: &RelationalKey,
    after: Option<&RelationalKey>,
) -> usize {
    let mut left = 0;
    let mut right = rows.len();
    while left < right {
        let middle = left + (right - left) / 2;
        let row = &rows[middle];
        let before = row.table.as_str() < table
            || (row.table == table && row.partition_key < *partition)
            || (row.table == table
                && row.partition_key == *partition
                && after.is_some_and(|watermark| row.order_key <= *watermark));
        if before {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    left
}

#[derive(Debug)]
struct AppendKeyPositions {
    partition: Vec<usize>,
    order: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendStageMode {
    User,
    Provisional,
    Materialized,
}

fn admit_transaction(
    transaction: &AppendTransaction,
    limits: AppendMutationLimits,
) -> Result<(), AppendTableError> {
    let mut rows = 0usize;
    let mut payload_bytes = 0usize;
    for write in &transaction.writes {
        match write {
            AppendWrite::CreateTable { schema } => {
                payload_bytes = checked_admission_sum(
                    payload_bytes,
                    estimated_schema_bytes(schema)?,
                    "append transaction payload",
                )?;
            }
            AppendWrite::Append {
                table,
                rows: append_rows,
            } => {
                rows = checked_admission_sum(rows, append_rows.len(), "append transaction rows")?;
                payload_bytes = checked_admission_sum(
                    payload_bytes,
                    table.len(),
                    "append transaction payload",
                )?;
                for row in append_rows {
                    payload_bytes = checked_admission_sum(
                        payload_bytes,
                        estimated_row_bytes(row)?,
                        "append transaction payload",
                    )?;
                }
            }
            AppendWrite::AppendGenerated {
                table,
                rows: append_rows,
            } => {
                rows = checked_admission_sum(rows, append_rows.len(), "append transaction rows")?;
                payload_bytes = checked_admission_sum(
                    payload_bytes,
                    table.len(),
                    "append transaction payload",
                )?;
                for row in append_rows {
                    for value in &row.values {
                        let value_bytes = checked_admission_sum(
                            value.estimated_payload_bytes(),
                            1,
                            "append row payload",
                        )?;
                        payload_bytes = checked_admission_sum(
                            payload_bytes,
                            value_bytes,
                            "append transaction payload",
                        )?;
                    }
                    payload_bytes = checked_admission_sum(
                        payload_bytes,
                        std::mem::size_of::<i64>() + 1,
                        "append transaction payload",
                    )?;
                }
            }
        }
    }
    if rows > limits.max_rows.get() {
        return Err(AppendTableError::Admission(format!(
            "transaction contains {rows} rows, exceeding max_rows {}",
            limits.max_rows
        )));
    }
    if payload_bytes > limits.max_payload_bytes.get() {
        return Err(AppendTableError::Admission(format!(
            "transaction contains {payload_bytes} payload bytes, exceeding max_payload_bytes {}",
            limits.max_payload_bytes
        )));
    }
    Ok(())
}

fn validate_table_schema(schema: &AppendTableSchema) -> Result<(), AppendTableError> {
    if schema.name.is_empty() || schema.columns.is_empty() || schema.order_key.is_empty() {
        return Err(AppendTableError::Schema(
            "table name, columns, and order key must be non-empty".to_string(),
        ));
    }
    let mut names = BTreeSet::new();
    for column in &schema.columns {
        if column.name.is_empty() || !names.insert(column.name.as_str()) {
            return Err(AppendTableError::Schema(format!(
                "duplicate or empty column {}",
                column.name
            )));
        }
        if let Some(default) = &column.default {
            match default {
                RelationalColumnDefault::Literal(value) => validate_value_type(column, value)?,
                RelationalColumnDefault::UuidV7 => {
                    return Err(AppendTableError::Schema(format!(
                        "append table column {} does not support uuidv7() defaults",
                        column.name
                    )));
                }
            }
        }
    }
    let mut key_columns = BTreeSet::new();
    for (kind, columns) in [
        ("partition key", schema.partition_key.as_slice()),
        ("order key", schema.order_key.as_slice()),
    ] {
        for column in columns {
            let position = schema.column_position(column).ok_or_else(|| {
                AppendTableError::Schema(format!("{kind} references unknown column {column}"))
            })?;
            if !key_columns.insert(column.as_str()) {
                return Err(AppendTableError::Schema(format!(
                    "partition and order keys repeat column {column}"
                )));
            }
            if schema.columns[position].nullable {
                return Err(AppendTableError::Schema(format!(
                    "{kind} column {column} must be NOT NULL"
                )));
            }
        }
    }
    if schema.order_mode == AppendOrderMode::CommitSequence {
        let position = generated_order_position(schema)?;
        let column = &schema.columns[position];
        if column.default.is_some() {
            return Err(AppendTableError::Schema(format!(
                "generated order column {} must not declare a default",
                column.name
            )));
        }
    }
    Ok(())
}

fn generated_order_position(schema: &AppendTableSchema) -> Result<usize, AppendTableError> {
    if schema.order_key.len() != 1 {
        return Err(AppendTableError::Schema(format!(
            "generated-order table {} requires exactly one order-key column",
            schema.name
        )));
    }
    let position = schema
        .column_position(&schema.order_key[0])
        .ok_or_else(|| {
            AppendTableError::Schema(format!(
                "order key references unknown column {}",
                schema.order_key[0]
            ))
        })?;
    let column = &schema.columns[position];
    if column.scalar_type != RelationalScalarType::BigInt || column.nullable {
        return Err(AppendTableError::Schema(format!(
            "generated order column {} must be BIGINT NOT NULL",
            column.name
        )));
    }
    Ok(position)
}

fn validate_generated_row(
    schema: &AppendTableSchema,
    order_position: usize,
    row: &AppendGeneratedRow,
) -> Result<(), AppendTableError> {
    let expected = schema.columns.len().saturating_sub(1);
    if row.values.len() != expected {
        return Err(AppendTableError::Schema(format!(
            "table {} expects {} caller-provided columns but generated row has {}",
            schema.name,
            expected,
            row.values.len()
        )));
    }
    if row
        .values
        .iter()
        .any(|value| matches!(value, RelationalValue::Overflow(_)))
    {
        return Err(AppendTableError::Constraint(
            "append input rows cannot contain unresolved overflow references".to_string(),
        ));
    }
    let mut input = row.values.iter();
    for (position, column) in schema.columns.iter().enumerate() {
        if position == order_position {
            continue;
        }
        let value = input
            .next()
            .expect("generated row length was validated before type validation");
        validate_value_type(column, value)?;
    }
    Ok(())
}

fn materialize_generated_row(
    order_position: usize,
    row: &AppendGeneratedRow,
    order_value: i64,
) -> RelationalRow {
    let mut values = Vec::with_capacity(row.values.len() + 1);
    let mut input = row.values.iter();
    for position in 0..=row.values.len() {
        if position == order_position {
            values.push(RelationalValue::BigInt(order_value));
        } else {
            values.push(
                input
                    .next()
                    .expect("generated row shape was validated before materialization")
                    .clone(),
            );
        }
    }
    RelationalRow::new(values)
}

fn derive_generated_order_watermarks(
    schemas: &BTreeMap<String, AppendTableSchema>,
    base_watermarks: &BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>,
) -> Result<BTreeMap<String, i64>, AppendTableError> {
    let mut generated = BTreeMap::new();
    for (table, schema) in schemas {
        if schema.order_mode != AppendOrderMode::CommitSequence {
            continue;
        }
        let mut watermark = 0;
        if let Some(partitions) = base_watermarks.get(table) {
            for order in partitions.values() {
                let value = match order.0.as_slice() {
                    [RelationalValue::BigInt(value)] if *value > 0 => *value,
                    _ => {
                        return Err(AppendTableError::Corruption(format!(
                            "generated-order table {table} has an invalid checkpoint order key"
                        )));
                    }
                };
                watermark = watermark.max(value);
            }
        }
        generated.insert(table.clone(), watermark);
    }
    Ok(generated)
}

fn validate_generated_order_watermarks(
    schemas: &BTreeMap<String, AppendTableSchema>,
    base_watermarks: &BTreeMap<String, BTreeMap<RelationalKey, RelationalKey>>,
    generated_order_watermarks: &BTreeMap<String, i64>,
) -> Result<(), AppendTableError> {
    for (table, watermark) in generated_order_watermarks {
        let schema = schemas.get(table).ok_or_else(|| {
            AppendTableError::Corruption(format!(
                "generated-order watermark references unknown table {table}"
            ))
        })?;
        if schema.order_mode != AppendOrderMode::CommitSequence || *watermark < 0 {
            return Err(AppendTableError::Corruption(format!(
                "table {table} has an invalid generated-order watermark"
            )));
        }
    }
    for (table, schema) in schemas {
        match schema.order_mode {
            AppendOrderMode::CallerProvided => {
                if generated_order_watermarks.contains_key(table) {
                    return Err(AppendTableError::Corruption(format!(
                        "caller-provided order table {table} has a generated-order watermark"
                    )));
                }
            }
            AppendOrderMode::CommitSequence => {
                let watermark = generated_order_watermarks.get(table).ok_or_else(|| {
                    AppendTableError::Corruption(format!(
                        "generated-order table {table} has no durable watermark"
                    ))
                })?;
                let derived = derive_generated_order_watermarks(
                    &BTreeMap::from([(table.clone(), schema.clone())]),
                    base_watermarks,
                )?
                .remove(table)
                .expect("generated table produces a derived watermark");
                if derived != *watermark {
                    return Err(AppendTableError::Corruption(format!(
                        "generated-order table {table} checkpoint watermark {watermark} does not match row watermark {derived}"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_row(schema: &AppendTableSchema, row: &RelationalRow) -> Result<(), AppendTableError> {
    if row.values().len() != schema.columns.len() {
        return Err(AppendTableError::Schema(format!(
            "table {} expects {} columns but row has {}",
            schema.name,
            schema.columns.len(),
            row.values().len()
        )));
    }
    if row
        .values()
        .iter()
        .any(|value| matches!(value, RelationalValue::Overflow(_)))
    {
        return Err(AppendTableError::Constraint(
            "append input rows cannot contain unresolved overflow references".to_string(),
        ));
    }
    for (column, value) in schema.columns.iter().zip(row.values()) {
        validate_value_type(column, value)?;
    }
    Ok(())
}

fn validate_value_type(
    column: &RelationalColumnSchema,
    value: &RelationalValue,
) -> Result<(), AppendTableError> {
    if matches!(value, RelationalValue::Null) {
        return if column.nullable {
            Ok(())
        } else {
            Err(AppendTableError::Constraint(format!(
                "column {} is NOT NULL",
                column.name
            )))
        };
    }
    if value.scalar_type() != Some(column.scalar_type) {
        return Err(AppendTableError::Schema(format!(
            "column {} expects {:?} but received {:?}",
            column.name,
            column.scalar_type,
            value.scalar_type()
        )));
    }
    Ok(())
}

fn key_from_row(
    row: &RelationalRow,
    positions: &[usize],
) -> Result<RelationalKey, AppendTableError> {
    let values = positions
        .iter()
        .map(|position| {
            let value = row.values()[*position].clone();
            if matches!(value, RelationalValue::Null | RelationalValue::Overflow(_)) {
                return Err(AppendTableError::Constraint(
                    "append keys must contain inline, non-null values".to_string(),
                ));
            }
            Ok(value)
        })
        .collect::<Result<_, _>>()?;
    Ok(RelationalKey(values))
}

fn validate_key_shape(
    schema: &AppendTableSchema,
    positions: &[usize],
    key: &RelationalKey,
    kind: &str,
) -> Result<(), AppendTableError> {
    if positions.len() != key.0.len() {
        return Err(AppendTableError::Corruption(format!(
            "append {kind} key has {} values but schema expects {}",
            key.0.len(),
            positions.len()
        )));
    }
    for (position, value) in positions.iter().zip(&key.0) {
        if matches!(value, RelationalValue::Null | RelationalValue::Overflow(_))
            || value.scalar_type() != Some(schema.columns[*position].scalar_type)
        {
            return Err(AppendTableError::Corruption(format!(
                "append {kind} key does not match schema column {}",
                schema.columns[*position].name
            )));
        }
    }
    Ok(())
}

fn checked_admission_sum(
    left: usize,
    right: usize,
    context: &str,
) -> Result<usize, AppendTableError> {
    left.checked_add(right)
        .ok_or_else(|| AppendTableError::Admission(format!("{context} overflows usize")))
}

fn estimated_schema_bytes(schema: &AppendTableSchema) -> Result<usize, AppendTableError> {
    let mut bytes = schema.name.len();
    for column in &schema.columns {
        bytes = checked_admission_sum(bytes, column.name.len(), "append schema payload")?;
        bytes = checked_admission_sum(bytes, 2, "append schema payload")?;
    }
    for key in schema.partition_key.iter().chain(&schema.order_key) {
        bytes = checked_admission_sum(bytes, key.len(), "append schema payload")?;
    }
    Ok(bytes)
}

fn estimated_row_bytes(row: &RelationalRow) -> Result<usize, AppendTableError> {
    row.values().iter().try_fold(0usize, |bytes, value| {
        let value_bytes =
            checked_admission_sum(value.estimated_payload_bytes(), 1, "append row payload")?;
        checked_admission_sum(bytes, value_bytes, "append row payload")
    })
}

pub fn append_row_payload_bytes(
    row: &RelationalRow,
) -> std::result::Result<usize, AppendTableError> {
    row.values().iter().try_fold(0usize, |total, value| {
        let value_bytes = value
            .estimated_payload_bytes()
            .checked_add(1)
            .ok_or_else(append_read_payload_overflow)?;
        total
            .checked_add(value_bytes)
            .ok_or_else(append_read_payload_overflow)
    })
}

pub fn append_read_payload_overflow() -> AppendTableError {
    AppendTableError::Admission("append read payload size overflows usize".to_string())
}

pub fn merge_live_read_report(
    report: &mut crate::AppendSegmentReadReport,
    rows_returned: usize,
    batches_examined: usize,
    batches_pruned: usize,
    rows_examined: usize,
) {
    report.rows_returned = report.rows_returned.saturating_add(rows_returned);
    report.live_batches_examined = report
        .live_batches_examined
        .saturating_add(batches_examined);
    report.live_batches_pruned = report.live_batches_pruned.saturating_add(batches_pruned);
    report.live_rows_examined = report.live_rows_examined.saturating_add(rows_examined);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RelationalScalarType;

    #[test]
    fn admission_size_overflow_fails_closed() {
        assert!(matches!(
            checked_admission_sum(usize::MAX, 1, "append test payload"),
            Err(AppendTableError::Admission(message))
                if message == "append test payload overflows usize"
        ));
    }

    fn schema() -> AppendTableSchema {
        AppendTableSchema {
            name: "events".to_string(),
            columns: vec![
                RelationalColumnSchema {
                    name: "stream".to_string(),
                    scalar_type: RelationalScalarType::Text,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "sequence".to_string(),
                    scalar_type: RelationalScalarType::BigInt,
                    nullable: false,
                    default: None,
                },
                RelationalColumnSchema {
                    name: "payload".to_string(),
                    scalar_type: RelationalScalarType::Bytea,
                    nullable: false,
                    default: None,
                },
            ],
            partition_key: vec!["stream".to_string()],
            order_key: vec!["sequence".to_string()],
            order_mode: AppendOrderMode::CallerProvided,
        }
    }

    fn row(stream: &str, sequence: i64) -> RelationalRow {
        RelationalRow::new(vec![
            RelationalValue::Text(stream.to_string()),
            RelationalValue::BigInt(sequence),
            RelationalValue::Bytea(vec![sequence as u8]),
        ])
    }

    fn generated_schema() -> AppendTableSchema {
        AppendTableSchema {
            order_mode: AppendOrderMode::CommitSequence,
            ..schema()
        }
    }

    fn generated_row(stream: &str, payload: u8) -> AppendGeneratedRow {
        AppendGeneratedRow::new(vec![
            RelationalValue::Text(stream.to_string()),
            RelationalValue::Bytea(vec![payload]),
        ])
    }

    fn create_generated_state() -> AppendState {
        AppendState::default()
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::CreateTable {
                        schema: generated_schema(),
                    }],
                },
                AppendMutationLimits::default(),
            )
            .expect("create generated-order append table")
    }

    fn create_state() -> AppendState {
        AppendState::default()
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::CreateTable { schema: schema() }],
                },
                AppendMutationLimits::default(),
            )
            .expect("create append table")
    }

    #[test]
    fn partitions_advance_independently() {
        let state = create_state()
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row("a", 1), row("b", 1), row("a", 2)],
                    }],
                },
                AppendMutationLimits::default(),
            )
            .expect("append rows");

        assert_eq!(
            state.watermark(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())])
            ),
            Some(&RelationalKey(vec![RelationalValue::BigInt(2)]))
        );
        assert_eq!(
            state.watermark(
                "events",
                &RelationalKey(vec![RelationalValue::Text("b".to_string())])
            ),
            Some(&RelationalKey(vec![RelationalValue::BigInt(1)]))
        );
    }

    #[test]
    fn duplicate_or_out_of_order_batch_is_atomic() {
        let state = create_state();
        let error = state
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row("a", 2), row("a", 1)],
                    }],
                },
                AppendMutationLimits::default(),
            )
            .expect_err("out-of-order batch must fail");

        assert!(matches!(error, AppendTableError::Constraint(_)));
        assert_eq!(state.live_rows(), 0);
        assert!(state
            .watermark(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())])
            )
            .is_none());
    }

    #[test]
    fn create_and_append_share_one_atomic_transaction() {
        let state = AppendState::default()
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![
                        AppendWrite::CreateTable { schema: schema() },
                        AppendWrite::Append {
                            table: "events".to_string(),
                            rows: vec![row("a", 1)],
                        },
                    ],
                },
                AppendMutationLimits::default(),
            )
            .expect("create and append");

        assert!(state.schema("events").is_some());
        assert_eq!(state.live_rows(), 1);
    }

    #[test]
    fn schema_or_admission_failure_does_not_publish() {
        let state = AppendState::default();
        let mut invalid = schema();
        invalid.order_key = vec!["missing".to_string()];
        assert!(state
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::CreateTable { schema: invalid }],
                },
                AppendMutationLimits::default(),
            )
            .is_err());
        assert!(state.schemas().is_empty());

        let state = create_state();
        let limits = AppendMutationLimits {
            max_rows: NonZeroUsize::new(1).expect("non-zero"),
            ..AppendMutationLimits::default()
        };
        assert!(matches!(
            state.stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row("a", 1), row("a", 2)],
                    }],
                },
                limits
            ),
            Err(AppendTableError::Admission(_))
        ));
        assert_eq!(state.live_rows(), 0);
    }

    #[test]
    fn live_reader_preserves_append_order_across_batches() {
        let state = create_state()
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row("a", 1), row("a", 2)],
                    }],
                },
                AppendMutationLimits::default(),
            )
            .expect("first append")
            .stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::Append {
                        table: "events".to_string(),
                        rows: vec![row("a", 3), row("a", 4)],
                    }],
                },
                AppendMutationLimits::default(),
            )
            .expect("second append");
        let mut order = Vec::new();
        let report = state
            .visit_live_rows(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())]),
                Some(&RelationalKey(vec![RelationalValue::BigInt(1)])),
                2,
                |row| {
                    order.push(row.order_key.clone());
                    Ok(())
                },
            )
            .expect("read live rows");

        assert_eq!(report.rows_returned, 2);
        assert_eq!(report.batches_examined, 2);
        assert_eq!(report.batches_pruned, 0);
        assert_eq!(report.rows_examined, 2);
        assert_eq!(
            order,
            vec![
                RelationalKey(vec![RelationalValue::BigInt(2)]),
                RelationalKey(vec![RelationalValue::BigInt(3)])
            ]
        );

        let mut tail = Vec::new();
        let tail_report = state
            .visit_live_rows(
                "events",
                &RelationalKey(vec![RelationalValue::Text("a".to_string())]),
                Some(&RelationalKey(vec![RelationalValue::BigInt(2)])),
                10,
                |row| {
                    tail.push(row.order_key.clone());
                    Ok(())
                },
            )
            .expect("read pruned live tail");
        assert_eq!(tail_report.batches_examined, 2);
        assert_eq!(tail_report.batches_pruned, 1);
        assert_eq!(tail_report.rows_examined, 2);
        assert_eq!(tail_report.rows_returned, 2);
    }

    #[test]
    fn commit_preparation_assigns_one_table_wide_contiguous_sequence() {
        let state = create_generated_state();
        let transaction = AppendTransaction {
            writes: vec![
                AppendWrite::AppendGenerated {
                    table: "events".to_string(),
                    rows: vec![generated_row("a", 10), generated_row("b", 20)],
                },
                AppendWrite::AppendGenerated {
                    table: "events".to_string(),
                    rows: vec![generated_row("a", 30)],
                },
            ],
        };

        let prepared = state
            .prepare_transaction(&transaction, AppendMutationLimits::default())
            .expect("prepare generated append");

        assert_eq!(state.generated_order_watermark("events"), Some(0));
        assert_eq!(prepared.state.generated_order_watermark("events"), Some(3));
        assert_eq!(
            prepared
                .mutation_outcomes
                .iter()
                .flat_map(|outcome| outcome.generated_order_keys.iter())
                .cloned()
                .collect::<Vec<_>>(),
            vec![
                RelationalKey(vec![RelationalValue::BigInt(1)]),
                RelationalKey(vec![RelationalValue::BigInt(2)]),
                RelationalKey(vec![RelationalValue::BigInt(3)]),
            ]
        );
        assert!(prepared
            .materialized_transaction
            .writes
            .iter()
            .all(|write| !matches!(write, AppendWrite::AppendGenerated { .. })));
    }

    #[test]
    fn aborted_preparation_does_not_consume_sequence_values() {
        let state = create_generated_state();
        let transaction = AppendTransaction {
            writes: vec![AppendWrite::AppendGenerated {
                table: "events".to_string(),
                rows: vec![generated_row("a", 10)],
            }],
        };

        let first = state
            .prepare_transaction(&transaction, AppendMutationLimits::default())
            .expect("first preparation");
        let retry = state
            .prepare_transaction(&transaction, AppendMutationLimits::default())
            .expect("retry preparation");

        assert_eq!(first.mutation_outcomes, retry.mutation_outcomes);
        assert_eq!(state.generated_order_watermark("events"), Some(0));
    }

    #[test]
    fn generated_rows_are_only_visible_after_materialized_commit() {
        let state = create_generated_state();
        let transaction = AppendTransaction {
            writes: vec![AppendWrite::AppendGenerated {
                table: "events".to_string(),
                rows: vec![generated_row("a", 10)],
            }],
        };

        let provisional = state
            .stage_provisional_transaction(&transaction, AppendMutationLimits::default())
            .expect("validate provisional generated append");
        assert_eq!(provisional.live_rows(), 0);
        assert_eq!(provisional.generated_order_watermark("events"), Some(0));
        assert!(matches!(
            state.stage_transaction(&transaction, AppendMutationLimits::default()),
            Err(AppendTableError::Constraint(_))
        ));
    }

    #[test]
    fn recovery_requires_the_exact_next_generated_value() {
        let state = create_generated_state();
        let gap = AppendTransaction {
            writes: vec![AppendWrite::Append {
                table: "events".to_string(),
                rows: vec![row("a", 2)],
            }],
        };
        assert!(matches!(
            state.apply_recovered_transaction(&gap, AppendMutationLimits::default()),
            Err(AppendTableError::Corruption(_))
        ));

        let next = AppendTransaction {
            writes: vec![AppendWrite::Append {
                table: "events".to_string(),
                rows: vec![row("a", 1)],
            }],
        };
        let recovered = state
            .apply_recovered_transaction(&next, AppendMutationLimits::default())
            .expect("apply exact recovered sequence");
        assert_eq!(recovered.generated_order_watermark("events"), Some(1));
    }

    #[test]
    fn generated_sequence_exhaustion_is_atomic() {
        let partition = RelationalKey(vec![RelationalValue::Text("a".to_string())]);
        let state = AppendState::from_checkpoint_with_generated_order_watermarks(
            BTreeMap::from([("events".to_string(), generated_schema())]),
            BTreeMap::from([(
                "events".to_string(),
                BTreeMap::from([(
                    partition,
                    RelationalKey(vec![RelationalValue::BigInt(i64::MAX)]),
                )]),
            )]),
            BTreeMap::from([("events".to_string(), i64::MAX)]),
        )
        .expect("restore exhausted generated sequence");
        let transaction = AppendTransaction {
            writes: vec![AppendWrite::AppendGenerated {
                table: "events".to_string(),
                rows: vec![generated_row("a", 10)],
            }],
        };

        assert!(matches!(
            state.prepare_transaction(&transaction, AppendMutationLimits::default()),
            Err(AppendTableError::SequenceExhausted {
                watermark: i64::MAX,
                requested: 1,
                ..
            })
        ));
        assert_eq!(state.generated_order_watermark("events"), Some(i64::MAX));
    }

    #[test]
    fn generated_order_schema_and_write_modes_fail_closed() {
        let mut invalid_type = generated_schema();
        invalid_type.columns[1].scalar_type = RelationalScalarType::Text;
        assert!(matches!(
            AppendState::default().stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::CreateTable {
                        schema: invalid_type,
                    }],
                },
                AppendMutationLimits::default(),
            ),
            Err(AppendTableError::Schema(_))
        ));

        let mut invalid_default = generated_schema();
        invalid_default.columns[1].default =
            Some(RelationalColumnDefault::Literal(RelationalValue::BigInt(1)));
        assert!(matches!(
            AppendState::default().stage_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::CreateTable {
                        schema: invalid_default,
                    }],
                },
                AppendMutationLimits::default(),
            ),
            Err(AppendTableError::Schema(_))
        ));

        let explicit = create_state();
        assert!(matches!(
            explicit.prepare_transaction(
                &AppendTransaction {
                    writes: vec![AppendWrite::AppendGenerated {
                        table: "events".to_string(),
                        rows: vec![generated_row("a", 1)],
                    }],
                },
                AppendMutationLimits::default(),
            ),
            Err(AppendTableError::Constraint(_))
        ));
    }
}
