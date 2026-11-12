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

//! Import of a logical dump into a fresh, unpublished database directory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use hawdb_core::{PropertyType, SchemaObjectState, TableKind, VectorDimension};
use hawdb_storage::mutation::GraphMutation;
use serde_json::Value as Json;

use super::export::{quote_identifier, SYSTEM_SCHEMA_REGISTRY_TABLE};
use super::files::{read_metadata, resolve_entry_path, verify_bytes, RecordReader, Stage};
use super::model::{
    Column, ColumnDefault, FileEntry, FileKind, GraphConstraintKind, GraphIndexKind, GraphSchema,
    GraphTableKind, Manifest, ReferentialAction, Schema, SchemaState, Table, FORMAT_NAME,
    MANIFEST_FILE,
};
use super::value::{self, malformed};
use super::{
    LogicalDumpImportReport, LogicalDumpOptions, LogicalDumpVerifyReport,
    LOGICAL_DUMP_FORMAT_VERSION,
};
use crate::embedded::{HawDBEmbedded, HawDBEmbeddedOpenOptions};
use crate::store::{DurabilityPolicy, KernelWriteBatch};
use crate::{HawDBError, Result, Value};

/// Owner of the engine's own rows in the migration registry table.
const ENGINE_SCHEMA_OWNER: &str = hawdb_relational::system_schema::ENGINE_SYSTEM_SCHEMA_OWNER;
/// Upper bound on bound parameters in one generated INSERT statement.
const MAX_INSERT_PARAMETERS: usize = 8192;

/// A dump whose manifest and schema verified and whose file plan is
/// consistent. Data files are verified as they are streamed.
struct OpenedDump {
    manifest_bytes: u64,
    manifest: Manifest,
    manifest_sha256: String,
    schema: Schema,
}

fn open_dump(source: &Path) -> Result<OpenedDump> {
    let manifest_bytes = read_metadata(&source.join(MANIFEST_FILE))?;
    let manifest = parse_manifest(&manifest_bytes)?;
    let manifest_sha256 = {
        let mut hasher = hawdb_integrity::IntegrityHasher::new();
        hasher.update(&manifest_bytes);
        hasher.finish().sha256.to_string()
    };
    if manifest.schema.kind != FileKind::Schema {
        return Err(malformed("manifest schema entry must have kind schema"));
    }
    let schema_bytes = read_metadata(&resolve_entry_path(source, &manifest.schema.path)?)?;
    verify_bytes(&manifest.schema, &schema_bytes)?;
    let schema: Schema = serde_json::from_slice(&schema_bytes)
        .map_err(|error| malformed(format!("schema.json is not a format 1 schema: {error}")))?;
    Ok(OpenedDump {
        manifest_bytes: manifest_bytes.len() as u64,
        manifest,
        manifest_sha256,
        schema,
    })
}

impl OpenedDump {
    fn file_count(&self) -> u64 {
        self.manifest.files.len() as u64 + 2
    }

    fn total_bytes(&self) -> u64 {
        self.manifest
            .files
            .iter()
            .map(|file| file.bytes)
            .sum::<u64>()
            + self.manifest.schema.bytes
            + self.manifest_bytes
    }
}

/// Verifies every file of the dump at `source` against its manifest (length,
/// SHA-256 and record count) and the manifest against the schema, writing
/// nothing.
pub(super) fn verify(source: &Path) -> Result<LogicalDumpVerifyReport> {
    let dump = open_dump(source)?;
    Plan::new(&dump.manifest, &dump.schema)?;
    for entry in &dump.manifest.files {
        let mut reader = RecordReader::open(source, entry)?;
        while reader.next_record()?.is_some() {}
        reader.finish()?;
    }
    let manifest = &dump.manifest;
    Ok(LogicalDumpVerifyReport {
        dump_format_version: manifest.dump_format_version,
        source_hawdb_version: manifest.created_by.hawdb_version.clone(),
        source_persistent_format_version: manifest.created_by.persistent_format_version,
        branch: manifest.source.branch.clone(),
        commit_epoch: manifest.source.commit_epoch,
        table_count: manifest.counts.tables,
        table_row_count: manifest.counts.table_rows,
        node_count: manifest.counts.nodes,
        relationship_count: manifest.counts.relationships,
        file_count: dump.file_count(),
        total_bytes: dump.total_bytes(),
        manifest_sha256: dump.manifest_sha256.clone(),
    })
}

pub(super) fn import(
    source: &Path,
    destination: &Path,
    options: &LogicalDumpOptions,
) -> Result<LogicalDumpImportReport> {
    let dump = open_dump(source)?;
    let (manifest, schema) = (&dump.manifest, &dump.schema);
    let plan = Plan::new(manifest, schema)?;

    let stage = Stage::create(destination, "import")?;
    let mut target = Target::open(stage.path(), options)?;
    target.create_tables(&schema.tables)?;
    for table in &schema.tables {
        target.load_table(source, table, plan.table_files(&table.name))?;
    }
    target.create_table_indexes(&schema.tables)?;
    target.create_graph_schema(&schema.graph)?;
    target.load_graph(source, &plan.nodes, &plan.relationships)?;
    target.finish_graph_schema(&schema.graph)?;
    target.verify(manifest)?;
    target.close()?;
    stage.publish()?;

    Ok(LogicalDumpImportReport {
        dump_format_version: manifest.dump_format_version,
        source_hawdb_version: manifest.created_by.hawdb_version.clone(),
        source_persistent_format_version: manifest.created_by.persistent_format_version,
        persistent_format_version: super::PERSISTENT_FORMAT_VERSION,
        table_count: manifest.counts.tables,
        table_row_count: manifest.counts.table_rows,
        node_count: manifest.counts.nodes,
        relationship_count: manifest.counts.relationships,
        file_count: dump.file_count(),
        total_bytes: dump.total_bytes(),
        manifest_sha256: dump.manifest_sha256.clone(),
    })
}

/// Checks the format name and version before interpreting anything else, so
/// a dump from a newer release is a typed compatibility refusal rather than a
/// parse error.
fn parse_manifest(bytes: &[u8]) -> Result<Manifest> {
    let header: Json = serde_json::from_slice(bytes)
        .map_err(|error| malformed(format!("manifest.json is not valid JSON: {error}")))?;
    if header.get("format").and_then(Json::as_str) != Some(FORMAT_NAME) {
        return Err(malformed(format!(
            "manifest.json does not name the {FORMAT_NAME} format"
        )));
    }
    let version = header
        .get("dump_format_version")
        .and_then(Json::as_u64)
        .ok_or_else(|| malformed("manifest.json has no dump_format_version"))?;
    if version == 0 || version > u64::from(LOGICAL_DUMP_FORMAT_VERSION) {
        return Err(HawDBError::UnsupportedStorageFormat(
            hawdb_core::StorageFormatError::UnsupportedVersion {
                format: "logical dump",
                found: u16::try_from(version).unwrap_or(u16::MAX),
                supported: LOGICAL_DUMP_FORMAT_VERSION,
            },
        ));
    }
    serde_json::from_value(header)
        .map_err(|error| malformed(format!("manifest.json is not a format 1 manifest: {error}")))
}

/// The manifest's data files, validated against the schema before any write.
struct Plan<'a> {
    tables: BTreeMap<&'a str, Vec<&'a FileEntry>>,
    nodes: Vec<&'a FileEntry>,
    relationships: Vec<&'a FileEntry>,
}

impl<'a> Plan<'a> {
    fn new(manifest: &'a Manifest, schema: &'a Schema) -> Result<Self> {
        let names = schema
            .tables
            .iter()
            .map(|table| table.name.as_str())
            .collect::<BTreeSet<_>>();
        if names.len() != schema.tables.len() {
            return Err(malformed("schema.json lists a table twice"));
        }
        let mut paths = BTreeSet::from([manifest.schema.path.as_str()]);
        let mut plan = Self {
            tables: BTreeMap::new(),
            nodes: Vec::new(),
            relationships: Vec::new(),
        };
        for file in &manifest.files {
            if !paths.insert(file.path.as_str()) {
                return Err(malformed(format!("manifest lists {} twice", file.path)));
            }
            match file.kind {
                FileKind::TableRows => {
                    let table = file
                        .table
                        .as_deref()
                        .filter(|table| names.contains(table))
                        .ok_or_else(|| {
                            malformed(format!(
                                "{} holds rows for a table schema.json does not define",
                                file.path
                            ))
                        })?;
                    let table = names.get(table).expect("checked table name");
                    plan.tables.entry(table).or_default().push(file);
                }
                FileKind::Nodes => plan.nodes.push(file),
                FileKind::Relationships => plan.relationships.push(file),
                FileKind::Schema => {
                    return Err(malformed(format!("{} is a second schema file", file.path)))
                }
            }
        }
        let rows = manifest
            .files
            .iter()
            .filter(|file| file.kind == FileKind::TableRows)
            .map(|file| file.records)
            .sum::<u64>();
        let count = |kind| {
            manifest
                .files
                .iter()
                .filter(|file| file.kind == kind)
                .map(|file| file.records)
                .sum::<u64>()
        };
        if manifest.counts.tables != schema.tables.len() as u64
            || manifest.counts.table_rows != rows
            || manifest.counts.nodes != count(FileKind::Nodes)
            || manifest.counts.relationships != count(FileKind::Relationships)
        {
            return Err(malformed(
                "manifest counts do not match its schema and file entries",
            ));
        }
        Ok(plan)
    }

    fn table_files(&self, table: &str) -> &[&'a FileEntry] {
        self.tables.get(table).map_or(&[], Vec::as_slice)
    }
}

fn parse_column_type(column: &Column) -> Result<String> {
    let text = column.column_type.as_str();
    let invalid = || malformed(format!("column {} has unknown type {text:?}", column.name));
    let argument = |prefix: &str| -> Option<&str> { text.strip_prefix(prefix)?.strip_suffix(')') };
    let digits = |value: &str| -> Result<u32> {
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        value.parse().map_err(|_| invalid())
    };
    Ok(match text {
        "boolean" => "BOOLEAN".to_string(),
        "bigint" => "BIGINT".to_string(),
        "double precision" => "DOUBLE PRECISION".to_string(),
        "text" => "TEXT".to_string(),
        "bytea" => "BYTEA".to_string(),
        "uuid" => "UUID".to_string(),
        _ => {
            if let Some(arguments) = argument("numeric(") {
                let (precision, scale) = arguments.split_once(',').ok_or_else(invalid)?;
                format!("NUMERIC({},{})", digits(precision)?, digits(scale)?)
            } else if let Some(dimension) = argument("vector(") {
                format!("VECTOR({})", digits(dimension)?)
            } else if let Some(precision) = argument("timestamptz(") {
                format!("TIMESTAMPTZ({})", digits(precision)?)
            } else if let Some(precision) = argument("timestamp(") {
                format!("TIMESTAMP({})", digits(precision)?)
            } else {
                return Err(invalid());
            }
        }
    })
}

fn sql_string(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// A column default as SQL text. Literals are rendered as a typed string
/// literal (`'text'`), which the column type coerces exactly; numbers and
/// booleans use their plain spelling.
fn default_sql(column: &Column) -> Result<Option<String>> {
    Ok(match &column.default {
        None => None,
        Some(ColumnDefault::Uuidv7) => Some("uuidv7()".to_string()),
        Some(ColumnDefault::CurrentTimestamp { zoned, precision }) => Some(format!(
            "{}({precision})",
            if *zoned {
                "CURRENT_TIMESTAMP"
            } else {
                "LOCALTIMESTAMP"
            }
        )),
        Some(ColumnDefault::Literal(literal)) => Some(match value::decode(literal)? {
            Value::Null => "NULL".to_string(),
            Value::Bool(true) => "TRUE".to_string(),
            Value::Bool(false) => "FALSE".to_string(),
            Value::Int(item) => item.to_string(),
            Value::Float(item) if item.is_finite() => format!("{item:?}"),
            Value::Decimal(item) => item.to_string(),
            Value::String(item) => sql_string(&item),
            Value::Uuid(item) => sql_string(&item.to_string()),
            Value::LocalDateTime(_) | Value::DateTime(_) => {
                let text = match value::decode(literal)? {
                    Value::LocalDateTime(item) => item.to_string(),
                    Value::DateTime(item) => item.to_string(),
                    _ => unreachable!("matched timestamp default"),
                };
                sql_string(&text)
            }
            other => {
                return Err(malformed(format!(
                    "column {} has a {other:?} default that format 1 cannot render",
                    column.name
                )))
            }
        }),
    })
}

fn referential_action_sql(action: ReferentialAction) -> &'static str {
    match action {
        ReferentialAction::NoAction => "NO ACTION",
        ReferentialAction::Restrict => "RESTRICT",
        ReferentialAction::Cascade => "CASCADE",
    }
}

fn column_list(columns: &[String]) -> String {
    columns
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ")
}

fn create_table_sql(table: &Table) -> Result<String> {
    let mut parts = Vec::new();
    for column in &table.columns {
        let mut part = format!(
            "{} {}",
            quote_identifier(&column.name),
            parse_column_type(column)?
        );
        if !column.nullable {
            part.push_str(" NOT NULL");
        }
        if let Some(default) = default_sql(column)? {
            part.push_str(" DEFAULT ");
            part.push_str(&default);
        }
        parts.push(part);
    }
    if !table.primary_key.is_empty() {
        parts.push(format!("PRIMARY KEY ({})", column_list(&table.primary_key)));
    }
    for unique in &table.unique {
        parts.push(format!("UNIQUE ({})", column_list(unique)));
    }
    for key in &table.foreign_keys {
        parts.push(format!(
            "FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
            column_list(&key.columns),
            quote_identifier(&key.references_table),
            column_list(&key.references_columns),
            referential_action_sql(key.on_delete),
            referential_action_sql(key.on_update),
        ));
    }
    Ok(format!(
        "CREATE TABLE {} ({})",
        quote_identifier(&table.name),
        parts.join(", ")
    ))
}

fn property_type(text: &str) -> Result<PropertyType> {
    Ok(match text {
        "any" => PropertyType::Any,
        "bool" => PropertyType::Bool,
        "int" => PropertyType::Int,
        "float" => PropertyType::Float,
        "string" => PropertyType::String,
        "text" => PropertyType::Text,
        "list" => PropertyType::List,
        _ => {
            let dimension = text
                .strip_prefix("vector(")
                .and_then(|rest| rest.strip_suffix(')'))
                .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|digits| digits.parse::<u32>().ok())
                .and_then(VectorDimension::new)
                .ok_or_else(|| malformed(format!("unknown graph property type {text:?}")))?;
            PropertyType::Vector(dimension)
        }
    })
}

fn graph_table_kind(kind: GraphTableKind) -> TableKind {
    match kind {
        GraphTableKind::Node => TableKind::Node,
        GraphTableKind::Relationship => TableKind::Relationship,
    }
}

fn schema_object_state(state: SchemaState) -> SchemaObjectState {
    match state {
        SchemaState::DeleteOnly => SchemaObjectState::DeleteOnly,
        SchemaState::WriteOnly => SchemaObjectState::WriteOnly,
        SchemaState::Backfill => SchemaObjectState::Backfill,
        SchemaState::Validating => SchemaObjectState::Validating,
        SchemaState::Public => SchemaObjectState::Public,
        SchemaState::Gc => SchemaObjectState::Gc,
    }
}

fn record_field<'a>(record: &'a Json, field: &str, file: &FileEntry) -> Result<&'a Json> {
    record
        .get(field)
        .ok_or_else(|| malformed(format!("a record in {} has no {field}", file.path)))
}

fn record_u64(record: &Json, field: &str, file: &FileEntry) -> Result<u64> {
    record_field(record, field, file)?
        .as_u64()
        .ok_or_else(|| malformed(format!("{field} in {} is not an unsigned id", file.path)))
}

fn record_str<'a>(record: &'a Json, field: &str, file: &FileEntry) -> Result<&'a str> {
    record_field(record, field, file)?
        .as_str()
        .ok_or_else(|| malformed(format!("{field} in {} is not a string", file.path)))
}

fn decode_row(record: &Json, table: &Table, file: &FileEntry) -> Result<Vec<Value>> {
    record
        .as_array()
        .filter(|values| values.len() == table.columns.len())
        .ok_or_else(|| {
            malformed(format!(
                "a row in {} does not have {} values",
                file.path,
                table.columns.len()
            ))
        })?
        .iter()
        .map(value::decode)
        .collect()
}

/// Nullable self-referencing foreign-key columns, set after every row of
/// the table exists, and the primary-key columns that address each row.
struct DeferredSelfReferences {
    columns: Vec<usize>,
    key: Vec<usize>,
}

fn deferred_self_references(table: &Table) -> Result<Option<DeferredSelfReferences>> {
    let position = |name: &str| {
        table
            .columns
            .iter()
            .position(|column| column.name == name)
            .ok_or_else(|| malformed(format!("table {} has no column {name}", table.name)))
    };
    let mut columns = BTreeSet::new();
    for key in &table.foreign_keys {
        if key.references_table != table.name {
            continue;
        }
        let positions = key
            .columns
            .iter()
            .map(|column| position(column))
            .collect::<Result<Vec<_>>>()?;
        if positions
            .iter()
            .all(|position| table.columns[*position].nullable)
        {
            columns.extend(positions);
        }
    }
    if columns.is_empty() || table.primary_key.is_empty() {
        return Ok(None);
    }
    let key = table
        .primary_key
        .iter()
        .map(|column| position(column))
        .collect::<Result<Vec<_>>>()?;
    if key.iter().any(|position| columns.contains(position)) {
        return Ok(None);
    }
    Ok(Some(DeferredSelfReferences {
        columns: columns.into_iter().collect(),
        key,
    }))
}

/// The unpublished import database.
struct Target {
    database: HawDBEmbedded,
    page_rows: usize,
    checkpoint_records: u64,
    since_checkpoint: u64,
    /// Rows written per table, for the final count check.
    loaded_rows: BTreeMap<String, u64>,
    /// Engine-owned registry rows the target created itself and kept.
    engine_registry_rows: u64,
    registry_preexisting: bool,
}

impl Target {
    fn open(path: &Path, options: &LogicalDumpOptions) -> Result<Self> {
        // The target is private until publication, so per-write sync buys
        // nothing: the final checkpoint makes the complete import durable
        // before the rename publishes it.
        let database = HawDBEmbedded::open_with_options(
            HawDBEmbeddedOpenOptions::new(path).with_durability(DurabilityPolicy::SyncOnCheckpoint),
        )?;
        Ok(Self {
            database,
            page_rows: options.page_rows.get(),
            checkpoint_records: options.import_checkpoint_records.get() as u64,
            since_checkpoint: 0,
            loaded_rows: BTreeMap::new(),
            engine_registry_rows: 0,
            registry_preexisting: false,
        })
    }

    fn sql(&mut self, sql: &str, parameters: &[Value]) -> Result<()> {
        self.database
            .database_mut()
            .query_sql_with_params(sql, parameters)
            .map(|_| ())
    }

    fn existing_tables(&self) -> Result<BTreeSet<String>> {
        Ok(self
            .database
            .database()
            .logical_dump_target_state()?
            .2
            .into_iter()
            .collect())
    }

    fn create_tables(&mut self, tables: &[Table]) -> Result<()> {
        let existing = self.existing_tables()?;
        for table in tables {
            if existing.contains(&table.name) {
                if table.name != SYSTEM_SCHEMA_REGISTRY_TABLE {
                    return Err(HawDBError::Storage(format!(
                        "logical dump: fresh import target already has table {}",
                        table.name
                    )));
                }
                self.registry_preexisting = true;
                continue;
            }
            let sql = create_table_sql(table)?;
            self.sql(&sql, &[])?;
        }
        if self.registry_preexisting {
            self.engine_registry_rows = self.count_rows(SYSTEM_SCHEMA_REGISTRY_TABLE)?;
        }
        Ok(())
    }

    fn create_table_indexes(&mut self, tables: &[Table]) -> Result<()> {
        for table in tables {
            if table.name == SYSTEM_SCHEMA_REGISTRY_TABLE && self.registry_preexisting {
                continue;
            }
            for index in &table.indexes {
                let sql = format!(
                    "CREATE {}INDEX {} ON {} ({})",
                    if index.unique { "UNIQUE " } else { "" },
                    quote_identifier(&index.name),
                    quote_identifier(&table.name),
                    column_list(&index.columns)
                );
                self.sql(&sql, &[])?;
            }
        }
        Ok(())
    }

    /// Streams one table's rows into multi-row INSERT statements.
    ///
    /// Foreign keys are checked per statement. Tables are loaded parents
    /// first, so only a self-referencing key can point at a row that is not
    /// loaded yet. When every such column is nullable, the first pass writes
    /// them as NULL and a second pass over the same files sets them by
    /// primary key once every row exists. A NOT NULL self-reference is loaded
    /// in primary-key order in one pass and fails (publishing nothing) if a
    /// row references a later one.
    fn load_table(&mut self, source: &Path, table: &Table, files: &[&FileEntry]) -> Result<()> {
        let width = table.columns.len().max(1);
        let batch_rows = self.page_rows.min(MAX_INSERT_PARAMETERS / width).max(1);
        let owner_position = (table.name == SYSTEM_SCHEMA_REGISTRY_TABLE
            && self.registry_preexisting)
            .then(|| {
                table
                    .columns
                    .iter()
                    .position(|column| column.name == "owner")
            })
            .flatten();
        let deferred = deferred_self_references(table)?;
        let mut loaded = 0u64;
        let mut batch = Vec::with_capacity(batch_rows);
        let mut pending_updates = 0u64;
        for file in files {
            let mut reader = RecordReader::open(source, file)?;
            while let Some(record) = reader.next_record()? {
                let mut values = decode_row(&record, table, file)?;
                // The importing engine keeps its own registry rows; only the
                // application owners' migration records come from the dump.
                if let Some(position) = owner_position
                    && values[position] == Value::String(ENGINE_SCHEMA_OWNER.to_string())
                {
                    continue;
                }
                if let Some(deferred) = &deferred
                    && deferred
                        .columns
                        .iter()
                        .any(|position| values[*position] != Value::Null)
                {
                    pending_updates += 1;
                    for position in &deferred.columns {
                        values[*position] = Value::Null;
                    }
                }
                batch.push(values);
                if batch.len() == batch_rows {
                    loaded += self.insert_rows(table, &mut batch)?;
                }
            }
            reader.finish()?;
        }
        loaded += self.insert_rows(table, &mut batch)?;
        if let Some(deferred) = deferred
            && pending_updates > 0
        {
            self.restore_self_references(source, table, files, &deferred)?;
        }
        self.loaded_rows.insert(table.name.clone(), loaded);
        Ok(())
    }

    fn restore_self_references(
        &mut self,
        source: &Path,
        table: &Table,
        files: &[&FileEntry],
        deferred: &DeferredSelfReferences,
    ) -> Result<()> {
        let assignments = deferred
            .columns
            .iter()
            .enumerate()
            .map(|(index, position)| {
                format!(
                    "{} = ${}",
                    quote_identifier(&table.columns[*position].name),
                    index + 1
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = deferred
            .key
            .iter()
            .enumerate()
            .map(|(index, position)| {
                format!(
                    "{} = ${}",
                    quote_identifier(&table.columns[*position].name),
                    deferred.columns.len() + index + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "UPDATE {} SET {assignments} WHERE {predicate}",
            quote_identifier(&table.name)
        );
        for file in files {
            let mut reader = RecordReader::open(source, file)?;
            while let Some(record) = reader.next_record()? {
                let values = decode_row(&record, table, file)?;
                if deferred
                    .columns
                    .iter()
                    .all(|position| values[*position] == Value::Null)
                {
                    continue;
                }
                let parameters = deferred
                    .columns
                    .iter()
                    .chain(&deferred.key)
                    .map(|position| values[*position].clone())
                    .collect::<Vec<_>>();
                self.sql(&sql, &parameters)?;
            }
            reader.finish()?;
        }
        Ok(())
    }

    fn insert_rows(&mut self, table: &Table, batch: &mut Vec<Vec<Value>>) -> Result<u64> {
        if batch.is_empty() {
            return Ok(0);
        }
        let width = table.columns.len();
        let columns = table
            .columns
            .iter()
            .map(|column| quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let tuples = (0..batch.len())
            .map(|row| {
                format!(
                    "({})",
                    (1..=width)
                        .map(|column| format!("${}", row * width + column))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {} ({columns}) VALUES {tuples}",
            quote_identifier(&table.name)
        );
        let rows = batch.len() as u64;
        let parameters = batch.drain(..).flatten().collect::<Vec<_>>();
        self.sql(&sql, &parameters)?;
        Ok(rows)
    }

    fn count_rows(&mut self, table: &str) -> Result<u64> {
        let output = self.database.database_mut().query_sql(&format!(
            "SELECT COUNT(*) AS n FROM {}",
            quote_identifier(table)
        ))?;
        output
            .value_rows()
            .next()
            .and_then(|row| match row.first() {
                Some(Value::Int(count)) => u64::try_from(*count).ok(),
                _ => None,
            })
            .ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
                    "logical dump: COUNT(*) on imported table {table} returned no count"
                ))
            })
    }

    fn commit_graph(&mut self, graph: Vec<GraphMutation>) -> Result<()> {
        if graph.is_empty() {
            return Ok(());
        }
        self.database
            .database_mut()
            .commit_kernel_write_batch(KernelWriteBatch {
                graph,
                ..KernelWriteBatch::default()
            })
            .map(|_| ())
    }

    /// Labels, relationship types, typed tables and properties: everything
    /// node and relationship writes are validated against.
    fn create_graph_schema(&mut self, graph: &GraphSchema) -> Result<()> {
        let mut mutations = Vec::new();
        for label in &graph.node_labels {
            mutations.push(GraphMutation::CreateNodeLabel {
                label: label.clone(),
            });
        }
        for rel_type in &graph.relationship_types {
            mutations.push(GraphMutation::CreateRelationshipType {
                rel_type: rel_type.clone(),
            });
        }
        for table in &graph.tables {
            mutations.push(match table.kind {
                GraphTableKind::Node => GraphMutation::CreateNodeTable {
                    name: table.name.clone(),
                },
                GraphTableKind::Relationship => GraphMutation::CreateRelationshipTable {
                    name: table.name.clone(),
                },
            });
        }
        for property in &graph.properties {
            mutations.push(GraphMutation::CreateProperty {
                table_kind: graph_table_kind(property.table_kind),
                table: property.table.clone(),
                property: property.name.clone(),
                value_type: property_type(&property.property_type)?,
                nullable: property.nullable,
            });
        }
        self.commit_graph(mutations)
    }

    /// Indexes and constraints are built over the loaded data, which also
    /// validates every constraint against it; then non-public schema states
    /// are restored.
    fn finish_graph_schema(&mut self, graph: &GraphSchema) -> Result<()> {
        let mut mutations = Vec::new();
        for index in &graph.indexes {
            let single = || -> Result<String> {
                match index.properties.as_slice() {
                    [property] => Ok(property.clone()),
                    _ => Err(malformed(format!(
                        "graph index on {} must name exactly one property",
                        index.label
                    ))),
                }
            };
            let label = index.label.clone();
            mutations.push(match index.kind {
                GraphIndexKind::Equality => GraphMutation::CreateIndex {
                    label,
                    property: single()?,
                },
                GraphIndexKind::Range => GraphMutation::CreateRangeIndex {
                    label,
                    property: single()?,
                },
                GraphIndexKind::FullText => GraphMutation::CreateFullTextIndex {
                    label,
                    property: single()?,
                },
                GraphIndexKind::Composite => GraphMutation::CreateCompositeIndex {
                    label,
                    properties: index.properties.clone(),
                },
            });
        }
        for constraint in &graph.constraints {
            let subject = constraint.subject.clone();
            let property = constraint.property.clone();
            mutations.push(match constraint.kind {
                GraphConstraintKind::NodeUnique => GraphMutation::CreateUniqueConstraint {
                    label: subject,
                    property,
                },
                GraphConstraintKind::NodeExists => {
                    GraphMutation::CreateNodePropertyExistsConstraint {
                        label: subject,
                        property,
                    }
                }
                GraphConstraintKind::RelationshipUnique => {
                    GraphMutation::CreateRelationshipUniqueConstraint {
                        rel_type: subject,
                        property,
                    }
                }
                GraphConstraintKind::RelationshipExists => {
                    GraphMutation::CreateRelationshipPropertyExistsConstraint {
                        rel_type: subject,
                        property,
                    }
                }
            });
        }
        for table in &graph.tables {
            if table.state != SchemaState::Public {
                mutations.push(GraphMutation::AlterTableState {
                    table_kind: graph_table_kind(table.kind),
                    table: table.name.clone(),
                    state: schema_object_state(table.state),
                });
            }
        }
        for property in &graph.properties {
            if property.state != SchemaState::Public {
                mutations.push(GraphMutation::AlterPropertyState {
                    table_kind: graph_table_kind(property.table_kind),
                    table: property.table.clone(),
                    property: property.name.clone(),
                    state: schema_object_state(property.state),
                });
            }
        }
        self.commit_graph(mutations)
    }

    fn graph_batch(
        &mut self,
        nodes: &mut Vec<crate::store::GraphSnapshotNodeImport>,
        relationships: &mut Vec<crate::store::GraphSnapshotRelationshipImport>,
    ) -> Result<()> {
        let records = (nodes.len() + relationships.len()) as u64;
        if records == 0 {
            return Ok(());
        }
        self.database
            .database_mut()
            .logical_dump_import_graph_batch(
                std::mem::take(nodes),
                std::mem::take(relationships),
            )?;
        self.since_checkpoint += records;
        if self.since_checkpoint >= self.checkpoint_records {
            self.database.database_mut().checkpoint()?;
            self.since_checkpoint = 0;
        }
        Ok(())
    }

    fn load_graph(
        &mut self,
        source: &Path,
        node_files: &[&FileEntry],
        relationship_files: &[&FileEntry],
    ) -> Result<()> {
        let mut nodes = Vec::with_capacity(self.page_rows);
        let mut relationships = Vec::new();
        for file in node_files {
            let mut reader = RecordReader::open(source, file)?;
            while let Some(record) = reader.next_record()? {
                nodes.push((
                    hawdb_core::ids::NodeId(record_u64(&record, "id", file)?),
                    record_str(&record, "label", file)?.to_string(),
                    value::decode_properties(record_field(&record, "properties", file)?)?,
                ));
                if nodes.len() == self.page_rows {
                    self.graph_batch(&mut nodes, &mut relationships)?;
                }
            }
            reader.finish()?;
        }
        self.graph_batch(&mut nodes, &mut relationships)?;
        for file in relationship_files {
            let mut reader = RecordReader::open(source, file)?;
            while let Some(record) = reader.next_record()? {
                relationships.push((
                    hawdb_core::ids::RelId(record_u64(&record, "id", file)?),
                    hawdb_core::ids::NodeId(record_u64(&record, "source", file)?),
                    hawdb_core::ids::NodeId(record_u64(&record, "target", file)?),
                    record_str(&record, "type", file)?.to_string(),
                    value::decode_properties(record_field(&record, "properties", file)?)?,
                ));
                if relationships.len() == self.page_rows {
                    self.graph_batch(&mut nodes, &mut relationships)?;
                }
            }
            reader.finish()?;
        }
        self.graph_batch(&mut nodes, &mut relationships)
    }

    fn verify(&mut self, manifest: &Manifest) -> Result<()> {
        for (table, loaded) in self.loaded_rows.clone() {
            let mut expected = loaded;
            if table == SYSTEM_SCHEMA_REGISTRY_TABLE && self.registry_preexisting {
                expected += self.engine_registry_rows;
            }
            let counted = self.count_rows(&table)?;
            if counted != expected {
                return Err(HawDBError::StorageIntegrity(format!(
                    "logical dump: imported table {table} holds {counted} rows; {expected} were written"
                )));
            }
        }
        let (nodes, relationships, _) = self.database.database().logical_dump_target_state()?;
        if nodes != manifest.counts.nodes || relationships != manifest.counts.relationships {
            return Err(HawDBError::StorageIntegrity(format!(
                "logical dump: imported {nodes} nodes and {relationships} relationships; the \
                 manifest records {} and {}",
                manifest.counts.nodes, manifest.counts.relationships
            )));
        }
        Ok(())
    }

    /// Makes the complete import durable and releases the directory.
    fn close(mut self) -> Result<()> {
        self.database.database_mut().checkpoint()?;
        drop(self.database);
        Ok(())
    }
}
