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

//! Export of one pinned read snapshot into a logical dump directory.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use hawdb_core::{
    ConstraintKind, ConstraintSubject, IndexKind, PropertyDefault, PropertyDescriptor,
    PropertyType, SchemaObjectState, TableKind,
};
use hawdb_executor::QueryStreamOptions;
use hawdb_storage::relational::{
    RelationalColumnDefault, RelationalReferentialAction, RelationalScalarType,
    RelationalTableSchema,
};
use serde_json::{json, Value as Json};

use super::files::{io_error, HashedWriter, Stage};
use super::model::{
    Column, ColumnDefault, Counts, CreatedBy, FileEntry, FileKind, ForeignKey, GraphConstraint,
    GraphConstraintKind, GraphIndex, GraphIndexKind, GraphProperty, GraphPropertyDefault,
    GraphSchema, GraphTable, GraphTableKind, Index, Manifest, ReferentialAction, SchemaState,
    Source, Table, DATA_DIR, FORMAT_NAME, MANIFEST_FILE, SCHEMA_FILE,
};
use super::{value, LogicalDumpExportReport, LogicalDumpOptions, LOGICAL_DUMP_FORMAT_VERSION};
use crate::api::DatabaseReadTransaction;
use crate::schema::Catalog;
use crate::store::GraphStore;
use crate::{HawDBError, Result, Value};

/// The engine-owned migration registry. Its rows are exported like any table;
/// import keeps the importing engine's own rows (see import.rs).
pub(super) const SYSTEM_SCHEMA_REGISTRY_TABLE: &str =
    hawdb_relational::system_schema::ENGINE_SYSTEM_SCHEMA_REGISTRY_TABLE;

fn unsupported(kind: &str, detail: impl std::fmt::Display) -> HawDBError {
    HawDBError::Semantic(format!(
        "logical dump format {LOGICAL_DUMP_FORMAT_VERSION} cannot represent {kind} ({detail}); \
         nothing was exported. See docs/LOGICAL_DUMP.md, \"Coverage\""
    ))
}

pub(super) fn export(
    source: &DatabaseReadTransaction,
    destination: &Path,
    options: &LogicalDumpOptions,
) -> Result<LogicalDumpExportReport> {
    let (catalog, store) = source.logical_dump_source();
    store.ensure_usable()?;
    refuse_unrepresentable(store)?;
    let tables = relational_tables(store)?;
    let graph = graph_schema(catalog)?;

    let stage = Stage::create(destination, "export")?;
    let data = stage.path().join(DATA_DIR);
    fs::create_dir(&data).map_err(|error| io_error("create", &data, error))?;

    let mut files = Vec::new();
    let mut counts = Counts {
        tables: tables.len() as u64,
        ..Counts::default()
    };
    for (ordinal, (schema, table)) in tables.iter().enumerate() {
        counts.table_rows += export_table_rows(
            source,
            schema,
            table,
            ordinal,
            stage.path(),
            options,
            &mut files,
        )?;
    }
    let (nodes, relationships) = export_graph(catalog, store, stage.path(), options, &mut files)?;
    counts.nodes = nodes;
    counts.relationships = relationships;
    sync_directory(&data)?;

    let schema_document = super::model::Schema {
        tables: tables.into_iter().map(|(_, table)| table).collect(),
        graph,
    };
    let mut schema_writer = HashedWriter::create(stage.path(), SCHEMA_FILE.to_string())?;
    schema_writer.write_document(&schema_document)?;
    let schema = schema_writer.finish(FileKind::Schema, None)?;

    let manifest = Manifest {
        format: FORMAT_NAME.to_string(),
        dump_format_version: LOGICAL_DUMP_FORMAT_VERSION,
        created_by: CreatedBy {
            hawdb_version: env!("CARGO_PKG_VERSION").to_string(),
            persistent_format_version: super::PERSISTENT_FORMAT_VERSION,
        },
        source: Source {
            branch: source.logical_dump_branch_name(),
            commit_epoch: source.commit_epoch(),
        },
        schema,
        files,
        counts,
    };
    let mut manifest_writer = HashedWriter::create(stage.path(), MANIFEST_FILE.to_string())?;
    manifest_writer.write_document(&manifest)?;
    let manifest_entry = manifest_writer.finish(FileKind::Schema, None)?;
    stage.publish()?;

    Ok(LogicalDumpExportReport {
        dump_format_version: LOGICAL_DUMP_FORMAT_VERSION,
        persistent_format_version: super::PERSISTENT_FORMAT_VERSION,
        hawdb_version: manifest.created_by.hawdb_version,
        branch: manifest.source.branch,
        commit_epoch: manifest.source.commit_epoch,
        table_count: counts.tables,
        table_row_count: counts.table_rows,
        node_count: counts.nodes,
        relationship_count: counts.relationships,
        file_count: manifest.files.len() as u64 + 2,
        total_bytes: manifest.files.iter().map(|file| file.bytes).sum::<u64>()
            + manifest.schema.bytes
            + manifest_entry.bytes,
        manifest_sha256: manifest_entry.sha256,
    })
}

fn sync_directory(path: &Path) -> Result<()> {
    super::files::sync_directory(path)
}

/// Kinds of persistent state format 1 has no representation for. Refusing
/// them up front keeps the contract simple: a dump that exists is complete.
fn refuse_unrepresentable(store: &GraphStore) -> Result<()> {
    if let Some(table) = store.append_state().schemas().keys().next() {
        return Err(unsupported(
            "strict append tables",
            format!("table {table}"),
        ));
    }
    if let Some(status) = store.projected_graph_statuses().first() {
        return Err(unsupported(
            "persisted projected graph definitions",
            format!("projected graph {}", status.name),
        ));
    }
    for schema in store.relational_state().table_schemas() {
        if schema.materialized_view.is_some() {
            return Err(unsupported(
                "materialized views",
                format!("view {}", schema.name),
            ));
        }
    }
    Ok(())
}

pub(super) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) fn scalar_type_name(scalar_type: RelationalScalarType) -> String {
    match scalar_type {
        RelationalScalarType::Boolean => "boolean".to_string(),
        RelationalScalarType::BigInt => "bigint".to_string(),
        RelationalScalarType::DoublePrecision => "double precision".to_string(),
        RelationalScalarType::Decimal(decimal) => {
            format!("numeric({},{})", decimal.precision(), decimal.scale())
        }
        RelationalScalarType::Text => "text".to_string(),
        RelationalScalarType::Bytea => "bytea".to_string(),
        RelationalScalarType::Uuid => "uuid".to_string(),
        RelationalScalarType::Vector(dimension) => format!("vector({})", dimension.get()),
        RelationalScalarType::Timestamp(precision) => format!("timestamp({})", precision.get()),
        RelationalScalarType::TimestampTz(precision) => {
            format!("timestamptz({})", precision.get())
        }
    }
}

fn referential_action(action: RelationalReferentialAction) -> ReferentialAction {
    match action {
        RelationalReferentialAction::NoAction => ReferentialAction::NoAction,
        RelationalReferentialAction::Restrict => ReferentialAction::Restrict,
        RelationalReferentialAction::Cascade => ReferentialAction::Cascade,
    }
}

fn table_model(schema: &RelationalTableSchema) -> Result<Table> {
    let columns = schema
        .columns
        .iter()
        .map(|column| {
            let default = match &column.default {
                None => None,
                Some(RelationalColumnDefault::Literal(literal)) => {
                    let literal = hawdb_relational::query_value::relational_to_value(literal)?;
                    // Import re-declares the default as SQL text; refuse here,
                    // before anything is written, what it could not restate.
                    if !matches!(
                        literal,
                        Value::Null
                            | Value::Bool(_)
                            | Value::Int(_)
                            | Value::Decimal(_)
                            | Value::String(_)
                            | Value::Uuid(_)
                            | Value::LocalDateTime(_)
                            | Value::DateTime(_)
                    ) && !matches!(literal, Value::Float(item) if item.is_finite())
                    {
                        return Err(unsupported(
                            "this column default",
                            format!("{}.{} DEFAULT {literal:?}", schema.name, column.name),
                        ));
                    }
                    Some(ColumnDefault::Literal(value::encode(&literal)))
                }
                Some(RelationalColumnDefault::UuidV7) => Some(ColumnDefault::Uuidv7),
                Some(RelationalColumnDefault::CurrentTimestamp(kind)) => {
                    Some(ColumnDefault::CurrentTimestamp {
                        zoned: kind.zoned,
                        precision: kind.precision.get(),
                    })
                }
            };
            Ok(Column {
                name: column.name.clone(),
                column_type: scalar_type_name(column.scalar_type),
                nullable: column.nullable,
                default,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Table {
        name: schema.name.clone(),
        columns,
        primary_key: schema.primary_key.clone(),
        unique: schema.unique_constraints.clone(),
        foreign_keys: schema
            .foreign_keys
            .iter()
            .map(|key| ForeignKey {
                columns: key.columns.clone(),
                references_table: key.referenced_table.clone(),
                references_columns: key.referenced_columns.clone(),
                on_delete: referential_action(key.on_delete),
                on_update: referential_action(key.on_update),
            })
            .collect(),
        indexes: schema
            .indexes
            .iter()
            .map(|index| Index {
                name: index.name.clone(),
                columns: index.columns.clone(),
                unique: index.unique,
            })
            .collect(),
        search_indexes: Vec::new(),
    })
}

/// Tables ordered so that every foreign key's target precedes its source.
/// A self-reference is allowed (rows are then written in primary-key order and
/// imported in one statement per page); a cycle between tables is refused.
fn relational_tables(store: &GraphStore) -> Result<Vec<(RelationalTableSchema, Table)>> {
    let schemas = store
        .relational_state()
        .table_schemas()
        .map(|schema| (schema.name.clone(), schema.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut ordered = Vec::with_capacity(schemas.len());
    let mut placed = BTreeSet::new();
    while placed.len() < schemas.len() {
        let ready = schemas
            .values()
            .filter(|schema| !placed.contains(&schema.name))
            .find(|schema| {
                schema.foreign_keys.iter().all(|key| {
                    key.referenced_table == schema.name || placed.contains(&key.referenced_table)
                })
            });
        let Some(schema) = ready else {
            let remaining = schemas
                .keys()
                .filter(|name| !placed.contains(*name))
                .cloned()
                .collect::<Vec<_>>();
            return Err(unsupported(
                "foreign-key cycles between tables",
                format!("tables {}", remaining.join(", ")),
            ));
        };
        placed.insert(schema.name.clone());
        ordered.push((schema.clone(), table_model(schema)?));
    }
    Ok(ordered)
}

fn data_file_name(prefix: &str, ordinal: usize, part: usize) -> String {
    format!("{DATA_DIR}/{prefix}-{ordinal:05}-{part:05}.jsonl")
}

/// Rotates data files at `file_records` records.
struct DataFiles<'a> {
    root: &'a Path,
    prefix: &'static str,
    ordinal: usize,
    kind: FileKind,
    table: Option<String>,
    limit: u64,
    part: usize,
    current: Option<HashedWriter>,
    files: &'a mut Vec<FileEntry>,
}

impl<'a> DataFiles<'a> {
    fn new(
        root: &'a Path,
        prefix: &'static str,
        ordinal: usize,
        kind: FileKind,
        table: Option<String>,
        options: &LogicalDumpOptions,
        files: &'a mut Vec<FileEntry>,
    ) -> Self {
        Self {
            root,
            prefix,
            ordinal,
            kind,
            table,
            limit: options.file_records.get() as u64,
            part: 0,
            current: None,
            files,
        }
    }

    fn write(&mut self, record: &Json) -> Result<()> {
        if self
            .current
            .as_ref()
            .is_some_and(|writer| writer.records() >= self.limit)
        {
            self.rotate()?;
        }
        if self.current.is_none() {
            self.current = Some(HashedWriter::create(
                self.root,
                data_file_name(self.prefix, self.ordinal, self.part),
            )?);
            self.part += 1;
        }
        self.current
            .as_mut()
            .expect("current data file")
            .write_record(record)
    }

    fn rotate(&mut self) -> Result<()> {
        if let Some(writer) = self.current.take() {
            self.files
                .push(writer.finish(self.kind, self.table.clone())?);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.rotate()
    }
}

fn is_page_budget_error(error: &HawDBError) -> bool {
    matches!(
        error,
        HawDBError::ReadBudgetExceeded(_) | HawDBError::ResourceBudgetExceeded(_)
    )
}

/// Streams one table in primary-key keyset pages (offset pages for a table
/// without a primary key) and cross-checks the row count at the same snapshot.
fn export_table_rows(
    source: &DatabaseReadTransaction,
    schema: &RelationalTableSchema,
    table: &Table,
    ordinal: usize,
    root: &Path,
    options: &LogicalDumpOptions,
    files: &mut Vec<FileEntry>,
) -> Result<u64> {
    let name = quote_identifier(&schema.name);
    let column_list = schema
        .columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let key_positions = schema
        .primary_key
        .iter()
        .map(|key| {
            schema
                .columns
                .iter()
                .position(|column| &column.name == key)
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(format!(
                        "logical dump: table {} primary key column {key} is missing",
                        schema.name
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let order_by = schema
        .primary_key
        .iter()
        .map(|key| format!("{} ASC", quote_identifier(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let max_payload_bytes = source.logical_dump_payload_budget();
    let mut writer = DataFiles::new(
        root,
        "table",
        ordinal,
        FileKind::TableRows,
        Some(table.name.clone()),
        options,
        files,
    );
    let mut page_rows = options.page_rows.get();
    let mut last_key: Option<Vec<Value>> = None;
    let mut offset = 0usize;
    let mut exported = 0u64;
    loop {
        let (sql, parameters) = if key_positions.is_empty() {
            (
                format!("SELECT {column_list} FROM {name} LIMIT {page_rows} OFFSET {offset}"),
                Vec::new(),
            )
        } else if let Some(last) = &last_key {
            (
                format!(
                    "SELECT {column_list} FROM {name} WHERE {} ORDER BY {order_by} LIMIT {page_rows}",
                    keyset_predicate(&schema.primary_key)
                ),
                last.clone(),
            )
        } else {
            (
                format!("SELECT {column_list} FROM {name} ORDER BY {order_by} LIMIT {page_rows}"),
                Vec::new(),
            )
        };
        let output = match source.query_sql_with_params_options(
            &sql,
            &parameters,
            QueryStreamOptions {
                max_rows: Some(page_rows),
                max_payload_bytes,
            },
        ) {
            Ok(output) => output,
            Err(error) if is_page_budget_error(&error) && page_rows > 1 => {
                page_rows = page_rows.div_ceil(2);
                continue;
            }
            Err(error) => return Err(error),
        };
        let mut returned = 0usize;
        let mut last_row = None;
        for row in output.value_rows() {
            if row.len() != schema.columns.len() {
                return Err(HawDBError::StorageIntegrity(format!(
                    "logical dump: table {} returned {} values for {} columns",
                    schema.name,
                    row.len(),
                    schema.columns.len()
                )));
            }
            writer.write(&Json::Array(row.iter().map(value::encode).collect()))?;
            returned += 1;
            last_row = Some(row);
        }
        exported += returned as u64;
        if let Some(row) = last_row {
            last_key = Some(
                key_positions
                    .iter()
                    .map(|position| row[*position].clone())
                    .collect(),
            );
        }
        offset += returned;
        if returned < page_rows {
            break;
        }
    }
    writer.finish()?;
    let counted = source
        .query_sql(&format!("SELECT COUNT(*) AS n FROM {name}"))?
        .value_rows()
        .next()
        .and_then(|row| match row.first() {
            Some(Value::Int(count)) => u64::try_from(*count).ok(),
            _ => None,
        })
        .ok_or_else(|| {
            HawDBError::StorageIntegrity(format!(
                "logical dump: COUNT(*) on table {} returned no count",
                schema.name
            ))
        })?;
    if counted != exported {
        return Err(HawDBError::StorageIntegrity(format!(
            "logical dump: table {} exported {exported} rows but holds {counted} at the same snapshot",
            schema.name
        )));
    }
    Ok(exported)
}

/// `(k1, k2, ...) > ($1, $2, ...)` expanded into the OR-of-prefixes form,
/// since row-value comparison is not part of the SQL surface.
pub(super) fn keyset_predicate(keys: &[String]) -> String {
    (0..keys.len())
        .map(|split| {
            let mut terms = (0..split)
                .map(|index| format!("{} = ${}", quote_identifier(&keys[index]), index + 1))
                .collect::<Vec<_>>();
            terms.push(format!(
                "{} > ${}",
                quote_identifier(&keys[split]),
                split + 1
            ));
            format!("({})", terms.join(" AND "))
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn schema_state(state: SchemaObjectState) -> SchemaState {
    match state {
        SchemaObjectState::DeleteOnly => SchemaState::DeleteOnly,
        SchemaObjectState::WriteOnly => SchemaState::WriteOnly,
        SchemaObjectState::Backfill => SchemaState::Backfill,
        SchemaObjectState::Validating => SchemaState::Validating,
        SchemaObjectState::Public => SchemaState::Public,
        SchemaObjectState::Gc => SchemaState::Gc,
    }
}

fn table_kind(kind: TableKind) -> GraphTableKind {
    match kind {
        TableKind::Node => GraphTableKind::Node,
        TableKind::Relationship => GraphTableKind::Relationship,
    }
}

pub(super) fn property_type_name(property_type: PropertyType) -> String {
    match property_type {
        PropertyType::Any => "any".to_string(),
        PropertyType::Bool => "bool".to_string(),
        PropertyType::Int => "int".to_string(),
        PropertyType::Float => "float".to_string(),
        PropertyType::String => "string".to_string(),
        PropertyType::Text => "text".to_string(),
        PropertyType::List => "list".to_string(),
        PropertyType::Vector(dimension) => format!("vector({})", dimension.get()),
    }
}

/// A declared graph property default in the dump encoding.
///
/// Only a `BACKFILL` descriptor still uses the backfill value resolved at
/// declaration time (schema maintenance writes it into records that lacked
/// the property). Import re-declares the default, which resolves that value
/// again: identical for a literal, a different instant for
/// `current_timestamp()`. That one case is refused rather than restored with
/// a different value.
fn property_default(
    table: &str,
    property: &PropertyDescriptor,
) -> Result<Option<GraphPropertyDefault>> {
    let Some(declared) = &property.default else {
        return Ok(None);
    };
    let backfilling = property.state == SchemaObjectState::Backfill;
    Ok(Some(match &declared.value {
        PropertyDefault::Value(Value::Null) => {
            return Err(HawDBError::StorageIntegrity(format!(
                "logical dump: property {table}.{} declares DEFAULT NULL",
                property.name
            )));
        }
        PropertyDefault::Value(literal) => {
            if backfilling && declared.backfill_value != *literal {
                return Err(HawDBError::StorageIntegrity(format!(
                    "logical dump: property {table}.{} backfills a value other than its literal default",
                    property.name
                )));
            }
            GraphPropertyDefault::Literal(value::encode(literal))
        }
        PropertyDefault::CurrentTimestamp => {
            if backfilling {
                return Err(unsupported(
                    "a current_timestamp() property default whose backfill has not finished",
                    format!(
                        "{table}.{} is in BACKFILL; run schema maintenance until it is PUBLIC, then export",
                        property.name
                    ),
                ));
            }
            GraphPropertyDefault::CurrentTimestamp
        }
    }))
}

fn graph_schema(catalog: &Catalog) -> Result<GraphSchema> {
    let label = |id| {
        catalog.label_name(id).map(str::to_string).ok_or_else(|| {
            HawDBError::StorageIntegrity(format!("logical dump: catalog label {id:?} has no name"))
        })
    };
    let rel_type = |id| {
        catalog
            .rel_type_name(id)
            .map(str::to_string)
            .ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
                    "logical dump: catalog relationship type {id:?} has no name"
                ))
            })
    };
    let mut tables = Vec::new();
    let mut table_names = BTreeMap::new();
    for table in catalog.table_descriptors() {
        table_names.insert(table.id, (table.kind, table.name.clone()));
        tables.push(GraphTable {
            kind: table_kind(table.kind),
            name: table.name.clone(),
            state: schema_state(table.state),
        });
    }
    let properties = catalog
        .property_descriptors()
        .map(|property| {
            let (kind, table) = table_names.get(&property.table_id).ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
                    "logical dump: property {} references a missing table",
                    property.name
                ))
            })?;
            Ok(GraphProperty {
                table_kind: table_kind(*kind),
                table: table.clone(),
                name: property.name.clone(),
                property_type: property_type_name(property.value_type),
                nullable: property.nullable,
                state: schema_state(property.state),
                default: property_default(table, property)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut indexes = catalog
        .property_indexes()
        .map(|index| {
            Ok(GraphIndex {
                label: label(index.label_id)?,
                properties: vec![index.property.clone()],
                kind: match index.kind {
                    IndexKind::Equality => GraphIndexKind::Equality,
                    IndexKind::Range => GraphIndexKind::Range,
                    IndexKind::FullText => GraphIndexKind::FullText,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    for index in catalog.composite_property_indexes() {
        indexes.push(GraphIndex {
            label: label(index.label_id)?,
            properties: index.properties.clone(),
            kind: GraphIndexKind::Composite,
        });
    }
    let mut constraints = Vec::new();
    let mut seen = BTreeSet::new();
    for constraint in catalog
        .unique_constraints()
        .chain(catalog.node_property_exists_constraints())
        .chain(catalog.relationship_unique_constraints())
        .chain(catalog.relationship_property_exists_constraints())
    {
        if !seen.insert(constraint.id) {
            continue;
        }
        let subject = match constraint.subject {
            ConstraintSubject::Node(id) => label(id)?,
            ConstraintSubject::Relationship(id) => rel_type(id)?,
        };
        constraints.push(GraphConstraint {
            kind: match constraint.kind {
                ConstraintKind::NodePropertyUnique => GraphConstraintKind::NodeUnique,
                ConstraintKind::NodePropertyExists => GraphConstraintKind::NodeExists,
                ConstraintKind::RelationshipPropertyUnique => {
                    GraphConstraintKind::RelationshipUnique
                }
                ConstraintKind::RelationshipPropertyExists => {
                    GraphConstraintKind::RelationshipExists
                }
            },
            subject,
            property: constraint.property.clone(),
        });
    }
    Ok(GraphSchema {
        node_labels: catalog.labels().map(|label| label.name.clone()).collect(),
        relationship_types: catalog
            .rel_types()
            .map(|rel_type| rel_type.name.clone())
            .collect(),
        tables,
        properties,
        indexes,
        constraints,
    })
}

fn export_graph(
    catalog: &Catalog,
    store: &GraphStore,
    root: &Path,
    options: &LogicalDumpOptions,
    files: &mut Vec<FileEntry>,
) -> Result<(u64, u64)> {
    let mut nodes = 0u64;
    {
        let mut writer = DataFiles::new(root, "nodes", 0, FileKind::Nodes, None, options, files);
        for node in store.node_records_owned() {
            let node = node?;
            let mut labels = node.labels.iter();
            let (Some(label_id), None) = (labels.next(), labels.next()) else {
                return Err(unsupported(
                    "nodes without exactly one label",
                    format!("node {} has {} labels", node.id.0, node.labels.len()),
                ));
            };
            let label = catalog.label_name(*label_id).ok_or_else(|| {
                HawDBError::StorageIntegrity(format!(
                    "logical dump: node {} references a missing label",
                    node.id.0
                ))
            })?;
            writer.write(&json!({
                "id": node.id.0,
                "label": label,
                "properties": value::encode_properties(&node.properties),
            }))?;
            nodes += 1;
        }
        writer.finish()?;
    }
    let mut relationships = 0u64;
    {
        let mut writer = DataFiles::new(
            root,
            "relationships",
            0,
            FileKind::Relationships,
            None,
            options,
            files,
        );
        for relationship in store.relationship_records_owned() {
            let relationship = relationship?;
            let rel_type = catalog
                .rel_type_name(relationship.rel_type)
                .ok_or_else(|| {
                    HawDBError::StorageIntegrity(format!(
                        "logical dump: relationship {} references a missing type",
                        relationship.id.0
                    ))
                })?;
            writer.write(&json!({
                "id": relationship.id.0,
                "type": rel_type,
                "source": relationship.source.0,
                "target": relationship.target.0,
                "properties": value::encode_properties(&relationship.properties),
            }))?;
            relationships += 1;
        }
        writer.finish()?;
    }
    let statistics = store.basic_statistics();
    if statistics.node_count != nodes || statistics.relationship_count != relationships {
        return Err(HawDBError::StorageIntegrity(format!(
            "logical dump: exported {nodes} nodes and {relationships} relationships, but the \
             snapshot's statistics record {} and {}",
            statistics.node_count, statistics.relationship_count
        )));
    }
    Ok((nodes, relationships))
}
