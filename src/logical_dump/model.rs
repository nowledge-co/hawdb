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

//! Serialized shapes of `manifest.json` and `schema.json`.
//!
//! These structs are the dump format. Field names and enum spellings are part
//! of the documented contract in docs/LOGICAL_DUMP.md: a later reader must
//! keep accepting every spelling written by format version 1. Unknown object
//! fields are ignored on read (no `deny_unknown_fields`); unknown enum
//! spellings fail deserialization, so a reader never silently drops a kind of
//! data it does not understand.

use serde::{Deserialize, Serialize};

pub(super) const FORMAT_NAME: &str = "hawdb-logical-dump";
pub(super) const MANIFEST_FILE: &str = "manifest.json";
pub(super) const SCHEMA_FILE: &str = "schema.json";
pub(super) const DATA_DIR: &str = "data";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub format: String,
    pub dump_format_version: u16,
    pub created_by: CreatedBy,
    pub source: Source,
    pub schema: FileEntry,
    pub files: Vec<FileEntry>,
    pub counts: Counts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CreatedBy {
    pub hawdb_version: String,
    pub persistent_format_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Source {
    /// Name of the exported branch, when the database has a branch catalog.
    pub branch: Option<String>,
    pub commit_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct FileEntry {
    pub path: String,
    pub kind: FileKind,
    /// The relational table whose rows the file holds (`table_rows` only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// JSON Lines records in the file; 0 for `schema`.
    pub records: u64,
    pub bytes: u64,
    /// Lowercase hex SHA-256 of the file's bytes.
    pub sha256: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum FileKind {
    Schema,
    TableRows,
    Nodes,
    Relationships,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Counts {
    pub tables: u64,
    pub table_rows: u64,
    pub nodes: u64,
    pub relationships: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub(super) struct Schema {
    /// Tables in an order that satisfies every foreign key, parents first.
    pub tables: Vec<Table>,
    pub graph: GraphSchema,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    #[serde(default)]
    pub unique: Vec<Vec<String>>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
    #[serde(default)]
    pub indexes: Vec<Index>,
    /// BM25 search indexes over this table's TEXT columns. Only the
    /// definition is dumped; import recreates each index before loading the
    /// table's rows, so the postings are rebuilt from those rows. Omitted when
    /// empty, so a dump without search indexes is byte-identical to one
    /// written before the field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub search_indexes: Vec<SearchIndex>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(super) struct Column {
    pub name: String,
    /// `boolean`, `bigint`, `double precision`, `numeric(p,s)`, `text`,
    /// `bytea`, `uuid`, `vector(n)`, `timestamp(p)` or `timestamptz(p)`.
    #[serde(rename = "type")]
    pub column_type: String,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<ColumnDefault>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ColumnDefault {
    /// A literal in the dump value encoding.
    Literal(serde_json::Value),
    Uuidv7,
    /// `CURRENT_TIMESTAMP(p)` when `zoned`, otherwise `LOCALTIMESTAMP(p)`.
    CurrentTimestamp {
        zoned: bool,
        precision: u8,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ForeignKey {
    pub columns: Vec<String>,
    pub references_table: String,
    pub references_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct SearchIndex {
    pub name: String,
    /// The indexed TEXT column.
    pub column: String,
    /// The analyzer name as `CREATE SEARCH INDEX ... WITH (analyzer = ...)`
    /// spells it, e.g. `cjk_ngram`.
    pub analyzer: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct GraphSchema {
    #[serde(default)]
    pub node_labels: Vec<String>,
    #[serde(default)]
    pub relationship_types: Vec<String>,
    #[serde(default)]
    pub tables: Vec<GraphTable>,
    #[serde(default)]
    pub properties: Vec<GraphProperty>,
    #[serde(default)]
    pub indexes: Vec<GraphIndex>,
    #[serde(default)]
    pub constraints: Vec<GraphConstraint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum GraphTableKind {
    Node,
    Relationship,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SchemaState {
    DeleteOnly,
    WriteOnly,
    Backfill,
    Validating,
    Public,
    Gc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct GraphTable {
    pub kind: GraphTableKind,
    pub name: String,
    pub state: SchemaState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct GraphProperty {
    pub table_kind: GraphTableKind,
    pub table: String,
    pub name: String,
    /// `any`, `bool`, `int`, `float`, `string`, `text`, `list` or `vector(n)`.
    #[serde(rename = "type")]
    pub property_type: String,
    pub nullable: bool,
    pub state: SchemaState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum GraphIndexKind {
    Equality,
    Range,
    FullText,
    Composite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct GraphIndex {
    pub label: String,
    pub properties: Vec<String>,
    pub kind: GraphIndexKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum GraphConstraintKind {
    NodeUnique,
    NodeExists,
    RelationshipUnique,
    RelationshipExists,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct GraphConstraint {
    pub kind: GraphConstraintKind,
    /// The node label or relationship type the constraint applies to.
    pub subject: String,
    pub property: String,
}
