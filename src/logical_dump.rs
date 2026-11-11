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

//! Logical export and import across persistent format changes.
//!
//! A logical dump is a directory of documented JSON files that is independent
//! of HawDB's physical storage format. Every later HawDB release reads every
//! dump format version up to its own, so an embedding host can carry a
//! database across a persistent format change: export with the old library,
//! import with the new one. See docs/LOGICAL_DUMP.md for the format and the
//! host recipe.
//!
//! Export reads one pinned read snapshot in bounded pages and publishes the
//! dump directory only after every file is written and synchronized. Import
//! builds the database in an unpublished sibling directory and renames it onto
//! the destination only after every file verified and every count matched; a
//! failed or interrupted import never leaves a database at the destination.

mod export;
mod files;
mod import;
mod model;
mod value;

use std::num::NonZeroUsize;
use std::path::Path;

use crate::api::{Database, DatabaseReadTransaction};
use crate::embedded::HawDBEmbedded;
use crate::{HawDBError, Result};

pub use hawdb_storage::persistent_format::PERSISTENT_FORMAT_VERSION;

/// The newest logical dump format version this release writes and reads.
/// Import accepts every version from 1 up to this one.
pub const LOGICAL_DUMP_FORMAT_VERSION: u16 = 1;

/// The engine-wide persistent format version of this build. Two releases with
/// the same value open each other's databases; a host upgrading across a
/// different value must export with the old release and import with the new.
pub const fn persistent_format_version() -> u32 {
    PERSISTENT_FORMAT_VERSION
}

/// Paging limits for export and import. The defaults bound memory to a few
/// pages of rows regardless of database size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalDumpOptions {
    /// Rows read per relational page, and graph records per import commit.
    /// Export halves a page that exceeds the read payload budget, down to a
    /// single row, before refusing.
    pub page_rows: NonZeroUsize,
    /// Records per data file before the next file starts.
    pub file_records: NonZeroUsize,
    /// Graph records imported between checkpoints, bounding resident WAL
    /// overlay memory during a large import.
    pub import_checkpoint_records: NonZeroUsize,
}

impl Default for LogicalDumpOptions {
    fn default() -> Self {
        Self {
            page_rows: NonZeroUsize::new(512).expect("nonzero"),
            file_records: NonZeroUsize::new(100_000).expect("nonzero"),
            import_checkpoint_records: NonZeroUsize::new(200_000).expect("nonzero"),
        }
    }
}

/// What an export wrote. Counts cover the exported branch only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDumpExportReport {
    pub dump_format_version: u16,
    pub persistent_format_version: u32,
    pub hawdb_version: String,
    /// Branch name when the database has a branch catalog.
    pub branch: Option<String>,
    /// Commit epoch of the pinned snapshot the dump reflects.
    pub commit_epoch: u64,
    pub table_count: u64,
    pub table_row_count: u64,
    pub node_count: u64,
    pub relationship_count: u64,
    /// Data files plus `schema.json` and `manifest.json`.
    pub file_count: u64,
    pub total_bytes: u64,
    /// Lowercase hex SHA-256 of `manifest.json`.
    pub manifest_sha256: String,
}

/// What an import verified and published.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalDumpImportReport {
    pub dump_format_version: u16,
    /// The HawDB release that wrote the dump.
    pub source_hawdb_version: String,
    pub source_persistent_format_version: u32,
    /// This build's persistent format version, now used by the destination.
    pub persistent_format_version: u32,
    pub table_count: u64,
    pub table_row_count: u64,
    pub node_count: u64,
    pub relationship_count: u64,
    pub file_count: u64,
    pub total_bytes: u64,
    pub manifest_sha256: String,
}

impl DatabaseReadTransaction {
    /// Writes this pinned snapshot as a logical dump directory at
    /// `destination`, which must not exist. Concurrent writers do not affect
    /// the dump: it reflects exactly [`DatabaseReadTransaction::commit_epoch`].
    pub fn export_logical_dump(
        &self,
        destination: impl AsRef<Path>,
        options: &LogicalDumpOptions,
    ) -> Result<LogicalDumpExportReport> {
        export::export(self, destination.as_ref(), options)
    }
}

impl Database {
    /// Exports the selected branch's current snapshot as a logical dump.
    /// See [`DatabaseReadTransaction::export_logical_dump`].
    pub fn export_logical_dump(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<LogicalDumpExportReport> {
        self.export_logical_dump_with_options(destination, &LogicalDumpOptions::default())
    }

    pub fn export_logical_dump_with_options(
        &self,
        destination: impl AsRef<Path>,
        options: &LogicalDumpOptions,
    ) -> Result<LogicalDumpExportReport> {
        self.begin_read_transaction()?
            .export_logical_dump(destination, options)
    }

    /// Builds a new database at `destination` (which must not exist) from the
    /// logical dump at `source`, and publishes it only on complete success.
    pub fn import_logical_dump(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<LogicalDumpImportReport> {
        Self::import_logical_dump_with_options(source, destination, &LogicalDumpOptions::default())
    }

    pub fn import_logical_dump_with_options(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        options: &LogicalDumpOptions,
    ) -> Result<LogicalDumpImportReport> {
        import::import(source.as_ref(), destination.as_ref(), options)
    }
}

impl HawDBEmbedded {
    /// Exports the current snapshot as a logical dump. Refused while this
    /// handle has an open transaction, whose uncommitted writes a dump could
    /// not include.
    pub fn export_logical_dump(
        &self,
        destination: impl AsRef<Path>,
    ) -> Result<LogicalDumpExportReport> {
        self.export_logical_dump_with_options(destination, &LogicalDumpOptions::default())
    }

    pub fn export_logical_dump_with_options(
        &self,
        destination: impl AsRef<Path>,
        options: &LogicalDumpOptions,
    ) -> Result<LogicalDumpExportReport> {
        if self.transaction_active() {
            return Err(HawDBError::Semantic(
                "logical dump export is refused while a transaction is open on this handle"
                    .to_string(),
            ));
        }
        self.database()
            .export_logical_dump_with_options(destination, options)
    }

    /// See [`Database::import_logical_dump`].
    pub fn import_logical_dump(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<LogicalDumpImportReport> {
        Database::import_logical_dump(source, destination)
    }
}
