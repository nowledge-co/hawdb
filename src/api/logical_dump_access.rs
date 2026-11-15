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

//! Narrow crate-internal access used by `crate::logical_dump`.
//!
//! The logical dump reads one pinned read transaction and writes a fresh,
//! unpublished import target. It needs the snapshot's catalog and store, and
//! an id-preserving graph batch import on the target; nothing else.

use super::{Database, DatabaseReadTransaction, Result};
use crate::schema::Catalog;
use crate::store::{GraphSnapshotNodeImport, GraphSnapshotRelationshipImport, GraphStore};

impl DatabaseReadTransaction {
    /// The pinned catalog and store of this read transaction.
    pub(crate) fn logical_dump_source(&self) -> (&Catalog, &GraphStore) {
        (&self.catalog, &self.store)
    }

    /// The configured per-statement read payload budget.
    pub(crate) fn logical_dump_payload_budget(&self) -> Option<usize> {
        self.config.max_read_result_payload_bytes
    }

    /// Name of the branch this transaction reads, when the database has a
    /// branch catalog.
    pub(crate) fn logical_dump_branch_name(&self) -> Option<String> {
        self.current_branch
            .as_ref()
            .map(|branch| branch.info.name.clone())
    }
}

impl Database {
    /// Commits one bounded batch of graph rows with their original ids.
    /// See `GraphStore::import_graph_rows_batch`.
    pub(crate) fn logical_dump_import_graph_batch(
        &mut self,
        nodes: Vec<GraphSnapshotNodeImport>,
        relationships: Vec<GraphSnapshotRelationshipImport>,
    ) -> Result<()> {
        self.ensure_writable()?;
        let mut branch_runtime_access = self.runtime.get_mut()?;
        let branch_runtime = &mut *branch_runtime_access;
        branch_runtime.store.import_graph_rows_batch(
            &mut branch_runtime.catalog,
            nodes,
            relationships,
        )
    }

    /// Node and relationship counts, and the user-visible relational tables
    /// of the current head.
    pub(crate) fn logical_dump_target_state(&self) -> Result<(u64, u64, Vec<String>)> {
        let runtime = self.runtime.get()?;
        let statistics = runtime.store.basic_statistics();
        let tables = runtime
            .store
            .relational_state()
            .table_schemas()
            .map(|schema| schema.name.clone())
            .collect();
        Ok((statistics.node_count, statistics.relationship_count, tables))
    }
}
