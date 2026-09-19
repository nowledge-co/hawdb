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

//! Inward read contract for the graph engine.
//!
//! The embedded store still owns the concrete `GraphStore`; this module names
//! the read-only observability surface the facade is allowed to depend on so it
//! can be generic over the engine instead of the concrete store.

use crate::{
    AppendTableSchema, PublishedReadView, SegmentCacheSnapshot, StorageRecoveryReport,
    StorageResidencyReport,
};
use hawdb_core::{BasicGraphStatistics, Catalog, GraphStatistics};

/// Read-only observability surface consumed by the embedded facade.
pub trait GraphReadEngine {
    fn storage_version(&self) -> &'static str;

    fn commit_epoch(&self) -> u64;

    fn storage_handle_poisoned(&self) -> bool;

    fn published_read_view(&self) -> PublishedReadView;

    fn basic_statistics(&self) -> BasicGraphStatistics;

    fn statistics(&self, catalog: &Catalog) -> GraphStatistics;

    fn storage_recovery_report(&self) -> StorageRecoveryReport;

    fn segment_cache_snapshot(&self) -> Option<SegmentCacheSnapshot>;

    fn storage_residency_report(&self) -> StorageResidencyReport;

    fn append_table_schema(&self, table: &str) -> Option<&AppendTableSchema>;

    fn initial_import_source_fingerprint(&self) -> Option<&str>;
}
