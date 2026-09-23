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

use crate::index_runtime::RelationalIndexStoreReader;
use hawdb_storage::relational_index_view::RelationalIndexReadViewReport;
use hawdb_storage::store::{GraphStore, RelationalIndexProbeStatistics};
use hawdb_storage::{RelationalIndexReadLimits, RelationalIndexShadowError, RelationalKey};

#[doc(hidden)]
pub type RelationalIndexReadMode<'a> =
    crate::index_runtime::RelationalIndexReadMode<'a, GraphStore>;

impl RelationalIndexStoreReader for GraphStore {
    fn relational_index_probe_statistics(
        &self,
        table: &str,
        index: &str,
        prefix_len: usize,
    ) -> Option<RelationalIndexProbeStatistics> {
        GraphStore::relational_index_probe_statistics(self, table, index, prefix_len)
    }

    fn visit_relational_index_read_view_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        GraphStore::visit_relational_index_read_view_prefix_entries(
            self, table, index, prefix, limits, visit,
        )
    }

    fn visit_relational_index_read_view_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        GraphStore::visit_relational_index_read_view_prefix_entries_many(
            self, table, index, prefixes, limits, visit,
        )
    }

    fn visit_relational_index_read_view_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &hawdb_storage::RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        GraphStore::visit_relational_index_read_view_range_entries(
            self, table, index, scan, limits, visit,
        )
    }
}
