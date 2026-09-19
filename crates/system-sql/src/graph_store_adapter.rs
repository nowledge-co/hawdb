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

//! System-SQL view of the graph store.
//!
//! This impl must live in the root crate: `hawdb-system-sql` reaches the store
//! through `hawdb-executor`, which already depends on `hawdb-storage`, so the
//! storage crate cannot name the trait without a dependency cycle.

use hawdb_core::schema::{Catalog, GraphStatistics};
use hawdb_storage::store::GraphStore;

impl crate::SystemSqlStore for GraphStore {
    fn commit_epoch(&self) -> u64 {
        GraphStore::commit_epoch(self)
    }

    fn append_storage_residency_report(&self) -> hawdb_storage::AppendStorageResidencyReport {
        GraphStore::append_storage_residency_report(self)
    }

    fn statistics(&self, catalog: &Catalog) -> GraphStatistics {
        GraphStore::statistics(self, catalog)
    }

    fn projected_graph_statuses(&self) -> Vec<hawdb_storage::ProjectedGraphStatus> {
        GraphStore::projected_graph_statuses(self)
    }

    fn search_projection_changefeed_status(
        &self,
    ) -> hawdb_storage::SearchProjectionChangefeedStatus {
        GraphStore::search_projection_changefeed_status(self)
    }
}
