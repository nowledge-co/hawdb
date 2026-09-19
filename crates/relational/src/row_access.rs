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

use crate::row_runtime::RelationalRowStoreReader;
use hawdb_core::error::Result;
use hawdb_storage::store::{GraphStore, RelationalTransactionRowView};
use hawdb_storage::RelationalRowPageSnapshotReader;

#[doc(hidden)]
pub type RelationalRowReadMode<'a> =
    crate::row_runtime::RelationalRowReadMode<'a, GraphStore>;

impl RelationalRowStoreReader for GraphStore {
    type TransactionRows = RelationalTransactionRowView;

    fn open_relational_row_snapshot_reader(
        &self,
    ) -> Result<Option<RelationalRowPageSnapshotReader>> {
        GraphStore::open_relational_row_snapshot_reader(self)
    }

    fn open_relational_transaction_row_snapshot_reader(
        &self,
        rows: &Self::TransactionRows,
    ) -> Result<RelationalRowPageSnapshotReader> {
        GraphStore::open_relational_transaction_row_snapshot_reader(self, rows)
    }
}
