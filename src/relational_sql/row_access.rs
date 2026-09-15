use crate::error::Result;
use crate::store::{GraphStore, RelationalTransactionRowView};
use skein_relational::row_runtime::RelationalRowStoreReader;
use skein_storage::RelationalRowPageSnapshotReader;

pub(crate) type RelationalRowReadMode<'a> =
    skein_relational::row_runtime::RelationalRowReadMode<'a, GraphStore>;

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
