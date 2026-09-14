use crate::store::{GraphStore, RelationalIndexProbeStatistics};
use skein_relational::index_runtime::RelationalIndexStoreReader;
use skein_storage::relational_index_view::RelationalIndexReadViewReport;
use skein_storage::{RelationalIndexReadLimits, RelationalIndexShadowError, RelationalKey};

pub(crate) use skein_relational::index_runtime::RelationalIndexExecutionEvidence;

pub(crate) type RelationalIndexReadMode<'a> =
    skein_relational::index_runtime::RelationalIndexReadMode<'a, GraphStore>;
pub(crate) type RelationalIndexRuntime<'a> =
    skein_relational::index_runtime::RelationalIndexRuntime<'a, GraphStore>;

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
        scan: &skein_storage::RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&RelationalKey, &RelationalKey) -> bool,
    ) -> Option<std::result::Result<RelationalIndexReadViewReport, RelationalIndexShadowError>>
    {
        GraphStore::visit_relational_index_read_view_range_entries(
            self, table, index, scan, limits, visit,
        )
    }
}
