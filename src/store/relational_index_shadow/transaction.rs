use super::{GraphStore, RelationalIndexReadLimits, RelationalTransactionIndexView};
use std::sync::Arc;

impl GraphStore {
    pub(crate) fn begin_authoritative_relational_transaction_index(
        &self,
    ) -> crate::Result<Option<RelationalTransactionIndexView>> {
        if !self
            .relational_index_shadow
            .mode
            .requires_authoritative_indexes()
        {
            return Ok(None);
        }
        self.validate_authoritative_relational_index_open()?;
        let view = Arc::clone(
            self.relational_index_shadow
                .current_read_view(self.commit_epoch)
                .expect("validated authoritative view must remain current"),
        );
        Ok(Some(RelationalTransactionIndexView::new(
            view,
            self.relational_index_shadow.live_limits,
            RelationalIndexReadLimits::default(),
        )))
    }
}
