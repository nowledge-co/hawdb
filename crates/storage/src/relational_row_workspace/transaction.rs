use crate::{
    RelationalError, RelationalRowChangeCapture, RelationalRowChangeCaptureLimits,
    RelationalRowPageLiveError, RelationalRowPageReadView,
};
use std::sync::Arc;

/// A bounded transaction-private row overlay pinned to the committed row view.
///
/// Successful statements append immutable row-change batches. Failed
/// statements stage a replacement view first and therefore leave this view
/// unchanged. The private visible epoch is only an ordering token; it is never
/// published as a database commit epoch.
#[derive(Debug)]
pub struct RelationalTransactionRowView {
    view: Arc<RelationalRowPageReadView>,
    limits: RelationalRowChangeCaptureLimits,
}

impl RelationalTransactionRowView {
    pub fn new(
        view: Arc<RelationalRowPageReadView>,
        limits: RelationalRowChangeCaptureLimits,
    ) -> Self {
        Self { view, limits }
    }

    pub fn read_view(&self) -> &Arc<RelationalRowPageReadView> {
        &self.view
    }

    pub fn stage_advance(
        &self,
        capture: RelationalRowChangeCapture,
    ) -> Result<Self, RelationalError> {
        let next_epoch = self
            .view
            .identity()
            .visible_commit_epoch
            .checked_add(1)
            .ok_or_else(|| {
                RelationalError::Admission(
                    "transaction-private row overlay epoch overflow".to_string(),
                )
            })?;
        let view = self
            .view
            .advance(next_epoch, Some(capture), self.limits)
            .map_err(map_transaction_row_live_error)?;
        Ok(Self {
            view: Arc::new(view),
            limits: self.limits,
        })
    }
}

fn map_transaction_row_live_error(error: RelationalRowPageLiveError) -> RelationalError {
    match error {
        RelationalRowPageLiveError::Admission(message)
        | RelationalRowPageLiveError::Invalidated(message) => RelationalError::Admission(message),
        RelationalRowPageLiveError::RequiresCheckpoint { tables } => {
            RelationalError::Admission(format!(
                "transaction-private row overlay requires a canonical checkpoint for tables {}",
                tables.join(",")
            ))
        }
        RelationalRowPageLiveError::Corrupt(message) => RelationalError::Corruption(message),
    }
}
