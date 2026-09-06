use super::{
    RelationalRowPagePhysicalGeneration, RelationalRowPagePublicationError,
    RelationalRowPageRootReader,
};
use skein_core::RuntimeTaskContext;
use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageRewriteConfig {
    pub max_live_ratio_percent: u8,
    pub max_scan_pages: NonZeroU64,
    pub max_rewrite_bytes: NonZeroU64,
}

impl Default for RelationalRowPageRewriteConfig {
    fn default() -> Self {
        Self {
            max_live_ratio_percent: 50,
            max_scan_pages: NonZeroU64::new(1_000_000).unwrap(),
            max_rewrite_bytes: NonZeroU64::new(128 * 1024 * 1024 * 1024).unwrap(),
        }
    }
}

impl RelationalRowPageRewriteConfig {
    pub fn validate(self) -> Result<(), RelationalRowPagePublicationError> {
        if !(1..=100).contains(&self.max_live_ratio_percent) {
            return Err(RelationalRowPagePublicationError::Admission(
                "row-page rewrite live ratio must be in 1..=100 percent".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) struct RowPageRewriteControls<'a> {
    pub config: RelationalRowPageRewriteConfig,
    pub task: &'a RuntimeTaskContext,
}

impl RowPageRewriteControls<'_> {
    pub(super) fn validate(
        self,
        base: Option<&RelationalRowPageRootReader>,
    ) -> Result<(), RelationalRowPagePublicationError> {
        self.checkpoint()?;
        self.config.validate()?;
        if base
            .is_some_and(|base| base.manifest().root_page_count > self.config.max_scan_pages.get())
        {
            return Err(RelationalRowPagePublicationError::Admission(
                "row-page rewrite exceeds its descriptor scan limit".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn checkpoint(self) -> Result<(), RelationalRowPagePublicationError> {
        self.task.checkpoint().map_err(|reason| {
            RelationalRowPagePublicationError::Admission(format!(
                "row-page rewrite stopped: {reason}"
            ))
        })
    }

    pub(super) fn selects(self, entry: &RelationalRowPagePhysicalGeneration) -> bool {
        u128::from(entry.live_pages) * 100
            <= u128::from(entry.allocated_pages) * u128::from(self.config.max_live_ratio_percent)
    }
}
