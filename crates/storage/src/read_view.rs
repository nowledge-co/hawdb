use crate::ManifestGeneration;

/// The immutable identity of one published database read.
///
/// The logical commit epoch controls visibility. The physical generation only
/// identifies the checkpoint base beneath that logical snapshot; later commits
/// may be represented by an immutable delta without changing the base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedReadView {
    visible_commit_epoch: u64,
    checkpoint_commit_epoch: Option<u64>,
    physical_generation: Option<ManifestGeneration>,
}

impl PublishedReadView {
    #[doc(hidden)]
    pub fn new(
        visible_commit_epoch: u64,
        checkpoint_commit_epoch: Option<u64>,
        physical_generation: Option<ManifestGeneration>,
    ) -> Self {
        debug_assert_eq!(
            checkpoint_commit_epoch.is_some(),
            physical_generation.is_some(),
            "checkpoint epoch and physical generation must be published together"
        );
        debug_assert!(
            checkpoint_commit_epoch.is_none_or(|epoch| epoch <= visible_commit_epoch),
            "checkpoint commit epoch must not exceed the visible commit epoch"
        );
        Self {
            visible_commit_epoch,
            checkpoint_commit_epoch,
            physical_generation,
        }
    }

    pub const fn visible_commit_epoch(self) -> u64 {
        self.visible_commit_epoch
    }

    pub const fn checkpoint_commit_epoch(self) -> Option<u64> {
        self.checkpoint_commit_epoch
    }

    pub const fn physical_generation(self) -> Option<ManifestGeneration> {
        self.physical_generation
    }

    pub const fn physical_base_is_current(self) -> bool {
        matches!(
            self.checkpoint_commit_epoch,
            Some(checkpoint_commit_epoch) if checkpoint_commit_epoch == self.visible_commit_epoch
        )
    }

    pub const fn has_delta_after_physical_generation(self) -> bool {
        matches!(
            self.checkpoint_commit_epoch,
            Some(checkpoint_commit_epoch) if checkpoint_commit_epoch < self.visible_commit_epoch
        )
    }
}
