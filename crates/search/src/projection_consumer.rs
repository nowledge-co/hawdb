use crate::{SearchIndex, SearchProjectionCatchUpReport};
use skein_core::{Result, SkeinError};
use skein_storage::{SearchProjectionChangefeedReadiness, SearchProjectionChangefeedStatus};
use std::fmt;
use std::num::NonZeroU64;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SearchProjectionConsumerId(String);

impl SearchProjectionConsumerId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        {
            return Err(SkeinError::Semantic(
                "consumer ID must contain 1-128 ASCII letters, digits, '.', '_' or '-'".into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchProjectionConsumerOptions {
    max_idle_commits: NonZeroU64,
}

impl SearchProjectionConsumerOptions {
    pub fn new(max_idle_commits: NonZeroU64) -> Self {
        Self { max_idle_commits }
    }
    pub fn max_idle_commits(&self) -> NonZeroU64 {
        self.max_idle_commits
    }
}

/// Exclusive owner of a registered projection. Dropping it releases its lease,
/// while its durable registration remains until explicitly unregistered.
#[derive(Debug)]
pub struct SearchProjectionConsumer {
    id: SearchProjectionConsumerId,
    projection: crate::consumer::ConsumerProjection,
}

impl SearchProjectionConsumer {
    #[doc(hidden)]
    pub fn from_projection(
        id: SearchProjectionConsumerId,
        projection: crate::consumer::ConsumerProjection,
    ) -> Self {
        Self { id, projection }
    }

    pub fn id(&self) -> &SearchProjectionConsumerId {
        &self.id
    }

    #[doc(hidden)]
    pub fn projection(&self) -> &crate::consumer::ConsumerProjection {
        &self.projection
    }

    #[doc(hidden)]
    pub fn projection_mut(&mut self) -> &mut crate::consumer::ConsumerProjection {
        &mut self.projection
    }

    pub fn search_index(&self) -> &SearchIndex {
        self.projection.index()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchProjectionConsumerState {
    Unverified,
    Active,
    RebuildRequired(SearchProjectionConsumerRebuildReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchProjectionConsumerRebuildReason {
    DatabaseIdentityMismatch,
    ProjectionIdentityMismatch,
    CheckpointMismatch,
    SourceRewound,
    RetentionLimitExceeded { resume_floor_commit_epoch: u64 },
    Expired { expires_at_commit_epoch: u64 },
    UntrackedProjectionMutation,
    RegistryUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionConsumerStatus {
    pub consumer_id: SearchProjectionConsumerId,
    pub state: SearchProjectionConsumerState,
    pub durable_complete_through_commit_epoch: Option<u64>,
    pub expires_at_commit_epoch: u64,
    pub minimum_valid_consumer_commit_epoch: Option<u64>,
    pub changefeed: SearchProjectionChangefeedStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionConsumerReadiness {
    pub consumer: SearchProjectionConsumerStatus,
    pub changefeed: SearchProjectionChangefeedReadiness,
}

impl SearchProjectionConsumerReadiness {
    pub fn is_ready(&self) -> bool {
        self.consumer.state == SearchProjectionConsumerState::Active && self.changefeed.ready
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchProjectionConsumerCatchUpReport {
    pub catch_up: SearchProjectionCatchUpReport,
    pub consumer: SearchProjectionConsumerStatus,
}

#[derive(Debug)]
pub enum SearchProjectionConsumerError {
    Database(SkeinError),
    InvalidHandle,
    AlreadyRegistered,
    RegistryFull,
    SourceNotDurable,
    RebuildRequired(SearchProjectionConsumerRebuildReason),
}

impl fmt::Display for SearchProjectionConsumerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => error.fmt(formatter),
            Self::InvalidHandle => formatter.write_str("invalid search projection consumer handle"),
            Self::AlreadyRegistered => {
                formatter.write_str("search projection consumer is already registered")
            }
            Self::RegistryFull => {
                formatter.write_str("search projection consumer registry is full")
            }
            Self::SourceNotDurable => formatter.write_str(
                "search projection consumer requires a durable source outside a WAL sync group",
            ),
            Self::RebuildRequired(reason) => write!(
                formatter,
                "search projection consumer requires rebuild: {reason:?}"
            ),
        }
    }
}

impl std::error::Error for SearchProjectionConsumerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SkeinError> for SearchProjectionConsumerError {
    fn from(error: SkeinError) -> Self {
        Self::Database(error)
    }
}

pub type SearchProjectionConsumerResult<T> = std::result::Result<T, SearchProjectionConsumerError>;

mod registry;

#[doc(hidden)]
pub use registry::{ConsumerRegistry, Record, MAX_CONSUMERS};

#[cfg(test)]
mod tests {
    use super::{SearchProjectionConsumerId, SearchProjectionConsumerOptions};
    use std::num::NonZeroU64;

    #[test]
    fn consumer_contract_keeps_identifier_and_lease_bounds() {
        assert!(SearchProjectionConsumerId::new("consumer_A-1.v2").is_ok());
        assert!(SearchProjectionConsumerId::new("bad/id").is_err());
        assert!(SearchProjectionConsumerId::new("a".repeat(128)).is_ok());
        assert!(SearchProjectionConsumerId::new("a".repeat(129)).is_err());

        let options = SearchProjectionConsumerOptions::new(NonZeroU64::new(7).unwrap());
        assert_eq!(options.max_idle_commits().get(), 7);
    }
}
