//! One query's shared working/result capacity, bounded by its admitted task.

use crate::{Result, RuntimeTaskContext, SkeinError};
use skein_executor::{QueryMemoryAccount, QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger};
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::Deref;

#[derive(Debug, Default)]
pub(crate) struct AdmittedScores {
    scores: BTreeMap<String, f64>,
    // The data drops before the charge, even when its consumer returns early.
    _memory: Option<QueryMemoryLease>,
}

impl AdmittedScores {
    pub(crate) fn new(scores: BTreeMap<String, f64>, memory: QueryMemoryLease) -> Self {
        Self {
            scores,
            _memory: Some(memory),
        }
    }
}

impl Deref for AdmittedScores {
    type Target = BTreeMap<String, f64>;

    fn deref(&self) -> &Self::Target {
        &self.scores
    }
}

#[cfg(test)]
impl PartialEq<BTreeMap<String, f64>> for AdmittedScores {
    fn eq(&self, other: &BTreeMap<String, f64>) -> bool {
        self.scores == *other
    }
}

#[cfg(test)]
impl PartialEq for AdmittedScores {
    fn eq(&self, other: &Self) -> bool {
        self.scores == other.scores
    }
}

#[derive(Debug)]
pub(crate) struct QueryMemory {
    #[cfg(test)]
    pub(crate) ledger: QueryMemoryLedger,
    pub(crate) working: QueryMemoryAccount,
    pub(crate) scores: QueryMemoryAccount,
    pub(crate) limit: u64,
    pub(crate) result_limit: u64,
}

impl QueryMemory {
    pub(crate) fn new(configured: NonZeroU64, task: Option<&RuntimeTaskContext>) -> Result<Self> {
        let reservation = task.and_then(RuntimeTaskContext::memory_reservation);
        let limit = reservation.map_or(configured.get(), |value| {
            configured.get().min(value.memory_bytes())
        });
        let result_limit = reservation.map_or(limit, |value| limit.min(value.result_bytes()));
        let addressable = usize::try_from(limit).unwrap_or(usize::MAX);
        // Zero-byte reservations must fail closed, not become an unlimited root.
        let budget = NonZeroUsize::new(addressable).ok_or_else(|| {
            SkeinError::Execution("search query has no admitted working memory".to_string())
        })?;
        let ledger = QueryMemoryLedger::new(budget);
        let working = ledger.account(
            QueryMemoryClass::ExternalRead,
            "search query streams",
            budget,
        );
        let scores = ledger.account(
            QueryMemoryClass::ResultMaterialization,
            "search query scores",
            NonZeroUsize::new(usize::try_from(result_limit).unwrap_or(usize::MAX))
                .unwrap_or(NonZeroUsize::MIN),
        );
        Ok(Self {
            #[cfg(test)]
            ledger,
            working,
            scores,
            limit: addressable as u64,
            result_limit,
        })
    }
}
