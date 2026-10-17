// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Versioned source facts, not host mapping or publication authority.

use super::SearchProjectionGraphChange;
use serde::{Deserialize, Serialize};

/// Only identity-dependent changes need a preimage. Other changes are hydrated
/// from the one pinned end snapshot. Unsupported IDs must never be coerced.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum OwnerBeforeV1 {
    NotRecorded,
    Absent,
    Present { labels: Vec<u32>, id: OwnerIdV1 },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum OwnerIdV1 {
    Absent,
    String(String),
    Unsupported,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnerEffectV1 {
    pub node_id: u64,
    pub before: OwnerBeforeV1,
}
impl OwnerEffectV1 {
    pub fn estimated_retained_bytes(&self) -> usize {
        match &self.before {
            OwnerBeforeV1::Present { labels, id } => labels
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>())
                .saturating_add(match id {
                    OwnerIdV1::String(id) => id.capacity(),
                    _ => 0,
                }),
            _ => 0,
        }
    }
}

/// The payload encoding remains V1; checkpoint capture contract V2 additionally
/// guarantees all relationship-effect endpoints. None means an unqualified
/// record. An empty set is meaningful for relational-only commits. Deleted
/// physical nodes are never legacy upserts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedProjectionChange {
    pub legacy: std::sync::Arc<SearchProjectionGraphChange>,
    pub owners_v1: Option<Vec<OwnerEffectV1>>,
}
impl std::ops::Deref for CapturedProjectionChange {
    type Target = SearchProjectionGraphChange;
    fn deref(&self) -> &Self::Target {
        &self.legacy
    }
}
impl std::ops::DerefMut for CapturedProjectionChange {
    fn deref_mut(&mut self) -> &mut Self::Target {
        std::sync::Arc::make_mut(&mut self.legacy)
    }
}
impl CapturedProjectionChange {
    pub fn estimated_retained_bytes(&self) -> usize {
        self.legacy
            .estimated_retained_bytes()
            .saturating_add(std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>())
            .saturating_add(self.owners_v1.as_ref().map_or(0, |owners| {
                owners
                    .capacity()
                    .saturating_mul(std::mem::size_of::<OwnerEffectV1>())
                    .saturating_add(
                        owners
                            .iter()
                            .map(OwnerEffectV1::estimated_retained_bytes)
                            .fold(0, usize::saturating_add),
                    )
            }))
    }
}
