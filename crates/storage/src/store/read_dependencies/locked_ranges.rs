// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Validate newly acquired relational locks against the original snapshot.
//! Locks prevent later changes; retained stamps detect changes before the grant,
//! including missing keys, tombstones and ABA. This does not refresh any data.

use super::*;
use crate::cow::CowPageWeight;
use crate::relational::{RelationalKey, RelationalScalarType, RelationalValue};
use crate::transaction_locks::{LockNamespace, LockRequest, LockTarget};
use std::ops::Bound;

const MAX_REQUESTS: usize = 4096;
const MAX_METADATA_BYTES: usize = 64 * 1024;
const MAX_STAMPS: usize = 65_536;

impl GraphStore {
    /// Internal coordinator contract: register the capture at transaction begin,
    /// acquire these locks, then validate under the canonical commit owner while
    /// retaining the locks through transaction completion. This is not a permit
    /// for an unlocked caller or validation of arbitrary snapshot reads.
    #[doc(hidden)]
    pub fn validate_locked_snapshot(
        &self,
        captured: &GraphReadDependencies,
        requests: &[LockRequest],
    ) -> std::result::Result<(), GraphReadDependencyError> {
        let result = self.validate_locked_snapshot_inner(captured, requests);
        if let Err(error) = &result {
            self.read_dependency_validation_failures.record(error);
        }
        result
    }

    fn validate_locked_snapshot_inner(
        &self,
        captured: &GraphReadDependencies,
        requests: &[LockRequest],
    ) -> std::result::Result<(), GraphReadDependencyError> {
        self.validate_read_dependencies_inner(captured)?;
        if self
            .version_index
            .history_reclaimed_after(captured.source_epoch)
        {
            return Err(GraphReadDependencyError::CaptureRefused);
        }
        if requests.len() > MAX_REQUESTS {
            return Err(GraphReadDependencyError::CaptureRefused);
        }
        let mut budget = ValidationBudget::default();
        for request in requests {
            match &request.target {
                LockTarget::RelationalTable { table } => {
                    self.validate_table(captured, table, &mut budget)?;
                }
                LockTarget::RelationalRange {
                    namespace: LockNamespace::RelationalIndex { table, columns },
                    lower,
                    upper,
                } => {
                    let schema = self
                        .relational_state
                        .table_schema(table)
                        .ok_or(GraphReadDependencyError::CaptureRefused)?;
                    let index = if columns == &schema.primary_key {
                        None
                    } else {
                        let Some(index) = schema.indexes.iter().find(|i| &i.columns == columns)
                        else {
                            // Constraint-only indexes have no payload-sensitive
                            // prefix summaries. A table dependency is safe.
                            self.validate_table(captured, table, &mut budget)?;
                            continue;
                        };
                        if index.columns.len() > 32
                            || index.columns.iter().any(|name| {
                                !schema.columns.iter().any(|column| {
                                    &column.name == name
                                        && !column.nullable
                                        && matches!(
                                            column.scalar_type,
                                            RelationalScalarType::Boolean
                                                | RelationalScalarType::BigInt
                                                | RelationalScalarType::Text
                                                | RelationalScalarType::Bytea
                                                | RelationalScalarType::Uuid
                                        )
                                })
                            })
                        {
                            self.validate_table(captured, table, &mut budget)?;
                            continue;
                        }
                        self.validate_lock_stamp(captured, VersionKey::ReadBarrier)?;
                        Some((index.name.as_str(), index.columns.len()))
                    };
                    self.validate_range(
                        captured,
                        VersionRange {
                            table,
                            index,
                            lower,
                            upper,
                        },
                        &mut budget,
                    )?;
                }
                // SQL fallbacks and graph footprints retain the epoch fence.
                _ if self.commit_epoch != captured.source_epoch => {
                    return Err(GraphReadDependencyError::Conflict(VersionConflict {
                        key: VersionKey::Database,
                        committed_epoch: self.commit_epoch,
                    }));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_table(
        &self,
        captured: &GraphReadDependencies,
        table: &str,
        budget: &mut ValidationBudget,
    ) -> std::result::Result<(), GraphReadDependencyError> {
        budget.metadata(std::mem::size_of::<VersionKey>().saturating_add(table.len()))?;
        self.validate_lock_stamp(captured, VersionKey::ReadBarrier)?;
        self.validate_lock_stamp(
            captured,
            VersionKey::ReadRelationalTableDomain(table.into()),
        )
    }

    fn validate_lock_stamp(
        &self,
        captured: &GraphReadDependencies,
        key: VersionKey,
    ) -> std::result::Result<(), GraphReadDependencyError> {
        if let Some(stamp) = self.version_index.stamp(&key)
            && stamp.commit_epoch > captured.source_epoch
        {
            return Err(GraphReadDependencyError::Conflict(VersionConflict {
                key,
                committed_epoch: stamp.commit_epoch,
            }));
        }
        Ok(())
    }

    fn validate_range(
        &self,
        captured: &GraphReadDependencies,
        range: VersionRange<'_>,
        budget: &mut ValidationBudget,
    ) -> std::result::Result<(), GraphReadDependencyError> {
        // Admit owned seek keys before cloning any variable-length payload.
        let bytes = std::mem::size_of::<VersionKey>()
            .saturating_add(range.table.len())
            .saturating_add(range.index.map_or(0, |(name, _)| name.len()))
            .saturating_add(1);
        budget.metadata(bytes.saturating_mul(2))?;
        if range.index.is_some() {
            budget.metadata(bytes)?;
            self.validate_lock_stamp(captured, range.key(RelationalKey(Vec::new())))?;
        }
        for bound in [range.lower, range.upper] {
            if let Bound::Included(key) | Bound::Excluded(key) = bound {
                budget.metadata(key.0.iter().fold(0usize, |size, value| {
                    size.saturating_add(std::mem::size_of::<RelationalValue>())
                        .saturating_add(value.estimated_payload_bytes())
                }))?;
            }
        }
        let first = range.key(match range.lower {
            Bound::Included(key) | Bound::Excluded(key) => key.clone(),
            Bound::Unbounded => RelationalKey(Vec::new()),
        });
        let last = match range.upper {
            Bound::Included(key) | Bound::Excluded(key) => range.key(key.clone()),
            Bound::Unbounded => {
                // A NUL-appended namespace sorts after all keys in this exact
                // namespace, without needing a maximum scalar sentinel.
                match range.index {
                    Some((name, _)) => VersionKey::ReadRelationalIndexPrefix {
                        table: range.table.into(),
                        index: format!("{name}\0"),
                        prefix: RelationalKey(Vec::new()),
                    },
                    None => VersionKey::RelationalRow {
                        table: format!("{}\0", range.table),
                        primary_key: RelationalKey(Vec::new()),
                    },
                }
            }
        };
        if first > last {
            return Ok(());
        }
        for (key, stamp) in self.version_index.stamps_in_range(&first, &last) {
            budget.visit()?;
            let Some(value) = range.value(key) else {
                continue;
            };
            if !inside(value, range.lower, range.upper) {
                continue;
            }
            if stamp.commit_epoch > captured.source_epoch {
                budget.metadata(key.cow_page_bytes())?;
                return Err(GraphReadDependencyError::Conflict(VersionConflict {
                    key: key.clone(),
                    committed_epoch: stamp.commit_epoch,
                }));
            }
        }
        Ok(())
    }
}

#[derive(Default)]
struct ValidationBudget {
    bytes: usize,
    stamps: usize,
}

impl ValidationBudget {
    fn metadata(&mut self, bytes: usize) -> std::result::Result<(), GraphReadDependencyError> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > MAX_METADATA_BYTES {
            return Err(GraphReadDependencyError::CaptureRefused);
        }
        Ok(())
    }

    fn visit(&mut self) -> std::result::Result<(), GraphReadDependencyError> {
        self.stamps += 1;
        if self.stamps > MAX_STAMPS {
            return Err(GraphReadDependencyError::CaptureRefused);
        }
        Ok(())
    }
}

struct VersionRange<'a> {
    table: &'a str,
    index: Option<(&'a str, usize)>,
    lower: &'a Bound<RelationalKey>,
    upper: &'a Bound<RelationalKey>,
}

impl VersionRange<'_> {
    fn key(&self, key: RelationalKey) -> VersionKey {
        match self.index {
            Some((name, _)) => VersionKey::ReadRelationalIndexPrefix {
                table: self.table.into(),
                index: name.into(),
                prefix: key,
            },
            None => VersionKey::RelationalRow {
                table: self.table.into(),
                primary_key: key,
            },
        }
    }

    fn value<'a>(&self, key: &'a VersionKey) -> Option<&'a RelationalKey> {
        match (self.index, key) {
            (None, VersionKey::RelationalRow { table, primary_key }) if table == self.table => {
                Some(primary_key)
            }
            (
                Some((name, width)),
                VersionKey::ReadRelationalIndexPrefix {
                    table,
                    index,
                    prefix,
                },
            ) if table == self.table && index == name && prefix.0.len() == width => Some(prefix),
            // Short summaries cover rows on both sides of a range boundary;
            // only complete index keys prove membership in this interval.
            _ => None,
        }
    }
}

fn inside(key: &RelationalKey, lower: &Bound<RelationalKey>, upper: &Bound<RelationalKey>) -> bool {
    (match lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    }) && match upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    }
}

#[cfg(test)]
#[path = "locked_ranges/tests.rs"]
mod tests;
