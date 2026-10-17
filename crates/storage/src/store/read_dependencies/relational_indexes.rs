// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Reuse complete staged captures, not WAL destination guesses. Index membership
//! alone omits payload-only updates; new row capture closes that gap.
use super::*;
use crate::relational::{
    RelationalIndexChangeCapture, RelationalRowChangeCapture, RelationalState, RelationalValue,
};
use crate::version::{relational_index_prefix_read_key, relational_index_prefix_read_key_bytes};

const MAX_WORK: usize = 65536;
const MAX_METADATA_BYTES: usize = 64 * 1024;

struct Budget {
    work: usize,
    bytes: usize,
}
impl Budget {
    fn visit(&mut self) -> bool {
        self.work += 1;
        self.work <= MAX_WORK
    }
    fn prefix<'a>(
        &mut self,
        table: &str,
        index: &str,
        values: impl Iterator<Item = &'a RelationalValue> + Clone,
        stamps: &mut VersionWriteSet,
    ) -> Result<bool> {
        if !self.visit() {
            return Ok(false);
        }
        // A reference's actual equality value is unknown, even when the SQL
        // collector can spell its small hydrated value. Refuse that inference.
        if values
            .clone()
            .any(|v| matches!(v, RelationalValue::Overflow(_)))
        {
            return Ok(false);
        }
        let Some(bytes) = relational_index_prefix_read_key_bytes(table, index, values.clone())
        else {
            return Ok(true);
        };
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > MAX_METADATA_BYTES {
            return Ok(false);
        }
        record_stamp(
            stamps,
            relational_index_prefix_read_key(table, index, values)
                .expect("preflighted index prefix"),
        )?;
        Ok(true)
    }
}
impl GraphStore {
    pub(in crate::store) fn extend_relational_read_invalidation_stamps(
        &self,
        state: Option<&RelationalState>,
        indexes: Option<&RelationalIndexChangeCapture>,
        rows: Option<&RelationalRowChangeCapture>,
        stamps: &mut VersionWriteSet,
    ) -> Result<()> {
        if !self.tracks_read_dependencies()
            || state.is_none()
            || stamps.contains_key(&VersionKey::Database)
        {
            return Ok(());
        }
        let state = state.expect("checked staged state");
        let (
            Some(RelationalIndexChangeCapture::Captured {
                changes: indexes, ..
            }),
            Some(RelationalRowChangeCapture::Captured { changes: rows, .. }),
        ) = (indexes, rows)
        else {
            return record_stamp(stamps, VersionKey::ReadBarrier);
        };
        let mut budget = Budget { work: 0, bytes: 0 };
        for change in indexes {
            if !budget.visit() {
                return record_stamp(stamps, VersionKey::ReadBarrier);
            }
            let Some(schema) = state.table_schema(&change.table) else {
                return record_stamp(stamps, VersionKey::ReadBarrier);
            };
            // Constraint-support and primary indexes are not this read family.
            for index in &schema.indexes {
                if !budget.visit() {
                    return record_stamp(stamps, VersionKey::ReadBarrier);
                }
                if index.name != change.index {
                    continue;
                }
                if change.index_key.0.len() > 32 {
                    return record_stamp(stamps, VersionKey::ReadBarrier);
                }
                for end in 1..=change.index_key.0.len() {
                    if !budget.prefix(
                        &change.table,
                        &index.name,
                        change.index_key.0[..end].iter(),
                        stamps,
                    )? {
                        return record_stamp(stamps, VersionKey::ReadBarrier);
                    }
                }
            }
        }
        let mut previous_table = None;
        for change in rows {
            if !budget.visit() {
                return record_stamp(stamps, VersionKey::ReadBarrier);
            }
            // Row capture includes tombstones. Stamp before skipping their
            // absent payload; index membership alone misses payload-only writes.
            // Only the last borrowed name is retained. Sorted/repeated rows
            // avoid another clone, but correctness does not require sorting.
            if previous_table != Some(change.table.as_str()) {
                budget.bytes = budget.bytes.saturating_add(
                    std::mem::size_of::<VersionKey>().saturating_add(change.table.len()),
                );
                if budget.bytes > MAX_METADATA_BYTES {
                    return record_stamp(stamps, VersionKey::ReadBarrier);
                }
                record_stamp(
                    stamps,
                    VersionKey::ReadRelationalTableDomain(change.table.clone()),
                )?;
                previous_table = Some(change.table.as_str());
            }
            let Some(row) = &change.row else {
                continue;
            };
            let Some(schema) = state.table_schema(&change.table) else {
                return record_stamp(stamps, VersionKey::ReadBarrier);
            };
            for index in &schema.indexes {
                if !budget.visit() || index.columns.len() > 32 {
                    return record_stamp(stamps, VersionKey::ReadBarrier);
                }
                // Fixed borrowed directory; never clone unused body values.
                let mut prefix = [None; 32];
                for (ordinal, name) in index.columns.iter().enumerate() {
                    if !budget.visit() {
                        return record_stamp(stamps, VersionKey::ReadBarrier);
                    }
                    let mut position = None;
                    for (candidate, column) in schema.columns.iter().enumerate() {
                        if !budget.visit() {
                            return record_stamp(stamps, VersionKey::ReadBarrier);
                        }
                        if &column.name == name {
                            position = Some(candidate);
                            break;
                        }
                    }
                    let Some(position) = position else {
                        return record_stamp(stamps, VersionKey::ReadBarrier);
                    };
                    let Some(value) = row.values().get(position) else {
                        return record_stamp(stamps, VersionKey::ReadBarrier);
                    };
                    prefix[ordinal] = Some(value);
                }
                for end in 1..=index.columns.len() {
                    if !budget.prefix(
                        &change.table,
                        &index.name,
                        prefix[..end].iter().flatten().copied(),
                        stamps,
                    )? {
                        return record_stamp(stamps, VersionKey::ReadBarrier);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::MutationLimits;
    use crate::relational::{
        RelationalColumnSchema, RelationalIndexSchema, RelationalInsertMode, RelationalKey,
        RelationalRow, RelationalScalarType, RelationalTableSchema, RelationalTransaction,
        RelationalWrite,
    };
    fn setup(store: &mut GraphStore, catalog: &mut Catalog) {
        store
            .commit_relational_transaction(
                catalog,
                RelationalTransaction {
                    writes: vec![RelationalWrite::CreateTable(RelationalTableSchema {
                        name: "rows".into(),
                        columns: vec![
                            RelationalColumnSchema {
                                name: "id".into(),
                                scalar_type: RelationalScalarType::BigInt,
                                nullable: false,
                                default: None,
                            },
                            RelationalColumnSchema {
                                name: "group".into(),
                                scalar_type: RelationalScalarType::Text,
                                nullable: false,
                                default: None,
                            },
                            RelationalColumnSchema {
                                name: "body".into(),
                                scalar_type: RelationalScalarType::Text,
                                nullable: false,
                                default: None,
                            },
                        ],
                        primary_key: vec!["id".into()],
                        unique_constraints: vec![],
                        foreign_keys: vec![],
                        indexes: vec![RelationalIndexSchema {
                            name: "group_index".into(),
                            columns: vec!["group".into(), "id".into()],
                            unique: false,
                        }],
                    })],
                },
            )
            .unwrap();
    }
    fn insert(ids: impl Iterator<Item = i64>, group: &str) -> RelationalTransaction {
        RelationalTransaction {
            writes: vec![RelationalWrite::Insert {
                table: "rows".into(),
                rows: ids
                    .map(|id| {
                        RelationalRow::new(vec![
                            RelationalValue::BigInt(id),
                            RelationalValue::Text(group.into()),
                            RelationalValue::Text("body".into()),
                        ])
                    })
                    .collect(),
                mode: RelationalInsertMode::Error,
            }],
        }
    }
    fn capture(store: &GraphStore, group: &str) -> GraphReadDependencies {
        let mut read = store.capture_read_dependencies(32, 8192).unwrap();
        read.record(
            relational_index_prefix_read_key(
                "rows",
                "group_index",
                [RelationalValue::Text(group.into())].iter(),
            )
            .unwrap(),
        )
        .unwrap();
        read
    }
    #[test]
    fn same_prefix_disjoint_writers_commit_without_shared_writer_intents() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        setup(&mut store, &mut catalog);
        let mut table = store.capture_read_dependencies(32, 8192).unwrap();
        table
            .record(VersionKey::ReadRelationalTableDomain("rows".into()))
            .unwrap();
        let matching = capture(&store, "g");
        let absent = capture(&store, "elsewhere");
        let first = store.begin_mutation_transaction(&catalog);
        let second = store.begin_mutation_transaction(&catalog);
        for (transaction, id) in [(first, 1), (second, 2)] {
            store
                .commit_mutation_transaction_and_relational(
                    &mut catalog,
                    transaction,
                    insert(std::iter::once(id), "g"),
                    MutationLimits::default(),
                )
                .unwrap();
        }
        assert_eq!(store.relational_state.row_count("rows"), 2);
        assert!(matches!(
            store.validate_read_dependencies(&matching),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::ReadRelationalIndexPrefix { .. },
                ..
            }))
        ));
        assert!(matches!(
            store.validate_read_dependencies(&table),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::ReadRelationalTableDomain(_),
                ..
            }))
        ));
        store.validate_read_dependencies(&absent).unwrap();
    }
    #[test]
    fn summary_admission_falls_back_without_invalidating_unrelated_point_tokens() {
        let mut store = GraphStore::default();
        let mut catalog = Catalog::default();
        setup(&mut store, &mut catalog);
        let mut table = store.capture_read_dependencies(32, 8192).unwrap();
        table
            .record(VersionKey::ReadRelationalTableDomain("rows".into()))
            .unwrap();
        let domain = capture(&store, "g");
        let mut point = store.capture_read_dependencies(32, 8192).unwrap();
        point
            .record(VersionKey::RelationalRow {
                table: "rows".into(),
                primary_key: RelationalKey(vec![RelationalValue::BigInt(0)]),
            })
            .unwrap();
        store
            .commit_relational_transaction(&mut catalog, insert(1..=1000, "other"))
            .unwrap();
        assert_eq!(store.relational_state.row_count("rows"), 1000);
        assert!(matches!(
            store.validate_read_dependencies(&domain),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::ReadBarrier,
                ..
            }))
        ));
        assert!(matches!(
            store.validate_read_dependencies(&table),
            Err(GraphReadDependencyError::Conflict(VersionConflict {
                key: VersionKey::ReadBarrier,
                ..
            }))
        ));
        store.validate_read_dependencies(&point).unwrap();
    }
    #[test]
    fn failed_wal_never_installs_live_prefix_summaries() {
        use crate::store::{set_wal_append_failpoint, WalAppendFailure};
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        for failure in [
            WalAppendFailure::PartialWrite,
            WalAppendFailure::Rollback,
            WalAppendFailure::Sync,
        ] {
            let path = std::env::temp_dir().join(format!(
                "hawdb-rel-prefix-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let mut catalog = Catalog::default();
            let mut store = GraphStore::open(&path, &mut catalog).unwrap();
            setup(&mut store, &mut catalog);
            let read = capture(&store, "g");
            let epoch = store.commit_epoch();
            let key = relational_index_prefix_read_key(
                "rows",
                "group_index",
                [RelationalValue::Text("g".into())].iter(),
            )
            .unwrap();
            let table_key = VersionKey::ReadRelationalTableDomain("rows".into());
            let table_stamp = store.version_index.stamp(&table_key);
            let stamp = store.version_index.stamp(&key);
            set_wal_append_failpoint(failure);
            assert!(store
                .commit_relational_transaction(&mut catalog, insert(std::iter::once(1), "g"))
                .is_err());
            assert_eq!(store.commit_epoch(), epoch);
            assert_eq!(store.version_index.stamp(&key), stamp);
            assert_eq!(store.version_index.stamp(&table_key), table_stamp);
            assert_eq!(store.relational_state.row_count("rows"), 0);
            drop(read);
            drop(store);
            let mut catalog = Catalog::default();
            match GraphStore::open(&path, &mut catalog) {
                Ok(recovered) => {
                    let count = recovered.relational_state.row_count("rows");
                    assert!(count <= 1);
                    assert_eq!(recovered.commit_epoch(), epoch + count as u64);
                    if count == 1 {
                        assert_eq!(
                            recovered
                                .relational_state
                                .row("rows", &RelationalKey(vec![RelationalValue::BigInt(1)]))
                                .unwrap()
                                .values(),
                            &[
                                RelationalValue::BigInt(1),
                                RelationalValue::Text("g".into()),
                                RelationalValue::Text("body".into())
                            ]
                        );
                    }
                }
                Err(error) => {
                    assert!(failure == WalAppendFailure::Rollback);
                    assert!(error
                        .to_string()
                        .contains("strict WAL recovery rejected torn tail"));
                }
            }
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
