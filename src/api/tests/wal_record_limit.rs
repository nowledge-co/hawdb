// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A commit whose WAL record exceeds `max_wal_record_bytes` or
//! `max_wal_batch_operations` (#1797) is refused with the typed
//! `HawDBError::WalRecordLimitExceeded`, before anything is written, on every
//! commit surface. The refusal is not a storage failure: the handle keeps
//! accepting writes, and a reopen shows exactly the earlier commits.

use super::*;
use crate::{
    ConcurrentDatabase, ConcurrentTransactionOptions, EmbeddedStoreHandle,
    EmbeddedStoreOpenOptions, GraphMode, WalRecordLimit,
};

const LIMIT: usize = 4096;

fn limited_config() -> DatabaseConfig {
    DatabaseConfig {
        max_wal_record_bytes: Some(LIMIT),
        ..DatabaseConfig::default()
    }
}

fn payload(bytes: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([("payload".to_string(), Value::String("x".repeat(bytes)))])
}

const OVERSIZED: &str = "CREATE (:Memory {id: 'oversized', content: $payload})";
const IDS: &str = "MATCH (m:Memory) RETURN m.id AS id ORDER BY id";

/// Asserts the typed record-byte refusal and returns it.
fn assert_record_limit(error: HawDBError) -> crate::WalRecordLimitError {
    let HawDBError::WalRecordLimitExceeded(limit) = error else {
        panic!("expected a typed WAL record limit refusal, got {error:?}");
    };
    assert_eq!(limit.limit, WalRecordLimit::RecordBytes);
    assert!(limit.requested > limit.maximum, "{limit:?}");
    assert_eq!(
        limit.limit.config_option(),
        "DatabaseConfig.max_wal_record_bytes"
    );
    *limit
}

fn ids(rows: &crate::executor::QueryRows) -> Vec<Value> {
    rows.iter().map(|row| row["id"].clone()).collect()
}

fn string(value: &str) -> Value {
    Value::String(value.to_string())
}

#[test]
fn database_autocommit_and_transaction_refuse_typed_and_stay_usable() {
    let path = unique_test_dir("wal_record_limit_database");
    {
        let mut db = Database::open_with_config(&path, limited_config()).unwrap();
        db.query("CREATE (:Memory {id: 'before'})").unwrap();
        let epoch = db.statistics().unwrap().computed_at_commit_epoch;

        let limit = assert_record_limit(
            db.query_with_params(OVERSIZED, &payload(2 * LIMIT))
                .unwrap_err(),
        );
        assert_eq!(limit.maximum, LIMIT as u64);
        let message = HawDBError::WalRecordLimitExceeded(Box::new(limit)).to_string();
        assert!(
            message.contains("WAL record byte limit exceeded before append"),
            "{message}"
        );
        assert!(message.contains("max_wal_record_bytes=4096"), "{message}");
        assert_eq!(db.statistics().unwrap().computed_at_commit_epoch, epoch);

        // Every statement of a transaction is accepted; the commit is refused
        // as a whole and applies none of them.
        let mut transaction = db.begin_transaction().unwrap();
        transaction
            .query("CREATE (:Memory {id: 'small-in-transaction'})")
            .unwrap();
        transaction
            .query_with_params(OVERSIZED, &payload(2 * LIMIT))
            .unwrap();
        assert_record_limit(transaction.commit().unwrap_err());
        assert_eq!(db.statistics().unwrap().computed_at_commit_epoch, epoch);
        assert_eq!(ids(&db.query(IDS).unwrap().rows), vec![string("before")]);

        // The handle is not poisoned: ordinary writes and a smaller value
        // commit afterwards.
        db.query("CREATE (:Memory {id: 'after'})").unwrap();
        db.query_with_params(
            "CREATE (:Memory {id: 'fits', content: $payload})",
            &payload(LIMIT / 4),
        )
        .unwrap();
    }
    {
        let mut db = Database::open_with_config(&path, limited_config()).unwrap();
        assert_eq!(
            ids(&db.query(IDS).unwrap().rows),
            vec![string("after"), string("before"), string("fits")]
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn default_limit_refuses_a_sixteen_mib_property_typed() {
    // The issue's shape: one ~16 MiB property value under the default
    // `max_wal_record_bytes` (16 MiB).
    let path = unique_test_dir("wal_record_limit_default");
    {
        let mut db = Database::open(&path).unwrap();
        let limit = assert_record_limit(
            db.query_with_params(OVERSIZED, &payload(16 * 1024 * 1024))
                .unwrap_err(),
        );
        assert_eq!(
            limit.maximum,
            hawdb_storage::config::DEFAULT_MAX_WAL_RECORD_BYTES as u64
        );
        db.query("CREATE (:Memory {id: 'after'})").unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(ids(&db.query(IDS).unwrap().rows), vec![string("after")]);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn batch_operation_limit_is_typed_too() {
    let path = unique_test_dir("wal_record_limit_batch_operations");
    let config = DatabaseConfig {
        max_wal_batch_operations: Some(1),
        ..DatabaseConfig::default()
    };
    {
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        let mut transaction = db.begin_transaction().unwrap();
        transaction.query("CREATE (:Memory {id: 'one'})").unwrap();
        transaction.query("CREATE (:Memory {id: 'two'})").unwrap();
        let HawDBError::WalRecordLimitExceeded(limit) = transaction.commit().unwrap_err() else {
            panic!("expected a typed batch operation refusal");
        };
        assert_eq!(limit.limit, WalRecordLimit::BatchOperations);
        assert_eq!((limit.requested, limit.maximum), (2, 1));
        assert_eq!(
            limit.limit.config_option(),
            "DatabaseConfig.max_wal_batch_operations"
        );
        db.query("CREATE (:Memory {id: 'after'})").unwrap();
    }
    {
        let mut db = Database::open_with_config(&path, config).unwrap();
        assert_eq!(ids(&db.query(IDS).unwrap().rows), vec![string("after")]);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn concurrent_autocommit_and_transactions_refuse_typed_and_stay_usable() {
    let path = unique_test_dir("wal_record_limit_concurrent");
    {
        let db = ConcurrentDatabase::open_with_config(&path, limited_config()).unwrap();
        db.query("CREATE (:Memory {id: 'before'})").unwrap();
        assert_record_limit(
            db.query_with_params(OVERSIZED, &payload(2 * LIMIT))
                .unwrap_err(),
        );
        for options in [
            ConcurrentTransactionOptions::optimistic(),
            ConcurrentTransactionOptions::default(),
        ] {
            let mut transaction = db.begin_transaction(options).unwrap();
            transaction
                .query("CREATE (:Memory {id: 'small-in-transaction'})")
                .unwrap();
            transaction
                .query_with_params(OVERSIZED, &payload(2 * LIMIT))
                .unwrap();
            assert_record_limit(transaction.commit().unwrap_err());
        }
        assert_eq!(ids(&db.query(IDS).unwrap().rows), vec![string("before")]);
        db.query("CREATE (:Memory {id: 'after'})").unwrap();
    }
    {
        let db = ConcurrentDatabase::open_with_config(&path, limited_config()).unwrap();
        assert_eq!(
            ids(&db.query(IDS).unwrap().rows),
            vec![string("after"), string("before")]
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn embedded_handle_with_transaction_refuses_typed_and_stays_usable() {
    let path = unique_test_dir("wal_record_limit_embedded");
    let options = || {
        EmbeddedStoreOpenOptions::graph_only(&path, GraphMode::WritableCutover)
            .with_database_config(limited_config())
    };
    {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options()).unwrap();
        handle
            .with_transaction(|transaction| {
                transaction.query("CREATE (:Memory {id: 'before'})")?;
                Ok(())
            })
            .unwrap();
        let error = handle
            .with_transaction(|transaction| {
                transaction.query("CREATE (:Memory {id: 'small-in-transaction'})")?;
                transaction.query_with_params(OVERSIZED, &payload(2 * LIMIT))?;
                Ok(())
            })
            .unwrap_err();
        assert_record_limit(error);
        handle
            .with_transaction(|transaction| {
                transaction.query("CREATE (:Memory {id: 'after'})")?;
                Ok(())
            })
            .unwrap();
        let rows = handle
            .with_transaction(|transaction| transaction.query(IDS))
            .unwrap();
        assert_eq!(ids(&rows.rows), vec![string("after"), string("before")]);
    }
    {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options()).unwrap();
        let rows = handle
            .with_transaction(|transaction| transaction.query(IDS))
            .unwrap();
        assert_eq!(ids(&rows.rows), vec![string("after"), string("before")]);
    }
    std::fs::remove_dir_all(path).unwrap();
}
