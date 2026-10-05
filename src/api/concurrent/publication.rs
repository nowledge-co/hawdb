// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::super::{
    observability::StatementRecorder, Database, DatabaseReadSnapshot, DatabaseReadTransaction,
    PlanCache, PreparedRuntimeQuery, QuerySystemVariables, SharedState,
};
use crate::{QueryOutput, Result, Value};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) struct PublishedConcurrentRead {
    pub(super) snapshot: DatabaseReadSnapshot,
    pub(super) recorder: StatementRecorder,
    system_variables: QuerySystemVariables,
    plan_cache: Arc<SharedState<PlanCache>>,
}

impl PublishedConcurrentRead {
    pub(super) fn capture(database: &Database) -> Result<Self> {
        let runtime = database.runtime.get()?;
        Ok(Self {
            snapshot: database.read_snapshot_without_observations()?,
            recorder: database.statement_recorder(),
            system_variables: database.system_variables.clone(),
            plan_cache: Arc::clone(&runtime.plan_cache),
        })
    }

    pub(super) fn begin_read_transaction(&self) -> Result<DatabaseReadTransaction> {
        let mut snapshot = self.snapshot.fork(None)?;
        // Observation tables include reads completed since the last write.
        self.recorder.refresh_read_snapshot(&mut snapshot);
        Ok(snapshot)
    }

    pub(super) fn query_sql(
        &self,
        sql_text: &str,
        parameters: &[Value],
        prepared: crate::relational_sql::PreparedRelationalSql,
    ) -> Result<QueryOutput> {
        if matches!(
            prepared.statement(),
            crate::sql::SqlStatement::Select(select)
                if hawdb_system_sql::is_virtual_catalog_select(select)
        ) {
            // Observation tables include reads completed after publication.
            // Capture them privately rather than mutating the shared view.
            return self
                .begin_read_transaction()?
                .query_sql_with_prepared_params(sql_text, parameters, prepared);
        }
        // SQL execution borrows its store. The caller's publication Arc keeps
        // the complete schema/data view and generation pin alive, so ordinary
        // reads need neither a second store snapshot nor observation copies.
        self.snapshot
            .0
            .query_sql_with_prepared_params(sql_text, parameters, prepared)
    }

    pub(super) fn prepare(
        &self,
        cypher_text: String,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<PreparedRuntimeQuery> {
        self.snapshot.prepare_runtime_query(
            cypher_text,
            parameters,
            &self.system_variables,
            &self.plan_cache,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::super::{ConcurrentDatabase, WalGroupCommitConfig};
    use super::*;
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::{mpsc, Condvar, Mutex};
    use std::time::Duration;

    fn count(snapshot: &mut DatabaseReadTransaction) -> (i64, i64) {
        let graph = snapshot
            .query("MATCH (n:Memory) RETURN count(n) AS n")
            .unwrap();
        let sql = snapshot
            .query_sql("SELECT count(*) AS n FROM records")
            .unwrap();
        let Value::Int(graph) = graph.rows[0]["n"] else {
            panic!("integer graph count")
        };
        let Value::Int(sql) = sql.rows[0]["n"] else {
            panic!("integer SQL count")
        };
        (graph, sql)
    }

    #[test]
    fn reader_pressure_excludes_only_the_idle_internal_publication() {
        let db = Database::new().into_concurrent();
        let epoch = db.commit_epoch().unwrap();
        let oldest = || {
            db.storage_pressure_snapshot()
                .unwrap()
                .oldest_reader_commit_epoch
        };
        assert_eq!(oldest(), None);
        let borrowed = db.inner.commits.read_view().unwrap();
        assert_eq!(oldest(), Some(epoch));
        drop(borrowed);
        assert_eq!(oldest(), None);
        let reader = db.begin_read_transaction().unwrap();
        assert_eq!(oldest(), Some(epoch));
        db.query("CREATE (:Memory {id: 1})").unwrap();
        assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
        assert_eq!(oldest(), Some(epoch));
        drop(reader);
        assert_eq!(oldest(), None);
    }

    #[test]
    fn published_primary_key_reads_keep_row_and_payload_limits() {
        let mut database = Database::new();
        database
            .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        database
            .query_sql("INSERT INTO records (id, body) VALUES (1, 'nonempty payload')")
            .unwrap();
        let db = database.into_concurrent();
        let view = db.inner.commits.read_view().unwrap();
        let snapshot = &view.snapshot.0;
        let prepared = snapshot
            .relational_plan_template_cache
            .prepare("SELECT body FROM records WHERE id = $1")
            .unwrap();
        for (rows, payload, expected) in [
            (0, 4096, "max_output_rows"),
            (16, 1, "max_output_payload_bytes"),
        ] {
            // Keep the intermediate budget independent so this exercises the
            // final output bounds of the primary-key reservation fast path.
            let limits = crate::api::relational_query_limits_with_payload(
                &snapshot.config,
                Some(rows),
                Some(payload),
            );
            let query = |id| {
                crate::relational_sql::execute_prepared_relational_query_with_resources(
                    prepared.clone(),
                    &[Value::Int(id)],
                    snapshot.store.relational_state(),
                    crate::relational_sql::RelationalQueryReadModes::new(
                        crate::relational_sql::RelationalIndexReadMode::Materialized,
                        crate::relational_sql::RelationalRowReadMode::CanonicalMemory,
                    ),
                    crate::relational_sql::RelationalQueryResourceContext::new(
                        hawdb_optimizer::RelationalJoinEnumerationConfig::default(),
                        limits,
                        &snapshot.config.execution_memory,
                        None,
                    ),
                )
            };
            let error = query(1).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            assert!(query(2).unwrap().rows.is_empty());
        }
    }

    #[test]
    fn committed_read_publication_waits_for_group_durability() {
        for fail_sync in [false, true] {
            let path = std::env::temp_dir().join(format!(
                "hawdb-read-publication-{}",
                hawdb_core::generate_uuidv7().unwrap()
            ));
            let mut database = Database::open(&path).unwrap();
            database.query("CREATE (:Memory {id: 1})").unwrap();
            database
                .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY)")
                .unwrap();
            database
                .query_sql("INSERT INTO records (id) VALUES (1)")
                .unwrap();
            let group = WalGroupCommitConfig::benchmark_candidate(
                NonZeroUsize::new(1).unwrap(),
                NonZeroU64::new(1024 * 1024).unwrap(),
                Duration::ZERO,
            )
            .unwrap();
            let db = ConcurrentDatabase::new_with_wal_group_commit(database, group);
            let before = db.published_read_view().unwrap();
            let old_publication = db.inner.commits.read_view().unwrap();
            let (staged, stage_events) = mpsc::channel();
            let (release, release_events) = mpsc::channel();
            let writer_db = db.clone();
            let writer = std::thread::spawn(move || {
                writer_db.inner.commits.execute_grouped(move |database| {
                    let mut tx = database.begin_transaction()?;
                    tx.query("CREATE (:Memory {id: 2})")?;
                    tx.query_sql("INSERT INTO records (id) VALUES (2)")?;
                    let result = tx.commit()?;
                    assert!(database.runtime.get()?.store.wal_sync_group_active());
                    staged.send(()).unwrap();
                    release_events
                        .recv_timeout(Duration::from_secs(30))
                        .unwrap();
                    if fail_sync {
                        crate::store::set_wal_group_sync_failpoint(true);
                    }
                    Ok(result)
                })
            });
            let writer_ready = stage_events.recv_timeout(Duration::from_secs(5));
            let (read_done, read_events) = mpsc::channel();
            let reader_db = db.clone();
            let reader = std::thread::spawn(move || {
                let result = reader_db.begin_read_transaction().map(|mut snapshot| {
                    let rows = count(&mut snapshot);
                    (snapshot, rows)
                });
                let _ = read_done.send(result);
            });
            let while_staged = read_events.recv_timeout(Duration::from_secs(5));
            let _ = release.send(());
            reader.join().unwrap();
            let committed = writer.join().unwrap();
            writer_ready.unwrap();
            let (mut old, rows) = while_staged
                .expect("reader must not wait for group sync")
                .unwrap();
            assert_eq!(rows, (1, 1));
            assert_eq!(old.published_read_view(), before);
            if fail_sync {
                assert!(committed.is_err());
                // Snapshots share the live store's poison atomics, including
                // explicit reads acquired before the failing durability barrier.
                assert!(old.query("RETURN 1").is_err());
                assert!(old_publication.snapshot.ensure_usable().is_err());
                assert!(db.begin_read_transaction().is_err());
                assert!(db.published_read_view().is_err());
                assert!(db
                    .inner
                    .commits
                    .finish_read(&old_publication, Ok(()))
                    .is_err());
            } else {
                committed.unwrap();
                assert_eq!(count(&mut old), (1, 1));
                let mut current = db.begin_read_transaction().unwrap();
                assert_eq!(count(&mut current), (2, 2));
                assert_eq!(current.commit_epoch(), before.visible_commit_epoch() + 1);
            }
            drop(old);
            drop(old_publication);
            drop(db);
            let reopened = Database::open(&path).unwrap();
            assert_eq!(
                count(&mut reopened.begin_read_transaction().unwrap()),
                (2, 2)
            );
            drop(reopened);
            std::fs::remove_dir_all(path).unwrap();
        }
    }

    #[test]
    fn committed_read_publication_stays_invalid_after_unfinished_group() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-read-abandoned-group-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut database = Database::open(&path).unwrap();
        database.query("CREATE (:Memory {id: 1})").unwrap();
        let db = database.into_concurrent();
        let old = db.inner.commits.read_view().unwrap();
        {
            let mut database = db.inner.commits.lock().unwrap();
            assert!(database.begin_wal_sync_group().unwrap());
            database.query("CREATE (:Memory {id: 2})").unwrap();
            // Model a coordinator error after mutation but before group flush.
        }
        assert!(db.begin_read_transaction().is_err());
        assert!(db.inner.commits.finish_read(&old, Ok(())).is_err());
        {
            let mut database = db.inner.commits.lock().unwrap();
            database.finish_wal_sync_group().unwrap();
            assert!(database
                .runtime
                .get()
                .unwrap()
                .store
                .ensure_usable()
                .is_ok());
        }
        // A later healthy guard cannot reopen an invalidated serving boundary.
        assert!(db.begin_read_transaction().is_err());
        assert!(db.inner.commits.finish_read(&old, Ok(())).is_err());
        drop(old);
        drop(db);
        let mut reopened = Database::open(&path).unwrap();
        let output = reopened
            .query("MATCH (n:Memory) RETURN count(n) AS n")
            .unwrap();
        assert_eq!(output.rows[0]["n"], Value::Int(2));
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn committed_read_publication_refreshes_physical_generations() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-read-checkpoint-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let config = crate::DatabaseConfig {
            storage_residency_mode: crate::StorageResidencyMode::OutOfCore,
            ..crate::DatabaseConfig::default()
        };
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        database.query("CREATE (:Memory {id: 1})").unwrap();
        database
            .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY)")
            .unwrap();
        database
            .query_sql("INSERT INTO records (id) VALUES (1)")
            .unwrap();
        let db = database.into_concurrent();
        let before = db.published_read_view().unwrap();
        db.checkpoint().unwrap();
        let checkpointed = db.published_read_view().unwrap();
        assert_eq!(
            checkpointed.visible_commit_epoch(),
            before.visible_commit_epoch()
        );
        assert_ne!(
            checkpointed.physical_generation(),
            before.physical_generation()
        );
        let pinned_generation = checkpointed.physical_generation().unwrap().0;
        let data = crate::api::tests::active_storage_root(&path);
        let mut old = db.begin_read_transaction().unwrap();
        assert_eq!(old.published_read_view(), checkpointed);
        for id in 2..=3 {
            db.query(&format!("CREATE (:Memory {{id: {id}}})")).unwrap();
            db.query_sql(&format!("INSERT INTO records (id) VALUES ({id})"))
                .unwrap();
            db.checkpoint().unwrap();
        }
        assert_eq!(count(&mut old), (1, 1));
        assert_eq!(count(&mut db.begin_read_transaction().unwrap()), (3, 3));
        assert!(data
            .join(format!("canonical.{pinned_generation}.hawdb"))
            .exists());
        drop(old);
        db.checkpoint().unwrap();
        assert!(!data
            .join(format!("canonical.{pinned_generation}.hawdb"))
            .exists());
        drop(db);
        let reopened = Database::open_with_config(&path, config).unwrap();
        assert_eq!(
            count(&mut reopened.begin_read_transaction().unwrap()),
            (3, 3)
        );
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn committed_read_publication_rejects_new_and_late_reads_after_writer_panic() {
        let db = Database::new().into_concurrent();
        db.query("CREATE (:Memory {id: 1})").unwrap();
        let (captured, capture_events) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        db.set_autocommit_read_gate(captured, Arc::clone(&release))
            .unwrap();
        let reader_db = db.clone();
        let reader = std::thread::spawn(move || reader_db.query("MATCH (n:Memory) RETURN n.id"));
        let ready = capture_events.recv_timeout(Duration::from_secs(5));
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.with_autocommit_exclusive(|database| {
                database.query("CREATE (:Memory {id: 2})")?;
                panic!("writer panic after mutation");
            })
        }));
        db.clear_autocommit_read_gate().unwrap();
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        let late = reader.join().unwrap();
        ready.unwrap();
        assert!(panic.is_err());
        assert!(late.unwrap_err().to_string().contains("close and reopen"));
        assert!(db.begin_read_transaction().is_err());
        assert!(db.query("RETURN 1").is_err());
        assert!(db.query_sql("SELECT 1").is_err());
    }
}
