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

#[path = "../benches/wal_group_commit/recovery.rs"]
mod recovery;

#[cfg(test)]
mod tests {
    use super::recovery::*;
    use hawdb::Database;
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fixture {
        path: PathBuf,
        epoch: u64,
    }

    impl Fixture {
        fn create(rows: &[(i64, &str)]) -> Self {
            static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "hawdb-wal-benchmark-recovery-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed),
            ));
            std::fs::create_dir(&path).unwrap();
            let mut database = Database::open(&path).unwrap();
            database
                .query_sql(
                    "CREATE TABLE public.messages (id BIGINT PRIMARY KEY, body TEXT NOT NULL)",
                )
                .unwrap();
            for (id, body) in rows {
                database
                    .query_sql(&format!(
                        "INSERT INTO public.messages (id, body) VALUES ({id}, '{body}')"
                    ))
                    .unwrap();
            }
            let epoch = database.commit_epoch();
            drop(database);
            Self { path, epoch }
        }

        fn expected(&self) -> ExpectedRecovery {
            ExpectedRecovery {
                final_epoch: self.epoch,
                commit_count: 2,
                warmup_start_id: 2,
                warmup_commit_count: 0,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    #[test]
    fn one_reopen_proves_order_and_every_row_including_warmup() {
        let fixture = Fixture::create(&[(0, "payload-0"), (1, "payload-1"), (2, "warmup-2")]);
        let mut phases = Vec::new();
        let verified = verify_recovery(
            &fixture.path,
            ExpectedRecovery {
                warmup_commit_count: 1,
                ..fixture.expected()
            },
            |phase| phases.push(phase),
        );
        assert_eq!(
            verified,
            RecoveryVerification {
                wal_order_verified: true,
                strict_recovery_verified: true,
            }
        );
        assert_eq!(
            phases,
            [
                "recovery-open",
                "recovery-order",
                "recovery-rows",
                "recovery-close"
            ]
        );
    }

    #[test]
    fn clean_recovery_keeps_the_original_separate_proofs() {
        let fixture = Fixture::create(&[(0, "payload-0"), (1, "payload-1")]);
        let original_order = Database::open(&fixture.path).is_ok_and(|db| {
            let report = db.storage_recovery_report();
            report.wal_replay_start_lsn == Some(1)
                && report.next_lsn_after_replay == Some(fixture.epoch + 1)
                && report.replayed_wal_entries as u64 == fixture.epoch
                && report.torn_tail_reason.is_none()
        });
        let original_rows = Database::open(&fixture.path)
            .and_then(|mut db| {
                db.query_sql("SELECT id FROM public.messages ORDER BY id")
                    .map(|rows| rows.rows.len() == 2 && db.commit_epoch() == fixture.epoch)
            })
            .unwrap_or(false);
        let verified = verify_recovery(&fixture.path, fixture.expected(), |_| {});
        assert!(original_order && original_rows);
        assert_eq!(verified.wal_order_verified, original_order);
        assert_eq!(verified.strict_recovery_verified, original_rows);
    }

    #[test]
    fn wrong_epoch_rejects_both_proofs() {
        let fixture = Fixture::create(&[(0, "payload-0"), (1, "payload-1")]);
        for final_epoch in [fixture.epoch - 1, fixture.epoch + 1, u64::MAX] {
            assert_eq!(
                verify_recovery(
                    &fixture.path,
                    ExpectedRecovery {
                        final_epoch,
                        ..fixture.expected()
                    },
                    |_| {},
                ),
                RecoveryVerification::default(),
            );
        }
    }

    #[test]
    fn valid_wal_cannot_hide_missing_extra_or_replaced_rows() {
        for rows in [
            vec![(0, "payload-0")],
            vec![(0, "payload-0"), (1, "payload-1"), (2, "payload-2")],
            vec![(0, "payload-0"), (2, "payload-2")],
            vec![(0, "payload-0"), (1, "wrong-payload")],
        ] {
            let fixture = Fixture::create(&rows);
            let verified = verify_recovery(&fixture.path, fixture.expected(), |_| {});
            assert!(verified.wal_order_verified);
            assert!(
                !verified.strict_recovery_verified,
                "accepted rows: {rows:?}"
            );
        }
    }

    #[test]
    fn checkpointed_rows_do_not_substitute_for_full_wal_replay() {
        let fixture = Fixture::create(&[(0, "payload-0"), (1, "payload-1")]);
        Database::open(&fixture.path).unwrap().checkpoint().unwrap();
        let verified = verify_recovery(&fixture.path, fixture.expected(), |_| {});
        assert!(!verified.wal_order_verified);
        assert!(verified.strict_recovery_verified);
    }

    #[test]
    fn torn_wal_rejects_recovery_before_any_row_proof() {
        let fixture = Fixture::create(&[(0, "payload-0"), (1, "payload-1")]);
        let wal_path = std::fs::read_dir(&fixture.path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("wal.")
                    && path
                        .extension()
                        .is_some_and(|extension| extension == "hawdb")
            })
            .expect("fixture must retain its WAL");
        std::fs::OpenOptions::new()
            .append(true)
            .open(wal_path)
            .unwrap()
            .write_all(b"torn-entry-without-checksum")
            .unwrap();
        let mut phases = Vec::new();
        let verified = verify_recovery(&fixture.path, fixture.expected(), |phase| {
            phases.push(phase)
        });
        assert_eq!(verified, RecoveryVerification::default());
        assert_eq!(phases, ["recovery-open"]);
    }
}
