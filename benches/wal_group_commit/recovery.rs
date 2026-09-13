use skein::{Database, Value};
use std::path::Path;

pub(super) struct ExpectedRecovery {
    pub final_epoch: u64,
    pub commit_count: usize,
    pub warmup_start_id: usize,
    pub warmup_commit_count: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct RecoveryVerification {
    pub wal_order_verified: bool,
    pub strict_recovery_verified: bool,
}

pub(super) fn verify_recovery(
    path: &Path,
    expected: ExpectedRecovery,
    mut phase: impl FnMut(&'static str),
) -> RecoveryVerification {
    phase("recovery-open");
    let mut reopened = match Database::open(path) {
        Ok(database) => database,
        Err(error) => {
            eprintln!("wal_group_commit recovery open failed: {error}");
            return RecoveryVerification::default();
        }
    };
    phase("recovery-order");
    // Strict recovery checks contiguous LSNs for both supported WAL encodings.
    // Inspect its original report before any read query, and keep this handle
    // for the row proof so recovery and derived-artifact work happen only once.
    let report = reopened.storage_recovery_report();
    let wal_order_verified = report.wal_replay_start_lsn == Some(1)
        && expected
            .final_epoch
            .checked_add(1)
            .is_some_and(|next_lsn| report.next_lsn_after_replay == Some(next_lsn))
        && report.replayed_wal_entries as u64 == expected.final_epoch
        && report.torn_tail_reason.is_none();

    phase("recovery-rows");
    let strict_recovery_verified =
        match reopened.query_sql("SELECT id, body FROM public.messages ORDER BY id") {
            Ok(rows) => {
                let expected_rows = (0..expected.commit_count)
                    .map(|id| (id, format!("payload-{id}")))
                    .chain((0..expected.warmup_commit_count).map(|offset| {
                        let id = expected.warmup_start_id + offset;
                        (id, format!("warmup-{id}"))
                    }));
                rows.rows.len() == expected.commit_count + expected.warmup_commit_count
                    && reopened.commit_epoch() == expected.final_epoch
                    && rows
                        .rows
                        .iter()
                        .zip(expected_rows)
                        .all(|(row, (id, body))| {
                            i64::try_from(id).is_ok_and(|id| row.get("id") == Some(&Value::Int(id)))
                                && row.get("body") == Some(&Value::String(body))
                        })
            }
            Err(error) => {
                eprintln!("wal_group_commit recovery query failed: {error}");
                false
            }
        };
    phase("recovery-close");
    drop(reopened);
    RecoveryVerification {
        wal_order_verified,
        strict_recovery_verified,
    }
}
