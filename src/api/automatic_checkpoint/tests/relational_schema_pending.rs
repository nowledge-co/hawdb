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

//! A required schema checkpoint that cannot fit its dirty budget stays
//! pending (#1593). Background checkpoints must not retry it in a loop.

use super::*;
use crate::Value;
use hawdb_storage::relational::RelationalRowPagePublicationConfig;
use hawdb_storage::store::relational_row_pages::set_relational_row_publication_config_override_for_test;
use std::num::NonZeroU64;

/// Process CPU time, for the log only. Windows has no `getrusage`; the
/// attempt-count assertions below bound the background work there.
#[cfg(unix)]
fn process_cpu() -> Option<Duration> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage writes one complete rusage for RUSAGE_SELF.
    let usage = unsafe {
        assert_eq!(libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()), 0);
        usage.assume_init()
    };
    let time = |value: libc::timeval| {
        Duration::from_secs(value.tv_sec as u64) + Duration::from_micros(value.tv_usec as u64)
    };
    Some(time(usage.ru_utime) + time(usage.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu() -> Option<Duration> {
    None
}

#[test]
fn background_checkpoints_do_not_spin_on_a_deferred_schema_checkpoint() {
    let fixture = Fixture::new();
    let mut db = Database::open_with_config(
        &fixture.0,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query_sql("CREATE TABLE t0 (id BIGINT PRIMARY KEY, body TEXT)")
        .unwrap();
    for start in (0..3_000).step_by(500) {
        let values = (start..start + 500)
            .map(|id| format!("({id}, '{}')", "x".repeat(180)))
            .collect::<Vec<_>>()
            .join(", ");
        db.query_sql(&format!("INSERT INTO t0 (id, body) VALUES {values}"))
            .unwrap();
    }
    db.checkpoint().unwrap();

    // Three thousand rows encode to about 0.7 MiB; 64 KiB cannot hold them.
    set_relational_row_publication_config_override_for_test(
        &fixture.0,
        Some(RelationalRowPagePublicationConfig {
            max_dirty_bytes: NonZeroU64::new(64 * 1024).unwrap(),
            ..Default::default()
        }),
    );
    db.query_sql("CREATE TABLE t1 (id BIGINT PRIMARY KEY)")
        .unwrap();
    let store_reason = db
        .runtime
        .get()
        .unwrap()
        .store
        .relational_row_schema_checkpoint_deferred()
        .map(str::to_string)
        .expect("the schema checkpoint is deferred");

    // Keep the source changing so every age deadline is due.
    let started = Instant::now();
    let cpu_before = process_cpu();
    let mut id = 0;
    while started.elapsed() < Duration::from_secs(3) {
        db.query_sql(&format!("INSERT INTO t1 (id) VALUES ({id})"))
            .unwrap();
        id += 1;
        std::thread::sleep(Duration::from_millis(50));
    }
    let cpu = process_cpu()
        .zip(cpu_before)
        .map(|(after, before)| after - before);
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    eprintln!(
        "deferred schema checkpoint over {:?}: cpu={cpu:?} report={report:?}",
        started.elapsed()
    );
    set_relational_row_publication_config_override_for_test(&fixture.0, None);

    assert_eq!(report.completed_checkpoints, 0, "{report:?}");
    // The ordinary operation-failure circuit stops background attempts after
    // MAX_OPERATION_FAILURES in one WAL generation; source changes do not
    // reopen it.
    assert!(
        report.operation_failures <= u64::from(MAX_OPERATION_FAILURES),
        "background checkpoints retried a refused schema checkpoint: {report:?}"
    );
    assert!(report.operation_retry_exhausted, "{report:?}");
    assert_eq!(
        report.relational_schema_checkpoint_pending.as_deref(),
        Some(store_reason.as_str())
    );
    let residency = db.storage_residency_report().unwrap().relational_rows;
    assert!(residency.schema_checkpoint_pending);
    assert_eq!(
        residency.schema_checkpoint_pending_reason.as_deref(),
        Some(store_reason.as_str())
    );

    // An explicit checkpoint with the default budget completes it and clears
    // the pending reason from both reports.
    db.checkpoint().unwrap();
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert_eq!(report.relational_schema_checkpoint_pending, None);
    let residency = db.storage_residency_report().unwrap().relational_rows;
    assert!(!residency.schema_checkpoint_pending);
    assert_eq!(residency.schema_checkpoint_pending_reason, None);
    assert_eq!(
        db.query_sql("SELECT count(*) AS c FROM t1").unwrap().rows[0]["c"],
        Value::Int(id)
    );
}
