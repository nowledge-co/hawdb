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

//! Databases written by persistent format 2 (HawDB 0.33.0) carry a full-row
//! `relational.N.hawdb` checkpoint. Format 3 reads them and then checkpoints
//! incrementally without writing that file again (#1737).

use super::*;

/// Files written by HawDB 0.33.0 (main `150ffe84d`), packed by
/// `property_defaults::unpack_fixture`'s bundle format: `threads` with a
/// unique title, `events` (composite key, foreign key, secondary index on
/// `seq`, overflow bodies), keyless `notes`, one full-row checkpoint and a
/// WAL tail (update, delete, overflow insert, duplicate keyless row).
const FORMAT_2_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/format_2_relational_checkpoint.bundle");

fn noise(seed: u64, len: usize) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}

fn full_row_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![root.join("branches")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name != "objects") {
                    pending.push(path);
                }
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_prefix("relational."))
                .and_then(|rest| rest.strip_suffix(".hawdb"))
                .is_some_and(|generation| generation.parse::<u64>().is_ok())
            {
                found.push(path);
            }
        }
    }
    found
}

fn expected_events() -> BTreeMap<(i64, i64), String> {
    let mut events = BTreeMap::new();
    for thread in 1..=4i64 {
        for seq in 1..=10i64 {
            let body = if seq == 10 && thread <= 2 {
                format!(
                    "large {thread}/{seq} {}",
                    noise((thread * 100 + seq) as u64, 9000)
                )
            } else {
                format!("event {thread}/{seq}")
            };
            events.insert((thread, seq), body);
        }
    }
    events.insert((2, 5), "updated tail".to_string());
    events.remove(&(3, 7));
    events.insert((4, 11), format!("large tail {}", noise(4031, 9000)));
    events
}

fn assert_fixture_rows(db: &mut Database, extra: &[(i64, i64, &str)]) {
    let mut expected = expected_events();
    for (thread, seq, body) in extra {
        expected.insert((*thread, *seq), (*body).to_string());
    }
    let rows = db
        .query_sql("SELECT thread, seq, body FROM events ORDER BY thread, seq")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), expected.len());
    for (row, ((thread, seq), body)) in rows.iter().zip(&expected) {
        assert_eq!(row["thread"], Value::Int(*thread));
        assert_eq!(row["seq"], Value::Int(*seq));
        assert!(
            row["body"] == Value::String(body.clone()),
            "event {thread}/{seq} differs"
        );
    }
    let threads = db
        .query_sql("SELECT id, title FROM threads ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(threads.len(), 4);
    assert_eq!(threads[3]["title"], Value::String("thread 4".into()));
    let notes = db
        .query_sql("SELECT k, v FROM notes ORDER BY k, v")
        .unwrap()
        .rows;
    assert_eq!(notes.len(), 7);
    assert_eq!(
        db.query_sql("SELECT thread FROM events WHERE seq = 9 ORDER BY thread")
            .unwrap()
            .rows
            .len(),
        4
    );
    // Unique, foreign-key and primary-key constraints are live.
    assert!(db
        .query_sql("INSERT INTO threads (id, title) VALUES (9, 'thread 1')")
        .is_err());
    assert!(db
        .query_sql("INSERT INTO events (thread, seq, body) VALUES (99, 1, 'orphan')")
        .is_err());
    assert!(db
        .query_sql("INSERT INTO events (thread, seq, body) VALUES (1, 1, 'duplicate')")
        .is_err());
}

#[test]
fn format_2_full_row_checkpoint_reopens_and_then_checkpoints_incrementally() {
    let path = unique_test_dir("relational_checkpoint_format_2_fixture");
    super::property_defaults::unpack_fixture(FORMAT_2_FIXTURE, &path);
    let legacy = full_row_files(&path);
    assert!(!legacy.is_empty(), "the fixture carries a full-row file");

    // Read-only open decodes the full-row file and replays the WAL tail.
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                recovery_mode: crate::RecoveryMode::Strict,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let rows = db
            .query_sql("SELECT COUNT(*) AS n FROM events")
            .unwrap()
            .rows;
        assert_eq!(rows[0]["n"], Value::Int(expected_events().len() as i64));
    }

    let large = format!("large after upgrade {}", noise(77, 9000));
    {
        let mut db = Database::open(&path).unwrap();
        assert_fixture_rows(&mut db, &[]);
        db.query_sql_with_params(
            "INSERT INTO events (thread, seq, body) VALUES (1, 11, $1)",
            &[Value::String(large.clone())],
        )
        .unwrap();
        db.checkpoint().unwrap();
        // No new full-row file is written.
        let after_first = full_row_files(&path);
        assert!(after_first.iter().all(|file| legacy.contains(file)));
        db.query_sql("UPDATE events SET body = 'second round' WHERE thread = 4 AND seq = 1")
            .unwrap();
        db.checkpoint().unwrap();
        // Reclamation removed the old full-row files; the session still reads
        // every overflow value from the published overflow extents.
        assert!(
            full_row_files(&path).is_empty(),
            "{:?}",
            full_row_files(&path)
        );
        assert_fixture_rows(&mut db, &[(1, 11, &large), (4, 1, "second round")]);
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_fixture_rows(&mut db, &[(1, 11, &large), (4, 1, "second round")]);
    }
    std::fs::remove_dir_all(path).unwrap();
}
