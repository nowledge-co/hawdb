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

use super::*;
use crate::{DatabaseConfig, Value};
use std::sync::mpsc;

#[test]
fn selected_checkpoint_does_not_make_cached_reads_wait_for_the_writer() {
    let path = std::env::temp_dir().join(format!(
        "hawdb-read-handoff-writer-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut database = Database::open_with_config(
        &path,
        DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    let suspension = database.runtime.suspend_automatic_checkpoint().unwrap();
    database.query("CREATE (:Memory {id: 'before'})").unwrap();
    let sequencer = CommitSequencer::new(database, WalGroupCommitConfig::default());
    let previous = sequencer.read_view().unwrap();
    let before = previous.snapshot.0.published_read_view();
    drop(suspension);
    let deadline = Instant::now() + Duration::from_secs(15);
    while !sequencer
        .checkpoint_control
        .report()
        .unwrap()
        .unwrap()
        .waiting_for_handoff
    {
        assert!(Instant::now() < deadline, "owner did not publish");
        std::thread::sleep(Duration::from_millis(5));
    }
    let (sent, received) = mpsc::channel();
    let result = std::thread::scope(|scope| {
        let writer = sequencer.lock().unwrap();
        scope.spawn(|| sent.send(sequencer.read_view()).unwrap());
        let result = received.recv_timeout(Duration::from_secs(1));
        assert!(sequencer.checkpoint_control.has_pending_handoff());
        drop(writer);
        result
    });
    let current = result.expect("cached read waited for the writer").unwrap();
    assert_eq!(current.snapshot.0.published_read_view(), before);
    assert_eq!(
        current
            .begin_read_transaction()
            .unwrap()
            .query("MATCH (n:Memory) RETURN n.id AS id")
            .unwrap()
            .rows[0]["id"],
        Value::String("before".into())
    );
    let adopted = sequencer.read_view().unwrap();
    assert!(!sequencer.checkpoint_control.has_pending_handoff());
    let after = adopted.snapshot.0.published_read_view();
    assert_eq!(after.visible_commit_epoch(), before.visible_commit_epoch());
    assert_ne!(after.physical_generation(), before.physical_generation());
    drop(adopted);
    drop(current);
    drop(previous);
    drop(sequencer);
    let mut reopened = Database::open(&path).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (n:Memory) RETURN n.id AS id")
            .unwrap()
            .rows[0]["id"],
        Value::String("before".into())
    );
    drop(reopened);
    std::fs::remove_dir_all(path).unwrap();
}
