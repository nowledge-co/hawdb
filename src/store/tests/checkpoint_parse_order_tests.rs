use super::{active_checkpoint_path, read_durable_text, rewrite_checksummed_file, unique_test_dir};
use crate::{Database, DatabaseConfig, Value};
use skein_storage::text::encode_properties;
use std::collections::BTreeMap;
use std::path::PathBuf;

struct Fixture(PathBuf);

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn assert_inline_checkpoint_statistics(include_statistics: bool) {
    let root = Fixture(unique_test_dir("checkpoint_parse_order"));
    let mut database = Database::open(&root.0).unwrap();
    database
        .query("CREATE (:InlineCheckpoint {id: 1})-[:INLINE_LINK]->(:InlineCheckpoint {id: 2})")
        .unwrap();
    database.checkpoint().unwrap();
    drop(database);

    let first = encode_properties(&BTreeMap::from([("id".to_string(), Value::Int(1))]));
    let second = encode_properties(&BTreeMap::from([("id".to_string(), Value::Int(2))]));
    let checkpoint = active_checkpoint_path(&root.0);
    rewrite_checksummed_file(
        &checkpoint,
        "canonical_records\ttrue\n",
        &format!("node\t0\t0\t{first}\nnode\t1\t0\t{second}\nrel\t0\t0\t1\t0\t\n"),
        "checkpoint",
    );
    if !include_statistics {
        let text = read_durable_text(&checkpoint, "checkpoint").unwrap();
        let (body, _) = text.rsplit_once("checksum\t").unwrap();
        let without_statistics = body
            .lines()
            .filter(|line| !line.starts_with("stat_"))
            .map(|line| format!("{line}\n"))
            .collect::<String>();
        rewrite_checksummed_file(&checkpoint, body, &without_statistics, "checkpoint");
    }

    for read_only in [true, false] {
        let database = Database::open_with_config(
            &root.0,
            DatabaseConfig {
                read_only,
                ..Default::default()
            },
        )
        .unwrap();
        let statistics = database.basic_statistics();
        assert_eq!(statistics.node_count, 2);
        assert_eq!(statistics.relationship_count, 1);
        assert_eq!(statistics.label_counts.values().copied().sum::<u64>(), 2);
        assert_eq!(statistics.rel_type_counts.values().copied().sum::<u64>(), 1);
    }
}

#[test]
fn inline_checkpoint_statistics_after_records_are_not_counted_twice() {
    assert_inline_checkpoint_statistics(true);
}

#[test]
fn inline_checkpoint_without_statistics_rebuilds_basic_counts() {
    assert_inline_checkpoint_statistics(false);
}
