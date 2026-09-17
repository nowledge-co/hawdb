use super::*;
use skein_core::{IndexKind, PropertyType, TableKind};

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);
        let id = NEXT_TEST_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-statistics-kernel-{}-{stamp}-{id}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn assert_empty(&self) {
        assert_eq!(fs::read_dir(&self.0).unwrap().count(), 0);
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn options(memory_budget_bytes: usize) -> StatsRunOptions {
    StatsRunOptions {
        memory_budget_bytes,
        max_spill_bytes: 32 * 1024 * 1024,
        max_spill_runs: 1024,
        max_generated_facts: 1_000_000,
    }
}

fn path_count(target: u32) -> StatsRecord {
    StatsRecord::PathCount {
        source_label: LabelId(0),
        rel_type: RelTypeId(0),
        target_label: LabelId(target),
    }
}

fn assert_execution_error<T>(result: Result<T>, expected: &str) {
    match result {
        Err(SkeinError::Execution(message)) => assert!(message.contains(expected), "{message}"),
        Err(error) => panic!("expected execution error containing {expected:?}: {error}"),
        Ok(_) => panic!("expected execution error containing {expected:?}"),
    }
}

#[test]
fn run_admission_preserves_inclusive_fact_memory_spill_byte_and_run_limits() {
    let root = TestRoot::new();
    let catalog = Catalog::default();
    for memory in [63, 64] {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = options(memory);
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        let result = writer.push(path_count(1));
        if memory == 63 {
            assert_execution_error(result, "fact uses 64 bytes");
            assert!(writer.chunk.is_empty());
        } else {
            result.unwrap();
            let (statistics, report) = writer.finish(base_statistics()).unwrap();
            assert_eq!(statistics.path_counts.values().copied().sum::<u64>(), 1);
            assert_eq!(report.peak_buffer_bytes, 64);
        }
        drop(spill);
        root.assert_empty();
    }
    for bytes in [8, 9] {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = StatsRunOptions {
            max_spill_bytes: bytes,
            ..options(4096)
        };
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        writer.push(path_count(1)).unwrap();
        let result = writer.finish(base_statistics());
        if bytes == 8 {
            assert_execution_error(result, "max_spill_bytes 8");
        } else {
            assert_eq!(result.unwrap().1.spilled_bytes, 9);
            assert_eq!(
                fs::read(spill.path().join("run.00000000.skein")).unwrap(),
                b"pc\t0\t0\t1\n"
            );
        }
        drop(spill);
        root.assert_empty();
    }
    {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = StatsRunOptions {
            max_generated_facts: 1,
            ..options(4096)
        };
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        writer.push(path_count(1)).unwrap();
        assert_execution_error(writer.push(path_count(1)), "max_generated_facts 1");
        assert_eq!(writer.chunk.len(), 1);
        assert!(writer.runs.is_empty());
    }
    root.assert_empty();
    {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = StatsRunOptions {
            max_spill_runs: 1,
            ..options(4096)
        };
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        writer.push(path_count(1)).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.runs.len(), 1);
        writer.push(path_count(2)).unwrap();
        assert_execution_error(writer.finish(base_statistics()), "max_spill_runs 1");
    }
    root.assert_empty();
}

#[test]
fn output_and_exclusion_state_admission_preserve_exact_boundaries() {
    let root = TestRoot::new();
    let catalog = Catalog::default();
    let two_counters = 2 * (std::mem::size_of::<(LabelId, RelTypeId, LabelId)>() + 48);
    for memory in [two_counters - 1, two_counters] {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = options(memory);
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        writer.push(path_count(1)).unwrap();
        writer.push(path_count(2)).unwrap();
        let result = writer.finish(base_statistics());
        if memory < two_counters {
            assert_execution_error(result, "output state exceeds memory_budget_bytes");
        } else {
            let (statistics, report) = result.unwrap();
            assert_eq!(statistics.path_counts.len(), 2);
            assert_eq!(report.output_statistics_bytes, two_counters);
        }
        drop(spill);
        root.assert_empty();
    }
    for memory in [99, 100] {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = options(memory);
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        let result = writer.push_node_property(LabelId(0), "skip", &Value::Binary(vec![]));
        if memory == 99 {
            assert_execution_error(
                result,
                "excluded-property state exceeds memory_budget_bytes",
            );
            assert!(writer.excluded_property_groups.is_empty());
        } else {
            result.unwrap();
            let (_, report) = writer.finish(base_statistics()).unwrap();
            assert_eq!(report.excluded_property_group_count, 1);
            assert_eq!(report.peak_buffer_bytes, 100);
        }
        drop(spill);
        root.assert_empty();
    }
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Items");
    catalog.get_or_create_property_index(label, "key");
    for memory in [95, 96] {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = options(memory);
        let writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        let result = writer.finish(base_statistics());
        if memory == 95 {
            assert_execution_error(
                result,
                "index statistics output state exceeds memory_budget_bytes",
            );
        } else {
            assert_eq!(result.unwrap().1.output_statistics_bytes, 96);
        }
        drop(spill);
        root.assert_empty();
    }
}

#[test]
fn corrupt_missing_and_existing_runs_fail_and_transient_files_are_reclaimed() {
    let root = TestRoot::new();
    let catalog = Catalog::default();
    let options = options(4096);
    for corrupt in [
        Some("pc\t0\t0\t1\nnp\t0\t61\tff\n"),
        Some("pc\t0\t0\t"),
        None,
    ] {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        writer.push(path_count(1)).unwrap();
        writer.flush().unwrap();
        let path = &writer.runs[0];
        if let Some(corrupt) = corrupt {
            fs::write(path, corrupt).unwrap();
        } else {
            fs::remove_file(path).unwrap();
        }
        assert!(matches!(
            writer.finish(base_statistics()),
            Err(SkeinError::Storage(_))
        ));
        drop(spill);
        root.assert_empty();
    }
    {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        let path = spill.path().join("run.00000000.skein");
        fs::write(&path, b"existing-run").unwrap();
        writer.push(path_count(1)).unwrap();
        assert!(matches!(
            writer.finish(base_statistics()),
            Err(SkeinError::Storage(_))
        ));
        assert_eq!(fs::read(path).unwrap(), b"existing-run");
    }
    root.assert_empty();
    let result = std::panic::catch_unwind(|| {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        writer.push(path_count(1)).unwrap();
        writer.flush().unwrap();
        panic!("injected unwind after spill");
    });
    assert!(result.is_err());
    root.assert_empty();
}

#[test]
fn statistics_record_framing_and_malformed_fields_remain_unchanged() {
    let mut records = facts(0);
    records.sort_unstable();
    let mut kinds = BTreeSet::new();
    for record in &records {
        let encoded = record.encode();
        assert!(!encoded.contains('\n'));
        kinds.insert(encoded.split('\t').next().unwrap().to_string());
        assert_eq!(StatsRecord::decode(&encoded).unwrap(), *record);
    }
    assert_eq!(
        kinds,
        ["np", "rp", "ix", "rs", "rt", "pc", "ps", "pt", "bc", "bs", "bt"]
            .into_iter()
            .map(str::to_string)
            .collect()
    );
    for (encoded, expected) in [
        (
            "pc\t1\t2\t3",
            StatsRecord::PathCount {
                source_label: LabelId(1),
                rel_type: RelTypeId(2),
                target_label: LabelId(3),
            },
        ),
        (
            "np\t1\t73636f7265\t692d37",
            StatsRecord::NodeProperty {
                label: LabelId(1),
                property: "score".into(),
                value: Value::Int(-7),
            },
        ),
        (
            "bs\t1\t2\t3\t2\t7",
            StatsRecord::BoundedPathSource {
                source_label: LabelId(1),
                rel_type: RelTypeId(2),
                target_label: LabelId(3),
                hop: 2,
                node: NodeId(7),
            },
        ),
    ] {
        assert_eq!(StatsRecord::decode(encoded).unwrap(), expected);
        assert_eq!(expected.encode(), encoded);
    }
    for encoded in [
        "",
        "np",
        "pc\t0\t0\t1\textra",
        "rs\t4294967296\t0",
        "rt\t0\t18446744073709551616",
        "bc\t0\t0\t1\t-1",
        "np\t0\ta\u{e9}a\t6931",
        "rp\t0\t61\tc3a9",
        "ix\t0\tff",
    ] {
        let result = std::panic::catch_unwind(|| StatsRecord::decode(encoded));
        assert!(result.is_ok(), "decoder panicked for {encoded:?}");
        assert!(
            matches!(result.unwrap(), Err(SkeinError::Storage(_))),
            "{encoded:?}"
        );
    }
}

fn catalog() -> Catalog {
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Source");
    catalog.get_or_create_label("Target");
    catalog.get_or_create_rel_type("LINKS");
    for property in ["score", "other", "absent"] {
        catalog.get_or_create_property_index(label, property);
    }
    catalog
}

fn base_statistics() -> GraphStatistics {
    GraphStatistics {
        computed_at_commit_epoch: 23,
        advanced_statistics_complete: true,
        histogram_sample_limit: 512,
        node_count: 512,
        relationship_count: 256,
        label_counts: BTreeMap::from([(LabelId(0), 512)]),
        rel_type_counts: BTreeMap::from([(RelTypeId(0), 256)]),
        ..Default::default()
    }
}

// Count bags and sets directly; do not invoke the run sorter or accumulator.
fn reference_statistics(records: &[StatsRecord], catalog: &Catalog) -> GraphStatistics {
    let mut statistics = base_statistics();
    for index in catalog.property_indexes() {
        statistics
            .index_samples
            .insert(index.id, IndexStatisticsSample::exact(0, 0));
    }
    let mut bags = BTreeMap::<StatsRecord, u64>::new();
    let mut properties = BTreeMap::<(bool, u32, String), BTreeSet<Value>>::new();
    let mut indexes = BTreeMap::<IndexId, Vec<String>>::new();
    for record in records {
        match record {
            StatsRecord::NodeProperty {
                label,
                property,
                value,
            } => {
                properties
                    .entry((false, label.0, property.clone()))
                    .or_default()
                    .insert(value.clone());
            }
            StatsRecord::RelProperty {
                rel_type,
                property,
                value,
            } => {
                properties
                    .entry((true, rel_type.0, property.clone()))
                    .or_default()
                    .insert(value.clone());
            }
            StatsRecord::IndexEntry { index, key } => {
                indexes.entry(*index).or_default().push(key.clone());
            }
            _ => *bags.entry(record.clone()).or_default() += 1,
        }
    }
    for (record, count) in bags {
        match record {
            StatsRecord::RelSource { rel_type, .. } => {
                *statistics
                    .rel_type_source_counts
                    .entry(rel_type)
                    .or_default() += 1
            }
            StatsRecord::RelTarget { rel_type, .. } => {
                *statistics
                    .rel_type_target_counts
                    .entry(rel_type)
                    .or_default() += 1
            }
            StatsRecord::PathCount {
                source_label,
                rel_type,
                target_label,
            } => {
                *statistics
                    .path_counts
                    .entry((source_label, rel_type, target_label))
                    .or_default() += count
            }
            StatsRecord::PathSource {
                source_label,
                rel_type,
                target_label,
                ..
            } => {
                *statistics
                    .path_source_distinct_counts
                    .entry((source_label, rel_type, target_label))
                    .or_default() += 1
            }
            StatsRecord::PathTarget {
                source_label,
                rel_type,
                target_label,
                ..
            } => {
                *statistics
                    .path_target_distinct_counts
                    .entry((source_label, rel_type, target_label))
                    .or_default() += 1
            }
            StatsRecord::BoundedPathCount {
                source_label,
                rel_type,
                target_label,
                hop,
            } => {
                *statistics
                    .bounded_path_counts
                    .entry((source_label, rel_type, target_label, hop))
                    .or_default() += count
            }
            StatsRecord::BoundedPathSource {
                source_label,
                rel_type,
                target_label,
                hop,
                ..
            } => {
                *statistics
                    .bounded_path_source_distinct_counts
                    .entry((source_label, rel_type, target_label, hop))
                    .or_default() += 1
            }
            StatsRecord::BoundedPathTarget {
                source_label,
                rel_type,
                target_label,
                hop,
                ..
            } => {
                *statistics
                    .bounded_path_target_distinct_counts
                    .entry((source_label, rel_type, target_label, hop))
                    .or_default() += 1
            }
            _ => unreachable!(),
        }
    }
    for ((relationship, id, property), values) in properties {
        assert!(
            values.len() <= 128,
            "this oracle expects exact small histograms"
        );
        let count = values.len() as u64;
        let values = values.into_iter().collect();
        if relationship {
            let key = (RelTypeId(id), property);
            statistics
                .rel_property_distinct_counts
                .insert(key.clone(), count);
            statistics
                .rel_property_histograms
                .insert(key.clone(), values);
            statistics
                .sampled_rel_property_histograms
                .insert(key, false);
        } else {
            let key = (LabelId(id), property);
            statistics
                .property_distinct_counts
                .insert(key.clone(), count);
            statistics.property_histograms.insert(key.clone(), values);
            statistics.sampled_property_histograms.insert(key, false);
        }
    }
    for (index, keys) in indexes {
        let distinct = keys.iter().collect::<BTreeSet<_>>().len() as u64;
        statistics.index_samples.insert(
            index,
            IndexStatisticsSample::exact(keys.len() as u64, distinct),
        );
    }
    statistics
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn facts(seed: u64) -> Vec<StatsRecord> {
    let mut random = seed + 1;
    let mut records = Vec::new();
    let source_label = LabelId(0);
    let target_label = LabelId(1);
    let rel_type = RelTypeId(0);
    for step in 0..256 {
        let node = NodeId(next_random(&mut random) % 11);
        let value = Value::Int((next_random(&mut random) % 17) as i64);
        let hop = (next_random(&mut random) % 3 + 1) as usize;
        for record in [
            StatsRecord::NodeProperty {
                label: source_label,
                property: "score".into(),
                value: value.clone(),
            },
            StatsRecord::RelProperty {
                rel_type,
                property: "weight".into(),
                value,
            },
            StatsRecord::IndexEntry {
                index: IndexId(step % 2),
                key: format!("key:{}\t\n", next_random(&mut random) % 13),
            },
            StatsRecord::RelSource { rel_type, node },
            StatsRecord::RelTarget { rel_type, node },
            StatsRecord::PathCount {
                source_label,
                rel_type,
                target_label,
            },
            StatsRecord::PathSource {
                source_label,
                rel_type,
                target_label,
                node,
            },
            StatsRecord::PathTarget {
                source_label,
                rel_type,
                target_label,
                node,
            },
            StatsRecord::BoundedPathCount {
                source_label,
                rel_type,
                target_label,
                hop,
            },
            StatsRecord::BoundedPathSource {
                source_label,
                rel_type,
                target_label,
                hop,
                node,
            },
            StatsRecord::BoundedPathTarget {
                source_label,
                rel_type,
                target_label,
                hop,
                node,
            },
        ] {
            if step % 5 == 0 {
                records.push(record.clone());
            }
            records.push(record);
        }
    }
    for index in (1..records.len()).rev() {
        let other = (next_random(&mut random) % (index + 1) as u64) as usize;
        records.swap(index, other);
    }
    records
}

fn check_seed(seed: u64) {
    let root = TestRoot::new();
    let catalog = catalog();
    let records = facts(seed);
    let expected = reference_statistics(&records, &catalog);
    for memory in [16 * 1024, 256 * 1024] {
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = options(memory);
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        for record in &records {
            writer.push(record.clone()).unwrap();
        }
        let (actual, report) = writer.finish(base_statistics()).unwrap();
        assert_eq!(actual, expected, "seed={seed}, memory={memory}");
        assert_eq!(report.generated_facts, records.len() as u64);
        assert_eq!(report.spill_run_count > 1, memory == 16 * 1024);
        let on_disk: u64 = fs::read_dir(spill.path())
            .unwrap()
            .map(|entry| entry.unwrap().metadata().unwrap().len())
            .sum();
        assert_eq!(report.spilled_bytes, on_disk);
        assert!(report.peak_buffer_bytes <= memory);
        assert!(report.output_statistics_bytes <= report.peak_buffer_bytes);
        drop(spill);
        root.assert_empty();
    }
}

#[test]
fn external_merge_matches_bag_and_set_statistics_across_run_layouts() {
    check_seed(0);
    check_seed(7);
}

#[test]
#[ignore = "explicit local statistics-refresh differential campaign"]
fn statistics_refresh_differential_campaign() {
    for seed in 0..64 {
        check_seed(seed);
    }
    println!("statistics refresh: 64 seeds, 128 run layouts, 433664 input facts");
}

#[test]
fn late_exclusion_discards_values_already_spilled_and_respects_declared_types() {
    let root = TestRoot::new();
    let mut catalog = catalog();
    let node_table = catalog.get_or_create_table(TableKind::Node, "Source");
    let rel_table = catalog.get_or_create_table(TableKind::Relationship, "LINKS");
    for table in [node_table, rel_table] {
        catalog.get_or_create_property(table, "body", PropertyType::Text, true);
        catalog.get_or_create_property(table, "list", PropertyType::List, true);
    }
    let spill = RefreshSpillDirectory::create(&root.0).unwrap();
    let options = options(16 * 1024);
    let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
    for property in ["mixed", "valid"] {
        writer
            .push_node_property(LabelId(0), property, &Value::Int(1))
            .unwrap();
        writer
            .push_relationship_property(RelTypeId(0), property, &Value::Int(2))
            .unwrap();
    }
    writer.flush().unwrap();
    for (property, value) in [
        ("mixed", Value::Binary(vec![1])),
        ("body", Value::String("text".into())),
        ("list", Value::Null),
    ] {
        writer
            .push_node_property(LabelId(0), property, &value)
            .unwrap();
        writer
            .push_relationship_property(RelTypeId(0), property, &value)
            .unwrap();
    }
    writer
        .push_node_property(LabelId(0), "mixed", &Value::Int(3))
        .unwrap();
    writer
        .push_relationship_property(RelTypeId(0), "mixed", &Value::Int(4))
        .unwrap();
    let (statistics, report) = writer.finish(base_statistics()).unwrap();
    assert_eq!(report.generated_facts, 4);
    assert_eq!(report.excluded_property_group_count, 3);
    assert_eq!(report.excluded_relationship_property_group_count, 3);
    assert_eq!(
        statistics.property_distinct_counts,
        BTreeMap::from([((LabelId(0), "valid".into()), 1)])
    );
    assert_eq!(
        statistics.rel_property_distinct_counts,
        BTreeMap::from([((RelTypeId(0), "valid".into()), 1)])
    );
    assert_eq!(
        statistics.property_histograms,
        BTreeMap::from([((LabelId(0), "valid".into()), vec![Value::Int(1)])])
    );
    assert_eq!(
        statistics.rel_property_histograms,
        BTreeMap::from([((RelTypeId(0), "valid".into()), vec![Value::Int(2)])])
    );
    drop(spill);
    root.assert_empty();
}

#[test]
fn index_samples_match_value_tuples_including_missing_null_and_nested_keys() {
    let root = TestRoot::new();
    let mut catalog = Catalog::default();
    let label = catalog.get_or_create_label("Items");
    let scalar = catalog.get_or_create_property_index(label, "key");
    let composite =
        catalog.get_or_create_composite_property_index(label, &["key".into(), "other".into()]);
    let empty = catalog.get_or_create_property_index(label, "absent");
    let full_text =
        catalog.get_or_create_property_index_with_kind(label, "body", IndexKind::FullText);
    let mut nodes = Vec::new();
    for value in [
        Value::Null,
        Value::Int(1),
        Value::Int(1),
        Value::String("a:b\n".into()),
        Value::List(vec![Value::Null]),
        Value::Map(BTreeMap::from([("a".into(), Value::Int(2))])),
    ] {
        nodes.push(NodeRecord {
            id: NodeId(nodes.len() as u64),
            labels: BTreeSet::from([label]),
            properties: BTreeMap::from([
                ("key".into(), value),
                ("other".into(), Value::String("value".into())),
                ("body".into(), Value::String("ignored".into())),
            ]),
        });
    }
    nodes[0].properties.remove("other");
    nodes.push(NodeRecord {
        id: NodeId(99),
        labels: BTreeSet::from([label]),
        properties: BTreeMap::new(),
    });
    let spill = RefreshSpillDirectory::create(&root.0).unwrap();
    let options = options(4096);
    let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
    for node in &nodes {
        writer.push_node_index_entries(node).unwrap();
        writer.flush().unwrap();
    }
    let (statistics, _) = writer.finish(base_statistics()).unwrap();
    for (index, properties) in [
        (scalar, vec!["key"]),
        (composite, vec!["key", "other"]),
        (empty, vec!["absent"]),
    ] {
        let keys = nodes
            .iter()
            .filter_map(|node| {
                properties
                    .iter()
                    .map(|property| node.properties.get(*property).cloned())
                    .collect::<Option<Vec<_>>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            statistics.index_samples[&index],
            IndexStatisticsSample::exact(
                keys.len() as u64,
                keys.iter().collect::<BTreeSet<_>>().len() as u64
            )
        );
    }
    assert!(!statistics.index_samples.contains_key(&full_text));
    drop(spill);
    root.assert_empty();
}

#[test]
fn sampled_histograms_preserve_hash_selection_and_adaptive_thresholds() {
    for (count, limit) in [
        (128, 128),
        (129, 128),
        (1024, 128),
        (1025, 256),
        (4096, 256),
        (4097, 512),
    ] {
        let root = TestRoot::new();
        let catalog = Catalog::default();
        let spill = RefreshSpillDirectory::create(&root.0).unwrap();
        let options = options(64 * 1024);
        let mut writer = StatsRunWriter::new(spill.path(), &catalog, &options);
        for value in (0..count).rev() {
            writer
                .push_node_property(LabelId(0), "score", &Value::Int(value))
                .unwrap();
        }
        let (statistics, report) = writer.finish(base_statistics()).unwrap();
        // Independent FNV-1a ranking of the existing integer wire representation.
        let mut ranked = (0..count)
            .map(|value| {
                let hash = format!("i{value}")
                    .bytes()
                    .fold(14_695_981_039_346_656_037u64, |hash, byte| {
                        (hash ^ u64::from(byte)).wrapping_mul(1_099_511_628_211)
                    });
                (hash, value)
            })
            .collect::<Vec<_>>();
        ranked.sort_unstable();
        ranked.truncate(512);
        let mut selected = ranked
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();
        selected.sort_unstable();
        let expected = if selected.len() <= limit {
            selected
        } else {
            (0..limit)
                .map(|index| selected[index * (selected.len() - 1) / (limit - 1)])
                .collect()
        }
        .into_iter()
        .map(Value::Int)
        .collect::<Vec<_>>();
        let key = (LabelId(0), "score".into());
        assert_eq!(
            statistics.property_histograms[&key], expected,
            "count={count}"
        );
        assert_eq!(statistics.property_distinct_counts[&key], count as u64);
        assert_eq!(
            statistics.sampled_property_histograms[&key],
            count as usize > limit
        );
        assert_eq!(adaptive_histogram_sample_limit(count as usize), limit);
        assert!(report.peak_buffer_bytes <= options.memory_budget_bytes);
        drop(spill);
        root.assert_empty();
    }
}

#[test]
fn index_statistics_keys_are_self_delimiting_and_spill_safe() {
    let left_values = [
        Value::String("a".to_string()),
        Value::String("bc".to_string()),
    ];
    let right_values = [
        Value::String("ab".to_string()),
        Value::String("c".to_string()),
    ];
    let left_refs = left_values.iter().collect::<Vec<_>>();
    let right_refs = right_values.iter().collect::<Vec<_>>();
    let left_bytes = index_statistics_key_bytes(&left_refs);
    let right_bytes = index_statistics_key_bytes(&right_refs);
    let left = encode_index_statistics_key(&left_refs, left_bytes);
    let right = encode_index_statistics_key(&right_refs, right_bytes);
    assert_ne!(left, right);

    let nested = Value::Map(BTreeMap::from([(
        "key:with-delimiters".to_string(),
        Value::List(vec![Value::Null, Value::String("line\nvalue".to_string())]),
    )]));
    let nested_refs = [&nested];
    let nested_bytes = index_statistics_key_bytes(&nested_refs);
    let nested_key = encode_index_statistics_key(&nested_refs, nested_bytes);
    assert_eq!(nested_key.len(), nested_bytes);

    let record = StatsRecord::IndexEntry {
        index: IndexId(7),
        key: nested_key,
    };
    assert_eq!(StatsRecord::decode(&record.encode()).unwrap(), record);
}

#[test]
fn refresh_resource_contract_rejects_invalid_limits_and_preserves_accounting_boundaries() {
    let root = TestRoot::new();
    let mut options = OptimizerStatisticsRefreshOptions {
        memory_budget_bytes: 4095,
        max_spill_bytes: 1,
        max_spill_runs: 1,
        max_input_records: 2,
        max_generated_facts: 1,
        max_path_expansions: 1,
        spill_directory: root.0.join("spill"),
    };
    assert!(matches!(
        options.validate(),
        Err(SkeinError::Semantic(message)) if message.contains("memory_budget_bytes must be at least 4096")
    ));

    options.memory_budget_bytes = 4096;
    options.validate().unwrap();
    let mut accounting = OptimizerStatisticsRefreshAccounting::new(&options);
    accounting.read_node().unwrap();
    accounting.read_relationship().unwrap();
    assert_eq!(accounting.node_records_read(), 1);
    assert_eq!(accounting.relationship_records_read(), 1);
    assert_execution_error(accounting.read_node(), "max_input_records 2");

    let mut path_accounting = OptimizerStatisticsRefreshAccounting::new(&options);
    path_accounting.expand_path().unwrap();
    assert_execution_error(path_accounting.expand_path(), "max_path_expansions 1");
}

#[test]
fn refresh_report_keeps_the_stable_json_protocol() {
    let report = OptimizerStatisticsRefreshReport {
        source_commit_epoch: 7,
        node_records_read: 11,
        relationship_records_read: 13,
        path_expansions: 17,
        generated_facts: 19,
        spill_run_count: 23,
        spilled_bytes: 29,
        peak_buffer_bytes: 31,
        output_statistics_bytes: 37,
        property_group_count: 41,
        relationship_property_group_count: 43,
        excluded_property_group_count: 47,
        excluded_relationship_property_group_count: 53,
        index_sample_count: 59,
        path_group_count: 61,
        bounded_path_group_count: 67,
        checkpoint_persisted: true,
    };
    assert_eq!(
        report.json()["protocol"],
        "skein-optimizer-statistics-refresh-v1"
    );
    assert_eq!(report.json()["node_records_read"], 11);
    assert_eq!(report.json()["checkpoint_persisted"], true);
}
