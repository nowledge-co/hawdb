use super::*;
use crate::NodeId;

#[test]
fn historical_decoder_tolerance_is_not_tightened() {
    let decoded = decode_payload(&reference::envelope(
        "row\t1\t\n\nSKEIN_SOURCE_SCAN_SEGMENT_V1\n",
    ))
    .unwrap();
    assert_eq!(
        decoded,
        vec![SourceScanRow {
            node_id: 1,
            properties: BTreeMap::new()
        }]
    );
    let (descriptor, checksum) = reference::descriptor_file("graph_epoch\t1\ngraph_epoch\t2\n");
    assert_eq!(
        decode_descriptor(&descriptor, checksum)
            .unwrap()
            .graph_epoch,
        2
    );
}

#[test]
fn source_scan_differential_smoke() {
    campaign(2, 8);
}

#[test]
#[ignore = "explicit local generated source sidecar campaign"]
fn source_scan_differential_campaign() {
    campaign(32, 64);
}

struct Generator(u64);
impl Generator {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn campaign(seeds: u64, cases_per_seed: usize) {
    let values = vec![
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(i64::MIN),
        Value::Int(i64::MAX),
        Value::Int(-9_007_199_254_740_993),
        Value::Int(-9_007_199_254_740_992),
        Value::Int(9_007_199_254_740_992),
        Value::Int(9_007_199_254_740_993),
        Value::Int(0),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Float(-2.5),
        Value::Float(3.5),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(f64::from_bits(0x7ff8_0000_0000_0001)),
        Value::String("".into()),
        Value::String("a\t;=:\n\0\u{65e5}\u{1f980}".into()),
        Value::String("2026-08-01T00:00:00Z".into()),
        Value::String("2026-08-01T08:00:00+08:00".into()),
        Value::String("1969-12-31T23:59:59.999Z".into()),
        Value::String("not-a-date".into()),
        Value::Binary(vec![0, 0xff, 0x80]),
        Value::Uuid(skein_core::Uuid::from_u128(7)),
        Value::List(vec![
            Value::Null,
            Value::Int(-1),
            Value::String("nested".into()),
        ]),
        Value::Map(BTreeMap::from([(
            "nested;=".into(),
            Value::Binary(vec![0, 1]),
        )])),
    ];
    let mut state_checks = 0;
    let mut disk_cases = 0;
    for seed in 0..seeds {
        let mut rng = Generator(seed + 1);
        for case in 0..cases_per_seed {
            let count = [0, 1, 127, 128, 129, 255, 256, 257][case % 8];
            let label = (case % 9 != 0).then_some(LabelId(7));
            let start_id = if case % 5 == 0 {
                u64::MAX - count as u64
            } else {
                seed * 1000
            };
            let nodes = (0..count)
                .map(|index| {
                    let item = values[(rng.next() % values.len() as u64) as usize].clone();
                    let mut properties = BTreeMap::from([("value".into(), item.clone())]);
                    if index % 11 != 0 {
                        properties.insert(
                            "id".into(),
                            if index % 3 == 0 {
                                item
                            } else {
                                Value::String(format!("id-{}", index % 13))
                            },
                        );
                    }
                    if index % 7 == 0 {
                        properties.insert("optional".into(), Value::Null);
                    }
                    if index % 5 == 0 {
                        properties.insert("".into(), Value::String("2026-08-02T00:00:00Z".into()));
                    }
                    NodeRecord {
                        id: NodeId(start_id + index as u64),
                        labels: BTreeSet::from([if rng.next().is_multiple_of(4) {
                            LabelId(8)
                        } else {
                            LabelId(7)
                        }]),
                        properties,
                    }
                })
                .collect::<Vec<_>>();
            let persist = case % 16 == (seed % 16) as usize;
            let epoch = if case % 7 == 0 { u64::MAX } else { rng.next() };
            check_projection(&nodes, label, epoch, persist);
            if persist {
                disk_cases += 1;
            }
            state_checks += 1;
        }
        state_checks += corruption_states(seed);
    }
    eprintln!("source-scan-differential-v1 seeds={seeds} generated_cases={} disk_cases={disk_cases} state_checks={state_checks}", seeds as usize * cases_per_seed);
}

fn corruption_states(seed: u64) -> usize {
    let directory = TestDir::new();
    let nodes = [source(seed + 1, LabelId(7), "state")];
    let mut projection = build(seed, Some(LabelId(7)), nodes.iter());
    let publication = write(directory.path(), &mut projection).unwrap();
    let checksum = publication.descriptor_checksum();
    let descriptor_path = directory.path().join(SOURCE_SCAN_DESCRIPTOR_FILE);
    let payload_path = directory.path().join(SOURCE_SCAN_PAYLOAD_FILE);
    let descriptor = fs::read(&descriptor_path).unwrap();
    let payload = fs::read(&payload_path).unwrap();
    assert!(load(directory.path(), seed, checksum).unwrap().is_some());
    assert!(load(directory.path(), seed + 1, checksum)
        .unwrap()
        .is_none());
    let mut changed = payload.clone();
    let index = seed as usize % changed.len();
    changed[index] ^= 1;
    fs::write(&payload_path, changed).unwrap();
    assert_eq!(
        storage_error(load(directory.path(), seed, checksum)),
        "source scan payload checksum mismatch"
    );
    fs::write(&payload_path, &payload).unwrap();
    assert!(load(directory.path(), seed, checksum).unwrap().is_some());
    let mut changed = descriptor.clone();
    changed[seed as usize % 16] ^= 1;
    fs::write(&descriptor_path, changed).unwrap();
    assert_eq!(
        storage_error(load(directory.path(), seed, checksum)),
        "source scan descriptor checksum mismatch"
    );
    fs::write(&descriptor_path, descriptor).unwrap();
    assert!(load(directory.path(), seed, checksum).unwrap().is_some());
    6
}

fn rows(nodes: &[NodeRecord], label: Option<LabelId>) -> Vec<SourceScanRow> {
    nodes
        .iter()
        .filter(|node| label.is_some_and(|label| node.labels.contains(&label)))
        .map(|node| SourceScanRow {
            node_id: node.id.0,
            properties: node.properties.clone(),
        })
        .collect()
}

fn unpack(bytes: &[u8]) -> String {
    let boundary = bytes.windows(2).position(|pair| pair == b"\n\n").unwrap();
    let header = std::str::from_utf8(&bytes[..boundary]).unwrap();
    let mut lines = header.lines();
    assert_eq!(lines.next(), Some("SKEIN_COMPRESSED_V1"));
    let fields = lines
        .map(|line| line.split_once('\t').unwrap())
        .collect::<BTreeMap<_, _>>();
    assert_eq!(fields.len(), 5);
    assert_eq!(fields["codec"], "zstd");
    let compressed = &bytes[boundary + 2..];
    let raw = zstd::stream::decode_all(compressed).unwrap();
    assert_eq!(
        fields["compressed_checksum"],
        reference::crc(compressed).to_string()
    );
    assert_eq!(
        fields["uncompressed_checksum"],
        reference::crc(&raw).to_string()
    );
    assert_eq!(fields["compressed_len"], compressed.len().to_string());
    assert_eq!(fields["uncompressed_len"], raw.len().to_string());
    String::from_utf8(raw).unwrap()
}

fn check_projection(nodes: &[NodeRecord], label: Option<LabelId>, epoch: u64, persist: bool) {
    let expected = rows(nodes, label);
    let mut projection = build(epoch, label, nodes.iter());
    assert_eq!(projection.graph_epoch, epoch);
    assert_eq!(projection.segments.len(), expected.len().div_ceil(128));
    let mut summaries = expected
        .chunks(128)
        .enumerate()
        .map(|(id, rows)| reference::summary(id as u64, rows))
        .collect::<Vec<_>>();
    let mut offset = 0;
    let mut ranges = Vec::new();
    let mut payloads = Vec::new();
    for (index, segment) in projection.segments.iter_mut().enumerate() {
        let expected_rows = &expected[index * 128..expected.len().min((index + 1) * 128)];
        assert_eq!(segment.rows, expected_rows);
        reference::validate_signed_zero_ties(
            &mut summaries[index],
            &segment.summary,
            expected_rows,
        );
        let raw = reference::payload(expected_rows);
        let payload = encode_segment_payload(expected_rows).unwrap();
        assert_eq!(unpack(&payload), raw);
        assert_eq!(
            decode_payload(&reference::envelope(&raw)).unwrap(),
            expected_rows
        );
        assert_eq!(decode_payload(&payload).unwrap(), expected_rows);
        let range = SegmentPayloadRange {
            artifact_id: 1,
            offset,
            length: NonZeroU64::new(payload.len() as u64).unwrap(),
            checksum: reference::crc(&payload),
        };
        offset += payload.len() as u64;
        segment.payload_range = Some(range);
        ranges.push(range);
        payloads.extend(payload);
    }
    let body = reference::descriptor(epoch, &summaries, &ranges);
    assert_eq!(encode_descriptor(&projection).unwrap(), body);
    let (file, checksum) = reference::descriptor_file(&body);
    let decoded = decode_descriptor(&file, checksum).unwrap();
    assert_eq!(decoded.graph_epoch, epoch);
    assert_eq!(encode_descriptor(&decoded).unwrap(), body);
    for (segment, expected) in decoded.segments.iter().zip(&projection.segments) {
        assert_eq!(segment.summary, expected.summary);
        assert_eq!(segment.payload_range, expected.payload_range);
        assert!(segment.rows.is_empty());
    }
    if persist {
        let directory = TestDir::new();
        let publication = write(directory.path(), &mut projection).unwrap();
        assert_eq!(publication.graph_epoch(), epoch);
        assert_eq!(publication.descriptor_checksum(), checksum);
        assert_eq!(
            fs::read_to_string(directory.path().join(SOURCE_SCAN_DESCRIPTOR_FILE)).unwrap(),
            file
        );
        assert_eq!(
            fs::read(directory.path().join(SOURCE_SCAN_PAYLOAD_FILE)).unwrap(),
            payloads
        );
        let loaded = load(directory.path(), epoch, checksum).unwrap().unwrap();
        assert_eq!(loaded.graph_epoch(), epoch);
        assert_eq!(
            loaded
                .segments()
                .iter()
                .map(|segment| segment.summary.clone())
                .collect::<Vec<_>>(),
            summaries
        );
        assert_eq!(
            loaded
                .segments()
                .iter()
                .map(|segment| segment.payload_range)
                .collect::<Vec<_>>(),
            ranges
        );
        assert!(!directory
            .path()
            .join("source_scan_segments.skein.tmp")
            .exists());
        assert!(!directory
            .path()
            .join("source_scan_segment_payloads.skein.tmp")
            .exists());
    }
}

#[test]
fn signed_zero_bounds_preserve_supported_encodings_and_wire_bits() {
    let range = SegmentPayloadRange {
        artifact_id: 1,
        offset: 0,
        length: NonZeroU64::new(1).unwrap(),
        checksum: 0,
    };
    for values in [
        vec![Value::Float(0.0), Value::Float(-0.0)],
        vec![Value::Float(-0.0), Value::Int(0)],
        vec![Value::Float(-0.0), Value::Float(0.0), Value::Float(-0.0)],
        vec![Value::Float(-1.0), Value::Float(0.0), Value::Float(-0.0)],
        vec![Value::Float(-0.0), Value::Float(0.0), Value::Float(1.0)],
    ] {
        let nodes = values
            .iter()
            .enumerate()
            .map(|(index, value)| NodeRecord {
                id: NodeId(index as u64),
                labels: BTreeSet::from([LabelId(7)]),
                properties: BTreeMap::from([("id".into(), value.clone())]),
            })
            .collect::<Vec<_>>();
        check_projection(&nodes, Some(LabelId(7)), 42, true);
        let rows = rows(&nodes, Some(LabelId(7)));
        for min in [-0.0_f64, 0.0] {
            for max in [-0.0_f64, 0.0] {
                let mut expected = reference::summary(0, &rows);
                let mut actual = expected.clone();
                let mut bounds = actual.fields["id"].numeric_min_max.unwrap();
                if bounds.min == 0.0 {
                    bounds.min = min;
                }
                if bounds.max == 0.0 {
                    bounds.max = max;
                }
                actual.fields.get_mut("id").unwrap().numeric_min_max = Some(bounds);
                reference::validate_signed_zero_ties(&mut expected, &actual, &rows);
                let body = reference::descriptor(42, &[expected], &[range]);
                let (file, checksum) = reference::descriptor_file(&body);
                let decoded = decode_descriptor(&file, checksum).unwrap();
                assert_eq!(encode_descriptor(&decoded).unwrap(), body);
                let decoded_bounds = decoded.segments[0].summary.fields["id"]
                    .numeric_min_max
                    .unwrap();
                assert_eq!(decoded_bounds.min.to_bits(), bounds.min.to_bits());
                assert_eq!(decoded_bounds.max.to_bits(), bounds.max.to_bits());
            }
        }
    }
}

#[test]
fn signed_zero_oracle_rejects_unobserved_signs_and_wrong_bounds() {
    for (values, wrong) in [
        (vec![Value::Float(0.0)], -0.0),
        (vec![Value::Int(0)], -0.0),
        (vec![Value::Float(-0.0)], 0.0),
        (
            vec![Value::Float(0.0), Value::Float(-0.0)],
            f64::from_bits(1),
        ),
        (vec![Value::Float(0.0), Value::Float(-0.0)], f64::INFINITY),
        (vec![Value::Float(0.0), Value::Float(-0.0)], f64::NAN),
        (vec![Value::Float(-1.0), Value::Float(1.0)], 0.0),
    ] {
        let rows = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| SourceScanRow {
                node_id: index as u64,
                properties: BTreeMap::from([("id".into(), value)]),
            })
            .collect::<Vec<_>>();
        for change_min in [false, true] {
            let expected = reference::summary(0, &rows);
            let mut actual = expected.clone();
            let bounds = actual
                .fields
                .get_mut("id")
                .unwrap()
                .numeric_min_max
                .as_mut()
                .unwrap();
            if change_min {
                bounds.min = wrong;
            } else {
                bounds.max = wrong;
            }
            assert!(
                std::panic::catch_unwind(|| {
                    let mut expected = expected.clone();
                    reference::validate_signed_zero_ties(&mut expected, &actual, &rows);
                })
                .is_err(),
                "oracle accepted wrong numeric bits {}",
                wrong.to_bits()
            );
        }
    }
}

#[test]
fn segment_boundaries_and_label_selection_match_independent_model() {
    for count in [0, 1, 127, 128, 129, 255, 256, 257] {
        let nodes = (1..=count)
            .map(|id| source(id, LabelId(7), "space"))
            .collect::<Vec<_>>();
        check_projection(&nodes, Some(LabelId(7)), 42, true);
        check_projection(&nodes, Some(LabelId(8)), 42, false);
        check_projection(&nodes, None, 42, true);
    }
}

fn write_reference(directory: &Path, body: &str, payload: &[u8]) -> u64 {
    let (descriptor, checksum) = reference::descriptor_file(body);
    fs::write(directory.join(SOURCE_SCAN_DESCRIPTOR_FILE), descriptor).unwrap();
    fs::write(directory.join(SOURCE_SCAN_PAYLOAD_FILE), payload).unwrap();
    checksum
}

fn storage_error<T: std::fmt::Debug>(result: Result<T>) -> String {
    match result {
        Err(SkeinError::Storage(message)) => message,
        other => panic!("expected storage error, got {other:?}"),
    }
}

#[test]
fn missing_sidecar_and_stale_epoch_preserve_fallback_and_validation_order() {
    let directory = TestDir::new();
    let path = directory.path();
    fs::write(path.join("source_scan_segments.skein.tmp"), b"unfinished").unwrap();
    assert!(load(path, 7, 0).unwrap().is_none());
    let (descriptor, checksum) =
        reference::descriptor_file("SKEIN_SOURCE_SCAN_SEGMENTS_V1\ngraph_epoch\t7\n");
    fs::write(path.join(SOURCE_SCAN_DESCRIPTOR_FILE), descriptor).unwrap();
    // A stale epoch must not try to open an absent payload; checksum admission still precedes it.
    assert!(load(path, 8, checksum).unwrap().is_none());
    assert_eq!(
        storage_error(load(path, 8, checksum ^ 1)),
        "source scan descriptor checksum mismatch"
    );
    assert!(load(path, 7, checksum).is_err());
    fs::write(path.join(SOURCE_SCAN_PAYLOAD_FILE), b"").unwrap();
    assert!(load(path, 7, checksum)
        .unwrap()
        .unwrap()
        .segments()
        .is_empty());
}

#[test]
fn descriptor_rejects_malformed_fields_with_stable_errors() {
    let cases = [
        ("", "source scan descriptor missing graph epoch"),
        (
            "graph_epoch\t7\nfield\t6964\t1\t0\t0\t\t\t\t\t\n",
            "source scan field appears before a segment",
        ),
        (
            "graph_epoch\t7\nexact\t6964\t733631\t0\n",
            "source scan exact cursor appears before a segment",
        ),
        (
            "graph_epoch\t7\nsegment\t0\t1\t0\t0\t0\t1\n",
            "source scan payload length is zero",
        ),
        (
            "graph_epoch\t7\nsegment\t0\t1\t0\t1\t0\t1\nfield\t6964\t2\t0\t0\t\t\t\t\t\n",
            "invalid source scan field counts",
        ),
        (
            "graph_epoch\t7\nsegment\t0\t1\t0\t1\t0\t1\nexact\t6964\t733631\t0\n",
            "source scan exact cursor references unknown field",
        ),
        (
            "unknown\tvalue\n",
            "invalid source scan descriptor line: unknown\tvalue",
        ),
    ];
    for (body, expected) in cases {
        let (file, checksum) = reference::descriptor_file(body);
        assert_eq!(storage_error(decode_descriptor(&file, checksum)), expected);
    }
    assert_eq!(
        storage_error(decode_descriptor("graph_epoch\t7\n", 0)),
        "source scan descriptor missing checksum footer"
    );
    assert_eq!(
        storage_error(decode_row_ids("2,1")),
        "source scan exact row ids are not ordered"
    );
    assert_eq!(
        storage_error(decode_row_ids("1,1")),
        "source scan exact row ids are not ordered"
    );
}

#[test]
fn payload_preserves_order_and_rejects_malformed_rows() {
    for (raw, expected) in [
        (
            "SKEIN_SOURCE_SCAN_SEGMENT_V1\nrow\t2\t\nrow\t1\t\n",
            "source scan segment rows are not strictly ordered",
        ),
        (
            "SKEIN_SOURCE_SCAN_SEGMENT_V1\nrow\t1\t\nrow\t1\t\n",
            "source scan segment rows are not strictly ordered",
        ),
        (
            "SKEIN_SOURCE_SCAN_SEGMENT_V1\nunknown\t1\n",
            "invalid source scan segment line: unknown\t1",
        ),
    ] {
        assert_eq!(
            storage_error(decode_payload(&reference::envelope(raw))),
            expected
        );
    }
    assert_eq!(
        storage_error(decode_payload(b"row\t1\t\n")),
        "source scan segment is missing the V1 compressed envelope"
    );
    // The move does not silently sort a caller's unordered node iterator.
    let nodes = [
        source(2, LabelId(7), "space"),
        source(1, LabelId(7), "space"),
    ];
    let projection = build(7, Some(LabelId(7)), nodes.iter());
    assert_eq!(projection.segments[0].rows[0].node_id, 2);
    assert!(
        decode_payload(&encode_segment_payload(&projection.segments[0].rows).unwrap()).is_err()
    );
}

#[test]
fn load_validates_artifact_ranges_checksums_rows_and_manifest_order() {
    let directory = TestDir::new();
    let path = directory.path();
    let raw = "SKEIN_SOURCE_SCAN_SEGMENT_V1\nrow\t1\t\n";
    let payload = reference::envelope(raw);
    let len = payload.len() as u64;
    let crc = reference::crc(&payload);
    for (id, artifact, offset, length, checksum, count, expected) in [
        (
            0,
            2,
            0,
            len,
            crc,
            1,
            "source scan descriptor has unsupported artifact",
        ),
        (
            0,
            1,
            u64::MAX,
            len,
            crc,
            1,
            "source scan payload range overflow",
        ),
        (
            0,
            1,
            1,
            len,
            crc,
            1,
            "source scan payload range exceeds artifact",
        ),
        (
            0,
            1,
            0,
            len,
            crc ^ 1,
            1,
            "source scan payload checksum mismatch",
        ),
        (
            0,
            1,
            0,
            len,
            crc,
            2,
            "source scan payload row count mismatch",
        ),
        (
            1,
            1,
            0,
            len,
            crc,
            1,
            "scan segment manifest expected id 0, got 1",
        ),
    ] {
        let body = format!("SKEIN_SOURCE_SCAN_SEGMENTS_V1\ngraph_epoch\t7\nsegment\t{id}\t{artifact}\t{offset}\t{length}\t{checksum}\t{count}\n");
        let binding = write_reference(path, &body, &payload);
        assert_eq!(storage_error(load(path, 7, binding)), expected);
    }
    let body = format!("SKEIN_SOURCE_SCAN_SEGMENTS_V1\ngraph_epoch\t7\nsegment\t0\t1\t0\t{len}\t{crc}\t1\nsegment\t1\t1\t0\t{len}\t{crc}\t1\n");
    let binding = write_reference(path, &body, &payload);
    assert_eq!(
        storage_error(load(path, 7, binding)),
        "scan segment manifest has overlapping artifact 1"
    );
    let body = format!(
        "SKEIN_SOURCE_SCAN_SEGMENTS_V1\ngraph_epoch\t7\nsegment\t0\t1\t0\t{len}\t{crc}\t1\n"
    );
    let binding = write_reference(path, &body, &payload);
    assert!(load(path, 7, binding).unwrap().is_some());
    for length in [0, 1, payload.len() / 2, payload.len() - 1] {
        fs::write(path.join(SOURCE_SCAN_PAYLOAD_FILE), &payload[..length]).unwrap();
        assert_eq!(
            storage_error(load(path, 7, binding)),
            "source scan payload range exceeds artifact"
        );
    }
}

fn source(id: u64, source_label: LabelId, scope: &str) -> NodeRecord {
    NodeRecord {
        id: NodeId(id),
        labels: BTreeSet::from([source_label]),
        properties: BTreeMap::from([
            ("id".to_string(), Value::String(format!("source-{id}"))),
            ("space_id".to_string(), Value::String(scope.to_string())),
            (
                "created_at".to_string(),
                Value::String("2026-08-01T00:00:00Z".to_string()),
            ),
        ]),
    }
}

#[test]
fn sidecar_round_trips_payload_ranges_and_exact_cursors() {
    let directory = TestDir::new();
    let directory = directory.path();
    let source_label = LabelId(7);
    let nodes = [
        source(1, source_label, "alpha"),
        source(2, source_label, "beta"),
    ];
    let mut projection = build(4, Some(source_label), nodes.iter());
    let publication = write(directory, &mut projection).unwrap();

    let manifest = load(directory, 4, publication.descriptor_checksum())
        .unwrap()
        .unwrap();
    assert_eq!(manifest.graph_epoch(), 4);
    assert_eq!(manifest.segments().len(), 1);
    let plan = manifest.plan_scan(
        4,
        &crate::ScanPredicate::Eq {
            property: "id".to_string(),
            value: Value::String("source-1".to_string()),
        },
    );
    let crate::ScanSegmentAccessPlan::Read(plan) = plan else {
        panic!("expected scan");
    };
    assert_eq!(plan.segments.len(), 1);
    assert_eq!(plan.segments[0].candidates.as_ref().unwrap().remaining(), 1);
}

struct TestDir(std::path::PathBuf);
impl TestDir {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let serial = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "skein-source-scan-{}-{nonce}-{serial}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
