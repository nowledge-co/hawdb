use super::*;
use std::collections::BTreeMap;

mod fixtures;
use fixtures::{INITIAL, PUBLISHED};

fn digest(value: u8) -> Sha256Digest {
    Sha256Digest::from_bytes([value; 32])
}

fn published() -> DurableManifest {
    DurableManifest {
        checkpoint_generation: Some(7),
        checkpoint_encoded_len: Some(101),
        checkpoint_encoded_checksum: Some(102),
        checkpoint_encoded_sha256: Some(digest(1)),
        canonical_manifest_encoded_len: Some(201),
        canonical_manifest_encoded_checksum: Some(202),
        canonical_manifest_encoded_sha256: Some(digest(2)),
        canonical_adjacency_generation_artifacts: Some(CanonicalAdjacencyGenerationArtifacts {
            generation: 7,
            source_commit_epoch: 42,
            relationship_count: 3,
            entry_count: 6,
            adjacency_artifact: CanonicalAdjacencyArtifactMetadata {
                encoded_len: 301,
                encoded_crc32c: 302,
                encoded_sha256: digest(3),
            },
            descriptor_root_artifact: GraphDescriptorTreeArtifactMetadata {
                encoded_len: 303,
                encoded_crc32c: 304,
                encoded_sha256: digest(4),
            },
        }),
        property_spill_manifest_encoded_len: Some(401),
        property_spill_manifest_encoded_checksum: Some(402),
        property_spill_manifest_encoded_sha256: Some(digest(5)),
        property_projection_manifest_encoded_len: Some(501),
        property_projection_manifest_encoded_checksum: Some(502),
        property_projection_manifest_encoded_sha256: Some(digest(6)),
        relational_row_generation_artifacts: Some(RelationalRowPageGenerationArtifacts {
            generation: 7,
            source_commit_epoch: 42,
            root_set_digest: digest(8),
            manifest_artifact: RelationalRowPageArtifactMetadata {
                encoded_len: 601,
                encoded_crc32c: 602,
                encoded_sha256: digest(7),
            },
        }),
        relational_overflow_generation_artifacts: Some(RelationalOverflowGenerationArtifacts {
            generation: 7,
            source_commit_epoch: 42,
            root_set_digest: digest(10),
            manifest_artifact: RelationalOverflowArtifactMetadata {
                encoded_len: 701,
                encoded_crc32c: 702,
                encoded_sha256: digest(9),
            },
        }),
        relational_index_generation_artifacts: Some(RelationalIndexGenerationArtifacts {
            generation: 7,
            source_commit_epoch: 42,
            catalog_schema_digest: digest(14),
            root_set_digest: digest(13),
            page_artifact: RelationalIndexArtifactMetadata {
                encoded_len: 801,
                encoded_crc32c: 802,
                encoded_sha256: digest(11),
            },
            manifest_artifact: RelationalIndexArtifactMetadata {
                encoded_len: 803,
                encoded_crc32c: 804,
                encoded_sha256: digest(12),
            },
        }),
        append_generation_artifacts: Some(AppendGenerationArtifacts {
            generation: 7,
            source_commit_epoch: 42,
            root_set_digest: digest(16),
            manifest_artifact: AppendSegmentArtifactMetadata {
                encoded_len: 901,
                encoded_crc32c: 902,
                encoded_sha256: digest(15),
            },
        }),
        wal_generation: 7,
        checkpoint_epoch: 7,
        checkpoint_commit_epoch: 42,
        oldest_reader_commit_epoch: Some(41),
        safe_reclaim_commit_epoch: 40,
        wal_replay_start_lsn: 40,
        next_lsn: 45,
        source_scan_commit_epoch: Some(39),
        source_scan_descriptor_checksum: Some(1001),
    }
}

fn fields(text: &str) -> Vec<(String, String)> {
    text.lines()
        .skip(1)
        .filter_map(|line| {
            let (key, value) = line.split_once('\t')?;
            (key != "checksum").then(|| (key.to_string(), value.to_string()))
        })
        .collect()
}

fn frame(fields: &[(String, String)]) -> String {
    let mut body = "SKEIN_MANIFEST_V1\n".to_string();
    for (key, value) in fields {
        body.push_str(&format!("{key}\t{value}\n"));
    }
    seal(&body)
}

fn seal(body: &str) -> String {
    format!(
        "{body}checksum\t{}\n",
        skein_integrity::checksum_u64(body.as_bytes())
    )
}

fn replace(fields: &mut [(String, String)], key: &str, value: impl ToString) {
    fields.iter_mut().find(|(name, _)| name == key).unwrap().1 = value.to_string();
}

fn rejected(text: &str) -> String {
    let result = std::panic::catch_unwind(|| DurableManifest::decode(text));
    let error = result
        .expect("manifest decoder panicked")
        .expect_err("invalid manifest admitted");
    assert!(matches!(&error, SkeinError::Storage(_)), "{error:?}");
    error.to_string()
}

#[test]
fn frozen_v1_bytes_preserve_all_binding_fields_and_paths() {
    for (expected, bytes) in [
        (DurableManifest::default(), INITIAL),
        (published(), PUBLISHED),
    ] {
        expected.validate().unwrap();
        assert_eq!(expected.encode(), bytes);
        let decoded = DurableManifest::decode(bytes).unwrap();
        // Retain the original model's trait surface while comparing every field.
        assert_eq!(format!("{decoded:?}"), format!("{expected:?}"));
        assert_eq!(decoded.encode(), bytes);
    }
    assert_eq!(
        published().checkpoint_path(Path::new("root")),
        Path::new("root").join("checkpoint.7.skein")
    );
    assert_eq!(
        published().wal_path(Path::new("root")),
        Path::new("root").join("wal.7.skein")
    );
}

#[test]
fn field_inventory_and_partial_bindings_fail_closed() {
    let mut cases = 0;
    for text in [INITIAL, PUBLISHED] {
        let original = fields(text);
        assert_eq!(original.len(), 61);
        for (index, (key, _)) in original.iter().enumerate() {
            let mut changed = original.clone();
            changed.remove(index);
            if text == INITIAL && key.starts_with("append_") {
                assert!(DurableManifest::decode(&frame(&changed))
                    .unwrap()
                    .append_generation_artifacts
                    .is_none());
            } else {
                rejected(&frame(&changed));
            }
            changed = original.clone();
            changed.push(original[index].clone());
            assert!(rejected(&frame(&changed)).contains(&format!("duplicate field: {key}")));
            changed = original.clone();
            changed[index].1 = "invalid-\u{2603}".to_string();
            rejected(&frame(&changed));
            cases += 3;
        }
    }
    let original = fields(PUBLISHED);
    for prefix in [
        "canonical_adjacency_",
        "relational_row_",
        "relational_overflow_",
        "relational_index_",
        "append_",
        "checkpoint_encoded_",
        "canonical_manifest_encoded_",
        "property_spill_manifest_encoded_",
        "property_projection_manifest_encoded_",
    ] {
        let group = original
            .iter()
            .enumerate()
            .filter(|(_, (key, _))| key.starts_with(prefix))
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert!(!group.is_empty());
        // Enumerate every non-empty incomplete binding, not only one missing field.
        for mask in 1..(1usize << group.len()) - 1 {
            let mut changed = original.clone();
            for (bit, &index) in group.iter().enumerate() {
                if mask & (1 << bit) == 0 {
                    changed[index].1 = "none".to_string();
                }
            }
            rejected(&frame(&changed));
            cases += 1;
        }
    }
    assert_eq!(cases, 2620);
    eprintln!("durable-manifest-field-matrix-v1 cases={cases}");
}

#[test]
fn generation_lsn_and_artifact_ranges_preserve_validation() {
    for (key, value) in [
        ("wal_replay_start_lsn", "0"),
        ("next_lsn", "0"),
        ("next_lsn", "39"),
        ("wal_generation", "8"),
        ("checkpoint_generation", "8"),
        ("checkpoint_generation", "none"),
        ("checkpoint_commit_epoch", "43"),
        (
            "canonical_adjacency_relationship_count",
            "18446744073709551615",
        ),
        ("canonical_adjacency_entry_count", "5"),
        ("canonical_adjacency_artifact_encoded_len", "0"),
        ("canonical_adjacency_descriptor_root_encoded_len", "0"),
        ("relational_row_manifest_encoded_len", "0"),
        ("relational_overflow_manifest_encoded_len", "0"),
        ("relational_index_manifest_encoded_len", "0"),
        ("append_manifest_encoded_len", "0"),
        (
            "canonical_adjacency_descriptor_root_encoded_crc32c",
            "4294967296",
        ),
        ("relational_row_manifest_encoded_checksum", "4294967296"),
        (
            "relational_overflow_manifest_encoded_checksum",
            "4294967296",
        ),
        ("append_manifest_encoded_checksum", "4294967296"),
    ] {
        let mut changed = fields(PUBLISHED);
        replace(&mut changed, key, value);
        rejected(&frame(&changed));
    }
    for prefix in [
        "canonical_adjacency",
        "relational_row",
        "relational_overflow",
        "relational_index",
        "append",
    ] {
        for (suffix, value) in [
            ("generation", 0),
            ("generation", 8),
            ("source_commit_epoch", 43),
        ] {
            let mut changed = fields(PUBLISHED);
            replace(&mut changed, &format!("{prefix}_{suffix}"), value);
            rejected(&frame(&changed));
        }
    }
    // These three existing fields have a u64 wire/domain type, unlike the four
    // u32 fields above. Extraction must not silently tighten their admission.
    for key in [
        "canonical_adjacency_artifact_encoded_crc32c",
        "relational_index_page_encoded_checksum",
        "relational_index_manifest_encoded_checksum",
    ] {
        let mut changed = fields(PUBLISHED);
        replace(&mut changed, key, u64::MAX);
        assert_eq!(
            DurableManifest::decode(&frame(&changed)).unwrap().encode(),
            frame(&changed)
        );
    }
}

#[test]
fn compatibility_defaults_and_checksum_guards_remain_exact() {
    let mut no_append = fields(PUBLISHED);
    no_append.retain(|(key, _)| !key.starts_with("append_"));
    assert!(DurableManifest::decode(&frame(&no_append))
        .unwrap()
        .append_generation_artifacts
        .is_none());
    for oldest in [None, Some(0), Some(1), Some(41), Some(u64::MAX)] {
        let mut changed = fields(PUBLISHED);
        replace(&mut changed, "safe_reclaim_commit_epoch", 0);
        replace(
            &mut changed,
            "oldest_reader_commit_epoch",
            oldest.map_or_else(|| "none".to_string(), |v| v.to_string()),
        );
        let decoded = DurableManifest::decode(&frame(&changed)).unwrap();
        let expected = match oldest {
            None => 42,
            Some(0) => 0,
            Some(value) => value - 1,
        };
        assert_eq!(decoded.safe_reclaim_commit_epoch, expected);
    }
    let corrupt_payload = PUBLISHED.replace("next_lsn\t45\n", "next_lsn\t46\n");
    assert!(rejected(&corrupt_payload).contains("checksum mismatch"));
    let mut corrupt = PUBLISHED.to_string();
    corrupt.insert(0, '\n');
    assert!(rejected(&corrupt).contains("checksum mismatch"));
    let (body, _) = PUBLISHED.rsplit_once("checksum\t").unwrap();
    // The existing parser also recognizes the suffix of encoded-checksum keys.
    // Removing only the footer therefore reaches checksum validation first.
    assert!(rejected(body).contains("checksum mismatch"));
    assert!(rejected("SKEIN_MANIFEST_V1\nversion\tskein-storage-v1\n").contains("checksum footer"));
    assert!(rejected(&seal(&body.replacen(
        "SKEIN_MANIFEST_V1",
        "INVALID_HEADER",
        1
    )))
    .contains("V1 format header"));
    assert!(DurableManifest::decode(PUBLISHED.trim_end()).is_ok());
    let mut unsupported = fields(PUBLISHED);
    replace(&mut unsupported, "version", "skein-storage-v0");
    assert!(rejected(&frame(&unsupported)).contains("unsupported storage version"));
}

#[test]
fn durable_manifest_differential_smoke() {
    assert_eq!(campaign(4, 16), 260);
}

#[test]
#[ignore = "explicit local durable manifest mutation campaign"]
fn durable_manifest_differential_campaign() {
    assert_eq!(campaign(128, 64), 32896);
}

fn campaign(seeds: u64, cases_per_seed: usize) -> usize {
    let mut cases = 0;
    for seed in 0..seeds {
        let mut rng = Generator(seed + 1);
        let mut valid = fields(PUBLISHED);
        let generation = 1 + rng.below(u64::MAX - 1);
        let commit = 1 + rng.below(u64::MAX - 1);
        for key in [
            "checkpoint_generation",
            "checkpoint_epoch",
            "wal_generation",
            "canonical_adjacency_generation",
            "relational_row_generation",
            "relational_overflow_generation",
            "relational_index_generation",
            "append_generation",
        ] {
            replace(&mut valid, key, generation);
        }
        for key in [
            "checkpoint_commit_epoch",
            "canonical_adjacency_source_commit_epoch",
            "relational_row_source_commit_epoch",
            "relational_overflow_source_commit_epoch",
            "relational_index_source_commit_epoch",
            "append_source_commit_epoch",
        ] {
            replace(&mut valid, key, commit);
        }
        let expected = valid.iter().cloned().collect::<BTreeMap<_, _>>();
        // A field-table oracle also admits reordering, while the writer must
        // still emit its frozen canonical order and preserve every payload.
        for index in (1..valid.len()).rev() {
            let other = rng.below((index + 1) as u64) as usize;
            valid.swap(index, other);
        }
        let decoded = DurableManifest::decode(&frame(&valid)).unwrap();
        assert_eq!(
            fields(&decoded.encode())
                .into_iter()
                .collect::<BTreeMap<_, _>>(),
            expected
        );
        assert_eq!(decoded.checkpoint_epoch, generation);
        assert_eq!(decoded.checkpoint_commit_epoch, commit);
        cases += 1;
        for _ in 0..cases_per_seed {
            let index = rng.below(valid.len() as u64) as usize;
            for mutation in 0..4 {
                let mut changed = valid.clone();
                match mutation {
                    0 => {
                        changed.remove(index);
                    }
                    1 => changed.push(changed[index].clone()),
                    2 => changed[index].1 = "\0invalid-\u{2603}".to_string(),
                    _ => changed.push((format!("unknown_{}", rng.below(100)), "1".to_string())),
                }
                rejected(&frame(&changed));
                cases += 1;
            }
        }
    }
    eprintln!("durable-manifest-mutation-v1 seeds={seeds} cases={cases}");
    cases
}

#[test]
fn file_round_trip_and_failed_publication_preserve_selected_bytes() {
    let directory = TestDirectory::new();
    let path = directory.0.join("manifest.skein");
    DurableManifest::default().write(&path).unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), INITIAL);
    published().write(&path).unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), PUBLISHED);
    assert_eq!(DurableManifest::load(&path).unwrap().encode(), PUBLISHED);
    let tmp = path.with_extension("skein.tmp");
    assert!(!tmp.exists());
    fs::create_dir(&tmp).unwrap();
    assert!(DurableManifest::default().write(&path).is_err());
    assert_eq!(fs::read_to_string(&path).unwrap(), PUBLISHED);
    fs::remove_dir(&tmp).unwrap();
    fs::write(&path, [0xff, 0xfe]).unwrap();
    assert!(DurableManifest::load(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), [0xff, 0xfe]);
    assert!(!tmp.exists());
}

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "skein-durable-manifest-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Generator(u64);

impl Generator {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}
