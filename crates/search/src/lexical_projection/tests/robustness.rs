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

mod admission;

struct Fixture {
    root: PathBuf,
    documents: BTreeMap<String, SearchDocument>,
    analyzer: SearchAnalyzerLexicon,
    reader: Arc<LexicalProjectionReader>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let root = projection_root(name);
        fs::create_dir(&root).unwrap();
        let documents = BTreeMap::from([
            ("a".to_string(), document("a", "graph", "storage")),
            ("b".to_string(), document("b", "graph", "memory")),
        ]);
        let analyzer = SearchAnalyzerLexicon::default();
        let reader = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
            .write(&root, 1, Some(7), 11, 13, documents.values(), &analyzer)
            .unwrap();
        Self {
            root,
            documents,
            analyzer,
            reader,
        }
    }

    fn reopen(
        &self,
        config: LexicalProjectionConfig,
    ) -> Result<Option<Arc<LexicalProjectionReader>>> {
        LexicalProjectionReader::load(&self.root, Some(7), 11, 13, config)
    }

    fn scores(&self, delta: &LexicalMiniDelta) -> BTreeMap<String, f64> {
        self.reader
            .score(&BTreeSet::from(["graph".to_string()]), delta, None, |_| {
                Ok(true)
            })
            .unwrap()
            .scores
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn assert_delta_snapshot(
    fixture: &Fixture,
    delta: &LexicalMiniDelta,
    documents: &BTreeMap<String, SearchDocument>,
) {
    let tokens = |document: &SearchDocument| {
        analysis_tests::reference_document_tokens(document, &fixture.analyzer)
    };
    let terms = fixture
        .documents
        .values()
        .chain(documents.values())
        .flat_map(tokens)
        .collect::<BTreeSet<_>>();
    analysis_tests::assert_projection_scores(
        &fixture.reader,
        delta,
        documents,
        &fixture.analyzer,
        &terms,
    );
    let expected_len = documents
        .values()
        .map(|document| tokens(document).len() as u64)
        .sum();
    assert_eq!(
        delta.projected_corpus(
            fixture.reader.manifest.document_count,
            fixture.reader.manifest.total_document_len,
        ),
        (documents.len(), expected_len),
    );
    let resident = |document: &SearchDocument| {
        document.id.len() as u64
            + 64
            + tokens(document)
                .into_iter()
                .collect::<BTreeSet<_>>()
                .iter()
                .map(|term| term.len() as u64 + 32)
                .sum::<u64>()
    };
    let expected_bytes = delta
        .upserts
        .keys()
        .map(|id| resident(&documents[id]) + fixture.documents.get(id).map_or(0, resident))
        .chain(
            delta
                .deletes
                .keys()
                .map(|id| resident(&fixture.documents[id])),
        )
        .sum::<u64>();
    assert_eq!(delta.resident_bytes, expected_bytes);
}

#[test]
fn retained_delta_snapshots_survive_mutations_and_budget_rejection() {
    let fixture = Fixture::new("delta-snapshots");
    let config = LexicalProjectionConfig::default();
    let mut active = Arc::new(LexicalMiniDelta::default());
    let mut documents = fixture.documents.clone();
    let mut retained = Vec::new();
    for next in [
        Some(document("a", "graph", "first replacement")),
        Some(document("a", "memory", "second replacement")),
        None,
        Some(document("a", "graph", "restored storage")),
    ] {
        retained.push((Arc::clone(&active), documents.clone()));
        if let Some(next) = next {
            active
                .upsert(&next, documents.get("a"), &fixture.analyzer, config)
                .unwrap();
            documents.insert("a".into(), next);
        } else {
            assert!(active
                .delete("a", documents.get("a"), &fixture.analyzer, config)
                .unwrap());
            documents.remove("a");
        }
        assert_delta_snapshot(&fixture, &active, &documents);
        for (snapshot, expected) in &retained {
            assert_delta_snapshot(&fixture, snapshot, expected);
        }
    }
    let exact = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(active.resident_bytes).unwrap(),
        ..config
    };
    let next = documents["a"].clone();
    retained.push((Arc::clone(&active), documents.clone()));
    active
        .upsert(&next, None, &fixture.analyzer, exact)
        .unwrap();
    let short = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(active.resident_bytes - 1).unwrap(),
        ..config
    };
    assert!(active
        .upsert(&next, None, &fixture.analyzer, short)
        .is_err());
    assert!(!active
        .delete("b", documents.get("b"), &fixture.analyzer, short)
        .unwrap());
    let invalid = document("a", "graph", &"x".repeat(4097));
    assert!(active
        .upsert(&invalid, None, &fixture.analyzer, config)
        .is_err());
    assert_delta_snapshot(&fixture, &active, &documents);
    for (snapshot, expected) in retained {
        assert_delta_snapshot(&fixture, &snapshot, &expected);
    }
}

#[test]
#[ignore = "manual local mini-delta snapshot lifecycle campaign"]
fn mini_delta_snapshot_lifecycle_campaign() {
    let fixture = Fixture::new("delta-snapshot-campaign");
    let config = LexicalProjectionConfig::default();
    for seed in [392_u64, 7, 0x5eed] {
        let mut random = seed;
        let mut active = Arc::new(LexicalMiniDelta::default());
        let mut documents = fixture.documents.clone();
        let mut retained = std::collections::VecDeque::new();
        for step in 0..64 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let id = ["a", "b", "new"][(random as usize) % 3];
            retained.push_back((Arc::clone(&active), documents.clone()));
            if retained.len() > 4 {
                retained.pop_front();
            }
            match (random >> 8) % 4 {
                0 => {
                    assert!(active
                        .delete(id, documents.get(id), &fixture.analyzer, config)
                        .unwrap());
                    documents.remove(id);
                }
                1 => {
                    let invalid = document(id, "graph", &"x".repeat(4097));
                    assert!(active
                        .upsert(&invalid, documents.get(id), &fixture.analyzer, config)
                        .is_err());
                }
                _ => {
                    let body = [
                        "graph graph",
                        "storage memory",
                        "HTTPServerV2",
                        "\u{77e5}\u{8bc6}\u{56fe}\u{8c31}",
                    ][(random >> 16) as usize % 4];
                    let next = document(id, if step % 2 == 0 { "graph" } else { "" }, body);
                    active
                        .upsert(&next, documents.get(id), &fixture.analyzer, config)
                        .unwrap();
                    documents.insert(id.into(), next);
                }
            }
            assert_delta_snapshot(&fixture, &active, &documents);
            for (snapshot, expected) in &retained {
                assert_delta_snapshot(&fixture, snapshot, expected);
            }
        }
    }
}

#[test]
fn old_analyzer_fingerprints_are_not_reused_for_supplementary_han_ngrams() {
    let fixture = Fixture::new("analyzer-fingerprint");
    let analyzer = SearchAnalyzerLexicon::empty();
    let old_digest = checksum(b"hawdb-search-analyzer-v2-jieba-search");
    let current_digest = analyzer_digest(&analyzer);
    assert_ne!(old_digest, current_digest);
    let old = LexicalProjectionWriter::new(LexicalProjectionConfig::default())
        .write(
            &fixture.root,
            2,
            Some(7),
            old_digest,
            13,
            fixture.documents.values(),
            &analyzer,
        )
        .unwrap();
    assert!(LexicalProjectionReader::load(
        &fixture.root,
        Some(7),
        current_digest,
        13,
        LexicalProjectionConfig::default()
    )
    .unwrap()
    .is_none());
    // A stale derived projection is not evidence of corruption.
    assert!(fixture.root.join(artifact_file(old.generation())).exists());
}

#[test]
fn manifest_tampering_is_rejected_even_with_recomputed_envelope_checksum() {
    let fixture = Fixture::new("manifest-tampering");
    let original = fs::read(fixture.root.join(MANIFEST_FILE)).unwrap();
    let mut envelope: ManifestEnvelope = serde_json::from_slice(&original).unwrap();
    envelope.body.documents_digest ^= 1;
    assert!(
        ManifestBody::decode(&serde_json::to_vec(&envelope).unwrap())
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch")
    );

    for case in 0..6 {
        let mut body = fixture.reader.manifest.clone();
        match case {
            0 => body.artifact_file = "../search_lexical.1.hawdb".to_string(),
            1 => body.blocks[0].offset += 1,
            2 => body.document_count += 1,
            3 => body.term_statistics[0].document_frequency += 1,
            4 => body.blocks[0].length = 0,
            _ => body.format = "unrecognized".to_string(),
        }
        let checksum = checksum(&serde_json::to_vec(&body).unwrap());
        let mutated = serde_json::to_vec(&ManifestEnvelope { body, checksum }).unwrap();
        fs::write(fixture.root.join(MANIFEST_FILE), mutated).unwrap();
        assert!(
            fixture.reopen(LexicalProjectionConfig::default()).is_err(),
            "case {case}"
        );
    }
    fs::write(fixture.root.join(MANIFEST_FILE), original).unwrap();
    assert_eq!(
        fixture
            .reopen(LexicalProjectionConfig::default())
            .unwrap()
            .unwrap()
            .generation(),
        1
    );
}

#[test]
fn posting_bit_flips_fail_at_query_time_and_reopen() {
    let fixture = Fixture::new("posting-bit-flips");
    let artifact = fixture.root.join(artifact_file(1));
    let original = fs::read(&artifact).unwrap();
    let block = fixture
        .reader
        .manifest
        .blocks
        .iter()
        .find(|block| block.kind == BlockKind::Postings)
        .unwrap();
    for bit in 0..64 {
        let mut mutated = original.clone();
        let position = block.offset as usize + bit % block.length as usize;
        mutated[position] ^= 1 << (bit % 8);
        fs::write(&artifact, mutated).unwrap();
        let error = fixture.reader.read_block(block).unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"), "{error}");
        let error = fixture
            .reopen(LexicalProjectionConfig::default())
            .unwrap_err();
        assert!(
            error.to_string().contains("artifact checksum mismatch"),
            "{error}"
        );
    }
    fs::write(&artifact, original).unwrap();
    assert!(fixture
        .reopen(LexicalProjectionConfig::default())
        .unwrap()
        .is_some());
}

#[test]
fn posting_codec_rejects_every_truncated_prefix_and_trailing_bytes() {
    let fixture = Fixture::new("posting-truncation");
    let block = fixture
        .reader
        .manifest
        .blocks
        .iter()
        .find(|block| block.kind == BlockKind::Postings)
        .unwrap();
    let bytes = fixture.reader.read_block(block).unwrap();
    let decode = |bytes: &[u8]| decode_posting_block(bytes, 1, block, 4096, |_| Ok(()));
    for length in 0..bytes.len() {
        assert!(
            decode(&bytes[..length]).is_err(),
            "accepted prefix {length}"
        );
    }
    assert!(decode(&bytes).is_ok());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(decode(&trailing).is_err());
}

#[test]
fn spill_reader_rejects_partial_records_but_accepts_record_boundaries() {
    let fixture = Fixture::new("spill-truncation");
    let path = fixture.root.join("run.tmp");
    let mut bytes = RUN_HEADER.to_vec();
    let mut boundaries = vec![bytes.len()];
    for id in ["a", "b"] {
        encode_posting(
            &mut bytes,
            &Posting {
                term: "graph".into(),
                document_id: id.to_string(),
                term_frequency: 1,
                document_len: 2,
            },
        )
        .unwrap();
        boundaries.push(bytes.len());
    }
    for length in 0..=bytes.len() {
        fs::write(&path, &bytes[..length]).unwrap();
        let observed = (|| {
            let mut reader = RunReader::open(&path, LexicalProjectionConfig::default())?;
            let mut count = 0;
            while reader
                .next(LexicalProjectionConfig::default().build_memory_bytes.get())?
                .is_some()
            {
                count += 1;
            }
            Ok::<_, HawDBError>(count)
        })();
        if let Some(expected) = boundaries.iter().position(|boundary| *boundary == length) {
            assert_eq!(observed.unwrap(), expected);
        } else {
            assert!(observed.is_err(), "accepted partial record {length}");
        }
    }
    let mut oversized = RUN_HEADER.to_vec();
    oversized.extend_from_slice(&u32::MAX.to_le_bytes());
    fs::write(&path, oversized).unwrap();
    let error = RunReader::open(&path, LexicalProjectionConfig::default())
        .unwrap()
        .next(LexicalProjectionConfig::default().build_memory_bytes.get())
        .unwrap_err();
    assert!(error.to_string().contains("admitted length"), "{error}");
}

#[test]
fn failed_build_budgets_preserve_publication_and_remove_temporaries() {
    let fixture = Fixture::new("build-budgets");
    let manifest = fs::read(fixture.root.join(MANIFEST_FILE)).unwrap();
    let artifact = fs::read(fixture.root.join(artifact_file(1))).unwrap();
    let base = LexicalProjectionConfig::default();
    // Keep the 144-byte admission limit and force multiple posting runs without
    // failing earlier on the newly accounted per-term field markers.
    let spill_documents = ["a", "b", "c"]
        .into_iter()
        .map(|id| (id.to_string(), document(id, "red", "blue")))
        .collect::<BTreeMap<_, _>>();
    let limits = [
        (
            LexicalProjectionConfig {
                build_memory_bytes: NonZeroU64::new(144).unwrap(),
                ..base
            },
            "analyzer bytes",
            &fixture.documents,
        ),
        (
            LexicalProjectionConfig {
                build_memory_bytes: NonZeroU64::new(144).unwrap(),
                max_spill_runs: NonZeroUsize::MIN,
                ..base
            },
            "spill runs",
            &spill_documents,
        ),
        (
            LexicalProjectionConfig {
                max_spill_bytes: NonZeroU64::MIN,
                ..base
            },
            "spill bytes",
            &fixture.documents,
        ),
        (
            LexicalProjectionConfig {
                max_merge_fan_in: NonZeroUsize::MIN,
                ..base
            },
            "fan-in",
            &fixture.documents,
        ),
        (
            LexicalProjectionConfig {
                max_block_bytes: NonZeroU64::MIN,
                ..base
            },
            "byte block",
            &fixture.documents,
        ),
    ];
    for (config, expected_error, documents) in limits {
        let error = LexicalProjectionWriter::new(config)
            .write(
                &fixture.root,
                2,
                Some(7),
                11,
                13,
                documents.values(),
                &fixture.analyzer,
            )
            .unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(
            fs::read(fixture.root.join(MANIFEST_FILE)).unwrap(),
            manifest
        );
        assert_eq!(
            fs::read(fixture.root.join(artifact_file(1))).unwrap(),
            artifact
        );
        assert_eq!(fixture.reopen(base).unwrap().unwrap().generation(), 1);
        let mut names = fs::read_dir(&fixture.root)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(
            names,
            vec![
                std::ffi::OsString::from(artifact_file(1)),
                std::ffi::OsString::from(MANIFEST_FILE)
            ]
        );
    }
}

#[test]
fn compaction_charges_output_runs_and_cleans_up_when_budget_is_exhausted() {
    let fixture = Fixture::new("compaction-budget");
    let config = LexicalProjectionConfig {
        max_spill_runs: NonZeroUsize::new(3).unwrap(),
        max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
        ..LexicalProjectionConfig::default()
    };
    let mut runs = SpillRuns::new(&fixture.root, 2, config);
    for id in ["a", "b", "c"] {
        runs.spill(&mut vec![Posting {
            term: "graph".into(),
            document_id: id.to_string(),
            term_frequency: 1,
            document_len: 1,
        }])
        .unwrap();
    }
    assert_eq!(runs.sequence, 3);
    let error = runs.compact().unwrap_err();
    assert!(
        error.to_string().contains("requires 4 spill runs"),
        "{error}"
    );
    drop(runs);
    assert!(fs::read_dir(&fixture.root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".tmp")));
}

#[test]
fn block_read_admission_accepts_exact_limit_and_rejects_one_byte_less() {
    let fixture = Fixture::new("block-admission");
    let largest = fixture
        .reader
        .manifest
        .blocks
        .iter()
        .map(|block| block.length)
        .max()
        .unwrap();
    let exact = LexicalProjectionConfig {
        max_block_bytes: NonZeroU64::new(largest).unwrap(),
        ..LexicalProjectionConfig::default()
    };
    let reopened = fixture.reopen(exact).unwrap().unwrap();
    for block in &reopened.manifest.blocks {
        reopened.read_block(block).unwrap();
    }
    let error = fixture
        .reopen(LexicalProjectionConfig {
            max_block_bytes: NonZeroU64::new(largest - 1).unwrap(),
            ..exact
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("read admission limit"),
        "{error}"
    );
}

#[test]
fn mini_delta_budget_rejection_is_atomic_for_insert_replace_and_delete() {
    let fixture = Fixture::new("delta-admission");
    let config = LexicalProjectionConfig::default();
    let mut delta = Arc::new(LexicalMiniDelta::default());
    let inserted = document("c", "graph", "index");
    delta
        .upsert(&inserted, None, &fixture.analyzer, config)
        .unwrap();
    let scores = fixture.scores(&delta);
    let before = format!("{delta:?}");
    let exact = LexicalProjectionConfig {
        mini_delta_bytes: NonZeroU64::new(delta.resident_bytes).unwrap(),
        ..config
    };
    delta
        .upsert(&inserted, None, &fixture.analyzer, exact)
        .unwrap();
    assert_eq!(format!("{delta:?}"), before);
    for next in [
        document("d", "graph", "new"),
        document("c", "graph", "more distinct terms here"),
    ] {
        assert!(delta.upsert(&next, None, &fixture.analyzer, exact).is_err());
        assert_eq!(format!("{delta:?}"), before);
        assert_eq!(fixture.scores(&delta), scores);
    }
    assert!(!delta
        .delete("a", Some(&fixture.documents["a"]), &fixture.analyzer, exact)
        .unwrap());
    assert_eq!(format!("{delta:?}"), before);
    assert_eq!(fixture.scores(&delta), scores);
    assert!(delta
        .delete(
            "a",
            Some(&fixture.documents["a"]),
            &fixture.analyzer,
            config
        )
        .unwrap());
    assert!(!fixture.scores(&delta).contains_key("a"));
}
