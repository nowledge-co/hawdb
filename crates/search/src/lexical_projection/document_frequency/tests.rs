use super::*;
use crate::lexical_projection::analysis_tests::reference_document_tokens;
use std::sync::atomic::{AtomicU64, Ordering};

struct TestRoot(PathBuf);

impl TestRoot {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "skein-document-frequency-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn entries(&self) -> usize {
        fs::read_dir(&self.0).unwrap().count()
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn large_document(id: &str) -> SearchDocument {
    let mut content = "graph graph graph_storage ".to_string();
    for index in 0..4_096 {
        content.push_str(&format!("token{index:05} "));
        if index % 31 == 0 {
            content.push_str("graph graph_storage GraphRAG ");
        }
    }
    content.push_str("graph graph graph_storage \u{4e2d}\u{6587}\u{6570}\u{636e}\u{5e93}");
    SearchDocument {
        id: id.into(),
        title: "graph graph storage GraphRAG".into(),
        content,
        embedding: None,
        metadata: BTreeMap::from([
            ("kind".into(), "graph graph storage".into()),
            ("external_id".into(), "GraphRAG_graph_rag".into()),
            ("space_id".into(), "graph graph graph_storage".into()),
        ]),
    }
}

fn reference_frequencies(
    document: &SearchDocument,
    analyzer: &SearchAnalyzerLexicon,
) -> (BTreeMap<String, u32>, u32) {
    let tokens = reference_document_tokens(document, analyzer);
    let length = tokens.len() as u32;
    let mut frequencies = BTreeMap::new();
    for term in tokens {
        *frequencies.entry(term).or_default() += 1;
    }
    (frequencies, length)
}

#[test]
fn actual_document_spill_matches_complete_legacy_frequencies() {
    let document = large_document("large");
    let analyzer =
        SearchAnalyzerLexicon::default().with_normalized_alias_rule(["graph graph"], ["graph"]);
    let expected = reference_frequencies(&document, &analyzer);
    for budget in [96 * 1024, 128 * 1024, 192 * 1024] {
        let root = TestRoot::new();
        let config = LexicalProjectionConfig {
            build_memory_bytes: NonZeroU64::new(budget).unwrap(),
            ..Default::default()
        };
        assert!(analyze_delta_document(&document, &analyzer, config)
            .unwrap_err()
            .to_string()
            .contains("analyzer bytes"));
        let mut pool = SpillRuns::new(&root.0, 1, config);
        let result = analyze(&document, &analyzer, &mut pool, &mut Vec::new(), &mut 0).unwrap();
        assert!(matches!(result, AnalyzedDocument::Spilled { .. }));
        assert!(pool.sequence > 2, "actual run and merge I/O must occur");
        assert_eq!(result.document_len(), expected.1);
        let mut actual = BTreeMap::new();
        result
            .visit(config, |term, frequency, retained| {
                assert!(retained < budget);
                assert!(actual.insert(term, frequency).is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected.0, "budget={budget}");
        assert_eq!(
            root.entries(),
            0,
            "document-local runs are released after emission"
        );
        assert!(
            pool.paths.is_empty(),
            "document runs must not become corpus posting runs"
        );
    }
}

#[test]
fn exact_token_limit_does_not_count_duplicate_partial_phrases() {
    let root = TestRoot::new();
    let document = large_document("exact");
    let analyzer =
        SearchAnalyzerLexicon::default().with_normalized_alias_rule(["graph graph"], ["graph"]);
    let expected = reference_frequencies(&document, &analyzer);
    for (limit, accepted) in [
        (expected.1 as usize, true),
        (expected.1 as usize - 1, false),
    ] {
        let config = LexicalProjectionConfig {
            build_memory_bytes: NonZeroU64::new(96 * 1024).unwrap(),
            max_document_tokens: NonZeroUsize::new(limit).unwrap(),
            ..Default::default()
        };
        let mut pool = SpillRuns::new(&root.0, 1, config);
        let result = analyze(&document, &analyzer, &mut pool, &mut Vec::new(), &mut 0);
        assert_eq!(result.is_ok(), accepted);
        if accepted {
            assert_eq!(result.unwrap().document_len(), expected.1);
        } else {
            assert!(result.err().unwrap().to_string().contains("tokens"));
        }
        assert_eq!(root.entries(), 0);
    }
}

fn record(
    term: &str,
    field: u8,
    ordinal: u64,
    occurrence: TokenOccurrence,
    weight: u64,
) -> FrequencyRecord {
    let mut summary = PartialFieldFrequency::default();
    summary.push(ordinal, occurrence, weight).unwrap();
    FrequencyRecord {
        term: term.into(),
        field,
        summary,
    }
}

fn drain(path: &Path, config: LexicalProjectionConfig) -> Result<Vec<(String, u8, u64)>> {
    let mut reader = FrequencyRunReader::open(path, config)?;
    let mut records = Vec::new();
    while let Some(record) = reader.next()? {
        records.push((record.term, record.field, record.summary.frequency()?));
    }
    Ok(records)
}

#[test]
fn run_wire_admission_integrity_and_cleanup_are_explicit() {
    let root = TestRoot::new();
    let config = LexicalProjectionConfig::default();
    let mut pool = SpillRuns::new(&root.0, 1, config);
    let output = write_run(
        [
            Ok(record("alpha", 0, 1, TokenOccurrence::UniqueInField, 2)),
            Ok(record("alpha", 0, 2, TokenOccurrence::Repeated, 2)),
            Ok(record("alpha", 0, 3, TokenOccurrence::UniqueInField, 2)),
            Ok(record("beta", 1, 4, TokenOccurrence::Repeated, 1)),
        ],
        &mut pool,
        &mut FileSpillIo,
    )
    .unwrap();
    let original = fs::read(&output.guard.path).unwrap();
    let mut expected = b"SKNDOCF1".to_vec();
    for (term, field, repeated, ordinal, unique) in
        [("alpha", 0u8, 2u64, 1u64, 2u64), ("beta", 1, 1, 4, 0)]
    {
        expected.extend_from_slice(&(term.len() as u32).to_le_bytes());
        expected.extend_from_slice(term.as_bytes());
        expected.push(field);
        expected.extend_from_slice(&repeated.to_le_bytes());
        expected.extend_from_slice(&ordinal.to_le_bytes());
        expected.extend_from_slice(&unique.to_le_bytes());
    }
    let digest = checksum(&expected);
    expected.extend_from_slice(&2u64.to_le_bytes());
    expected.extend_from_slice(&digest.to_le_bytes());
    assert_eq!(original, expected);
    assert_eq!(pool.bytes, expected.len() as u64);
    assert_eq!(
        drain(&output.guard.path, config).unwrap(),
        vec![("alpha".into(), 0, 4), ("beta".into(), 1, 1)]
    );
    for length in 0..original.len() {
        fs::write(&output.guard.path, &original[..length]).unwrap();
        assert!(
            drain(&output.guard.path, config).is_err(),
            "truncated={length}"
        );
    }
    for index in 0..original.len() {
        let mut corrupt = original.clone();
        corrupt[index] ^= 1;
        fs::write(&output.guard.path, corrupt).unwrap();
        assert!(
            drain(&output.guard.path, config).is_err(),
            "flipped={index}"
        );
    }
    drop(output);
    assert_eq!(root.entries(), 0);
    for limit in 1..=58 {
        let config = LexicalProjectionConfig {
            max_spill_bytes: NonZeroU64::new(limit).unwrap(),
            ..config
        };
        let mut pool = SpillRuns::new(&root.0, 1, config);
        let result = write_run(
            [Ok(record("alpha", 0, 1, TokenOccurrence::Repeated, 2))],
            &mut pool,
            &mut FileSpillIo,
        );
        assert_eq!(result.is_ok(), limit == 58, "limit={limit}");
        drop(result);
        assert_eq!(root.entries(), 0);
    }
}

#[test]
fn run_quotas_are_shared_with_existing_posting_spills() {
    let root = TestRoot::new();
    let config = LexicalProjectionConfig {
        max_spill_runs: NonZeroUsize::new(1).unwrap(),
        ..Default::default()
    };
    let mut pool = SpillRuns::new(&root.0, 1, config);
    let run = write_run(
        [Ok(record("alpha", 0, 1, TokenOccurrence::Repeated, 2))],
        &mut pool,
        &mut FileSpillIo,
    )
    .unwrap();
    let mut postings = vec![Posting {
        term: "alpha".into(),
        document_id: "one".into(),
        term_frequency: 2,
        document_len: 2,
    }];
    assert!(pool
        .spill(&mut postings)
        .unwrap_err()
        .to_string()
        .contains("spill runs"));
    assert_eq!(pool.sequence, 1);
    assert_eq!(pool.bytes, 58);
    drop(run);
    assert_eq!(root.entries(), 0);
}

#[test]
fn failed_consumer_and_merge_remove_all_document_runs() {
    let root = TestRoot::new();
    let config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(96 * 1024).unwrap(),
        ..Default::default()
    };
    let document = large_document("cancelled");
    let mut pool = SpillRuns::new(&root.0, 1, config);
    let analyzed = analyze(
        &document,
        &SearchAnalyzerLexicon::default(),
        &mut pool,
        &mut Vec::new(),
        &mut 0,
    )
    .unwrap();
    let error = analyzed
        .visit(config, |_, _, _| {
            Err(SkeinError::Execution("cancel consumer".into()))
        })
        .unwrap_err();
    assert!(error.to_string().contains("cancel consumer"));
    assert_eq!(root.entries(), 0);
    let left = write_run(
        [Ok(record("alpha", 0, 1, TokenOccurrence::Repeated, 2))],
        &mut pool,
        &mut FileSpillIo,
    )
    .unwrap();
    let right = write_run(
        [Ok(record("beta", 0, 2, TokenOccurrence::Repeated, 2))],
        &mut pool,
        &mut FileSpillIo,
    )
    .unwrap();
    let mut bytes = fs::read(&right.guard.path).unwrap();
    let index = bytes.len() - 1;
    bytes[index] ^= 1;
    fs::write(&right.guard.path, bytes).unwrap();
    assert!(merge_pair(left, right, &mut pool).is_err());
    assert_eq!(root.entries(), 0);
}

fn assert_full_postings(
    reader: &LexicalProjectionReader,
    documents: &[SearchDocument],
    analyzer: &SearchAnalyzerLexicon,
) {
    let mut expected = BTreeMap::new();
    let mut expected_df = BTreeMap::<String, u64>::new();
    let mut total_len = 0u64;
    for document in documents {
        let (frequencies, length) = reference_frequencies(document, analyzer);
        total_len += u64::from(length);
        for (term, frequency) in frequencies {
            *expected_df.entry(term.clone()).or_default() += 1;
            expected.insert((term, document.id.clone()), (frequency, length));
        }
    }
    let mut actual = BTreeMap::new();
    for block in &reader.manifest.blocks {
        if block.kind != BlockKind::Postings {
            continue;
        }
        let bytes = reader.read_block(block).unwrap();
        decode_posting_block(
            &bytes,
            reader.generation(),
            block,
            reader.config.max_term_bytes.get(),
            |posting| {
                assert!(actual
                    .insert(
                        (posting.term, posting.document_id),
                        (posting.term_frequency, posting.document_len)
                    )
                    .is_none());
                Ok(())
            },
        )
        .unwrap();
    }
    assert_eq!(actual, expected);
    assert_eq!(reader.manifest.document_count, documents.len() as u64);
    assert_eq!(reader.manifest.total_document_len, total_len);
    assert_eq!(reader.manifest.posting_count, actual.len() as u64);
    assert_eq!(
        reader
            .manifest
            .term_statistics
            .iter()
            .map(|stats| (stats.term.clone(), stats.document_frequency))
            .collect::<BTreeMap<_, _>>(),
        expected_df
    );
}

#[test]
fn spilled_generation_preserves_all_postings_scores_reopen_and_failed_publication() {
    let root = TestRoot::new();
    let high_root = TestRoot::new();
    let mut second = large_document("second");
    second.content.push_str(" graph graph graph");
    let mut tied = second.clone();
    tied.id = "third".into();
    let documents = [large_document("first"), second, tied];
    let analyzer =
        SearchAnalyzerLexicon::default().with_normalized_alias_rule(["graph graph"], ["graph"]);
    let config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(96 * 1024).unwrap(),
        target_block_bytes: NonZeroU64::new(4096).unwrap(),
        max_block_bytes: NonZeroU64::new(8192).unwrap(),
        ..Default::default()
    };
    let reader = LexicalProjectionWriter::new(config)
        .write(&root.0, 1, Some(7), 11, 13, documents.iter(), &analyzer)
        .unwrap();
    assert_full_postings(&reader, &documents, &analyzer);
    let high_config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(32 * 1024 * 1024).unwrap(),
        ..config
    };
    let high = LexicalProjectionWriter::new(high_config)
        .write(
            &high_root.0,
            1,
            Some(7),
            11,
            13,
            documents.iter(),
            &analyzer,
        )
        .unwrap();
    assert_full_postings(&high, &documents, &analyzer);
    let reopened = LexicalProjectionReader::load(&high_root.0, Some(7), 11, 13, config)
        .unwrap()
        .unwrap();
    for terms in [
        BTreeSet::from(["graph".into()]),
        BTreeSet::from(["token04095".into(), "storage".into()]),
    ] {
        for filtered in [false, true] {
            let allowed = |id: &str| Ok(!filtered || id != "first");
            for limit in [None, Some(1), Some(2)] {
                let actual = reader
                    .score(&terms, &LexicalMiniDelta::default(), limit, allowed)
                    .unwrap();
                let reference = reopened
                    .score(&terms, &LexicalMiniDelta::default(), limit, allowed)
                    .unwrap();
                assert_eq!(actual.scores, reference.scores);
                assert_eq!(
                    actual.matching_document_count,
                    reference.matching_document_count
                );
            }
        }
    }
    let before = fs::read(root.0.join(MANIFEST_FILE)).unwrap();
    let limited = LexicalProjectionConfig {
        max_spill_bytes: NonZeroU64::new(128).unwrap(),
        ..config
    };
    assert!(LexicalProjectionWriter::new(limited)
        .write(&root.0, 2, Some(8), 11, 14, documents.iter(), &analyzer)
        .is_err());
    assert_eq!(fs::read(root.0.join(MANIFEST_FILE)).unwrap(), before);
    let recovered = LexicalProjectionReader::load(&root.0, Some(7), 11, 13, config)
        .unwrap()
        .unwrap();
    assert_full_postings(&recovered, &documents, &analyzer);
    assert!(fs::read_dir(&root.0).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".tmp")));
}

#[test]
fn document_record_buffer_admits_before_reserving_or_inserting() {
    let root = TestRoot::new();
    let config = LexicalProjectionConfig::default();
    let needed = std::mem::size_of::<FrequencyRecord>() as u64 + 5;
    for (limit, accepted) in [(needed, true), (needed - 1, false)] {
        let mut pool = SpillRuns::new(&root.0, 1, config);
        let mut analysis = SpillingAnalysis {
            records: Vec::new(),
            string_bytes: 0,
            buffer_limit: limit,
            lower_bound: 0,
            runs: DocumentRuns::default(),
        };
        let result = analysis.push(
            record("alpha", 0, 1, TokenOccurrence::Repeated, 2),
            &mut pool,
        );
        assert_eq!(result.is_ok(), accepted);
        assert_eq!(analysis.records.len(), usize::from(accepted));
        if !accepted {
            assert_eq!(analysis.records.capacity(), 0);
            assert_eq!(analysis.lower_bound, 0);
            assert_eq!(analysis.string_bytes, 0);
        }
        assert_eq!(pool.sequence, 0);
        assert_eq!(root.entries(), 0);
    }
    let pool = SpillRuns::new(
        &root.0,
        1,
        LexicalProjectionConfig {
            max_term_bytes: NonZeroU64::new(u64::MAX).unwrap(),
            ..config
        },
    );
    assert_eq!(progress_memory(&pool, "overflow"), u64::MAX);
}

struct FaultIo {
    remaining: usize,
    flush_error: bool,
    panic_write: bool,
}
struct FaultWriter {
    file: File,
    remaining: usize,
    flush_error: bool,
    panic_write: bool,
}
impl Write for FaultWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        assert!(!self.panic_write, "injected document run write panic");
        if self.remaining == 0 {
            return Err(std::io::Error::other("injected document run write failure"));
        }
        let count = bytes.len().min(self.remaining).min(3);
        let written = self.file.write(&bytes[..count])?;
        self.remaining -= written;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if self.flush_error {
            return Err(std::io::Error::other("injected document run flush failure"));
        }
        self.file.flush()
    }
}
impl SpillIo for FaultIo {
    type Writer = FaultWriter;
    fn create(&mut self, path: &Path) -> Result<Self::Writer> {
        Ok(FaultWriter {
            file: File::create(path)?,
            remaining: self.remaining,
            flush_error: self.flush_error,
            panic_write: self.panic_write,
        })
    }
    fn remove(&mut self, path: &Path) -> Result<()> {
        Ok(fs::remove_file(path)?)
    }
}

#[test]
fn document_run_partial_writes_flush_failures_and_unwind_cleanup() {
    let root = TestRoot::new();
    for boundary in 0..58 {
        let mut pool = SpillRuns::new(&root.0, 1, Default::default());
        let result = write_run(
            [Ok(record("alpha", 0, 1, TokenOccurrence::Repeated, 2))],
            &mut pool,
            &mut FaultIo {
                remaining: boundary,
                flush_error: false,
                panic_write: false,
            },
        );
        assert!(result.is_err(), "boundary={boundary}");
        assert_eq!(pool.bytes, 0);
        assert_eq!(root.entries(), 0);
    }
    for flush_error in [false, true] {
        let mut pool = SpillRuns::new(&root.0, 1, Default::default());
        let result = write_run(
            [Ok(record("alpha", 0, 1, TokenOccurrence::Repeated, 2))],
            &mut pool,
            &mut FaultIo {
                remaining: 58,
                flush_error,
                panic_write: false,
            },
        );
        assert_eq!(result.is_ok(), !flush_error);
        if let Ok(run) = result {
            assert_eq!(
                drain(&run.guard.path, pool.config).unwrap(),
                vec![("alpha".into(), 0, 2)]
            );
        }
        assert_eq!(root.entries(), 0);
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut pool = SpillRuns::new(&root.0, 1, Default::default());
        write_run(
            [Ok(record("alpha", 0, 1, TokenOccurrence::Repeated, 2))],
            &mut pool,
            &mut FaultIo {
                remaining: 58,
                flush_error: false,
                panic_write: true,
            },
        )
        .ok();
    }));
    assert!(result.is_err());
    assert_eq!(root.entries(), 0);
}

#[test]
#[ignore = "explicit local Bazel document-frequency fuzz campaign"]
fn document_frequency_spill_differential_campaign() {
    for seed in 0..128u64 {
        let root = TestRoot::new();
        let mut random = seed + 1;
        let mut next = || {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        };
        let analyzer = SearchAnalyzerLexicon::default()
            .with_normalized_alias_rule(["graph storage"], ["graph"])
            .with_stopwords(["skipword"]);
        let mut document = SearchDocument {
            id: format!("seed-{seed}"),
            title: "graph storage graph".into(),
            content: String::new(),
            embedding: None,
            metadata: BTreeMap::new(),
        };
        for index in 0..(256 + seed as usize % 129) {
            document
                .content
                .push_str(&format!("entry{:05} ", (index * 97) % 997));
            let suffix = match next() % 6 {
                0 => "graph storage ",
                1 => "GraphStorage graph_storage ",
                2 => "\u{4e2d}\u{6587}\u{6570}\u{636e}\u{5e93} ",
                3 => "skipword graph ",
                4 => "APIClient api client ",
                _ => "graph graph graph ",
            };
            document.content.push_str(suffix);
        }
        if seed % 2 == 0 {
            document
                .metadata
                .insert("source_id".into(), "GraphStorage graph storage".into());
        }
        if seed % 3 == 0 {
            document
                .metadata
                .insert("space_id".into(), "APIClient api client".into());
        }
        let expected = reference_frequencies(&document, &analyzer);
        let config = LexicalProjectionConfig {
            build_memory_bytes: NonZeroU64::new(34_816 + (seed % 8) * 1024).unwrap(),
            max_term_bytes: NonZeroU64::new(128).unwrap(),
            ..Default::default()
        };
        let mut pool = SpillRuns::new(&root.0, 1, config);
        let result = analyze(&document, &analyzer, &mut pool, &mut Vec::new(), &mut 0).unwrap();
        assert!(
            matches!(result, AnalyzedDocument::Spilled { .. }),
            "seed={seed}"
        );
        let bytes = pool.bytes;
        let runs = pool.sequence;
        assert!(runs >= 3, "seed={seed}");
        assert_eq!(result.document_len(), expected.1, "seed={seed}");
        let mut actual = BTreeMap::new();
        result
            .visit(config, |term, frequency, _| {
                assert!(actual.insert(term, frequency).is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected.0, "seed={seed}");
        assert_eq!(root.entries(), 0);
        for (limit, accepted) in [(bytes, true), (bytes - 1, false)] {
            let limited = LexicalProjectionConfig {
                max_spill_bytes: NonZeroU64::new(limit).unwrap(),
                ..config
            };
            let mut pool = SpillRuns::new(&root.0, 1, limited);
            let result = analyze(&document, &analyzer, &mut pool, &mut Vec::new(), &mut 0);
            assert_eq!(result.is_ok(), accepted, "seed={seed}, limit={limit}");
            drop(result);
            assert_eq!(root.entries(), 0);
        }
    }
}

#[test]
fn public_generation_writer_reopens_and_updates_spilled_documents() {
    use crate::{
        SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter,
        SearchOutOfCoreReader, SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
    };
    let root = TestRoot::new();
    let options = SearchOutOfCoreGenerationBuildOptions {
        lexical_build_memory_bytes: NonZeroU64::new(96 * 1024).unwrap(),
        ..Default::default()
    };
    let make_row = |id: &str, content: String| SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.into(),
        title: "Graph storage".into(),
        body: content,
        embedding: None,
        source_id: None,
        metadata: BTreeMap::from([("space_id".into(), "default".into())]),
    };
    let original = [
        make_row("a", large_document("a").content),
        make_row("b", large_document("b").content),
    ];
    let documents = original
        .iter()
        .cloned()
        .map(SearchProjectionRow::into_document)
        .collect::<Vec<_>>();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root.0, options.clone()).unwrap();
    for document in &documents {
        writer.push(document.clone()).unwrap();
    }
    let first = writer.finish().unwrap();
    assert_eq!(first.document_count, 2);
    assert!(first.active_manifest_published_last);
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(
                &documents
                    .iter()
                    .map(|doc| doc.id.clone())
                    .collect::<Vec<_>>()
            )
            .unwrap()
            .documents,
        documents
    );
    let mut updated = original[0].clone();
    updated.body.push_str(" replacementtoken replacementtoken");
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![updated.clone()],
            deletes: vec![documents[1].id.clone()],
            max_operations: Some(2),
            source_graph_commit_epoch: None,
        },
        options.clone(),
    )
    .unwrap();
    let (_, next, _) = update.finish().unwrap();
    assert_eq!(next.document_count, 1);
    assert!(next.generation > first.generation);
    let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(
        reopened
            .hydrate_documents(&[documents[0].id.clone()])
            .unwrap()
            .documents,
        vec![updated.clone().into_document()]
    );
    assert!(reopened
        .hydrate_documents(&[documents[1].id.clone()])
        .is_err());
    let failed = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reopened,
        SearchProjectionDelta {
            upserts: vec![updated.clone()],
            deletes: vec![],
            max_operations: Some(1),
            source_graph_commit_epoch: None,
        },
        SearchOutOfCoreGenerationBuildOptions {
            lexical_max_spill_bytes: NonZeroU64::MIN,
            ..options
        },
    )
    .unwrap();
    assert!(failed.finish().is_err());
    let retained = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(retained.generation(), next.generation);
    assert_eq!(
        retained
            .hydrate_documents(&[documents[0].id.clone()])
            .unwrap()
            .documents,
        vec![updated.into_document()]
    );
    assert!(fs::read_dir(&root.0).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .contains(".stage")));
}

#[test]
fn run_budget_is_enforced_before_physical_writes() {
    use std::cell::Cell;
    use std::rc::Rc;
    struct CountWriter {
        file: File,
        bytes: Rc<Cell<u64>>,
    }
    impl Write for CountWriter {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            let written = self.file.write(data)?;
            self.bytes.set(self.bytes.get() + written as u64);
            Ok(written)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.file.flush()
        }
    }
    struct CountIo {
        bytes: Rc<Cell<u64>>,
        creates: usize,
    }
    impl SpillIo for CountIo {
        type Writer = CountWriter;
        fn create(&mut self, path: &Path) -> Result<Self::Writer> {
            self.creates += 1;
            Ok(CountWriter {
                file: File::create(path)?,
                bytes: self.bytes.clone(),
            })
        }
        fn remove(&mut self, path: &Path) -> Result<()> {
            Ok(fs::remove_file(path)?)
        }
    }
    let root = TestRoot::new();
    for limit in 1..=63 {
        let config = LexicalProjectionConfig {
            max_spill_bytes: NonZeroU64::new(limit).unwrap(),
            ..Default::default()
        };
        let mut pool = SpillRuns::new(&root.0, 1, config);
        pool.bytes = 5;
        let bytes = Rc::new(Cell::new(0));
        let mut io = CountIo {
            bytes: bytes.clone(),
            creates: 0,
        };
        let result = write_run(
            [Ok(record("alpha", 0, 1, TokenOccurrence::Repeated, 2))],
            &mut pool,
            &mut io,
        );
        assert_eq!(result.is_ok(), limit == 63, "limit={limit}");
        assert!(
            bytes.get() <= limit.saturating_sub(5),
            "physical write before admission: limit={limit}, writes={}",
            bytes.get()
        );
        if limit < 29 {
            assert_eq!(io.creates, 0);
            assert_eq!(pool.sequence, 0);
        }
        drop(result);
        assert_eq!(root.entries(), 0);
    }
}
