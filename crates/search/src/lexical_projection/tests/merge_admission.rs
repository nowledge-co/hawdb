use super::artifact_admission::memory;
use super::*;
use crate::build_memory::SPOOL_BUFFER_BYTES;

pub(super) fn posting(term: &str, ordinal: u64) -> Posting {
    Posting {
        term: term.to_owned(),
        ordinal,
        term_frequency: 1,
    }
}

pub(super) fn run_bytes(postings: &[Posting]) -> Vec<u8> {
    let mut bytes = b"SKNLEXR1".to_vec();
    for posting in postings {
        bytes.extend_from_slice(&(posting.term.len() as u32).to_le_bytes());
        bytes.extend_from_slice(posting.term.as_bytes());
        bytes.extend_from_slice(&posting.ordinal.to_le_bytes());
        bytes.extend_from_slice(&posting.term_frequency.to_le_bytes());
    }
    bytes
}

pub(super) fn drain(paths: &[impl AsRef<Path>], memory: &BuildMemory) -> Result<Vec<Posting>> {
    let retained = memory.ledger.snapshot().used_bytes;
    let mut cursor = MergedPostings::new(
        paths,
        Default::default(),
        memory.clone(),
        RuntimeTaskContext::default(),
    )?;
    let mut output = Vec::new();
    while let Some(posting) = cursor.next()? {
        output.push(posting.clone());
    }
    // End of iteration, not only dropping the cursor, releases its working set.
    assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    assert!(cursor.next()?.is_none());
    Ok(output)
}

#[test]
fn run_reader_keeps_a_returned_posting_admitted_after_reader_drop() {
    let root = projection_root("run-reader-owner");
    fs::create_dir(&root).unwrap();
    let path = root.join("run.tmp");
    fs::write(&path, run_bytes(&[posting("graph", 7)])).unwrap();
    let memory = memory(SPOOL_BUFFER_BYTES + 5);
    let mut reader = RunReader::open(&path, Default::default(), memory.clone()).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, SPOOL_BUFFER_BYTES);
    let decoded = reader.next().unwrap().unwrap();
    assert_eq!(decoded.posting, posting("graph", 7));
    assert_eq!(memory.ledger.snapshot().used_bytes, SPOOL_BUFFER_BYTES + 5);
    drop(reader);
    assert_eq!(memory.ledger.snapshot().used_bytes, 5);
    drop(decoded);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn merge_preflights_buffers_slots_and_term_allocation() {
    let root = projection_root("merge-preflight");
    fs::create_dir(&root).unwrap();
    let path = root.join("run.tmp");
    let memory = memory(SPOOL_BUFFER_BYTES - 1);
    // Admission must fail before even trying to open the nonexistent file.
    let error = RunReader::open(&path, Default::default(), memory.clone())
        .err()
        .unwrap();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let memory = super::artifact_admission::memory(1);
    let error = MergedPostings::new(
        std::slice::from_ref(&path),
        Default::default(),
        memory.clone(),
        RuntimeTaskContext::default(),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::write(&path, run_bytes(&[posting("graph", 0)])).unwrap();
    let memory = super::artifact_admission::memory(SPOOL_BUFFER_BYTES + 4);
    let mut reader = RunReader::open(&path, Default::default(), memory.clone()).unwrap();
    merge::evidence::take();
    assert!(reader.next().is_err());
    assert_eq!(
        merge::evidence::take(),
        0,
        "denied before decoding the term"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, SPOOL_BUFFER_BYTES);
    drop(reader);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn merge_retains_current_while_admitting_refill_and_poisoned_errors_do_not_resume() {
    let root = projection_root("merge-held-posting");
    fs::create_dir(&root).unwrap();
    let path = root.join("run.tmp");
    let expected = vec![posting("a", 0), posting(&"z".repeat(101), u64::MAX)];
    fs::write(&path, run_bytes(&expected)).unwrap();
    let paths = [path];
    let baseline = memory(1024 * 1024);
    assert_eq!(drain(&paths, &baseline).unwrap(), expected);
    let peak = baseline.ledger.snapshot().peak_bytes;
    assert_eq!(drain(&paths, &memory(peak)).unwrap(), expected);
    let memory = memory(peak - 1);
    let mut cursor = MergedPostings::new(
        &paths,
        Default::default(),
        memory.clone(),
        RuntimeTaskContext::default(),
    )
    .unwrap();
    let seeded = memory.ledger.snapshot().used_bytes;
    assert_eq!(cursor.next().unwrap(), Some(&expected[0]));
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        seeded,
        "popping does not release the term"
    );
    assert_eq!(peak, seeded + 101, "old and incoming terms overlap");
    merge::evidence::take();
    assert!(cursor.next().is_err());
    assert_eq!(merge::evidence::take(), 0);
    assert!(cursor
        .next()
        .unwrap_err()
        .to_string()
        .contains("already failed"));
    assert_eq!(merge::evidence::take(), 0);
    drop(cursor);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn merge_matches_sorted_union_and_preserves_v1_run_bytes() {
    let root = projection_root("merge-union");
    fs::create_dir(&root).unwrap();
    let runs = [
        vec![posting("a\0", 1), posting("a\0", 1), posting("z", 0)],
        vec![
            posting("a\0", 1),
            posting("a\0", 2),
            posting("\u{4e2d}", u64::MAX),
        ],
        Vec::new(),
    ];
    let paths = runs
        .iter()
        .enumerate()
        .map(|(index, postings)| {
            let path = root.join(format!("{index}.tmp"));
            fs::write(&path, run_bytes(postings)).unwrap();
            path
        })
        .collect::<Vec<_>>();
    let mut expected = runs.concat();
    expected.sort();
    expected.dedup();
    let memory = memory(1024 * 1024);
    assert_eq!(drain(&paths, &memory).unwrap(), expected);
    let output = root.join("merged.tmp");
    let bytes = merge_runs(
        &paths,
        &output,
        Default::default(),
        &RuntimeTaskContext::default(),
        &memory,
    )
    .unwrap();
    let observed = fs::read(output).unwrap();
    assert_eq!(observed, run_bytes(&expected));
    assert_eq!(bytes, observed.len() as u64);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancelling_a_consumer_does_not_decode_the_next_record() {
    let root = projection_root("merge-cancel-refill");
    fs::create_dir(&root).unwrap();
    let path = root.join("run.tmp");
    let mut bytes = run_bytes(&[posting("first", 0)]);
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    fs::write(&path, bytes).unwrap();
    let memory = memory(1024 * 1024);
    let task = RuntimeTaskContext::default();
    let mut cursor =
        MergedPostings::new(&[path], Default::default(), memory.clone(), task.clone()).unwrap();
    merge::evidence::take();
    assert!(cursor.next().unwrap().is_some());
    assert_eq!(
        merge::evidence::take(),
        0,
        "yielding must not decode the invalid tail"
    );
    task.cancellation().cancel();
    let error = cursor.next().unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
    assert_eq!(merge::evidence::take(), 0);
    drop(cursor);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn corrupt_run_order_fails_closed_and_all_working_memory_is_released() {
    let root = projection_root("merge-corrupt-order");
    fs::create_dir(&root).unwrap();
    let path = root.join("run.tmp");
    fs::write(&path, run_bytes(&[posting("z", 1), posting("a", 2)])).unwrap();
    let memory = memory(1024 * 1024);
    assert!(drain(&[path], &memory)
        .unwrap_err()
        .to_string()
        .contains("not ordered"));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_compaction_removes_prior_outputs_and_every_input_run() {
    let root = projection_root("merge-cleanup");
    fs::create_dir(&root).unwrap();
    let memory = memory(1024 * 1024);
    let config = LexicalProjectionConfig {
        max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    };
    let mut runs = SpillRuns::new(&root, 2, config, memory.clone()).unwrap();
    for ordinal in 0..5 {
        runs.spill(&mut vec![posting("graph", ordinal)]).unwrap();
    }
    // The first output completes; the next group then fails during decoding.
    fs::write(&runs.paths[3], b"broken").unwrap();
    assert!(runs.compact().is_err());
    assert!(
        runs.sequence >= 7,
        "failure must follow one completed output"
    );
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(runs);
    fs::remove_dir(root).unwrap();
}

#[test]
fn spill_writer_and_merge_output_share_the_root_before_creating_files() {
    let root = projection_root("merge-output-budget");
    fs::create_dir(&root).unwrap();
    let memory = memory(SPOOL_BUFFER_BYTES - 1);
    let mut runs = SpillRuns::new(&root, 1, Default::default(), memory.clone()).unwrap();
    let mut postings = vec![posting("graph", 0)];
    assert!(runs.spill(&mut postings).is_err());
    assert_eq!(postings.len(), 1);
    assert_eq!(runs.sequence, 0);
    let output = root.join("output.tmp");
    assert!(merge_runs(
        &[] as &[PathBuf],
        &output,
        Default::default(),
        &RuntimeTaskContext::default(),
        &memory
    )
    .is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(runs);
    fs::remove_dir(root).unwrap();
}

#[test]
fn doclist_encoding_is_admitted_before_output_and_retains_no_heap_scratch() {
    let root = projection_root("doclist-encode-budget");
    fs::create_dir(&root).unwrap();
    let path = root.join("skip.tmp");
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    for (limit, succeeds) in [
        (path_bytes + posting_codec::MAX_BLOCK_BYTES - 1, false),
        (path_bytes + posting_codec::MAX_BLOCK_BYTES, true),
    ] {
        let memory = memory(limit);
        let mut writer = doclist::Writer::new(
            &path,
            dictionary_store::SpillBudget::new(0, u64::MAX),
            posting_codec::MAX_BLOCK_BYTES as u64,
            memory.clone(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut offset = 0;
        let result = writer.push_frame(
            &mut bytes,
            &mut offset,
            &[posting_codec::Posting { ordinal: 7, tf: 3 }],
        );
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(!bytes.is_empty(), succeeds);
        assert_eq!(offset > 0, succeeds);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, path_bytes);
        assert!(memory.ledger.snapshot().peak_bytes <= limit);
        drop(writer);
        assert!(!path.exists());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn artifact_merge_frame_denial_preserves_the_published_generation() {
    let root = projection_root("merge-frame-atomicity");
    fs::create_dir(&root).unwrap();
    let input = document("abc", "", "graph");
    let lexicon = SearchAnalyzerLexicon::empty();
    let reader = LexicalProjectionWriter::new(Default::default())
        .write(&root, 1, None, 11, 13, std::iter::once(&input), &lexicon)
        .unwrap();
    let manifest = fs::read(root.join(MANIFEST_FILE)).unwrap();
    let artifact = fs::read(root.join(artifact_file(1))).unwrap();
    // Enough for analysis/spill and the seeded cursor, but not the frame
    // alongside the artifact writer, directory, reader and live posting.
    let temporary = root.join(artifact_file(2)).with_extension("skein.tmp");
    let paths = paths::tests::publication_bytes(&root, 2)
        + 4 * std::mem::size_of::<OwnedPath>()
        + root.join(".search-lexical.2.0.tmp").capacity()
        + temporary.with_extension("skip.tmp").capacity()
        + temporary.with_extension("dictionary.tmp").capacity();
    let memory = memory(2 * SPOOL_BUFFER_BYTES + 1024 + paths);
    merge::evidence::take();
    let error = LexicalProjectionWriter::new(Default::default())
        .with_memory(memory.clone())
        .write(&root, 2, None, 11, 13, std::iter::once(&input), &lexicon)
        .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(
        merge::evidence::take(),
        1,
        "failure must reach the actual merge"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), manifest);
    assert_eq!(fs::read(root.join(artifact_file(1))).unwrap(), artifact);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 2);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn multi_pass_compaction_admits_readers_and_output_together() {
    let root = projection_root("merge-multi-pass-budget");
    fs::create_dir(&root).unwrap();
    let config = LexicalProjectionConfig {
        max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    };
    let compact = |memory: &BuildMemory| -> Result<()> {
        let mut runs = SpillRuns::new(&root, 1, config, memory.clone())?;
        for ordinal in 0..9 {
            runs.spill(&mut vec![posting("graph", ordinal)])?;
        }
        runs.compact()?;
        assert!(runs.sequence > 15, "exercise several compaction passes");
        let observed = drain(&runs.paths, memory)?;
        assert_eq!(
            observed,
            (0..9)
                .map(|ordinal| posting("graph", ordinal))
                .collect::<Vec<_>>()
        );
        Ok(())
    };
    let baseline = memory(1024 * 1024);
    compact(&baseline).unwrap();
    let peak = baseline.ledger.snapshot().peak_bytes;
    assert!(
        peak > 3 * SPOOL_BUFFER_BYTES,
        "two readers and one writer overlap"
    );
    for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
        let memory = memory(limit);
        assert_eq!(compact(&memory).is_ok(), succeeds);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn source_deletion_error_cleans_the_completed_output_and_moved_inputs() {
    let root = projection_root("merge-delete-error");
    fs::create_dir(&root).unwrap();
    let memory = memory(1024 * 1024);
    let config = LexicalProjectionConfig {
        max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
        ..Default::default()
    };
    let mut runs = SpillRuns::new(&root, 1, config, memory.clone()).unwrap();
    for ordinal in 0..3 {
        runs.spill(&mut vec![posting("graph", ordinal)]).unwrap();
    }
    let mut removals = 0;
    let error = runs
        .compact_with_remove(|source| {
            removals += 1;
            assert!(source.exists());
            assert_eq!(
                fs::read_dir(&root).unwrap().count(),
                4,
                "output must exist before the deletion error"
            );
            Err(std::io::Error::other("injected source deletion failure"))
        })
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("injected source deletion failure"));
    assert_eq!(removals, 1);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(runs);
    fs::remove_dir(root).unwrap();
}
