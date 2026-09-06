use super::*;

fn assert_cancelled(error: SkeinError) {
    assert!(matches!(error, SkeinError::Execution(_)), "{error}");
    assert!(error.to_string().contains("cancelled"), "{error}");
}

#[test]
fn cancelling_lexical_scan_removes_spill_files_and_preserves_previous_manifest() {
    let root = projection_root("cancel-build-spill");
    fs::create_dir(&root).unwrap();
    let config = LexicalProjectionConfig {
        build_memory_bytes: NonZeroU64::new(1024).unwrap(),
        ..Default::default()
    };
    let analyzer = SearchAnalyzerLexicon::default();
    let documents = (0..32)
        .map(|index| document(&format!("doc:{index:04}"), "graph memory", "query storage"))
        .collect::<Vec<_>>();
    let writer = LexicalProjectionWriter::new(config);
    drop(
        writer
            .write(&root, 1, None, 11, 13, documents.iter(), &analyzer)
            .unwrap(),
    );
    let before = fs::read(root.join(MANIFEST_FILE)).unwrap();
    let task = RuntimeTaskContext::default();
    let cancelled = writer.with_context(task.clone());
    let error = cancelled
        .write_scanned(
            &root,
            2,
            None,
            11,
            13,
            |consume| {
                for (ordinal, document) in documents.iter().enumerate() {
                    consume(ordinal as u64, document)?;
                    if ordinal == 7 {
                        assert!(fs::read_dir(&root).unwrap().any(|entry| entry
                            .unwrap()
                            .file_name()
                            .to_string_lossy()
                            .starts_with(".search-lexical.2.")));
                        task.cancellation().cancel();
                        break;
                    }
                }
                Ok(())
            },
            &analyzer,
        )
        .unwrap_err();
    assert_cancelled(error);
    assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), before);
    assert!(!root.join(artifact_file(2)).exists());
    assert!(fs::read_dir(&root).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with("tmp")));
    let retry = LexicalProjectionWriter::new(config)
        .write(&root, 2, None, 11, 13, documents.iter(), &analyzer)
        .unwrap();
    assert_eq!(retry.generation(), 2);
    drop(retry);
    fs::remove_dir_all(root).unwrap();
}

struct CancelOnWrite {
    task: RuntimeTaskContext,
    bytes: usize,
}

impl Write for CancelOnWrite {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes += bytes.len();
        self.task.cancellation().cancel();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn cancellation_during_skip_copy_stops_before_more_spill_reads() {
    let root = projection_root("cancel-skip-copy");
    fs::create_dir(&root).unwrap();
    let path = root.join("skip.tmp");
    let task = RuntimeTaskContext::default();
    let mut writer = doclist::Writer::new(
        &path,
        dictionary_store::SpillBudget::new(0, u64::MAX),
        posting_codec::MAX_BLOCK_BYTES as u64,
    )
    .unwrap()
    .with_context(task.clone());
    let mut bytes = Vec::new();
    let mut offset = 0;
    for first in [0, 128] {
        let postings = (first..first + 128)
            .map(|ordinal| posting_codec::Posting { ordinal, tf: 1 })
            .collect::<Vec<_>>();
        writer
            .push_frame(&mut bytes, &mut offset, &postings)
            .unwrap();
    }
    let mut output = CancelOnWrite { task, bytes: 0 };
    assert_cancelled(writer.finish(&mut output, &mut offset).unwrap_err());
    assert_eq!(output.bytes, 12, "only the skip header was written");
    drop(writer);
    assert!(!path.exists());
    fs::remove_dir(root).unwrap();
}

#[test]
fn cancellation_during_dictionary_copy_does_not_return_a_completed_directory() {
    let root = projection_root("cancel-dictionary-copy");
    fs::create_dir(&root).unwrap();
    let path = root.join("dictionary.tmp");
    let task = RuntimeTaskContext::default();
    let mut writer = dictionary_store::Writer::new(
        &path,
        Default::default(),
        dictionary_store::SpillBudget::new(0, u64::MAX),
        dictionary_store::DirectoryBudget::new(u64::MAX),
    )
    .unwrap()
    .with_context(task.clone());
    writer
        .push(
            "graph".to_string(),
            dictionary::Metadata {
                df: 1,
                posting_offset: 24,
                posting_bytes: 48,
                skip_offset: 0,
            },
        )
        .unwrap();
    let mut output = CancelOnWrite { task, bytes: 0 };
    let mut offset = 4096;
    assert_cancelled(writer.finish(&mut output, &mut offset).unwrap_err());
    assert!(output.bytes > 0);
    assert_eq!(offset, 4096);
    assert!(!path.exists());
    fs::remove_dir(root).unwrap();
}
