use super::artifact_admission::memory;
use super::*;
use dictionary_memory::Term;
use dictionary_store::{DirectoryBudget, SpillBudget, Writer};
use std::mem::size_of;

pub(super) fn metadata(index: u64) -> dictionary::Metadata {
    dictionary::Metadata {
        df: 1,
        posting_offset: 24 + index * 48,
        posting_bytes: 48,
        skip_offset: 0,
    }
}

pub(super) fn stored_keys(
    root: &Path,
    entries: &[(String, dictionary::Metadata)],
    config: LexicalProjectionConfig,
    memory: &BuildMemory,
) -> Result<BTreeMap<String, dictionary::Metadata>> {
    let directory = DirectoryBudget::new(config.max_directory_bytes.get()).with_memory(memory)?;
    let mut writer = Writer::new(
        &root.join("dictionary.tmp"),
        config,
        SpillBudget::new(0, u64::MAX),
        directory.clone(),
        memory.clone(),
    )?;
    for (key, metadata) in entries {
        writer.push(Term::new(key, memory)?, *metadata)?;
    }
    let mut output = Vec::new();
    let mut offset = 4096;
    let descriptors = writer.finish(&mut output, &mut offset)?;
    assert_eq!(offset, 4096 + output.len() as u64);
    let mut actual = BTreeMap::new();
    let limits = dictionary_store::limits(config)?;
    for descriptor in &descriptors {
        let start = (descriptor.offset - 4096) as usize;
        let block = &output[start..start + descriptor.length as usize];
        assert_eq!(checksum(block), descriptor.checksum);
        let dictionary = dictionary::Dictionary::open(block, limits, &mut || Ok(())).unwrap();
        dictionary
            .visit(|term, metadata| {
                assert_eq!(dictionary.get(term).unwrap(), Some(metadata));
                assert!(actual.insert(term.to_owned(), metadata).is_none());
                Ok(())
            })
            .unwrap();
    }
    // Staging and encoded buffers are gone; only returned descriptor storage is live.
    let retained = descriptors.capacity() * size_of::<dictionary_store::Descriptor>()
        + descriptors
            .iter()
            .map(|entry| entry.min_term.capacity() + entry.max_term.capacity())
            .sum::<usize>();
    assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    drop(descriptors);
    drop(directory);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(!root.join("dictionary.tmp").exists());
    Ok(actual)
}

#[test]
fn term_admission_precedes_cloning_and_follows_the_staging_owner() {
    for (limit, succeeds) in [(4, false), (5, true)] {
        let memory = memory(limit);
        dictionary_memory::evidence::take();
        let term = Term::new("graph", &memory);
        assert_eq!(term.is_ok(), succeeds);
        assert_eq!(
            dictionary_memory::evidence::take(),
            (usize::from(succeeds), 0, 0)
        );
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            if succeeds { 5 } else { 0 }
        );
        drop(term);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    let root = projection_root("dictionary-term-handoff");
    fs::create_dir(&root).unwrap();
    let path = root.join("dictionary.tmp");
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let slots = 4 * size_of::<(Term, dictionary::Metadata)>();
    let memory = memory(path_bytes + slots + 5);
    let mut writer = Writer::new(
        &path,
        Default::default(),
        SpillBudget::new(0, u64::MAX),
        DirectoryBudget::new(u64::MAX),
        memory.clone(),
    )
    .unwrap();
    let term = Term::new("graph", &memory).unwrap();
    writer.push(term, metadata(0)).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, path_bytes + slots + 5);
    assert_eq!(
        memory.ledger.snapshot().peak_bytes,
        path_bytes + slots + 5,
        "transfer must not charge a second key copy"
    );
    drop(writer);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir(root).unwrap();
}

#[test]
fn staging_slot_growth_keeps_old_and_new_arrays_admitted() {
    let root = projection_root("dictionary-slot-growth");
    fs::create_dir(&root).unwrap();
    let path = root.join("dictionary.tmp");
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let slot = size_of::<(Term, dictionary::Metadata)>();
    let peak = path_bytes + 12 * slot + 10;
    for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
        let memory = memory(limit);
        let mut writer = Writer::new(
            &path,
            Default::default(),
            SpillBudget::new(0, u64::MAX),
            DirectoryBudget::new(u64::MAX),
            memory.clone(),
        )
        .unwrap();
        for index in 0..4 {
            writer
                .push(
                    Term::new(&format!("k{index}"), &memory).unwrap(),
                    metadata(index),
                )
                .unwrap();
        }
        let result = writer.push(Term::new("k4", &memory).unwrap(), metadata(4));
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            if succeeds {
                path_bytes + 8 * slot + 10
            } else {
                path_bytes + 4 * slot + 8
            }
        );
        if succeeds {
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
        }
        drop(writer);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn builder_admission_is_before_allocation_and_output_owns_the_remaining_lease() {
    let entries = vec![("graph".to_owned(), metadata(0))];
    let limits = dictionary_store::limits(Default::default()).unwrap();
    let needed = dictionary::builder_reservation(&entries, limits).unwrap();
    for (limit, succeeds) in [(needed, true), (needed - 1, false)] {
        let memory = memory(limit);
        dictionary_memory::evidence::take();
        let result =
            dictionary_memory::encode(&entries, limits, &memory, &RuntimeTaskContext::default());
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(
            dictionary_memory::evidence::take(),
            (0, usize::from(succeeds), usize::from(succeeds))
        );
        if let Ok(encoded) = result {
            let encoded = encoded.unwrap();
            assert_eq!(
                encoded.bytes,
                dictionary::build(&entries, limits, &mut || Ok(())).unwrap()
            );
            assert_eq!(encoded.bytes.capacity(), limits.max_bytes);
            assert_eq!(
                memory.ledger.snapshot().used_bytes,
                encoded.bytes.capacity()
            );
            assert_eq!(memory.ledger.snapshot().peak_bytes, needed);
            drop(encoded);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn validation_scratch_is_admitted_alongside_the_retained_encoded_buffer() {
    let entries = vec![("graph".to_owned(), metadata(0))];
    let limits = dictionary_store::limits(Default::default()).unwrap();
    let needed = dictionary::builder_reservation(&entries, limits).unwrap();
    let memory = memory(needed);
    let encoded =
        dictionary_memory::encode(&entries, limits, &memory, &RuntimeTaskContext::default())
            .unwrap()
            .unwrap();
    let scratch = dictionary::validation_reservation(&encoded.bytes, limits).unwrap();
    let other = memory
        .spool
        .reserve(needed - encoded.bytes.capacity() - scratch + 1)
        .unwrap();
    dictionary_memory::evidence::take();
    assert!(encoded
        .validate(limits, &memory, &RuntimeTaskContext::default())
        .is_err());
    assert_eq!(dictionary_memory::evidence::take(), (0, 0, 0));
    drop(other);
    encoded
        .validate(limits, &memory, &RuntimeTaskContext::default())
        .unwrap()
        .unwrap();
    assert_eq!(dictionary_memory::evidence::take(), (0, 0, 1));
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        encoded.bytes.capacity()
    );
    drop(encoded);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn incoming_term_stays_admitted_while_the_previous_partition_flushes() {
    let root = projection_root("dictionary-held-term");
    fs::create_dir(&root).unwrap();
    let path = root.join("dictionary.tmp");
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let base = LexicalProjectionConfig::default();
    let needed = dictionary::builder_reservation(
        &[("aaaa", metadata(0))],
        dictionary_store::limits(base).unwrap(),
    )
    .unwrap();
    let slots = 4 * size_of::<(Term, dictionary::Metadata)>();
    let config = LexicalProjectionConfig {
        dictionary_build_memory_bytes: NonZeroU64::new((needed + slots + 8) as u64).unwrap(),
        ..base
    };
    let memory = memory(path_bytes + needed + slots + 8);
    let directory = DirectoryBudget::new(u64::MAX).with_memory(&memory).unwrap();
    let mut writer = Writer::new(
        &path,
        config,
        SpillBudget::new(0, u64::MAX),
        directory.clone(),
        memory.clone(),
    )
    .unwrap();
    writer
        .push(Term::new("aaaa", &memory).unwrap(), metadata(0))
        .unwrap();
    dictionary_memory::evidence::take();
    writer
        .push(Term::new("bbbb", &memory).unwrap(), metadata(1))
        .unwrap();
    assert_eq!(dictionary_memory::evidence::take(), (1, 1, 1));
    assert_eq!(
        memory.ledger.snapshot().peak_bytes,
        path_bytes + needed + slots + 8
    );
    let descriptor = size_of::<dictionary_store::Descriptor>() + 8;
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        path_bytes + slots + 4 + descriptor
    );
    drop(writer);
    drop(directory);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir(root).unwrap();
}

#[test]
fn rejected_writer_cannot_publish_after_a_retry() {
    let root = projection_root("dictionary-poison");
    fs::create_dir(&root).unwrap();
    let path = root.join("dictionary.tmp");
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let memory = memory(path_bytes + 5);
    let mut writer = Writer::new(
        &path,
        Default::default(),
        SpillBudget::new(0, u64::MAX),
        DirectoryBudget::new(u64::MAX),
        memory.clone(),
    )
    .unwrap();
    assert!(writer
        .push(Term::new("graph", &memory).unwrap(), metadata(0))
        .is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, path_bytes);
    let error = writer
        .push(Term::new("retry", &memory).unwrap(), metadata(1))
        .unwrap_err();
    assert!(error.to_string().contains("already failed"));
    let mut output = Vec::new();
    assert!(writer
        .finish(&mut output, &mut 0)
        .unwrap_err()
        .to_string()
        .contains("already failed"));
    assert!(output.is_empty());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(!root.join("dictionary.tmp").exists());
    fs::remove_dir(root).unwrap();
}

#[test]
fn cancellation_releases_incoming_keys_without_entering_the_fst_builder() {
    let root = projection_root("dictionary-cancel-admission");
    fs::create_dir(&root).unwrap();
    let memory = memory(4 * 1024 * 1024);
    let task = RuntimeTaskContext::default();
    let mut writer = Writer::new(
        &root.join("dictionary.tmp"),
        Default::default(),
        SpillBudget::new(0, u64::MAX),
        DirectoryBudget::new(u64::MAX),
        memory.clone(),
    )
    .unwrap()
    .with_context(task.clone());
    let term = Term::new("graph", &memory).unwrap();
    dictionary_memory::evidence::take();
    task.cancellation().cancel();
    assert!(writer
        .push(term, metadata(0))
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    assert!(dictionary_memory::encode(
        &[("graph", metadata(0))],
        dictionary_store::limits(Default::default()).unwrap(),
        &memory,
        &task
    )
    .is_err());
    assert_eq!(dictionary_memory::evidence::take(), (0, 0, 0));
    drop(writer);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir(root).unwrap();
}

#[test]
fn recursive_partitions_release_keys_and_keep_returned_directories_admitted() {
    let root = projection_root("dictionary-partition-admission");
    fs::create_dir(&root).unwrap();
    let config = LexicalProjectionConfig {
        max_block_bytes: NonZeroU64::new(512).unwrap(),
        ..Default::default()
    };
    let entries = (0..128)
        .map(|index| (format!("term:{index:04}"), metadata(index)))
        .collect::<Vec<_>>();
    let expected = entries.iter().cloned().collect();
    let baseline = memory(8 * 1024 * 1024);
    assert_eq!(
        stored_keys(&root, &entries, config, &baseline).unwrap(),
        expected
    );
    let peak = baseline.ledger.snapshot().peak_bytes;
    for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
        let memory = memory(limit);
        let result = stored_keys(&root, &entries, config, &memory);
        assert_eq!(result.is_ok(), succeeds);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn term_count_flush_reuses_admitted_slots_and_preserves_every_key() {
    let root = projection_root("dictionary-term-count-flush");
    fs::create_dir(&root).unwrap();
    let entries = (0..1025)
        .map(|index| (format!("key:{index:04}"), metadata(index)))
        .collect::<Vec<_>>();
    let expected = entries.iter().cloned().collect();
    let baseline = memory(16 * 1024 * 1024);
    dictionary_memory::evidence::take();
    assert_eq!(
        stored_keys(&root, &entries, Default::default(), &baseline).unwrap(),
        expected
    );
    assert_eq!(dictionary_memory::evidence::take(), (1025, 2, 2));
    let peak = baseline.ledger.snapshot().peak_bytes;
    for (limit, succeeds) in [(peak, true), (peak - 1, false)] {
        let memory = memory(limit);
        let result = stored_keys(&root, &entries, Default::default(), &memory);
        assert_eq!(result.is_ok(), succeeds);
        if let Ok(actual) = result {
            assert_eq!(actual, expected);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 3);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    }
    fs::remove_dir(root).unwrap();
}

#[test]
fn fst_stream_frame_allowance_covers_the_pinned_dependency_layout() {
    assert!(size_of::<(fst::raw::Node<'_>, usize, fst::raw::Output)>() <= 128);
    assert!(size_of::<fst::raw::Transition>() <= 24);
    assert!(size_of::<usize>() <= 8);
}

#[test]
fn actual_generation_fst_denial_preserves_the_published_artifacts() {
    let root = projection_root("dictionary-generation-admission");
    fs::create_dir(&root).unwrap();
    let input = document("abc", "", "graph");
    let lexicon = SearchAnalyzerLexicon::empty();
    let reader = LexicalProjectionWriter::new(Default::default())
        .write(&root, 1, None, 11, 13, std::iter::once(&input), &lexicon)
        .unwrap();
    let manifest = fs::read(root.join(MANIFEST_FILE)).unwrap();
    let artifact = fs::read(root.join(artifact_file(1))).unwrap();
    let memory = memory(1024 * 1024);
    dictionary_memory::evidence::take();
    let error = LexicalProjectionWriter::new(Default::default())
        .with_memory(memory.clone())
        .write(&root, 2, None, 11, 13, std::iter::once(&input), &lexicon)
        .unwrap_err();
    assert!(error.to_string().contains("memory"), "{error}");
    assert_eq!(
        dictionary_memory::evidence::take(),
        (1, 0, 0),
        "reach dictionary staging but reject before FST allocation"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(fs::read(root.join(MANIFEST_FILE)).unwrap(), manifest);
    assert_eq!(fs::read(root.join(artifact_file(1))).unwrap(), artifact);
    assert_eq!(fs::read_dir(&root).unwrap().count(), 2);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn directory_failure_drops_encoded_output_staging_and_temporary_file() {
    let root = projection_root("dictionary-directory-failure");
    fs::create_dir(&root).unwrap();
    let memory = memory(4 * 1024 * 1024);
    let directory = DirectoryBudget::new(1).with_memory(&memory).unwrap();
    let mut writer = Writer::new(
        &root.join("dictionary.tmp"),
        Default::default(),
        SpillBudget::new(0, u64::MAX),
        directory.clone(),
        memory.clone(),
    )
    .unwrap();
    writer
        .push(Term::new("graph", &memory).unwrap(), metadata(0))
        .unwrap();
    dictionary_memory::evidence::take();
    let mut output = Vec::new();
    assert!(writer
        .finish(&mut output, &mut 0)
        .unwrap_err()
        .to_string()
        .contains("directory budget"));
    assert_eq!(dictionary_memory::evidence::take(), (0, 1, 1));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(output.is_empty());
    assert!(!root.join("dictionary.tmp").exists());
    drop(directory);
    fs::remove_dir(root).unwrap();
}

#[test]
fn layout_rejections_and_root_admission_errors_have_distinct_retry_contracts() {
    let entries = [("graph", metadata(0))];
    let limits = dictionary_store::limits(Default::default()).unwrap();
    let needed = dictionary::builder_reservation(&entries, limits).unwrap();
    let short_layout = dictionary::Limits {
        max_builder_bytes: needed - 1,
        ..limits
    };
    let enough = memory(needed);
    dictionary_memory::evidence::take();
    assert!(dictionary_memory::encode(
        &entries,
        short_layout,
        &enough,
        &RuntimeTaskContext::default()
    )
    .unwrap()
    .is_err());
    let short_root = memory(needed - 1);
    assert!(dictionary_memory::encode(
        &entries,
        limits,
        &short_root,
        &RuntimeTaskContext::default()
    )
    .is_err());
    assert_eq!(dictionary_memory::evidence::take(), (0, 0, 0));
    assert_eq!(enough.ledger.snapshot().used_bytes, 0);
    assert_eq!(short_root.ledger.snapshot().used_bytes, 0);
}
