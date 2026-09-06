use super::*;
use std::fs::OpenOptions;

fn build(root: &Path, cache: Arc<SegmentCache>) -> Arc<LexicalProjectionReader> {
    fs::create_dir_all(root).unwrap();
    let documents = (0..257)
        .map(|index| document(&format!("memory:{index:08}"), "", "graph"))
        .collect::<Vec<_>>();
    LexicalProjectionWriter::new(LexicalProjectionConfig::default())
        .with_cache(cache)
        .write(
            root,
            1,
            None,
            11,
            13,
            documents.iter(),
            &SearchAnalyzerLexicon::default(),
        )
        .unwrap()
}

#[test]
fn cold_and_warm_queries_account_dictionary_doclist_and_mapping_bytes_separately() {
    let root = projection_root("compact-cache-counters");
    let cache = Arc::new(SegmentCache::new(1024 * 1024));
    let reader = build(&root, cache.clone());
    let terms = BTreeSet::from(["graph".to_string()]);
    let cold = reader
        .score(&terms, &LexicalMiniDelta::default(), None, |_| Ok(true))
        .unwrap();
    let metadata = reader.term_metadata("graph", &mut 0).unwrap().unwrap();
    assert_eq!(
        cold.dictionary_bytes_read,
        reader.manifest.dictionaries[0].length
    );
    assert_eq!(cold.posting_bytes_read, metadata.posting_bytes);
    assert_eq!(cold.document_bytes_read, document_mapping_bytes(&reader));
    assert_eq!(
        cold.bytes_read,
        cold.dictionary_bytes_read + cold.posting_bytes_read + cold.document_bytes_read
    );
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    let warm = reader
        .score(&terms, &LexicalMiniDelta::default(), None, |_| Ok(true))
        .unwrap();
    assert_eq!(warm.scores, cold.scores);
    assert_eq!(warm.dictionary_bytes_read, 0);
    assert_eq!(warm.document_bytes_read, 0);
    // Three frame prefixes and the complete embedded skip index remain physical reads.
    assert_eq!(warm.posting_bytes_read, 3 * 12 + 16 + 3 * 16);
    assert_eq!(warm.bytes_read, warm.posting_bytes_read);
    assert!(cache.snapshot().hit_count > 0);
    assert!(cache.snapshot().resident_bytes <= cache.snapshot().capacity_bytes);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn pinned_mapping_blocks_reject_cache_admission_until_the_last_lease_is_dropped() {
    let root = projection_root("compact-cache-pins");
    let initial = build(&root, Arc::new(SegmentCache::new(1024 * 1024)));
    let block = initial.manifest.blocks[0].clone();
    let cache = Arc::new(SegmentCache::new(block.length));
    assert!(initial.manifest.dictionaries[0].length <= block.length);
    drop(initial);
    let reader = LexicalProjectionReader::load_named_with_cache(
        &root,
        MANIFEST_FILE,
        None,
        11,
        13,
        LexicalProjectionConfig::default(),
        cache.clone(),
    )
    .unwrap()
    .unwrap();
    let mut mapping = DocumentLookup::new(&reader);
    assert_eq!(mapping.get(0).unwrap().0, "memory:00000000");
    assert_eq!(cache.snapshot().pinned_bytes, block.length);
    let error = reader.term_metadata("graph", &mut 0).unwrap_err();
    assert!(error.to_string().contains("cache admission failed"));
    assert_eq!(cache.snapshot().admission_rejection_count, 1);
    drop(mapping);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    assert_eq!(
        reader.term_metadata("graph", &mut 0).unwrap().unwrap().df,
        257
    );
    assert!(cache.snapshot().eviction_count > 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn reopened_readers_share_capacity_without_aliasing_immutable_cache_identities() {
    let root = projection_root("compact-cache-reopen");
    let cache = Arc::new(SegmentCache::new(1024 * 1024));
    let reader = build(&root, cache.clone());
    let expected = reader.term_metadata("graph", &mut 0).unwrap();
    let reopened = LexicalProjectionReader::load_named_with_cache(
        &root,
        MANIFEST_FILE,
        None,
        11,
        13,
        LexicalProjectionConfig::default(),
        cache.clone(),
    )
    .unwrap()
    .unwrap();
    assert!(!Arc::ptr_eq(&reader, &reopened));
    assert!(Arc::ptr_eq(&reader.cache, &reopened.cache));
    assert_ne!(reader.cache_namespace, reopened.cache_namespace);
    let mut read = 0;
    assert_eq!(
        reopened.term_metadata("graph", &mut read).unwrap(),
        expected
    );
    assert_eq!(read, reader.manifest.dictionaries[0].length);
    let before = cache.snapshot();
    assert!(reopened.read_cached_range(u64::MAX, 1, 0).is_err());
    assert_eq!(cache.snapshot(), before);
    drop(reader);
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn dictionary_lookup_reads_df_without_touching_a_damaged_doclist() {
    let root = projection_root("compact-dictionary-df");
    let reader = build(&root, Arc::new(SegmentCache::new(1024 * 1024)));
    let offset = reader.manifest.posting_offset;
    let path = root.join(&reader.manifest.artifact_file);
    let mut writer = OpenOptions::new().write(true).open(path).unwrap();
    writer.seek(SeekFrom::Start(offset + 12)).unwrap();
    writer.write_all(b"BAD!").unwrap();
    writer.sync_all().unwrap();
    drop(writer);
    let mut read = 0;
    assert_eq!(
        reader
            .term_metadata("graph", &mut read)
            .unwrap()
            .unwrap()
            .df,
        257
    );
    assert_eq!(read, reader.manifest.dictionaries[0].length);
    assert!(reader
        .score(
            &BTreeSet::from(["graph".to_string()]),
            &LexicalMiniDelta::default(),
            None,
            |_| Ok(true)
        )
        .is_err());
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn dictionary_staging_capacity_and_shared_directory_are_admitted() {
    use dictionary_store::{DirectoryBudget, SpillBudget};
    let root = projection_root("compact-dictionary-budgets");
    fs::create_dir_all(&root).unwrap();
    let path = root.join("dictionary.tmp");
    let config = LexicalProjectionConfig::default();
    let memory = BuildMemory::new(&RuntimeTaskContext::default()).unwrap();
    let mut writer = dictionary_store::Writer::new(
        &path,
        config,
        SpillBudget::new(0, u64::MAX),
        DirectoryBudget::new(u64::MAX),
        memory.clone(),
    )
    .unwrap();
    let mut key = String::with_capacity(config.dictionary_build_memory_bytes.get() as usize);
    key.push_str("graph");
    let metadata = dictionary::Metadata {
        df: 1,
        posting_offset: 24,
        posting_bytes: 48,
        skip_offset: 0,
    };
    // Logical key length fits; the owned allocation does not.
    assert!(writer
        .push(
            dictionary_memory::Term::from_owned(key, &memory).unwrap(),
            metadata
        )
        .unwrap_err()
        .to_string()
        .contains("staging budget"));
    drop(writer);
    assert!(!path.exists());

    let directory = DirectoryBudget::new(32);
    let mut document_entries = Vec::<u64>::new();
    directory.admit(&mut document_entries, 8).unwrap();
    document_entries.push(1);
    let mut dictionary_entries = Vec::<u64>::new();
    directory.clone().admit(&mut dictionary_entries, 8).unwrap();
    dictionary_entries.push(2);
    assert!(directory.admit(&mut dictionary_entries, 0).is_err());
    assert_eq!(document_entries, vec![1]);
    assert_eq!(dictionary_entries, vec![2]);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn bounded_dictionary_partitions_preserve_all_ordered_term_locations() {
    let root = projection_root("compact-dictionary-partitions");
    fs::create_dir_all(&root).unwrap();
    let config = LexicalProjectionConfig {
        max_block_bytes: NonZeroU64::new(512).unwrap(),
        ..LexicalProjectionConfig::default()
    };
    let path = root.join("dictionary.tmp");
    let memory = BuildMemory::new(&RuntimeTaskContext::default()).unwrap();
    let mut writer = dictionary_store::Writer::new(
        &path,
        config,
        dictionary_store::SpillBudget::new(0, u64::MAX),
        dictionary_store::DirectoryBudget::new(128 * 1024),
        memory.clone(),
    )
    .unwrap();
    let expected = (0..128)
        .map(|index| {
            (
                format!("term:{index:04}"),
                dictionary::Metadata {
                    df: 1,
                    posting_offset: 24 + index * 48,
                    posting_bytes: 48,
                    skip_offset: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for (term, metadata) in &expected {
        writer
            .push(
                dictionary_memory::Term::new(term, &memory).unwrap(),
                *metadata,
            )
            .unwrap();
    }
    let mut bytes = Vec::new();
    let mut offset = 4096;
    let descriptors = writer.finish(&mut bytes, &mut offset).unwrap();
    assert!(descriptors.len() > 1);
    assert_eq!(offset, 4096 + bytes.len() as u64);
    assert!(!path.exists());
    let mut actual = BTreeMap::new();
    for descriptor in &descriptors {
        assert!(descriptor.length <= config.max_block_bytes.get());
        let start = (descriptor.offset - 4096) as usize;
        let block = &bytes[start..start + descriptor.length as usize];
        assert_eq!(checksum(block), descriptor.checksum);
        let dictionary = dictionary::Dictionary::open(
            block,
            dictionary_store::limits(config).unwrap(),
            &mut || Ok(()),
        )
        .unwrap();
        descriptor.validate_dictionary(&dictionary, 1).unwrap();
        dictionary
            .visit(|term, metadata| {
                assert_eq!(dictionary.get(term).unwrap(), Some(metadata));
                assert!(actual.insert(term.to_string(), metadata).is_none());
                Ok(())
            })
            .unwrap();
        assert_eq!(dictionary.get("missing").unwrap(), None);
    }
    assert_eq!(actual, expected);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "full-text-search")]
#[test]
fn public_search_index_uses_the_injected_cache_across_checkpoint_and_reopen() {
    let root = projection_root("compact-public-cache");
    let cache = Arc::new(crate::SearchSegmentCache::new(1024 * 1024));
    let options = crate::SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: Some(10),
        fusion_weights: crate::SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    };
    let mut index = crate::SearchIndex::open_with_segment_cache(&root, cache.clone()).unwrap();
    index.upsert(document("a", "", "graph")).unwrap();
    index.checkpoint().unwrap();
    let result = index
        .try_search_with_options("graph", None, crate::SearchMode::Text, options.clone())
        .unwrap();
    assert_eq!(result.hits[0].id, "a");
    let report = result
        .retrievers
        .iter()
        .find(|retriever| retriever.name == "text")
        .unwrap();
    assert!(report.lexical_dictionary_bytes_read > 0);
    assert!(report.lexical_document_bytes_read > 0);
    assert!(report.posting_bytes_read > 0);
    assert!(cache.snapshot().resident_bytes > 0);
    let insertions = cache.snapshot().insertion_count;
    drop(index);
    let reopened = crate::SearchIndex::open_with_segment_cache(&root, cache.clone()).unwrap();
    let result = reopened
        .try_search_with_options("graph", None, crate::SearchMode::Text, options)
        .unwrap();
    assert_eq!(result.hits[0].id, "a");
    assert!(cache.snapshot().insertion_count > insertions);
    assert_eq!(cache.snapshot().pinned_bytes, 0);
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}
