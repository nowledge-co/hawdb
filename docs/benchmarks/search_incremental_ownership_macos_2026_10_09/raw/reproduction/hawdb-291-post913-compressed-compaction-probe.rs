use hawdb::{
    CompressedVectorSearchMode, SearchMode, SearchOutOfCoreGenerationWriter, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreReader, SearchOutOfCoreSegmentCompactionPolicy,
    SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
    SearchQueryOptions,
};
use std::num::{NonZeroU64, NonZeroUsize};

fn row(number: usize) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{number:06}"),
        title: format!("compaction {number}"),
        body: "immutable append compaction coverage".into(),
        embedding: Some(vec![1.0, number as f32]),
        source_id: None,
        metadata: Default::default(),
    }
}

fn compare(candidate: &SearchOutOfCoreReader, reference: &SearchOutOfCoreReader) -> usize {
    let mut comparisons = 0;
    for query in [[1.0f32, 2.5], [-2.0, 1.0]] {
        for limit in [1, 3, 10] {
            for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
                let options = SearchQueryOptions {
                    limit,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: Default::default(),
                    metadata_filters: Default::default(),
                    policy_epoch: None,
                };
                let expected = reference.search_with_options("compaction replacement", Some(&query), mode, options.clone()).unwrap();
                let actual = candidate.search_with_options("compaction replacement", Some(&query), mode, options.clone()).unwrap();
                assert_eq!(actual.result.hits, expected.result.hits);
                assert_eq!(actual.result.total_hits, expected.result.total_hits);
                comparisons += 1;
                if mode != SearchMode::Text {
                    let reference_compressed = reference.search_with_options_compressed_vector_projection_mode("compaction replacement", Some(&query), mode, options.clone(), CompressedVectorSearchMode::Required).unwrap();
                    let actual_compressed = candidate.search_with_options_compressed_vector_projection_mode("compaction replacement", Some(&query), mode, options, CompressedVectorSearchMode::Required).unwrap();
                    assert!(actual_compressed.metrics.rabitq_payload_bytes_read > 0);
                    assert!(reference_compressed.metrics.rabitq_payload_bytes_read > 0);
                    assert_eq!(actual_compressed.result.hits, reference_compressed.result.hits);
                    assert_eq!(actual_compressed.result.total_hits, reference_compressed.result.total_hits);
                    assert_eq!(actual_compressed.result.hits, expected.result.hits);
                    assert_eq!(actual_compressed.result.total_hits, expected.result.total_hits);
                    comparisons += 2;
                }
            }
        }
    }
    comparisons
}

fn main() {
    let root = std::env::args().nth(1).expect("isolated synthetic fixture directory required");
    let candidate_root = std::path::Path::new(&root).join("candidate");
    let reference_root = std::path::Path::new(&root).join("reference");
    let initial_options = SearchOutOfCoreGenerationBuildOptions { max_content_documents: NonZeroUsize::new(2).unwrap(), ..Default::default() };
    let mut writer = SearchOutOfCoreGenerationWriter::create(&candidate_root, initial_options).unwrap();
    for number in [0, 2, 4, 6] {
        writer.push(row(number).into_document()).unwrap();
    }
    let initial = writer.finish().unwrap();
    assert_eq!(initial.published_content_segments, 2);
    assert_eq!(initial.document_count, 4);
    for number in [1, 3] {
        let reader = SearchOutOfCoreReader::open(&candidate_root).unwrap();
        SearchOutOfCoreGenerationWriter::prepare_delta(&reader, SearchProjectionDelta { upserts: vec![row(number)], ..Default::default() }, Default::default()).unwrap().finish().unwrap();
    }
    let mut replacement = row(2);
    replacement.body = "replacement compaction exact corpus statistics".into();
    replacement.embedding = Some(vec![12.0, -3.0]);
    let reader = SearchOutOfCoreReader::open(&candidate_root).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(&reader, SearchProjectionDelta { upserts: vec![replacement.clone()], deletes: vec![row(4).into_document().id], ..Default::default() }, Default::default()).unwrap().finish().unwrap();
    drop(reader);
    let pinned = SearchOutOfCoreReader::open(&candidate_root).unwrap();
    assert_eq!(pinned.document_count(), 5);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&reference_root, Default::default()).unwrap();
    for number in [0, 1, 2, 3, 6] {
        writer.push(if number == 2 { replacement.clone() } else { row(number) }.into_document()).unwrap();
    }
    writer.finish().unwrap();
    let reference = SearchOutOfCoreReader::open(&reference_root).unwrap();
    let mut comparisons = compare(&pinned, &reference);
    let policy = SearchOutOfCoreSegmentCompactionPolicy::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::new(256 * 1024 * 1024).unwrap()).unwrap();
    for stage in 0..3 {
        let reader = SearchOutOfCoreReader::open(&candidate_root).unwrap();
        let report = SearchOutOfCoreGenerationWriter::compact_segments(&reader, policy, Default::default()).unwrap().expect("each overlapping merge must publish");
        assert!(!report.build().cleanup_retry_required);
        let fresh = SearchOutOfCoreReader::open(&candidate_root).unwrap();
        assert_eq!(fresh.document_count(), 5);
        comparisons += compare(&fresh, &reference);
        comparisons += compare(&pinned, &reference);
        println!("compressed_compaction_stage stage={stage} generation={} comparisons={comparisons}", fresh.generation());
    }
    assert_eq!(comparisons, 294);
    println!("compressed_compaction passed comparisons={comparisons} distinct_vectors=true changed_embedding=true merges=3 initial_owners=2 pinned_reader=true rabitq_required=true");
}
