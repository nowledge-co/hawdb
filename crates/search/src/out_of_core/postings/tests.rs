// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Metadata value postings select exactly the documents the metadata scan
//! selects, old postingless layouts keep the scan, and corrupt or unadmitted
//! postings fail closed (#1613).

use super::*;
use crate::out_of_core::candidate_set::{CandidateFilter, CandidateSet};
use crate::out_of_core::tests::{options, test_dir};
use crate::SearchEmbeddingManifest;
use hawdb_optimizer_predicate::SearchPredicateSet;

const DIMENSION: usize = 4;

/// Restores the thread's postings switches even when an assertion fails.
struct Switches;
impl Switches {
    fn new(cap: usize, legacy: bool) -> Self {
        set_distinct_cap(cap);
        write_legacy_layout(legacy);
        disable_read(false);
        Self
    }
}
impl Drop for Switches {
    fn drop(&mut self) {
        set_distinct_cap(MAX_DISTINCT_VALUES);
        write_legacy_layout(false);
        disable_read(false);
    }
}

fn embedding(number: usize) -> Vec<f32> {
    (0..DIMENSION)
        .map(|axis| ((number * 7 + axis * 3) % 11) as f32 / 11.0 + 0.05)
        .collect()
}

/// Test segments hold two documents: `team` and `tier` share one normalized
/// value per segment, spelled differently; `unique` differs in every
/// document, so a cap of one excludes it from every segment.
fn corpus() -> Vec<SearchDocument> {
    (0..40)
        .map(|number| {
            let pair = number / 2;
            let team = match (pair % 3, number % 2) {
                (0, 0) => "Alpha",
                (0, _) => "  alpha ",
                (1, 0) => "BETA",
                (1, _) => "beta",
                (_, 0) => "Gamma ",
                (_, _) => "gAMMA",
            };
            let mut metadata = BTreeMap::from([
                ("team".to_string(), team.to_string()),
                ("unique".to_string(), format!("U{number:02}")),
                (
                    "kind".to_string(),
                    if number % 3 == 0 { "memory" } else { "note" }.to_string(),
                ),
                (
                    "labels".to_string(),
                    if number % 4 == 0 {
                        "red, Blue"
                    } else {
                        "green"
                    }
                    .to_string(),
                ),
                ("metadata.topics".to_string(), "[\"x\",\"y\"]".to_string()),
            ]);
            if pair % 2 == 0 {
                metadata.insert(
                    "tier".to_string(),
                    if number % 2 == 0 { "Gold" } else { " gold" }.to_string(),
                );
            }
            SearchDocument {
                id: format!("memory:{number:04}"),
                title: format!("graph note {number}"),
                content: "storage vectors graph".to_string(),
                embedding: (number % 5 != 3).then(|| embedding(number)),
                metadata,
            }
        })
        .collect()
}

fn build(name: &str, documents: &[SearchDocument]) -> PathBuf {
    let path = test_dir(name);
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &path,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "postings-test".into(),
                version: Some("v1".into()),
                dimension: DIMENSION,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    for document in documents {
        writer.push(document.clone()).unwrap();
    }
    writer.finish().unwrap();
    path
}

fn build_resident(name: &str, documents: &[SearchDocument]) -> PathBuf {
    let path = test_dir(name);
    let mut index = SearchIndex::open(&path).unwrap();
    for document in documents {
        index.upsert(document.clone()).unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    path
}

fn filters() -> Vec<(BTreeMap<String, String>, bool)> {
    let filter = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect::<BTreeMap<_, _>>()
    };
    // (filter, whether every predicate is an Eq/In on an indexed field)
    vec![
        (filter(&[("team", "alpha")]), true),
        (filter(&[("team", " ALPHA  ")]), true),
        (
            filter(&[("team__in", "[\"Alpha\",\"gamma\",\"none\"]")]),
            true,
        ),
        (filter(&[("team", "beta"), ("tier", "GOLD")]), true),
        (
            filter(&[("team__in", "[\"beta\",\"alpha\"]"), ("tier", "gold")]),
            true,
        ),
        (filter(&[("unique", "u07")]), true),
        (filter(&[("unique__in", "[\"U03\",\"u31\"]")]), true),
        (filter(&[("team", "nobody")]), true),
        (filter(&[("absent", "x")]), true),
        (filter(&[("kind", "memory")]), false),
        (filter(&[("labels", "blue")]), false),
        (filter(&[("metadata.topics", "x")]), false),
        (filter(&[("team", "alpha"), ("space_id", "default")]), false),
        (filter(&[("team__not_in", "[\"alpha\"]")]), false),
        (filter(&[("unique__gte", "U30")]), false),
        (filter(&[("tier__exists", "true")]), false),
        (filter(&[("team__in", "[]")]), false),
    ]
}

fn predicates(filter: &BTreeMap<String, String>) -> SearchPredicateSet {
    SearchPredicateSet::from_metadata_filters(filter)
        .unwrap_or_else(|_| SearchPredicateSet::from_metadata_filters(&BTreeMap::new()).unwrap())
}

type Entries = Vec<(usize, u64, String, Option<u64>)>;
type Mutation = Box<dyn Fn(&mut Vec<u8>)>;

/// Candidates of the non-query path (`None`) or the admitted query path.
fn candidates(
    reader: &SearchOutOfCoreReader,
    filter: &BTreeMap<String, String>,
    query: Option<(QueryContext<'_>, &ScratchDirectory)>,
) -> Result<(Entries, SearchPredicatePushdownReport)> {
    let predicates = predicates(filter);
    let mut report = SearchPredicatePushdownReport::default();
    let mut metrics = SearchOutOfCoreMetrics::default();
    let set = reader.build_candidate_set(
        CandidateFilter {
            predicates: &predicates,
            eligible: None,
        },
        &mut report,
        &mut metrics,
        query.map(|(context, _)| context),
        query.map(|(_, scratch)| scratch),
        super::super::query_output::Memory(None),
    )?;
    assert!(matches!(set, CandidateSet::Spilled(_)));
    Ok((set.entries_for_test(), report))
}

/// Candidates of the admitted query path; each query owns fresh scratch.
fn query_candidates(
    reader: &SearchOutOfCoreReader,
    filter: &BTreeMap<String, String>,
    budget: Option<usize>,
) -> Result<(Entries, SearchPredicatePushdownReport)> {
    use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};
    let mut result = None;
    // The metadata scan admits a zstd window per segment; postings do not.
    crate::out_of_core::candidate_set::tests::with_query_memory(
        16 << 20,
        |_, _, _, context, scratch| {
            result = Some(match budget {
                None => candidates(reader, filter, Some((context, scratch))),
                Some(budget) => {
                    let budget = NonZeroUsize::new(budget).unwrap();
                    let ledger = QueryMemoryLedger::new(budget);
                    let account =
                        ledger.account(QueryMemoryClass::ExternalRead, "postings", budget);
                    let tight = QueryContext {
                        memory: &account,
                        ..context
                    };
                    let result = candidates(reader, filter, Some((tight, scratch)));
                    assert_eq!(ledger.snapshot().used_bytes, 0);
                    result
                }
            });
        },
    );
    result.unwrap()
}

fn expected_entries(
    documents: &[SearchDocument],
    filter: &BTreeMap<String, String>,
) -> Vec<String> {
    let predicates = predicates(filter);
    documents
        .iter()
        .filter(|document| {
            crate::search_metadata_matches_predicates(&document.id, &document.metadata, &predicates)
        })
        .map(|document| document.id.clone())
        .collect()
}

fn hits(
    reader: &SearchOutOfCoreReader,
    filter: &BTreeMap<String, String>,
) -> (Vec<(String, u64, u64)>, usize) {
    let query = embedding(9);
    let (mode, query) = match (
        cfg!(feature = "full-text-search"),
        cfg!(feature = "vector-search"),
    ) {
        (true, true) => (SearchMode::Hybrid, Some(query.as_slice())),
        (false, true) => (SearchMode::Vector, Some(query.as_slice())),
        (true, false) => (SearchMode::Text, None),
        // No retriever is compiled: compare the candidate sets only.
        (false, false) => {
            let (_, report) = candidates(reader, filter, None).unwrap();
            return (Vec::new(), report.postings_segment_count);
        }
    };
    let mut options = options(40, None);
    options.metadata_filters = filter.clone();
    let output = reader
        .search_with_options("graph", query, mode, options)
        .unwrap();
    (
        output
            .result
            .hits
            .iter()
            .map(|hit| {
                (
                    hit.id.clone(),
                    hit.score.to_bits(),
                    hit.vector_score.to_bits(),
                )
            })
            .collect(),
        output
            .result
            .candidate_set
            .metadata_predicate_pushdown
            .postings_segment_count,
    )
}

#[test]
fn postings_select_the_scanned_candidates_and_hits() {
    let documents = corpus();
    // Cap 1 leaves `unique` out of every segment's postings.
    for (cap, resident) in [(MAX_DISTINCT_VALUES, false), (1, false), (1, true)] {
        let _switches = Switches::new(cap, false);
        let path = match resident {
            false => build("postings-differential", &documents),
            true => build_resident("postings-differential-resident", &documents),
        };
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        for artifact in &reader.segments {
            assert_eq!(artifact.layout.format, OUT_OF_CORE_LAYOUT_FORMAT);
            assert!(artifact
                .layout
                .segments
                .iter()
                .all(|layout| layout.postings.is_some()));
        }
        for (filter, indexed) in filters() {
            disable_read(false);
            let (entries, report) = candidates(&reader, &filter, None).unwrap();
            let (searched, searched_postings) = hits(&reader, &filter);
            disable_read(true);
            let (scanned, scan_report) = candidates(&reader, &filter, None).unwrap();
            let (scan_hits, scan_postings) = hits(&reader, &filter);
            assert_eq!(entries, scanned, "{filter:?} cap {cap}");
            assert_eq!(searched, scan_hits, "{filter:?} cap {cap}");
            assert_eq!(
                entries
                    .iter()
                    .map(|entry| entry.2.clone())
                    .collect::<Vec<_>>(),
                expected_entries(&documents, &filter),
                "{filter:?}"
            );
            for (_, _, id, ordinal) in &entries {
                let document = documents
                    .iter()
                    .find(|document| &document.id == id)
                    .unwrap();
                assert_eq!(ordinal.is_some(), document.embedding.is_some(), "{id}");
            }
            assert_eq!(scan_report.postings_segment_count, 0);
            assert_eq!(scan_postings, 0);
            assert_eq!(searched_postings, report.postings_segment_count);
            let capped = cap == 1 && filter.keys().any(|key| key.starts_with("unique"));
            assert_eq!(
                report.postings_segment_count > 0,
                indexed && !capped && report.scanned_segment_count > 0,
                "{filter:?} cap {cap}: {report:?}"
            );
            if indexed && !capped {
                assert_eq!(report.postings_segment_count, report.scanned_segment_count);
            }
        }
        drop(reader);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn query_admitted_postings_select_the_scanned_candidates() {
    let documents = corpus();
    let _switches = Switches::new(MAX_DISTINCT_VALUES, false);
    let path = build("postings-query", &documents);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    for (filter, indexed) in filters() {
        disable_read(false);
        let (entries, report) = query_candidates(&reader, &filter, None).unwrap();
        disable_read(true);
        let (scanned, _) = query_candidates(&reader, &filter, None).unwrap();
        assert_eq!(entries, scanned, "{filter:?}");
        assert_eq!(
            report.postings_segment_count > 0,
            indexed && report.scanned_segment_count > 0,
            "{filter:?}"
        );
    }
    disable_read(false);
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn postingless_v2_layouts_search_through_the_metadata_scan() {
    let documents = corpus();
    let current = {
        let _switches = Switches::new(MAX_DISTINCT_VALUES, false);
        build("postings-current", &documents)
    };
    let legacy = {
        let _switches = Switches::new(MAX_DISTINCT_VALUES, true);
        build("postings-legacy", &documents)
    };
    let current_reader = SearchOutOfCoreReader::open(&current).unwrap();
    let legacy_reader = SearchOutOfCoreReader::open(&legacy).unwrap();
    for artifact in &legacy_reader.segments {
        assert_eq!(
            artifact.layout.format,
            POSTINGLESS_OUT_OF_CORE_LAYOUT_FORMAT
        );
        assert!(artifact
            .layout
            .segments
            .iter()
            .all(|layout| layout.postings.is_none()));
        // The serialized V2 shape has no postings key at all.
        let encoded = String::from_utf8(artifact.layout.encode().unwrap()).unwrap();
        assert!(!encoded.contains("postings"));
    }
    for (filter, _) in filters() {
        let (legacy_entries, legacy_report) = candidates(&legacy_reader, &filter, None).unwrap();
        let (current_entries, _) = candidates(&current_reader, &filter, None).unwrap();
        assert_eq!(legacy_entries, current_entries, "{filter:?}");
        assert_eq!(legacy_report.postings_segment_count, 0);
        let (legacy_hits, legacy_postings) = hits(&legacy_reader, &filter);
        assert_eq!(legacy_hits, hits(&current_reader, &filter).0, "{filter:?}");
        assert_eq!(legacy_postings, 0);
    }

    // V2 cannot carry postings, and an unknown newer layout is refused.
    let artifact = &current_reader.segments[0];
    let validate = |layout: &SearchOutOfCoreLayoutBody| {
        layout.validate(
            &current_reader.manifest.segments[0],
            &artifact.descriptor,
            current_reader.config.max_compressed_segment_bytes.get(),
        )
    };
    let mut layout = artifact.layout.clone();
    validate(&layout).unwrap();
    for format in [
        POSTINGLESS_OUT_OF_CORE_LAYOUT_FORMAT,
        "HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V4",
    ] {
        layout.format = format.into();
        let error = validate(&layout).unwrap_err();
        assert!(
            matches!(&error, HawDBError::Storage(message) if message.contains("layout header")),
            "{error:?}"
        );
    }
    // A postings range must follow its metadata range and count its documents.
    let mut layout = artifact.layout.clone();
    layout.segments[0].postings.as_mut().unwrap().entry_count += 1;
    assert!(matches!(validate(&layout), Err(HawDBError::Storage(_))));
    let mut layout = artifact.layout.clone();
    layout.segments[0].postings.as_mut().unwrap().offset += 1;
    assert!(matches!(validate(&layout), Err(HawDBError::Storage(_))));

    drop(current_reader);
    drop(legacy_reader);
    fs::remove_dir_all(current).unwrap();
    fs::remove_dir_all(legacy).unwrap();
}

#[test]
fn corrupt_postings_ranges_fail_closed() {
    let documents = corpus();
    let _switches = Switches::new(MAX_DISTINCT_VALUES, false);
    let path = build("postings-corrupt", &documents);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let filter = BTreeMap::from([("team".to_string(), "alpha".to_string())]);
    let (expected, _) = candidates(&reader, &filter, None).unwrap();
    let artifact = &reader.segments[0];
    let range = artifact.layout.segments[0].postings.unwrap();
    let file = path.join(&reader.manifest.segments[0].metadata_payload_file);
    let mut bytes = fs::read(&file).unwrap();
    bytes[(range.offset + range.length / 2) as usize] ^= 0x40;
    fs::write(&file, &bytes).unwrap();

    let error = candidates(&reader, &filter, None).unwrap_err();
    assert!(
        matches!(&error, HawDBError::Storage(message) if message.contains("postings payload checksum mismatch")),
        "{error:?}"
    );
    let mut options = options(10, None);
    options.metadata_filters = filter.clone();
    if cfg!(feature = "full-text-search") {
        assert!(reader
            .search_with_options("graph", None, SearchMode::Text, options)
            .is_err());
    }
    let error = query_candidates(&reader, &filter, None).unwrap_err();
    assert!(
        matches!(&error, HawDBError::Storage(message) if message.contains("postings payload checksum mismatch")),
        "{error:?}"
    );
    // The metadata scan never reads the postings range.
    disable_read(true);
    assert_eq!(candidates(&reader, &filter, None).unwrap().0, expected);
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn postings_reads_fail_closed_under_a_tight_query_budget() {
    let documents = corpus();
    let _switches = Switches::new(MAX_DISTINCT_VALUES, false);
    let path = build("postings-budget", &documents);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let filter = BTreeMap::from([("team__in".to_string(), "[\"alpha\",\"beta\"]".to_string())]);
    let (expected, _) = candidates(&reader, &filter, None).unwrap();
    let (mut refused, mut admitted) = (0, 0);
    for budget in (1usize..=64).map(|step| step * 512) {
        match query_candidates(&reader, &filter, Some(budget)) {
            Ok((entries, report)) => {
                assert_eq!(entries, expected);
                assert!(report.postings_segment_count > 0);
                admitted += 1;
            }
            Err(HawDBError::ResourceBudgetExceeded(_)) => refused += 1,
            Err(error) => panic!("unexpected error under budget {budget}: {error:?}"),
        }
    }
    assert!(refused > 0 && admitted > 0, "{refused} {admitted}");
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn payload_validation_refuses_inconsistent_postings() {
    let documents = corpus()[..2].to_vec();
    let references = documents.iter().collect::<Vec<_>>();
    let descriptor = SearchSegmentDescriptorEntry::from_documents(0, &references, &BTreeSet::new());
    let payload = encode(&documents, None, u64::MAX, || Ok(()))
        .unwrap()
        .unwrap()
        .payload;
    let range = SearchOutOfCoreRange {
        offset: 0,
        length: payload.len() as u64,
        checksum: checksum_bytes(&payload),
        entry_count: 2,
    };
    let layout = SearchOutOfCoreSegmentLayout {
        segment_id: 0,
        vector_ordinal_base: 5,
        metadata: range,
        vectors: SearchOutOfCoreRange {
            entry_count: 2,
            ..range
        },
        postings: Some(range),
    };
    let query = |filter: &[(&str, &str)]| {
        let filter = filter
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        Query::new(&predicates(&filter), None).unwrap()
    };
    let select = |payload: &[u8], query: &Query| {
        Segment::parse(payload, &descriptor, &layout)
            .and_then(|segment| segment.select(query, None, &mut || Ok(())))
            .map(|selected| selected.map(|(indexes, _)| indexes))
    };
    let team = query(&[("team", "ALPHA")]).unwrap();
    assert_eq!(select(&payload, &team).unwrap(), Some(vec![0, 1]));
    assert_eq!(
        select(
            &payload,
            &query(&[("tier", "gold"), ("unique", "u01")]).unwrap()
        )
        .unwrap(),
        Some(vec![1])
    );
    assert_eq!(
        select(&payload, &query(&[("absent", "x")]).unwrap()).unwrap(),
        Some(vec![])
    );
    for filter in [
        &[("kind", "memory")][..],
        &[("labels", "red")],
        &[("metadata.topics", "x")],
        &[("space_id", "default")],
        &[("document_id", "memory:0000")],
        &[("team__not_in", "[\"x\"]")],
    ] {
        assert!(query(filter).is_none(), "{filter:?}");
    }

    let capped = {
        set_distinct_cap(1);
        let encoded = encode(&documents, None, u64::MAX, || Ok(()));
        set_distinct_cap(MAX_DISTINCT_VALUES);
        encoded.unwrap().unwrap().payload
    };
    assert_eq!(
        select(&capped, &query(&[("unique", "u01")]).unwrap()).unwrap(),
        None
    );
    assert_eq!(select(&capped, &team).unwrap(), Some(vec![0, 1]));
    // A value longer than MAX_VALUE_BYTES leaves its field unindexed.
    let mut long = documents.clone();
    long[1]
        .metadata
        .insert("note".into(), "x".repeat(MAX_VALUE_BYTES + 1));
    long[0].metadata.insert("note".into(), "short".into());
    let long_payload = encode(&long, None, u64::MAX, || Ok(()))
        .unwrap()
        .unwrap()
        .payload;
    assert_eq!(
        select(&long_payload, &query(&[("note", "short")]).unwrap()).unwrap(),
        None
    );
    assert_eq!(select(&long_payload, &team).unwrap(), Some(vec![0, 1]));
    assert!(
        encode(&documents, None, payload.len() as u64 - 1, || Ok(()))
            .unwrap()
            .is_none()
    );

    // Each structural mutation is refused even though the range checksum
    // would be recomputed by a corrupting writer.
    let ids_at = FIXED_BYTES + 2 * 4;
    let flags_at = ids_at + documents[0].id.len() + documents[1].id.len();
    let mut mutations: Vec<Mutation> = vec![
        Box::new(|bytes| bytes[0] ^= 1),
        Box::new(|bytes| bytes[8] = 3),
        Box::new(move |bytes| bytes[ids_at + 10] = b'9'),
        Box::new(move |bytes| bytes[flags_at] = 2),
        Box::new(move |bytes| bytes[flags_at] = 0),
        Box::new(|bytes| bytes.push(0)),
        Box::new(|bytes| {
            bytes.pop();
        }),
    ];
    // The last document index of the last value list is out of range.
    let last = payload.len() - 4;
    mutations.push(Box::new(move |bytes| bytes[last] = 2));
    for (number, mutate) in mutations.iter().enumerate() {
        let mut corrupt = payload.clone();
        mutate(&mut corrupt);
        let result = select(&corrupt, &team);
        assert!(
            matches!(&result, Err(HawDBError::Storage(message)) if message.contains("postings")),
            "mutation {number}: {result:?}"
        );
    }
}

#[test]
fn postings_over_the_reader_segment_budget_fall_back_to_the_metadata_scan() {
    let _switches = Switches::new(MAX_DISTINCT_VALUES, false);
    // Repetitive values just under MAX_VALUE_BYTES compress well in the
    // metadata sidecar but are stored verbatim in the postings, so a reader
    // budget between the two admits every other range and refuses every
    // postings range.
    let mut documents = corpus();
    for (number, document) in documents.iter_mut().enumerate() {
        for field in 0..32 {
            document.metadata.insert(
                format!("note{field:02}"),
                format!("{}{number:02}", "x".repeat(MAX_VALUE_BYTES - 8)),
            );
        }
    }
    let path = build("postings-reader-budget", &documents);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let mut largest_other = 0u64;
    let mut smallest_postings = u64::MAX;
    for artifact in &reader.segments {
        for (layout, segment) in artifact
            .layout
            .segments
            .iter()
            .zip(&artifact.descriptor.segments)
        {
            largest_other = largest_other
                .max(layout.metadata.length)
                .max(layout.vectors.length)
                .max(segment.payload_range.unwrap().length);
            smallest_postings = smallest_postings.min(layout.postings.unwrap().length);
        }
    }
    assert!(
        largest_other < smallest_postings,
        "{largest_other} {smallest_postings}"
    );
    let mut config = reader.config.clone();
    config.max_compressed_segment_bytes = NonZeroU64::new(largest_other).unwrap();
    let tight = SearchOutOfCoreReader::open_with_config(&path, config).unwrap();
    assert!(tight.postings_byte_limit() < smallest_postings);
    for (filter, indexed) in filters() {
        let (expected, report) = candidates(&reader, &filter, None).unwrap();
        assert_eq!(
            report.postings_segment_count > 0,
            indexed && report.scanned_segment_count > 0,
            "{filter:?}"
        );
        let (entries, report) = candidates(&tight, &filter, None).unwrap();
        assert_eq!(entries, expected, "{filter:?}");
        assert_eq!(report.postings_segment_count, 0, "{filter:?}");
        let (queried, report) = query_candidates(&tight, &filter, None).unwrap();
        assert_eq!(queried, expected, "{filter:?}");
        assert_eq!(report.postings_segment_count, 0, "{filter:?}");
    }
    drop(tight);
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}

fn projection_row(document: &SearchDocument) -> crate::SearchProjectionRow {
    let mut metadata = document.metadata.clone();
    metadata.remove("kind");
    crate::SearchProjectionRow {
        kind: crate::SearchProjectionKind::Memory,
        external_id: document.id.trim_start_matches("memory:").to_string(),
        title: document.title.clone(),
        body: document.content.clone(),
        embedding: None,
        source_id: None,
        metadata,
    }
}

#[test]
fn postings_respect_documents_overwritten_or_deleted_in_newer_layers() {
    let _switches = Switches::new(MAX_DISTINCT_VALUES, false);
    let base = corpus()
        .iter()
        .map(|document| projection_row(document).into_document())
        .collect::<Vec<_>>();
    let path = test_dir("postings-shadowed");
    let mut writer = SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
    for document in &base {
        writer.push(document.clone()).unwrap();
    }
    writer.finish().unwrap();

    // A delta layer moves some documents to another team, deletes others and
    // adds new ones; the base layer's postings still list the old values of
    // every overwritten or deleted document.
    let mut upserts = Vec::new();
    let mut deletes = Vec::new();
    for (number, document) in corpus().iter().enumerate() {
        match number % 5 {
            0 => {
                let mut document = document.clone();
                document.metadata.insert("team".into(), "beta".into());
                document.metadata.remove("tier");
                upserts.push(projection_row(&document));
            }
            1 => deletes.push(document.id.clone()),
            _ => {}
        }
    }
    for number in 40..44 {
        let mut document = corpus()[number - 40].clone();
        document.id = format!("memory:{number:04}");
        document.metadata.insert("team".into(), "Alpha".into());
        upserts.push(projection_row(&document));
    }
    let mut documents = base
        .into_iter()
        .filter(|document| {
            !deletes.contains(&document.id)
                && !upserts
                    .iter()
                    .any(|row| format!("memory:{}", row.external_id) == document.id)
        })
        .chain(upserts.iter().map(|row| row.clone().into_document()))
        .collect::<Vec<_>>();
    documents.sort_by(|left, right| left.id.cmp(&right.id));
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        crate::SearchProjectionDelta {
            upserts,
            deletes,
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    drop(reader);

    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    assert_eq!(reader.manifest.segments.len(), 2);
    for (filter, indexed) in filters() {
        disable_read(false);
        let (entries, report) = candidates(&reader, &filter, None).unwrap();
        let (queried, _) = query_candidates(&reader, &filter, None).unwrap();
        disable_read(true);
        let (scanned, _) = candidates(&reader, &filter, None).unwrap();
        disable_read(false);
        assert_eq!(entries, scanned, "{filter:?}");
        assert_eq!(queried, scanned, "{filter:?}");
        let mut ids = entries
            .iter()
            .map(|entry| entry.2.clone())
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, expected_entries(&documents, &filter), "{filter:?}");
        assert_eq!(
            report.postings_segment_count > 0,
            indexed && report.scanned_segment_count > 0,
            "{filter:?}: {report:?}"
        );
    }
    drop(reader);
    fs::remove_dir_all(path).unwrap();
}
