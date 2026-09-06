use super::*;
use crate::query_memory::QueryMemory;
use crate::RuntimeTaskContext;
use vector_serving::{VectorQuery, VectorScoreScan};

mod fuzz;

fn fixture(name: &str) -> (PathBuf, SearchOutOfCoreReader) {
    let root = test_dir(name);
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions::default(),
    )
    .unwrap();
    for number in 0..9 {
        writer
            .push(document(
                number,
                if number % 2 == 0 { "team" } else { "private" },
            ))
            .unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open_with_config(
        &root,
        SearchOutOfCoreConfig {
            spill_directory: root.join("spill"),
            ..SearchOutOfCoreConfig::default()
        },
    )
    .unwrap();
    #[cfg(feature = "vector-search")]
    assert!(reader.rabitq_projection.is_some());
    (root, reader)
}

fn memory(bytes: usize, results: usize) -> QueryMemory {
    QueryMemory::new(
        NonZeroU64::new(bytes as u64).unwrap(),
        Some(&RuntimeTaskContext::default().with_memory_reservation(
            skein_core::RuntimeMemoryReservation::new(bytes as u64, results as u64),
        )),
    )
    .unwrap()
}

fn modes() -> &'static [CompressedVectorSearchMode] {
    &[
        CompressedVectorSearchMode::Disabled,
        #[cfg(feature = "vector-search")]
        CompressedVectorSearchMode::Required,
    ]
}

fn scan(
    reader: &SearchOutOfCoreReader,
    memory: &QueryMemory,
    mode: CompressedVectorSearchMode,
    limit: Option<usize>,
) -> Result<VectorScoreScan> {
    reader.scan_vector_scores(
        &[1.0, 0.5],
        &CandidateSet::All(reader.document_count()),
        limit,
        mode,
        VectorQuery {
            execution: VectorSearchExecutionOptions::default(),
            memory,
        },
        &mut SearchOutOfCoreMetrics::default(),
    )
}

#[test]
fn raw_vector_rows_retain_capacity_beyond_reader_and_query_drop() {
    let (root, reader) = fixture("vector-row-owner");
    let memory = memory(16 * 1024 * 1024, 16 * 1024 * 1024);
    let ledger = memory.ledger.clone();
    let rows = reader
        .read_vector_segment(
            &reader.descriptor.segments[0],
            &mut SearchOutOfCoreMetrics::default(),
            &memory,
            None,
        )
        .unwrap();
    let bytes = rows.capacity() * std::mem::size_of::<SearchVectorDocument>()
        + rows
            .iter()
            .map(|row| row.id.capacity() + row.embedding.capacity() * 4)
            .sum::<usize>();
    assert!(bytes > 0);
    assert_eq!(ledger.snapshot().used_bytes, bytes);
    drop(memory);
    drop(reader);
    assert_eq!(ledger.snapshot().used_bytes, bytes);
    assert_eq!(rows[0].id, "memory:000");
    drop(rows);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn scalar_and_reranked_scores_keep_the_shared_result_charge() {
    let (root, reader) = fixture("vector-score-owner");
    let mut retained = Vec::new();
    for &mode in modes() {
        let memory = memory(16 * 1024 * 1024, 16 * 1024 * 1024);
        let ledger = memory.ledger.clone();
        let scan = scan(&reader, &memory, mode, Some(3)).unwrap();
        assert_eq!(scan.scores.len(), 3);
        assert_eq!(scan.matching_count, 9);
        let bytes = ledger.snapshot().used_bytes;
        assert!(bytes >= scan.scores.keys().map(String::capacity).sum::<usize>() + 3 * 256);
        if mode == CompressedVectorSearchMode::Required {
            assert_eq!(scan.reranked_candidate_count, 9);
            assert!(scan.candidate_scan_payload_bytes_read > 0);
        }
        let scores = scan.scores;
        drop(memory);
        assert_eq!(ledger.snapshot().used_bytes, bytes);
        retained.push((scores, ledger, bytes));
    }
    drop(reader);
    for (scores, ledger, bytes) in retained {
        assert_eq!(ledger.snapshot().used_bytes, bytes);
        drop(scores);
        assert_eq!(ledger.snapshot().used_bytes, 0);
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn exact_and_short_vector_roots_preserve_preexisting_scores() {
    let (root, reader) = fixture("vector-root-budget");
    for &mode in modes() {
        let baseline = memory(16 * 1024 * 1024, 16 * 1024 * 1024);
        let other = baseline.scores.reserve(123).unwrap();
        let expected = scan(&reader, &baseline, mode, Some(3)).unwrap();
        let peak = baseline.ledger.snapshot().peak_bytes;
        for limit in [peak, peak - 1] {
            let bounded = memory(limit, limit);
            let owner = bounded.scores.reserve(123).unwrap();
            let actual = scan(&reader, &bounded, mode, Some(3));
            assert_eq!(actual.is_ok(), limit == peak);
            if let Ok(actual) = &actual {
                assert_eq!(actual.scores, expected.scores);
            }
            drop(actual);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 123);
            drop(owner);
            assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
        }
        drop(expected);
        drop(other);
        assert_eq!(baseline.ledger.snapshot().used_bytes, 0);
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn vector_rejection_before_io_and_shared_lexical_result_limit_are_enforced() {
    let (root, reader) = fixture("vector-shared-result-cap");
    let memory = memory(2 * 1024 * 1024, 512);
    let mut lexical =
        crate::score_collector::ScoreCollector::new(None, 1, 512, &memory.scores).unwrap();
    lexical.push("lexical".to_owned(), 1.0).unwrap();
    let scores = lexical.finish();
    let retained = memory.ledger.snapshot().used_bytes;
    for &mode in modes() {
        assert!(scan(&reader, &memory, mode, Some(1)).is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    }
    let blocker = memory
        .working
        .reserve(2 * 1024 * 1024 - retained - 1)
        .unwrap();
    query_io::evidence::take();
    assert!(scan(&reader, &memory, CompressedVectorSearchMode::Disabled, None).is_err());
    assert_eq!(query_io::evidence::take().0, 0);
    drop(blocker);
    drop(scores);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn forged_vector_shape_is_rejected_before_row_decoder_allocation() {
    let (root, mut reader) = fixture("vector-corrupt-preflight");
    let memory = memory(16 * 1024 * 1024, 16 * 1024 * 1024);
    let path = root.join("corrupt-vector-fixture");
    let original = reader.layout.segments[0].vectors;
    for (line, count) in [
        ("vector\t0\t61\t1,2\n", usize::MAX),
        ("vector\t0\t61\t1,2,3\n", 1),
        ("vector\t0\t61\tNaN,2\n", 1),
        ("vector\t0\t61\t1,2\textra\n", 1),
        ("vector\t1\t61\t1,2\n", 1),
        ("vector\t0\t61\t1,2\n", 2),
    ] {
        let text = format!("SKEIN_SEARCH_VECTOR_SEGMENT_V1\n{line}");
        let payload = encode_search_snapshot_text(&text).unwrap();
        fs::write(&path, &payload).unwrap();
        reader.vector_payload = Arc::new(File::open(&path).unwrap());
        reader.layout.segments[0].vectors = SearchOutOfCoreRange {
            offset: 0,
            length: payload.len() as u64,
            checksum: checksum_bytes(&payload),
            entry_count: count,
        };
        vector_io::evidence::take();
        assert!(reader
            .read_vector_segment(
                &reader.descriptor.segments[0],
                &mut SearchOutOfCoreMetrics::default(),
                &memory,
                None
            )
            .is_err());
        assert_eq!(vector_io::evidence::take(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    reader.layout.segments[0].vectors = original;
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        scan(
            &reopened,
            &memory,
            CompressedVectorSearchMode::Disabled,
            Some(3)
        )
        .unwrap()
        .scores
        .len(),
        3
    );
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancelled_vector_scan_cannot_read_or_decode_rows() {
    let (root, reader) = fixture("vector-cancelled");
    let memory = memory(2 * 1024 * 1024, 2 * 1024 * 1024);
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    for &mode in modes() {
        query_io::evidence::take();
        vector_io::evidence::take();
        let result = reader.scan_vector_scores(
            &[1.0, 0.5],
            &CandidateSet::All(9),
            Some(3),
            mode,
            VectorQuery {
                execution: VectorSearchExecutionOptions::admitted(2 * 1024 * 1024, &task),
                memory: &memory,
            },
            &mut SearchOutOfCoreMetrics::default(),
        );
        assert!(result.is_err());
        assert_eq!(query_io::evidence::take(), (0, 0));
        assert_eq!(vector_io::evidence::take(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "vector-search")]
#[test]
fn selected_ordinals_remain_charged_when_raw_rerank_starts() {
    use skein_core::{RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit};
    use std::sync::atomic::AtomicUsize;
    #[derive(Debug)]
    struct Probe {
        ledger: skein_executor::QueryMemoryLedger,
        first_bytes: usize,
        calls: AtomicUsize,
    }
    impl RuntimeIoWaveController for Probe {
        fn try_acquire(
            &self,
            slots: NonZeroUsize,
            task: &RuntimeTaskContext,
        ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
            self.acquire(slots, task).map(Some)
        }
        fn acquire(
            &self,
            slots: NonZeroUsize,
            _: &RuntimeTaskContext,
        ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
            assert_eq!(slots.get(), 1);
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                assert_eq!(self.ledger.snapshot().used_bytes, self.first_bytes);
            }
            Ok(Box::new(()))
        }
    }
    let (root, reader) = fixture("vector-rerank-overlap");
    let memory = memory(16 * 1024 * 1024, 16 * 1024 * 1024);
    let probe = Arc::new(Probe {
        ledger: memory.ledger.clone(),
        first_bytes: 9 * std::mem::size_of::<u64>()
            + 3 * std::mem::size_of::<(String, f64)>()
            + reader.layout.segments[0].vectors.length as usize,
        calls: AtomicUsize::new(0),
    });
    let task = RuntimeTaskContext::default().with_io_wave_controller(probe.clone());
    let output = reader
        .scan_vector_scores(
            &[1.0, 0.5],
            &CandidateSet::All(9),
            Some(3),
            CompressedVectorSearchMode::Required,
            VectorQuery {
                execution: VectorSearchExecutionOptions::admitted(16 * 1024 * 1024, &task),
                memory: &memory,
            },
            &mut SearchOutOfCoreMetrics::default(),
        )
        .unwrap();
    assert!(probe.calls.load(Ordering::SeqCst) > 0);
    drop(output);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
