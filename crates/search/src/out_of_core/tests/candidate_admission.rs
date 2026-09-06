use super::*;
use crate::query_memory::QueryMemory;
#[cfg(feature = "full-text-search")]
use skein_core::RuntimeMemoryReservation;
use skein_core::{
    RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeTaskContext,
};
use std::sync::atomic::AtomicUsize;

mod fuzz;
mod metadata;

fn memory(limit: usize) -> QueryMemory {
    QueryMemory::new(NonZeroU64::new(limit as u64).unwrap(), None).unwrap()
}

fn fixture(name: &str) -> (PathBuf, SearchOutOfCoreReader) {
    let root = test_dir(name);
    let mut index = SearchIndex::open(&root).unwrap();
    for number in 0..9 {
        index
            .upsert(document(
                number,
                if number % 2 == 0 { "team" } else { "private" },
            ))
            .unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let reader = SearchOutOfCoreReader::open_with_config(
        &root,
        SearchOutOfCoreConfig {
            spill_directory: root.join("spill"),
            ..SearchOutOfCoreConfig::default()
        },
    )
    .unwrap();
    (root, reader)
}

fn candidates(
    reader: &SearchOutOfCoreReader,
    memory: &QueryMemory,
    task: Option<&RuntimeTaskContext>,
) -> Result<CandidateSet> {
    let mut predicates = search_metadata_predicate_pushdown(&BTreeMap::from([(
        "space_id".to_owned(),
        "team".to_owned(),
    )]));
    reader.build_candidate_set(
        &predicates.predicates,
        &mut predicates.report,
        &mut SearchOutOfCoreMetrics::default(),
        memory,
        task,
    )
}

fn wire(rows: &[(&str, Option<u64>)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (id, ordinal) in rows {
        bytes.extend((id.len() as u32).to_le_bytes());
        bytes.extend(id.as_bytes());
        bytes.extend(ordinal.unwrap_or(u64::MAX).to_le_bytes());
    }
    bytes
}

fn spilled(
    root: &Path,
    groups: &[Vec<(&str, Option<u64>)>],
    memory: &QueryMemory,
) -> SpilledCandidateSet {
    let path = unique_candidate_path(root);
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.write_all(CANDIDATE_FILE_HEADER).unwrap();
    let directory_bytes = groups.len() * std::mem::size_of::<CandidateBlock>()
        + groups
            .iter()
            .map(|group| group.first().unwrap().0.len() + group.last().unwrap().0.len())
            .sum::<usize>();
    let lease = memory.working.reserve(directory_bytes).unwrap();
    let mut blocks = Vec::with_capacity(groups.len());
    let mut offset = CANDIDATE_FILE_HEADER.len() as u64;
    for (index, rows) in groups.iter().enumerate() {
        let bytes = wire(rows);
        file.write_all(&bytes).unwrap();
        blocks.push(CandidateBlock {
            segment_id: index as u64,
            first_document_id: rows.first().unwrap().0.to_owned(),
            last_document_id: rows.last().unwrap().0.to_owned(),
            offset,
            length: bytes.len() as u64,
            cardinality: rows.len(),
        });
        offset += bytes.len() as u64;
    }
    SpilledCandidateSet {
        file: Some(file),
        path,
        blocks,
        cardinality: groups.iter().map(Vec::len).sum(),
        max_block_bytes: 1024 * 1024,
        cache: Mutex::new(None),
        memory: memory.working.clone(),
        task: RuntimeTaskContext::default(),
        _directory_memory: lease,
    }
}

#[test]
fn directory_cache_and_query_owner_lifetimes_share_one_root() {
    let (root, reader) = fixture("candidate-owner");
    let memory = reader.lexical_projection.query_memory(None).unwrap();
    let ledger = memory.ledger.clone();
    let set = candidates(&reader, &memory, None).unwrap();
    let directory = candidate_memory::directory_bytes(&reader.descriptor.segments).unwrap()
        + std::mem::size_of::<SpilledCandidateSet>();
    assert_eq!(ledger.snapshot().used_bytes, directory);
    let mut metrics = SearchOutOfCoreMetrics::default();
    for number in 0..9 {
        assert_eq!(
            set.contains(&format!("memory:{number:03}"), &mut metrics)
                .unwrap(),
            number % 2 == 0
        );
    }
    let retained = ledger.snapshot().used_bytes;
    assert!(retained > directory);
    let CandidateSet::Spilled(spilled) = &set else {
        panic!("filtered fixture must spill");
    };
    assert_eq!(
        metrics.candidate_block_reads as usize,
        spilled
            .blocks
            .iter()
            .filter(|block| block.cardinality > 0)
            .count()
    );
    let reads = metrics.candidate_block_reads;
    assert!(set.contains("memory:008", &mut metrics).unwrap());
    assert_eq!(metrics.candidate_block_reads, reads);
    assert_eq!(ledger.snapshot().account_count, 2);
    drop(memory);
    drop(reader);
    assert_eq!(ledger.snapshot().used_bytes, retained);
    drop(set);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert!(fs::read_dir(root.join("spill")).unwrap().next().is_none());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn exact_candidate_build_and_one_short_failure_preserve_publication() {
    let (root, reader) = fixture("candidate-root-budget");
    let old = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let baseline = memory(128 * 1024 * 1024);
    drop(candidates(&reader, &baseline, None).unwrap());
    let peak = baseline.ledger.snapshot().peak_bytes;
    for limit in [peak, peak - 1] {
        let bounded = memory(limit);
        let result = candidates(&reader, &bounded, None);
        assert_eq!(result.is_ok(), limit == peak);
        drop(result);
        assert_eq!(bounded.ledger.snapshot().used_bytes, 0);
        assert!(fs::read_dir(root.join("spill")).unwrap().next().is_none());
        assert_eq!(fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(), old);
    }
    let directory = candidate_memory::directory_bytes(&reader.descriptor.segments).unwrap();
    query_io::evidence::take();
    assert!(candidates(&reader, &memory(directory - 1), None).is_err());
    assert_eq!(query_io::evidence::take(), (0, 0));
    drop(reader);
    let reopened = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reopened
            .hydrate_documents(&["memory:000".to_owned()])
            .unwrap()
            .documents
            .len(),
        1
    );
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_cache_replacement_keeps_old_entries_and_charges() {
    let root = test_dir("candidate-cache-replace");
    let memory = memory(8192);
    let mut set = spilled(
        &root,
        &[
            vec![("a", Some(1)), ("b", None)],
            vec![("c", Some(2)), ("d", Some(3))],
        ],
        &memory,
    );
    let mut metrics = SearchOutOfCoreMetrics::default();
    assert!(set.contains("a", &mut metrics).unwrap());
    let old = memory.ledger.snapshot().used_bytes;
    let blocker = memory.scores.reserve(8192 - old - 1).unwrap();
    query_io::evidence::take();
    assert!(set.contains("c", &mut metrics).is_err());
    assert_eq!(query_io::evidence::take().0, 0);
    assert!(set.contains("a", &mut metrics).unwrap());
    drop(blocker);
    assert_eq!(memory.ledger.snapshot().used_bytes, old);
    set.blocks[1].cardinality += 1;
    candidate_codec::evidence::take();
    assert!(set.contains("c", &mut metrics).is_err());
    assert_eq!(candidate_codec::evidence::take(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, old);
    assert!(set.contains("a", &mut metrics).unwrap());
    set.blocks[1].cardinality -= 1;
    assert!(set.contains("c", &mut metrics).unwrap());
    assert!(memory.ledger.snapshot().peak_bytes >= old + set.blocks[1].length as usize);
    drop(set);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn concurrent_cache_replacements_share_the_same_root() {
    let root = test_dir("candidate-concurrent-cache");
    let memory = memory(8192);
    let set = spilled(
        &root,
        &[
            vec![("a", None), ("b", None)],
            vec![("c", None), ("d", None)],
        ],
        &memory,
    );
    let initial = memory.ledger.snapshot().used_bytes;
    let entry_bytes = 2 * std::mem::size_of::<CandidateEntry>() + 2;
    let raw_bytes = wire(&[("a", None), ("b", None)]).len();
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let set = &set;
            scope.spawn(move || {
                let mut metrics = SearchOutOfCoreMetrics::default();
                for step in 0..100 {
                    assert!(set
                        .contains(
                            if (step + worker) % 2 == 0 { "a" } else { "c" },
                            &mut metrics
                        )
                        .unwrap());
                    assert!(!set.contains("z", &mut metrics).unwrap());
                }
            });
        }
    });
    assert_eq!(memory.ledger.snapshot().used_bytes, initial + entry_bytes);
    assert_eq!(
        memory.ledger.snapshot().peak_bytes,
        initial + 2 * entry_bytes + raw_bytes
    );
    set.task.cancellation().cancel();
    assert!(set
        .contains("a", &mut SearchOutOfCoreMetrics::default())
        .is_err());
    drop(set);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "vector-search")]
fn vector_allowlist_retains_its_budget_after_spill_drop() {
    let root = test_dir("candidate-vector-owner");
    let memory = memory(8192);
    let set = spilled(
        &root,
        &[
            vec![("a", Some(0)), ("b", None)],
            vec![("c", Some(1)), ("d", Some(2))],
        ],
        &memory,
    );
    let ordinals = set
        .vector_ordinals(8192, None, &mut SearchOutOfCoreMetrics::default())
        .unwrap();
    assert_eq!(ordinals.as_slice(), &[0, 1, 2]);
    let bytes = ordinals.capacity() * 8;
    drop(set);
    assert_eq!(memory.ledger.snapshot().used_bytes, bytes);
    assert!(fs::read_dir(&root).unwrap().next().is_none());
    drop(ordinals);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn candidate_codecs_reject_count_corruption_before_allocating() {
    let memory = memory(8192);
    let task = RuntimeTaskContext::default();
    let bytes = wire(&[("a\0雪", Some(0)), ("b", None)]);
    candidate_codec::evidence::take();
    for prefix in 0..bytes.len() {
        assert!(candidate_codec::entries(&bytes[..prefix], 2, &memory.working, &task).is_err());
    }
    assert!(candidate_codec::entries(&bytes, usize::MAX, &memory.working, &task).is_err());
    assert!(candidate_codec::entries(
        &wire(&[("b", None), ("a", None)]),
        2,
        &memory.working,
        &task
    )
    .is_err());
    assert_eq!(candidate_codec::evidence::take(), 0);
    let required = 2 * std::mem::size_of::<CandidateEntry>() + "a\0雪".len() + 1;
    let bounded = super::candidate_admission::memory(required - 1);
    assert!(candidate_codec::entries(&bytes, 2, &bounded.working, &task).is_err());
    assert_eq!(candidate_codec::evidence::take(), 0);
    let decoded = candidate_codec::entries(&bytes, 2, &memory.working, &task).unwrap();
    assert_eq!(decoded[0].id, "a\0雪");
    assert_eq!(memory.ledger.snapshot().used_bytes, required);
    drop(decoded);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn candidate_encoding_preflights_block_and_spill_limits() {
    let task = RuntimeTaskContext::default();
    let memory = memory(8192);
    let docs = vec![
        SearchMetadataDocument {
            document: document(0, "team"),
            vector_ordinal: Some(0),
        },
        SearchMetadataDocument {
            document: document(1, "private"),
            vector_ordinal: None,
        },
    ];
    let predicates = search_metadata_predicate_pushdown(&BTreeMap::from([(
        "space_id".to_owned(),
        "team".to_owned(),
    )]));
    let expected = wire(&[("memory:000", Some(0))]);
    candidate_memory::evidence::take();
    for (block, spill) in [
        (expected.len() as u64 - 1, 8192),
        (8192, expected.len() as u64 - 1),
    ] {
        assert!(candidate_memory::encode(
            &docs,
            &predicates.predicates,
            block,
            spill,
            &memory.working,
            &task
        )
        .is_err());
        assert_eq!(candidate_memory::evidence::take(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    let (encoded, count) = candidate_memory::encode(
        &docs,
        &predicates.predicates,
        expected.len() as u64,
        expected.len() as u64,
        &memory.working,
        &task,
    )
    .unwrap();
    assert_eq!(count, 1);
    assert_eq!(encoded.as_slice(), expected);
    assert_eq!(memory.ledger.snapshot().used_bytes, expected.len());
    drop(encoded);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn metadata_decode_is_bounded_before_native_entry_and_matches_stream_reader() {
    let task = RuntimeTaskContext::default();
    for length in [0, 1, 131072, 2097153] {
        let body = "x".repeat(length);
        let payload = crate::encode_search_snapshot_text(&body).unwrap();
        let required = query_io::DECODE_WORKSPACE_BYTES + length;
        let memory = memory(required);
        query_io::evidence::take();
        let decoded = query_io::decode(&payload, length as u64, &memory.working, &task).unwrap();
        assert_eq!(
            **decoded,
            crate::decode_search_snapshot_text_bounded(&payload, length as u64).unwrap()
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, length);
        drop(decoded);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        query_io::evidence::take();
        let short = super::candidate_admission::memory(required - 1);
        assert!(query_io::decode(&payload, length as u64, &short.working, &task).is_err());
        assert_eq!(query_io::evidence::take().1, 0);
    }
    assert!(query_io::metadata_bytes(
        "SKEIN_SEARCH_METADATA_SEGMENT_V1\nmeta\t61\t-\t\n",
        usize::MAX,
        &task
    )
    .is_err());
    assert!(query_io::add(usize::MAX, 1).is_err());
    assert!(query_io::mul(usize::MAX, 2).is_err());
}

#[derive(Debug, Default)]
struct Controller {
    active: Arc<AtomicUsize>,
    calls: AtomicUsize,
    cancel: bool,
    reject: bool,
}
#[derive(Debug)]
struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl RuntimeIoWaveController for Controller {
    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        self.acquire(slots, context).map(Some)
    }
    fn acquire(
        &self,
        slots: NonZeroUsize,
        context: &RuntimeTaskContext,
    ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        assert_eq!(slots.get(), 1);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject {
            return Err(RuntimeIoWaveError::ReservationExceeded {
                requested_slots: 1,
                reserved_slots: 0,
            });
        }
        self.active.fetch_add(1, Ordering::SeqCst);
        if self.cancel {
            context.cancellation().cancel();
        }
        Ok(Box::new(Permit(self.active.clone())))
    }
}

#[test]
fn io_rejection_and_cancellation_remove_only_the_failed_query_spill() {
    let (root, reader) = fixture("candidate-io-cancel");
    for cancel in [false, true] {
        let controller = Arc::new(Controller {
            cancel,
            reject: !cancel,
            ..Controller::default()
        });
        let task = RuntimeTaskContext::default().with_io_wave_controller(controller.clone());
        let memory = reader.lexical_projection.query_memory(Some(&task)).unwrap();
        query_io::evidence::take();
        let error = candidates(&reader, &memory, Some(&task)).err().unwrap();
        assert!(error
            .to_string()
            .contains(if cancel { "cancelled" } else { "I/O admission" }));
        assert_eq!(controller.calls.load(Ordering::SeqCst), 1);
        assert_eq!(controller.active.load(Ordering::SeqCst), 0);
        assert_eq!(query_io::evidence::take(), (0, 0));
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert!(fs::read_dir(root.join("spill")).unwrap().next().is_none());
    }
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn create_new_collision_cannot_delete_an_existing_candidate_file() {
    let root = test_dir("candidate-collision");
    let path = root.join("existing");
    fs::write(&path, b"existing owner").unwrap();
    assert!(CandidateFileGuard::create_new(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"existing owner");
    fs::remove_dir_all(root).unwrap();
}

#[test]
#[cfg(feature = "full-text-search")]
fn public_query_rejection_cleans_candidates_under_its_task_budget() {
    let (root, reader) = fixture("candidate-public-budget");
    let mut options = options(3, None);
    options
        .metadata_filters
        .insert("space_id".to_owned(), "team".to_owned());
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(1024, 1024));
    assert!(reader
        .search_with_options_compressed_vector_projection_context(
            "graph",
            None,
            SearchMode::Text,
            options.clone(),
            CompressedVectorSearchMode::Disabled,
            &task
        )
        .is_err());
    assert!(fs::read_dir(root.join("spill")).unwrap().next().is_none());
    let output = reader
        .search_with_options("graph", None, SearchMode::Text, options)
        .unwrap();
    assert_eq!(output.result.total_hits, 5);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}
