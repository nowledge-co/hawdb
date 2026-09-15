use super::*;
use skein_core::{RuntimeCancellationToken, RuntimeMemoryReservation};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::test_allocation as allocation;

const BUDGET: usize = 2 * 1024 * 1024;

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "skein-spill-ownership-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn context() -> (BuildMemory, RuntimeTaskContext) {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(BUDGET as u64, 0));
    (BuildMemory::new(&task).unwrap(), task)
}

fn fill(memory: &BuildMemory) -> QueryMemoryLease {
    memory
        .input
        .reserve(BUDGET - memory.ledger.snapshot().used_bytes)
        .unwrap()
}

fn populate(pool: &mut SpillRuns, memory: &BuildMemory, runs: usize) {
    let mut buffer = PendingPostings::new(Some(memory)).unwrap();
    for _ in 0..runs {
        for text in ["alpha", "beta", "gamma"] {
            pool.prepare(text.len(), 8).unwrap();
            buffer
                .push(Term::copy(text, Some(memory)).unwrap(), "document", 2, 6)
                .unwrap();
        }
        buffer.flush(pool).unwrap();
    }
}

#[test]
fn admitted_corpus_merge_levels_progress_at_a_full_root_and_cover_live_allocations() {
    let _serial = allocation::serial();
    // The serialization guard spans all measured owners. Other test threads
    // remain untracked, including their eventual deallocations.
    for fan_in in [2, 4, 32] {
        let fixture = Fixture::new();
        let (memory, task) = context();
        let config = LexicalProjectionConfig {
            max_merge_fan_in: NonZeroUsize::new(fan_in).unwrap(),
            max_term_bytes: NonZeroU64::new(1024 * 1024).unwrap(),
            ..Default::default()
        };
        let mut pool =
            SpillRuns::with_context(&fixture.0, 1, config, memory.clone(), task.clone()).unwrap();
        populate(&mut pool, &memory, 17);
        let pool_bytes = memory.ledger.snapshot().used_bytes;
        assert!(
            pool_bytes < 1024 * 1024,
            "actual short records must not reserve policy-sized heads"
        );
        let input = memory
            .admit_document(SearchDocument {
                id: "live-input".into(),
                title: "retained source".into(),
                content: "payload ".repeat(4096),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let competitor = fill(&memory);
        let mut retained = None;
        let ((result, count), peak) = allocation::measure(|| {
            let result = pool.compact();
            let mut count = 0;
            let result = result.and_then(|()| {
                visit_merged_postings_with_progress(
                    &pool.paths,
                    config,
                    pool.progress.as_ref(),
                    Some(&task),
                    |posting| {
                        assert_eq!(posting.term.as_str(), ["alpha", "beta", "gamma"][count]);
                        assert_eq!(posting.document_id, "document");
                        assert_eq!((posting.term_frequency, posting.document_len), (2, 6));
                        retained = Some(posting.term.clone());
                        count += 1;
                        Ok(())
                    },
                )
            });
            (result, count)
        });
        result.unwrap();
        assert_eq!(count, 3);
        assert!(
            peak <= pool_bytes,
            "{peak} live requested bytes exceed {pool_bytes} reserved bytes"
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, BUDGET);
        assert!(pool.paths.len() <= fan_in);
        drop((pool, input, competitor));
        assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            pool_bytes - fixture.0.as_os_str().len()
        );
        assert_eq!(allocation::live(), Term::reserved_bytes(5).unwrap());
        eprintln!(
            "spill fan_in={fan_in} reserved={pool_bytes} measured_peak={peak} retained={}",
            allocation::live()
        );
        drop(retained);
        assert_eq!(allocation::live(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }

    // Check the control block itself, not just the string's deallocation. The
    // final payload can be released on either of two concurrent threads.
    struct Observe(QueryMemoryLease);
    impl Drop for Observe {
        fn drop(&mut self) {
            assert_eq!(
                allocation::live(),
                0,
                "control block must die before its lease"
            );
            assert!(self.0.bytes() > 0);
        }
    }
    struct Payload {
        _text: String,
        _memory: Observe,
    }
    let (memory, _) = context();
    let lease = memory
        .retained
        .reserve(4096 + size_of::<Payload>() + 2 * size_of::<usize>())
        .unwrap();
    let ((left, right), _) = allocation::measure(|| {
        let left = crate::build_memory::shared::Shared::new(Payload {
            _text: "x".repeat(4096),
            _memory: Observe(lease),
        });
        let right = left.clone();
        (left, right)
    });
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            drop(left);
        });
        scope.spawn(|| {
            barrier.wait();
            drop(right);
        });
    });
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);

    let fixture = Fixture::new();
    let mut directory = fixture.0.clone();
    for _ in 0..5 {
        directory.push("native_path_component".repeat(5));
    }
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("probe.tmp");
    let scratch = native_path::bytes(&path).unwrap();
    let ((result, remove), peak) = allocation::measure(|| {
        let result = File::create(&path);
        // Close before unlink on platforms that require it.
        let result = result.map(drop);
        (result, fs::remove_file(&path))
    });
    result.unwrap();
    remove.unwrap();
    assert!(
        peak > 0 && peak <= scratch,
        "native path peak {peak} exceeds {scratch}"
    );
    assert_eq!(allocation::live(), 0);
    eprintln!(
        "native path bytes={} measured_peak={peak} admitted_scratch={scratch}",
        path.as_os_str().len()
    );
}

#[test]
fn decoded_posting_admits_both_strings_and_reader_before_allocation() {
    let fixture = Fixture::new();
    let path = fixture.0.join("run");
    let mut bytes = RUN_HEADER.to_vec();
    let posting = Posting {
        term: "alpha".into(),
        document_id: "document".into(),
        term_frequency: 2,
        document_len: 2,
    };
    encode_posting(&mut bytes, &posting).unwrap();
    fs::write(&path, bytes).unwrap();
    let exact = SPILL_IO_BUFFER_BYTES + Term::reserved_bytes(5).unwrap() + 8;
    for short in [0, 1] {
        let (memory, task) = context();
        let progress = ReservedMemory::with_scratch_capacity(
            &memory.spool,
            exact - short,
            native_path::bytes(&path).unwrap(),
        )
        .unwrap();
        let competitor = fill(&memory);
        let mut reader =
            RunReader::open_with_progress(&path, Default::default(), Some(&progress), Some(&task))
                .unwrap();
        let result = reader.next(u64::MAX);
        assert_eq!(result.is_ok(), short == 0);
        if short == 0 {
            assert_eq!(result.as_ref().unwrap().as_ref().unwrap().posting, posting);
        }
        drop((result, reader, progress, competitor));
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }

    let (memory, task) = context();
    let progress = ReservedMemory::new(&memory.spool, size_of::<RunReader>() - 1).unwrap();
    let error = visit_merged_postings_with_progress(
        &[fixture.0.join("missing")],
        Default::default(),
        Some(&progress),
        Some(&task),
        |_| Ok(()),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("spill progress"),
        "reader registry must be admitted before file open: {error}"
    );
}

#[test]
fn posting_buffer_admits_slots_and_id_copy_with_exact_and_one_short_root_capacity() {
    for short in [0, 1] {
        let (memory, _) = context();
        let term = Term::copy("alpha", Some(&memory)).unwrap();
        let mut buffer = PendingPostings::new(Some(&memory)).unwrap();
        let slots = 4 * size_of::<Posting>();
        let competitor = memory
            .input
            .reserve(BUDGET - memory.ledger.snapshot().used_bytes - slots - 8 + short)
            .unwrap();
        let result = buffer.push(term, "document", 2, 2);
        assert_eq!(result.is_ok(), short == 0);
        assert_eq!(buffer.values.len(), usize::from(short == 0));
        assert_eq!(buffer.slots.as_ref().unwrap().bytes(), slots);
        assert_eq!(
            buffer.strings.as_ref().unwrap().bytes(),
            if short == 0 { 8 } else { 0 }
        );
        drop((buffer, competitor));
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn merge_cancellation_releases_readers_and_cleanup_keeps_its_admission() {
    let fixture = Fixture::new();
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone())
        .with_memory_reservation(RuntimeMemoryReservation::new(BUDGET as u64, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut pool = SpillRuns::with_context(
        &fixture.0,
        1,
        Default::default(),
        memory.clone(),
        task.clone(),
    )
    .unwrap();
    populate(&mut pool, &memory, 3);
    let before = memory.ledger.snapshot().used_bytes;
    let mut count = 0;
    let error = visit_merged_postings_with_progress(
        &pool.paths,
        pool.config,
        pool.progress.as_ref(),
        Some(&task),
        |_| {
            count += 1;
            token.cancel();
            Ok(())
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(count, 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, before);
    drop(pool);
    assert_eq!(fs::read_dir(&fixture.0).unwrap().count(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
