use super::*;
use std::cell::Cell;
use std::io;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

mod fuzz;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "skein-spill-admission-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }

    fn assert_empty(&self) {
        let remaining = fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert!(remaining.is_empty(), "unowned staging files: {remaining:?}");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug, Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    CreateAfterFile,
    WriteAfter(usize),
    ShortWrites,
    Interrupted,
    Flush,
    Remove,
    RemoveAfter(usize),
    PanicRemoveAfter(usize),
    PanicAfter(usize),
}

#[derive(Default)]
struct ObservedIo {
    created: usize,
    removed: usize,
    written: Rc<Cell<usize>>,
    fired: Rc<Cell<bool>>,
    fault: Fault,
}

struct ObservedWriter {
    file: File,
    written: Rc<Cell<usize>>,
    fired: Rc<Cell<bool>>,
    fault: Fault,
    local_bytes: usize,
}

impl Write for ObservedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if matches!(self.fault, Fault::Interrupted) && !self.fired.replace(true) {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        let allowed = match self.fault {
            Fault::ShortWrites => bytes.len().min(2),
            Fault::WriteAfter(limit) | Fault::PanicAfter(limit) => {
                if self.local_bytes >= limit {
                    self.fired.set(true);
                    if matches!(self.fault, Fault::PanicAfter(_)) {
                        panic!("injected spill writer panic");
                    }
                    return Err(io::Error::other("injected spill write failure"));
                }
                bytes.len().min(limit - self.local_bytes)
            }
            _ => bytes.len(),
        };
        let count = self.file.write(&bytes[..allowed])?;
        self.local_bytes += count;
        self.written.set(self.written.get() + count);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if matches!(self.fault, Fault::Flush) {
            self.fired.set(true);
            return Err(io::Error::other("injected spill flush failure"));
        }
        self.file.flush()
    }
}

impl SpillIo for ObservedIo {
    type Writer = ObservedWriter;

    fn create(&mut self, path: &Path) -> Result<Self::Writer> {
        self.created += 1;
        let file = File::create(path)?;
        if matches!(self.fault, Fault::CreateAfterFile) {
            self.fired.set(true);
            return Err(SkeinError::Storage("injected spill create failure".into()));
        }
        Ok(ObservedWriter {
            file,
            written: self.written.clone(),
            fired: self.fired.clone(),
            fault: self.fault,
            local_bytes: 0,
        })
    }

    fn remove(&mut self, path: &Path) -> Result<()> {
        let fail = match self.fault {
            Fault::Remove => true,
            Fault::RemoveAfter(limit) | Fault::PanicRemoveAfter(limit) => self.removed >= limit,
            _ => false,
        };
        if fail {
            self.fired.set(true);
            if matches!(self.fault, Fault::PanicRemoveAfter(_)) {
                panic!("injected spill unlink panic");
            }
            return Err(SkeinError::Storage("injected spill unlink failure".into()));
        }
        fs::remove_file(path)?;
        self.removed += 1;
        Ok(())
    }
}

fn postings() -> Vec<Posting> {
    ["beta", "alpha", "beta", "gamma"]
        .into_iter()
        .map(|term| Posting {
            term: term.into(),
            document_id: "document".into(),
            term_frequency: 2,
            document_len: 6,
        })
        .collect()
}

// Independent wire oracle, including the existing exact-Posting deduplication.
fn reference_run(mut postings: Vec<Posting>) -> Vec<u8> {
    postings.sort();
    postings.dedup();
    let mut output = b"SKNLEXR1".to_vec();
    for posting in postings {
        for text in [posting.term.as_str(), posting.document_id.as_str()] {
            output.extend_from_slice(&(text.len() as u32).to_le_bytes());
            output.extend_from_slice(text.as_bytes());
        }
        output.extend_from_slice(&posting.term_frequency.to_le_bytes());
        output.extend_from_slice(&posting.document_len.to_le_bytes());
    }
    output
}

fn runs_with_three_inputs(fixture: &Fixture, io: &mut ObservedIo) -> SpillRuns {
    let mut runs = SpillRuns::new(
        &fixture.0,
        1,
        LexicalProjectionConfig {
            max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
            ..Default::default()
        },
    );
    for _ in 0..3 {
        runs.spill_with_io(&mut postings(), io).unwrap();
    }
    runs
}

#[test]
fn spill_rejects_budget_before_creating_output() {
    let fixture = Fixture::new();
    let bytes = reference_run(postings()).len() as u64;
    let mut runs = SpillRuns::new(
        &fixture.0,
        1,
        LexicalProjectionConfig {
            max_spill_bytes: NonZeroU64::new(bytes - 1).unwrap(),
            ..Default::default()
        },
    );
    let mut io = ObservedIo::default();
    let error = runs.spill_with_io(&mut postings(), &mut io).unwrap_err();
    assert!(error.to_string().contains("spill bytes"));
    assert_eq!(io.created, 0, "reject before opening or truncating a run");
    assert_eq!(io.written.get(), 0);
    drop(runs);
    fixture.assert_empty();
}

#[test]
fn spill_write_and_flush_errors_remove_partial_output() {
    for fault in [
        Fault::CreateAfterFile,
        Fault::WriteAfter(0),
        Fault::WriteAfter(15),
        Fault::Flush,
    ] {
        let fixture = Fixture::new();
        let mut runs = SpillRuns::new(&fixture.0, 1, Default::default());
        let mut io = ObservedIo {
            fault,
            ..Default::default()
        };
        let error = runs.spill_with_io(&mut postings(), &mut io).unwrap_err();
        assert!(error.to_string().contains("injected spill"));
        assert!(io.fired.get());
        fixture.assert_empty();
        drop(runs);
        fixture.assert_empty();
    }
}

#[test]
fn compaction_checks_remaining_budget_before_each_output_record() {
    let fixture = Fixture::new();
    let mut io = ObservedIo::default();
    let mut runs = runs_with_three_inputs(&fixture, &mut io);
    let remaining = 12;
    runs.config.max_spill_bytes = NonZeroU64::new(runs.bytes + remaining).unwrap();
    let before = io.written.get();
    let error = runs.compact_with_io(&mut io).unwrap_err();
    assert!(error.to_string().contains("spill bytes"));
    assert!(io.written.get() - before <= remaining as usize);
    drop(runs);
    fixture.assert_empty();
}

#[test]
fn spill_unwind_closes_and_removes_partial_output() {
    for limit in [0, 7, 15] {
        let fixture = Fixture::new();
        let runs = SpillRuns::new(&fixture.0, 1, Default::default());
        let mut io = ObservedIo {
            fault: Fault::PanicAfter(limit),
            ..Default::default()
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut runs = runs;
            runs.spill_with_io(&mut postings(), &mut io).unwrap();
        }));
        assert!(outcome.is_err());
        assert!(io.fired.get());
        fixture.assert_empty();
    }
}

#[test]
fn compaction_unlink_error_retains_ownership_until_drop() {
    let fixture = Fixture::new();
    let mut io = ObservedIo::default();
    let mut runs = runs_with_three_inputs(&fixture, &mut io);
    io.fault = Fault::Remove;
    let error = runs.compact_with_io(&mut io).unwrap_err();
    assert!(error.to_string().contains("unlink failure"));
    assert!(io.fired.get());
    drop(runs);
    fixture.assert_empty();
}

#[test]
fn compaction_unwind_removes_sources_and_partial_destination() {
    let fixture = Fixture::new();
    let mut io = ObservedIo::default();
    let runs = runs_with_three_inputs(&fixture, &mut io);
    io.fault = Fault::PanicAfter(15);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut owned = runs;
        owned.compact_with_io(&mut io).unwrap();
    }));
    assert!(result.is_err());
    assert!(io.fired.get());
    fixture.assert_empty();
}

#[test]
fn spill_wire_and_successful_compaction_match_the_legacy_oracle() {
    let fixture = Fixture::new();
    let mut io = ObservedIo::default();
    let mut runs = runs_with_three_inputs(&fixture, &mut io);
    let expected = reference_run(postings());
    for path in &runs.paths {
        assert_eq!(fs::read(path).unwrap(), expected);
    }
    runs.compact_with_io(&mut io).unwrap();
    assert_eq!(runs.paths.len(), 2);
    for path in &runs.paths {
        assert_eq!(fs::read(path).unwrap(), expected);
    }
    assert_eq!(runs.bytes, (expected.len() * 5) as u64);
    drop(runs);
    fixture.assert_empty();
}

#[test]
fn spill_exact_cumulative_budget_and_rejected_run_leave_counters_unchanged() {
    for fault in [Fault::None, Fault::ShortWrites, Fault::Interrupted] {
        let fixture = Fixture::new();
        let expected = reference_run(postings());
        let mut runs = SpillRuns::new(
            &fixture.0,
            1,
            LexicalProjectionConfig {
                max_spill_bytes: NonZeroU64::new((2 * expected.len()) as u64).unwrap(),
                ..Default::default()
            },
        );
        let mut io = ObservedIo {
            fault,
            ..Default::default()
        };
        for _ in 0..2 {
            let mut input = postings();
            runs.spill_with_io(&mut input, &mut io).unwrap();
            assert!(input.is_empty());
        }
        assert_eq!(runs.bytes, (2 * expected.len()) as u64);
        for path in &runs.paths {
            assert_eq!(fs::read(path).unwrap(), expected);
        }
        let mut rejected = postings();
        assert!(runs.spill_with_io(&mut rejected, &mut io).is_err());
        assert!(!rejected.is_empty());
        assert_eq!(runs.sequence, 2);
        assert_eq!(runs.bytes, (2 * expected.len()) as u64);
        assert_eq!(io.created, 2);
        assert_eq!(io.written.get(), 2 * expected.len());
        drop(runs);
        fixture.assert_empty();
    }
}

#[test]
fn spill_header_and_checked_arithmetic_boundaries_reject_without_output() {
    for (previous, limit, input) in [
        (0, 7, Vec::new()),
        (u64::MAX - 7, u64::MAX, Vec::new()),
        (u64::MAX - 8, u64::MAX, postings()),
    ] {
        let fixture = Fixture::new();
        let mut runs = SpillRuns::new(
            &fixture.0,
            1,
            LexicalProjectionConfig {
                max_spill_bytes: NonZeroU64::new(limit).unwrap(),
                ..Default::default()
            },
        );
        runs.bytes = previous;
        let mut io = ObservedIo::default();
        assert!(runs.spill_with_io(&mut input.clone(), &mut io).is_err());
        assert_eq!((runs.bytes, runs.sequence, io.created), (previous, 0, 0));
        fixture.assert_empty();
    }
    assert_eq!(
        checked_spill_bytes(u64::MAX - 8, 8, NonZeroU64::MAX).unwrap(),
        u64::MAX
    );
    let fixture = Fixture::new();
    let mut runs = SpillRuns::new(&fixture.0, 1, Default::default());
    runs.sequence = usize::MAX;
    runs.config.max_spill_runs = NonZeroUsize::MAX;
    let mut io = ObservedIo::default();
    assert!(runs
        .spill_with_io(&mut postings(), &mut io)
        .unwrap_err()
        .to_string()
        .contains("sequence overflow"));
    assert_eq!(io.created, 0);
    fixture.assert_empty();
}

// The oracle sums literal wire units rather than production size helpers.
#[expect(
    clippy::mutable_key_type,
    reason = "Posting order depends only on immutable values, never on the term lease."
)]
fn reference_units(input: &[Posting]) -> Vec<usize> {
    let unique = input.iter().collect::<BTreeSet<_>>();
    std::iter::once(8)
        .chain(
            unique
                .into_iter()
                .map(|posting| 16 + posting.term.len() + posting.document_id.len()),
        )
        .collect()
}

fn admitted_prefix(units: impl IntoIterator<Item = usize>, budget: usize) -> usize {
    let mut written = 0;
    for unit in units {
        if unit > budget - written {
            break;
        }
        written += unit;
    }
    written
}

#[test]
fn compaction_all_byte_boundaries_preserve_deduplicated_exact_admission() {
    let units = reference_units(&postings());
    let run_bytes = reference_run(postings()).len();
    for remaining in 0..=2 * run_bytes {
        let fixture = Fixture::new();
        let mut io = ObservedIo::default();
        let mut runs = runs_with_three_inputs(&fixture, &mut io);
        let before = io.written.get();
        runs.config.max_spill_bytes = NonZeroU64::new(runs.bytes + remaining as u64).unwrap();
        let result = runs.compact_with_io(&mut io);
        assert_eq!(
            result.is_ok(),
            remaining == 2 * run_bytes,
            "budget {remaining}"
        );
        assert_eq!(
            io.written.get() - before,
            admitted_prefix(units.iter().chain(&units).copied(), remaining),
            "budget {remaining}"
        );
        if result.is_ok() {
            assert_eq!(runs.bytes, (5 * run_bytes) as u64);
            assert_eq!(runs.paths.len(), 2);
        }
        drop(runs);
        fixture.assert_empty();
    }
}

#[test]
fn compaction_io_errors_and_panics_clean_all_owned_files() {
    for fault in [
        Fault::CreateAfterFile,
        Fault::WriteAfter(0),
        Fault::WriteAfter(15),
        Fault::Flush,
        Fault::RemoveAfter(1),
        Fault::RemoveAfter(2),
        Fault::PanicRemoveAfter(1),
        Fault::PanicRemoveAfter(2),
        Fault::PanicAfter(0),
    ] {
        let fixture = Fixture::new();
        let mut io = ObservedIo::default();
        let runs = runs_with_three_inputs(&fixture, &mut io);
        io.fault = fault;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut runs = runs;
            let result = runs.compact_with_io(&mut io);
            drop(runs);
            result
        }));
        match fault {
            Fault::PanicAfter(_) | Fault::PanicRemoveAfter(_) => assert!(outcome.is_err()),
            _ => assert!(outcome.unwrap().is_err()),
        }
        assert!(io.fired.get(), "fault did not fire: {fault:?}");
        fixture.assert_empty();
    }
}

#[test]
fn compaction_read_errors_preserve_cleanup_after_completed_groups() {
    for length in [0, 7, 9, reference_run(postings()).len() - 1] {
        let fixture = Fixture::new();
        let mut io = ObservedIo::default();
        let mut runs = runs_with_three_inputs(&fixture, &mut io);
        // The third input is read only after the first group was merged and
        // unlinked. Both completed and partial outputs must remain owned.
        let input = File::options().write(true).open(&runs.paths[2]).unwrap();
        input.set_len(length as u64).unwrap();
        drop(input);
        assert!(runs.compact_with_io(&mut io).is_err(), "prefix {length}");
        assert_eq!(io.removed, 2);
        drop(runs);
        fixture.assert_empty();
    }
}

#[test]
fn governed_compaction_faults_and_corruption_release_all_paths_and_capacity() {
    use skein_core::RuntimeMemoryReservation;
    let populate = |fixture: &Fixture, memory: &BuildMemory, task: &RuntimeTaskContext| {
        let mut pool = SpillRuns::with_context(
            &fixture.0,
            1,
            LexicalProjectionConfig {
                max_merge_fan_in: NonZeroUsize::new(2).unwrap(),
                ..Default::default()
            },
            memory.clone(),
            task.clone(),
        )
        .unwrap();
        for _ in 0..3 {
            pool.spill(&mut postings()).unwrap();
        }
        pool
    };
    for fault in [
        Fault::CreateAfterFile,
        Fault::WriteAfter(15),
        Fault::Flush,
        Fault::RemoveAfter(1),
        Fault::RemoveAfter(2),
        Fault::PanicRemoveAfter(1),
        Fault::PanicAfter(0),
    ] {
        let fixture = Fixture::new();
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(1024 * 1024, 0));
        let memory = BuildMemory::new(&task).unwrap();
        let pool = populate(&fixture, &memory, &task);
        let before = memory.ledger.snapshot();
        let competitor = memory
            .input
            .reserve(before.budget_bytes - before.used_bytes)
            .unwrap();
        let mut io = ObservedIo {
            fault,
            ..Default::default()
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut pool = pool;
            pool.compact_with_io(&mut io)
        }));
        match fault {
            Fault::PanicAfter(_) | Fault::PanicRemoveAfter(_) => assert!(outcome.is_err()),
            _ => assert!(outcome.unwrap().is_err()),
        }
        assert!(io.fired.get(), "fault did not fire: {fault:?}");
        fixture.assert_empty();
        assert_eq!(memory.ledger.snapshot().used_bytes, competitor.bytes());
        drop(competitor);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    for length in [0, 7, 9, reference_run(postings()).len() - 1] {
        let fixture = Fixture::new();
        let task = RuntimeTaskContext::default();
        let memory = BuildMemory::new(&task).unwrap();
        let mut pool = populate(&fixture, &memory, &task);
        File::options()
            .write(true)
            .open(&pool.paths[2])
            .unwrap()
            .set_len(length as u64)
            .unwrap();
        assert!(pool.compact().is_err());
        drop(pool);
        fixture.assert_empty();
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}
