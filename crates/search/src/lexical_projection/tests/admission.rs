use super::*;
use skein_core::{
    RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit, RuntimeMemoryReservation,
    RuntimeTaskContext,
};
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Default)]
struct IoController {
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    reject: bool,
    cancel_on_acquire: bool,
}

#[derive(Debug)]
struct IoPermit(Arc<AtomicUsize>);

impl Drop for IoPermit {
    fn drop(&mut self) {
        assert_eq!(self.0.fetch_sub(1, Ordering::SeqCst), 1);
    }
}

impl RuntimeIoWaveController for IoController {
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
        assert_eq!(slots, NonZeroUsize::MIN);
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.reject {
            return Err(RuntimeIoWaveError::ReservationExceeded {
                requested_slots: 1,
                reserved_slots: 0,
            });
        }
        assert_eq!(self.active.fetch_add(1, Ordering::SeqCst), 0);
        if self.cancel_on_acquire {
            context.cancellation().cancel();
        }
        Ok(Box::new(IoPermit(self.active.clone())))
    }
}

fn build(root: &Path) -> Arc<LexicalProjectionReader> {
    fs::create_dir_all(root).unwrap();
    let documents = (0..257)
        .map(|index| document(&format!("memory:{index:08}"), "", "graph"))
        .collect::<Vec<_>>();
    LexicalProjectionWriter::new(LexicalProjectionConfig::default())
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
fn lexical_task_memory_is_checked_before_dictionary_io() {
    let root = projection_root("compact-task-memory");
    let reader = build(&root);
    let context =
        RuntimeTaskContext::default().with_memory_reservation(RuntimeMemoryReservation::new(1, 1));
    let before = reader.cache.snapshot();
    let error = reader
        .score_with_context(
            &BTreeSet::from(["graph".to_string()]),
            &LexicalMiniDelta::default(),
            Some(1),
            Some(&context),
            |_| panic!("unadmitted query must not visit candidates"),
        )
        .unwrap_err();
    assert!(error.to_string().contains("streams require"));
    assert_eq!(reader.cache.snapshot(), before);
    let context = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(128 * 1024 * 1024, 0));
    let error = reader
        .score_with_context(
            &BTreeSet::from(["graph".to_string()]),
            &LexicalMiniDelta::default(),
            Some(1),
            Some(&context),
            |_| panic!("unadmitted result must not visit candidates"),
        )
        .unwrap_err();
    assert!(error.to_string().contains("score byte budget"));
    assert_eq!(reader.cache.snapshot(), before);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn lexical_cancellation_stops_the_active_posting_loop_and_releases_pins() {
    let root = projection_root("compact-task-cancellation");
    let reader = build(&root);
    let controller = Arc::new(IoController::default());
    let context = RuntimeTaskContext::default().with_io_wave_controller(controller.clone());
    let mut visited = 0;
    let error = reader
        .score_with_context(
            &BTreeSet::from(["graph".to_string()]),
            &LexicalMiniDelta::default(),
            Some(1),
            Some(&context),
            |_| {
                visited += 1;
                context.cancellation().cancel();
                Ok(true)
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    assert_eq!(visited, 1);
    assert!(controller.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(controller.active.load(Ordering::SeqCst), 0);
    assert_eq!(reader.cache.snapshot().pinned_bytes, 0);
    let before = reader.cache.snapshot();
    assert!(reader
        .score_with_context(
            &BTreeSet::from(["graph".to_string()]),
            &LexicalMiniDelta::default(),
            Some(1),
            Some(&context),
            |_| panic!("cancelled query resumed")
        )
        .is_err());
    assert_eq!(reader.cache.snapshot(), before);
    assert_eq!(
        reader
            .score(
                &BTreeSet::from(["graph".to_string()]),
                &LexicalMiniDelta::default(),
                Some(1),
                |_| Ok(true)
            )
            .unwrap()
            .matching_document_count,
        257
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn lexical_io_admission_and_cache_hits_have_distinct_permit_lifetimes() {
    let root = projection_root("compact-io-admission");
    let reader = build(&root);
    let controller = Arc::new(IoController {
        reject: true,
        ..IoController::default()
    });
    let context = RuntimeTaskContext::default().with_io_wave_controller(controller.clone());
    let read = ReadContext {
        projection: &reader,
        task: Some(&context),
    };
    assert!(read
        .term_metadata("graph", &mut 0)
        .unwrap_err()
        .to_string()
        .contains("I/O admission"));
    assert_eq!(controller.calls.load(Ordering::SeqCst), 1);
    assert_eq!(reader.cache.snapshot().resident_bytes, 0);
    reader.term_metadata("graph", &mut 0).unwrap();
    assert_eq!(
        read.term_metadata("graph", &mut 0).unwrap().unwrap().df,
        257
    );
    assert_eq!(controller.calls.load(Ordering::SeqCst), 1);

    let cancelled = Arc::new(IoController {
        cancel_on_acquire: true,
        ..IoController::default()
    });
    let context = RuntimeTaskContext::default().with_io_wave_controller(cancelled.clone());
    let read = ReadContext {
        projection: &reader,
        task: Some(&context),
    };
    assert!(read
        .read_range(reader.manifest.posting_offset, 12)
        .unwrap_err()
        .to_string()
        .contains("cancelled"));
    assert_eq!(cancelled.active.load(Ordering::SeqCst), 0);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn score_collectors_charge_owned_id_capacity_and_reject_without_replacing_results() {
    let memory =
        crate::query_memory::QueryMemory::new(NonZeroU64::new(4096).unwrap(), None).unwrap();
    let mut full = ScoreCollector::new(None, 10, 514, &memory.scores).unwrap();
    full.push("a".to_string(), 1.0).unwrap();
    full.push("b".to_string(), 2.0).unwrap();
    assert!(full
        .push("c".to_string(), 3.0)
        .unwrap_err()
        .to_string()
        .contains("score byte budget"));
    assert_eq!(full.finish().len(), 2);
    let mut top = ScoreCollector::new(Some(1), 10, 300, &memory.scores).unwrap();
    top.push("a".to_string(), 1.0).unwrap();
    let mut oversized = String::with_capacity(1024);
    oversized.push('b');
    assert!(top.push(oversized, 2.0).is_err());
    assert_eq!(top.finish(), BTreeMap::from([("a".to_string(), 1.0)]));
    assert!(ScoreCollector::new(Some(1000), 1000, 1, &memory.scores).is_err());
}
