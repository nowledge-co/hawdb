use super::*;
use hawdb_qos::{
    LocalQosPolicy, QosTelemetryEvent, QosTelemetryOutcome, QosTelemetryPhase, QosTelemetrySink,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Units {
    admitted: AtomicUsize,
    denied: AtomicUsize,
    oversized: AtomicUsize,
}

impl QosTelemetrySink for Units {
    fn record_qos(&self, event: QosTelemetryEvent) {
        if event.phase != QosTelemetryPhase::Admission {
            return;
        }
        if event.estimated_operations != 1 {
            self.oversized.fetch_add(1, Ordering::SeqCst);
        }
        if event.outcome == QosTelemetryOutcome::Admitted {
            self.admitted.fetch_add(1, Ordering::SeqCst);
        } else {
            self.denied.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn actual_owner_prepares_and_replays_with_one_local_operation_available() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let properties = |id| {
        std::collections::BTreeMap::from([
            ("id".into(), crate::Value::Int(id)),
            ("body".into(), crate::Value::String("p".repeat(512))),
        ])
    };
    for id in 0..32 {
        store
            .create_node(&mut catalog, "Memory", properties(id))
            .unwrap();
    }
    let control = Arc::new(Control::default());
    let (sealed, observed) = std::sync::mpsc::channel();
    let (resume, continuation) = std::sync::mpsc::channel();
    control.lock().unwrap().prefix_seal_probe = Some(Arc::new(PrefixSealProbe {
        sealed,
        resume: Mutex::new(continuation),
    }));
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    });
    let units = Arc::new(Units::default());
    scheduler.set_telemetry_sink(Some(units.clone()));
    let resources = governor(512 * 1024 * 1024);
    let mut owner = Owner::start(
        control.clone(),
        &store,
        &catalog,
        Arc::new(Mutex::new(ReaderPins::default())),
        &DatabaseConfig {
            automatic_checkpoint_max_age: Duration::from_millis(20),
            ..DatabaseConfig::default()
        },
        scheduler.clone(),
        Some(resources.clone()),
    )
    .unwrap()
    .unwrap();
    assert_eq!(observed.recv_timeout(Duration::from_secs(15)).unwrap(), 32);
    let base = fixture.0.join("checkpoint.1.hawdb");
    let base_bytes = std::fs::read(&base).unwrap();
    {
        let mut state = control.lock_frontend().unwrap();
        store
            .create_node(&mut catalog, "Memory", properties(32))
            .unwrap();
        let retired = control.submit(&mut state, &store, &catalog);
        state.prefix_seal_probe = None;
        drop(state);
        drop(retired);
    }
    resume.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let mut state = control.lock_frontend().unwrap();
        if state.selected.is_some() {
            control.adopt(&mut state, &mut store).unwrap();
            break;
        }
        assert!(
            Instant::now() < deadline,
            "unit owner did not publish: {:?}",
            state.report
        );
        drop(state);
        std::thread::sleep(Duration::from_millis(5));
    }
    owner.stop();
    assert_eq!(std::fs::read(&base).unwrap(), base_bytes);
    assert_eq!(scheduler.state().running_background_operations, 0);
    assert!(units.admitted.load(Ordering::SeqCst) > 32);
    assert_eq!(units.denied.load(Ordering::SeqCst), 0);
    assert_eq!(units.oversized.load(Ordering::SeqCst), 0);
    assert_eq!(control.lock().unwrap().report.failed_attempts, 0);
    assert_eq!(resources.snapshot().active_background_tasks, 0);
    assert_eq!(resources.snapshot().active_background_io_slots, 0);
    for id in 0..33 {
        assert_eq!(
            store
                .node_owned(hawdb_storage::NodeId(id))
                .unwrap()
                .unwrap()
                .properties,
            properties(id as i64)
        );
    }
    drop(owner);
    drop(store);
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 33);
    for id in 0..33 {
        assert_eq!(
            recovered
                .node_owned(hawdb_storage::NodeId(id))
                .unwrap()
                .unwrap()
                .properties,
            properties(id as i64)
        );
    }
}
