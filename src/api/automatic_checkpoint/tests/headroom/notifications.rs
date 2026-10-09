use super::*;

#[test]
fn clean_idle_owner_waits_for_an_event_instead_of_polling() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    let control = Arc::new(Control::default());
    let (waiting, observed) = mpsc::channel();
    control.lock().unwrap().owner_wait_probe = Some(waiting);
    let config = DatabaseConfig {
        automatic_checkpoint_max_age: Duration::from_secs(3600),
        ..DatabaseConfig::default()
    };
    let mut owner = Owner::start(
        control.clone(),
        &store,
        &catalog,
        Arc::new(Mutex::new(ReaderPins::default())),
        &config,
        LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default()),
        Some(governor(512 * 1024 * 1024)),
    )
    .unwrap()
    .unwrap();
    observed.recv_timeout(Duration::from_secs(15)).unwrap();
    let unsolicited_wake = observed.recv_timeout(Duration::from_millis(250));
    owner.stop();
    assert!(
        matches!(unsolicited_wake, Err(mpsc::RecvTimeoutError::Timeout)),
        "a clean owner woke without a source, state or resource change: {unsolicited_wake:?}"
    );
    let state = control.lock().unwrap();
    assert_eq!(state.report.deferred_attempts, 0);
    assert_eq!(state.report.completed_checkpoints, 0);
    drop(state);
    drop(owner);
    drop(store);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(recovered.node_count_for_label(None), 0);
}

#[test]
fn pressure_waiting_writer_wakes_admission_retry_without_the_timer() {
    let fixture = Fixture::new();
    let (mut db, suspension, next) = seeded(&fixture);
    db.set_runtime_governor(governor(1));
    let control = db.runtime.checkpoint_control_for_test();
    let (waiting, observed) = mpsc::channel();
    let (woken, wake) = mpsc::channel();
    let (frontend, entered) = mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.retry_wait_probe = Some(RetryWaitProbe { waiting, woken });
        state.frontend_wait_probe = Some(frontend);
    }
    drop(suspension);
    observed.recv_timeout(Duration::from_secs(15)).unwrap();
    create(&mut db, next).unwrap();
    let observed_phase = entered.recv_timeout(Duration::from_secs(1)).unwrap();
    let timer_expired = wake.recv_timeout(Duration::from_secs(15)).unwrap();
    assert_eq!(observed_phase, Phase::Idle);
    assert!(
        !timer_expired,
        "foreground pressure waited for the 100 ms retry timer"
    );
    assert!(
        db.automatic_checkpoint_report()
            .unwrap()
            .unwrap()
            .deferred_attempts
            >= 2
    );
    let expected = db
        .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(expected.len(), next as usize + 1);
    for (id, row) in expected.iter().enumerate() {
        assert_eq!(row["id"], crate::Value::Int(id as i64));
        assert_eq!(row["body"], crate::Value::String("b".repeat(512)));
    }
    db.checkpoint().unwrap();
    drop(db);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_eq!(
        reopened
            .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap()
            .rows,
        expected
    );
}

#[test]
fn handoff_waits_for_adoption_without_periodic_wakes_and_then_retires() {
    let fixture = Fixture::new();
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            std::collections::BTreeMap::from([
                ("id".into(), crate::Value::Int(17)),
                ("body".into(), crate::Value::String("complete".into())),
            ]),
        )
        .unwrap();
    let control = Arc::new(Control::default());
    let (waiting, observed) = mpsc::channel();
    control.lock().unwrap().owner_wait_probe = Some(waiting);
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
        LocalQosScheduler::new(hawdb_qos::LocalQosPolicy::default()),
        Some(resources.clone()),
    )
    .unwrap()
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        // Observe the actual wait entry, rather than a phase that may have
        // been published before the owner returns to its waiting loop.
        let phase = observed
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("owner did not wait after selecting its real candidate");
        if phase == Phase::Handoff {
            break;
        }
    }
    assert!(control.lock().unwrap().selected.is_some());
    let unsolicited_wake = observed.recv_timeout(Duration::from_millis(250));
    {
        let mut state = control.lock_frontend().unwrap();
        control.adopt(&mut state, &mut store).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if control.lock().unwrap().phase == Phase::Idle {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "adoption did not wake retirement"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    owner.stop();
    assert!(
        matches!(unsolicited_wake, Err(mpsc::RecvTimeoutError::Timeout)),
        "handoff woke without adoption: {unsolicited_wake:?}"
    );
    assert_eq!(resources.snapshot().admitted_memory_bytes, 0);
    assert_eq!(resources.snapshot().active_background_tasks, 0);
    assert_eq!(resources.snapshot().active_background_io_slots, 0);
    assert_eq!(
        store
            .node_owned(hawdb_storage::NodeId(0))
            .unwrap()
            .unwrap()
            .properties["body"],
        crate::Value::String("complete".into())
    );
    drop(store);
    let recovered = GraphStore::open(&fixture.0, &mut catalog).unwrap();
    assert_eq!(
        recovered
            .node_owned(hawdb_storage::NodeId(0))
            .unwrap()
            .unwrap()
            .properties["id"],
        crate::Value::Int(17)
    );
    assert_eq!(recovered.node_count_for_label(None), 1);
}
