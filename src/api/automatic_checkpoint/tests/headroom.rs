use super::*;
mod notifications;
use std::sync::mpsc::{self, Receiver, Sender};

fn pause() -> (Arc<OwnerPauseProbe>, Receiver<()>, Sender<()>) {
    let (paused, observation) = mpsc::channel();
    let (resume, continuation) = mpsc::channel();
    (
        Arc::new(OwnerPauseProbe {
            paused,
            resume: Mutex::new(continuation),
        }),
        observation,
        resume,
    )
}

fn create(db: &mut Database, id: i64) -> Result<()> {
    db.query_with_params(
        "CREATE (:Memory {id: $id, body: $body})",
        &std::collections::BTreeMap::from([
            ("id".into(), crate::Value::Int(id)),
            ("body".into(), crate::Value::String("b".repeat(512))),
        ]),
    )?;
    Ok(())
}

fn seeded(fixture: &Fixture) -> (Database, Suspension, i64) {
    let config = DatabaseConfig {
        automatic_checkpoint_max_age: Duration::from_secs(3600),
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        max_out_of_core_delta_bytes: Some(16 * 1024),
        ..DatabaseConfig::default()
    };
    let mut db = Database::open_with_config(&fixture.0, config).unwrap();
    db.checkpoint().unwrap();
    let suspension = db.runtime.suspend_automatic_checkpoint().unwrap().unwrap();
    let mut next = 0;
    while db
        .runtime
        .peek()
        .unwrap()
        .store
        .checkpoint_debt_snapshot()
        .unwrap()
        .delta_bytes
        * 1_000_000
        < 16 * 1024 * u64::from(hawdb_storage::pressure::STORAGE_PRESSURE_SOFT_RATIO_PER_MILLION)
    {
        create(&mut db, next).unwrap();
        next += 1;
    }
    assert!(next < 320);
    (db, suspension, next)
}

fn assert_values_and_reopen(mut db: Database, fixture: &Fixture) {
    let report = db.automatic_checkpoint_report().unwrap().unwrap();
    assert!(report.completed_checkpoints >= 2, "{report:?}");
    let expected = db
        .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
        .unwrap()
        .rows;
    assert_eq!(expected.len(), 320);
    for (id, row) in expected.iter().enumerate() {
        assert_eq!(row.get("id"), Some(&crate::Value::Int(id as i64)));
        assert_eq!(
            row.get("body"),
            Some(&crate::Value::String("b".repeat(512)))
        );
    }
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
fn pending_idle_owner_blocks_pressure_growth_before_admission() {
    let fixture = Fixture::new();
    let (mut db, suspension, next) = seeded(&fixture);
    let control = db.runtime.checkpoint_control_for_test();
    let (probe, paused, resume) = pause();
    let (waiting, waited) = mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.idle_start_probe = Some(probe);
        state.frontend_wait_probe = Some(waiting);
    }
    drop(suspension);
    paused.recv_timeout(Duration::from_secs(15)).unwrap();
    let producer = std::thread::spawn(move || -> Result<Database> {
        for id in next..320 {
            create(&mut db, id)?;
        }
        Ok(db)
    });
    let observed = waited.recv_timeout(Duration::from_secs(1));
    resume.send(()).unwrap();
    let produced = producer.join().unwrap();
    assert_eq!(observed.unwrap(), Phase::Idle);
    assert_values_and_reopen(produced.unwrap(), &fixture);
}

#[test]
fn retiring_owner_blocks_pressure_growth_until_cleanup_finishes() {
    let fixture = Fixture::new();
    let (mut db, suspension, next) = seeded(&fixture);
    let control = db.runtime.checkpoint_control_for_test();
    let (probe, paused, resume) = pause();
    {
        control.lock().unwrap().retirement_probe = Some(probe);
    }
    drop(suspension);
    wait_for(&db, |report| report.completed_checkpoints == 1);
    // Adopt the selected generation before pausing its off-gate retirement.
    drop(db.runtime.get_mut().unwrap());
    paused.recv_timeout(Duration::from_secs(15)).unwrap();
    let (waiting, waited) = mpsc::channel();
    control.lock().unwrap().frontend_wait_probe = Some(waiting);
    let producer = std::thread::spawn(move || -> Result<Database> {
        for id in next..320 {
            create(&mut db, id)?;
        }
        Ok(db)
    });
    let observed = waited.recv_timeout(Duration::from_secs(1));
    resume.send(()).unwrap();
    let produced = producer.join().unwrap();
    assert_eq!(observed.unwrap(), Phase::Retiring);
    assert_values_and_reopen(produced.unwrap(), &fixture);
}

#[test]
fn pending_idle_admission_denial_releases_the_frontend_without_a_host_retry() {
    let fixture = Fixture::new();
    let (mut db, suspension, next) = seeded(&fixture);
    db.set_runtime_governor(governor(1));
    let control = db.runtime.checkpoint_control_for_test();
    let (probe, paused, resume) = pause();
    let (waiting, waited) = mpsc::channel();
    {
        let mut state = control.lock().unwrap();
        state.idle_start_probe = Some(probe);
        state.frontend_wait_probe = Some(waiting);
    }
    drop(suspension);
    paused.recv_timeout(Duration::from_secs(15)).unwrap();
    let producer = std::thread::spawn(move || -> Result<Database> {
        create(&mut db, next)?;
        Ok(db)
    });
    let observed = waited.recv_timeout(Duration::from_secs(1));
    resume.send(()).unwrap();
    let mut produced = producer.join().unwrap().unwrap();
    assert_eq!(observed.unwrap(), Phase::Idle);
    let report = produced.automatic_checkpoint_report().unwrap().unwrap();
    assert!(report.deferred_attempts >= 1, "{report:?}");
    assert_eq!(report.completed_checkpoints, 0);
    assert_eq!(
        produced
            .query("MATCH (n:Memory) RETURN n.id")
            .unwrap()
            .rows
            .len(),
        next as usize + 1
    );
}
