use super::*;

#[test]
fn default_facade_sustained_writes_checkpoint_beyond_default_operation_limit() {
    let fixture = Fixture::new();
    let config = DatabaseConfig::default();
    assert_eq!(
        config.local_qos_policy.max_background_operations,
        Some(1024)
    );
    let observation_window = config.automatic_checkpoint_max_age + Duration::from_secs(15);
    let mut db = Database::open_with_config(&fixture.0, config).unwrap();
    let body = "p".repeat(512);
    let mut next_id = 0i64;
    {
        let mut append = |db: &mut Database| {
            db.query_with_params(
                "CREATE (:Memory {id: $id, body: $body})",
                &std::collections::BTreeMap::from([
                    ("id".into(), crate::Value::Int(next_id)),
                    ("body".into(), crate::Value::String(body.clone())),
                ]),
            )
            .unwrap();
            next_id += 1;
        };
        // All resources, durability and debt-age settings are the public
        // defaults. Sustained writes continue across two complete generations;
        // there is no caller checkpoint loop or raised local QoS ceiling.
        for _ in 0..1057 {
            append(&mut db);
        }
        for generation in 1..=2 {
            let deadline = Instant::now() + observation_window;
            loop {
                append(&mut db);
                let report = db.automatic_checkpoint_report().unwrap().unwrap();
                if report.completed_checkpoints >= generation {
                    break;
                }
                assert!(
                Instant::now() < deadline,
                "default checkpoint must progress beyond 1024 records while writes continue: {report:?}"
            );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    let assert_complete = |db: &mut Database| {
        let output = db
            .query("MATCH (n:Memory) RETURN n.id AS id, n.body AS body ORDER BY id")
            .unwrap();
        assert_eq!(output.rows.len(), usize::try_from(next_id).unwrap());
        for (id, row) in output.rows.iter().enumerate() {
            assert_eq!(
                row,
                std::collections::BTreeMap::from([
                    ("id".into(), crate::Value::Int(i64::try_from(id).unwrap())),
                    ("body".into(), crate::Value::String(body.clone())),
                ])
            );
        }
    };
    assert_complete(&mut db);
    drop(db);
    let mut reopened = Database::open(&fixture.0).unwrap();
    assert_complete(&mut reopened);
}
