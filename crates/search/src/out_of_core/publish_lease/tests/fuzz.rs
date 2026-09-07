use super::*;

#[test]
#[ignore = "local native-path and publisher ownership campaign"]
fn publisher_admission_campaign() {
    let root = root();
    fs::create_dir(&root).unwrap();
    let mut random = 0x206_10cc_u64;
    for case in 0..128 {
        random = random
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let base = root.join(format!("case-{case}"));
        fs::create_dir(&base).unwrap();
        let mut native = base.clone();
        for _ in 0..1 + (random as usize % 10) {
            native.push(format!(
                "path-{}-\u{130}-\u{1f680}",
                "x".repeat((random >> 32) as usize % 64)
            ));
            fs::create_dir(&native).unwrap();
        }
        fs::create_dir(native.join("child")).unwrap();
        let alias = native.join("child/..");
        let (peak, retained) = admission(&alias);
        let lock = native.join(SEARCH_PROJECTION_PUBLISH_LOCK_FILE);
        let marker = random.to_le_bytes();
        fs::write(&lock, marker).unwrap();
        for limit in [peak + 137 - 1, peak + 137] {
            let task = task(limit);
            let memory = BuildMemory::new(&task).unwrap();
            let competing = memory.input.reserve(137).unwrap();
            REGISTRATIONS.set(0);
            let result = SearchProjectionPublishLease::acquire_with_context(&alias, &memory, &task);
            if limit == peak + 137 {
                let lease = result.unwrap();
                assert_eq!(memory.ledger.snapshot().used_bytes, retained + 137);
                assert_eq!(memory.ledger.snapshot().peak_bytes, peak + 137);
                assert_eq!(REGISTRATIONS.get(), 1);
                let contender_task = task_with_room();
                let contender_memory = BuildMemory::new(&contender_task).unwrap();
                assert!(SearchProjectionPublishLease::acquire_with_context(
                    &native,
                    &contender_memory,
                    &contender_task,
                )
                .unwrap_err()
                .to_string()
                .contains("another search projection publication"));
                assert_eq!(contender_memory.ledger.snapshot().used_bytes, 0);
                assert_eq!(entries_for(&native), 1);
                drop(lease);
            } else {
                assert!(result.unwrap_err().to_string().contains("query memory"));
                assert_eq!(REGISTRATIONS.get(), 0);
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            drop(competing);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
            assert_eq!(entries_for(&native), 0);
            assert_eq!(fs::read(&lock).unwrap(), marker);
        }
        for after_lock in [false, true] {
            let task = task_with_room();
            let memory = BuildMemory::new(&task).unwrap();
            if after_lock {
                CANCEL_LOCK.set(true);
            } else {
                CANCEL_REGISTER.set(true);
            }
            let error = SearchProjectionPublishLease::acquire_with_context(&native, &memory, &task)
                .unwrap_err();
            assert!(error.to_string().contains("cancelled"));
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(entries_for(&native), 0);
            drop(SearchProjectionPublishLease::acquire(&native).unwrap());
            assert_eq!(fs::read(&lock).unwrap(), marker);
        }
        fs::remove_dir_all(base).unwrap();
    }
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    fs::remove_dir(root).unwrap();
    eprintln!(
        "publisher seed=0x20610cc paths=128 exact=128 short=128 conflicts=128 cancellations=256"
    );
}

fn task_with_room() -> RuntimeTaskContext {
    task(1024 * 1024)
}
