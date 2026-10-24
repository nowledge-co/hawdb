// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use tempfile::tempdir;

const LAYOUTS: [Layout; 3] = [Layout::Versioned, Layout::Hybrid, Layout::Cow];

#[test]
fn filtered_scan_chunks_preserve_deep_versions_tombstones_and_binary_bounds() {
    let keys = [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![1, 0],
        vec![255],
        vec![255, 0],
        vec![254; MAX_KEY],
    ];
    let upper = vec![255; MAX_KEY];
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let store = Store::open_layout(&path, layout).unwrap();
        let mut model = BTreeMap::new();
        let mut snapshots = vec![(store.begin_read().unwrap(), model.clone())];
        for round in 1u64..=97 {
            let mut tx = store.begin_write().unwrap();
            for (position, key) in keys.iter().enumerate() {
                if (position < 4 && round >= 65) || (position == 6 && round % 3 == 0) {
                    tx.delete(key).unwrap();
                    model.remove(key);
                } else {
                    let value = if position == 5 {
                        Vec::new()
                    } else {
                        vec![round as u8; if position == 8 { MAX_VALUE } else { 17 }]
                    };
                    tx.put(key, &value).unwrap();
                    model.insert(key.clone(), value);
                }
            }
            if round == 64 {
                tx.put(&[0, 127], b"late key").unwrap();
                model.insert(vec![0, 127], b"late key".to_vec());
            }
            tx.commit(round).unwrap();
            if [1, 17, 33, 64, 65, 66, 97].contains(&round) {
                snapshots.push((store.begin_read().unwrap(), model.clone()));
            }
        }
        for (snapshot, expected) in &snapshots {
            for (start, end) in [
                (&[][..], upper.as_slice()),
                (&[][..], &[0][..]),
                (&[0][..], &[1][..]),
                (&[1][..], upper.as_slice()),
                (&[255][..], upper.as_slice()),
            ] {
                let rows = expected
                    .iter()
                    .filter(|(key, _)| key.as_slice() >= start && key.as_slice() < end)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<Rows>();
                assert_eq!(snapshot.scan(start, end).unwrap(), rows);
            }
            for key in &keys {
                assert_eq!(snapshot.get(key).unwrap(), expected.get(key).cloned());
            }
        }
        drop(snapshots);
        drop(store);
        let recovered = Store::open_layout(&path, layout).unwrap();
        assert_eq!(
            recovered.begin_read().unwrap().scan(&[], &upper).unwrap(),
            model.into_iter().collect::<Rows>()
        );
    }
}

fn pair(tx: &mut WriteTxn, value: &[u8]) {
    tx.put(b"a", value).unwrap();
    tx.put(b"b", value).unwrap();
}

#[test]
fn snapshots_private_writes_rollback_and_reopen() {
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let store = Store::open_layout(&path, layout).unwrap();
        let mut tx = store.begin_write().unwrap();
        pair(&mut tx, b"old");
        tx.commit(1).unwrap();
        let old = store.begin_read().unwrap();
        let mut tx = store.begin_write().unwrap();
        pair(&mut tx, b"new");
        assert_eq!(tx.get(b"a").unwrap(), Some(b"new".to_vec()));
        assert_eq!(old.get(b"a").unwrap(), Some(b"old".to_vec()));
        tx.commit(2).unwrap();
        assert_eq!(
            old.scan(b"a", b"c").unwrap(),
            vec![
                (b"a".to_vec(), b"old".to_vec()),
                (b"b".to_vec(), b"old".to_vec())
            ]
        );
        let mut tx = store.begin_write().unwrap();
        tx.delete(b"a").unwrap();
        assert_eq!(tx.get(b"a").unwrap(), None);
        tx.rollback();
        assert_eq!(
            store.begin_read().unwrap().get(b"a").unwrap(),
            Some(b"new".to_vec())
        );
        drop(old);
        drop(store);
        let store = Store::open_layout(&path, layout).unwrap();
        assert_eq!(
            store.begin_read().unwrap().scan(b"a", b"c").unwrap(),
            vec![
                (b"a".to_vec(), b"new".to_vec()),
                (b"b".to_vec(), b"new".to_vec())
            ]
        );
    }
}

#[test]
fn partial_install_and_readers_never_observe_half_a_pair() {
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let store = Store::open_layout(&dir.path().join("journal"), layout).unwrap();
        let mut tx = store.begin_write().unwrap();
        pair(&mut tx, b"old");
        tx.commit(1).unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let writer = store.clone();
        let job = thread::spawn(move || {
            let mut tx = writer.begin_write().unwrap();
            pair(&mut tx, b"new");
            tx.commit_with_hook(2, |stage| {
                if stage == Stage::Installed(1) {
                    ready_tx.send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(20))
                        .unwrap();
                }
                Ok(())
            })
            .unwrap();
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(20))
            .unwrap();
        let snapshot = store.begin_read().unwrap();
        assert_eq!(
            snapshot.scan(b"a", b"c").unwrap(),
            vec![
                (b"a".to_vec(), b"old".to_vec()),
                (b"b".to_vec(), b"old".to_vec())
            ]
        );
        release_tx.send(()).unwrap();
        job.join().unwrap();
        assert_eq!(snapshot.get(b"b").unwrap(), Some(b"old".to_vec()));
        assert_eq!(
            store.begin_read().unwrap().get(b"b").unwrap(),
            Some(b"new".to_vec())
        );
    }
}

#[test]
fn first_committer_wins_but_disjoint_writes_can_commit() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("journal")).unwrap();
    let mut a = store.begin_write().unwrap();
    let mut b = store.begin_write().unwrap();
    let mut c = store.begin_write().unwrap();
    a.put(b"a", b"1").unwrap();
    b.put(b"a", b"2").unwrap();
    c.put(b"b", b"3").unwrap();
    a.commit(1).unwrap();
    assert!(matches!(b.commit(2), Err(Error::Conflict(_))));
    c.commit(3).unwrap();
    assert_eq!(store.begin_read().unwrap().epoch(), 2);
}

#[test]
fn operation_retry_is_exact_and_persists_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("journal");
    let store = Store::open(&path).unwrap();
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"v");
    assert_eq!(tx.commit(42).unwrap(), 1);
    let bytes = store.journal_bytes().unwrap();
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"v");
    assert_eq!(tx.commit(42).unwrap(), 1);
    assert_eq!(bytes, store.journal_bytes().unwrap());
    drop(store);
    let store = Store::open(&path).unwrap();
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"v");
    assert_eq!(tx.commit(42).unwrap(), 1);
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"different");
    assert!(matches!(tx.commit(42), Err(Error::Invalid(_))));
}

#[test]
fn codec_and_layouts_match_an_independent_model() {
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let store = Store::open_layout(&path, layout).unwrap();
        let keys = [
            vec![],
            vec![0],
            vec![0, 0],
            vec![0, 1],
            vec![1],
            vec![255],
            vec![255, 0],
        ];
        let mut model = BTreeMap::new();
        let mut saved = Vec::new();
        let mut rng = 42u64;
        for operation in 1..=140 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = &keys[(rng as usize) % keys.len()];
            let mut tx = store.begin_write().unwrap();
            if rng.is_multiple_of(4) {
                tx.delete(key).unwrap();
                model.remove(key);
            } else {
                let value = rng.to_le_bytes().to_vec();
                tx.put(key, &value).unwrap();
                model.insert(key.clone(), value);
            }
            tx.commit(operation).unwrap();
            let snap = store.begin_read().unwrap();
            for key in &keys {
                assert_eq!(snap.get(key).unwrap(), model.get(key).cloned());
            }
            assert_eq!(
                snap.scan(b"", &[255, 255]).unwrap(),
                model.clone().into_iter().collect::<Rows>()
            );
            if operation % 23 == 0 {
                saved.push((snap, model.clone()));
            }
        }
        for (snapshot, expected) in saved {
            assert_eq!(
                snapshot.scan(b"", &[255, 255]).unwrap(),
                expected.into_iter().collect::<Rows>()
            );
        }
        drop(store);
        let store = Store::open_layout(&path, layout).unwrap();
        assert_eq!(
            store.begin_read().unwrap().scan(b"", &[255, 255]).unwrap(),
            model.into_iter().collect::<Rows>()
        );
    }
}

#[test]
fn long_snapshot_does_not_hold_the_writer_or_native_iterator() {
    let dir = tempdir().unwrap();
    let store = Store::open(&dir.path().join("journal")).unwrap();
    let old = store.begin_read().unwrap();
    let writer = store.clone();
    let (send, recv) = mpsc::channel();
    let job = thread::spawn(move || {
        for op in 1u64..=100 {
            let mut tx = writer.begin_write().unwrap();
            pair(&mut tx, &op.to_le_bytes());
            tx.commit(op).unwrap();
        }
        send.send(()).unwrap();
    });
    recv.recv_timeout(std::time::Duration::from_secs(20))
        .expect("writer must progress with old snapshot retained");
    assert!(old.scan(b"a", b"c").unwrap().is_empty());
    job.join().unwrap();
}

#[test]
fn uncertain_installation_poisons_until_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("journal");
    let store = Store::open(&path).unwrap();
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"durable");
    assert!(matches!(
        tx.commit_with_hook(1, |s| if s == Stage::Installed(1) {
            Err(Error::Invalid("injected install error"))
        } else {
            Ok(())
        }),
        Err(Error::CommitUncertain(_))
    ));
    assert!(matches!(store.begin_read(), Err(Error::NeedsRecovery)));
    assert!(matches!(store.begin_write(), Err(Error::NeedsRecovery)));
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(
        store.begin_read().unwrap().get(b"b").unwrap(),
        Some(b"durable".to_vec())
    );
}

#[test]
fn framing_rejects_torn_corrupt_and_extra_tail_without_repair() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("source");
    let store = Store::open(&path).unwrap();
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"value");
    tx.commit(1).unwrap();
    drop(store);
    let original = std::fs::read(&path).unwrap();
    for (i, bytes) in [
        original[..original.len() - 1].to_vec(),
        {
            let mut b = original.clone();
            *b.last_mut().unwrap() ^= 1;
            b
        },
        {
            let mut b = original.clone();
            b.extend([0; 5]);
            b
        },
    ]
    .into_iter()
    .enumerate()
    {
        let damaged = dir.path().join(format!("damaged-{i}"));
        std::fs::write(&damaged, &bytes).unwrap();
        assert!(Store::open(&damaged).is_err());
        assert_eq!(std::fs::read(damaged).unwrap(), bytes);
    }
}

#[test]
fn owner_layout_and_resource_checks_precede_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("journal");
    let store = Store::open(&path).unwrap();
    assert!(matches!(Store::open(&path), Err(Error::Busy)));
    let mut tx = store.begin_write().unwrap();
    assert!(matches!(
        tx.put(&[0; MAX_KEY + 1], b"v"),
        Err(Error::Budget(_))
    ));
    assert!(matches!(
        tx.put(b"k", &[0; MAX_VALUE + 1]),
        Err(Error::Budget(_))
    ));
    let before = store.journal_bytes().unwrap();
    tx.rollback();
    assert_eq!(store.journal_bytes().unwrap(), before);
    drop(store);
    assert!(Store::open_layout(&path, Layout::Versioned).is_err());
}

#[test]
#[ignore = "subprocess entry point"]
fn crash_child() {
    let path = std::env::var_os("HAWDB_BFTREE_TEST_JOURNAL").expect("test child journal");
    let stage = std::env::var("HAWDB_BFTREE_TEST_STAGE").unwrap();
    let layout = Layout::parse(&std::env::var("HAWDB_BFTREE_TEST_LAYOUT").unwrap()).unwrap();
    let store = Store::open_layout(Path::new(&path), layout).unwrap();
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"committed");
    tx.commit_with_hook(9, |s| {
        if (stage == "durable" && s == Stage::Durable)
            || (stage == "install" && s == Stage::Installed(1))
            || (stage == "publish" && s == Stage::Published)
        {
            std::process::exit(86);
        }
        Ok(())
    })
    .unwrap();
    panic!("crash hook was not reached");
}

#[test]
fn process_crash_replays_the_whole_transaction_at_each_boundary() {
    for layout in LAYOUTS {
        for stage in ["durable", "install", "publish"] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("journal");
            let status = Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "adapter::tests::crash_child",
                    "--ignored",
                    "--nocapture",
                ])
                .env("HAWDB_BFTREE_TEST_JOURNAL", &path)
                .env("HAWDB_BFTREE_TEST_STAGE", stage)
                .env("HAWDB_BFTREE_TEST_LAYOUT", layout.name())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(86));
            let store = Store::open_layout(&path, layout).unwrap();
            assert_eq!(
                store.begin_read().unwrap().scan(b"a", b"c").unwrap(),
                vec![
                    (b"a".to_vec(), b"committed".to_vec()),
                    (b"b".to_vec(), b"committed".to_vec())
                ]
            );
            let mut tx = store.begin_write().unwrap();
            pair(&mut tx, b"committed");
            assert_eq!(tx.commit(9).unwrap(), 1);
        }
    }
}

#[test]
fn page_splits_long_history_deletes_and_exact_range_edges() {
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let store = Store::open_layout(&dir.path().join("journal"), layout).unwrap();
        let empty = store.begin_read().unwrap();
        let mut operation = 0;
        let mut snapshots = Vec::new();
        for round in 0u8..8 {
            for start in (0u16..384).step_by(MAX_WRITES) {
                let mut tx = store.begin_write().unwrap();
                for n in start..start + MAX_WRITES as u16 {
                    if round == 7 && n.is_multiple_of(3) {
                        tx.delete(&n.to_be_bytes()).unwrap();
                    } else {
                        tx.put(&n.to_be_bytes(), &[round; 100]).unwrap();
                    }
                }
                operation += 1;
                tx.commit(operation).unwrap();
            }
            snapshots.push(store.begin_read().unwrap());
        }
        assert!(empty.scan(b"", &[255]).unwrap().is_empty());
        for (round, snapshot) in snapshots.iter().enumerate() {
            let expected: Rows = (31u16..351)
                .filter(|n| round != 7 || !n.is_multiple_of(3))
                .map(|n| (n.to_be_bytes().to_vec(), vec![round as u8; 100]))
                .collect();
            assert_eq!(
                snapshot
                    .scan(&31u16.to_be_bytes(), &351u16.to_be_bytes())
                    .unwrap(),
                expected
            );
        }
    }
}

#[test]
fn panic_after_durability_requires_complete_replay() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("journal");
    let store = Store::open(&path).unwrap();
    let mut tx = store.begin_write().unwrap();
    pair(&mut tx, b"committed");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tx.commit_with_hook(3, |stage| {
            if stage == Stage::Installed(1) {
                panic!("injected unwind");
            }
            Ok(())
        })
    }));
    assert!(result.is_err());
    assert!(matches!(store.begin_read(), Err(Error::NeedsRecovery)));
    drop(store);
    let store = Store::open(&path).unwrap();
    let snapshot = store.begin_read().unwrap();
    assert_eq!(snapshot.get(b"a").unwrap(), Some(b"committed".to_vec()));
    assert_eq!(snapshot.get(b"b").unwrap(), Some(b"committed".to_vec()));
}

#[test]
fn entry_limit_and_full_size_fields_preserve_previous_transactions() {
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let store = Store::open_layout(&dir.path().join("journal"), layout).unwrap();
        let mut tx = store.begin_write().unwrap();
        for n in 0..MAX_WRITES {
            tx.put(&(n as u64).to_be_bytes(), b"v").unwrap();
        }
        assert!(matches!(tx.put(b"overflow", b"v"), Err(Error::Budget(_))));
        tx.commit(1).unwrap();
        let key = [255; MAX_KEY];
        let value = [7; MAX_VALUE];
        let mut tx = store.begin_write().unwrap();
        tx.put(&key, &value).unwrap();
        tx.commit(2).unwrap();
        let old = store.begin_read().unwrap();
        let mut tx = store.begin_write().unwrap();
        tx.delete(&key).unwrap();
        tx.commit(3).unwrap();
        assert_eq!(old.get(&key).unwrap(), Some(value.to_vec()));
        assert_eq!(store.begin_read().unwrap().get(&key).unwrap(), None);
    }
}

#[test]
fn alternating_point_reads_and_scans_keep_native_cache_consistent() {
    for layout in [Layout::Versioned, Layout::Hybrid] {
        let dir = tempdir().unwrap();
        let store = Store::open_layout(&dir.path().join("journal"), layout).unwrap();
        let mut old = None;
        for round in 0..3 {
            let mut tx = store.begin_write().unwrap();
            for n in 0u64..64 {
                tx.put(&n.to_be_bytes(), &[round; 100]).unwrap();
            }
            tx.commit(u64::from(round) + 1).unwrap();
            if round == 0 {
                old = Some(store.begin_read().unwrap());
            }
        }
        for _ in 0..256 {
            let current = store.begin_read().unwrap();
            for n in 0u64..64 {
                assert_eq!(current.get(&n.to_be_bytes()).unwrap(), Some(vec![2; 100]));
                assert_eq!(
                    old.as_ref().unwrap().get(&n.to_be_bytes()).unwrap(),
                    Some(vec![0; 100])
                );
            }
            assert_eq!(current.scan(b"", &[255]).unwrap().len(), 64);
            assert_eq!(old.as_ref().unwrap().scan(b"", &[255]).unwrap().len(), 64);
        }
    }
}

#[test]
fn row_budget_returns_error_instead_of_partial_results() {
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let store = Store::open_layout(&dir.path().join("journal"), layout).unwrap();
        for start in (0..=MAX_SCAN_ROWS).step_by(MAX_WRITES) {
            let mut tx = store.begin_write().unwrap();
            for n in start..(start + MAX_WRITES).min(MAX_SCAN_ROWS + 1) {
                tx.put(&(n as u64).to_be_bytes(), b"v").unwrap();
            }
            tx.commit(start as u64).unwrap();
        }
        let snapshot = store.begin_read().unwrap();
        assert_eq!(
            snapshot
                .scan(b"", &(MAX_SCAN_ROWS as u64).to_be_bytes())
                .unwrap()
                .len(),
            MAX_SCAN_ROWS
        );
        assert!(matches!(
            snapshot.scan(b"", &[255]),
            Err(Error::Budget("scan rows"))
        ));
    }
}

#[test]
fn oversized_journal_is_refused_without_modifying_evidence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oversized");
    File::create(&path)
        .unwrap()
        .set_len(MAX_JOURNAL + 1)
        .unwrap();
    assert!(matches!(
        Store::open(&path),
        Err(Error::Budget("journal bytes"))
    ));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), MAX_JOURNAL + 1);
}

#[test]
fn recovered_receipts_preserve_sparse_updates_deletes_and_old_retries() {
    for layout in LAYOUTS {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let store = Store::open_layout(&path, layout).unwrap();
        let mut seed = store.begin_write().unwrap();
        for (key, value) in [(b"a", b"A"), (b"b", b"B"), (b"c", b"C")] {
            seed.put(key, value).unwrap();
        }
        seed.commit(99).unwrap();
        let mut sparse = store.begin_write().unwrap();
        sparse.put(b"b", b"B1").unwrap();
        sparse.commit(7).unwrap();
        drop(store);
        let store = Store::open_layout(&path, layout).unwrap();
        let old = store.begin_read().unwrap();
        let mut retry = store.begin_write().unwrap();
        for (key, value) in [(b"a", b"A"), (b"b", b"B"), (b"c", b"C")] {
            retry.put(key, value).unwrap();
        }
        assert_eq!(retry.commit(99).unwrap(), 1);
        let mut update = store.begin_write().unwrap();
        update.delete(b"b").unwrap();
        update.put(b"c", b"C2").unwrap();
        update.commit(8).unwrap();
        let deleted = store.begin_read().unwrap();
        let mut reinsert = store.begin_write().unwrap();
        reinsert.put(b"b", b"B3").unwrap();
        reinsert.commit(2).unwrap();
        assert_eq!(old.get(b"b").unwrap(), Some(b"B1".to_vec()));
        assert_eq!(old.get(b"c").unwrap(), Some(b"C".to_vec()));
        assert_eq!(deleted.get(b"b").unwrap(), None);
        assert_eq!(
            store.begin_read().unwrap().scan(b"a", b"d").unwrap(),
            vec![
                (b"a".to_vec(), b"A".to_vec()),
                (b"b".to_vec(), b"B3".to_vec()),
                (b"c".to_vec(), b"C2".to_vec()),
            ]
        );
    }
}
