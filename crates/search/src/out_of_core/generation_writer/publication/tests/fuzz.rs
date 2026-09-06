use super::*;

#[test]
#[ignore = "local publication path ownership and commit-gate campaign"]
fn publication_admission_campaign() {
    let mut state = 0x206a71f5u64;
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    for case in 0..128 {
        let root = root().join("\u{130}/nested/".repeat(case % 17));
        let generation = next();
        let sequence = if case % 4 == 0 { u64::MAX - 3 } else { next() };
        let vector = case % 2 == 0;
        let peak = path_trial(&root, generation, sequence, vector, 1024 * 1024, true);
        assert_eq!(
            path_trial(&root, generation, sequence, vector, peak, true),
            peak
        );
        path_trial(&root, generation, sequence, vector, peak - 1, false);
        assert!(!root.exists());
    }
    let mut published = 0;
    let mut rejected = 0;
    let mut vector_publications = 0;
    for case in 0..24 {
        let (root, generation) = fixture();
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let files = entries(&root);
        let options = crate::SearchOutOfCoreGenerationBuildOptions {
            source_graph_commit_epoch: Some(case as u64 + 1),
            ..Default::default()
        };
        let mut expected = document(case + 1);
        expected.content = format!("published {case} \u{130}\0");
        #[cfg(feature = "vector-search")]
        if case % 3 != 0 {
            expected.embedding = Some(vec![1.0, case as f32]);
        }
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            options,
            task(16 * 1024 * 1024),
        )
        .unwrap();
        writer.push(expected.clone()).unwrap();
        let ledger = writer.memory.ledger.clone();
        match case % 4 {
            0 => BEFORE.with_borrow_mut(|slot| {
                *slot = Some(Box::new(|memory, _| occupy(memory, 6 * 3 * 128 - 1)))
            }),
            1 => COMMITTED.with_borrow_mut(|slot| {
                *slot = Some(Box::new(|memory, task| {
                    occupy(memory, 0);
                    task.cancellation().cancel();
                }))
            }),
            2 => PREPARED.with_borrow_mut(|slot| {
                *slot = Some(Box::new(|_, task| {
                    task.cancellation().cancel();
                }))
            }),
            _ => {}
        }
        let result = writer.finish();
        HELD.with_borrow_mut(|slot| slot.take());
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(
            result.is_ok(),
            case % 2 == 1,
            "case={case} result={result:?}"
        );
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        if result.is_ok() {
            vector_publications += usize::from(expected.embedding.is_some());
            assert_eq!(reader.generation(), generation + 1);
            assert_eq!(reader.source_graph_commit_epoch(), Some(case as u64 + 1));
            assert_eq!(
                reader
                    .hydrate_documents(&[expected.id.clone()])
                    .unwrap()
                    .documents,
                vec![expected]
            );
            published += 1;
        } else {
            assert_eq!(reader.generation(), generation);
            assert_eq!(
                fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
                before
            );
            assert_eq!(entries(&root), files);
            assert_eq!(
                reader
                    .hydrate_documents(&[document(0).id])
                    .unwrap()
                    .documents,
                vec![document(0)]
            );
            rejected += 1;
        }
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
    assert_eq!((published, rejected), (12, 12));
    assert_eq!(
        vector_publications,
        if cfg!(feature = "vector-search") {
            8
        } else {
            0
        }
    );
    eprintln!("publication seed=0x206a71f5 paths=128 exact=128 short=128 updates=24 published={published} rejected={rejected} late_cancelled_commits=6 vector_publications={vector_publications}");
}
