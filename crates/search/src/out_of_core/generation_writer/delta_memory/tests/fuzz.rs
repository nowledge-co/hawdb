use super::*;

#[test]
#[ignore = "local delta conversion and published merge ownership campaign"]
fn delta_admission_campaign() {
    let mut seed = 0x206de17au64;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };
    let mut consumed = 0;
    for case in 0..128 {
        let count = next() as usize % 9;
        let mut upserts = Vec::with_capacity(count + next() as usize % 17);
        for number in 0..count {
            let mut row = row(&format!("{number:03}"));
            row.body =
                ["", "\u{130}", "NUL\0", "delta"][next() as usize % 4].repeat(next() as usize % 33);
            row.external_id.reserve_exact(next() as usize % 31);
            if next() % 2 == 0 {
                row.source_id = None;
            }
            if next() % 3 == 0 {
                row.metadata.clear();
            }
            for field in 0..next() % 7 {
                row.metadata.insert(
                    format!("field:{field}"),
                    "payload".repeat(next() as usize % 9),
                );
            }
            upserts.push(row);
        }
        upserts.reverse();
        let mut deletes = Vec::with_capacity(3 + next() as usize % 11);
        for id in ["zzz", "z"] {
            let mut id = id.to_owned();
            id.reserve_exact(next() as usize % 101);
            deletes.push(id);
        }
        let delta = SearchProjectionDelta {
            upserts,
            deletes,
            ..Default::default()
        };
        // Cloning can change capacities, so derive every boundary from the
        // actual owned input used by that attempt, not from the original Vec.
        let expected = delta
            .upserts
            .iter()
            .map(reference)
            .map(|doc| (doc.id.clone(), doc))
            .collect::<BTreeMap<_, _>>();
        for (delta, exact) in [(delta.clone(), false), (delta, true)] {
            let peak = conversion_bytes(&delta, &RuntimeTaskContext::default()).unwrap();
            let task = task(peak + 137 - usize::from(!exact));
            let memory = BuildMemory::new(&task).unwrap();
            let other = memory.spool.reserve(137).unwrap();
            let result = Input::new(delta, &memory, u64::MAX, &task);
            assert_eq!(result.is_ok(), exact, "case={case}");
            if let Ok(mut input) = result {
                for expected in expected.values() {
                    input
                        .consume_upsert(|actual| {
                            assert_eq!(&actual, expected);
                            Ok(())
                        })
                        .unwrap();
                    consumed += 1;
                }
                assert_eq!(input.upsert_id(), None);
                assert_eq!(input.delete_id(), Some("z"));
                input.discard_delete();
                assert_eq!(input.delete_id(), Some("zzz"));
                input.discard_delete();
            }
            assert_eq!(memory.ledger.snapshot().used_bytes, 137);
            drop(other);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_eq!(memory.ledger.snapshot().account_count, 3);
        }
    }
    let mut published = 0;
    for case in 0..32 {
        let (root, reader) = fixture("fuzz");
        let before = manifest(&root);
        let mut expected = ["000", "002", "004", "008"]
            .into_iter()
            .map(|id| {
                let doc = reference(&row(id));
                (doc.id.clone(), doc)
            })
            .collect::<BTreeMap<_, _>>();
        let mut delta = SearchProjectionDelta::default();
        for id in ["000", "001", "002", "003", "004", "008", "999"] {
            match next() % 3 {
                0 => {
                    let mut row = row(id);
                    row.body = format!("updated {case} \u{130}\0");
                    let doc = reference(&row);
                    expected.insert(doc.id.clone(), doc);
                    delta.upserts.push(row);
                }
                1 => {
                    let id = format!("memory:{id}");
                    expected.remove(&id);
                    delta.deletes.push(id);
                }
                _ => {}
            }
        }
        delta.upserts.reverse();
        delta.deletes.reverse();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            delta,
            SearchOutOfCoreGenerationBuildOptions::default(),
            task(16 * 1024 * 1024),
        )
        .unwrap();
        assert_eq!(manifest(&root), before);
        assert_eq!(update.delta_report().after_document_count, expected.len());
        update.finish().unwrap();
        drop(reader);
        let reopened = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reopened.document_count(), expected.len());
        assert_eq!(
            reopened
                .hydrate_documents(&expected.keys().cloned().collect::<Vec<_>>())
                .unwrap()
                .documents,
            expected.into_values().collect::<Vec<_>>()
        );
        published += 1;
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }
    eprintln!("delta seed=0x206de17a input_cases=128 exact=128 short=128 published={published} consumed={consumed}");
}
