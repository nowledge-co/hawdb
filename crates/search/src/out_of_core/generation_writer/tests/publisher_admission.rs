use super::*;

#[test]
fn generation_publisher_stays_on_the_build_root_and_releases_on_failure() {
    let root = test_dir("publisher_shared_root");
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    initial.push(document(0)).unwrap();
    let first = initial.finish().unwrap();
    let before = published_files(&root);
    let limit = 16 * 1024 * 1024;
    let task = skein_core::RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(limit as u64, 0));
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_context(&root, Default::default(), task)
            .unwrap();
    writer.push(document(1)).unwrap();
    let ledger = writer.memory.ledger.clone();
    let baseline = ledger.snapshot().used_bytes - writer.spool_memory.as_ref().unwrap().bytes();
    let canonical_bytes = fs::canonicalize(&root).unwrap().capacity();
    let error = writer
        .finish_with_artifacts(|input, _, _| {
            let live = input.memory.ledger.snapshot().used_bytes;
            assert!(
                live > baseline + canonical_bytes,
                "publisher escaped the generation root"
            );
            assert!(super::super::super::SearchProjectionPublishLease::acquire(&root).is_err());
            // Reject downstream allocation while the publisher still holds its
            // charge, then prove failure preserves the preceding generation.
            input.memory.retained.reserve(limit - live + 1)?;
            panic!("allocation past the shared root was admitted");
        })
        .unwrap_err();
    assert!(error.to_string().contains("query memory"));
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(published_files(&root), before);
    let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), first.generation);
    assert_eq!(
        reader
            .hydrate_documents(&[document(0).id])
            .unwrap()
            .documents,
        vec![document(0)]
    );
    drop(reader);
    drop(super::super::super::SearchProjectionPublishLease::acquire(&root).unwrap());
    fs::remove_dir_all(root).unwrap();
}
