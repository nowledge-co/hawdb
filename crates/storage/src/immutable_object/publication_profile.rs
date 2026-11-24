// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual synchronized publication comparison; not a commit-throughput gate.

use super::*;
use std::time::Instant;

fn sample(batch: bool, reuse: bool, files: usize, index: usize) -> (u128, usize) {
    let root = std::env::temp_dir().join(format!(
        "hawdb-sealed-publication-profile-{}-{index}-{files}-{batch}-{reuse}",
        std::process::id()
    ));
    fs::create_dir(&root).unwrap();
    let _project = crate::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
        &root, 32, true,
    )
    .unwrap();
    let mut store = ImmutableObjectStore::open(&root).unwrap();
    let mut inputs = Vec::new();
    for file in 0..files {
        let mut payload = vec![17; 8192];
        payload[..8].copy_from_slice(&(file as u64).to_le_bytes());
        let source = root.join(format!("producer-{file}"));
        fs::write(&source, &payload).unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &payload);
        if reuse {
            store
                .publish_sealed_file_with_work(reference, &source, BlockWork::default())
                .unwrap();
        }
        inputs.push((source, reference, payload));
    }
    let before = PUBLICATION_DIRECTORY_SYNCS.with(std::cell::Cell::get);
    let started = Instant::now();
    if batch {
        let mut publication = SealedPublicationBatch::new(files);
        for (source, reference, _) in &inputs {
            store
                .publish_sealed_file_in_batch(
                    *reference,
                    source,
                    BlockWork::default(),
                    &mut publication,
                )
                .unwrap();
        }
        store
            .finish_sealed_publication(publication, BlockWork::default())
            .unwrap();
    } else {
        for (source, reference, _) in &inputs {
            store
                .publish_sealed_file_with_work(*reference, source, BlockWork::default())
                .unwrap();
        }
    }
    let elapsed = started.elapsed().as_nanos();
    let syncs = PUBLICATION_DIRECTORY_SYNCS.with(std::cell::Cell::get) - before;
    for (source, reference, payload) in &inputs {
        assert_eq!(fs::read(store.object_path(*reference)).unwrap(), *payload);
        fs::write(source, b"later producer write").unwrap();
        assert_eq!(fs::read(store.object_path(*reference)).unwrap(), *payload);
    }
    fs::remove_dir_all(root).unwrap();
    (elapsed, syncs)
}

#[test]
fn batch_closes_directory_barriers_once_per_namespace() {
    for reuse in [false, true] {
        let (_, separate) = sample(false, reuse, 8, 0);
        let (_, batched) = sample(true, reuse, 8, 0);
        assert_eq!(separate, if reuse { 8 } else { 16 });
        assert_eq!(batched, if reuse { 1 } else { 2 });
    }
}

#[test]
fn sealed_batches_bound_retained_aliases_and_close_the_final_tail() {
    for files in [8, 9, 17, 64, 65, 129] {
        for reuse in [false, true] {
            let (_, syncs) = sample(true, reuse, files, files);
            assert_eq!(
                syncs,
                files.div_ceil(sealed::MAX_BATCH_FILES) * if reuse { 1 } else { 2 }
            );
        }
    }
}

#[test]
#[ignore = "manual synchronized publication latency comparison"]
fn sealed_publication_batch_profile() {
    for files in [1, 8, 64] {
        for index in 0..31 {
            for reuse in [false, true] {
                for batch in if index % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let (elapsed, syncs) = sample(batch, reuse, files, index);
                    println!("{{\"sample\":{index},\"reuse\":{reuse},\"batch\":{batch},\"files\":{files},\"elapsed_ns\":{elapsed},\"directory_syncs\":{syncs}}}");
                }
            }
        }
    }
}
