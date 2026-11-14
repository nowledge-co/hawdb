// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::file_descriptors::ProjectFileDescriptors;
use crate::power_loss::{
    image::{ImageLimits, PersistOperation},
    IoEvent, ObservationBoundary, ObservationPoint, PowerLossModel,
};

fn selector() -> ProjectSelector {
    ProjectSelector::new(
        BranchId::new(hawdb_core::generate_uuidv7().unwrap()).unwrap(),
        BranchId::new(hawdb_core::generate_uuidv7().unwrap()).unwrap(),
    )
    .unwrap()
}

#[test]
fn bootstrap_torn_private_writes_never_publish_a_partial_ledger() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-bootstrap-torn-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let project = ProjectFileDescriptors::acquire(&root, 32).unwrap();
    let model = PowerLossModel::attach(&project, ImageLimits::default()).unwrap();
    crate::file_io::create_dir_all(root.join("ledger")).unwrap();
    crate::durability::sync_directory_ancestors(&root.join("ledger")).unwrap();
    let context = project.io_context();
    let directory = SourceDirectory::capture(&root.join("ledger"), &context).unwrap();
    model
        .observe(ObservationPoint {
            event: IoEvent::Write,
            relative_path: "ledger".into(),
            boundary: ObservationBoundary::After,
            skip_matches: 0,
            include_descendants: true,
            keep_last: false,
        })
        .unwrap();
    let identity = selector();
    assert_eq!(
        bootstrap(&directory, &context, identity, true).unwrap(),
        identity
    );
    let cut = model
        .take_observation()
        .unwrap()
        .expect("actual bootstrap write must execute");
    let path = cut.observed_path().unwrap();
    assert!(path
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("consumer-bootstrap-"));
    let writes = cut.uncovered_writes(path).unwrap();
    assert_eq!(writes.len(), 1);
    let write = writes[0];
    let mut checked = 0;
    for boundary in 1..write.length {
        for bytes in [0..boundary, boundary..write.length] {
            for reversed in [false, true] {
                let mut plan = cut.persist_all_plan();
                plan.persistence.retain(
                    |p| !matches!(p, PersistOperation::Whole(id) if *id == write.operation),
                );
                plan.persistence.push(PersistOperation::TornWrite {
                    operation: write.operation,
                    bytes: bytes.clone(),
                });
                if reversed {
                    plan.persistence.reverse();
                }
                assert!(cut
                    .crash(&plan)
                    .unwrap()
                    .bytes(Path::new("ledger/consumer-allocations.hawdb"))
                    .is_none());
                checked += 1;
            }
        }
    }
    assert!(checked > 0);
    let finished = model.capture().unwrap().crash(&Default::default()).unwrap();
    assert_eq!(
        finished
            .bytes(Path::new("ledger/consumer-allocations.hawdb"))
            .unwrap(),
        std::fs::read(root.join("ledger/consumer-allocations.hawdb")).unwrap()
    );
    drop((directory, context, model, project));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn bootstrap_lost_selection_reply_retries_the_original_identity() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-bootstrap-reply-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let project = ProjectFileDescriptors::acquire(&root, 8).unwrap();
    let context = project.io_context();
    let directory = SourceDirectory::capture(&root, &context).unwrap();
    let identity = selector();
    let result = SourceDirectory::observe_control_replies(
        || {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "lost bootstrap selection reply",
            ))
        },
        || bootstrap(&directory, &context, identity, true),
    )
    .unwrap();
    assert!(
        matches!(result, Err(HawDBError::Storage(ref message)) if message.contains("lost bootstrap selection reply"))
    );
    assert_eq!(
        bootstrap(&directory, &context, selector(), true).unwrap(),
        identity
    );
    assert_eq!(read(&directory, &context).unwrap().high_water, 0);
    drop((directory, context, project));
    std::fs::remove_dir_all(root).unwrap();
}
