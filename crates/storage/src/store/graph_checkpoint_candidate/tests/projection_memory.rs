use super::*;
use crate::background::CheckpointWorkContext;
use crate::projection::ProjectedGraphDefinition;

#[test]
fn checkpoint_units_projection_artifact_candidate_publication_and_snapshot_retain_admission() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-projection-memory-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    let nodes = (0..17)
        .map(|id| {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([("id".into(), Value::Int(id))]),
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    for id in 0..nodes.len() {
        store
            .create_relationship(
                &mut catalog,
                nodes[id],
                nodes[(id + 1) % nodes.len()],
                "LINKS",
                BTreeMap::new(),
            )
            .unwrap();
    }
    let definition = ProjectedGraphDefinition {
        node_labels: vec!["Memory".into()],
        rel_types: vec!["LINKS".into()],
        relationship_predicates: BTreeMap::new(),
    };
    store
        .register_projected_graph("g", definition.clone())
        .unwrap();
    store
        .register_projected_graph("other", definition.clone())
        .unwrap();
    store.rebuild_projected_graph_artifacts(&catalog).unwrap();
    let expected = store
        .projected_graph_artifacts
        .get("g")
        .unwrap()
        .data
        .clone();
    assert_eq!(expected.node_count(), 17);
    assert_eq!(expected.edge_count(), 17);
    let expected_nodes = store
        .node_records_owned()
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    let expected_relationships = store
        .relationship_records_owned()
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(expected_nodes.len(), 17);
    assert_eq!(expected_relationships.len(), 17);
    let original = store.checkpoint_source_identity();
    let source = store.checkpoint_source();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let mut candidate = source
        .prepare_checkpoint_candidate_with_work_context(&catalog, &work)
        .unwrap()
        .unwrap();
    assert_eq!(store.checkpoint_source_identity(), original);
    assert!(
        candidate
            .prepared
            .as_ref()
            .unwrap()
            .projected_graph_artifacts
            .is_none(),
        "private adoption moves the prepared root exactly once"
    );
    assert_eq!(
        candidate
            .store
            .as_ref()
            .unwrap()
            .projected_graph_artifacts
            .get("g")
            .unwrap()
            .data,
        expected
    );
    let prepared_data = std::ptr::from_ref(
        candidate
            .store
            .as_ref()
            .unwrap()
            .projected_graph_artifacts
            .get("g")
            .unwrap(),
    );
    candidate.finish_catch_up().unwrap();
    catalog = store
        .publish_checkpoint_candidate_deferred_reclamation(&mut candidate, None)
        .unwrap();
    assert_eq!(
        std::ptr::from_ref(store.projected_graph_artifacts.get("g").unwrap()),
        prepared_data,
        "publication transfers the prepared root without copying its map or arrays"
    );
    assert!(store
        .projected_graph_statuses()
        .iter()
        .all(|status| status.reusable));
    let snapshot = store.snapshot();
    let capture = store.checkpoint_source();
    store
        .register_projected_graph("g", definition.clone())
        .unwrap();
    let statuses = store.projected_graph_statuses();
    assert!(statuses
        .iter()
        .find(|status| status.name == "g")
        .unwrap()
        .projection_epoch
        .is_none());
    assert!(statuses
        .iter()
        .find(|status| status.name == "other")
        .unwrap()
        .projection_epoch
        .is_some());
    assert_eq!(
        snapshot.projected_graph_artifacts.get("g").unwrap().data,
        expected
    );
    assert!(snapshot
        .projected_graph_artifact("g", &definition)
        .is_some());
    drop(candidate);
    drop(source);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(store);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(capture);
    assert_eq!(governor.snapshot().admitted_memory_bytes, ceiling);
    drop(snapshot);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    let reopened = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        reopened
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_nodes
    );
    assert_eq!(
        reopened
            .relationship_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected_relationships
    );
    assert_eq!(reopened.projected_graph_definition("g"), Some(&definition));
    assert!(reopened.projected_graph_artifacts.get("g").is_none());
    // Ordinary recovery excludes every cache from the earlier commit epoch,
    // including names whose live stale status was retained before closure.
    assert_eq!(
        reopened.projected_graph_definition("other"),
        Some(&definition)
    );
    assert!(reopened.projected_graph_artifacts.get("other").is_none());
    assert_eq!(reopened.projected_graph_statuses().len(), 2);
    assert!(reopened
        .projected_graph_statuses()
        .iter()
        .all(|status| status.commit_epoch.is_none()));
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}
