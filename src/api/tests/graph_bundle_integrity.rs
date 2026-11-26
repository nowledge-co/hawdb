// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::StorageResidencyMode;

fn selected_root(path: &std::path::Path) -> hawdb_storage::sealed_root::SealedRoot {
    let head = hawdb_storage::branch_head::read_branch_head(
        &test_main_branch_directory(path)
            .unwrap()
            .join("branch.head"),
    )
    .unwrap();
    let objects =
        hawdb_storage::immutable_object::ImmutableObjectStore::open(path.join("branches/objects"))
            .unwrap();
    hawdb_storage::sealed_root::SealedRoot::decode(&objects.read(head.sealed_root).unwrap())
        .unwrap()
}

fn reject_rebinding_carried_artifact(family: &str) {
    let path = unique_test_dir(&format!("carried_binding_{family}"));
    let mut db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..DatabaseConfig::default()
        },
    )
    .unwrap();
    db.query_with_params(
        "CREATE (:Memory {id: 1, title: $title, body: $body})",
        &BTreeMap::from([
            ("title".into(), Value::String("retained".repeat(32))),
            ("body".into(), Value::String("payload".repeat(16_384))),
        ]),
    )
    .unwrap();
    db.query("CREATE (:Memory {id: 2, title: 'other'})")
        .unwrap();
    db.query("MATCH (s:Memory {id: 1}), (t:Memory {id: 2}) CREATE (s)-[:LINK]->(t)")
        .unwrap();
    db.query("CREATE INDEX ON :Memory(title)").unwrap();
    // Publish the graph first so the checkpoint below is relational-only;
    // empty-table DDL no longer checkpoints in its commit (#1942).
    db.checkpoint().unwrap();
    db.query_sql("CREATE TABLE binding_probe (id BIGINT PRIMARY KEY)")
        .unwrap();
    if matches!(family, "row-pages" | "overflow") {
        db.query_sql("CREATE TABLE retained_rows (id BIGINT PRIMARY KEY, body TEXT)")
            .unwrap();
        db.query_sql_with_params(
            "INSERT INTO retained_rows VALUES (1, $1)",
            &[Value::String("overflow".repeat(16_384))],
        )
        .unwrap();
    }
    db.checkpoint().unwrap();
    assert_eq!(
        db.storage_residency_report()
            .unwrap()
            .canonical_relationship_count,
        1
    );
    if family == "adjacency-descriptors" {
        db.query("MATCH (n:Memory {id: 1}) SET n.title = 'updated'")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("MATCH (n:Memory {id: 1}) SET n.title = 'updated-again'")
            .unwrap();
        db.checkpoint().unwrap();
    }
    let view = db.published_read_view().unwrap();
    let graph = view.graph_generation().unwrap().0;
    if family == "adjacency-descriptors" {
        assert!(view.adjacency_generation().unwrap().0 < graph);
    } else {
        assert!(graph < view.physical_generation().unwrap().0);
    }
    let name = match family {
        "adjacency-descriptors" => {
            hawdb_storage::canonical_adjacency::canonical_adjacency_descriptor_page_file(
                view.adjacency_generation().unwrap().0,
            )
        }
        "canonical-manifest" => {
            hawdb_storage::artifact_files::canonical_manifest_generation_file(graph)
        }
        "row-pages" | "overflow" => selected_root(&path)
            .checkpoint_bindings
            .iter()
            .find(|binding| {
                if family == "row-pages" {
                    binding.relative_path.starts_with("relational-row-pages-")
                        && binding.relative_path.ends_with(".pages.hawdb")
                } else {
                    binding.relative_path.starts_with("relational-overflow-")
                        && binding.relative_path.ends_with(".extents.hawdb")
                }
            })
            .expect("fixture must publish the requested physical row family")
            .relative_path
            .clone(),
        _ => format!("{family}.{graph}.hawdb"),
    };
    // Demonstrate that the selected physical path is genuinely carried, rather
    // than choosing an obsolete directory entry which publication will ignore.
    let original = selected_root(&path)
        .checkpoint_bindings
        .into_iter()
        .find(|binding| binding.relative_path == name)
        .unwrap();
    db.checkpoint().unwrap();
    assert!(selected_root(&path).checkpoint_bindings.contains(&original));
    let artifact = active_storage_root(&path).join(&name);
    let length = std::fs::metadata(&artifact).unwrap().len();
    assert!(length > 0, "fixture needs populated {family} artifact");
    if matches!(family, "canonical" | "properties" | "property-index") {
        assert!(length > 256, "fixture needs populated {family} payload");
    }
    let head = test_main_branch_directory(&path)
        .unwrap()
        .join("branch.head");
    let selected_head = std::fs::read(&head).unwrap();
    let snapshot = db.begin_read_transaction().unwrap();
    snapshot.query_sql("SELECT id FROM binding_probe").unwrap();

    // Preserve the admitted identity stamp, so publication must compare the
    // hashed bytes with the prior sealed root rather than mint a new binding.
    flip_test_file_byte(&artifact, length - 1, None);
    let error = db
        .checkpoint()
        .expect_err("changed carried bytes must not be rebound");
    assert!(matches!(error, HawDBError::StorageIntegrity(_)), "{error}");
    eprintln!("carried-binding {family}: {error}");
    if family == "canonical-manifest" {
        assert!(
            error
                .to_string()
                .contains("canonical manifest CRC32C mismatch"),
            "{error}"
        );
    } else if !matches!(family, "row-pages" | "overflow") {
        assert!(
            error.to_string().contains("previously published artifact"),
            "{error}"
        );
        assert!(error.to_string().contains(&name), "{error}");
    }
    assert_eq!(std::fs::read(&head).unwrap(), selected_head);
    assert!(db.storage_handle_poisoned().unwrap());
    assert!(snapshot
        .query_sql("SELECT id FROM binding_probe")
        .unwrap_err()
        .to_string()
        .contains("close and reopen"));
    drop(snapshot);
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn carried_checkpoint_rejects_rebinding_canonical_bytes() {
    reject_rebinding_carried_artifact("canonical");
}

#[test]
fn carried_checkpoint_rejects_rebinding_property_spill_bytes() {
    reject_rebinding_carried_artifact("properties");
}

#[test]
fn carried_checkpoint_rejects_rebinding_projection_bytes() {
    reject_rebinding_carried_artifact("property-index");
}

#[test]
fn carried_checkpoint_rejects_rebinding_adjacency_descriptor_bytes() {
    reject_rebinding_carried_artifact("adjacency-descriptors");
}

#[test]
fn carried_checkpoint_rejects_corrupt_manifest_binding() {
    reject_rebinding_carried_artifact("canonical-manifest");
}

#[test]
fn carried_checkpoint_rejects_rebinding_retained_row_page() {
    reject_rebinding_carried_artifact("row-pages");
}

#[test]
fn carried_checkpoint_rejects_rebinding_retained_overflow() {
    reject_rebinding_carried_artifact("overflow");
}
