// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual full-checkpoint comparison through the existing graph library path.

use hawdb_core::{schema::Catalog, value::Value, Uuid};
use hawdb_storage::{
    branch_catalog::{self, BranchId},
    mutation::GraphMutation,
    store::{BranchAdmissionRequest, GraphStore},
};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn node(index: usize) -> GraphMutation {
    GraphMutation::CreateNode {
        label: "Document".into(),
        properties: BTreeMap::from([
            ("id".into(), Value::Int(index as i64)),
            ("body".into(), Value::String("x".repeat(512))),
        ]),
    }
}

fn open(
    path: &Path,
    branch: bool,
    initialize: bool,
) -> Result<(GraphStore, Catalog), Box<dyn Error>> {
    let mut catalog = Catalog::default();
    if !branch {
        return Ok((GraphStore::open(path, &mut catalog)?, catalog));
    }
    let branch_id = BranchId::new(Uuid::from_u128(1))?;
    let project_id = BranchId::new(Uuid::from_u128(90))?;
    let catalog_path = path.join("branches/catalog.hawdb");
    let objects = path.join("immutable");
    let head_path = path
        .join("branches")
        .join(branch_id.as_uuid().to_string())
        .join("branch.head");
    if initialize {
        let source_path = path.join("producer");
        let mut source = GraphStore::open(&source_path, &mut catalog)?;
        source.create_node(&mut catalog, "Bootstrap", BTreeMap::new())?;
        source.checkpoint(&catalog)?;
        source.create_node(&mut catalog, "Bootstrap", BTreeMap::new())?;
        std::fs::create_dir_all(head_path.parent().ok_or("head has no parent")?)?;
        let head = source.initialize_immutable_root_head(
            &objects,
            &head_path,
            *project_id.as_uuid().as_bytes(),
            *branch_id.as_uuid().as_bytes(),
        )?;
        branch_catalog::initialize_catalog_file(&catalog_path, project_id, branch_id)?;
        branch_catalog::bind_main_head_file(
            &catalog_path,
            project_id,
            branch_id,
            *head.sealed_root.sha256.as_bytes(),
            head.logical_commit_epoch,
        )?;
        drop(source);
        std::fs::remove_dir_all(source_path)?;
    }
    let branch_catalog = branch_catalog::read_catalog(&catalog_path)?;
    let record = branch_catalog
        .branches
        .iter()
        .find(|record| record.id == branch_id)
        .ok_or("missing branch")?;
    Ok(GraphStore::admit_branch_from_head(BranchAdmissionRequest {
        catalog_path: &catalog_path,
        branch_id,
        expected_metadata_revision: record.metadata_revision,
        head_path: &head_path,
        immutable_store_root: &objects,
        durability: Default::default(),
        replay_config: Default::default(),
    })?
    .into_parts())
}

fn main() -> Result<(), Box<dyn Error>> {
    let root = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("missing new parent directory")?,
    );
    std::fs::create_dir(&root)?;
    let mut stdout = io::stdout().lock();
    for (sequence, line) in io::stdin().lock().lines().enumerate() {
        let request: serde_json::Value = serde_json::from_str(&line?)?;
        let rows = request["rows"].as_u64().ok_or("missing row count")? as usize;
        if rows > 10_000 {
            return Err("row count exceeds 10000".into());
        }
        let precheckpoint = request["precheckpoint"].as_bool().unwrap_or(false);
        let path = root.join(format!("case-{sequence}"));
        let branch = request["branch"].as_bool().unwrap_or(false);
        let (mut store, mut catalog) = open(&path, branch, true)?;
        if rows > 0 {
            store.commit_mutations(&mut catalog, (0..rows).map(node).collect())?;
        }
        if precheckpoint {
            store.checkpoint(&catalog)?;
            store.commit_mutations(&mut catalog, vec![node(rows)])?;
        }
        let before = store.node_records_owned().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            before.len(),
            rows + usize::from(precheckpoint) + if branch { 2 } else { 0 }
        );
        let started = Instant::now();
        store.checkpoint(&catalog)?;
        let elapsed = started.elapsed().as_nanos();
        assert_eq!(
            store.node_records_owned().collect::<Result<Vec<_>, _>>()?,
            before
        );
        drop(store);
        let (reopened, _) = open(&path, branch, false)?;
        assert_eq!(
            reopened
                .node_records_owned()
                .collect::<Result<Vec<_>, _>>()?,
            before
        );
        drop(reopened);
        std::fs::remove_dir_all(path)?;
        writeln!(
            stdout,
            "{}",
            json!({"rows":rows,"branch":branch,"precheckpoint":precheckpoint,"elapsed_ns":elapsed,"verified_nodes":before.len()})
        )?;
        stdout.flush()?;
    }
    std::fs::remove_dir(root)?;
    Ok(())
}
