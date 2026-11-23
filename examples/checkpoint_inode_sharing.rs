// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Manual checkpoint footprint probe; always pass a new scratch directory.

use hawdb::{Database, Value};
use serde_json::json;
use std::collections::BTreeMap;
#[cfg(unix)]
use std::collections::BTreeSet;
use std::path::Path;

fn footprint(root: &Path) -> std::io::Result<serde_json::Value> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = 0_u64;
    let mut logical_bytes = 0_u64;
    #[cfg(unix)]
    let (mut unique_inode_bytes, mut unique_allocated_bytes, mut identities) =
        (0_u64, 0_u64, BTreeSet::new());
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let path = entry?.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            if metadata.is_dir() {
                pending.push(path);
            } else if metadata.is_file() {
                files += 1;
                logical_bytes += metadata.len();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::MetadataExt;
                    if identities.insert((metadata.dev(), metadata.ino())) {
                        unique_inode_bytes += metadata.len();
                        unique_allocated_bytes += metadata.blocks() * 512;
                    }
                }
            }
        }
    }
    let report = json!({"files":files,"logical_bytes":logical_bytes,"unique_inodes":null,"unique_inode_bytes":null,"unique_allocated_bytes":null});
    #[cfg(unix)]
    let report = {
        let mut report = report;
        report["unique_inodes"] = json!(identities.len());
        report["unique_inode_bytes"] = json!(unique_inode_bytes);
        report["unique_allocated_bytes"] = json!(unique_allocated_bytes);
        report
    };
    Ok(report)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args_os()
        .nth(1)
        .ok_or("usage: checkpoint_inode_sharing <new-root>")?;
    let root = Path::new(&root);
    if root.exists() {
        return Err("probe refuses an existing directory".into());
    }
    let mut database = Database::open(root)?;
    for id in 0..1000 {
        database.query_with_params(
            "CREATE (:Note {id: $id, name: $name, body: $body})",
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("name".into(), Value::String(format!("note-{id}"))),
                ("body".into(), Value::String("x".repeat(256))),
            ]),
        )?;
    }
    for round in 1..=3 {
        if round == 3 {
            database.query("CREATE (:Note {id: 1000, name: 'last', body: 'last'})")?;
        }
        database.checkpoint()?;
        println!("{}", json!({"round":round,"footprint":footprint(root)?}));
    }
    drop(database);
    let mut reopened = Database::open(root)?;
    let result = reopened.query("MATCH (n:Note) RETURN count(n) AS count")?;
    assert_eq!(result.rows[0]["count"], Value::Int(1001));
    println!(
        "{}",
        json!({"reopened_nodes":1001,"footprint":footprint(root)?})
    );
    Ok(())
}
