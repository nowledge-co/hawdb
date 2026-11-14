// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Same-process reopen skips identity re-hashes of checkpoint objects that
//! this process published; cold opens and every remaining check still verify.

use super::*;
use hawdb_storage::immutable_object::{forget_published_object, published_object_skipped_rehashes};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const GRAPH: &str = "MATCH (a:Item)-[r:NEXT]->(b:Item) \
                     RETURN a.id AS a, a.label AS label, r.w AS w, b.id AS b ORDER BY a ASC";
const ROWS: &str = "SELECT id, v FROM public.items ORDER BY id";

fn populated(name: &str) -> PathBuf {
    let path = unique_test_dir(name);
    let mut db = Database::open(&path).unwrap();
    let items = (1..=300)
        .map(|id| {
            Value::Map(BTreeMap::from([
                ("id".into(), Value::Int(id)),
                ("label".into(), Value::String(format!("item-{id}"))),
            ]))
        })
        .collect();
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {id: r.id, label: r.label})",
        &BTreeMap::from([("rows".into(), Value::List(items))]),
    )
    .unwrap();
    for id in 1..300 {
        db.query(&format!(
            "MATCH (a:Item {{id: {id}}}), (b:Item {{id: {}}}) CREATE (a)-[:NEXT {{w: {}}}]->(b)",
            id + 1,
            id * 3
        ))
        .unwrap();
    }
    db.query_sql("CREATE TABLE public.items (id BIGINT PRIMARY KEY, v TEXT NOT NULL)")
        .unwrap();
    for id in 0..200 {
        db.query_sql(&format!(
            "INSERT INTO public.items VALUES ({id}, 'value-{id}')"
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
    path
}

fn object_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let objects = root.join("branches/objects/objects");
    for kind in std::fs::read_dir(objects).unwrap() {
        let kind = kind.unwrap().path();
        if !kind.is_dir() || kind.file_name().is_some_and(|name| name == ".staging") {
            continue;
        }
        for object in std::fs::read_dir(kind).unwrap() {
            files.push(object.unwrap().path());
        }
    }
    files.sort();
    files
}

fn skips(root: &Path) -> Vec<(PathBuf, Option<u64>)> {
    object_files(root)
        .into_iter()
        .map(|path| {
            let skipped = published_object_skipped_rehashes(&path).unwrap();
            (path, skipped)
        })
        .collect()
}

fn results(db: &mut Database) -> Results {
    (
        db.query(GRAPH).unwrap().rows,
        db.query_sql(ROWS).unwrap().rows,
    )
}

type Results = (hawdb_executor::QueryRows, hawdb_executor::QueryRows);

/// Reopens once in this process and returns the largest checkpoint object
/// whose identity re-hash that reopen skipped, with the clean results.
fn reopen_and_pick_skipped_object(path: &Path) -> (PathBuf, Results) {
    let before = skips(path);
    let expected = {
        // Branch admission (and its mount) runs on the first statement.
        let mut db = Database::open(path).unwrap();
        results(&mut db)
    };
    let object = skips(path)
        .into_iter()
        .filter(|(object, skipped)| {
            let previous = before
                .iter()
                .find(|(candidate, _)| candidate == object)
                .and_then(|(_, skipped)| *skipped)
                .unwrap_or(0);
            object.to_string_lossy().contains("checkpoint-artifact")
                && skipped.is_some_and(|skipped| skipped > previous)
        })
        .max_by_key(|(object, _)| std::fs::metadata(object).unwrap().len())
        .map(|(object, _)| object)
        .expect("same-process reopen skips at least one checkpoint artifact");
    (object, expected)
}

/// Flips one byte in the middle of `path` in place: same inode and length.
fn flip_middle_byte(path: &Path, modified: Option<SystemTime>) {
    let original = std::fs::metadata(path).unwrap().modified().unwrap();
    let length = std::fs::metadata(path).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut byte = [0_u8];
    file.seek(SeekFrom::Start(length / 2)).unwrap();
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xa5;
    file.seek(SeekFrom::Start(length / 2)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
    file.set_times(std::fs::FileTimes::new().set_modified(modified.unwrap_or(original)))
        .unwrap();
}

/// Opens, reads and finally scrubs; returns the first error. Any read that
/// succeeds must return exactly the clean results: damage is never served.
fn first_detection(path: &Path, expected: &Results) -> HawDBError {
    let mut db = match Database::open(path) {
        Ok(db) => db,
        Err(error) => return error,
    };
    match db.query(GRAPH) {
        Ok(output) => assert_eq!(output.rows, expected.0),
        Err(error) => return error,
    }
    match db.query_sql(ROWS) {
        Ok(output) => assert_eq!(output.rows, expected.1),
        Err(error) => return error,
    }
    db.scrub_storage()
        .expect_err("corruption must be detected by open, a read or scrub")
}

#[test]
fn same_process_reopen_skips_published_rehash_and_cold_open_verifies() {
    let path = populated("published_reopen");
    let expected = {
        let mut db = Database::open(&path).unwrap();
        results(&mut db)
    };
    assert_eq!(expected.0.len(), 299);
    assert_eq!(expected.1.len(), 200);
    let recorded = skips(&path);
    assert!(recorded.iter().any(|(_, skipped)| skipped.is_some()));

    // Same process: identical results, and published objects were not re-hashed.
    let before: u64 = recorded.iter().filter_map(|(_, skipped)| *skipped).sum();
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(results(&mut db), expected);
    }
    let after: u64 = skips(&path)
        .iter()
        .filter_map(|(_, skipped)| *skipped)
        .sum();
    assert!(after > before, "skipped {before} -> {after}");

    // Cold (as in a new process): no record, so nothing is skipped.
    for object in object_files(&path) {
        forget_published_object(&object).unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(results(&mut db), expected);
        let scrub = db.scrub_storage().unwrap();
        assert!(scrub.sha256_verified_file_count > 0);
    }
    for (object, skipped) in skips(&path) {
        assert_eq!(skipped, None, "{}", object.display());
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn modified_published_object_is_rehashed_on_same_process_reopen() {
    let path = populated("published_modified");
    let (object, expected) = reopen_and_pick_skipped_object(&path);
    let later = std::fs::metadata(&object).unwrap().modified().unwrap() + Duration::from_secs(1);
    flip_middle_byte(&object, Some(later));
    // The changed modification time disqualifies the record.
    assert_eq!(published_object_skipped_rehashes(&object).unwrap(), None);

    let error = first_detection(&path, &expected);
    assert!(
        matches!(error, HawDBError::StorageIntegrity(_)),
        "unexpected error: {error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn mtime_preserving_corruption_is_caught_by_remaining_checks_or_cold_open() {
    let path = populated("published_mtime_preserved");
    let (object, expected) = reopen_and_pick_skipped_object(&path);
    flip_middle_byte(&object, None);

    // The record still matches, so the identity re-hash is skipped. Block and
    // page checks on reads, or the explicit scrub, detect the damage. (Here
    // the flipped adjacency descriptor page is not read by either query, so
    // both return clean results and `scrub_storage` reports the CRC32C.)
    let skipped = published_object_skipped_rehashes(&object).unwrap();
    let error = first_detection(&path, &expected);
    assert!(
        published_object_skipped_rehashes(&object).unwrap() > skipped,
        "the same-process reopen took the skip path"
    );
    let message = error.to_string();
    assert!(
        message.contains("checksum")
            || message.contains("CRC32C")
            || message.contains("SHA-256")
            || message.contains("digest")
            || message.contains("corrupt"),
        "unexpected error: {message}"
    );

    // The diagnostic check ignores the record, even in this same process, and
    // reports the damage instead of declaring the artifact healthy.
    assert!(published_object_skipped_rehashes(&object)
        .unwrap()
        .is_some());
    let health = hawdb_storage::store::doctor::DatabaseDoctor::derived_artifact_health(
        &path,
        hawdb_storage::derived_repair::DerivedArtifactRebuildOptions::default(),
    );
    assert!(
        health
            .as_ref()
            .map_or(true, |health| health.repair_required),
        "derived-artifact health must not accept mtime-preserving damage: {health:?}"
    );

    // A cold open (no record) fails during branch admission itself.
    forget_published_object(&object).unwrap();
    let cold = Database::open(&path)
        .and_then(|mut db| db.query(GRAPH).map(|_| ()))
        .unwrap_err();
    assert!(
        matches!(cold, HawDBError::StorageIntegrity(_)),
        "unexpected cold error: {cold}"
    );
    std::fs::remove_dir_all(path).unwrap();
}

/// Every object whose re-hash a same-process reopen skipped, damaged in turn
/// with its modification time preserved, is either refused by a format
/// checksum on open/read or reported by `scrub_storage`; reads that succeed
/// return exactly the clean results.
#[test]
fn every_skipped_object_corruption_is_refused_or_scrubbed() {
    let path = populated("published_sweep");
    let before = skips(&path);
    let expected = {
        let mut db = Database::open(&path).unwrap();
        results(&mut db)
    };
    let skipped = skips(&path)
        .into_iter()
        .filter(|(object, skipped)| {
            let previous = before
                .iter()
                .find(|(candidate, _)| candidate == object)
                .and_then(|(_, skipped)| *skipped)
                .unwrap_or(0);
            skipped.is_some_and(|skipped| skipped > previous)
        })
        .map(|(object, _)| object)
        .filter(|object| std::fs::metadata(object).unwrap().len() > 0)
        .collect::<Vec<_>>();
    assert!(skipped.len() >= 10, "skipped {} objects", skipped.len());
    for object in &skipped {
        flip_middle_byte(object, None);
        let detected = first_detection(&path, &expected);
        assert!(
            matches!(
                detected,
                HawDBError::Storage(_) | HawDBError::StorageIntegrity(_)
            ),
            "{}: {detected}",
            object.display()
        );
        // Flipping back restores the clean bytes and modification time.
        flip_middle_byte(object, None);
    }
    std::fs::remove_dir_all(path).unwrap();
}

/// Object files the single branch mounts: those sharing an inode with a
/// branch `data` alias, with their lengths.
#[cfg(unix)]
fn mounted_objects(root: &Path) -> BTreeMap<PathBuf, u64> {
    use std::os::unix::fs::MetadataExt;
    let identity = |metadata: &std::fs::Metadata| (metadata.dev(), metadata.ino());
    let mut aliases = std::collections::BTreeSet::new();
    for branch in std::fs::read_dir(root.join("branches")).unwrap() {
        let Ok(entries) = std::fs::read_dir(branch.unwrap().path().join("data")) else {
            continue;
        };
        for alias in entries {
            aliases.insert(identity(&std::fs::metadata(alias.unwrap().path()).unwrap()));
        }
    }
    let mounted = object_files(root)
        .into_iter()
        .filter_map(|object| {
            let metadata = std::fs::metadata(&object).unwrap();
            aliases
                .contains(&identity(&metadata))
                .then_some((object, metadata.len()))
        })
        .collect::<BTreeMap<_, _>>();
    assert!(!mounted.is_empty(), "the branch mounts checkpoint objects");
    mounted
}

/// #1440: a new-process open (no publication records) hashes every mounted
/// checkpoint object exactly once. Admission's first cold handle open of each
/// mounted object reuses the mount's verification instead of hashing the same
/// inode again; results are identical and cold-open damage is still refused
/// at branch admission.
#[cfg(unix)]
#[test]
fn new_process_open_hashes_each_mounted_object_once() {
    let path = populated("mount_verification_reuse");
    let expected = {
        let mut db = Database::open(&path).unwrap();
        results(&mut db)
    };
    let forget_all = |path: &Path| {
        for object in object_files(path) {
            forget_published_object(&object).unwrap();
        }
    };
    forget_all(&path);
    {
        let mut db = Database::open(&path).unwrap();
        // Branch admission runs on the first statement.
        assert_eq!(results(&mut db), expected);
        let metrics = db.file_descriptor_metrics().unwrap();
        assert_eq!(
            metrics.identity_verified_bytes,
            mounted_objects(&path).values().sum::<u64>(),
            "each mounted object is hashed once, not again on its first cold open: {metrics:?}"
        );
        assert!(metrics.mount_verification_reuses > 0, "{metrics:?}");
    }
    for (object, skipped) in skips(&path) {
        assert_eq!(skipped, None, "{}", object.display());
    }

    // Damage that keeps inode, length and modification time is still refused
    // by the mount's full hash when a new process admits the branch.
    let (object, _) = mounted_objects(&path)
        .into_iter()
        .max_by_key(|(_, length)| *length)
        .unwrap();
    flip_middle_byte(&object, None);
    forget_all(&path);
    let error = Database::open(&path)
        .and_then(|mut db| db.query(GRAPH).map(|_| ()))
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::StorageIntegrity(_)),
        "unexpected cold error: {error}"
    );
    std::fs::remove_dir_all(path).unwrap();
}
