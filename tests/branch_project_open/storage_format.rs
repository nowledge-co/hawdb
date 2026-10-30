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

use super::*;
use hawdb::StorageFormatError;
use hawdb_storage::append_table::{encode_append_wal_batch, AppendTransaction};
use hawdb_storage::relational::{
    encode_relational_checkpoint, encode_relational_wal_batch, MaterializedViewColumn,
    MaterializedViewDefinition, MaterializedViewExpression, RelationalColumnSchema,
    RelationalScalarType, RelationalState, RelationalTableSchema, RelationalTransaction,
    RelationalWrite,
};
use hawdb_storage::store::{DatabaseDoctor, GraphStore, WalDoctorOptions};
use hawdb_storage::wal::{WalCursorEvent, WalEntry, WalOp, WalOpenOutcome, WalRecordCursor};
use std::io::Write;
use std::path::Path;

fn view_transaction() -> RelationalTransaction {
    let base = RelationalTableSchema {
        name: "format_events".into(),
        columns: vec![RelationalColumnSchema {
            name: "id".into(),
            scalar_type: RelationalScalarType::BigInt,
            nullable: false,
            default: None,
        }],
        primary_key: vec!["id".into()],
        unique_constraints: vec![],
        foreign_keys: vec![],
        indexes: vec![],
        materialized_view: None,
    };
    let view = MaterializedViewDefinition {
        source: base.name.clone(),
        group_by: vec![],
        filter: None,
        columns: vec![MaterializedViewColumn {
            name: "compatibility_count".into(),
            expression: MaterializedViewExpression::Count(None),
        }],
    }
    .table_schema("format_stats".into(), &base)
    .unwrap();
    RelationalTransaction {
        writes: vec![
            RelationalWrite::CreateTable(base),
            RelationalWrite::CreateTable(view),
        ],
    }
}

fn unknown_expression(encoded: &mut [u8]) {
    let name = b"compatibility_count";
    let offset = encoded
        .windows(name.len())
        .rposition(|bytes| bytes == name)
        .unwrap()
        + name.len();
    assert_eq!(encoded[offset], 1);
    encoded[offset] = 255;
}

fn refresh_envelope_checksum(encoded: &mut [u8]) {
    let digest = hawdb_integrity::integrity_digest(&encoded[64..]);
    encoded[28..32].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[32..64].copy_from_slice(digest.sha256.as_bytes());
}

fn unsupported_record(kind: &str, epoch: u64) -> (WalOp, StorageFormatError) {
    let (mut encoded, format, supported) = match kind {
        "append" => (
            encode_append_wal_batch(epoch, &AppendTransaction::default()).unwrap(),
            "append WAL",
            3,
        ),
        "snapshot" => (
            encode_relational_checkpoint(epoch, &RelationalState::default()).unwrap(),
            "relational durable codec",
            2,
        ),
        _ => (
            encode_relational_wal_batch(epoch, &view_transaction()).unwrap(),
            "relational durable codec",
            2,
        ),
    };
    let expected = if kind == "feature" {
        unknown_expression(&mut encoded);
        refresh_envelope_checksum(&mut encoded);
        StorageFormatError::UnsupportedFeature {
            format: "relational schema",
            feature: "materialized view expression",
            tag: 255,
        }
    } else {
        encoded[8..10].copy_from_slice(&(supported + 1_u16).to_le_bytes());
        StorageFormatError::UnsupportedVersion {
            format,
            found: supported + 1,
            supported,
        }
    };
    let record = encoded.into();
    let op = match kind {
        "append" => WalOp::Append { record },
        "snapshot" => WalOp::RelationalSnapshot { record },
        _ => WalOp::Relational { record },
    };
    (WalOp::Batch(vec![op]), expected)
}

fn append_record(path: &Path, generation: u64, lsn: u64, epoch: u64, op: WalOp) {
    let payload =
        hawdb_storage::wal::binary::encode_binary_wal_record(&WalEntry { lsn, op }, epoch).unwrap();
    let position = std::fs::metadata(path).unwrap().len();
    let framed = hawdb_storage::wal::frame::frame_binary_wal_record(generation, &payload, position);
    let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(&framed).unwrap();
    file.write_all(b"torn").unwrap();
}

fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, output: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, output);
            } else {
                output.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut output = BTreeMap::new();
    visit(root, root, &mut output);
    output
}

fn seed_unpublished_recovery_evidence(path: &Path) {
    std::fs::write(
        path.join("relational.999999.hawdb"),
        b"unpublished future artifact",
    )
    .unwrap();
    let candidate = path.join(".checkpoint.999999.prepare");
    std::fs::create_dir(&candidate).unwrap();
    std::fs::write(candidate.join("evidence"), b"retain candidate evidence").unwrap();
}

fn assert_refusal(error: HawDBError, expected: &StorageFormatError) {
    assert_eq!(
        error,
        HawDBError::UnsupportedStorageFormat(expected.clone())
    );
}

#[test]
fn unsupported_active_wal_preserves_open_readiness_and_doctor_evidence() {
    for kind in ["relational", "append", "snapshot", "feature"] {
        let project = Project::new();
        let mut database = Database::open(&project.0).unwrap();
        database
            .query_sql("CREATE TABLE retained (id BIGINT PRIMARY KEY)")
            .unwrap();
        let main = main_id(&database);
        drop(database);
        let directory = project.0.join("branches").join(main.to_string());
        let head =
            hawdb_storage::branch_head::read_branch_head(&directory.join("branch.head")).unwrap();
        let objects = hawdb_storage::immutable_object::ImmutableObjectStore::open(
            project.0.join("branches/objects"),
        )
        .unwrap();
        let root = hawdb_storage::sealed_root::SealedRoot::decode(
            &objects.read(head.sealed_root).unwrap(),
        )
        .unwrap();
        let wal = directory.join(hawdb_storage::artifact_files::wal_generation_file(
            head.active_wal.generation,
        ));
        let WalOpenOutcome::Cursor(mut cursor) = WalRecordCursor::open(&wal, None).unwrap() else {
            panic!("valid WAL header")
        };
        let mut lsn = cursor.start_lsn();
        let mut epoch = root.commit_epoch;
        loop {
            match cursor.next().unwrap() {
                WalCursorEvent::Entry { .. } => {
                    lsn += 1;
                    epoch += 1;
                }
                WalCursorEvent::Eof => break,
                _ => panic!("unexpected damaged WAL fixture"),
            }
        }
        drop(cursor);
        let (op, expected) = unsupported_record(kind, epoch + 1);
        append_record(&wal, head.active_wal.generation, lsn, epoch + 1, op);
        let before = files(&project.0);
        for read_only in [false, true] {
            let mut database = Database::open_with_config(
                &project.0,
                DatabaseConfig {
                    read_only,
                    recovery_mode: hawdb_storage::config::RecoveryMode::Strict,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_refusal(
                database.query_sql("SELECT * FROM retained").unwrap_err(),
                &expected,
            );
            drop(database);
            assert_eq!(files(&project.0), before);
        }
        let options = hawdb::NowledgeMemOpenOptions::graph_only(
            &project.0,
            hawdb::NowledgeMemGraphMode::WritableCutover,
        );
        let Err(error) = hawdb::NowledgeMemEmbeddedStore::open_with_options(options) else {
            panic!("readiness accepted incompatible storage");
        };
        assert_refusal(error, &expected);
        assert_eq!(files(&project.0), before);
        assert_refusal(
            DatabaseDoctor::plan_wal_tail_repair(&project.0, WalDoctorOptions::default())
                .unwrap_err(),
            &expected,
        );
        assert_eq!(files(&project.0), before);
        let mut database = Database::open_with_config(
            &project.0,
            DatabaseConfig {
                recovery_mode: hawdb::RecoveryMode::AutoRepairTornTail,
                ..Default::default()
            },
        )
        .unwrap();
        assert_refusal(
            database.query_sql("SELECT * FROM retained").unwrap_err(),
            &expected,
        );
        drop(database);
        assert_eq!(files(&project.0), before);
    }
}

#[test]
fn unknown_checkpoint_expression_refuses_without_cleanup_or_repair() {
    let project = Project::new();
    let mut catalog = hawdb_core::Catalog::default();
    let mut store = GraphStore::open(&project.0, &mut catalog).unwrap();
    store
        .commit_relational_transaction(&mut catalog, view_transaction())
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let manifest_path = project.0.join("manifest.hawdb");
    let mut manifest =
        hawdb_storage::durable_manifest::DurableManifest::load(&manifest_path).unwrap();
    let binding = manifest
        .relational_row_generation_artifacts
        .as_mut()
        .unwrap();
    let path = project.0.join(
        hawdb_storage::relational::relational_row_page_manifest_generation_file(binding.generation),
    );
    let mut encoded = std::fs::read(&path).unwrap();
    unknown_expression(&mut encoded);
    let mut hasher = hawdb_integrity::IntegrityHasher::new();
    hasher.update(&encoded[..280]);
    hasher.update(&encoded[316..]);
    let digest = hasher.finish();
    encoded[280..284].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    encoded[284..316].copy_from_slice(digest.sha256.as_bytes());
    let digest = hawdb_integrity::integrity_digest(&encoded);
    binding.manifest_artifact.encoded_crc32c = digest.crc32c.get();
    binding.manifest_artifact.encoded_sha256 = digest.sha256;
    std::fs::write(path, encoded).unwrap();
    manifest.write(&manifest_path).unwrap();
    let wal = manifest.wal_path(&project.0);
    std::fs::OpenOptions::new()
        .append(true)
        .open(wal)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    seed_unpublished_recovery_evidence(&project.0);
    let before = files(&project.0);
    let expected = StorageFormatError::UnsupportedFeature {
        format: "relational schema",
        feature: "materialized view expression",
        tag: 255,
    };
    assert_refusal(
        DatabaseDoctor::plan_wal_tail_repair(&project.0, WalDoctorOptions::default()).unwrap_err(),
        &expected,
    );
    assert_eq!(files(&project.0), before);
    for read_only in [false, true] {
        assert_refusal(
            Database::open_with_config(
                &project.0,
                DatabaseConfig {
                    read_only,
                    recovery_mode: hawdb_storage::config::RecoveryMode::Strict,
                    ..Default::default()
                },
            )
            .unwrap_err(),
            &expected,
        );
        assert_eq!(files(&project.0), before);
    }
}

#[test]
fn unsupported_full_checkpoint_preserves_recovery_files() {
    for feature in [false, true] {
        let project = Project::new();
        let mut catalog = hawdb_core::Catalog::default();
        let mut store = GraphStore::open(&project.0, &mut catalog).unwrap();
        store
            .commit_relational_transaction(&mut catalog, view_transaction())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        drop(store);
        let manifest_path = project.0.join("manifest.hawdb");
        let mut manifest =
            hawdb_storage::durable_manifest::DurableManifest::load(&manifest_path).unwrap();
        let path = project.0.join(
            hawdb_storage::artifact_files::relational_checkpoint_generation_file(
                manifest.checkpoint_generation.unwrap(),
            ),
        );
        let mut encoded = std::fs::read(&path).unwrap();
        if feature {
            unknown_expression(&mut encoded);
            refresh_envelope_checksum(&mut encoded);
        } else {
            encoded[8..10].copy_from_slice(&3_u16.to_le_bytes());
        }
        let digest = hawdb_integrity::integrity_digest(&encoded);
        std::fs::write(path, encoded).unwrap();
        let checkpoint_path = manifest.checkpoint_path(&project.0);
        let text = hawdb_storage::store::read_durable_text(&checkpoint_path, "checkpoint").unwrap();
        let (body, _) = hawdb_storage::checkpoint::split_checkpoint_checksum(&text).unwrap();
        let mut updated = String::new();
        for line in body.lines() {
            if line.starts_with("relational_checkpoint_encoded_checksum\t") {
                updated.push_str(&format!(
                    "relational_checkpoint_encoded_checksum\t{}\n",
                    digest.crc32c.get()
                ));
            } else if line.starts_with("relational_checkpoint_encoded_sha256\t") {
                updated.push_str(&format!(
                    "relational_checkpoint_encoded_sha256\t{}\n",
                    digest.sha256
                ));
            } else {
                updated.push_str(line);
                updated.push('\n');
            }
        }
        updated.push_str(&format!(
            "checksum\t{}\n",
            hawdb_integrity::checksum_u64(updated.as_bytes())
        ));
        let encoded =
            hawdb_storage::store::encode_durable_text(&updated, Default::default()).unwrap();
        let digest = hawdb_integrity::integrity_digest(&encoded);
        manifest.checkpoint_encoded_len = Some(encoded.len() as u64);
        manifest.checkpoint_encoded_checksum = Some(u64::from(digest.crc32c.get()));
        manifest.checkpoint_encoded_sha256 = Some(digest.sha256);
        std::fs::write(checkpoint_path, encoded).unwrap();
        manifest.write(&manifest_path).unwrap();
        let wal = manifest.wal_path(&project.0);
        std::fs::OpenOptions::new()
            .append(true)
            .open(wal)
            .unwrap()
            .write_all(b"torn")
            .unwrap();
        seed_unpublished_recovery_evidence(&project.0);
        let before = files(&project.0);
        let expected = if feature {
            StorageFormatError::UnsupportedFeature {
                format: "relational schema",
                feature: "materialized view expression",
                tag: 255,
            }
        } else {
            StorageFormatError::UnsupportedVersion {
                format: "relational durable codec",
                found: 3,
                supported: 2,
            }
        };
        assert_refusal(
            DatabaseDoctor::plan_wal_tail_repair(&project.0, WalDoctorOptions::default())
                .unwrap_err(),
            &expected,
        );
        assert_eq!(files(&project.0), before);
        assert_refusal(Database::open(&project.0).unwrap_err(), &expected);
        assert_eq!(files(&project.0), before);
    }
}
