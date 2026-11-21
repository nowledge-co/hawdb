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
use crate::relational::{
    MaterializedViewColumn, MaterializedViewDefinition, MaterializedViewExpression,
};
use hawdb_core::StorageFormatError;

fn assert_unsupported(error: RelationalError, expected: &StorageFormatError) {
    assert!(
        matches!(error, RelationalError::UnsupportedFormat(ref actual) if actual == expected),
        "{error:?}"
    );
}

fn verify_checkpoint(encoded: &[u8], expected: &StorageFormatError) {
    let limits = RelationalDecodeLimits::checkpoint();
    assert_unsupported(
        decode_relational_checkpoint(encoded, limits).unwrap_err(),
        expected,
    );
    let path = std::env::temp_dir().join(format!(
        "hawdb-format-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::write(&path, encoded).unwrap();
    assert_unsupported(
        validate_relational_checkpoint_file_format(&path).unwrap_err(),
        expected,
    );
    assert_unsupported(
        decode_relational_checkpoint_file(&path, limits).unwrap_err(),
        expected,
    );
    let work =
        crate::background::CheckpointWorkContext::new(hawdb_core::RuntimeTaskContext::default());
    assert_unsupported(
        decode_relational_checkpoint_file_with_work_context(
            &path,
            limits,
            RelationalCheckpointIndexLoad::MaterializedPostings,
            &work,
        )
        .unwrap_err(),
        expected,
    );
    let mut corrupt = encoded.to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    std::fs::write(&path, &corrupt).unwrap();
    assert!(matches!(
        validate_relational_checkpoint_file_format(&path),
        Err(RelationalError::Corruption(_))
    ));
    assert!(matches!(
        decode_relational_checkpoint(&corrupt, limits),
        Err(RelationalError::Corruption(_))
    ));
    assert!(matches!(
        decode_relational_checkpoint_file(&path, limits),
        Err(RelationalError::Corruption(_))
    ));
    assert!(matches!(
        decode_relational_checkpoint_file_with_work_context(
            &path,
            limits,
            RelationalCheckpointIndexLoad::MaterializedPostings,
            &work,
        ),
        Err(RelationalError::Corruption(_))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), corrupt);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn intact_unsupported_versions_are_distinct_from_integrity_failures() {
    for version in [1_u16, 2, CODEC_VERSION + 1, u16::MAX] {
        let expected = StorageFormatError::UnsupportedVersion {
            format: "relational durable codec",
            found: version,
            supported: CODEC_VERSION,
        };
        let mut wal = encode_relational_wal_batch(1, &RelationalTransaction::default()).unwrap();
        wal[8..10].copy_from_slice(&version.to_le_bytes());
        assert_unsupported(
            decode_relational_wal_batch(&wal, RelationalDecodeLimits::wal()).unwrap_err(),
            &expected,
        );
        let last = wal.len() - 1;
        wal[last] ^= 1;
        assert!(matches!(
            decode_relational_wal_batch(&wal, RelationalDecodeLimits::wal()),
            Err(RelationalError::Corruption(_))
        ));
        let mut checkpoint = encode_relational_checkpoint(0, &RelationalState::default()).unwrap();
        checkpoint[8..10].copy_from_slice(&version.to_le_bytes());
        verify_checkpoint(&checkpoint, &expected);
    }
    let mut wal = encode_relational_wal_batch(1, &RelationalTransaction::default()).unwrap();
    wal[10] = 1;
    assert!(matches!(
        decode_relational_wal_batch(&wal, RelationalDecodeLimits::wal()),
        Err(RelationalError::Corruption(_))
    ));
}

fn downgrade_full_index_record_to_v3(
    mut record: Vec<u8>,
    schema: &RelationalTableSchema,
) -> Vec<u8> {
    let current = encode_relational_table_schema(schema).unwrap();
    let legacy =
        encode_relational_table_schema_for_layout(schema, IndexSchemaLayout::Full).unwrap();
    assert_eq!(current.len(), legacy.len() + 5);
    let offset = record[HEADER_BYTES..]
        .windows(current.len())
        .position(|candidate| candidate == current)
        .map(|offset| HEADER_BYTES + offset)
        .expect("current schema bytes in durable record");
    record.splice(offset..offset + current.len(), legacy);
    record[8..10].copy_from_slice(&3_u16.to_le_bytes());
    let payload_len = u64::try_from(record.len() - HEADER_BYTES).unwrap();
    record[20..28].copy_from_slice(&payload_len.to_le_bytes());
    let digest = integrity_digest(&record[HEADER_BYTES..]);
    record[28..32].copy_from_slice(&digest.crc32c.get().to_le_bytes());
    record[32..64].copy_from_slice(digest.sha256.as_bytes());
    record
}

#[test]
fn version_3_wal_and_checkpoint_decode_full_named_indexes() {
    let schema = RelationalTableSchema {
        name: "documents".into(),
        columns: vec![
            RelationalColumnSchema {
                name: "id".into(),
                scalar_type: RelationalScalarType::BigInt,
                nullable: false,
                default: None,
            },
            RelationalColumnSchema {
                name: "body".into(),
                scalar_type: RelationalScalarType::Text,
                nullable: false,
                default: None,
            },
        ],
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: vec![RelationalIndexSchema {
            included_columns: Vec::new(),
            name: "documents_body_idx".into(),
            columns: vec!["body".into()],
            unique: false,
            predicate: None,
        }],
        materialized_view: None,
    };
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::CreateTable(schema.clone()),
            RelationalWrite::Insert {
                table: schema.name.clone(),
                rows: vec![RelationalRow::new(vec![
                    RelationalValue::BigInt(1),
                    RelationalValue::Text("one".into()),
                ])],
                mode: RelationalInsertMode::Error,
            },
        ],
    };
    let state = RelationalState::default()
        .stage_transaction(transaction.clone(), Default::default(), Default::default())
        .unwrap();

    let wal = downgrade_full_index_record_to_v3(
        encode_relational_wal_batch(7, &transaction).unwrap(),
        &schema,
    );
    let decoded = decode_relational_wal_batch(&wal, RelationalDecodeLimits::wal()).unwrap();
    assert_eq!(decoded.epoch, 7);
    assert_eq!(decoded.transaction, transaction);

    let checkpoint = downgrade_full_index_record_to_v3(
        encode_relational_checkpoint(7, &state).unwrap(),
        &schema,
    );
    let limits = RelationalDecodeLimits::checkpoint();
    let decoded = decode_relational_checkpoint(&checkpoint, limits).unwrap();
    assert_eq!(decoded.epoch, 7);
    assert_eq!(decoded.state.table_schema("documents"), Some(&schema));
    assert_eq!(decoded.state.row_count("documents"), 1);

    let path = std::env::temp_dir().join(format!(
        "hawdb-v3-full-index-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::write(&path, &checkpoint).unwrap();
    validate_relational_checkpoint_file_format(&path).unwrap();
    let decoded = decode_relational_checkpoint_file(&path, limits).unwrap();
    assert_eq!(decoded.state.table_schema("documents"), Some(&schema));
    let work =
        crate::background::CheckpointWorkContext::new(hawdb_core::RuntimeTaskContext::default());
    let decoded = decode_relational_checkpoint_file_with_work_context(
        &path,
        limits,
        RelationalCheckpointIndexLoad::MaterializedPostings,
        &work,
    )
    .unwrap();
    assert_eq!(decoded.state.table_schema("documents"), Some(&schema));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn legacy_standalone_schema_decodes_declared_indexes_as_full_indexes() {
    let schema = RelationalTableSchema {
        name: "documents".into(),
        columns: vec![RelationalColumnSchema {
            name: "id".into(),
            scalar_type: RelationalScalarType::BigInt,
            nullable: false,
            default: None,
        }],
        primary_key: vec!["id".into()],
        unique_constraints: Vec::new(),
        foreign_keys: Vec::new(),
        indexes: vec![RelationalIndexSchema {
            included_columns: Vec::new(),
            name: "documents_id".into(),
            columns: vec!["id".into()],
            unique: false,
            predicate: None,
        }],
        materialized_view: None,
    };
    let legacy =
        encode_relational_table_schema_for_layout(&schema, IndexSchemaLayout::Full).unwrap();

    assert_eq!(
        decode_relational_table_schema_for_layout(
            &legacy,
            legacy.len(),
            64,
            IndexSchemaLayout::Full
        )
        .unwrap(),
        schema
    );
    assert!(decode_relational_table_schema(&legacy, legacy.len(), 64).is_err());

    let work =
        crate::background::CheckpointWorkContext::new(hawdb_core::RuntimeTaskContext::default());
    assert_eq!(
        decode_relational_table_schema_with_layout_and_work(
            &legacy,
            IndexSchemaLayout::Full,
            legacy.len(),
            64,
            &work
        )
        .unwrap(),
        schema
    );
}

#[test]
fn intact_unknown_view_expression_is_not_corruption() {
    let base = RelationalTableSchema {
        name: "events".into(),
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
    let definition = MaterializedViewDefinition {
        source: "events".into(),
        columns: vec![MaterializedViewColumn {
            name: "compatibility_count".into(),
            expression: MaterializedViewExpression::Count(None),
        }],
        group_by: vec![],
        filter: None,
        search: None,
    };
    let view = definition.table_schema("stats".into(), &base).unwrap();
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::CreateTable(base),
            RelationalWrite::CreateTable(view),
        ],
    };
    let state = RelationalState::default()
        .stage_transaction(transaction.clone(), Default::default(), Default::default())
        .unwrap();
    let expected = StorageFormatError::UnsupportedFeature {
        format: "relational schema",
        feature: "materialized view expression",
        tag: 255,
    };
    let mutate = |encoded: &mut Vec<u8>| {
        let name = b"compatibility_count";
        let offset = encoded
            .windows(name.len())
            .rposition(|bytes| bytes == name)
            .unwrap()
            + name.len();
        assert_eq!(encoded[offset], 1);
        encoded[offset] = 255;
        let digest = integrity_digest(&encoded[HEADER_BYTES..]);
        encoded[28..32].copy_from_slice(&digest.crc32c.get().to_le_bytes());
        encoded[32..64].copy_from_slice(digest.sha256.as_bytes());
    };
    let mut wal = encode_relational_wal_batch(1, &transaction).unwrap();
    mutate(&mut wal);
    assert_unsupported(
        decode_relational_wal_batch(&wal, RelationalDecodeLimits::wal()).unwrap_err(),
        &expected,
    );
    let mut checkpoint = encode_relational_checkpoint(1, &state).unwrap();
    mutate(&mut checkpoint);
    verify_checkpoint(&checkpoint, &expected);
}

#[test]
fn search_index_definitions_round_trip_and_refuse_unknown_analyzers_and_layouts() {
    use crate::relational::{SearchIndexAnalyzer, SearchIndexSpec};
    let column = |name: &str, scalar_type| RelationalColumnSchema {
        name: name.into(),
        scalar_type,
        nullable: true,
        default: None,
    };
    let base = RelationalTableSchema {
        name: "events".into(),
        columns: vec![
            RelationalColumnSchema {
                nullable: false,
                ..column("id", RelationalScalarType::BigInt)
            },
            column("search_body_column", RelationalScalarType::Text),
        ],
        primary_key: vec!["id".into()],
        unique_constraints: vec![],
        foreign_keys: vec![],
        indexes: vec![],
        materialized_view: None,
    };
    let index = MaterializedViewDefinition {
        source: "events".into(),
        columns: vec![],
        group_by: vec![],
        filter: None,
        search: Some(SearchIndexSpec {
            column: "search_body_column".into(),
            analyzer: SearchIndexAnalyzer::CjkNgram,
        }),
    }
    .table_schema("events_text".into(), &base)
    .unwrap();
    let transaction = RelationalTransaction {
        writes: vec![
            RelationalWrite::CreateTable(base),
            RelationalWrite::CreateTable(index.clone()),
            RelationalWrite::Insert {
                table: "events".into(),
                rows: vec![RelationalRow::new(vec![
                    RelationalValue::BigInt(1),
                    RelationalValue::Text("数据库 search".into()),
                ])],
                mode: RelationalInsertMode::Error,
            },
        ],
    };
    let state = RelationalState::default()
        .stage_transaction(transaction.clone(), Default::default(), Default::default())
        .unwrap();
    // Six term dictionary rows (数, 数据, 据, 据库, 库, search) with inline
    // postings, one document row, one lengths row and the statistics row.
    assert_eq!(state.row_count("events_text"), 9);
    let wal = encode_relational_wal_batch(1, &transaction).unwrap();
    assert_eq!(
        decode_relational_wal_batch(&wal, RelationalDecodeLimits::wal())
            .unwrap()
            .transaction,
        transaction
    );
    let checkpoint = encode_relational_checkpoint(1, &state).unwrap();
    let decoded = decode_relational_checkpoint(&checkpoint, RelationalDecodeLimits::checkpoint())
        .unwrap()
        .state;
    assert_eq!(decoded.table_schema("events_text"), Some(&index));
    assert_eq!(
        decoded.rows("events_text").collect::<Vec<_>>(),
        state.rows("events_text").collect::<Vec<_>>()
    );
    // The analyzer tag, then the physical layout tag, follow the column name.
    for (position, feature, tag) in [
        (0, "search index analyzer", 1),
        (1, "search index layout", 3),
    ] {
        let expected = StorageFormatError::UnsupportedFeature {
            format: "relational schema",
            feature,
            tag: 255,
        };
        let mutate = |encoded: &mut Vec<u8>| {
            let name = b"search_body_column";
            let offset = encoded
                .windows(name.len())
                .rposition(|bytes| bytes == name)
                .unwrap()
                + name.len()
                + position;
            assert_eq!(encoded[offset], tag);
            encoded[offset] = 255;
            let digest = integrity_digest(&encoded[HEADER_BYTES..]);
            encoded[28..32].copy_from_slice(&digest.crc32c.get().to_le_bytes());
            encoded[32..64].copy_from_slice(digest.sha256.as_bytes());
        };
        let mut wal = wal.clone();
        mutate(&mut wal);
        assert_unsupported(
            decode_relational_wal_batch(&wal, RelationalDecodeLimits::wal()).unwrap_err(),
            &expected,
        );
        let mut checkpoint = checkpoint.clone();
        mutate(&mut checkpoint);
        verify_checkpoint(&checkpoint, &expected);
    }
}
