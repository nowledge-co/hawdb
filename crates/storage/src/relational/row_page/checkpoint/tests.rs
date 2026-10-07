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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler, WorkClass, WorkRequest};
use std::sync::{atomic::Ordering, Arc};

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..LocalQosPolicy::default()
    })
}

fn limits() -> RelationalRowPageLimits {
    RelationalRowPageLimits {
        max_page_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
        max_rows: NonZeroUsize::new(2048).unwrap(),
        max_columns: NonZeroUsize::new(2048).unwrap(),
        max_key_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        max_row_bytes: NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
        max_inline_value_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        max_value_bytes: NonZeroUsize::new(2 * 1024 * 1024).unwrap(),
        ..RelationalRowPageLimits::default()
    }
}

fn page(rows: usize, columns: usize, payload: usize) -> ImmutableRelationalRowPage {
    ImmutableRelationalRowPage {
        generation: 2,
        source_commit_epoch: 11,
        page_id: RelationalRowPageId::new(NonZeroU64::new(3).unwrap()),
        schema_digest: hawdb_integrity::integrity_digest(b"checkpoint-page-schema").sha256,
        column_count: columns,
        rows: (0..rows)
            .map(|id| RelationalRowPageEntry {
                primary_key: RelationalKey(vec![RelationalValue::BigInt(id as i64)]),
                row: RelationalRow::new(
                    (0..columns)
                        .map(|column| match column % 8 {
                            0 => RelationalValue::Null,
                            1 => RelationalValue::Boolean(id % 2 == 0),
                            2 => RelationalValue::BigInt(id as i64 - 9),
                            3 => RelationalValue::DoublePrecision(if id % 2 == 0 {
                                -0.0
                            } else {
                                f64::NAN
                            }),
                            4 => RelationalValue::Text("界\0é".repeat(payload)),
                            5 => RelationalValue::Bytea(
                                (0..payload).map(|index| (index % 251) as u8).collect(),
                            ),
                            6 => RelationalValue::Uuid(hawdb_core::Uuid::from_u128(id as u128 + 1)),
                            _ => RelationalValue::Overflow(RelationalOverflowRef {
                                digest: hawdb_integrity::integrity_digest(b"full overflow digest")
                                    .sha256,
                                scalar_type: RelationalScalarType::Text,
                                compressed_bytes: 31,
                                uncompressed_bytes: 4096,
                            }),
                        })
                        .collect(),
                ),
            })
            .collect(),
    }
}

#[test]
fn checkpoint_units_row_page_codec_matches_complete_bytes_and_every_value_for_wide_pages() {
    for mut page in [
        page(1025, 8, 17),
        page(1, 1025, 101),
        page(2, 8, 32 * 1024 + 3),
    ] {
        if page.rows.len() == 2 {
            for (id, entry) in page.rows.iter_mut().enumerate() {
                entry.primary_key = RelationalKey(vec![RelationalValue::Text(format!(
                    "{}{}",
                    "shared-prefix-界".repeat(8193),
                    id
                ))]);
            }
        }
        let source = page.clone();
        let ordinary = page.encode(limits()).unwrap();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = encode(&page, limits(), &probe.context(local.clone())).unwrap();
        assert_eq!(actual, ordinary);
        assert_eq!(
            ImmutableRelationalRowPage::decode(&actual, limits()).unwrap(),
            source
        );
        assert_eq!(page, source);
        probe.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_row_page_codec_cancel_and_deny_every_actual_unit_preserves_source_and_retries()
{
    let page = page(2, 8, 11 * 1024 + 1);
    let source = page.clone();
    let expected = page.encode(limits()).unwrap();
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        encode(&page, limits(), &baseline.context(local.clone())).unwrap(),
        expected
    );
    let count = baseline.completed.load(Ordering::SeqCst);
    assert!(count > 100);
    baseline.assert_released(&local);
    for limit in 1..=count {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = encode(&page, limits(), &probe.context(local.clone())).unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        probe.assert_released(&local);
        assert_eq!(page, source);
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            encode(&page, limits(), &retry.context(local.clone())).unwrap(),
            expected
        );
        retry.assert_released(&local);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    let work = probe.context(local.clone());
    let held = local
        .try_start(WorkRequest::background(WorkClass::Mutation, 1))
        .unwrap();
    let error = encode(&page, limits(), &work).unwrap_err();
    assert!(
        error.to_string().contains("admission deferred"),
        "{error:?}"
    );
    assert_eq!(probe.completed.load(Ordering::SeqCst), 0);
    drop(held);
    probe.assert_released(&local);
    let retry = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        encode(&page, limits(), &retry.context(local.clone())).unwrap(),
        expected
    );
    retry.assert_released(&local);
}

#[test]
fn checkpoint_units_row_page_codec_preserves_ordinary_error_priority() {
    let original = page(2, 8, 3);
    let mut cases = Vec::new();
    for mutation in 0..11 {
        let mut page = original.clone();
        match mutation {
            0 => page.generation = 0,
            1 => page.source_commit_epoch = 0,
            2 => page.rows.clear(),
            3 => page.column_count = 0,
            4 => page.column_count = 2049,
            5 => page.rows[0].primary_key.0.clear(),
            6 => page.rows[1].primary_key = page.rows[0].primary_key.clone(),
            7 => page.rows.swap(0, 1),
            8 => page.rows[0].row = RelationalRow::new(vec![]),
            9 => {
                page.rows[0].primary_key =
                    RelationalKey(vec![RelationalValue::Overflow(RelationalOverflowRef {
                        digest: page.schema_digest,
                        scalar_type: RelationalScalarType::Text,
                        compressed_bytes: 1,
                        uncompressed_bytes: 1,
                    })])
            }
            _ => {
                page.rows[0].row =
                    RelationalRow::new(vec![
                        RelationalValue::Overflow(RelationalOverflowRef {
                            digest: page.schema_digest,
                            scalar_type: RelationalScalarType::Text,
                            compressed_bytes: 0,
                            uncompressed_bytes: 4096,
                        });
                        8
                    ])
            }
        }
        cases.push((page, limits()));
    }
    for dimension in 0..7 {
        let mut limit = limits();
        match dimension {
            0 => limit.max_page_bytes = NonZeroUsize::new(139).unwrap(),
            1 => limit.max_rows = NonZeroUsize::MIN,
            2 => limit.max_columns = NonZeroUsize::MIN,
            3 => limit.max_key_bytes = NonZeroUsize::MIN,
            4 => limit.max_row_bytes = NonZeroUsize::MIN,
            5 => limit.max_inline_value_bytes = NonZeroUsize::MIN,
            _ => limit.max_page_bytes = NonZeroUsize::new(256).unwrap(),
        }
        cases.push((original.clone(), limit));
    }
    for (page, limits) in cases {
        let expected = page.encode(limits).unwrap_err();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = encode(&page, limits, &probe.context(local.clone())).unwrap_err();
        assert_eq!(actual, expected);
        probe.assert_released(&local);
    }
}
