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
use crate::relational::{RelationalKey, RelationalValue};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(4),
        ..LocalQosPolicy::default()
    })
}

struct Fixture {
    catalog: Catalog,
    statistics: GraphStatistics,
    projected: BTreeMap<String, ProjectedGraphDefinition>,
    changes: Vec<SearchProjectionGraphChange>,
}

impl Fixture {
    fn new() -> Self {
        let mut catalog = Catalog::default();
        for id in 0..128 {
            let label = catalog.get_or_create_label(&format!("Memory-{id:04}-{}", "界".repeat(80)));
            let kind = catalog.get_or_create_rel_type(&format!("LINK-{id:04}-{}", "界".repeat(80)));
            for property in ["key", "second"] {
                catalog.get_or_create_property_index(label, property);
            }
            catalog.get_or_create_composite_property_index(label, &["key".into(), "second".into()]);
            catalog.get_or_create_unique_constraint(label, "unique");
            catalog.get_or_create_node_property_exists_constraint(label, "required");
            catalog.get_or_create_relationship_property_exists_constraint(kind, "required");
            catalog.get_or_create_relationship_unique_constraint(kind, "unique");
        }
        let mut statistics = GraphStatistics {
            computed_at_commit_epoch: 4099,
            advanced_statistics_complete: true,
            histogram_sample_limit: 512,
            node_count: 1025,
            relationship_count: 1024,
            ..GraphStatistics::default()
        };
        for id in 0..128 {
            statistics.label_counts.insert(LabelId(id), 8);
            statistics.rel_type_counts.insert(RelTypeId(id), 8);
            statistics.rel_type_source_counts.insert(RelTypeId(id), 7);
            statistics.rel_type_target_counts.insert(RelTypeId(id), 6);
            let key = (LabelId(id), RelTypeId(id), LabelId(id));
            statistics.path_counts.insert(key, 19);
            statistics.path_source_distinct_counts.insert(key, 17);
            statistics.path_target_distinct_counts.insert(key, 13);
            for hops in 1..=3 {
                let key = (LabelId(id), RelTypeId(id), LabelId(id), hops);
                statistics.bounded_path_counts.insert(key, 11);
                statistics
                    .bounded_path_source_distinct_counts
                    .insert(key, 7);
                statistics
                    .bounded_path_target_distinct_counts
                    .insert(key, 5);
            }
        }
        let mut random = 47u64;
        for id in 0..1025u32 {
            let mut values = vec![
                Value::Null,
                Value::Bool(true),
                Value::Bool(false),
                Value::Int(-71),
                Value::Float(1.5),
                Value::Uuid(Uuid::from_u128(17)),
                Value::String(format!("宽值-{id}")),
                Value::Binary(vec![0, 7, 255]),
                Value::List(vec![
                    Value::String("nested".into()),
                    Value::List(Vec::new()),
                ]),
                Value::Map(BTreeMap::from([(
                    "unicode-界".into(),
                    Value::List(vec![Value::Int(9)]),
                )])),
            ];
            if id % 257 == 0 {
                let bytes = (0..65537)
                    .map(|_| {
                        random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                        (random >> 32) as u8
                    })
                    .collect();
                values.push(Value::Binary(bytes));
                values.push(Value::String("界".repeat(25000)));
            }
            let name = format!("property-{id}-界");
            let node_key = (LabelId(id % 128), name.clone());
            statistics
                .property_distinct_counts
                .insert(node_key.clone(), values.len() as u64);
            statistics
                .property_histograms
                .insert(node_key.clone(), values.clone());
            statistics
                .sampled_property_histograms
                .insert(node_key, false);
            let rel_key = (RelTypeId(id % 128), name);
            statistics
                .rel_property_distinct_counts
                .insert(rel_key.clone(), values.len() as u64);
            statistics
                .rel_property_histograms
                .insert(rel_key.clone(), values);
            statistics
                .sampled_rel_property_histograms
                .insert(rel_key, false);
            statistics
                .index_samples
                .insert(IndexId(id), IndexStatisticsSample::exact(31, 7));
        }
        let mut captured = RelationalPrimaryKeyChangeCapture::Captured {
            tables: (0..3)
                .map(|table| RelationalTablePrimaryKeyChanges {
                    table: format!("table-{table}-界"),
                    primary_keys: (0..1025)
                        .map(|id| {
                            RelationalKey(vec![
                                RelationalValue::BigInt(id),
                                RelationalValue::Text(format!("key-{id}-界")),
                            ])
                        })
                        .collect(),
                })
                .collect(),
            encoded_bytes: 0,
        };
        if let RelationalPrimaryKeyChangeCapture::Captured {
            tables,
            encoded_bytes,
        } = &mut captured
        {
            *encoded_bytes = tables
                .iter()
                .map(|table| {
                    4 + table.table.len()
                        + table
                            .primary_keys
                            .iter()
                            .map(|key| 4 + encode_relational_primary_key(key).unwrap().len())
                            .sum::<usize>()
                })
                .sum();
        }
        let mut changes = vec![SearchProjectionGraphChange {
            commit_epoch: 3,
            upsert_node_ids: (0..4097).collect(),
            delete_document_ids: (0..1025).map(|id| format!("deleted-{id:06}-界")).collect(),
            relational_primary_key_changes: captured,
        }];
        for (position, reason) in [
            RelationalPrimaryKeyChangeRebuildReason::SchemaRewrite,
            RelationalPrimaryKeyChangeRebuildReason::CaptureLimitExceeded,
            RelationalPrimaryKeyChangeRebuildReason::UnsupportedKeyEncoding,
            RelationalPrimaryKeyChangeRebuildReason::WalEncodingLimitExceeded,
            RelationalPrimaryKeyChangeRebuildReason::MissingWalCapture,
            RelationalPrimaryKeyChangeRebuildReason::SnapshotReplacement,
            RelationalPrimaryKeyChangeRebuildReason::MultipleRelationalTransactions,
        ]
        .into_iter()
        .enumerate()
        {
            changes.push(SearchProjectionGraphChange {
                commit_epoch: 4 + position as u64,
                upsert_node_ids: Vec::new(),
                delete_document_ids: Vec::new(),
                relational_primary_key_changes:
                    RelationalPrimaryKeyChangeCapture::RequiresRebuild { reason },
            });
        }
        Self {
            catalog,
            statistics,
            changes,
            projected: BTreeMap::from([(
                "projected-界".repeat(17000),
                ProjectedGraphDefinition {
                    node_labels: vec!["Memory-界".into(), "".into()],
                    rel_types: vec!["LINK-界".into()],
                },
            )]),
        }
    }

    fn image(&self) -> CheckpointImage<'_> {
        CheckpointImage {
            catalog: &self.catalog,
            commit_epoch: 4099,
            next_node_id: 4100,
            next_rel_id: 4101,
            search_projection_change_log_start_epoch: 2,
            search_projection_graph_changes: &self.changes,
            statistics: &self.statistics,
            projected_graphs: &self.projected,
            initial_import_source_fingerprint: Some("fingerprint-界"),
            search_projection_database_identity: Some(Uuid::from_u128(51)),
            relational_checkpoint: Some(DurableArtifactMetadata::for_bytes(b"relational-closure")),
        }
    }
}

#[test]
fn checkpoint_units_metadata_body_matches_ordinary_bytes_and_complete_decode() {
    let fixture = Fixture::new();
    let image = fixture.image();
    let expected = encode_checkpoint_body_with_changes(&image, 17, fixture.changes.iter()).unwrap();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let actual = encode_checkpoint_body_with_work_context(
        &image,
        17,
        fixture.changes.iter(),
        &probe.context(local.clone()),
    )
    .unwrap();
    assert!(
        actual == expected,
        "complete V1 metadata differs from independent ordinary encoder"
    );
    assert!(probe.completed.load(Ordering::SeqCst) > 10000);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert_eq!(probe.io_waves.load(Ordering::SeqCst), 0);
    probe.assert_released(&local);
    let mut catalog = Catalog::default();
    let mut decoded = DecodedCheckpoint::default();
    parse_checkpoint(&actual, &mut catalog, &mut decoded).unwrap();
    assert!(
        decoded.search_projection_graph_changes == fixture.changes,
        "complete decoded graph and relational change capture differs"
    );
    decoded.checkpoint_statistics.advanced_statistics_complete =
        decoded.statistics_complete.unwrap();
    assert!(
        decoded.checkpoint_statistics == fixture.statistics,
        "complete decoded statistics differs"
    );
    assert_eq!(catalog.labels().count(), fixture.catalog.labels().count());
    assert_eq!(decoded.projected_graphs, fixture.projected);
}

#[test]
fn checkpoint_units_metadata_body_cancels_inner_fields_and_final_completion_then_retries() {
    let fixture = Fixture::new();
    let image = fixture.image();
    let local = scheduler();
    let probe = Arc::new(CheckpointWorkProbe::default());
    let expected = encode_checkpoint_body_with_work_context(
        &image,
        17,
        fixture.changes.iter(),
        &probe.context(local.clone()),
    )
    .unwrap();
    let total = probe.completed.load(Ordering::SeqCst);
    probe.assert_released(&local);
    for limit in [1, 17, 512, 4097, total / 2, total - 1, total] {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(limit, Ordering::SeqCst);
        let error = encode_checkpoint_body_with_work_context(
            &image,
            17,
            fixture.changes.iter(),
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "storage error: checkpoint build stopped: cancelled"
        );
        assert_eq!(probe.completed.load(Ordering::SeqCst), limit);
        probe.assert_released(&local);
    }
    let probe = Arc::new(CheckpointWorkProbe::default());
    assert!(
        encode_checkpoint_body_with_work_context(
            &image,
            17,
            fixture.changes.iter(),
            &probe.context(local.clone())
        )
        .unwrap()
            == expected
    );
    probe.assert_released(&local);
}

#[test]
fn checkpoint_units_metadata_body_preserves_validation_order_and_errors() {
    let fixture = Fixture::new();
    for scenario in 0..8 {
        let mut changes = fixture.changes.clone();
        let mut image = fixture.image();
        match scenario {
            0 => image.search_projection_change_log_start_epoch = 4100,
            1 => changes[0].commit_epoch = 2,
            2 => changes[0].commit_epoch = 4100,
            3 => changes[0].upsert_node_ids[1025] = 1024,
            4 => changes[0].delete_document_ids.swap(512, 513),
            5 => {
                let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
                    &mut changes[0].relational_primary_key_changes
                else {
                    unreachable!()
                };
                tables.swap(0, 1);
            }
            6 => {
                let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
                    &mut changes[0].relational_primary_key_changes
                else {
                    unreachable!()
                };
                tables[1].primary_keys.clear();
            }
            _ => {
                let RelationalPrimaryKeyChangeCapture::Captured { tables, .. } =
                    &mut changes[0].relational_primary_key_changes
                else {
                    unreachable!()
                };
                tables[2].primary_keys.swap(1023, 1024);
            }
        }
        let expected = encode_checkpoint_body_with_changes(&image, 17, changes.iter()).unwrap_err();
        let local = scheduler();
        let probe = Arc::new(CheckpointWorkProbe::default());
        let actual = encode_checkpoint_body_with_work_context(
            &image,
            17,
            changes.iter(),
            &probe.context(local.clone()),
        )
        .unwrap_err();
        assert_eq!(actual, expected, "scenario={scenario}");
        probe.assert_released(&local);
    }
}
