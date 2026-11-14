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
use crate::mutation::{materialize_unwind_mutations, stage_unwind_mutations};
use crate::Row;
use hawdb_core::{HawDBError, RuntimeCancellationToken};
use hawdb_plan_cypher::{BatchMatchedRelationship, BatchMutationOperation, BatchMutationValue};
use hawdb_storage::mutation::MutationLimits;
use hawdb_storage::store::GraphStore;
use std::num::NonZeroUsize;

const PEOPLE: i64 = 3_000;

fn ledger(bytes: usize) -> QueryMemoryLedger {
    QueryMemoryLedger::new(NonZeroUsize::new(bytes).unwrap())
}

fn row(pairs: &[(&str, Value)]) -> Value {
    Value::Map(
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect(),
    )
}

/// `Person {id, kind}` nodes, a duplicate `id`, a node without `id`, an
/// `Other` node sharing an `id`, and an optional index on `Person.id`.
fn graph(indexed: bool) -> (Catalog, GraphStore) {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    if indexed {
        store
            .create_property_index(&mut catalog, "Person", "id")
            .unwrap();
    }
    for id in 0..PEOPLE {
        let kind = Value::String(if id % 2 == 0 { "even" } else { "odd" }.into());
        store
            .create_node(
                &mut catalog,
                "Person",
                BTreeMap::from([("id".into(), Value::Int(id)), ("kind".into(), kind)]),
            )
            .unwrap();
    }
    store
        .create_node(
            &mut catalog,
            "Person",
            BTreeMap::from([
                ("id".into(), Value::Int(7)),
                ("kind".into(), Value::String("odd".into())),
            ]),
        )
        .unwrap();
    store
        .create_node(&mut catalog, "Person", BTreeMap::new())
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Other",
            BTreeMap::from([("id".into(), Value::Int(3))]),
        )
        .unwrap();
    (catalog, store)
}

fn relationship(
    source: &[(&str, &str)],
    target_label: &str,
    target: &[(&str, &str)],
) -> BatchMutationOperation {
    let properties = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(property, column)| {
                (
                    (*property).to_string(),
                    BatchMutationValue::RowProperty((*column).to_string()),
                )
            })
            .collect()
    };
    BatchMutationOperation::CreateMatchedRelationship(Box::new(BatchMatchedRelationship {
        source_label: "Person".into(),
        source_properties: properties(source),
        target_label: target_label.into(),
        target_properties: properties(target),
        rel_type: "KNOWS".into(),
        rel_properties: BTreeMap::from([(
            "w".to_string(),
            BatchMutationValue::RowProperty("w".into()),
        )]),
    }))
}

/// Rows with duplicates, misses, wrong types and an endpoint that exists only
/// under another label.
fn rows() -> Vec<Value> {
    let mut rows = Vec::new();
    for index in 0..PEOPLE {
        rows.push(row(&[
            ("src", Value::Int(index)),
            ("dst", Value::Int((index * 7 + 1) % PEOPLE)),
            ("w", Value::Int(index)),
            ("kind", Value::String("odd".into())),
        ]));
    }
    for (src, dst) in [
        (Value::Int(7), Value::Int(7)),
        (Value::Int(1), Value::Int(PEOPLE + 5)),
        (Value::Int(1), Value::String("1".into())),
        (Value::Float(1.0), Value::Int(2)),
        (Value::Int(3), Value::Int(3)),
    ] {
        rows.push(row(&[
            ("src", src),
            ("dst", dst),
            ("w", Value::Int(-1)),
            ("kind", Value::String("odd".into())),
        ]));
    }
    rows
}

fn commit(
    store: &mut GraphStore,
    catalog: &mut Catalog,
    mutations: Vec<GraphMutation>,
) -> Vec<Row> {
    store
        .commit_mutations_with_limits(catalog, mutations, MutationLimits::default())
        .unwrap()
        .rows
}

#[test]
fn resolved_endpoints_create_exactly_what_per_row_scans_create() {
    for indexed in [false, true] {
        for (operation, patterns) in [
            (
                relationship(&[("id", "src")], "Person", &[("id", "dst")]),
                1,
            ),
            (relationship(&[("id", "src")], "Other", &[("id", "dst")]), 2),
            (
                relationship(
                    &[("id", "src"), ("kind", "kind")],
                    "Person",
                    &[("id", "dst")],
                ),
                2,
            ),
            (
                relationship(&[("id", "src")], "Missing", &[("id", "dst")]),
                1,
            ),
        ] {
            let rows = rows();
            let (mut expected_catalog, mut expected_store) = graph(indexed);
            let unresolved = materialize_unwind_mutations(
                &rows,
                "r",
                &operation,
                MutationLimits::default(),
                None,
            )
            .unwrap();
            let expected = commit(&mut expected_store, &mut expected_catalog, unresolved);

            let (mut catalog, mut store) = graph(indexed);
            let ledger = ledger(256 * 1024 * 1024);
            let mut staged = stage_unwind_mutations(
                &rows,
                "r",
                &operation,
                MutationLimits::default(),
                &ledger,
                None,
            )
            .unwrap();
            let report = staged
                .resolve_matched_endpoints(&catalog, &store, MutationLimits::default(), None)
                .unwrap();
            assert_eq!(report.patterns, patterns, "{operation:?}");
            // One pass per pattern, independent of the row count.
            assert_eq!(report.label_scans + report.index_lookups, patterns);
            let label_nodes = PEOPLE as usize + 3;
            assert!(report.nodes_visited <= patterns * label_nodes, "{report:?}");
            for mutation in staged.mutations() {
                let GraphMutation::CreateRelationshipsBetweenMatches(request) = mutation else {
                    panic!("{mutation:?}");
                };
                if request.target_label != "Missing" {
                    assert!(matches!(
                        request.source_filter,
                        Some(PropertyFilter::And(ref filters))
                            if matches!(
                                filters[0],
                                PropertyFilter::IdEq { .. } | PropertyFilter::IdIn { .. }
                            )
                    ));
                }
            }
            let (mutations, _charge) = staged.into_parts();
            let actual = commit(&mut store, &mut catalog, mutations);
            assert_eq!(actual, expected, "indexed={indexed} {operation:?}");
            let missing = matches!(
                &operation,
                BatchMutationOperation::CreateMatchedRelationship(request)
                    if request.target_label == "Missing"
            );
            assert_eq!(actual.is_empty(), missing);
        }
    }
    let (catalog, store) = graph(true);
    let mut staged = stage_unwind_mutations(
        &rows(),
        "r",
        &relationship(&[("id", "src")], "Person", &[("id", "dst")]),
        MutationLimits::default(),
        &ledger(256 * 1024 * 1024),
        None,
    )
    .unwrap();
    let report = staged
        .resolve_matched_endpoints(&catalog, &store, MutationLimits::default(), None)
        .unwrap();
    assert_eq!(
        (report.index_lookups, report.label_scans),
        (1, 0),
        "{report:?}"
    );
}

#[test]
fn duplicate_endpoints_keep_the_cartesian_product() {
    let (mut catalog, mut store) = graph(false);
    let operation = relationship(&[("id", "src")], "Person", &[("id", "dst")]);
    let rows = vec![row(&[
        ("src", Value::Int(7)),
        ("dst", Value::Int(7)),
        ("w", Value::Int(1)),
    ])];
    let mut staged = stage_unwind_mutations(
        &rows,
        "r",
        &operation,
        MutationLimits::default(),
        &ledger(1024 * 1024),
        None,
    )
    .unwrap();
    staged
        .resolve_matched_endpoints(&catalog, &store, MutationLimits::default(), None)
        .unwrap();
    let (mutations, _charge) = staged.into_parts();
    // Two `id: 7` nodes on each side.
    assert_eq!(commit(&mut store, &mut catalog, mutations).len(), 4);
}

#[test]
fn endpoint_resolution_stops_when_cancelled() {
    let (catalog, store) = graph(false);
    let mut staged = stage_unwind_mutations(
        &rows(),
        "r",
        &relationship(&[("id", "src")], "Person", &[("id", "dst")]),
        MutationLimits::default(),
        &ledger(256 * 1024 * 1024),
        None,
    )
    .unwrap();
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let task = RuntimeTaskContext::new(token, None);
    let error = staged
        .resolve_matched_endpoints(&catalog, &store, MutationLimits::default(), Some(&task))
        .unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
}

#[test]
fn unwind_lists_stay_uncharged_and_endpoint_maps_fail_closed_on_the_ledger() {
    let operation = relationship(&[("id", "src")], "Person", &[("id", "dst")]);
    let rows = rows();
    // An `UNWIND` list is bounded by the mutation limits, as before staging,
    // so a ledger far smaller than the list still stages it.
    let tight = ledger(4 * 1024);
    let mut staged = stage_unwind_mutations(
        &rows,
        "r",
        &operation,
        MutationLimits::default(),
        &tight,
        None,
    )
    .unwrap();
    assert_eq!(staged.len(), rows.len());
    assert_eq!(staged.charged_bytes(), size_of::<StagedMutations>());

    // The scans and id maps built by endpoint resolution are charged.
    let (catalog, store) = graph(false);
    let error = staged
        .resolve_matched_endpoints(&catalog, &store, MutationLimits::default(), None)
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::ResourceBudgetExceeded(_)),
        "{error:?}"
    );
}

/// Rows whose source pattern `Person {kind: "odd"}` matches every odd person
/// (`PEOPLE / 2 + 1` nodes): a fan-out endpoint copied into every row.
fn fan_out_rows(rows: usize, dst: i64) -> Vec<Value> {
    (0..rows)
        .map(|index| {
            row(&[
                ("dst", Value::Int(dst)),
                ("w", Value::Int(index as i64)),
                ("kind", Value::String("odd".into())),
            ])
        })
        .collect()
}

const ODD_PEOPLE: usize = PEOPLE as usize / 2 + 1;

fn affected_rows(max_affected_rows: usize) -> MutationLimits {
    MutationLimits {
        max_affected_rows: NonZeroUsize::new(max_affected_rows).unwrap(),
        max_operations: NonZeroUsize::new(10_000_000).unwrap(),
        max_result_rows: NonZeroUsize::new(10_000_000).unwrap(),
        max_result_payload_bytes: NonZeroUsize::new(usize::MAX).unwrap(),
    }
}

#[test]
fn fan_out_beyond_the_affected_row_limit_fails_before_copying_ids() {
    let operation = relationship(&[("kind", "kind")], "Person", &[("id", "dst")]);
    // (limit, dst): a hit creates ODD_PEOPLE relationships per row, a miss
    // none, but the commit still bounds each endpoint by the remaining rows.
    for (limit, dst) in [
        (ODD_PEOPLE - 1, 1),
        (ODD_PEOPLE, 1),
        (ODD_PEOPLE * 2, 1),
        (ODD_PEOPLE * 3 - 1, 1),
        (ODD_PEOPLE * 3, 1),
        // `id: 7` names two people: the Cartesian check, not the endpoint
        // bound, refuses these rows.
        (ODD_PEOPLE * 2 - 1, 7),
        (ODD_PEOPLE * 5, 7),
        (ODD_PEOPLE * 6, 7),
        (ODD_PEOPLE - 1, PEOPLE + 5),
        (ODD_PEOPLE, PEOPLE + 5),
    ] {
        let rows = fan_out_rows(3, dst);
        let limits = affected_rows(limit);
        // Oracle: the commit of the unresolved per-row filters.
        let (mut expected_catalog, mut expected_store) = graph(false);
        let unresolved =
            materialize_unwind_mutations(&rows, "r", &operation, limits, None).unwrap();
        let expected = expected_store
            .commit_mutations_with_limits(&mut expected_catalog, unresolved, limits)
            .map(|summary| summary.rows)
            .map_err(|error| error.to_string());

        let (mut catalog, mut store) = graph(false);
        let before = store.relationship_records_owned().count();
        let ledger = ledger(256 * 1024 * 1024);
        let mut staged =
            stage_unwind_mutations(&rows, "r", &operation, limits, &ledger, None).unwrap();
        let resolved = staged.resolve_matched_endpoints(&catalog, &store, limits, None);
        let id_led = |mutation: &GraphMutation| {
            let GraphMutation::CreateRelationshipsBetweenMatches(request) = mutation else {
                panic!("{mutation:?}");
            };
            [&request.source_filter, &request.target_filter].map(|filter| match filter {
                Some(PropertyFilter::And(filters)) => match &filters[0] {
                    PropertyFilter::IdIn { values } => Some(values.len()),
                    PropertyFilter::IdEq { .. } => Some(1),
                    _ => None,
                },
                _ => None,
            })
        };
        let actual = match resolved {
            Err(error) => {
                // Refused with the commit's error before the refusing row's
                // ids were copied, and nothing reached storage.
                let refused = staged
                    .mutations()
                    .iter()
                    .position(|mutation| id_led(mutation) == [None, None])
                    .expect("the refusing row keeps its original filters");
                assert!(refused < rows.len(), "limit={limit} dst={dst}");
                assert!(staged.mutations()[refused..]
                    .iter()
                    .all(|mutation| id_led(mutation) == [None, None]));
                assert_eq!(store.relationship_records_owned().count(), before);
                Err(error.to_string())
            }
            Ok(_) => {
                for mutation in staged.mutations() {
                    // A row with an empty endpoint copies no ids for either side.
                    let expected_ids = match dst {
                        1 => [Some(ODD_PEOPLE), Some(1)],
                        7 => [Some(ODD_PEOPLE), Some(2)],
                        _ => [Some(0), Some(0)],
                    };
                    assert_eq!(id_led(mutation), expected_ids);
                }
                let (mutations, _charge) = staged.into_parts();
                store
                    .commit_mutations_with_limits(&mut catalog, mutations, limits)
                    .map(|summary| summary.rows)
                    .map_err(|error| error.to_string())
            }
        };
        assert_eq!(actual, expected, "limit={limit} dst={dst}");
        if let Err(error) = &actual {
            assert!(
                error.contains("mutation would exceed max_mutation_affected_rows"),
                "{error}"
            );
        }
    }
}

#[test]
fn unwind_id_copies_are_charged_and_fail_closed_on_the_ledger() {
    let operation = relationship(&[("kind", "kind")], "Person", &[("id", "dst")]);
    let limits = affected_rows(10_000_000);
    let (catalog, store) = graph(false);
    let small = 1024 * 1024;
    // The endpoint maps fit the small ledger: the miss copies no ids.
    let ledger_small = ledger(small);
    let mut staged = stage_unwind_mutations(
        &fan_out_rows(200, PEOPLE + 5),
        "r",
        &operation,
        limits,
        &ledger_small,
        None,
    )
    .unwrap();
    staged
        .resolve_matched_endpoints(&catalog, &store, limits, None)
        .unwrap();
    drop(staged);

    // 200 rows x ODD_PEOPLE copied ids do not fit, even though the `UNWIND`
    // list itself is uncharged.
    let rows = fan_out_rows(200, 1);
    let mut staged =
        stage_unwind_mutations(&rows, "r", &operation, limits, &ledger_small, None).unwrap();
    assert_eq!(staged.charged_bytes(), size_of::<StagedMutations>());
    let error = staged
        .resolve_matched_endpoints(&catalog, &store, limits, None)
        .unwrap_err();
    assert!(
        matches!(error, HawDBError::ResourceBudgetExceeded(_)),
        "{error:?}"
    );

    // With room, the copies stay charged until the batch is committed.
    let mut staged = stage_unwind_mutations(
        &rows,
        "r",
        &operation,
        limits,
        &ledger(256 * 1024 * 1024),
        None,
    )
    .unwrap();
    staged
        .resolve_matched_endpoints(&catalog, &store, limits, None)
        .unwrap();
    assert!(
        staged.charged_bytes() > rows.len() * ODD_PEOPLE * size_of::<Value>(),
        "{}",
        staged.charged_bytes()
    );
}

#[test]
fn store_unwind_mutations_charge_the_configured_execution_memory() {
    let plan = hawdb_plan_cypher::PhysicalPlan::UnwindMutation {
        rows: fan_out_rows(50, 1),
        variable: "r".into(),
        operation: relationship(&[("kind", "kind")], "Person", &[("id", "dst")]),
    };
    let limits = affected_rows(10_000_000);
    let execute = |memory: &crate::ExecutionMemoryConfig| {
        let (mut catalog, mut store) = graph(false);
        let result = crate::mutation::execute_mutation_with_store_output_limits(
            &plan,
            &mut catalog,
            &mut store,
            limits,
            crate::result_delivery::OutputLimits::default(),
            memory,
            None,
        );
        (result, store.relationship_records_owned().count())
    };
    // The host's budget, not the default one, bounds endpoint resolution.
    let tight = crate::ExecutionMemoryConfig {
        query_memory_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        ..crate::ExecutionMemoryConfig::default()
    };
    let (result, relationships) = execute(&tight);
    assert!(
        matches!(result, Err(HawDBError::ResourceBudgetExceeded(_))),
        "{result:?}"
    );
    assert_eq!(relationships, 0);
    let (result, relationships) = execute(&crate::ExecutionMemoryConfig::default());
    assert_eq!(result.unwrap().len(), 50 * ODD_PEOPLE);
    assert_eq!(relationships, 50 * ODD_PEOPLE);
}
