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

//! Unlabeled sources and relationship-property `IN` lists seed the source of
//! a single-hop expand too (#1548). Every case is compared with the
//! node-first expand, whose source `WITH` binds first.

use super::*;
use crate::api::PlanCacheLookup;
use hawdb_storage::config::StorageResidencyMode;
use hawdb_storage::scan::{ScanPruningReport, ScanPruningStrategy, ScanPruningTargetKind};

/// Mem's `skill_mirror` occurrence check, verbatim.
const MEM: &str = "MATCH ()-[r:SYNTHESIZED_FROM]->() WHERE r._nmem_source_table_id = $table_id AND r._nmem_source_offset IN $offsets RETURN true LIMIT 1";
const MEM_NODE_FIRST: &str = "MATCH (s) WITH s MATCH (s)-[r:SYNTHESIZED_FROM]->() WHERE r._nmem_source_table_id = $table_id AND r._nmem_source_offset IN $offsets RETURN true LIMIT 1";
const ROWS: &str = "MATCH (s)-[r:SYNTHESIZED_FROM]->(t) WHERE r._nmem_source_table_id = $table_id AND r._nmem_source_offset IN $offsets RETURN s.id, r._nmem_source_offset, t.id";
const ROWS_NODE_FIRST: &str = "MATCH (s) WITH s MATCH (s)-[r:SYNTHESIZED_FROM]->(t) WHERE r._nmem_source_table_id = $table_id AND r._nmem_source_offset IN $offsets RETURN s.id, r._nmem_source_offset, t.id";

const SKILLS: i64 = 30;
const MEMORIES: i64 = 40;

/// Skills synthesized from memories (table 3), memories from memories
/// (table 4), and decoys: another type with the same properties and
/// unrelated nodes.
fn populate_mem(db: &mut Database) {
    for (label, count) in [("Skill", SKILLS), ("Memory", MEMORIES), ("Other", 10)] {
        let rows = (0..count)
            .map(|id| Value::Map(BTreeMap::from([("id".into(), Value::Int(id))])))
            .collect();
        db.query_with_params(
            &format!("UNWIND $rows AS row CREATE (:{label} {{id: row.id}})"),
            &BTreeMap::from([("rows".into(), Value::List(rows))]),
        )
        .unwrap();
    }
    let mut create = |source: &str, from: i64, to: i64, rel: &str, table: i64, offset: i64| {
        db.query_with_params(
            &format!(
                "MATCH (a:{source} {{id: $from}}), (b:Memory {{id: $to}}) \
                 CREATE (a)-[:{rel} {{_nmem_source_table_id: $table, _nmem_source_offset: $offset, _nmem_source_kind: 'Skill'}}]->(b)"
            ),
            &BTreeMap::from([
                ("from".into(), Value::Int(from)),
                ("to".into(), Value::Int(to)),
                ("table".into(), Value::Int(table)),
                ("offset".into(), Value::Int(offset)),
            ]),
        )
        .unwrap();
    };
    for offset in 0..60 {
        create(
            "Skill",
            offset % SKILLS,
            (offset * 7) % MEMORIES,
            "SYNTHESIZED_FROM",
            3,
            offset,
        );
    }
    for offset in 100..110 {
        create(
            "Memory",
            offset % MEMORIES,
            (offset + 1) % MEMORIES,
            "SYNTHESIZED_FROM",
            4,
            offset,
        );
    }
    for offset in 0..10 {
        create("Skill", offset, offset, "LINK", 3, offset);
    }
}

fn mem_database() -> Database {
    let mut db = Database::new();
    populate_mem(&mut db);
    db
}

fn reopened_out_of_core(name: &str, config: DatabaseConfig) -> Database {
    let path = unique_test_dir(name);
    {
        let mut db = Database::open(&path).unwrap();
        populate_mem(&mut db);
        db.checkpoint().unwrap();
    }
    let db = Database::open_with_config(
        &path,
        DatabaseConfig {
            storage_residency_mode: StorageResidencyMode::OutOfCore,
            ..config
        },
    )
    .unwrap();
    assert!(db.storage_residency_report().unwrap().out_of_core);
    db
}

fn occurrence(table: i64, offsets: Value) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("table_id".into(), Value::Int(table)),
        ("offsets".into(), offsets),
    ])
}

fn ints(values: &[i64]) -> Value {
    Value::List(values.iter().map(|value| Value::Int(*value)).collect())
}

/// Lists covering a single value, several, none, NULL elements, duplicates,
/// a missing value, another table and other value types.
fn offset_lists() -> Vec<(i64, Value)> {
    vec![
        (3, ints(&[5])),
        (3, ints(&[5, 6, 7])),
        (3, ints(&[])),
        (3, Value::List(vec![Value::Null])),
        (3, Value::List(vec![Value::Int(5), Value::Null])),
        (3, ints(&[5, 5, 6, 6])),
        (3, ints(&[999])),
        (4, ints(&[100, 105])),
        (3, ints(&[100])),
        (3, Value::List(vec![Value::Float(5.0)])),
        (3, Value::List(vec![Value::String("5".into())])),
        (3, ints(&(0..60).collect::<Vec<_>>())),
    ]
}

/// Mem's check and the row form equal the node-first expand for every list.
fn assert_lists_match_node_first(
    mut query: impl FnMut(&str, &BTreeMap<String, Value>) -> Vec<Vec<Value>>,
) {
    for (table, offsets) in offset_lists() {
        let parameters = occurrence(table, offsets.clone());
        for (seeded, node_first) in [(MEM, MEM_NODE_FIRST), (ROWS, ROWS_NODE_FIRST)] {
            assert_eq!(
                query(seeded, &parameters),
                query(node_first, &parameters),
                "{seeded} {table} {offsets:?}"
            );
        }
    }
}

fn seed_report(
    db: &mut Database,
    query: &str,
    parameters: &BTreeMap<String, Value>,
) -> Option<ScanPruningReport> {
    db.explain_analyze_query_with_params(query, parameters)
        .unwrap()
        .execution_profile
        .scan_pruning_reports
        .into_iter()
        .find(|report| {
            report.target_kind == ScanPruningTargetKind::Node && report.rel_type_id.is_some()
        })
}

fn explain_with(db: &mut Database, query: &str, parameters: &BTreeMap<String, Value>) -> String {
    let explain = db
        .query_with_params(&format!("EXPLAIN {query}"), parameters)
        .unwrap();
    let Value::String(plan) = &explain.rows[0]["plan"] else {
        panic!("missing plan")
    };
    plan.clone()
}

/// Unlabeled endpoints with relationship property equalities seed from every
/// node, in each direction and for anonymous endpoints.
#[test]
fn unlabeled_sources_seed_from_relationship_equalities() {
    let mut db = mem_database();
    for pattern in [
        "(s)-[r:SYNTHESIZED_FROM {_nmem_source_table_id: 3, _nmem_source_offset: 5}]->(t)",
        "(s)<-[r:SYNTHESIZED_FROM {_nmem_source_table_id: 3, _nmem_source_offset: 5}]-(t)",
        "(s)-[r:SYNTHESIZED_FROM {_nmem_source_table_id: 3, _nmem_source_offset: 5}]-(t)",
        "(s)-[r:SYNTHESIZED_FROM {_nmem_source_offset: 105}]->(t)",
        "(s)-[r:SYNTHESIZED_FROM {_nmem_source_offset: 999}]->(t)",
        "(s)-[r:LINK {_nmem_source_offset: 3}]->(t)",
    ] {
        for tail in ["", " LIMIT 1"] {
            let seeded = format!("MATCH {pattern} RETURN s.id, r._nmem_source_offset, t.id{tail}");
            let reference = format!(
                "MATCH (s) WITH s MATCH {pattern} RETURN s.id, r._nmem_source_offset, t.id{tail}"
            );
            assert_eq!(
                rows(&mut db, &seeded),
                rows(&mut db, &reference),
                "{seeded}"
            );
            assert!(
                explain(&mut db, &seeded).contains("RelationshipSeededNodeScan variable=s label= "),
                "{seeded}"
            );
            assert!(!explain(&mut db, &reference).contains("RelationshipSeededNodeScan"));
        }
    }
    // The verification shape: anonymous endpoints, WHERE equalities.
    let anonymous = "MATCH ()-[r:SYNTHESIZED_FROM]->() WHERE r._nmem_source_table_id = 3 AND r._nmem_source_offset = 7 RETURN r._nmem_source_offset AS offset";
    assert!(explain(&mut db, anonymous).contains("RelationshipSeededNodeScan"));
    assert_eq!(rows(&mut db, anonymous), vec![vec![Value::Int(7)]]);
    let report = seed_report(&mut db, anonymous, &BTreeMap::new()).expect("the source seeds");
    assert_eq!(report.label_id, None);
    assert_eq!(
        report.candidate_count_before_pruning as i64,
        SKILLS + MEMORIES + 10
    );
    assert_eq!(report.candidate_count_before_filter, 1);
}

/// Mem's `skill_mirror` check seeds from the `IN` list's postings and keeps
/// its filter; every list equals the node-first expand, and a non-list
/// parameter fails the same way.
#[test]
fn relationship_property_in_lists_seed_and_match_the_node_first_expand() {
    let mut db = mem_database();
    let parameters = occurrence(3, ints(&[5, 6]));
    let plan = explain_with(&mut db, MEM, &parameters);
    assert!(
        plan.contains("RelationshipSeededNodeScan variable=__anon0 label= rel=SYNTHESIZED_FROM")
            && plan.contains("in=_nmem_source_offset:[Int(5), Int(6)]"),
        "{plan}"
    );
    assert!(plan.contains("FilterExec predicate=PropertyIn"), "{plan}");
    assert!(
        !explain_with(&mut db, MEM_NODE_FIRST, &parameters).contains("RelationshipSeededNodeScan")
    );
    assert_lists_match_node_first(|query, parameters| {
        values(db.query_with_params(query, parameters).unwrap())
    });
    for value in [Value::Int(5), Value::Null] {
        let parameters = occurrence(3, value);
        let seeded = db.query_with_params(MEM, &parameters).map(values);
        let node_first = db
            .query_with_params(MEM_NODE_FIRST, &parameters)
            .map(values);
        assert_eq!(
            seeded.map_err(|error| error.to_string()),
            node_first.map_err(|error| error.to_string())
        );
    }
    // A labeled source with only the `IN` list seeds too.
    for offsets in [ints(&[4, 30]), ints(&[]), ints(&[4, 4, 999])] {
        // An empty literal list folds the whole pattern to an empty plan.
        let empty = offsets == ints(&[]);
        let parameters = BTreeMap::from([("o".into(), offsets)]);
        let seeded = "MATCH (s:Entity)-[r:RELATES_TO]->(t:Entity) WHERE r.offset IN $o RETURN s.id, r.offset, t.id";
        let reference = "MATCH (s:Entity) WITH s MATCH (s)-[r:RELATES_TO]->(t:Entity) WHERE r.offset IN $o RETURN s.id, r.offset, t.id";
        let mut entity = database();
        let plan = explain_with(&mut entity, seeded, &parameters);
        assert!(
            plan.contains(if empty {
                "EmptyExec"
            } else {
                "RelationshipSeededNodeScan"
            }),
            "{plan}"
        );
        assert_eq!(
            values(entity.query_with_params(seeded, &parameters).unwrap()),
            values(entity.query_with_params(reference, &parameters).unwrap())
        );
    }
}

/// The scan-pruning report names the probed `IN` property; the cached plan
/// rebinds the list and its fingerprint follows the values.
#[test]
fn in_list_reports_rebinds_cached_plans_and_fingerprints() {
    let mut db = mem_database();
    let first = db
        .explain_analyze_query_with_params(ROWS, &occurrence(3, ints(&[5, 6, 7])))
        .unwrap();
    let report = first
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|report| report.target_kind == ScanPruningTargetKind::Node)
        .expect("the source seeds")
        .clone();
    assert_eq!(
        report.strategy,
        ScanPruningStrategy::PropertyIn {
            property: "_nmem_source_offset".into()
        }
    );
    assert!(report.pruned);
    assert_eq!(report.label_id, None);
    assert_eq!(
        report.candidate_count_before_pruning as i64,
        SKILLS + MEMORIES + 10
    );
    assert_eq!(report.candidate_count_before_filter, 3);
    assert_eq!(first.output.rows.len(), 3);

    let second = db
        .explain_analyze_query_with_params(ROWS, &occurrence(3, ints(&[8, 9, 10])))
        .unwrap();
    assert_eq!(second.plan_cache_lookup, PlanCacheLookup::Hit);
    let rebound = format!("{:?}", second.physical_plan);
    assert!(
        rebound.contains(
            "rel_property_in: Some((\"_nmem_source_offset\", [Int(8), Int(9), Int(10)]))"
        ),
        "{rebound}"
    );
    assert_eq!(
        values(second.output),
        values(
            db.query_with_params(ROWS_NODE_FIRST, &occurrence(3, ints(&[8, 9, 10])))
                .unwrap()
        )
    );
    let fingerprint = first.physical_plan.instance_fingerprint();
    assert!(
        fingerprint.contains("_nmem_source_offset IN ["),
        "{fingerprint}"
    );
    assert_ne!(fingerprint, second.physical_plan.instance_fingerprint());
    assert_eq!(
        first.physical_plan.fingerprint(),
        second.physical_plan.fingerprint()
    );
}

const WRITES: [&str; 3] = [
    "MATCH (a:Skill {id: 1}), (b:Memory {id: 2}) CREATE (a)-[:SYNTHESIZED_FROM {_nmem_source_table_id: 3, _nmem_source_offset: 600}]->(b)",
    "MATCH ()-[r:SYNTHESIZED_FROM {_nmem_source_offset: 6}]->() DELETE r",
    "MATCH ()-[r:SYNTHESIZED_FROM {_nmem_source_offset: 8}]->() SET r._nmem_source_offset = 9",
];

fn written_lists() -> Vec<(i64, Value)> {
    let mut lists = offset_lists();
    lists.extend([
        (3, ints(&[600])),
        (3, ints(&[6, 8])),
        (3, ints(&[9])),
        (3, ints(&[6, 9, 600])),
    ]);
    lists
}

/// Writes inside a transaction are visible to the seeded scan, resident and
/// out of core.
#[test]
fn transaction_writes_match_the_node_first_expand() {
    for out_of_core in [false, true] {
        let mut db = if out_of_core {
            reopened_out_of_core(
                "relationship_seeds_in_transaction",
                DatabaseConfig::default(),
            )
        } else {
            mem_database()
        };
        let mut transaction = db.begin_transaction().unwrap();
        for write in WRITES {
            transaction.query(write).unwrap();
        }
        for (table, offsets) in written_lists() {
            let parameters = occurrence(table, offsets.clone());
            for (seeded, node_first) in [(MEM, MEM_NODE_FIRST), (ROWS, ROWS_NODE_FIRST)] {
                assert_eq!(
                    values(transaction.query_with_params(seeded, &parameters).unwrap()),
                    values(
                        transaction
                            .query_with_params(node_first, &parameters)
                            .unwrap()
                    ),
                    "out of core {out_of_core}: {seeded} {offsets:?}"
                );
            }
        }
        transaction.commit().unwrap();
        for (table, offsets) in written_lists() {
            let parameters = occurrence(table, offsets);
            assert_eq!(
                values(db.query_with_params(ROWS, &parameters).unwrap()),
                values(db.query_with_params(ROWS_NODE_FIRST, &parameters).unwrap())
            );
        }
    }
}

/// Out of core, the `IN` postings come from the persisted projection merged
/// with the delta, and the report shows the seeding.
#[test]
fn out_of_core_in_lists_seed_from_the_persisted_projection() {
    let mut db = reopened_out_of_core(
        "relationship_seeds_in_out_of_core",
        DatabaseConfig::default(),
    );
    assert_lists_match_node_first(|query, parameters| {
        values(db.query_with_params(query, parameters).unwrap())
    });
    // Block-granular estimates tie in this one-block projection; the smaller
    // actual posting (the `IN` list, not the shared table id) answers.
    let report =
        seed_report(&mut db, ROWS, &occurrence(3, ints(&[5, 6, 7]))).expect("the source seeds");
    assert_eq!(
        report.strategy,
        ScanPruningStrategy::PropertyIn {
            property: "_nmem_source_offset".into()
        }
    );
    assert_eq!(report.candidate_count_before_filter, 3);
    let unlabeled =
        "MATCH (s)-[r:SYNTHESIZED_FROM {_nmem_source_offset: 105}]->(t) RETURN s.id, t.id";
    assert!(seed_report(&mut db, unlabeled, &BTreeMap::new()).is_some());
    for write in WRITES {
        db.query(write).unwrap();
    }
    for (table, offsets) in written_lists() {
        let parameters = occurrence(table, offsets);
        assert_eq!(
            values(db.query_with_params(ROWS, &parameters).unwrap()),
            values(db.query_with_params(ROWS_NODE_FIRST, &parameters).unwrap())
        );
    }
}

/// Under tight query and batch budgets the seeded scan falls back or emits
/// smaller batches; it never fails where the node-first expand runs.
#[test]
fn tight_memory_budgets_match_the_node_first_expand_for_in_lists() {
    for (index, (query_bytes, batch_bytes)) in [(65_536, 4_096), (24_576, 1_024), (16_384, 512)]
        .into_iter()
        .enumerate()
    {
        let config = DatabaseConfig {
            execution_memory: crate::executor::ExecutionMemoryConfig {
                query_memory_bytes: std::num::NonZeroUsize::new(query_bytes).unwrap(),
                batch_rows: std::num::NonZeroUsize::new(2).unwrap(),
                batch_payload_bytes: std::num::NonZeroUsize::new(batch_bytes).unwrap(),
                ..Default::default()
            },
            ..DatabaseConfig::default()
        };
        let mut resident = Database::new_with_config(config.clone());
        populate_mem(&mut resident);
        let out_of_core =
            reopened_out_of_core(&format!("relationship_seeds_in_budget_{index}"), config);
        for mut db in [resident, out_of_core] {
            for (table, offsets) in [(3, ints(&[5, 6])), (3, ints(&(0..60).collect::<Vec<_>>()))] {
                let parameters = occurrence(table, offsets);
                for (seeded, node_first) in [(MEM, MEM_NODE_FIRST), (ROWS, ROWS_NODE_FIRST)] {
                    let seeded = db.query_with_params(seeded, &parameters).map(values);
                    let node_first = db.query_with_params(node_first, &parameters).map(values);
                    match (seeded, node_first) {
                        (Ok(seeded), Ok(node_first)) => assert_eq!(seeded, node_first),
                        // Both refuse: the per-row ceiling applies to either plan.
                        (Err(_), Err(_)) => {}
                        (seeded, node_first) => panic!(
                            "budget {query_bytes}/{batch_bytes}: seeded {:?}, node-first {:?}",
                            seeded.map(|rows| rows.len()),
                            node_first.map(|rows| rows.len())
                        ),
                    }
                }
            }
        }
    }
}
