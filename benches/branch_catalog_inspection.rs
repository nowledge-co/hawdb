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

use hawdb::{Database, DatabaseConfig, Uuid, Value};
use hawdb_storage::branch_catalog::{
    self, BranchId, BranchName, BranchRecord, BranchState, Catalog, CreateOutcome,
};
use hawdb_storage::branch_project::ProjectMetadata;
use serde_json::json;
use std::hint::black_box;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const CODEC_LIMIT: usize = 100_000;
const COUNTS: &[usize] = if SMOKE {
    &[1, 16]
} else {
    &[1, 16, 256, 4_096, 16_384, 65_536, CODEC_LIMIT]
};
const WARMUPS: usize = if SMOKE { 1 } else { 3 };
const SAMPLES: usize = if SMOKE { 2 } else { 31 };

fn main() {
    let mut cases = Vec::new();
    for shape in ["star", "chain"] {
        for &count in COUNTS {
            let deleted_percentages: &[usize] = if (SMOKE && count == 16) || count >= 65_536 {
                &[0, 50, 90]
            } else {
                &[0]
            };
            for &deleted_percent in deleted_percentages {
                cases.push(measure_case(shape, count, deleted_percent));
            }
        }
    }
    println!(
        "branch_catalog_inspection {}",
        json!({
            "protocol": "hawdb-branch-catalog-inspection-benchmark-v2",
            "evidence_kind": "synthetic_metadata_scaling",
            "production_eligible": false,
            "fixture_project_identity": "preserved_from_ordinary_open",
            "catalog_record_limit": CODEC_LIMIT,
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH,
            "warmup_samples": WARMUPS,
            "measurement_samples": SAMPLES,
            "cases": cases,
        })
    );
}

fn measure_case(shape: &str, count: usize, deleted_percent: usize) -> serde_json::Value {
    let root = std::env::temp_dir().join(format!(
        "hawdb-catalog-bench-{}-{}-{shape}-{count}-{deleted_percent}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let mut database = Database::open(&root).expect("open benchmark database");
    let seed = ProjectMetadata::open(&root, DatabaseConfig::default().max_open_files)
        .expect("read ordinary project identities")
        .catalog()
        .clone();
    let catalog = fixture(shape, count, deleted_percent, seed);
    let encoded = catalog.encode().expect("encode fixture");
    let last = catalog.branches.last().expect("nonempty fixture");
    assert_eq!(last.state, BranchState::Ready);
    let target_id = Value::Uuid(last.id.as_uuid());
    let target_name = Value::String(last.name.as_str().to_string());
    let deleted_count = catalog
        .branches
        .iter()
        .filter(|branch| branch.state == BranchState::Deleted)
        .count();
    assert_eq!(deleted_count, (count - 1) * deleted_percent / 100);
    let successful_create_receipt_count = catalog
        .branches
        .iter()
        .filter(|branch| branch.create_outcome == CreateOutcome::Succeeded)
        .count();
    assert_eq!(successful_create_receipt_count, count);
    let offset = count / 2;
    let page_id = Value::Uuid(catalog.branches[offset].id.as_uuid());
    let catalog_directory = root.join("branches");
    branch_catalog::write_catalog(&catalog_directory.join("catalog.hawdb"), &catalog)
        .expect("publish synthetic catalog");
    let published = ProjectMetadata::open(&root, DatabaseConfig::default().max_open_files)
        .expect("revalidate fixture against the published project selector");
    assert_eq!(published.catalog().project_id, catalog.project_id);
    assert_eq!(published.catalog().branches.len(), count);
    assert_eq!(published.main().state, BranchState::Ready);
    drop(published);

    let validation = measure(|| catalog.validate().expect("validate fixture"));
    let decode = measure(|| {
        let decoded = Catalog::decode(black_box(&encoded)).expect("decode fixture");
        assert_eq!(decoded.branches.len(), count);
        assert_eq!(decoded.project_id, catalog.project_id);
        black_box(decoded);
    });
    let page = measure(|| {
        let output = database
            .query_sql_with_params(
                "SHOW BRANCHES LIMIT 1 OFFSET $1",
                &[Value::Int(offset as i64)],
            )
            .expect("read one catalog page");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("branch_id"), Some(&page_id));
        black_box(output);
    });
    let by_name = measure(|| {
        let output = database
            .query_sql_with_params("SHOW BRANCH NAME $1", std::slice::from_ref(&target_name))
            .expect("lookup branch name");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("branch_id"), Some(&target_id));
        black_box(output);
    });
    let by_id = measure(|| {
        let output = database
            .query_sql_with_params("SHOW BRANCH ID $1", std::slice::from_ref(&target_id))
            .expect("lookup branch UUID");
        assert_eq!(output.rows.len(), 1);
        assert_eq!(output.rows[0].get("branch_id"), Some(&target_id));
        black_box(output);
    });
    let deleted_by_id = catalog
        .branches
        .iter()
        .find(|branch| branch.state == BranchState::Deleted)
        .map(|branch| {
            let deleted_id = Value::Uuid(branch.id.as_uuid());
            measure(|| {
                let output = database
                    .query_sql_with_params("SHOW BRANCH ID $1", std::slice::from_ref(&deleted_id))
                    .expect("lookup retained deletion receipt");
                assert_eq!(output.rows.len(), 1);
                assert_eq!(output.rows[0].get("branch_id"), Some(&deleted_id));
                assert_eq!(
                    output.rows[0].get("state"),
                    Some(&Value::String("deleted".into()))
                );
                black_box(output);
            })
        });
    drop(database);
    std::fs::remove_dir_all(root).expect("remove owned benchmark fixture");
    json!({
        "lineage_shape": shape,
        "branch_count": count,
        "ready_branch_count": count - deleted_count,
        "retained_deleted_count": deleted_count,
        "deleted_percent_of_children": deleted_percent,
        "successful_create_receipt_count": successful_create_receipt_count,
        "catalog_encoded_bytes": encoded.len(),
        "page_offset": offset,
        "result_rows_per_query": 1,
        "validate": validation,
        "decode": decode,
        "show_page": page,
        "show_name": by_name,
        "show_id": by_id,
        "show_deleted_id": deleted_by_id,
    })
}

fn fixture(shape: &str, count: usize, deleted_percent: usize, mut catalog: Catalog) -> Catalog {
    assert!((1..=CODEC_LIMIT).contains(&count));
    assert_eq!(catalog.branches.len(), 1);
    let main = catalog.branches[0].clone();
    assert_eq!(main.name, BranchName::main());
    assert_eq!(main.state, BranchState::Ready);
    assert!(main.base_root_digest.is_some());
    let deleted_count = (count - 1) * deleted_percent / 100;
    let mut parent_id = main.id;
    for index in 1..count {
        let id = branch_id(u128::MAX - count as u128 + index as u128);
        assert_ne!(id, main.id);
        assert_ne!(id, catalog.project_id);
        let deleted = index <= deleted_count;
        catalog.branches.push(BranchRecord {
            id,
            name: BranchName::new(format!("branch-{index:06}")).unwrap(),
            parent_id: Some(if shape == "chain" { parent_id } else { main.id }),
            source_commit_epoch: 0,
            base_root_digest: main.base_root_digest,
            metadata_revision: if deleted { 4 } else { 2 },
            state: if deleted {
                BranchState::Deleted
            } else {
                BranchState::Ready
            },
            owner: None,
            create_request_key: format!("request-{index:06}"),
            request_fingerprint: [1; 32],
            create_outcome: CreateOutcome::Succeeded,
        });
        parent_id = id;
    }
    catalog.branches.sort_unstable_by_key(|branch| branch.id);
    catalog.validate().expect("validate synthetic fixture");
    catalog
}

fn branch_id(value: u128) -> BranchId {
    BranchId::new(Uuid::from_u128(value)).expect("nonzero branch identity")
}

fn measure(mut operation: impl FnMut()) -> serde_json::Value {
    for _ in 0..WARMUPS {
        operation();
    }
    let samples: Vec<u64> = (0..SAMPLES)
        .map(|_| {
            let start = Instant::now();
            operation();
            u64::try_from(start.elapsed().as_nanos()).expect("sample duration fits u64")
        })
        .collect();
    let mut sorted = samples.clone();
    sorted.sort_unstable();
    json!({
        "samples_ns": samples,
        "p50_ns": sorted[(SAMPLES * 50).div_ceil(100) - 1],
        "p95_ns": sorted[(SAMPLES * 95).div_ceil(100) - 1],
        "p99_ns": sorted[(SAMPLES * 99).div_ceil(100) - 1],
    })
}
