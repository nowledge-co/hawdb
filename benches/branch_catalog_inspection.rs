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

use hawdb::{Database, Uuid, Value};
use hawdb_storage::branch_catalog::{
    self, BranchId, BranchName, BranchRecord, BranchState, Catalog, CreateOutcome,
};
use serde_json::json;
use std::hint::black_box;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const SMOKE: bool = cfg!(debug_assertions);
const COUNTS: &[usize] = if SMOKE {
    &[1, 16]
} else {
    &[1, 16, 256, 4_096, 16_384]
};
const WARMUPS: usize = if SMOKE { 1 } else { 3 };
const SAMPLES: usize = if SMOKE { 2 } else { 31 };

fn main() {
    let mut cases = Vec::new();
    for shape in ["star", "chain"] {
        for &count in COUNTS {
            cases.push(measure_case(shape, count));
        }
    }
    println!(
        "branch_catalog_inspection {}",
        json!({
            "protocol": "hawdb-branch-catalog-inspection-benchmark-v1",
            "evidence_kind": "synthetic_metadata_scaling",
            "production_eligible": false,
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH,
            "warmup_samples": WARMUPS,
            "measurement_samples": SAMPLES,
            "cases": cases,
        })
    );
}

fn measure_case(shape: &str, count: usize) -> serde_json::Value {
    let catalog = fixture(shape, count);
    let encoded = catalog.encode().expect("encode fixture");
    let last = catalog.branches.last().expect("nonempty fixture");
    let target_id = Value::Uuid(last.id.as_uuid());
    let target_name = Value::String(last.name.as_str().to_string());
    let offset = count / 2;
    let page_id = Value::Uuid(catalog.branches[offset].id.as_uuid());
    let root = std::env::temp_dir().join(format!(
        "hawdb-catalog-bench-{}-{}-{shape}-{count}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let mut database = Database::open(&root).expect("open benchmark database");
    let catalog_directory = root.join("branches");
    std::fs::create_dir_all(&catalog_directory).expect("create catalog directory");
    branch_catalog::write_catalog(&catalog_directory.join("catalog.hawdb"), &catalog)
        .expect("publish synthetic catalog");

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
    drop(database);
    std::fs::remove_dir_all(root).expect("remove owned benchmark fixture");
    json!({
        "lineage_shape": shape,
        "branch_count": count,
        "catalog_encoded_bytes": encoded.len(),
        "page_offset": offset,
        "result_rows_per_query": 1,
        "validate": validation,
        "decode": decode,
        "show_page": page,
        "show_name": by_name,
        "show_id": by_id,
    })
}

fn fixture(shape: &str, count: usize) -> Catalog {
    let main_id = branch_id(1);
    let mut catalog = Catalog::bootstrap(branch_id(u128::MAX), main_id).unwrap();
    for index in 1..count {
        catalog.branches.push(BranchRecord {
            id: branch_id(index as u128 + 1),
            name: BranchName::new(format!("branch-{index:06}")).unwrap(),
            parent_id: Some(if shape == "chain" {
                branch_id(index as u128)
            } else {
                main_id
            }),
            source_commit_epoch: 0,
            base_root_digest: Some([1; 32]),
            metadata_revision: 1,
            state: BranchState::Ready,
            owner: None,
            create_request_key: format!("request-{index:06}"),
            request_fingerprint: [1; 32],
            create_outcome: CreateOutcome::Succeeded,
        });
    }
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
