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
use hawdb_storage::relational::{
    relational_index_shadow_artifact_file, RelationalIndexReadLimits, RelationalIndexShadowConfig,
    RelationalIndexShadowReader, RelationalIndexShadowWriter, RelationalKey, RelationalValue,
};
use hawdb_storage::relational_index_view::{
    RelationalIndexReadView, RelationalIndexReadViewBackendReport, RelationalTransactionIndexView,
};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

struct OwnedFixture(std::path::PathBuf);

impl Drop for OwnedFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove only the owned index fixture");
    }
}

fn execute_nested_index_join(limited: bool) {
    execute_nested_index_join_with_statement_limit(limited, None);
}

#[derive(Clone, Copy, Debug)]
enum StatementBudget {
    Pages,
    Bytes,
    FileBytes,
    Rows,
}

fn execute_nested_index_join_with_statement_limit(
    limited: bool,
    statement_budget: Option<StatementBudget>,
) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);
    let state = merge_join_state();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let fixture = OwnedFixture(std::env::temp_dir().join(format!(
        "hawdb-authoritative-nested-{}-{nonce}-{limited}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
    )));
    std::fs::create_dir(&fixture.0).unwrap();
    let config = RelationalIndexShadowConfig::default();
    RelationalIndexShadowWriter::new(config)
        .publish(&fixture.0, &state, 1, 40, None)
        .unwrap();
    let reader = RelationalIndexShadowReader::open(&fixture.0, 1, 40, config).unwrap();
    let inner_root = reader
        .manifest()
        .root("merge_right", "idx_merge_right_key")
        .unwrap();
    let corrupt_offset = (inner_root.root_page_id.get() - 1) * reader.manifest().page_bytes;
    let view = Arc::new(RelationalIndexReadView::from_base(reader));
    let prefix = RelationalKey(vec![RelationalValue::Text("tenant-1".into())]);
    let report = view
        .visit_prefix_entries(
            "merge_left",
            "idx_merge_left_tenant_key",
            &prefix,
            Default::default(),
            |_, _| true,
        )
        .unwrap();
    assert_eq!(report.rows_visited, 2);
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        unreachable!()
    };
    let limits = RelationalIndexReadLimits {
        max_pages: if limited {
            NonZeroUsize::new(report.pages_read).unwrap()
        } else {
            RelationalIndexReadLimits::default().max_pages
        },
        ..Default::default()
    };
    let transaction = RelationalTransactionIndexView::new(view.clone(), Default::default(), limits);
    let mode =
        RelationalIndexReadMode::<crate::RelationalMaterializedReader>::AuthoritativeTransaction(
            &transaction,
        );
    let prepared = prepare_merge_join_with_index_read_mode(&state, mode);
    let RelationalPhysicalJoinNode::Join {
        algorithm,
        left,
        right,
        ..
    } = &prepared.access_plan.physical_join_plan().unwrap().root
    else {
        panic!("expected join")
    };
    assert_eq!(*algorithm, RelationalPhysicalJoinAlgorithm::BatchedIndex);
    let RelationalPhysicalJoinNode::Relation(left) = left.as_ref() else {
        panic!("expected outer relation")
    };
    let RelationalPhysicalJoinNode::Relation(right) = right.as_ref() else {
        panic!("expected inner relation")
    };
    assert!(
        matches!(&left.access, RelationalPhysicalAccess::Base(access)
        if matches!(&access.access, RelationalBaseAccess::Index { name, .. } if name == "idx_merge_left_tenant_key"))
    );
    assert!(
        matches!(&right.access, RelationalPhysicalAccess::Probe(access)
        if matches!(&access.access, RelationalJoinAccess::Index { name, .. } if name == "idx_merge_right_key"))
    );
    if limited || statement_budget.is_some() {
        // A nested inner read must reject before inspecting this unknown damage.
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(fixture.0.join(relational_index_shadow_artifact_file(1)))
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
        file.write_all(&byte).unwrap();
    }
    let memory = hawdb_executor::ExecutionMemoryConfig {
        batch_rows: NonZeroUsize::MIN,
        ..Default::default()
    };
    let mut query_limits = batched_index_join_limits();
    if let Some(budget) = statement_budget {
        match budget {
            StatementBudget::Pages => {
                query_limits.index_read.max_pages = NonZeroUsize::new(report.pages_read).unwrap();
            }
            StatementBudget::Bytes => {
                query_limits.index_read.max_bytes = NonZeroUsize::new(report.bytes_read).unwrap();
            }
            StatementBudget::FileBytes => {
                assert!(report.file_bytes_read > 0);
                query_limits.index_read.max_file_bytes = report.file_bytes_read;
            }
            StatementBudget::Rows => query_limits.index_read.max_rows = NonZeroUsize::MIN,
        }
    }
    let execution = prepared
        .execution
        .admit(
            &state,
            RelationalQueryReadModes::new(
                mode,
                RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
            ),
            RelationalQueryResourceContext::new(
                RelationalJoinEnumerationConfig::default(),
                query_limits,
                &memory,
                None,
            ),
        )
        .unwrap();
    let output = execute_select(&prepared, &[], execution);
    if limited || statement_budget.is_some() {
        assert!(
            matches!(output, Err(HawDBError::Execution(_))),
            "must reject by budget before reading inner payload: {output:?}"
        );
        assert!(!view.is_poisoned());
    } else {
        let output = output.expect("indexed outer callback can execute the batched inner probe");
        let mut rows = output
            .rows
            .iter()
            .map(|row| {
                let Value::String(left) = &row["left_id"] else {
                    panic!("left id must be text")
                };
                let Value::String(right) = &row["right_id"] else {
                    panic!("right id must be text")
                };
                (left.clone(), right.clone())
            })
            .collect::<Vec<_>>();
        rows.sort();
        assert_eq!(
            rows,
            vec![
                ("left-a".into(), "right-a".into()),
                ("left-b".into(), "right-b-1".into()),
                ("left-b".into(), "right-b-2".into())
            ]
        );
    }
}

#[test]
fn authoritative_indexed_outer_join_keeps_nested_batch_results() {
    execute_nested_index_join(false);
}

#[test]
fn authoritative_indexed_outer_join_admits_nested_io_before_payload() {
    execute_nested_index_join(true);
}

#[test]
fn statement_page_budget_admits_nested_io_before_payload() {
    execute_nested_index_join_with_statement_limit(false, Some(StatementBudget::Pages));
}

#[test]
fn statement_byte_budget_admits_nested_io_before_payload() {
    execute_nested_index_join_with_statement_limit(false, Some(StatementBudget::Bytes));
}

#[test]
fn statement_file_budget_admits_nested_io_before_payload() {
    execute_nested_index_join_with_statement_limit(false, Some(StatementBudget::FileBytes));
}

#[test]
fn statement_row_budget_admits_nested_io_before_payload() {
    execute_nested_index_join_with_statement_limit(false, Some(StatementBudget::Rows));
}
