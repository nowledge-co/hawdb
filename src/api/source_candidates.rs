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

use super::{Database, DatabaseReadTransaction};
use crate::error::Result;
use crate::schema::Catalog;
use crate::store::{GraphStore, SourceScanCandidateRead};
use hawdb_storage::{
    render_source_candidate_page, select_source_candidate, validate_source_candidate_scan_request,
    ScanSegmentFallback,
};
use std::num::{NonZeroU64, NonZeroUsize};

const SOURCE_SCAN_IO_DEPTH: usize = 2;
const SOURCE_SCAN_MAX_COALESCED_BYTES: u64 = 512 * 1024;
const SOURCE_SCAN_MAX_WAVE_BYTES: u64 = 2 * 1024 * 1024;
pub use hawdb_storage::{
    SourceCandidateRow as KnowledgeSourceCandidateRow,
    SourceCandidateScanOrigin as KnowledgeSourceCandidateScanOrigin,
    SourceCandidateScanOutput as KnowledgeSourceCandidateScanOutput,
    SourceCandidateScanRequest as KnowledgeSourceCandidateScanRequest,
};

impl Database {
    pub fn knowledge_source_candidates(
        &self,
        request: &KnowledgeSourceCandidateScanRequest,
    ) -> Result<KnowledgeSourceCandidateScanOutput> {
        knowledge_source_candidates(&self.catalog, &self.store, request)
    }
}

impl DatabaseReadTransaction {
    pub fn knowledge_source_candidates(
        &self,
        request: &KnowledgeSourceCandidateScanRequest,
    ) -> Result<KnowledgeSourceCandidateScanOutput> {
        knowledge_source_candidates(&self.catalog, &self.store, request)
    }
}

fn knowledge_source_candidates(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSourceCandidateScanRequest,
) -> Result<KnowledgeSourceCandidateScanOutput> {
    validate_source_candidate_scan_request(request)?;
    let graph_commit_epoch = store.commit_epoch();
    let source_label_id = catalog.label_id("Source");
    let (nodes, origin, read_report) = match store.read_published_source_scan_candidates(
        &request.predicate,
        NonZeroUsize::new(SOURCE_SCAN_IO_DEPTH).expect("non-zero I/O depth"),
        NonZeroU64::new(SOURCE_SCAN_MAX_COALESCED_BYTES).expect("non-zero coalesced range"),
        NonZeroU64::new(SOURCE_SCAN_MAX_WAVE_BYTES).expect("non-zero I/O wave"),
    ) {
        Ok(SourceScanCandidateRead::Rows {
            graph_epoch,
            skipped_segment_count,
            report,
            rows,
        }) => {
            let mut nodes = Vec::with_capacity(rows.len());
            for row in rows {
                let Some(node) = store.node_owned(crate::store::NodeId(row.node_id))? else {
                    return canonical_fallback(
                        catalog,
                        store,
                        request,
                        ScanSegmentFallback::NoManifest,
                    );
                };
                if source_label_id.is_none_or(|label_id| !node.labels.contains(&label_id))
                    || node.properties != row.properties
                {
                    return canonical_fallback(
                        catalog,
                        store,
                        request,
                        ScanSegmentFallback::NoManifest,
                    );
                }
                nodes.push(node);
            }
            (
                nodes,
                KnowledgeSourceCandidateScanOrigin::Sidecar {
                    graph_epoch,
                    skipped_segment_count,
                },
                Some(report),
            )
        }
        Ok(SourceScanCandidateRead::Fallback(reason)) => {
            return canonical_fallback(catalog, store, request, reason)
        }
        Err(_) => {
            return canonical_fallback(catalog, store, request, ScanSegmentFallback::NoManifest)
        }
    };

    render_source_candidate_page(graph_commit_epoch, nodes, request, origin, read_report)
}

fn canonical_fallback(
    catalog: &Catalog,
    store: &GraphStore,
    request: &KnowledgeSourceCandidateScanRequest,
    reason: ScanSegmentFallback,
) -> Result<KnowledgeSourceCandidateScanOutput> {
    let mut nodes = Vec::with_capacity(request.limit.saturating_add(1));
    let mut callback_error = None;
    if let Some(label_id) = catalog.label_id("Source") {
        store.visit_nodes_owned(Some(label_id), |node| {
            if let Err(error) = select_source_candidate(&mut nodes, node, request) {
                callback_error = Some(error);
                return crate::store::GraphScanControl::Stop;
            }
            crate::store::GraphScanControl::Continue
        })?;
    }
    if let Some(error) = callback_error {
        return Err(error);
    }
    render_source_candidate_page(
        store.commit_epoch(),
        nodes,
        request,
        KnowledgeSourceCandidateScanOrigin::CanonicalFallback { reason },
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseConfig, StorageResidencyMode, Value};
    use hawdb_storage::ScanPredicate;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "hawdb_source_candidates_{name}_{}_{}",
            std::process::id(),
            nonce
        ))
    }

    fn request() -> KnowledgeSourceCandidateScanRequest {
        KnowledgeSourceCandidateScanRequest {
            predicate: ScanPredicate::Eq {
                property: "source_type".to_string(),
                value: Value::String("file".to_string()),
            },
            after: None,
            limit: 8,
            max_payload_bytes: 1024 * 1024,
            property_names: vec!["id".to_string(), "source_type".to_string()],
        }
    }

    #[test]
    fn candidate_scan_reads_checkpointed_source_sidecar() {
        let directory = test_dir("sidecar");
        let mut db = Database::open(&directory).unwrap();
        db.query("CREATE (:Source {id: 'source-a', source_type: 'file'})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source-b', source_type: 'url'})")
            .unwrap();
        db.checkpoint().unwrap();

        let output = db.knowledge_source_candidates(&request()).unwrap();
        assert!(matches!(
            output.origin,
            KnowledgeSourceCandidateScanOrigin::Sidecar {
                skipped_segment_count: 0,
                ..
            }
        ));
        assert!(output.read_report.is_some());
        assert_eq!(output.rows.len(), 2);
        assert!(output
            .rows
            .iter()
            .any(|row| row.source_id.as_deref() == Some("source-a")));
        assert!(output
            .rows
            .iter()
            .all(|row| row.properties.contains_key("source_type")));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn candidate_scan_falls_back_after_uncheckpointed_source_write() {
        let directory = test_dir("fallback");
        let mut db = Database::open(&directory).unwrap();
        db.query("CREATE (:Source {id: 'source-a', source_type: 'file'})")
            .unwrap();
        db.checkpoint().unwrap();
        db.query("CREATE (:Source {id: 'source-new', source_type: 'file'})")
            .unwrap();

        let output = db.knowledge_source_candidates(&request()).unwrap();
        assert!(matches!(
            output.origin,
            KnowledgeSourceCandidateScanOrigin::CanonicalFallback { .. }
        ));
        assert!(output.read_report.is_none());
        assert!(output
            .rows
            .iter()
            .any(|row| row.source_id.as_deref() == Some("source-new")));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn candidate_scan_uses_created_at_and_node_id_cursor_without_repeating_rows() {
        let directory = test_dir("cursor");
        let mut db = Database::open(&directory).unwrap();
        db.query("CREATE (:Source {id: 'source-a', source_type: 'file', created_at: 10})")
            .unwrap();
        db.query("CREATE (:Source {id: 'source-b', source_type: 'file', created_at: 20})")
            .unwrap();
        db.checkpoint().unwrap();

        let mut first_request = request();
        first_request.limit = 1;
        let first = db.knowledge_source_candidates(&first_request).unwrap();
        let cursor = first.next_cursor.expect("second candidate cursor");
        assert_eq!(first.rows[0].source_id.as_deref(), Some("source-b"));

        let second = db
            .knowledge_source_candidates(&KnowledgeSourceCandidateScanRequest {
                after: Some(cursor),
                ..first_request
            })
            .unwrap();
        assert_eq!(second.rows.len(), 1);
        assert_eq!(second.rows[0].source_id.as_deref(), Some("source-a"));
        assert_ne!(second.rows[0].source_id, first.rows[0].source_id);
        assert!(second.next_cursor.is_none());
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn out_of_core_fallback_keeps_only_the_bounded_candidate_page() {
        let directory = test_dir("out_of_core_bounded_fallback");
        let mut db = Database::open_with_config(
            &directory,
            DatabaseConfig {
                storage_residency_mode: StorageResidencyMode::OutOfCore,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        for id in 0..100 {
            db.query_with_params(
                "CREATE (:Source {id: $id, source_type: 'file', created_at: $created_at})",
                &BTreeMap::from([
                    ("id".to_string(), Value::String(format!("source-{id}"))),
                    ("created_at".to_string(), Value::Int(id)),
                ]),
            )
            .unwrap();
        }
        db.checkpoint().unwrap();

        let mut bounded = request();
        bounded.limit = 3;
        let output = db.knowledge_source_candidates(&bounded).unwrap();
        assert!(matches!(
            output.origin,
            KnowledgeSourceCandidateScanOrigin::CanonicalFallback { .. }
        ));
        assert_eq!(output.rows.len(), 3);
        assert_eq!(output.rows[0].source_id.as_deref(), Some("source-99"));
        assert!(output.next_cursor.is_some());

        bounded.max_payload_bytes = 1;
        let error = db.knowledge_source_candidates(&bounded).unwrap_err();
        assert!(error.to_string().contains("payload budget exceeded"));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
