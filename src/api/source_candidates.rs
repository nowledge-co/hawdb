use super::{Database, DatabaseReadTransaction};
use crate::error::{Result, SkeinError};
use crate::schema::Catalog;
use crate::store::{GraphStore, NodeRecord, SourceScanCandidateRead};
use crate::value::Value;
use skein_storage::{ScanPredicate, ScanSegmentFallback, SegmentReadExecutionReport};
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

const SOURCE_SCAN_IO_DEPTH: usize = 2;
const SOURCE_SCAN_MAX_COALESCED_BYTES: u64 = 512 * 1024;
const SOURCE_SCAN_MAX_WAVE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_SOURCE_CANDIDATE_ROWS: usize = 10_000;

/// A bounded Source candidate scan. The predicate is used for storage pruning;
/// callers must retain semantic residual evaluation for unsupported terms.
#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeSourceCandidateScanRequest {
    pub predicate: ScanPredicate,
    pub after: Option<KnowledgeSourceCandidateCursor>,
    pub limit: usize,
    pub max_payload_bytes: usize,
    pub property_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceCandidateCursor {
    pub created_at: Option<Value>,
    pub node_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceCandidateRow {
    pub node_id: u64,
    pub source_id: Option<String>,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnowledgeSourceCandidateScanOrigin {
    Sidecar {
        graph_epoch: u64,
        skipped_segment_count: usize,
    },
    CanonicalFallback {
        reason: ScanSegmentFallback,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSourceCandidateScanOutput {
    pub graph_commit_epoch: u64,
    pub rows: Vec<KnowledgeSourceCandidateRow>,
    pub next_cursor: Option<KnowledgeSourceCandidateCursor>,
    pub origin: KnowledgeSourceCandidateScanOrigin,
    pub read_report: Option<SegmentReadExecutionReport>,
}

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
    validate_request(request)?;
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

    render_page(graph_commit_epoch, nodes, request, origin, read_report)
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
            if let Err(error) = push_bounded_source_candidate(&mut nodes, node, request) {
                callback_error = Some(error);
                return crate::store::GraphScanControl::Stop;
            }
            crate::store::GraphScanControl::Continue
        })?;
    }
    if let Some(error) = callback_error {
        return Err(error);
    }
    render_page(
        store.commit_epoch(),
        nodes,
        request,
        KnowledgeSourceCandidateScanOrigin::CanonicalFallback { reason },
        None,
    )
}

fn render_page(
    graph_commit_epoch: u64,
    mut nodes: Vec<NodeRecord>,
    request: &KnowledgeSourceCandidateScanRequest,
    origin: KnowledgeSourceCandidateScanOrigin,
    read_report: Option<SegmentReadExecutionReport>,
) -> Result<KnowledgeSourceCandidateScanOutput> {
    let mut bounded = Vec::with_capacity(request.limit.saturating_add(1));
    for node in nodes.drain(..) {
        push_bounded_source_candidate(&mut bounded, node, request)?;
    }
    nodes = bounded;
    let has_more = nodes.len() > request.limit;
    nodes.truncate(request.limit);
    let next_cursor = has_more
        .then(|| {
            nodes.last().map(|node| KnowledgeSourceCandidateCursor {
                created_at: node.properties.get("created_at").cloned(),
                node_id: node.id.0,
            })
        })
        .flatten();
    let rows = nodes
        .into_iter()
        .map(|node| KnowledgeSourceCandidateRow {
            node_id: node.id.0,
            source_id: node
                .properties
                .get("id")
                .and_then(|value| matches!(value, Value::String(_)).then(|| value.clone()))
                .and_then(|value| match value {
                    Value::String(value) => Some(value),
                    _ => None,
                }),
            properties: node
                .properties
                .iter()
                .filter(|(name, _)| request.property_names.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
        })
        .collect::<Vec<_>>();
    Ok(KnowledgeSourceCandidateScanOutput {
        graph_commit_epoch,
        rows,
        next_cursor,
        origin,
        read_report,
    })
}

fn push_bounded_source_candidate(
    nodes: &mut Vec<NodeRecord>,
    mut node: NodeRecord,
    request: &KnowledgeSourceCandidateScanRequest,
) -> Result<()> {
    if request
        .after
        .as_ref()
        .is_some_and(|after| !compare_source_candidate_to_cursor(&node, after).is_gt())
    {
        return Ok(());
    }
    node.properties.retain(|name, _| {
        name == "id" || name == "created_at" || request.property_names.contains(name)
    });
    let insertion = nodes
        .binary_search_by(|existing| compare_source_candidates(existing, &node))
        .unwrap_or_else(|index| index);
    nodes.insert(insertion, node);
    if nodes.len() > request.limit.saturating_add(1) {
        nodes.pop();
    }
    let payload_bytes = nodes
        .iter()
        .map(estimated_source_candidate_payload_bytes)
        .fold(0usize, usize::saturating_add);
    if payload_bytes > request.max_payload_bytes {
        return Err(SkeinError::Execution(format!(
            "knowledge source candidate payload budget exceeded: estimated_payload_bytes={payload_bytes}, max_payload_bytes={}",
            request.max_payload_bytes
        )));
    }
    Ok(())
}

fn estimated_source_candidate_payload_bytes(node: &NodeRecord) -> usize {
    node.properties
        .iter()
        .fold(16usize, |bytes, (name, value)| {
            bytes
                .saturating_add(name.len())
                .saturating_add(estimated_value_bytes(value))
        })
}

fn estimated_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 1,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::Float(_) => 8,
        Value::String(value) => value.len(),
        Value::Binary(value) => value.len(),
        Value::Uuid(_) => 16,
        Value::List(values) => values
            .iter()
            .map(estimated_value_bytes)
            .fold(16usize, usize::saturating_add),
        Value::Map(values) => values.iter().fold(16usize, |bytes, (name, value)| {
            bytes
                .saturating_add(name.len())
                .saturating_add(estimated_value_bytes(value))
        }),
    }
}

fn compare_source_candidates(left: &NodeRecord, right: &NodeRecord) -> std::cmp::Ordering {
    compare_source_candidate_keys(
        left.properties.get("created_at"),
        left.id.0,
        right.properties.get("created_at"),
        right.id.0,
    )
}

fn compare_source_candidate_to_cursor(
    node: &NodeRecord,
    cursor: &KnowledgeSourceCandidateCursor,
) -> std::cmp::Ordering {
    compare_source_candidate_keys(
        node.properties.get("created_at"),
        node.id.0,
        cursor.created_at.as_ref(),
        cursor.node_id,
    )
}

fn compare_source_candidate_keys(
    left_created_at: Option<&Value>,
    left_node_id: u64,
    right_created_at: Option<&Value>,
    right_node_id: u64,
) -> std::cmp::Ordering {
    right_created_at
        .cmp(&left_created_at)
        .then_with(|| right_node_id.cmp(&left_node_id))
}

fn validate_request(request: &KnowledgeSourceCandidateScanRequest) -> Result<()> {
    if request.limit == 0 {
        return Err(SkeinError::Semantic(
            "knowledge source candidate scan requires a positive limit".to_string(),
        ));
    }
    if request.limit > MAX_SOURCE_CANDIDATE_ROWS {
        return Err(SkeinError::Semantic(format!(
            "knowledge source candidate scan limit {} exceeds {MAX_SOURCE_CANDIDATE_ROWS}",
            request.limit
        )));
    }
    if request.max_payload_bytes == 0 {
        return Err(SkeinError::Semantic(
            "knowledge source candidate scan requires a positive payload budget".to_string(),
        ));
    }
    if request
        .property_names
        .iter()
        .any(|name| name.trim().is_empty())
    {
        return Err(SkeinError::Semantic(
            "knowledge source candidate scan requires non-empty property names".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseConfig, StorageResidencyMode};
    use std::path::PathBuf;

    fn test_dir(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_source_candidates_{name}_{}_{}",
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
