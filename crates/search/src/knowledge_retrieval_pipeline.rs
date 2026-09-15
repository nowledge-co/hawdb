use skein_core::{Result, SkeinError};
use skein_executor::{
    QueryMemoryClass, QueryMemoryLease, QueryMemoryLedger, QueryMemoryLedgerSnapshot,
};
use std::num::NonZeroUsize;

const STAGE_ORDER: [KnowledgeRetrievalStage; 6] = [
    KnowledgeRetrievalStage::SearchCandidate,
    KnowledgeRetrievalStage::MetadataFilter,
    KnowledgeRetrievalStage::AuthorizedGraphExpand,
    KnowledgeRetrievalStage::Rerank,
    KnowledgeRetrievalStage::TopK,
    KnowledgeRetrievalStage::CanonicalHydration,
];

/// Ordered stages of the bounded search-and-graph retrieval pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnowledgeRetrievalStage {
    SearchCandidate,
    MetadataFilter,
    AuthorizedGraphExpand,
    Rerank,
    TopK,
    CanonicalHydration,
}

impl KnowledgeRetrievalStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SearchCandidate => "search_candidate",
            Self::MetadataFilter => "metadata_filter",
            Self::AuthorizedGraphExpand => "authorized_graph_expand",
            Self::Rerank => "rerank",
            Self::TopK => "top_k",
            Self::CanonicalHydration => "canonical_hydration",
        }
    }
}

/// Memory and ordering evidence from a bounded retrieval pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeRetrievalPipelineReport {
    pub stages: Vec<KnowledgeRetrievalStage>,
    pub graph_snapshot_commit_epoch: u64,
    pub query_memory_budget_bytes: usize,
    pub peak_tracked_memory_bytes: usize,
    pub result_payload_budget_bytes: usize,
    pub result_payload_bytes: usize,
    pub canonical_identity_filtered_out_count: usize,
    pub canonical_output_hydrated_node_count: usize,
    pub canonical_output_hydrated_candidate_count: usize,
    pub canonical_output_hydration_after_top_k: bool,
    pub metadata_filter_authorized_graph_expansion: bool,
}

/// Internal ownership seam for bounded retrieval memory accounting.
#[doc(hidden)]
pub struct KnowledgeRetrievalPipelineBudget {
    ledger: QueryMemoryLedger,
    working: QueryMemoryLease,
    result: QueryMemoryLease,
    result_payload_budget: usize,
    result_payload_bytes: usize,
    stages: Vec<KnowledgeRetrievalStage>,
}

impl KnowledgeRetrievalPipelineBudget {
    pub fn new(query_memory_budget: NonZeroUsize, result_payload_budget: usize) -> Result<Self> {
        if result_payload_budget == 0 {
            return Err(SkeinError::Execution(
                "knowledge retrieval requires a positive result payload budget".to_string(),
            ));
        }
        let ledger = QueryMemoryLedger::new(query_memory_budget);
        let working = ledger
            .account(
                QueryMemoryClass::BlockingState,
                "knowledge_retrieval_working",
                query_memory_budget,
            )
            .reserve(0)?;
        let result = ledger
            .account(
                QueryMemoryClass::ResultMaterialization,
                "knowledge_retrieval_result",
                query_memory_budget,
            )
            .reserve(0)?;
        Ok(Self {
            ledger,
            working,
            result,
            result_payload_budget,
            result_payload_bytes: 0,
            stages: Vec::with_capacity(STAGE_ORDER.len()),
        })
    }

    pub fn enter(&mut self, stage: KnowledgeRetrievalStage) -> Result<()> {
        let expected = STAGE_ORDER.get(self.stages.len()).copied();
        if expected != Some(stage) {
            return Err(SkeinError::Execution(format!(
                "knowledge retrieval stage order violation: expected {}, got {}",
                expected.map_or("<complete>", KnowledgeRetrievalStage::as_str),
                stage.as_str(),
            )));
        }
        self.stages.push(stage);
        Ok(())
    }

    pub fn retain_working(&mut self, bytes: usize) -> Result<()> {
        self.working.grow(bytes)
    }

    pub fn retain_result(&mut self, memory_bytes: usize, payload_bytes: usize) -> Result<()> {
        let next_payload = self
            .result_payload_bytes
            .checked_add(payload_bytes)
            .ok_or_else(|| {
                SkeinError::Execution(
                    "knowledge retrieval result payload accounting overflow".to_string(),
                )
            })?;
        if next_payload > self.result_payload_budget {
            return Err(SkeinError::Execution(format!(
                "knowledge retrieval result uses {next_payload} payload bytes, exceeding max_read_result_payload_bytes {}",
                self.result_payload_budget
            )));
        }
        self.result.grow(memory_bytes)?;
        self.result_payload_bytes = next_payload;
        Ok(())
    }

    pub fn finish(
        self,
        graph_snapshot_commit_epoch: u64,
        canonical_identity_filtered_out_count: usize,
        canonical_output_hydrated_node_count: usize,
        canonical_output_hydrated_candidate_count: usize,
        metadata_filter_authorized_graph_expansion: bool,
    ) -> Result<KnowledgeRetrievalPipelineReport> {
        if self.stages.as_slice() != STAGE_ORDER {
            return Err(SkeinError::Execution(format!(
                "knowledge retrieval pipeline completed after {} of {} required stages",
                self.stages.len(),
                STAGE_ORDER.len()
            )));
        }
        let QueryMemoryLedgerSnapshot {
            budget_bytes,
            peak_bytes,
            ..
        } = self.ledger.snapshot();
        Ok(KnowledgeRetrievalPipelineReport {
            stages: self.stages,
            graph_snapshot_commit_epoch,
            query_memory_budget_bytes: budget_bytes,
            peak_tracked_memory_bytes: peak_bytes,
            result_payload_budget_bytes: self.result_payload_budget,
            result_payload_bytes: self.result_payload_bytes,
            canonical_identity_filtered_out_count,
            canonical_output_hydrated_node_count,
            canonical_output_hydrated_candidate_count,
            canonical_output_hydration_after_top_k: true,
            metadata_filter_authorized_graph_expansion,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{KnowledgeRetrievalPipelineBudget, KnowledgeRetrievalStage};
    use std::num::NonZeroUsize;

    fn budget() -> KnowledgeRetrievalPipelineBudget {
        KnowledgeRetrievalPipelineBudget::new(NonZeroUsize::new(1024).unwrap(), 128).unwrap()
    }

    #[test]
    fn enforces_stage_order_and_complete_delivery() {
        let mut pipeline = budget();
        let error = pipeline.enter(KnowledgeRetrievalStage::Rerank).unwrap_err();
        assert!(error.to_string().contains("expected search_candidate"));

        for stage in [
            KnowledgeRetrievalStage::SearchCandidate,
            KnowledgeRetrievalStage::MetadataFilter,
            KnowledgeRetrievalStage::AuthorizedGraphExpand,
            KnowledgeRetrievalStage::Rerank,
            KnowledgeRetrievalStage::TopK,
            KnowledgeRetrievalStage::CanonicalHydration,
        ] {
            pipeline.enter(stage).unwrap();
        }
        let report = pipeline.finish(7, 1, 2, 3, true).unwrap();
        assert_eq!(report.graph_snapshot_commit_epoch, 7);
        assert_eq!(report.result_payload_bytes, 0);
        assert!(report.canonical_output_hydration_after_top_k);
    }

    #[test]
    fn bounds_result_payload_before_memory_retention() {
        let mut pipeline = budget();
        let error = pipeline.retain_result(1, 129).unwrap_err();
        assert!(error
            .to_string()
            .contains("exceeding max_read_result_payload_bytes 128"));
    }
}
