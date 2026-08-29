#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorCandidateSource {
    Scalar,
    Ann,
    Quantized,
}

impl VectorCandidateSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Ann => "ann",
            Self::Quantized => "quantized",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorBackendSelectionReason {
    CompressionDisabled,
    RecallValidationProbe,
    SmallFilteredCandidateSet,
    HighFilterSelectivity,
    RawVectorsWithinMemoryBudget,
    QuantizedPreferred,
    QuantizedRequired,
    QuantizedProjectionUnavailable,
    QuantizedProjectionCoverageIncomplete,
}

impl VectorBackendSelectionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CompressionDisabled => "compression_disabled",
            Self::RecallValidationProbe => "recall_validation_probe",
            Self::SmallFilteredCandidateSet => "small_filtered_candidate_set",
            Self::HighFilterSelectivity => "high_filter_selectivity",
            Self::RawVectorsWithinMemoryBudget => "raw_vectors_within_memory_budget",
            Self::QuantizedPreferred => "quantized_preferred",
            Self::QuantizedRequired => "quantized_required",
            Self::QuantizedProjectionUnavailable => "quantized_projection_unavailable",
            Self::QuantizedProjectionCoverageIncomplete => {
                "quantized_projection_coverage_incomplete"
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearchLogicalPlan {
    pub embedding_dimension: usize,
    pub filter_fields: Vec<String>,
    pub residual_filter_fields: Vec<String>,
    pub initial_candidate_limit: usize,
    pub candidate_source: VectorCandidateSource,
    pub candidate_limit: usize,
    pub top_k: usize,
}

/// Planner-owned resource ceilings carried with a vector physical plan.
///
/// Runtime admission may grant fewer resources, but execution must never
/// exceed these limits or silently replace them with process defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorExecutionResourceProfile {
    pub priority: u8,
    pub max_parallelism: usize,
    pub max_working_memory_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorPhysicalPlan {
    Filter {
        fields: Vec<String>,
    },
    VectorCandidateScan {
        source: VectorCandidateSource,
        embedding_dimension: usize,
        candidate_limit: usize,
        input: Box<VectorPhysicalPlan>,
    },
    ResidualFilter {
        fields: Vec<String>,
        initial_candidate_limit: usize,
        input: Box<VectorPhysicalPlan>,
    },
    RawVectorRerank {
        embedding_dimension: usize,
        input: Box<VectorPhysicalPlan>,
    },
    TopK {
        limit: usize,
        input: Box<VectorPhysicalPlan>,
    },
}

impl VectorPhysicalPlan {
    pub fn operator_name(&self) -> &'static str {
        match self {
            Self::Filter { .. } => "Filter",
            Self::VectorCandidateScan { .. } => "VectorCandidateScan",
            Self::ResidualFilter { .. } => "ResidualFilter",
            Self::RawVectorRerank { .. } => "RawVectorRerank",
            Self::TopK { .. } => "TopK",
        }
    }

    pub fn input(&self) -> Option<&Self> {
        match self {
            Self::Filter { .. } => None,
            Self::VectorCandidateScan { input, .. }
            | Self::ResidualFilter { input, .. }
            | Self::RawVectorRerank { input, .. }
            | Self::TopK { input, .. } => Some(input),
        }
    }

    pub fn operator_pipeline(&self) -> Vec<&'static str> {
        let mut operators = Vec::new();
        let mut current = Some(self);
        while let Some(plan) = current {
            operators.push(plan.operator_name());
            current = plan.input();
        }
        operators.reverse();
        operators
    }

    pub fn explain_summary(&self) -> String {
        match self {
            Self::TopK { limit, input } => format!(
                "pipeline={} top_k={limit} {}",
                self.operator_pipeline().join("->"),
                input.explain_details()
            ),
            _ => format!("pipeline={}", self.operator_pipeline().join("->")),
        }
    }

    pub fn fingerprint(&self) -> String {
        match self {
            Self::Filter { fields } => format!("Filter({})", fields.join(",")),
            Self::VectorCandidateScan {
                source,
                embedding_dimension,
                candidate_limit,
                input,
            } => format!(
                "VectorCandidateScan({}:{}:{}:{})",
                source.as_str(),
                embedding_dimension,
                candidate_limit,
                input.fingerprint()
            ),
            Self::ResidualFilter {
                fields,
                initial_candidate_limit,
                input,
            } => format!(
                "ResidualFilter({}:{}:{})",
                fields.join(","),
                initial_candidate_limit,
                input.fingerprint()
            ),
            Self::RawVectorRerank {
                embedding_dimension,
                input,
            } => format!(
                "RawVectorRerank({embedding_dimension}:{})",
                input.fingerprint()
            ),
            Self::TopK { limit, input } => {
                format!("TopK({limit}:{})", input.fingerprint())
            }
        }
    }

    fn explain_details(&self) -> String {
        match self {
            Self::RawVectorRerank {
                embedding_dimension,
                input,
            } => format!(
                "dimension={embedding_dimension} {}",
                input.explain_details()
            ),
            Self::ResidualFilter {
                fields,
                initial_candidate_limit,
                input,
            } => format!(
                "residual_fields={fields:?} initial_candidates={initial_candidate_limit} {}",
                input.explain_details()
            ),
            Self::VectorCandidateScan {
                source,
                candidate_limit,
                input,
                ..
            } => format!(
                "source={} candidates={candidate_limit} {}",
                source.as_str(),
                input.explain_details()
            ),
            Self::Filter { fields } => format!("filter_fields={fields:?}"),
            Self::TopK { input, .. } => input.explain_details(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::VectorBackendSelectionReason;

    #[test]
    fn backend_selection_reasons_have_stable_names() {
        assert_eq!(
            VectorBackendSelectionReason::CompressionDisabled.as_str(),
            "compression_disabled"
        );
        assert_eq!(
            VectorBackendSelectionReason::RecallValidationProbe.as_str(),
            "recall_validation_probe"
        );
        assert_eq!(
            VectorBackendSelectionReason::SmallFilteredCandidateSet.as_str(),
            "small_filtered_candidate_set"
        );
        assert_eq!(
            VectorBackendSelectionReason::HighFilterSelectivity.as_str(),
            "high_filter_selectivity"
        );
        assert_eq!(
            VectorBackendSelectionReason::RawVectorsWithinMemoryBudget.as_str(),
            "raw_vectors_within_memory_budget"
        );
        assert_eq!(
            VectorBackendSelectionReason::QuantizedPreferred.as_str(),
            "quantized_preferred"
        );
        assert_eq!(
            VectorBackendSelectionReason::QuantizedRequired.as_str(),
            "quantized_required"
        );
        assert_eq!(
            VectorBackendSelectionReason::QuantizedProjectionUnavailable.as_str(),
            "quantized_projection_unavailable"
        );
        assert_eq!(
            VectorBackendSelectionReason::QuantizedProjectionCoverageIncomplete.as_str(),
            "quantized_projection_coverage_incomplete"
        );
    }
}
