use super::*;
use skein_resource_profile::StorageResourceProfileObservation;

pub use skein_resource_profile::{
    StorageResourceProfileLimits, StorageResourceProfileReport, STORAGE_RESOURCE_PROFILE_PROTOCOL,
};

impl Database {
    pub fn storage_resource_profile(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
    ) -> Result<StorageResourceProfileReport> {
        self.storage_resource_profile_with_binding(
            cypher_text,
            parameters,
            limits,
            None,
            None,
            None,
        )
    }

    pub fn storage_resource_profile_for_production(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
    ) -> Result<StorageResourceProfileReport> {
        evidence_binding.validate_for(&expected_identity)?;
        let commit_epoch = self.commit_epoch();
        if evidence_binding.identity.canonical_graph_commit_epoch != commit_epoch {
            return Err(SkeinError::Semantic(format!(
                "production evidence canonical graph commit epoch {} does not match database epoch {commit_epoch}",
                evidence_binding.identity.canonical_graph_commit_epoch
            )));
        }
        self.storage_resource_profile_with_binding(
            cypher_text,
            parameters,
            limits,
            Some(evidence_binding),
            Some(expected_identity),
            None,
        )
    }

    pub(crate) fn storage_resource_profile_for_production_with_context(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
        evidence_binding: crate::ProductionEvidenceBinding,
        expected_identity: crate::ProductionQualificationIdentity,
        task_context: &skein_core::RuntimeTaskContext,
    ) -> Result<StorageResourceProfileReport> {
        evidence_binding.validate_for(&expected_identity)?;
        let commit_epoch = self.commit_epoch();
        if evidence_binding.identity.canonical_graph_commit_epoch != commit_epoch {
            return Err(SkeinError::Semantic(format!(
                "production evidence canonical graph commit epoch {} does not match database epoch {commit_epoch}",
                evidence_binding.identity.canonical_graph_commit_epoch
            )));
        }
        self.storage_resource_profile_with_binding(
            cypher_text,
            parameters,
            limits,
            Some(evidence_binding),
            Some(expected_identity),
            Some(task_context),
        )
    }

    fn storage_resource_profile_with_binding(
        &self,
        cypher_text: &str,
        parameters: &BTreeMap<String, Value>,
        limits: StorageResourceProfileLimits,
        evidence_binding: Option<crate::ProductionEvidenceBinding>,
        expected_identity: Option<crate::ProductionQualificationIdentity>,
        task_context: Option<&skein_core::RuntimeTaskContext>,
    ) -> Result<StorageResourceProfileReport> {
        limits.validate()?;
        let canonical_graph_commit_epoch = self.commit_epoch();
        let durable = self.storage_recovery_report().durable;
        let before = self.storage_residency_report();
        let mut read = self.begin_read_transaction();
        let stream_options = QueryStreamOptions {
            max_rows: Some(limits.max_output_rows),
            max_payload_bytes: Some(limits.max_output_payload_bytes),
        };
        let query = match task_context {
            Some(task_context) => read.query_with_params_streaming_context(
                cypher_text,
                parameters,
                stream_options,
                task_context,
                |_| Ok(()),
            ),
            None => read.query_with_params_streaming(
                cypher_text,
                parameters,
                stream_options,
                |_| Ok(()),
            ),
        }?;
        drop(read);
        let after = self.storage_residency_report();

        Ok(StorageResourceProfileReport::from_observation(
            StorageResourceProfileObservation {
                canonical_graph_commit_epoch,
                limits,
                durable,
                before,
                after,
                query,
                evidence_binding,
                expected_identity,
            },
        ))
    }
}
