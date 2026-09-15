use crate::{
    external_shadow_ready_missing_capabilities, external_shadow_trace_health_from_bundle,
    external_shadow_trace_report_json, ExternalShadowReady,
};
use skein_core::{Result, SkeinError};
use skein_evidence::inventory::{
    background_maintenance_evidence_health_from_bundle,
    replacement_readiness_family_evidence_health_from_bundle,
    storage_recovery_evidence_health_from_bundle, BackgroundMaintenanceEvidenceHealth,
    ReplacementReadinessFamilyEvidenceHealth, StorageRecoveryEvidenceHealth,
};

#[doc(hidden)]
pub fn add_shadow_ready_report(
    bundle: &mut serde_json::Value,
    ready: &ExternalShadowReady,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_ready".to_string(),
        serde_json::json!({
            "protocol_version": ready.protocol_version,
            "capabilities": &ready.capabilities,
            "engine_kind": &ready.engine_kind,
            "wrapper_identity": &ready.wrapper_identity,
        }),
    );
    Ok(())
}

#[doc(hidden)]
pub fn add_shadow_run_report(
    bundle: &mut serde_json::Value,
    shadow_name: &str,
    self_shadow: bool,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_run".to_string(),
        serde_json::json!({
            "shadow_name": shadow_name,
            "self_shadow": self_shadow,
            "evidence_kind": if self_shadow {
                "protocol_smoke"
            } else {
                "previous_wrapper"
            },
        }),
    );
    Ok(())
}

#[doc(hidden)]
pub fn add_shadow_trace_report(
    bundle: &mut serde_json::Value,
    trace_path: &str,
    request_count: u64,
) -> Result<()> {
    let object = bundle.as_object_mut().ok_or_else(|| {
        SkeinError::Execution("migration gate bundle must be a JSON object".to_string())
    })?;
    object.insert(
        "shadow_trace".to_string(),
        external_shadow_trace_report_json(trace_path, request_count),
    );
    Ok(())
}

#[doc(hidden)]
pub fn cutover_evidence_is_eligible(bundle: &serde_json::Value) -> bool {
    bundle
        .get("cutover_evidence")
        .and_then(|evidence| evidence.get("eligible"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

#[doc(hidden)]
pub struct ExternalShadowCutoverEvidence {
    pub eligible: bool,
    pub evidence_kind: &'static str,
    pub ready_preflight: bool,
    pub ready_engine_kind: Option<String>,
    pub ready_wrapper_identity: Option<String>,
    pub ready_missing_capabilities: Vec<&'static str>,
    pub shadow_evidence_present: bool,
    pub shadow_trace_health: crate::ExternalShadowTraceHealth,
    pub storage_recovery_health: StorageRecoveryEvidenceHealth,
    pub background_maintenance_health: BackgroundMaintenanceEvidenceHealth,
    pub replacement_family_health: ReplacementReadinessFamilyEvidenceHealth,
    pub migration_gate_ready: bool,
    pub blockers: Vec<String>,
}

#[doc(hidden)]
pub fn assess_external_shadow_cutover_evidence(
    bundle: &serde_json::Value,
    self_shadow: bool,
    shadow_ready: Option<&ExternalShadowReady>,
    storage_recovery_required: bool,
    background_maintenance_required: bool,
) -> Result<ExternalShadowCutoverEvidence> {
    let evidence_kind = if self_shadow {
        "protocol_smoke"
    } else {
        "previous_wrapper"
    };
    let ready_preflight = shadow_ready.is_some();
    let ready_engine_kind = shadow_ready.and_then(|ready| ready.engine_kind.clone());
    let ready_wrapper_identity = shadow_ready.and_then(|ready| ready.wrapper_identity.clone());
    let ready_missing_capabilities = external_shadow_ready_missing_capabilities(shadow_ready);
    let migration_gate = bundle
        .get("migration_gate")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            SkeinError::Execution("migration gate bundle missing migration_gate".to_string())
        })?;
    let migration_gate_ready = migration_gate
        .get("decision")
        .and_then(serde_json::Value::as_str)
        == Some("ready");
    let shadow_evidence_present = migration_gate
        .get("shadow_evidence_present")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let shadow_trace_health = external_shadow_trace_health_from_bundle(bundle);
    let storage_recovery_health =
        storage_recovery_evidence_health_from_bundle(bundle, storage_recovery_required);
    let background_maintenance_health =
        background_maintenance_evidence_health_from_bundle(bundle, background_maintenance_required);
    let replacement_family_health =
        replacement_readiness_family_evidence_health_from_bundle(bundle);
    let mut blockers = Vec::new();
    if self_shadow {
        blockers.push("shadow run is protocol smoke, not previous-wrapper evidence".to_string());
    }
    if !ready_preflight {
        blockers.push("shadow ready preflight was not executed".to_string());
    }
    if ready_preflight && ready_engine_kind.is_none() {
        blockers.push("shadow ready response missing engine_kind".to_string());
    }
    if ready_engine_kind
        .as_deref()
        .is_some_and(|kind| kind != "previous_wrapper")
    {
        blockers.push("shadow ready engine_kind is not previous_wrapper".to_string());
    }
    if ready_preflight
        && ready_engine_kind.as_deref() == Some("previous_wrapper")
        && ready_wrapper_identity.is_none()
    {
        blockers.push("shadow ready response missing wrapper_identity".to_string());
    }
    if ready_preflight && !ready_missing_capabilities.is_empty() {
        blockers.push("shadow ready response missing required capabilities".to_string());
    }
    if !shadow_evidence_present {
        blockers.push("no matched shadow checks are present".to_string());
    }
    if shadow_trace_health.present && !shadow_trace_health.complete {
        blockers.push("shadow trace is incomplete or unavailable".to_string());
    }
    if !storage_recovery_health.ready {
        blockers.extend(storage_recovery_health.blockers.iter().cloned());
    }
    if !background_maintenance_health.ready {
        blockers.extend(background_maintenance_health.blockers.iter().cloned());
    }
    if !replacement_family_health.ready {
        blockers.extend(replacement_family_health.blockers.iter().cloned());
    }
    if !migration_gate_ready {
        blockers.push("migration gate decision is not ready".to_string());
    }

    Ok(ExternalShadowCutoverEvidence {
        eligible: blockers.is_empty(),
        evidence_kind,
        ready_preflight,
        ready_engine_kind,
        ready_wrapper_identity,
        ready_missing_capabilities,
        shadow_evidence_present,
        shadow_trace_health,
        storage_recovery_health,
        background_maintenance_health,
        replacement_family_health,
        migration_gate_ready,
        blockers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_previous_wrapper_metadata_to_a_migration_gate_bundle() {
        let mut bundle = serde_json::json!({
            "migration_gate": { "decision": "ready" }
        });
        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec!["execute".to_string()],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        add_shadow_ready_report(&mut bundle, &ready).unwrap();
        add_shadow_run_report(&mut bundle, "legacy-wrapper", false).unwrap();

        assert_eq!(bundle["shadow_ready"]["protocol_version"], 1);
        assert_eq!(bundle["shadow_run"]["evidence_kind"], "previous_wrapper");
    }

    #[test]
    fn marks_protocol_smoke_and_extracts_cutover_eligibility() {
        let mut bundle = serde_json::json!({
            "migration_gate": { "decision": "ready" },
            "cutover_evidence": { "eligible": true }
        });

        add_shadow_run_report(&mut bundle, "skein-shadow-self", true).unwrap();

        assert_eq!(bundle["shadow_run"]["evidence_kind"], "protocol_smoke");
        assert!(cutover_evidence_is_eligible(&bundle));
    }

    #[test]
    fn cutover_policy_requires_previous_wrapper_evidence() {
        let bundle = serde_json::json!({
            "migration_gate": {
                "decision": "ready",
                "shadow_evidence_present": true
            }
        });
        let ready = ExternalShadowReady {
            protocol_version: 1,
            capabilities: vec![
                "execute".to_string(),
                "execute_session".to_string(),
                "project_graph".to_string(),
            ],
            engine_kind: Some("previous_wrapper".to_string()),
            wrapper_identity: Some("nowledge-previous-wrapper:test".to_string()),
        };

        let evidence =
            assess_external_shadow_cutover_evidence(&bundle, false, Some(&ready), false, false)
                .unwrap();

        assert!(evidence.eligible);
        let smoke =
            assess_external_shadow_cutover_evidence(&bundle, true, Some(&ready), false, false)
                .unwrap();
        assert!(!smoke.eligible);
        assert_eq!(
            smoke.blockers,
            vec!["shadow run is protocol smoke, not previous-wrapper evidence"]
        );
    }
}
