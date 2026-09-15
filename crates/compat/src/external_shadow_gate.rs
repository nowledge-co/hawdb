use crate::{external_shadow_trace_report_json, ExternalShadowReady};
use skein_core::{Result, SkeinError};

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
}
