use super::*;

#[test]
fn blackbox_facade_preserves_public_function_and_type_identity() {
    // Both historical paths retain the owner's concrete types, not wrappers
    // that could make existing host function signatures diverge.
    type Report = fn(&crate::BlackboxReportOptions) -> crate::Result<crate::BlackboxReport>;
    type JsonReport = fn(&crate::BlackboxReportOptions) -> crate::Result<serde_json::Value>;
    let _: [Report; 4] = [
        crate::blackbox_report,
        blackbox_report,
        skein_evidence::blackbox::blackbox_report,
        crate::write_blackbox_report_typed,
    ];
    let _: [JsonReport; 4] = [
        crate::blackbox_report_json,
        blackbox_report_json,
        crate::write_blackbox_report,
        skein_evidence::blackbox::write_blackbox_report,
    ];
    let status: skein_evidence::blackbox::BlackboxRunStatus = crate::BlackboxRunStatus::Failed;
    assert_eq!(status, BlackboxRunStatus::Failed);
    assert_eq!(crate::BLACKBOX_REPORT_PROTOCOL, "skein-blackbox-report-v1");
    assert_eq!(crate::BLACKBOX_EVENT_PROTOCOL, "skein-blackbox-event-v1");
    for value in [
        serde_json::Value::Null,
        serde_json::json!({}),
        serde_json::json!([]),
    ] {
        let readiness: crate::BlackboxReadinessReport =
            blackbox_readiness_from_manifest_json(&value);
        assert!(!readiness.ready);
        assert_eq!(
            readiness,
            skein_evidence::blackbox::blackbox_readiness_from_manifest_json(&value),
        );
        assert_eq!(
            readiness,
            crate::blackbox_readiness_from_manifest_json(&value)
        );
    }
}
