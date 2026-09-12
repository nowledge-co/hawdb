use super::*;

#[test]
fn inventory_health_facade_preserves_owner_types_and_entrypoints() {
    let report = serde_json::json!({"protocol": "wrong", "readiness": {}});
    let owner: skein_evidence::inventory::StorageRecoveryEvidenceHealth =
        storage_recovery_evidence_health(Some(&report), true);
    let facade: crate::StorageRecoveryEvidenceHealth = owner.clone();
    assert_eq!(
        facade,
        crate::storage_recovery_evidence_health(Some(&report), true)
    );
    assert_eq!(
        owner,
        skein_evidence::inventory::storage_recovery_evidence_health(Some(&report), true)
    );

    let owner: skein_evidence::inventory::BackgroundMaintenanceEvidenceHealth =
        background_maintenance_evidence_health(Some(&report), true);
    let facade: crate::BackgroundMaintenanceEvidenceHealth = owner.clone();
    assert_eq!(
        facade,
        crate::background_maintenance_evidence_health(Some(&report), true)
    );
    assert_eq!(
        owner,
        skein_evidence::inventory::background_maintenance_evidence_health(Some(&report), true)
    );

    let families = serde_json::json!([]);
    let owner: skein_evidence::inventory::ReplacementReadinessFamilyEvidenceHealth =
        replacement_readiness_family_evidence_health(Some(&families));
    let facade: crate::ReplacementReadinessFamilyEvidenceHealth = owner.clone();
    assert_eq!(
        facade,
        crate::replacement_readiness_family_evidence_health(Some(&families))
    );
    assert_eq!(
        owner,
        skein_evidence::inventory::replacement_readiness_family_evidence_health(Some(&families))
    );
    assert_eq!(
        REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES,
        skein_evidence::inventory::REQUIRED_NOWLEDGE_REPLACEMENT_QUERY_FAMILIES
    );
}

#[test]
fn inventory_health_bundle_facade_preserves_optional_and_required_evidence() {
    for bundle in [
        serde_json::json!({}),
        serde_json::json!({
            "storage_recovery": null,
            "background_maintenance": null,
            "replacement_readiness_by_query_family": []
        }),
    ] {
        for required in [false, true] {
            assert_eq!(
                crate::storage_recovery_evidence_health_from_bundle(&bundle, required),
                skein_evidence::inventory::storage_recovery_evidence_health_from_bundle(
                    &bundle, required
                )
            );
            assert_eq!(
                crate::background_maintenance_evidence_health_from_bundle(&bundle, required),
                skein_evidence::inventory::background_maintenance_evidence_health_from_bundle(
                    &bundle, required
                )
            );
        }
        assert_eq!(
            crate::replacement_readiness_family_evidence_health_from_bundle(&bundle),
            skein_evidence::inventory::replacement_readiness_family_evidence_health_from_bundle(
                &bundle
            )
        );
    }
}
