//! Compatibility module paths for contracts owned by internal crates.
//!
//! The embedded `skein` facade keeps these paths stable while the behavior,
//! types, and serialization contracts remain owned by their dedicated crates.

/// Compatibility facade for storage-neutral, redacted diagnostic evidence.
pub mod blackbox {
    pub use skein_evidence::blackbox::*;

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn blackbox_facade_preserves_public_function_and_type_identity() {
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
            let status: skein_evidence::blackbox::BlackboxRunStatus =
                crate::BlackboxRunStatus::Failed;
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
    }
}

/// Compatibility re-exports for evidence-owned background-maintenance preflight.
pub mod background_maintenance_evidence {
    pub use skein_evidence::background_maintenance_evidence::*;

    #[cfg(test)]
    mod tests {
        use super::nowledge_background_maintenance_evidence_json;

        #[test]
        fn root_compatibility_module_preserves_evidence_entrypoint() {
            let facade: fn(&serde_json::Value, bool) -> serde_json::Value =
                nowledge_background_maintenance_evidence_json;
            assert!(std::ptr::fn_addr_eq(
                facade,
                skein_evidence::background_maintenance_evidence::nowledge_background_maintenance_evidence_json
                    as fn(_, _) -> _,
            ));
        }
    }
}

/// Compatibility re-exports for the bounded-read evidence CLI adapter.
pub mod bounded_read_evidence {
    pub use skein_readiness::bounded_read_evidence_cli::*;

    #[cfg(test)]
    mod tests {
        use super::{parse_graph_route_readiness_json, parse_read_report_json};
        use crate::{NowledgeMemReadReport, NowledgeMemRouteReadinessSummary, Result};

        #[test]
        fn bounded_read_evidence_facade_preserves_contract_types() {
            let _: fn(&serde_json::Value) -> Result<NowledgeMemReadReport> = parse_read_report_json;
            let _: fn(&serde_json::Value) -> Result<NowledgeMemRouteReadinessSummary> =
                parse_graph_route_readiness_json;
        }
    }
}

/// Compatibility facade for storage crash-recovery evidence contracts.
pub mod crash_recovery_evidence {
    pub use skein_evidence::{
        StorageCrashCaseEvidence, StorageCrashPoint, StorageCrashRecoveryEvidence,
        STORAGE_CRASH_RECOVERY_EVIDENCE_PROTOCOL,
    };
}

/// Compatibility facade for `skein-cypher`.
pub mod cypher {
    pub use skein_cypher::*;
}

/// Compatibility re-exports for readiness-owned graph route CLI adapters.
pub mod graph_route_readiness {
    pub use skein_readiness::graph_route_cli::*;

    #[cfg(test)]
    mod tests {
        use super::nowledge_graph_route_readiness_json;

        #[test]
        fn root_compatibility_module_preserves_graph_route_readiness_entrypoint() {
            let facade: fn(&serde_json::Value) -> skein_core::Result<serde_json::Value> =
                nowledge_graph_route_readiness_json;
            assert!(std::ptr::fn_addr_eq(
                facade,
                skein_readiness::graph_route::nowledge_graph_route_readiness_json as fn(_) -> _,
            ));
        }
    }
}

/// Compatibility re-exports for readiness-owned integration bundle CLI adapters.
pub mod mem_integration_bundle {
    pub use skein_readiness::integration_bundle_cli::*;

    #[cfg(test)]
    mod tests {
        use skein_readiness::integration_bundle_cli;
        use std::any::TypeId;

        #[test]
        fn facade_preserves_owner_type_and_function_identity() {
            assert_eq!(
                TypeId::of::<crate::IntegrationBundleInputs>(),
                TypeId::of::<integration_bundle_cli::IntegrationBundleInputs>()
            );
            let bundle: fn(
                integration_bundle_cli::IntegrationBundleInputs,
            ) -> crate::Result<serde_json::Value> = crate::nowledge_mem_integration_bundle_json;
            assert!(std::ptr::fn_addr_eq(
                bundle,
                integration_bundle_cli::nowledge_mem_integration_bundle_json as fn(_) -> _,
            ));
            let runner: fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)> =
                crate::mem_integration_bundle::run_nowledge_mem_integration_bundle;
            assert!(std::ptr::fn_addr_eq(
                runner,
                integration_bundle_cli::run_nowledge_mem_integration_bundle
                    as fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)>,
            ));

            use skein_readiness::graph_summary;
            assert_eq!(
                TypeId::of::<crate::GraphRouteReadinessSummary>(),
                TypeId::of::<graph_summary::GraphRouteReadinessSummary>()
            );
            let summary: fn(&serde_json::Value) -> graph_summary::GraphRouteReadinessSummary =
                crate::nowledge_graph_route_readiness_summary;
            assert!(std::ptr::fn_addr_eq(
                summary,
                graph_summary::nowledge_graph_route_readiness_summary as fn(_) -> _,
            ));
            let from_bundle: fn(&serde_json::Value) -> graph_summary::GraphRouteReadinessSummary =
                crate::nowledge_graph_route_readiness_summary_from_bundle;
            assert!(std::ptr::fn_addr_eq(
                from_bundle,
                graph_summary::nowledge_graph_route_readiness_summary_from_bundle as fn(_) -> _,
            ));
        }
    }
}

/// Compatibility re-exports for readiness-owned integration cutover evaluation.
pub mod mem_integration_readiness {
    pub use skein_readiness::integration_readiness::*;

    #[cfg(test)]
    mod tests {
        use super::nowledge_mem_integration_readiness_json;

        #[test]
        fn root_compatibility_module_preserves_integration_entrypoint() {
            let facade: fn(&serde_json::Value) -> serde_json::Value =
                nowledge_mem_integration_readiness_json;
            assert!(std::ptr::fn_addr_eq(
                facade,
                skein_readiness::integration_readiness::nowledge_mem_integration_readiness_json
                    as fn(_) -> _,
            ));
        }
    }
}

/// Compatibility facade for `skein-optimizer`.
pub mod optimizer {
    pub use skein_optimizer::*;
    pub use skein_plan::{PhysicalOperatorDomain, PhysicalPlan, PhysicalPlanChildren};
}

/// Compatibility facade for `skein-plan`.
pub mod planner {
    pub use skein_plan::*;
}

/// Compatibility re-exports for the readiness-owned previous-wrapper preflight.
pub mod previous_wrapper_preflight {
    pub use skein_readiness::previous_wrapper_preflight::*;

    #[cfg(test)]
    mod tests {
        use super::{
            nowledge_previous_wrapper_preflight_check_usage,
            run_nowledge_previous_wrapper_preflight_check,
            NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
        };

        #[test]
        fn root_compatibility_module_preserves_preflight_entrypoint() {
            let error = run_nowledge_previous_wrapper_preflight_check(std::iter::empty())
                .expect_err("missing inputs must retain the public usage error");
            assert_eq!(
                error.to_string(),
                format!(
                    "semantic error: {}",
                    nowledge_previous_wrapper_preflight_check_usage()
                )
            );
            assert_eq!(
                NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
                skein_readiness::previous_wrapper_preflight::NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT_PROTOCOL,
            );
        }
    }
}

/// Compatibility facade for production qualification evidence contracts.
pub mod production_evidence {
    pub use skein_evidence::{
        ProductionEvidenceBinding, ProductionQualificationIdentity,
        PRODUCTION_QUALIFICATION_POLICY_VERSION,
    };
}

/// Compatibility facade for `skein-qos`.
pub mod qos {
    pub use skein_qos::*;
}

/// Compatibility re-exports for evidence-owned query-family preflight.
pub mod query_family_evidence {
    pub use skein_evidence::query_family_evidence::*;

    #[cfg(test)]
    mod tests {
        use super::nowledge_query_family_evidence_json;

        #[test]
        fn root_compatibility_module_preserves_evidence_entrypoint() {
            let facade: fn(&serde_json::Value) -> crate::Result<serde_json::Value> =
                nowledge_query_family_evidence_json;
            assert!(std::ptr::fn_addr_eq(
                facade,
                skein_evidence::query_family_evidence::nowledge_query_family_evidence_json
                    as fn(_) -> _,
            ));
        }
    }
}

/// Compatibility facade for graph route ownership and readiness contracts.
pub mod route_ownership {
    pub use skein_route_ownership::graph::{
        nowledge_mem_route_ownership_all_legacy, nowledge_mem_route_ownership_all_skein,
        nowledge_mem_route_ownership_for_engine, nowledge_mem_route_ownership_readiness,
        NowledgeMemRouteOwnership, NowledgeMemRouteOwnershipPolicy,
        NowledgeMemRouteOwnershipReadinessReport, NowledgeMemRouteReadEngine,
        NOWLEDGE_MEM_ROUTE_OWNERSHIP_PROTOCOL,
    };

    #[cfg(test)]
    mod facade_tests {
        use super::*;
        use skein_route_ownership::graph as owner;

        #[test]
        fn ownership_facade_preserves_concrete_types_and_function_signatures() {
            let summarize: fn(
                &[owner::NowledgeMemRouteOwnership],
                Option<&crate::NowledgeMemRouteReadinessSummary>,
                owner::NowledgeMemRouteOwnershipPolicy,
            ) -> crate::NowledgeMemRouteOwnershipReadinessReport =
                crate::nowledge_mem_route_ownership_readiness;
            let routes: Vec<owner::NowledgeMemRouteOwnership> =
                nowledge_mem_route_ownership_all_legacy();
            let report: NowledgeMemRouteOwnershipReadinessReport =
                summarize(&routes, None, NowledgeMemRouteOwnershipPolicy::migration());
            assert!(report.ready);
            assert!(!report.production_cutover_ready);
            assert_eq!(report.route_catalog_digest, "fnv1a64:80816a4f519d9693");
            assert_eq!(
                report,
                owner::nowledge_mem_route_ownership_readiness(
                    &routes,
                    None,
                    NowledgeMemRouteOwnershipPolicy::migration(),
                )
            );
        }

        #[test]
        fn catalog_facade_preserves_public_paths_and_concrete_types() {
            let lookup: fn(&str) -> Option<&'static owner::NowledgeMemGraphReadRouteSpec> =
                crate::nowledge_mem_graph_read_route_spec;
            let summary: Option<&owner::NowledgeMemRouteReadinessSummary> =
                None::<&crate::nowledge_mem::NowledgeMemRouteReadinessSummary>;
            assert!(summary.is_none());
            for expected in owner::NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS {
                assert_eq!(lookup(expected.route).unwrap(), expected);
                let actual: &crate::NowledgeMemGraphReadRouteSpec =
                    crate::nowledge_mem::nowledge_mem_graph_read_route_spec(expected.route)
                        .unwrap();
                assert_eq!(actual, expected);
                let _: crate::NowledgeMemGraphReadRouteOwner = expected.owner;
                let _: crate::NowledgeMemGraphReadRouteEvidenceKind =
                    expected.required_evidence_kind;
            }
            assert_eq!(
                crate::nowledge_mem::NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS,
                owner::NOWLEDGE_MEM_GRAPH_READ_ROUTE_SPECS
            );
            assert_eq!(
                crate::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES,
                owner::REQUIRED_NOWLEDGE_MEM_BOUNDED_READ_ROUTES
            );
            assert_eq!(
                crate::nowledge_mem_graph_read_route_specs_json(),
                owner::nowledge_mem_graph_read_route_specs_json()
            );
            assert_eq!(
                crate::nowledge_mem_graph_read_route_catalog_digest(),
                "fnv1a64:80816a4f519d9693"
            );
        }
    }
}

/// Compatibility re-exports for search-owned candidate evidence CLI adapters.
pub mod search_candidate_shadow_evidence {
    pub use skein_search::candidate_evidence_cli::*;

    #[cfg(test)]
    mod tests {
        use super::parse_search_candidate_shadow_probe;

        #[test]
        fn root_compatibility_module_preserves_candidate_probe_entrypoint() {
            let facade: fn(
                &serde_json::Value,
            ) -> skein_core::Result<
                skein_search::candidate_evidence::NowledgeMemSearchCandidateShadowAccumulator,
            > = parse_search_candidate_shadow_probe;
            assert!(std::ptr::fn_addr_eq(
                facade,
                skein_search::candidate_evidence::parse_search_candidate_shadow_probe as fn(_) -> _,
            ));
        }
    }
}

/// Compatibility re-exports for search-owned projection evidence CLI adapters.
pub mod search_projection_evidence {
    pub use skein_search::projection_evidence_cli::*;

    #[cfg(test)]
    mod tests {
        use skein_search::projection_evidence_cli;
        use std::any::TypeId;

        #[test]
        fn facade_preserves_owner_type_and_function_identity() {
            assert_eq!(
                TypeId::of::<crate::NowledgeSearchProjectionEvidenceReport>(),
                TypeId::of::<projection_evidence_cli::NowledgeSearchProjectionEvidenceReport>()
            );
            let evidence: fn(&serde_json::Value) -> serde_json::Value =
                crate::search_projection_evidence::nowledge_search_projection_evidence_json;
            assert!(std::ptr::fn_addr_eq(
                evidence,
                projection_evidence_cli::nowledge_search_projection_evidence_json as fn(_) -> _,
            ));
            let runner: fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)> =
                crate::search_projection_evidence::run_nowledge_search_projection_evidence;
            assert!(std::ptr::fn_addr_eq(
                runner,
                projection_evidence_cli::run_nowledge_search_projection_evidence
                    as fn(std::vec::IntoIter<String>) -> crate::Result<(serde_json::Value, bool)>,
            ));
        }
    }
}

/// Compatibility re-exports for evidence-owned storage-recovery preflight.
pub mod storage_recovery_evidence {
    pub use skein_evidence::storage_recovery_evidence::*;

    #[cfg(test)]
    mod tests {
        use super::nowledge_storage_recovery_evidence_json;

        #[test]
        fn root_compatibility_module_preserves_evidence_entrypoint() {
            let facade: fn(&serde_json::Value, bool) -> serde_json::Value =
                nowledge_storage_recovery_evidence_json;
            assert!(std::ptr::fn_addr_eq(
                facade,
                skein_evidence::storage_recovery_evidence::nowledge_storage_recovery_evidence_json
                    as fn(_, _) -> _,
            ));
        }
    }
}
