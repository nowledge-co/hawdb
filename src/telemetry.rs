pub use skein_telemetry::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::{QosTelemetryEvent, QosTelemetryOutcome, QosTelemetryPhase};
    use crate::{
        Database, LocalQosPolicy, LocalQosScheduler, MetadataRepairOptions, SearchDocument,
        SearchIndex, SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
        SearchRebuildOptions,
    };
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Default)]
    struct RecordingSink {
        events: Mutex<Vec<(bool, u64, u64, usize)>>,
        kernel_events: Mutex<Vec<KernelTelemetry>>,
        qos_events: Mutex<Vec<QosTelemetryEvent>>,
    }

    impl TelemetrySink for RecordingSink {
        fn record_query(&self, event: QueryTelemetry<'_>) {
            self.events.lock().unwrap().push((
                event.success,
                event.elapsed_micros,
                event.parse_nanos,
                event.row_count,
            ));
        }

        fn record_kernel(&self, event: KernelTelemetry) {
            self.kernel_events.lock().unwrap().push(event);
        }

        fn record_qos(&self, event: QosTelemetryEvent) {
            self.qos_events.lock().unwrap().push(event);
        }
    }

    #[test]
    fn sink_contract_does_not_require_query_text_or_parameters() {
        let sink = RecordingSink::default();
        sink.record_query(QueryTelemetry {
            query_language: "cypher",
            query_digest: "q1:test",
            statement_kind: "match_return",
            success: true,
            elapsed_micros: 12,
            parse_nanos: 450,
            row_count: 3,
            intermediate_rows: 7,
            intermediate_payload_bytes: 128,
            output_payload_bytes: 64,
            steady_resident_bytes: Some(1024),
            peak_resident_bytes: Some(2048),
            total_page_faults: Some(4),
            minor_page_faults: Some(3),
            major_page_faults: Some(1),
        });

        assert_eq!(*sink.events.lock().unwrap(), vec![(true, 12, 450, 3)]);
    }

    #[test]
    fn kernel_sink_contract_uses_bounded_operation_kinds() {
        let sink = RecordingSink::default();
        sink.record_kernel(KernelTelemetry {
            operation: KernelTelemetryOperation::Checkpoint,
            success: true,
            elapsed_micros: 18,
            item_count: 4,
            byte_count: 128,
            fsync_micros: 7,
            generation: Some(3),
        });

        assert_eq!(
            *sink.kernel_events.lock().unwrap(),
            vec![KernelTelemetry {
                operation: KernelTelemetryOperation::Checkpoint,
                success: true,
                elapsed_micros: 18,
                item_count: 4,
                byte_count: 128,
                fsync_micros: 7,
                generation: Some(3),
            }]
        );
    }

    #[test]
    fn operations_telemetry_readiness_requires_host_owned_graph_and_search_sinks() {
        let missing = operations_telemetry_readiness(false, false);
        assert!(!missing.ready);
        assert_eq!(missing.required_operations, REQUIRED_OPERATIONS_TELEMETRY);
        assert!(missing
            .blocker_codes
            .contains(&"operations_telemetry_graph_sink_missing".to_string()));
        assert!(missing
            .blocker_codes
            .contains(&"operations_telemetry_search_projection_sink_missing".to_string()));

        let ready = operations_telemetry_readiness(true, true);
        assert!(ready.ready);
        assert!(ready.blocker_codes.is_empty());
        assert!(ready
            .required_operations
            .contains(&KernelTelemetryOperation::IndexMaintenance));
        assert!(ready
            .required_operations
            .contains(&KernelTelemetryOperation::BackgroundAdmission));
    }

    #[test]
    fn qos_adapter_forwards_only_typed_bounded_fields() {
        let sink = Arc::new(RecordingSink::default());
        let adapter = qos_telemetry_sink(sink.clone());

        adapter.record_qos(QosTelemetryEvent {
            phase: QosTelemetryPhase::Admission,
            outcome: QosTelemetryOutcome::Deferred,
            class: crate::qos::WorkClass::Projection,
            estimated_operations: 8,
            elapsed_micros: 0,
            admission_code: Some(crate::qos::QosAdmissionCode::PerWorkLimitExceeded),
        });

        assert_eq!(
            *sink.qos_events.lock().unwrap(),
            vec![QosTelemetryEvent {
                phase: QosTelemetryPhase::Admission,
                outcome: QosTelemetryOutcome::Deferred,
                class: crate::qos::WorkClass::Projection,
                estimated_operations: 8,
                elapsed_micros: 0,
                admission_code: Some(crate::qos::QosAdmissionCode::PerWorkLimitExceeded),
            }]
        );
    }

    #[test]
    fn scheduled_search_work_uses_the_host_telemetry_sink_automatically() {
        let sink = Arc::new(RecordingSink::default());
        let mut index = SearchIndex::in_memory();
        index.set_telemetry_sink(Some(sink.clone()));
        let mut scheduler = LocalQosScheduler::new(LocalQosPolicy::default());

        index
            .apply_scheduled_background_projection_delta(
                &mut scheduler,
                SearchProjectionDelta {
                    upserts: vec![SearchProjectionRow {
                        kind: SearchProjectionKind::Memory,
                        external_id: "qos-telemetry".to_string(),
                        title: "QoS telemetry".to_string(),
                        body: "Scheduled projection".to_string(),
                        embedding: None,
                        source_id: None,
                        metadata: BTreeMap::new(),
                    }],
                    ..SearchProjectionDelta::default()
                },
            )
            .unwrap();

        let events = sink.qos_events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].phase, QosTelemetryPhase::Admission);
        assert_eq!(events[0].outcome, QosTelemetryOutcome::Admitted);
        assert_eq!(events[1].phase, QosTelemetryPhase::Completion);
        assert_eq!(events[1].outcome, QosTelemetryOutcome::Completed);
    }

    #[test]
    fn database_emits_query_metrics_without_owning_a_global_provider() {
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::new();
        database.set_telemetry_sink(Some(sink.clone()));

        database
            .query("CREATE (:Memory {id: 'telemetry-1'})")
            .unwrap();

        let events = sink.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].0);
        assert!(events[0].2 > 0);
    }

    #[test]
    fn database_operations_telemetry_readiness_uses_configured_library_sinks() {
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::new();
        let mut search_index = SearchIndex::in_memory();

        let missing = database.operations_telemetry_readiness(Some(&search_index));
        assert!(!missing.ready);

        database.set_telemetry_sink(Some(sink.clone()));
        let graph_only = database.operations_telemetry_readiness(Some(&search_index));
        assert!(!graph_only.ready);
        assert!(graph_only.graph_sink_configured);
        assert!(!graph_only.search_projection_sink_configured);

        search_index.set_telemetry_sink(Some(sink));
        let ready = database.operations_telemetry_readiness(Some(&search_index));
        assert!(ready.ready);
        assert!(ready.blocker_codes.is_empty());
    }

    #[test]
    fn durable_database_emits_recovery_wal_and_checkpoint_metrics() {
        let path = unique_test_dir("kernel_storage");
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::open(&path).unwrap();
        database.set_telemetry_sink(Some(sink.clone()));

        database
            .query("CREATE (:Memory {id: 'telemetry-durable'})")
            .unwrap();
        database.checkpoint().unwrap();

        let events = sink.kernel_events.lock().unwrap();
        assert_eq!(
            events
                .iter()
                .map(|event| event.operation)
                .collect::<Vec<_>>(),
            vec![
                KernelTelemetryOperation::Recovery,
                KernelTelemetryOperation::WalAppend,
                KernelTelemetryOperation::Checkpoint,
            ]
        );
        assert!(events.iter().all(|event| event.success));
        assert_eq!(events[1].item_count, 1);
        drop(events);
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_checkpoint_emits_bounded_index_metrics() {
        let path = unique_test_dir("kernel_search");
        let sink = Arc::new(RecordingSink::default());
        let mut index = SearchIndex::open(&path).unwrap();
        index.set_telemetry_sink(Some(sink.clone()));
        index
            .upsert(SearchDocument {
                id: "memory:telemetry-search".to_string(),
                title: "Telemetry search".to_string(),
                content: "Bounded index metric".to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();

        index.checkpoint().unwrap();

        let events = sink.kernel_events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].operation,
            KernelTelemetryOperation::SearchCheckpoint
        );
        assert!(events[0].success);
        assert_eq!(events[0].item_count, 1);
        assert!(events[0].byte_count > 0);
        assert!(events[0]
            .generation
            .is_some_and(|generation| generation > 0));
        drop(events);
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_projection_rebuild_and_repair_emit_index_maintenance_metrics() {
        let sink = Arc::new(RecordingSink::default());
        let mut database = Database::new();
        database
            .query("CREATE (:Memory {id: 'telemetry-index', title: 'Index telemetry'})")
            .unwrap();
        let mut index = SearchIndex::in_memory();
        index.set_telemetry_sink(Some(sink.clone()));

        database
            .rebuild_search_projection(&mut index, SearchRebuildOptions::default())
            .unwrap();
        database
            .repair_search_projection_metadata(&mut index, MetadataRepairOptions::default())
            .unwrap();

        let events = sink.kernel_events.lock().unwrap();
        let index_events = events
            .iter()
            .filter(|event| event.operation == KernelTelemetryOperation::IndexMaintenance)
            .collect::<Vec<_>>();
        assert_eq!(index_events.len(), 2);
        assert!(index_events.iter().all(|event| event.success));
        assert!(index_events.iter().all(|event| event.item_count >= 1));
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_telemetry_{name}_{}_{}",
            std::process::id(),
            nonce
        ))
    }
}
