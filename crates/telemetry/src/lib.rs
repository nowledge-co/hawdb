// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(feature = "opentelemetry")]
use hawdb_qos::RuntimeTelemetryEventKind;
use hawdb_qos::{QosTelemetryEvent, QosTelemetrySink, RuntimeTelemetryEvent, RuntimeTelemetrySink};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTelemetry<'a> {
    pub query_language: &'a str,
    /// A literal-free query shape digest. Metrics adapters should avoid using
    /// this high-cardinality value as a metric attribute.
    pub query_digest: &'a str,
    pub statement_kind: &'a str,
    pub success: bool,
    pub elapsed_micros: u64,
    pub parse_nanos: u64,
    pub row_count: usize,
    pub intermediate_rows: usize,
    pub intermediate_payload_bytes: usize,
    pub output_payload_bytes: usize,
    pub steady_resident_bytes: Option<u64>,
    pub peak_resident_bytes: Option<u64>,
    pub total_page_faults: Option<u64>,
    pub minor_page_faults: Option<u64>,
    pub major_page_faults: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KernelTelemetryOperation {
    WalAppend,
    Checkpoint,
    Recovery,
    IndexMaintenance,
    SearchCheckpoint,
    BackgroundAdmission,
}

impl KernelTelemetryOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WalAppend => "wal_append",
            Self::Checkpoint => "checkpoint",
            Self::Recovery => "recovery",
            Self::IndexMaintenance => "index_maintenance",
            Self::SearchCheckpoint => "search_checkpoint",
            Self::BackgroundAdmission => "background_admission",
        }
    }
}

pub const REQUIRED_OPERATIONS_TELEMETRY: [KernelTelemetryOperation; 6] = [
    KernelTelemetryOperation::WalAppend,
    KernelTelemetryOperation::Checkpoint,
    KernelTelemetryOperation::Recovery,
    KernelTelemetryOperation::IndexMaintenance,
    KernelTelemetryOperation::SearchCheckpoint,
    KernelTelemetryOperation::BackgroundAdmission,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationsTelemetryReadiness {
    pub ready: bool,
    pub graph_sink_configured: bool,
    pub search_projection_sink_configured: bool,
    pub required_operations: Vec<KernelTelemetryOperation>,
    pub blocker_codes: Vec<String>,
}

pub fn operations_telemetry_readiness(
    graph_sink_configured: bool,
    search_projection_sink_configured: bool,
) -> OperationsTelemetryReadiness {
    let mut blocker_codes = Vec::new();
    if !graph_sink_configured {
        blocker_codes.push("operations_telemetry_graph_sink_missing".to_string());
    }
    if !search_projection_sink_configured {
        blocker_codes.push("operations_telemetry_search_projection_sink_missing".to_string());
    }
    OperationsTelemetryReadiness {
        ready: blocker_codes.is_empty(),
        graph_sink_configured,
        search_projection_sink_configured,
        required_operations: REQUIRED_OPERATIONS_TELEMETRY.to_vec(),
        blocker_codes,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelTelemetry {
    pub operation: KernelTelemetryOperation,
    pub success: bool,
    pub elapsed_micros: u64,
    pub item_count: usize,
    pub byte_count: u64,
    pub fsync_micros: u64,
    pub generation: Option<u64>,
}

pub trait TelemetrySink: Debug + Send + Sync {
    fn record_query(&self, event: QueryTelemetry<'_>);

    fn record_kernel(&self, _event: KernelTelemetry) {}

    fn record_qos(&self, _event: QosTelemetryEvent) {}

    fn record_runtime(&self, _event: RuntimeTelemetryEvent) {}
}

#[derive(Debug)]
struct HostQosTelemetrySink {
    telemetry: Arc<dyn TelemetrySink>,
}

impl QosTelemetrySink for HostQosTelemetrySink {
    fn record_qos(&self, event: QosTelemetryEvent) {
        self.telemetry.record_qos(event);
    }
}

pub fn qos_telemetry_sink(telemetry: Arc<dyn TelemetrySink>) -> Arc<dyn QosTelemetrySink> {
    Arc::new(HostQosTelemetrySink { telemetry })
}

#[derive(Debug)]
struct HostRuntimeTelemetrySink {
    telemetry: Arc<dyn TelemetrySink>,
}

impl RuntimeTelemetrySink for HostRuntimeTelemetrySink {
    fn record_runtime(&self, event: RuntimeTelemetryEvent) {
        self.telemetry.record_runtime(event);
    }
}

pub fn runtime_telemetry_sink(telemetry: Arc<dyn TelemetrySink>) -> Arc<dyn RuntimeTelemetrySink> {
    Arc::new(HostRuntimeTelemetrySink { telemetry })
}

#[cfg(feature = "opentelemetry")]
#[derive(Debug)]
pub struct OpenTelemetryMetrics {
    query_count: opentelemetry::metrics::Counter<u64>,
    query_duration_micros: opentelemetry::metrics::Histogram<u64>,
    query_parse_nanos: opentelemetry::metrics::Histogram<u64>,
    query_rows: opentelemetry::metrics::Histogram<u64>,
    query_intermediate_rows: opentelemetry::metrics::Histogram<u64>,
    query_intermediate_bytes: opentelemetry::metrics::Histogram<u64>,
    query_output_bytes: opentelemetry::metrics::Histogram<u64>,
    query_steady_resident_bytes: opentelemetry::metrics::Histogram<u64>,
    query_peak_resident_bytes: opentelemetry::metrics::Histogram<u64>,
    query_total_page_faults: opentelemetry::metrics::Histogram<u64>,
    query_minor_page_faults: opentelemetry::metrics::Histogram<u64>,
    query_major_page_faults: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_count: opentelemetry::metrics::Counter<u64>,
    kernel_operation_duration_micros: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_items: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_bytes: opentelemetry::metrics::Histogram<u64>,
    kernel_operation_fsync_micros: opentelemetry::metrics::Histogram<u64>,
    runtime_event_count: opentelemetry::metrics::Counter<u64>,
    runtime_admission_wait_micros: opentelemetry::metrics::Histogram<u64>,
}

#[cfg(feature = "opentelemetry")]
impl OpenTelemetryMetrics {
    pub fn new(meter: &opentelemetry::metrics::Meter) -> Self {
        Self {
            query_count: meter.u64_counter("hawdb.query.count").build(),
            query_duration_micros: meter
                .u64_histogram("hawdb.query.duration")
                .with_unit("us")
                .build(),
            query_parse_nanos: meter
                .u64_histogram("hawdb.query.parse.duration")
                .with_unit("ns")
                .build(),
            query_rows: meter.u64_histogram("hawdb.query.rows").build(),
            query_intermediate_rows: meter.u64_histogram("hawdb.query.intermediate.rows").build(),
            query_intermediate_bytes: meter
                .u64_histogram("hawdb.query.intermediate.bytes")
                .build(),
            query_output_bytes: meter.u64_histogram("hawdb.query.output.bytes").build(),
            query_steady_resident_bytes: meter.u64_histogram("hawdb.query.resident.steady").build(),
            query_peak_resident_bytes: meter.u64_histogram("hawdb.query.resident.peak").build(),
            query_total_page_faults: meter.u64_histogram("hawdb.query.page_faults.total").build(),
            query_minor_page_faults: meter.u64_histogram("hawdb.query.page_faults.minor").build(),
            query_major_page_faults: meter.u64_histogram("hawdb.query.page_faults.major").build(),
            kernel_operation_count: meter.u64_counter("hawdb.kernel.operation.count").build(),
            kernel_operation_duration_micros: meter
                .u64_histogram("hawdb.kernel.operation.duration")
                .with_unit("us")
                .build(),
            kernel_operation_items: meter.u64_histogram("hawdb.kernel.operation.items").build(),
            kernel_operation_bytes: meter.u64_histogram("hawdb.kernel.operation.bytes").build(),
            kernel_operation_fsync_micros: meter
                .u64_histogram("hawdb.kernel.operation.fsync_duration")
                .with_unit("us")
                .build(),
            runtime_event_count: meter.u64_counter("hawdb.runtime.event.count").build(),
            runtime_admission_wait_micros: meter
                .u64_histogram("hawdb.runtime.admission.wait.duration")
                .with_unit("us")
                .build(),
        }
    }
}

#[cfg(feature = "opentelemetry")]
impl TelemetrySink for OpenTelemetryMetrics {
    fn record_query(&self, event: QueryTelemetry<'_>) {
        use opentelemetry::KeyValue;

        let attributes = [
            KeyValue::new("db.system", "hawdb"),
            KeyValue::new("db.query.language", event.query_language.to_string()),
            KeyValue::new("db.operation.name", event.statement_kind.to_string()),
            KeyValue::new("error.type", if event.success { "" } else { "query_error" }),
        ];
        self.query_count.add(1, &attributes);
        self.query_duration_micros
            .record(event.elapsed_micros, &attributes);
        self.query_parse_nanos
            .record(event.parse_nanos, &attributes);
        self.query_rows.record(event.row_count as u64, &attributes);
        self.query_intermediate_rows
            .record(event.intermediate_rows as u64, &attributes);
        self.query_intermediate_bytes
            .record(event.intermediate_payload_bytes as u64, &attributes);
        self.query_output_bytes
            .record(event.output_payload_bytes as u64, &attributes);
        if let Some(bytes) = event.steady_resident_bytes {
            self.query_steady_resident_bytes.record(bytes, &attributes);
        }
        if let Some(bytes) = event.peak_resident_bytes {
            self.query_peak_resident_bytes.record(bytes, &attributes);
        }
        if let Some(faults) = event.total_page_faults {
            self.query_total_page_faults.record(faults, &attributes);
        }
        if let Some(faults) = event.minor_page_faults {
            self.query_minor_page_faults.record(faults, &attributes);
        }
        if let Some(faults) = event.major_page_faults {
            self.query_major_page_faults.record(faults, &attributes);
        }
    }

    fn record_kernel(&self, event: KernelTelemetry) {
        use opentelemetry::KeyValue;

        let attributes = [
            KeyValue::new("db.system", "hawdb"),
            KeyValue::new("db.operation.name", event.operation.as_str()),
            KeyValue::new(
                "error.type",
                if event.success {
                    ""
                } else {
                    "kernel_operation_error"
                },
            ),
        ];
        self.kernel_operation_count.add(1, &attributes);
        self.kernel_operation_duration_micros
            .record(event.elapsed_micros, &attributes);
        self.kernel_operation_items
            .record(event.item_count as u64, &attributes);
        self.kernel_operation_bytes
            .record(event.byte_count, &attributes);
        self.kernel_operation_fsync_micros
            .record(event.fsync_micros, &attributes);
    }

    fn record_qos(&self, event: QosTelemetryEvent) {
        use opentelemetry::KeyValue;

        let attributes = [
            KeyValue::new("db.system", "hawdb"),
            KeyValue::new("db.operation.name", "background_qos"),
            KeyValue::new("hawdb.qos.phase", event.phase.as_str()),
            KeyValue::new("hawdb.qos.outcome", event.outcome.as_str()),
            KeyValue::new("hawdb.work.class", event.class.as_str()),
            KeyValue::new(
                "hawdb.qos.admission_code",
                event.admission_code.map(|code| code.as_str()).unwrap_or(""),
            ),
        ];
        self.kernel_operation_count.add(1, &attributes);
        self.kernel_operation_duration_micros
            .record(event.elapsed_micros, &attributes);
        self.kernel_operation_items
            .record(event.estimated_operations as u64, &attributes);
    }

    fn record_runtime(&self, event: RuntimeTelemetryEvent) {
        use opentelemetry::KeyValue;

        let attributes = [
            KeyValue::new("db.system", "hawdb"),
            KeyValue::new("hawdb.runtime.event", event.kind.as_str()),
            KeyValue::new(
                "hawdb.work.priority",
                event
                    .priority
                    .map(|priority| priority.as_str())
                    .unwrap_or(""),
            ),
            KeyValue::new(
                "hawdb.work.kind",
                event.work_kind.map(|kind| kind.as_str()).unwrap_or(""),
            ),
            KeyValue::new(
                "hawdb.runtime.admission_code",
                event.admission_code.map(|code| code.as_str()).unwrap_or(""),
            ),
            KeyValue::new(
                "hawdb.runtime.retryable",
                event
                    .retryable
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            ),
        ];
        self.runtime_event_count.add(1, &attributes);
        if event.kind == RuntimeTelemetryEventKind::AdmissionWait {
            self.runtime_admission_wait_micros
                .record(event.elapsed_micros, &attributes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct RecordingSink {
        queries: Mutex<Vec<(bool, usize)>>,
    }

    impl TelemetrySink for RecordingSink {
        fn record_query(&self, event: QueryTelemetry<'_>) {
            self.queries
                .lock()
                .unwrap()
                .push((event.success, event.row_count));
        }
    }

    #[test]
    fn telemetry_contract_does_not_require_query_text() {
        let sink = RecordingSink::default();
        sink.record_query(QueryTelemetry {
            query_language: "cypher",
            query_digest: "q1:test",
            statement_kind: "read",
            success: true,
            elapsed_micros: 10,
            parse_nanos: 500,
            row_count: 2,
            intermediate_rows: 3,
            intermediate_payload_bytes: 32,
            output_payload_bytes: 16,
            steady_resident_bytes: None,
            peak_resident_bytes: None,
            total_page_faults: None,
            minor_page_faults: None,
            major_page_faults: None,
        });

        assert_eq!(*sink.queries.lock().unwrap(), vec![(true, 2)]);
    }

    #[test]
    fn readiness_requires_both_library_owned_sinks() {
        assert!(!operations_telemetry_readiness(true, false).ready);
        assert!(operations_telemetry_readiness(true, true).ready);
    }
}
