use crate::{
    EmbeddedQueryEntrypoint, EmbeddedQueryPathReadiness, QueryOutput, QueryStreamOptions,
    QueryStreamReport, Row, SkeinEmbedded, SkeinEmbeddedOpenOptions, SkeinError, Value,
};
use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};
use skein_qos::{RuntimeGovernorSnapshot, RuntimeWorkKind, RuntimeWorkRequest};
use skein_runtime_tokio::{
    tokio_bounded_channel, TokioBoundedReceiver, TokioBoundedTrySendError, TokioHandle,
    TokioRuntimeAdapter, TokioRuntimeConfig, TokioRuntimeError, TokioRuntimeOwnership,
    TokioTaskError,
};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

const DEFAULT_STREAM_CHANNEL_CAPACITY: usize = 2;

#[derive(Debug, Clone)]
pub struct SkeinTokioEmbedded {
    embedded: Arc<Mutex<SkeinEmbedded>>,
    runtime: TokioRuntimeAdapter,
}

#[derive(Debug)]
pub enum SkeinTokioEmbeddedError {
    Database(SkeinError),
    Runtime(TokioRuntimeError),
    Task(TokioTaskError<SkeinError>),
    StreamingMutation,
    StreamingUnsupported,
    StreamProducerClosed,
}

impl Display for SkeinTokioEmbeddedError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(error) => Display::fmt(error, formatter),
            Self::Runtime(error) => Display::fmt(error, formatter),
            Self::Task(error) => Display::fmt(error, formatter),
            Self::StreamingMutation => {
                formatter.write_str("mutation statements cannot use the asynchronous row stream")
            }
            Self::StreamingUnsupported => formatter.write_str(
                "the statement requires materialization and cannot use the asynchronous row stream",
            ),
            Self::StreamProducerClosed => formatter
                .write_str("the asynchronous row producer closed without a terminal report"),
        }
    }
}

impl Error for SkeinTokioEmbeddedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Task(error) => Some(error),
            Self::StreamingMutation | Self::StreamingUnsupported | Self::StreamProducerClosed => {
                None
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokioQueryStreamOptions {
    pub channel_capacity: NonZeroUsize,
}

impl Default for TokioQueryStreamOptions {
    fn default() -> Self {
        Self {
            channel_capacity: NonZeroUsize::new(DEFAULT_STREAM_CHANNEL_CAPACITY)
                .expect("the default stream channel capacity is non-zero"),
        }
    }
}

#[derive(Debug)]
enum TokioQueryStreamEvent {
    Batch(Vec<Row>),
    Finished(Box<QueryStreamReport>),
    Error(SkeinTokioEmbeddedError),
}

#[derive(Debug)]
pub struct TokioQueryBatchStream {
    receiver: TokioBoundedReceiver<TokioQueryStreamEvent>,
    cancellation: RuntimeCancellationToken,
    report: Option<QueryStreamReport>,
    terminated: bool,
}

impl TokioQueryBatchStream {
    pub async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, SkeinTokioEmbeddedError> {
        if self.terminated {
            return Ok(None);
        }
        match self.receiver.recv().await {
            Some(TokioQueryStreamEvent::Batch(batch)) => Ok(Some(batch)),
            Some(TokioQueryStreamEvent::Finished(report)) => {
                self.report = Some(*report);
                self.terminated = true;
                Ok(None)
            }
            Some(TokioQueryStreamEvent::Error(error)) => {
                self.terminated = true;
                Err(error)
            }
            None => {
                self.terminated = true;
                Err(SkeinTokioEmbeddedError::StreamProducerClosed)
            }
        }
    }

    pub fn report(&self) -> Option<&QueryStreamReport> {
        self.report.as_ref()
    }
}

impl Drop for TokioQueryBatchStream {
    fn drop(&mut self) {
        self.receiver.close();
        if !self.terminated {
            self.cancellation.cancel();
        }
    }
}

impl From<SkeinError> for SkeinTokioEmbeddedError {
    fn from(error: SkeinError) -> Self {
        Self::Database(error)
    }
}

impl From<TokioRuntimeError> for SkeinTokioEmbeddedError {
    fn from(error: TokioRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<TokioTaskError<SkeinError>> for SkeinTokioEmbeddedError {
    fn from(error: TokioTaskError<SkeinError>) -> Self {
        Self::Task(error)
    }
}

impl SkeinTokioEmbedded {
    pub fn open_owned(options: SkeinEmbeddedOpenOptions) -> Result<Self, SkeinTokioEmbeddedError> {
        let embedded = SkeinEmbedded::open_with_options(options)?;
        let config = TokioRuntimeConfig::from_governor(embedded.runtime_governor());
        Self::from_owned(embedded, config)
    }

    pub fn open_owned_with_config(
        options: SkeinEmbeddedOpenOptions,
        config: TokioRuntimeConfig,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        Self::from_owned(SkeinEmbedded::open_with_options(options)?, config)
    }

    pub fn open_borrowed(
        options: SkeinEmbeddedOpenOptions,
        handle: TokioHandle,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        let embedded = SkeinEmbedded::open_with_options(options)?;
        let config = TokioRuntimeConfig::from_governor(embedded.runtime_governor());
        Ok(Self::from_borrowed(embedded, handle, config))
    }

    pub fn open_borrowed_with_config(
        options: SkeinEmbeddedOpenOptions,
        handle: TokioHandle,
        config: TokioRuntimeConfig,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        let embedded = SkeinEmbedded::open_with_options(options)?;
        Ok(Self::from_borrowed(embedded, handle, config))
    }

    pub fn from_owned(
        embedded: SkeinEmbedded,
        config: TokioRuntimeConfig,
    ) -> Result<Self, SkeinTokioEmbeddedError> {
        let runtime = TokioRuntimeAdapter::owned(embedded.runtime_governor().clone(), config)?;
        Ok(Self {
            embedded: Arc::new(Mutex::new(embedded)),
            runtime,
        })
    }

    pub fn from_borrowed(
        embedded: SkeinEmbedded,
        handle: TokioHandle,
        config: TokioRuntimeConfig,
    ) -> Self {
        let runtime =
            TokioRuntimeAdapter::borrowed(handle, embedded.runtime_governor().clone(), config);
        Self {
            embedded: Arc::new(Mutex::new(embedded)),
            runtime,
        }
    }

    pub fn runtime(&self) -> &TokioRuntimeAdapter {
        &self.runtime
    }

    pub fn ownership(&self) -> TokioRuntimeOwnership {
        self.runtime.ownership()
    }

    pub fn runtime_snapshot(&self) -> RuntimeGovernorSnapshot {
        self.runtime.governor_snapshot()
    }

    pub fn admitted_query_path_readiness(&self) -> EmbeddedQueryPathReadiness {
        EmbeddedQueryPathReadiness::admitted(EmbeddedQueryEntrypoint::AdmittedTokio)
    }

    pub fn refresh_runtime_resources(&self) -> bool {
        lock_embedded(&self.embedded).refresh_runtime_resources()
    }

    pub fn with_embedded<R>(&self, operation: impl FnOnce(&SkeinEmbedded) -> R) -> R {
        operation(&lock_embedded(&self.embedded))
    }

    pub fn with_embedded_mut<R>(&self, operation: impl FnOnce(&mut SkeinEmbedded) -> R) -> R {
        operation(&mut lock_embedded(&self.embedded))
    }

    pub async fn query(
        &self,
        cypher_text: impl Into<String>,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        self.query_with_params(cypher_text, BTreeMap::new(), task_context)
            .await
    }

    pub async fn query_with_params(
        &self,
        cypher_text: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        let cypher_text = cypher_text.into();
        let prepared = self.with_embedded_mut(|embedded| {
            embedded
                .database_mut()
                .prepare_runtime_query(cypher_text, &parameters)
        })?;
        let result_budget_bytes = self.with_embedded(SkeinEmbedded::admitted_result_budget_bytes);
        let snapshot = self.with_embedded(|embedded| embedded.runtime_governor().snapshot());
        let request = prepared
            .admission()
            .runtime_work_request_for_snapshot(result_budget_bytes, snapshot);
        let streaming_eligible = prepared.admission().streaming_eligible;
        self.execute_query_with_request(
            prepared,
            parameters,
            request,
            streaming_eligible,
            task_context,
        )
        .await
    }

    pub async fn query_with_request(
        &self,
        cypher_text: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        request: RuntimeWorkRequest,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        let cypher_text = cypher_text.into();
        let prepared = self.with_embedded_mut(|embedded| {
            embedded
                .database_mut()
                .prepare_runtime_query(cypher_text, &parameters)
        })?;
        let admission = prepared.admission();
        let request = if admission.is_mutation {
            request
                .with_kind(RuntimeWorkKind::Mutation)
                .with_memory_bytes(request.memory_bytes.max(admission.estimated_memory_bytes))
                .with_result_bytes(0)
        } else if request.kind == RuntimeWorkKind::Mutation {
            request
                .with_kind(RuntimeWorkKind::Query)
                .with_memory_bytes(request.memory_bytes.max(admission.estimated_memory_bytes))
        } else {
            request.with_memory_bytes(request.memory_bytes.max(admission.estimated_memory_bytes))
        };
        let limits = self.with_embedded(|embedded| embedded.runtime_governor().snapshot().limits);
        let minimum_io_slots = admission.runtime_work_request(0, limits).io_slots;
        let request = request.with_io_slots(request.io_slots.max(minimum_io_slots));
        let result_budget_bytes = self.with_embedded(SkeinEmbedded::admitted_result_budget_bytes);
        let request = if admission.is_mutation {
            request
        } else if request.result_bytes > 0 {
            request.with_result_bytes(request.result_bytes.min(result_budget_bytes))
        } else {
            request.with_result_bytes(result_budget_bytes)
        };
        let streaming_eligible = admission.streaming_eligible;
        self.execute_query_with_request(
            prepared,
            parameters,
            request,
            streaming_eligible,
            task_context,
        )
        .await
    }

    pub async fn query_stream(
        &self,
        cypher_text: impl Into<String>,
        task_context: RuntimeTaskContext,
    ) -> Result<TokioQueryBatchStream, SkeinTokioEmbeddedError> {
        self.query_stream_with_params_and_options(
            cypher_text,
            BTreeMap::new(),
            TokioQueryStreamOptions::default(),
            task_context,
        )
        .await
    }

    pub async fn query_stream_with_params(
        &self,
        cypher_text: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        task_context: RuntimeTaskContext,
    ) -> Result<TokioQueryBatchStream, SkeinTokioEmbeddedError> {
        self.query_stream_with_params_and_options(
            cypher_text,
            parameters,
            TokioQueryStreamOptions::default(),
            task_context,
        )
        .await
    }

    pub async fn query_stream_with_options(
        &self,
        cypher_text: impl Into<String>,
        options: TokioQueryStreamOptions,
        task_context: RuntimeTaskContext,
    ) -> Result<TokioQueryBatchStream, SkeinTokioEmbeddedError> {
        self.query_stream_with_params_and_options(
            cypher_text,
            BTreeMap::new(),
            options,
            task_context,
        )
        .await
    }

    pub async fn query_stream_with_params_and_options(
        &self,
        cypher_text: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        options: TokioQueryStreamOptions,
        task_context: RuntimeTaskContext,
    ) -> Result<TokioQueryBatchStream, SkeinTokioEmbeddedError> {
        let cypher_text = cypher_text.into();
        let prepared = self.with_embedded_mut(|embedded| {
            embedded
                .database_mut()
                .prepare_runtime_query(cypher_text, &parameters)
        })?;
        let admission = prepared.admission();
        if admission.is_mutation {
            return Err(SkeinTokioEmbeddedError::StreamingMutation);
        }
        if !admission.streaming_eligible {
            return Err(SkeinTokioEmbeddedError::StreamingUnsupported);
        }

        let result_budget_bytes = self.with_embedded(SkeinEmbedded::admitted_result_budget_bytes);
        let (max_rows, batch_rows, batch_payload_bytes) = self.with_embedded(|embedded| {
            let config = embedded.database().config();
            (
                config.max_read_result_rows,
                config.execution_memory.batch_rows.get(),
                config.execution_memory.batch_payload_bytes.get(),
            )
        });
        let buffered_payload_bytes = u64::try_from(batch_payload_bytes)
            .unwrap_or(u64::MAX)
            .saturating_mul(
                u64::try_from(options.channel_capacity.get().saturating_add(1)).unwrap_or(u64::MAX),
            );
        let snapshot = self.with_embedded(|embedded| embedded.runtime_governor().snapshot());
        let request = admission.runtime_work_request_for_snapshot(result_budget_bytes, snapshot);
        let request =
            request.with_memory_bytes(request.memory_bytes.saturating_add(buffered_payload_bytes));
        let max_payload_bytes = usize::try_from(request.result_bytes).unwrap_or(usize::MAX);
        let producer_context = task_context.child().with_admitted_parallelism(
            NonZeroUsize::new(request.cpu_slots).unwrap_or(NonZeroUsize::MIN),
        );
        let cancellation = producer_context.cancellation().clone();
        let (terminal_sender, receiver) = tokio_bounded_channel(options.channel_capacity);
        let batch_sender = terminal_sender.clone();
        let embedded = Arc::clone(&self.embedded);
        let runtime = self.runtime.clone();
        let handle = runtime.handle().clone();
        handle.spawn(async move {
            let result = runtime
                .execute_blocking(request, producer_context, move |task_context| {
                    let mut read_transaction =
                        lock_embedded(&embedded).database().begin_read_transaction();
                    let mut batch = Vec::with_capacity(batch_rows);
                    let mut batch_bytes = 0usize;
                    let report = read_transaction.query_prepared_with_params_streaming_context(
                        prepared,
                        &parameters,
                        QueryStreamOptions {
                            max_rows,
                            max_payload_bytes: Some(max_payload_bytes),
                        },
                        task_context,
                        |row| {
                            let row_bytes = crate::executor::map_memory_bytes(&row);
                            if row_bytes > batch_payload_bytes {
                                return Err(SkeinError::Execution(format!(
                                    "asynchronous result row uses {row_bytes} bytes, exceeding batch_payload_bytes {batch_payload_bytes}"
                                )));
                            }
                            if !batch.is_empty()
                                && (batch.len() == batch_rows
                                    || batch_bytes.saturating_add(row_bytes)
                                        > batch_payload_bytes)
                            {
                                send_async_query_batch(
                                    &batch_sender,
                                    &mut batch,
                                    task_context,
                                )?;
                                batch_bytes = 0;
                            }
                            batch_bytes = batch_bytes.saturating_add(row_bytes);
                            batch.push(row);
                            Ok(())
                        },
                    )?;
                    if !batch.is_empty() {
                        send_async_query_batch(&batch_sender, &mut batch, task_context)?;
                    }
                    Ok(report)
                })
                .await;
            let event = match result {
                Ok(report) => TokioQueryStreamEvent::Finished(Box::new(report)),
                Err(error) => {
                    TokioQueryStreamEvent::Error(SkeinTokioEmbeddedError::Task(error))
                }
            };
            let _ = terminal_sender.send(event).await;
        });
        Ok(TokioQueryBatchStream {
            receiver,
            cancellation,
            report: None,
            terminated: false,
        })
    }

    async fn execute_query_with_request(
        &self,
        prepared: crate::api::PreparedRuntimeQuery,
        parameters: BTreeMap<String, Value>,
        request: RuntimeWorkRequest,
        streaming_eligible: bool,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, SkeinTokioEmbeddedError> {
        let embedded = Arc::clone(&self.embedded);
        let task_context = task_context.with_admitted_parallelism(
            NonZeroUsize::new(request.cpu_slots).unwrap_or(NonZeroUsize::MIN),
        );
        if request.kind == RuntimeWorkKind::Mutation {
            self.runtime
                .execute_blocking(request, task_context, move |task_context| {
                    lock_embedded(&embedded)
                        .database_mut()
                        .query_prepared_with_params_context(prepared, &parameters, task_context)
                })
                .await
                .map_err(SkeinTokioEmbeddedError::Task)
        } else {
            let max_rows =
                self.with_embedded(|embedded| embedded.database().config().max_read_result_rows);
            let max_payload_bytes = usize::try_from(request.result_bytes).unwrap_or(usize::MAX);
            self.runtime
                .execute_blocking(request, task_context, move |task_context| {
                    let mut read_transaction =
                        lock_embedded(&embedded).database().begin_read_transaction();
                    if !streaming_eligible {
                        return read_transaction.query_prepared_with_params_context(
                            prepared,
                            &parameters,
                            task_context,
                        );
                    }
                    let mut rows = Vec::new();
                    read_transaction.query_prepared_with_params_streaming_context(
                        prepared,
                        &parameters,
                        QueryStreamOptions {
                            max_rows,
                            max_payload_bytes: Some(max_payload_bytes),
                        },
                        task_context,
                        |row| {
                            rows.push(row);
                            Ok(())
                        },
                    )?;
                    Ok(QueryOutput { rows })
                })
                .await
                .map_err(SkeinTokioEmbeddedError::Task)
        }
    }
}

fn send_async_query_batch(
    sender: &skein_runtime_tokio::TokioBoundedSender<TokioQueryStreamEvent>,
    batch: &mut Vec<Row>,
    task_context: &RuntimeTaskContext,
) -> Result<(), SkeinError> {
    let capacity = batch.capacity();
    let ready = std::mem::replace(batch, Vec::with_capacity(capacity));
    let mut event = TokioQueryStreamEvent::Batch(ready);
    loop {
        task_context.checkpoint().map_err(|reason| {
            SkeinError::Execution(format!("asynchronous row producer stopped: {reason}"))
        })?;
        match sender.try_send(event) {
            Ok(()) => return Ok(()),
            Err(TokioBoundedTrySendError::Full(returned)) => {
                event = returned;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(TokioBoundedTrySendError::Closed(_)) => {
                return Err(SkeinError::Execution(
                    "asynchronous row consumer closed".to_string(),
                ));
            }
        }
    }
}

fn lock_embedded(embedded: &Mutex<SkeinEmbedded>) -> MutexGuard<'_, SkeinEmbedded> {
    embedded
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmbeddedDeploymentProfile;
    use skein_core::RuntimeCancellationToken;
    use skein_qos::{RuntimeTelemetryEvent, RuntimeTelemetryEventKind, RuntimeTelemetrySink};
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Default)]
    struct RuntimeEvents(Mutex<Vec<RuntimeTelemetryEvent>>);

    impl RuntimeTelemetrySink for RuntimeEvents {
        fn record_runtime(&self, event: RuntimeTelemetryEvent) {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event);
        }
    }

    #[test]
    fn owned_facade_runs_queries_through_the_bounded_adapter() {
        let path = unique_test_path("owned");
        let embedded =
            SkeinTokioEmbedded::open_owned(SkeinEmbeddedOpenOptions::new(&path)).unwrap();
        assert_eq!(embedded.ownership(), TokioRuntimeOwnership::Owned);
        let readiness = embedded.admitted_query_path_readiness();
        assert_eq!(readiness.entrypoint, EmbeddedQueryEntrypoint::AdmittedTokio);
        assert!(readiness.admission_safe);

        embedded
            .runtime()
            .block_on(embedded.query(
                "CREATE (:Memory {id: 'runtime'})",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        let output = embedded
            .runtime()
            .block_on(embedded.query(
                "MATCH (m:Memory) RETURN m.id AS id",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("runtime".to_string()))
        );
        assert_eq!(embedded.runtime_snapshot().completions, 2);
    }

    #[test]
    fn borrowed_facade_keeps_the_host_runtime_alive() {
        let path = unique_test_path("borrowed");
        let host = tokio_runtime();
        let embedded = SkeinTokioEmbedded::open_borrowed(
            SkeinEmbeddedOpenOptions::mobile(&path),
            host.handle().clone(),
        )
        .unwrap();
        assert_eq!(embedded.ownership(), TokioRuntimeOwnership::Borrowed);
        assert_eq!(
            embedded.with_embedded(SkeinEmbedded::deployment_profile),
            EmbeddedDeploymentProfile::MobileEmbedded
        );
        host.block_on(embedded.query("CREATE (:Probe {value: 1})", RuntimeTaskContext::default()))
            .unwrap();
        let output = host
            .block_on(embedded.query(
                "MATCH (p:Probe) RETURN p.value AS probe",
                RuntimeTaskContext::default(),
            ))
            .unwrap();
        assert_eq!(output.rows[0].get("probe"), Some(&Value::Int(1)));
        drop(embedded);
        assert_eq!(host.block_on(async { 9 }), 9);
    }

    #[test]
    fn admission_uses_physical_mutation_semantics() {
        let path = unique_test_path("admission-semantics");
        let mut embedded = SkeinEmbedded::open(&path).unwrap();
        let create = embedded
            .database_mut()
            .runtime_admission_plan("CREATE (:Probe {value: 1})", &BTreeMap::new())
            .unwrap();
        let read = embedded
            .database_mut()
            .runtime_admission_plan("MATCH (p:Probe) RETURN p.value AS value", &BTreeMap::new())
            .unwrap();

        assert_eq!(create.work_request.class, skein_qos::WorkClass::Query);
        assert!(create.is_mutation);
        assert!(!read.is_mutation);
        assert!(create.estimated_memory_bytes > 0);
        assert!(read.estimated_memory_bytes > 0);
        assert!(read.streaming_eligible);
    }

    #[test]
    fn admission_uses_database_execution_and_mutation_limits() {
        let path = unique_test_path("admission-config");
        let mut config = crate::DatabaseConfig::default();
        config.execution_memory.batch_payload_bytes = std::num::NonZeroUsize::new(1024).unwrap();
        config.execution_memory.blocking_operator_bytes =
            std::num::NonZeroUsize::new(4096).unwrap();
        config.max_wal_record_bytes = Some(1024);
        config.mutation_limits = skein_storage::MutationLimits {
            max_affected_rows: std::num::NonZeroUsize::new(3).unwrap(),
            max_operations: std::num::NonZeroUsize::new(2).unwrap(),
            max_result_rows: std::num::NonZeroUsize::new(4).unwrap(),
            max_result_payload_bytes: std::num::NonZeroUsize::new(5).unwrap(),
        };
        let mut embedded = SkeinEmbedded::open_with_options(
            SkeinEmbeddedOpenOptions::new(&path).with_config(config),
        )
        .unwrap();

        let read = embedded
            .database_mut()
            .runtime_admission_plan("MATCH (p:Probe) RETURN p.value AS value", &BTreeMap::new())
            .unwrap();
        let mutation = embedded
            .database_mut()
            .runtime_admission_plan("CREATE (:Probe {value: 1})", &BTreeMap::new())
            .unwrap();

        assert!((2 * 1024..64 * 1024).contains(&read.estimated_memory_bytes));
        assert_eq!(
            mutation.estimated_memory_bytes,
            2 * 1024 + 2 * 64 + 3 * 16 + 4 * 64 + 5
        );
    }

    #[test]
    fn cancelled_query_is_rejected_before_database_execution() {
        let path = unique_test_path("cancelled-before-start");
        let embedded =
            SkeinTokioEmbedded::open_owned(SkeinEmbeddedOpenOptions::new(&path)).unwrap();
        let token = RuntimeCancellationToken::new();
        token.cancel();
        let result = embedded
            .runtime()
            .block_on(embedded.query(
                "CREATE (:Probe {value: 1})",
                RuntimeTaskContext::without_deadline(token),
            ))
            .unwrap();

        assert!(matches!(
            result,
            Err(SkeinTokioEmbeddedError::Task(TokioTaskError::Stopped(
                skein_core::RuntimeCancellationReason::Cancelled
            )))
        ));
        let count = embedded
            .runtime()
            .block_on(embedded.query(
                "MATCH (p:Probe) RETURN p.value AS value",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        assert!(count.rows.is_empty());
    }

    #[test]
    fn custom_request_cannot_override_mutation_semantics() {
        let path = unique_test_path("custom-request-mutation");
        let embedded =
            SkeinTokioEmbedded::open_owned(SkeinEmbeddedOpenOptions::new(&path)).unwrap();
        let events = Arc::new(RuntimeEvents::default());
        embedded
            .runtime()
            .governor()
            .set_telemetry_sink(Some(events.clone()));
        embedded
            .runtime()
            .block_on(embedded.query_with_request(
                "CREATE (:Probe {value: 1})",
                BTreeMap::new(),
                RuntimeWorkRequest::foreground_query(0, 1024),
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();

        assert_eq!(embedded.runtime_snapshot().completions, 1);
        assert!(events
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .any(|event| {
                event.kind == RuntimeTelemetryEventKind::Admitted
                    && event.work_kind == Some(RuntimeWorkKind::Mutation)
            }));
    }

    #[test]
    fn default_query_enforces_the_governor_result_byte_budget() {
        let path = unique_test_path("result-byte-budget");
        let options = SkeinEmbeddedOpenOptions::new(&path).with_runtime_governor_config(
            skein_qos::RuntimeGovernorConfig {
                result_budget_bytes: 64,
                ..skein_qos::RuntimeGovernorConfig::desktop_bound()
            },
        );
        let embedded = SkeinTokioEmbedded::open_owned(options).unwrap();
        embedded
            .runtime()
            .block_on(embedded.query(
                format!("CREATE (:Probe {{value: '{}'}})", "x".repeat(256)),
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();

        let error = embedded
            .runtime()
            .block_on(embedded.query(
                "MATCH (p:Probe) RETURN p.value AS value",
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("max_payload_bytes 64"));
    }

    #[test]
    fn asynchronous_row_stream_delivers_bounded_batches_and_a_report() {
        let path = unique_test_path("bounded-row-stream");
        let mut config = crate::DatabaseConfig::default();
        config.execution_memory.batch_rows = NonZeroUsize::new(2).unwrap();
        config.execution_memory.batch_payload_bytes = NonZeroUsize::new(1024).unwrap();
        let embedded = SkeinTokioEmbedded::open_owned(
            SkeinEmbeddedOpenOptions::new(&path).with_config(config),
        )
        .unwrap();
        embedded.with_embedded_mut(|embedded| {
            for value in 0..7 {
                embedded
                    .database_mut()
                    .query(&format!("CREATE (:Probe {{value: {value}}})"))
                    .unwrap();
            }
        });

        let (rows, batch_sizes, report) = embedded
            .runtime()
            .block_on(async {
                let mut stream = embedded
                    .query_stream_with_options(
                        "MATCH (p:Probe) RETURN p.value AS value ORDER BY value",
                        TokioQueryStreamOptions {
                            channel_capacity: NonZeroUsize::new(1).unwrap(),
                        },
                        RuntimeTaskContext::default(),
                    )
                    .await
                    .unwrap();
                let mut rows = Vec::new();
                let mut batch_sizes = Vec::new();
                while let Some(batch) = stream.next_batch().await.unwrap() {
                    batch_sizes.push(batch.len());
                    rows.extend(batch);
                }
                (rows, batch_sizes, stream.report().cloned().unwrap())
            })
            .unwrap();

        assert_eq!(rows.len(), 7);
        assert!(batch_sizes.iter().all(|size| *size <= 2));
        assert!(batch_sizes.len() > 1);
        assert_eq!(report.output_rows, 7);
        assert!(report.output_payload_bytes > 0);
        assert_eq!(embedded.runtime_snapshot().completions, 1);
    }

    #[test]
    fn dropping_an_async_row_stream_cancels_its_admitted_producer() {
        let path = unique_test_path("cancel-row-stream");
        let mut config = crate::DatabaseConfig::default();
        config.execution_memory.batch_rows = NonZeroUsize::new(1).unwrap();
        config.execution_memory.batch_payload_bytes = NonZeroUsize::new(1024).unwrap();
        let embedded = SkeinTokioEmbedded::open_owned(
            SkeinEmbeddedOpenOptions::new(&path).with_config(config),
        )
        .unwrap();
        embedded.with_embedded_mut(|embedded| {
            for value in 0..128 {
                embedded
                    .database_mut()
                    .query(&format!("CREATE (:Probe {{value: {value}}})"))
                    .unwrap();
            }
        });

        let stream = embedded
            .runtime()
            .block_on(embedded.query_stream_with_options(
                "MATCH (p:Probe) RETURN p.value AS value",
                TokioQueryStreamOptions {
                    channel_capacity: NonZeroUsize::new(1).unwrap(),
                },
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        for _ in 0..100 {
            if embedded.runtime_snapshot().active_blocking_tasks == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(embedded.runtime_snapshot().active_blocking_tasks, 1);
        drop(stream);
        for _ in 0..100 {
            let snapshot = embedded.runtime_snapshot();
            if snapshot.active_blocking_tasks == 0 && snapshot.cancellations == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let snapshot = embedded.runtime_snapshot();
        assert_eq!(snapshot.active_blocking_tasks, 0);
        assert_eq!(snapshot.cancellations, 1);
    }

    #[test]
    fn async_row_stream_deadline_interrupts_a_backpressured_producer() {
        let path = unique_test_path("deadline-row-stream");
        let mut config = crate::DatabaseConfig::default();
        config.execution_memory.batch_rows = NonZeroUsize::new(1).unwrap();
        config.execution_memory.batch_payload_bytes = NonZeroUsize::new(1024).unwrap();
        let embedded = SkeinTokioEmbedded::open_owned(
            SkeinEmbeddedOpenOptions::new(&path).with_config(config),
        )
        .unwrap();
        embedded.with_embedded_mut(|embedded| {
            for value in 0..32 {
                embedded
                    .database_mut()
                    .query(&format!("CREATE (:Probe {{value: {value}}})"))
                    .unwrap();
            }
        });

        let mut stream = embedded
            .runtime()
            .block_on(embedded.query_stream_with_options(
                "MATCH (p:Probe) RETURN p.value AS value",
                TokioQueryStreamOptions {
                    channel_capacity: NonZeroUsize::new(1).unwrap(),
                },
                RuntimeTaskContext::with_timeout(std::time::Duration::from_millis(10)),
            ))
            .unwrap()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(30));
        assert!(
            embedded
                .runtime()
                .block_on(stream.next_batch())
                .unwrap()
                .unwrap()
                .unwrap()
                .len()
                <= 1
        );
        let error = embedded
            .runtime()
            .block_on(stream.next_batch())
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error,
            SkeinTokioEmbeddedError::Task(TokioTaskError::Stopped(
                skein_core::RuntimeCancellationReason::DeadlineExceeded
            ))
        ));
        assert_eq!(embedded.runtime_snapshot().deadline_exceeded, 1);
    }

    #[test]
    fn asynchronous_row_stream_rejects_mutations() {
        let path = unique_test_path("mutation-row-stream");
        let embedded =
            SkeinTokioEmbedded::open_owned(SkeinEmbeddedOpenOptions::new(&path)).unwrap();
        let error = embedded
            .runtime()
            .block_on(
                embedded.query_stream("CREATE (:Probe {value: 1})", RuntimeTaskContext::default()),
            )
            .unwrap()
            .unwrap_err();

        assert!(matches!(error, SkeinTokioEmbeddedError::StreamingMutation));
        assert_eq!(embedded.runtime_snapshot().admissions, 0);
    }

    fn tokio_runtime() -> skein_runtime_tokio::TokioRuntime {
        skein_runtime_tokio::TokioRuntimeBuilder::new_multi_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    fn unique_test_path(prefix: &str) -> std::path::PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "skein-tokio-embedded-{prefix}-{}-{id}",
            std::process::id()
        ))
    }
}
