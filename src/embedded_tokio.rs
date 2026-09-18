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

use crate::{
    EmbeddedQueryEntrypoint, EmbeddedQueryPathReadiness, HawDBEmbedded, HawDBEmbeddedOpenOptions,
    HawDBError, QueryOutput, QueryStreamOptions, QueryStreamReport, Row, Value,
};
use hawdb_core::{RuntimeCancellationToken, RuntimeTaskContext};
use hawdb_qos::{
    RuntimeGovernorSnapshot, RuntimeWorkKind, RuntimeWorkPriority, RuntimeWorkRequest,
};
use hawdb_runtime_tokio::{
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
const MAX_MUTATION_PLANNING_ATTEMPTS: usize = 3;

struct RuntimeQueryInput {
    cypher_text: String,
    parameters: BTreeMap<String, Value>,
}

impl RuntimeQueryInput {
    fn planning_request(&self, priority: RuntimeWorkPriority) -> RuntimeWorkRequest {
        crate::api::runtime_planning_request(self.cypher_text.len(), priority)
    }

    fn prepare_for_execution(
        &self,
        planning: &crate::api::RuntimePlanningSnapshot,
        admitted: &crate::api::RuntimeAdmissionPlan,
        context: &RuntimeTaskContext,
    ) -> Result<crate::api::PreparedRuntimeQuery, HawDBError> {
        context
            .checkpoint()
            .map_err(|reason| HawDBError::Execution(format!("runtime task stopped: {reason}")))?;
        let prepared = planning.prepare(self.cypher_text.clone(), &self.parameters)?;
        if prepared.admission() != admitted {
            return Err(HawDBError::Execution(
                "query admission changed during preparation; retry the query".to_string(),
            ));
        }
        Ok(prepared)
    }
}

#[derive(Debug, Clone)]
pub struct HawDBTokioEmbedded {
    embedded: Arc<Mutex<HawDBEmbedded>>,
    runtime: TokioRuntimeAdapter,
}

#[derive(Debug)]
pub enum HawDBTokioEmbeddedError {
    Database(HawDBError),
    Runtime(TokioRuntimeError),
    Task(TokioTaskError<HawDBError>),
    StreamingMutation,
    StreamingUnsupported,
    StreamProducerClosed,
}

impl Display for HawDBTokioEmbeddedError {
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

impl Error for HawDBTokioEmbeddedError {
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
    Error(HawDBTokioEmbeddedError),
}

#[derive(Debug)]
pub struct TokioQueryBatchStream {
    receiver: TokioBoundedReceiver<TokioQueryStreamEvent>,
    cancellation: RuntimeCancellationToken,
    report: Option<QueryStreamReport>,
    terminated: bool,
}

impl TokioQueryBatchStream {
    /// Returns the next batch or an in-band terminal error. A terminal error
    /// can follow batches that were already delivered.
    pub async fn next_batch(&mut self) -> Result<Option<Vec<Row>>, HawDBTokioEmbeddedError> {
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
                Err(HawDBTokioEmbeddedError::StreamProducerClosed)
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

impl From<HawDBError> for HawDBTokioEmbeddedError {
    fn from(error: HawDBError) -> Self {
        Self::Database(error)
    }
}

impl From<TokioRuntimeError> for HawDBTokioEmbeddedError {
    fn from(error: TokioRuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<TokioTaskError<HawDBError>> for HawDBTokioEmbeddedError {
    fn from(error: TokioTaskError<HawDBError>) -> Self {
        Self::Task(error)
    }
}

impl HawDBTokioEmbedded {
    pub fn open_owned(options: HawDBEmbeddedOpenOptions) -> Result<Self, HawDBTokioEmbeddedError> {
        let embedded = HawDBEmbedded::open_with_options(options)?;
        let config = TokioRuntimeConfig::from_governor(embedded.runtime_governor());
        Self::from_owned(embedded, config)
    }

    pub fn open_owned_with_config(
        options: HawDBEmbeddedOpenOptions,
        config: TokioRuntimeConfig,
    ) -> Result<Self, HawDBTokioEmbeddedError> {
        Self::from_owned(HawDBEmbedded::open_with_options(options)?, config)
    }

    pub fn open_borrowed(
        options: HawDBEmbeddedOpenOptions,
        handle: TokioHandle,
    ) -> Result<Self, HawDBTokioEmbeddedError> {
        let embedded = HawDBEmbedded::open_with_options(options)?;
        let config = TokioRuntimeConfig::from_governor(embedded.runtime_governor());
        Ok(Self::from_borrowed(embedded, handle, config))
    }

    pub fn open_borrowed_with_config(
        options: HawDBEmbeddedOpenOptions,
        handle: TokioHandle,
        config: TokioRuntimeConfig,
    ) -> Result<Self, HawDBTokioEmbeddedError> {
        let embedded = HawDBEmbedded::open_with_options(options)?;
        Ok(Self::from_borrowed(embedded, handle, config))
    }

    pub fn from_owned(
        embedded: HawDBEmbedded,
        config: TokioRuntimeConfig,
    ) -> Result<Self, HawDBTokioEmbeddedError> {
        let runtime = TokioRuntimeAdapter::owned(embedded.runtime_governor().clone(), config)?;
        Ok(Self {
            embedded: Arc::new(Mutex::new(embedded)),
            runtime,
        })
    }

    pub fn from_borrowed(
        embedded: HawDBEmbedded,
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

    pub fn with_embedded<R>(&self, operation: impl FnOnce(&HawDBEmbedded) -> R) -> R {
        operation(&lock_embedded(&self.embedded))
    }

    pub fn with_embedded_mut<R>(&self, operation: impl FnOnce(&mut HawDBEmbedded) -> R) -> R {
        operation(&mut lock_embedded(&self.embedded))
    }

    pub async fn query(
        &self,
        cypher_text: impl Into<String>,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, HawDBTokioEmbeddedError> {
        self.query_with_params(cypher_text, BTreeMap::new(), task_context)
            .await
    }

    pub async fn query_with_params(
        &self,
        cypher_text: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, HawDBTokioEmbeddedError> {
        let input = Arc::new(RuntimeQueryInput {
            cypher_text: cypher_text.into(),
            parameters,
        });
        let admission = self
            .prepare_admission(
                Arc::clone(&input),
                RuntimeWorkPriority::Foreground,
                task_context.clone(),
            )
            .await?;
        let result_budget_bytes = self.with_embedded(HawDBEmbedded::admitted_result_budget_bytes);
        let snapshot = self.with_embedded(|embedded| embedded.runtime_governor().snapshot());
        let request = admission.runtime_work_request_for_snapshot(result_budget_bytes, snapshot);
        let request_admission = admission.clone();
        self.execute_query_with_request_factory(
            input,
            admission,
            request,
            move |snapshot| {
                request_admission.runtime_work_request_for_snapshot(result_budget_bytes, snapshot)
            },
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
    ) -> Result<QueryOutput, HawDBTokioEmbeddedError> {
        let input = Arc::new(RuntimeQueryInput {
            cypher_text: cypher_text.into(),
            parameters,
        });
        let admission = self
            .prepare_admission(Arc::clone(&input), request.priority, task_context.clone())
            .await?;
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
        let request = apply_segment_io_requirement(request, minimum_io_slots);
        let result_budget_bytes = self.with_embedded(HawDBEmbedded::admitted_result_budget_bytes);
        let request = if admission.is_mutation {
            request
        } else if request.result_bytes > 0 {
            request.with_result_bytes(request.result_bytes.min(result_budget_bytes))
        } else {
            request.with_result_bytes(result_budget_bytes)
        };
        self.execute_query_with_request_factory(
            input,
            admission,
            request,
            move |_| request,
            task_context,
        )
        .await
    }

    pub async fn query_stream(
        &self,
        cypher_text: impl Into<String>,
        task_context: RuntimeTaskContext,
    ) -> Result<TokioQueryBatchStream, HawDBTokioEmbeddedError> {
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
    ) -> Result<TokioQueryBatchStream, HawDBTokioEmbeddedError> {
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
    ) -> Result<TokioQueryBatchStream, HawDBTokioEmbeddedError> {
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
    ) -> Result<TokioQueryBatchStream, HawDBTokioEmbeddedError> {
        let input = Arc::new(RuntimeQueryInput {
            cypher_text: cypher_text.into(),
            parameters,
        });
        let admission = self
            .prepare_admission(
                Arc::clone(&input),
                RuntimeWorkPriority::Foreground,
                task_context.clone(),
            )
            .await?;
        if admission.is_mutation {
            return Err(HawDBTokioEmbeddedError::StreamingMutation);
        }
        if !admission.streaming_eligible {
            return Err(HawDBTokioEmbeddedError::StreamingUnsupported);
        }

        let result_budget_bytes = self.with_embedded(HawDBEmbedded::admitted_result_budget_bytes);
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
        let executor_memory_bytes = request.memory_bytes;
        let request =
            request.with_memory_bytes(request.memory_bytes.saturating_add(buffered_payload_bytes));
        let max_payload_bytes = usize::try_from(request.result_bytes).unwrap_or(usize::MAX);
        let producer_context = task_context.child();
        let cancellation = producer_context.cancellation().clone();
        let (terminal_sender, receiver) = tokio_bounded_channel(options.channel_capacity);
        let batch_sender = terminal_sender.clone();
        let embedded = Arc::clone(&self.embedded);
        let runtime = self.runtime.clone();
        let handle = runtime.handle().clone();
        let selected_executor_memory =
            Arc::new(std::sync::atomic::AtomicU64::new(executor_memory_bytes));
        let request_memory = Arc::clone(&selected_executor_memory);
        let request_admission = admission.clone();
        let planning_memory_bytes = input.planning_request(request.priority).memory_bytes;
        let request_for_snapshot = move |snapshot| {
            let request =
                request_admission.runtime_work_request_for_snapshot(result_budget_bytes, snapshot);
            request_memory.store(request.memory_bytes, std::sync::atomic::Ordering::Release);
            request.with_memory_bytes(
                request
                    .memory_bytes
                    .max(planning_memory_bytes)
                    .saturating_add(buffered_payload_bytes),
            )
        };
        let execute_stream = move |task_context: &RuntimeTaskContext| {
            let task_context = task_context.clone().with_memory_reservation(
                hawdb_core::RuntimeMemoryReservation::new(
                    selected_executor_memory.load(std::sync::atomic::Ordering::Acquire),
                    request.result_bytes,
                ),
            );
            let (planning, mut read_transaction) = {
                let embedded = lock_embedded(&embedded);
                let database = embedded.database();
                (
                    database.runtime_planning_snapshot(),
                    database.begin_read_transaction(),
                )
            };
            let prepared = input.prepare_for_execution(&planning, &admission, &task_context)?;
            drop(planning);
            let mut batch = Vec::with_capacity(batch_rows);
            let mut batch_bytes = 0usize;
            let report = read_transaction.query_prepared_with_params_streaming_context(
                prepared,
                &input.parameters,
                QueryStreamOptions {
                    max_rows,
                    max_payload_bytes: Some(max_payload_bytes),
                },
                crate::executor::StreamDelivery::Incremental,
                &task_context,
                |row| {
                    let row_bytes = crate::executor::map_memory_bytes(&row);
                    if row_bytes > batch_payload_bytes {
                        return Err(HawDBError::Execution(format!(
                            "asynchronous result row uses {row_bytes} bytes, exceeding batch_payload_bytes {batch_payload_bytes}"
                        )));
                    }
                    if !batch.is_empty()
                        && (batch.len() == batch_rows
                            || batch_bytes.saturating_add(row_bytes) > batch_payload_bytes)
                    {
                        send_async_query_batch(&batch_sender, &mut batch, &task_context)?;
                        batch_bytes = 0;
                    }
                    batch_bytes = batch_bytes.saturating_add(row_bytes);
                    batch.push(row);
                    Ok(())
                },
            )?;
            if !batch.is_empty() {
                send_async_query_batch(&batch_sender, &mut batch, &task_context)?;
            }
            Ok(report)
        };
        handle.spawn(async move {
            let result = runtime
                .execute_blocking_with_request_factory(
                    request_for_snapshot,
                    producer_context,
                    execute_stream,
                )
                .await;
            let event = match result {
                Ok(report) => TokioQueryStreamEvent::Finished(Box::new(report)),
                Err(error) => TokioQueryStreamEvent::Error(HawDBTokioEmbeddedError::Task(error)),
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

    async fn prepare_admission(
        &self,
        input: Arc<RuntimeQueryInput>,
        priority: RuntimeWorkPriority,
        task_context: RuntimeTaskContext,
    ) -> Result<crate::api::RuntimeAdmissionPlan, HawDBTokioEmbeddedError> {
        let embedded = Arc::clone(&self.embedded);
        self.runtime
            .execute_blocking(
                input.planning_request(priority),
                task_context,
                move |context| {
                    let planning = lock_embedded(&embedded)
                        .database()
                        .runtime_planning_snapshot();
                    // Only this fixed-size descriptor may outlive the planning permit.
                    // Parsed and optimized state is dropped before the operation returns.
                    context.checkpoint().map_err(|reason| {
                        HawDBError::Execution(format!("runtime task stopped: {reason}"))
                    })?;
                    let prepared =
                        planning.prepare(input.cypher_text.clone(), &input.parameters)?;
                    Ok(prepared.admission().clone())
                },
            )
            .await
            .map_err(HawDBTokioEmbeddedError::Task)
    }

    async fn execute_query_with_request_factory<R>(
        &self,
        input: Arc<RuntimeQueryInput>,
        admission: crate::api::RuntimeAdmissionPlan,
        request: RuntimeWorkRequest,
        mut request_for_snapshot: R,
        task_context: RuntimeTaskContext,
    ) -> Result<QueryOutput, HawDBTokioEmbeddedError>
    where
        R: FnMut(hawdb_qos::RuntimeGovernorSnapshot) -> RuntimeWorkRequest + Send,
    {
        let embedded = Arc::clone(&self.embedded);
        let planning_memory_bytes = input.planning_request(request.priority).memory_bytes;
        let request_for_snapshot = move |snapshot| {
            let request = request_for_snapshot(snapshot);
            request
                .with_cpu_slots(request.cpu_slots.max(1))
                .with_memory_bytes(request.memory_bytes.max(planning_memory_bytes))
        };
        if request.kind == RuntimeWorkKind::Mutation {
            self.runtime
                .execute_blocking_with_request_factory(
                    request_for_snapshot,
                    task_context,
                    move |task_context| {
                        for _ in 0..MAX_MUTATION_PLANNING_ATTEMPTS {
                            let planning = lock_embedded(&embedded)
                                .database()
                                .runtime_planning_snapshot();
                            let prepared =
                                input.prepare_for_execution(&planning, &admission, task_context)?;
                            let mut embedded = lock_embedded(&embedded);
                            if planning.is_current_for(embedded.database(), &prepared) {
                                return embedded.database_mut().query_prepared_with_params_context(
                                    prepared,
                                    &input.parameters,
                                    task_context,
                                );
                            }
                            // Drop both the stale plan and lock before another planning attempt.
                        }
                        Err(HawDBError::Execution(
                            "database changed during mutation planning; retry the query"
                                .to_string(),
                        ))
                    },
                )
                .await
                .map_err(HawDBTokioEmbeddedError::Task)
        } else {
            let max_rows =
                self.with_embedded(|embedded| embedded.database().config().max_read_result_rows);
            let max_payload_bytes = usize::try_from(request.result_bytes).unwrap_or(usize::MAX);
            self.runtime
                .execute_blocking_with_request_factory(
                    request_for_snapshot,
                    task_context,
                    move |task_context| {
                        let (planning, mut read_transaction) = {
                            let embedded = lock_embedded(&embedded);
                            let database = embedded.database();
                            (
                                database.runtime_planning_snapshot(),
                                database.begin_read_transaction(),
                            )
                        };
                        let prepared =
                            input.prepare_for_execution(&planning, &admission, task_context)?;
                        drop(planning);
                        if !admission.streaming_eligible {
                            return read_transaction.query_prepared_with_params_context(
                                prepared,
                                &input.parameters,
                                task_context,
                            );
                        }
                        let mut rows = Vec::new();
                        read_transaction.query_prepared_with_params_streaming_context(
                            prepared,
                            &input.parameters,
                            QueryStreamOptions {
                                max_rows,
                                max_payload_bytes: Some(max_payload_bytes),
                            },
                            crate::executor::StreamDelivery::Incremental,
                            task_context,
                            |row| {
                                rows.push(row);
                                Ok(())
                            },
                        )?;
                        Ok(QueryOutput { rows: rows.into() })
                    },
                )
                .await
                .map_err(HawDBTokioEmbeddedError::Task)
        }
    }
}

fn apply_segment_io_requirement(
    request: RuntimeWorkRequest,
    minimum_io_slots: usize,
) -> RuntimeWorkRequest {
    if minimum_io_slots > 0 {
        request.with_io_wave_slots(request.io_slots.max(minimum_io_slots))
    } else {
        request
    }
}

fn send_async_query_batch(
    sender: &hawdb_runtime_tokio::TokioBoundedSender<TokioQueryStreamEvent>,
    batch: &mut Vec<Row>,
    task_context: &RuntimeTaskContext,
) -> Result<(), HawDBError> {
    send_async_query_batch_with_retry(
        sender,
        batch,
        || task_context.checkpoint(),
        || std::thread::sleep(std::time::Duration::from_millis(1)),
    )
}

fn send_async_query_batch_with_retry(
    sender: &hawdb_runtime_tokio::TokioBoundedSender<TokioQueryStreamEvent>,
    batch: &mut Vec<Row>,
    mut checkpoint: impl FnMut() -> Result<(), hawdb_core::RuntimeCancellationReason>,
    mut retry_wait: impl FnMut(),
) -> Result<(), HawDBError> {
    let capacity = batch.capacity();
    let ready = std::mem::replace(batch, Vec::with_capacity(capacity));
    let mut event = TokioQueryStreamEvent::Batch(ready);
    loop {
        checkpoint().map_err(|reason| {
            HawDBError::Execution(format!("asynchronous row producer stopped: {reason}"))
        })?;
        match sender.try_send(event) {
            Ok(()) => return Ok(()),
            Err(TokioBoundedTrySendError::Full(returned)) => {
                event = returned;
                retry_wait();
            }
            Err(TokioBoundedTrySendError::Closed(_)) => {
                return Err(HawDBError::Execution(
                    "asynchronous row consumer closed".to_string(),
                ));
            }
        }
    }
}

fn lock_embedded(embedded: &Mutex<HawDBEmbedded>) -> MutexGuard<'_, HawDBEmbedded> {
    embedded
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EmbeddedDeploymentProfile;
    use hawdb_core::RuntimeCancellationToken;
    use hawdb_qos::{RuntimeTelemetryEvent, RuntimeTelemetryEventKind, RuntimeTelemetrySink};
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
    fn saturated_tokio_entrypoints_wait_before_parsing() {
        let path = unique_test_path("planning-admission-gate");
        let embedded =
            HawDBTokioEmbedded::open_owned(HawDBEmbeddedOpenOptions::new(&path)).unwrap();
        let governor = embedded.runtime().governor();
        let busy = governor
            .try_admit(
                RuntimeWorkRequest::new(RuntimeWorkPriority::Foreground, RuntimeWorkKind::Control)
                    .with_cpu_slots(governor.snapshot().limits.effective_cpu_slots.get()),
            )
            .unwrap();
        embedded
            .runtime()
            .block_on(async {
                for route in 0..3 {
                    let context =
                        RuntimeTaskContext::with_timeout(std::time::Duration::from_millis(20));
                    let result = match route {
                        0 => embedded.query("MATCH (", context).await.map(|_| ()),
                        1 => embedded
                            .query_with_request(
                                "MATCH (",
                                BTreeMap::new(),
                                RuntimeWorkRequest::foreground_query(0, 0),
                                context,
                            )
                            .await
                            .map(|_| ()),
                        _ => embedded.query_stream("MATCH (", context).await.map(|_| ()),
                    };
                    assert!(matches!(
                        result,
                        Err(HawDBTokioEmbeddedError::Task(TokioTaskError::Stopped(
                            hawdb_core::RuntimeCancellationReason::DeadlineExceeded
                        )))
                    ));
                }
            })
            .unwrap();
        assert_eq!(
            governor.snapshot().admissions,
            1,
            "only the occupying task was admitted"
        );
        drop(busy);
        let error = embedded
            .runtime()
            .block_on(embedded.query("MATCH (", RuntimeTaskContext::default()))
            .unwrap()
            .unwrap_err();
        assert!(matches!(
            error,
            HawDBTokioEmbeddedError::Task(TokioTaskError::Operation(_))
        ));
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.admitted_memory_bytes, 0);
        assert_eq!(snapshot.active_cpu_slots, 0);
        assert_eq!(snapshot.active_blocking_tasks, 0);
        assert_eq!(snapshot.admissions, snapshot.completions);
    }

    #[test]
    fn custom_request_preserves_task_scope_without_segment_io() {
        let request = RuntimeWorkRequest::io(hawdb_qos::RuntimeWorkPriority::Foreground, 1, 0);

        let normalized = apply_segment_io_requirement(request, 0);

        assert_eq!(normalized.io_slots, 1);
        assert_eq!(
            normalized.io_reservation_scope,
            hawdb_qos::RuntimeIoReservationScope::Task
        );
    }

    #[test]
    fn custom_request_uses_wave_scope_for_segment_io() {
        let request = RuntimeWorkRequest::io(hawdb_qos::RuntimeWorkPriority::Foreground, 1, 0);

        let normalized = apply_segment_io_requirement(request, 2);

        assert_eq!(normalized.io_slots, 2);
        assert_eq!(
            normalized.io_reservation_scope,
            hawdb_qos::RuntimeIoReservationScope::Wave
        );
    }

    #[test]
    fn owned_facade_runs_queries_through_the_bounded_adapter() {
        let path = unique_test_path("owned");
        let embedded =
            HawDBTokioEmbedded::open_owned(HawDBEmbeddedOpenOptions::new(&path)).unwrap();
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
        assert_eq!(embedded.runtime_snapshot().completions, 4);
    }

    #[test]
    fn checkpointed_and_reopened_facade_accepts_mutations() {
        use hawdb_storage::StorageResidencyMode;

        for mode in [
            StorageResidencyMode::Materialized,
            StorageResidencyMode::OutOfCore,
        ] {
            let path = unique_test_path("checkpointed-mutation");
            let options = HawDBEmbeddedOpenOptions::new(&path).with_config(crate::DatabaseConfig {
                storage_residency_mode: mode,
                ..crate::DatabaseConfig::default()
            });
            for round in 0..2 {
                let embedded = HawDBTokioEmbedded::open_owned(options.clone()).unwrap();
                embedded
                    .runtime()
                    .block_on(async {
                        for query in [
                            "CREATE (:Memory {id: 'before-checkpoint'})",
                            "CHECKPOINT",
                            "CREATE (:Memory {id: 'after-checkpoint'})",
                        ] {
                            embedded
                                .query(query, RuntimeTaskContext::default())
                                .await
                                .unwrap();
                        }
                        let output = embedded
                            .query(
                                "MATCH (m:Memory) RETURN m.id AS id",
                                RuntimeTaskContext::default(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(output.rows.len(), 2 * (round + 1));
                    })
                    .unwrap();
                let snapshot = embedded.runtime_snapshot();
                assert_eq!(snapshot.admissions, 8);
                assert_eq!(snapshot.completions, 8);
                assert_eq!(snapshot.admitted_memory_bytes, 0);
                drop(embedded);
            }
            std::fs::remove_dir_all(&path).unwrap();
        }
    }

    #[test]
    fn borrowed_facade_keeps_the_host_runtime_alive() {
        let path = unique_test_path("borrowed");
        let host = tokio_runtime();
        let embedded = HawDBTokioEmbedded::open_borrowed(
            HawDBEmbeddedOpenOptions::mobile(&path),
            host.handle().clone(),
        )
        .unwrap();
        assert_eq!(embedded.ownership(), TokioRuntimeOwnership::Borrowed);
        assert_eq!(
            embedded.with_embedded(HawDBEmbedded::deployment_profile),
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
        let mut embedded = HawDBEmbedded::open(&path).unwrap();
        let create = embedded
            .database_mut()
            .runtime_admission_plan("CREATE (:Probe {value: 1})", &BTreeMap::new())
            .unwrap();
        let read = embedded
            .database_mut()
            .runtime_admission_plan("MATCH (p:Probe) RETURN p.value AS value", &BTreeMap::new())
            .unwrap();

        assert_eq!(create.work_request.class, hawdb_qos::WorkClass::Query);
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
        config.mutation_limits = hawdb_storage::MutationLimits {
            max_affected_rows: std::num::NonZeroUsize::new(3).unwrap(),
            max_operations: std::num::NonZeroUsize::new(2).unwrap(),
            max_result_rows: std::num::NonZeroUsize::new(4).unwrap(),
            max_result_payload_bytes: std::num::NonZeroUsize::new(5).unwrap(),
        };
        let mut embedded = HawDBEmbedded::open_with_options(
            HawDBEmbeddedOpenOptions::new(&path).with_config(config),
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

        assert_eq!(
            read.estimated_memory_bytes, 1024,
            "the fused projection scan owns one admitted pipeline batch"
        );
        assert_eq!(
            mutation.estimated_memory_bytes,
            2 * 1024 + 2 * 64 + 3 * 16 + 4 * 64 + 5
        );
    }

    #[test]
    fn cancelled_query_is_rejected_before_database_execution() {
        let path = unique_test_path("cancelled-before-start");
        let embedded =
            HawDBTokioEmbedded::open_owned(HawDBEmbeddedOpenOptions::new(&path)).unwrap();
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
            Err(HawDBTokioEmbeddedError::Task(TokioTaskError::Stopped(
                hawdb_core::RuntimeCancellationReason::Cancelled
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
            HawDBTokioEmbedded::open_owned(HawDBEmbeddedOpenOptions::new(&path)).unwrap();
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

        assert_eq!(embedded.runtime_snapshot().completions, 2);
        let phases: Vec<_> = events
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    RuntimeTelemetryEventKind::Admitted | RuntimeTelemetryEventKind::Completed
                )
            })
            .map(|event| (event.kind, event.work_kind))
            .collect();
        assert_eq!(
            phases,
            vec![
                (
                    RuntimeTelemetryEventKind::Admitted,
                    Some(RuntimeWorkKind::Control)
                ),
                (
                    RuntimeTelemetryEventKind::Completed,
                    Some(RuntimeWorkKind::Control)
                ),
                (
                    RuntimeTelemetryEventKind::Admitted,
                    Some(RuntimeWorkKind::Mutation)
                ),
                (
                    RuntimeTelemetryEventKind::Completed,
                    Some(RuntimeWorkKind::Mutation)
                ),
            ]
        );
    }

    #[test]
    fn default_query_enforces_the_governor_result_byte_budget() {
        let path = unique_test_path("result-byte-budget");
        let options = HawDBEmbeddedOpenOptions::new(&path).with_runtime_governor_config(
            hawdb_qos::RuntimeGovernorConfig {
                result_budget_bytes: 64,
                ..hawdb_qos::RuntimeGovernorConfig::shared_host()
            },
        );
        let embedded = HawDBTokioEmbedded::open_owned(options).unwrap();
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
        let embedded = HawDBTokioEmbedded::open_owned(
            HawDBEmbeddedOpenOptions::new(&path).with_config(config),
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
        let admitted_executor_bytes = embedded.with_embedded_mut(|embedded| {
            let admission = embedded
                .database_mut()
                .runtime_admission_plan(
                    "MATCH (p:Probe) RETURN p.value AS value ORDER BY value",
                    &BTreeMap::new(),
                )
                .unwrap();
            admission
                .runtime_work_request_for_snapshot(
                    embedded.admitted_result_budget_bytes(),
                    embedded.runtime_governor().snapshot(),
                )
                .memory_bytes
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
        assert_eq!(
            report
                .execution_profile
                .pipeline_memory_report
                .query_memory_budget_bytes,
            usize::try_from(admitted_executor_bytes).unwrap()
        );
        assert_eq!(embedded.runtime_snapshot().completions, 2);
    }

    #[test]
    fn asynchronous_row_stream_surfaces_a_terminal_limit_after_prior_batches() {
        let path = unique_test_path("terminal-row-limit");
        let mut config = crate::DatabaseConfig {
            max_read_result_rows: Some(2),
            ..crate::DatabaseConfig::default()
        };
        config.execution_memory.batch_rows = NonZeroUsize::new(1).unwrap();
        config.execution_memory.batch_payload_bytes = NonZeroUsize::new(1024).unwrap();
        let embedded = HawDBTokioEmbedded::open_owned(
            HawDBEmbeddedOpenOptions::new(&path).with_config(config),
        )
        .unwrap();
        embedded.with_embedded_mut(|embedded| {
            for value in 0..3 {
                embedded
                    .database_mut()
                    .query(&format!("CREATE (:Probe {{value: {value}}})"))
                    .unwrap();
            }
        });

        let mut stream = embedded
            .runtime()
            .block_on(embedded.query_stream_with_options(
                "MATCH (p:Probe) RETURN p.value AS value ORDER BY value",
                TokioQueryStreamOptions {
                    channel_capacity: NonZeroUsize::new(1).unwrap(),
                },
                RuntimeTaskContext::default(),
            ))
            .unwrap()
            .unwrap();
        let first_batch = embedded
            .runtime()
            .block_on(stream.next_batch())
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first_batch.len(), 1);

        let error = embedded
            .runtime()
            .block_on(stream.next_batch())
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("max_read_result_rows 2"));
        assert!(stream.report().is_none());
    }

    #[test]
    fn dropping_an_async_row_stream_cancels_its_admitted_producer() {
        let path = unique_test_path("cancel-row-stream");
        let mut config = crate::DatabaseConfig::default();
        config.execution_memory.batch_rows = NonZeroUsize::new(1).unwrap();
        config.execution_memory.batch_payload_bytes = NonZeroUsize::new(1024).unwrap();
        let embedded = HawDBTokioEmbedded::open_owned(
            HawDBEmbeddedOpenOptions::new(&path).with_config(config),
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
    fn async_row_batch_retry_observes_deadlines_and_cancellation() {
        use hawdb_core::RuntimeCancellationReason;
        use std::cell::Cell;

        let runtime = hawdb_runtime_tokio::TokioRuntimeBuilder::new_current_thread()
            .build()
            .unwrap();
        for reason in [
            RuntimeCancellationReason::DeadlineExceeded,
            RuntimeCancellationReason::Cancelled,
        ] {
            for retry_limit in [1, 2, 16, 64] {
                let (sender, mut receiver) = tokio_bounded_channel(NonZeroUsize::MIN);
                let buffered = vec![Row::from([("value".to_string(), Value::Int(1))])];
                sender
                    .try_send(TokioQueryStreamEvent::Batch(buffered.clone()))
                    .unwrap();
                let mut pending = vec![Row::from([("value".to_string(), Value::Int(2))])];
                let retries = Cell::new(0);
                let checks = Cell::new(0);

                // Expire only after observing a full channel. No scheduler or
                // wall-clock assumption is needed to exercise the retry path.
                let error = send_async_query_batch_with_retry(
                    &sender,
                    &mut pending,
                    || {
                        checks.set(checks.get() + 1);
                        if retries.get() == retry_limit {
                            Err(reason)
                        } else {
                            Ok(())
                        }
                    },
                    || {
                        assert!(retries.get() < retry_limit, "retry ignored its stop check");
                        retries.set(retries.get() + 1);
                    },
                )
                .unwrap_err();
                assert_eq!(retries.get(), retry_limit);
                assert_eq!(checks.get(), retry_limit + 1);
                assert!(matches!(error, HawDBError::Execution(message)
                    if message == format!("asynchronous row producer stopped: {reason}")));
                assert!(pending.is_empty());
                drop(sender);
                assert!(matches!(runtime.block_on(receiver.recv()),
                    Some(TokioQueryStreamEvent::Batch(batch)) if batch == buffered));
                assert!(runtime.block_on(receiver.recv()).is_none());
            }
        }
    }

    #[test]
    fn async_row_batch_retry_delivers_after_backpressure_is_released() {
        let runtime = hawdb_runtime_tokio::TokioRuntimeBuilder::new_current_thread()
            .build()
            .unwrap();
        let (sender, mut receiver) = tokio_bounded_channel(NonZeroUsize::MIN);
        let buffered = vec![Row::from([("value".to_string(), Value::Int(1))])];
        let expected = vec![Row::from([("value".to_string(), Value::Int(2))])];
        sender
            .try_send(TokioQueryStreamEvent::Batch(buffered.clone()))
            .unwrap();
        let mut pending = expected.clone();
        let mut retries = 0;
        let context = RuntimeTaskContext::default();
        send_async_query_batch_with_retry(
            &sender,
            &mut pending,
            || context.checkpoint(),
            || {
                retries += 1;
                assert_eq!(retries, 1);
                assert!(matches!(runtime.block_on(receiver.recv()),
                    Some(TokioQueryStreamEvent::Batch(batch)) if batch == buffered));
            },
        )
        .unwrap();
        assert_eq!(retries, 1);
        assert!(pending.is_empty());
        drop(sender);
        assert!(matches!(runtime.block_on(receiver.recv()),
            Some(TokioQueryStreamEvent::Batch(batch)) if batch == expected));
        assert!(runtime.block_on(receiver.recv()).is_none());
    }

    #[test]
    fn async_row_stream_deadline_surfaces_terminal_error_and_releases_resources() {
        let path = unique_test_path("deadline-row-stream");
        let mut config = crate::DatabaseConfig::default();
        config.execution_memory.batch_rows = NonZeroUsize::new(1).unwrap();
        config.execution_memory.batch_payload_bytes = NonZeroUsize::new(1024).unwrap();
        let embedded = HawDBTokioEmbedded::open_owned(
            HawDBEmbeddedOpenOptions::new(&path).with_config(config),
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

        for (index, timeout_ms) in [0, 10].into_iter().enumerate() {
            let result = embedded
                .runtime()
                .block_on(embedded.query_stream_with_options(
                    "MATCH (p:Probe) RETURN p.value AS value",
                    TokioQueryStreamOptions {
                        channel_capacity: NonZeroUsize::MIN,
                    },
                    RuntimeTaskContext::with_timeout(std::time::Duration::from_millis(timeout_ms)),
                ))
                .unwrap();
            let error = match result {
                Err(error) => error,
                Ok(mut stream) => {
                    // Do not drain until the producer has stopped: freeing a
                    // slot could let a send already past its checkpoint finish.
                    let watchdog = std::time::Instant::now() + std::time::Duration::from_secs(5);
                    while embedded.runtime_snapshot().deadline_exceeded != index as u64 + 1 {
                        assert!(
                            std::time::Instant::now() < watchdog,
                            "producer did not stop"
                        );
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                    let mut batches = 0;
                    let error = loop {
                        match embedded.runtime().block_on(stream.next_batch()).unwrap() {
                            Ok(Some(batch)) => {
                                batches += 1;
                                assert_eq!(batch.len(), 1);
                                assert!(batches <= 1, "only one batch fits before the deadline");
                            }
                            Ok(None) => panic!("the deadline must not produce a success report"),
                            Err(error) => break error,
                        }
                    };
                    assert!(stream.report().is_none());
                    assert!(embedded
                        .runtime()
                        .block_on(stream.next_batch())
                        .unwrap()
                        .unwrap()
                        .is_none());
                    error
                }
            };
            assert!(matches!(
                error,
                HawDBTokioEmbeddedError::Task(TokioTaskError::Stopped(
                    hawdb_core::RuntimeCancellationReason::DeadlineExceeded
                ))
            ));
            let snapshot = embedded.runtime_snapshot();
            assert_eq!(snapshot.deadline_exceeded, index as u64 + 1);
            assert_eq!(snapshot.active_blocking_tasks, 0);
            assert_eq!(snapshot.admitted_memory_bytes, 0);
            assert_eq!(snapshot.active_cpu_slots, 0);
            assert_eq!(snapshot.active_foreground_tasks, 0);
            assert_eq!(snapshot.cancellations, 0);
        }
    }

    #[test]
    fn asynchronous_row_stream_rejects_mutations() {
        let path = unique_test_path("mutation-row-stream");
        let embedded =
            HawDBTokioEmbedded::open_owned(HawDBEmbeddedOpenOptions::new(&path)).unwrap();
        let error = embedded
            .runtime()
            .block_on(
                embedded.query_stream("CREATE (:Probe {value: 1})", RuntimeTaskContext::default()),
            )
            .unwrap()
            .unwrap_err();

        assert!(matches!(error, HawDBTokioEmbeddedError::StreamingMutation));
        // Classification requires an admitted parse, but execution never starts.
        assert_eq!(embedded.runtime_snapshot().admissions, 1);
        assert_eq!(embedded.runtime_snapshot().completions, 1);
        assert_eq!(embedded.runtime_snapshot().admitted_memory_bytes, 0);
    }

    fn tokio_runtime() -> hawdb_runtime_tokio::TokioRuntime {
        hawdb_runtime_tokio::TokioRuntimeBuilder::new_multi_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    fn unique_test_path(prefix: &str) -> std::path::PathBuf {
        let id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "hawdb-tokio-embedded-{prefix}-{}-{id}",
            std::process::id()
        ))
    }
}
