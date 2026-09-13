use super::{
    is_mutation_statement, CompatibilityShadowEngine, CypherFixtureStatement,
    ProjectedGraphFixtureCheck, ProjectedGraphShadowOutput, ProjectedGraphShadowResult,
    ShadowRequestContext, ShadowRequestPhase, EXTERNAL_SHADOW_PROTOCOL_VERSION,
};
use skein_core::{Result, SkeinError, Value};
use skein_executor::{QueryOutput, Row};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const DEFAULT_EXTERNAL_SHADOW_REQUEST_TIMEOUT_MS: u64 = 30_000;
const EXTERNAL_SHADOW_STDERR_TAIL_BYTES: usize = 8192;
const EXTERNAL_SHADOW_STDOUT_TAIL_BYTES: usize = 2048;
pub const REQUIRED_EXTERNAL_SHADOW_CAPABILITIES: [&str; 3] =
    ["execute", "execute_session", "project_graph"];

#[derive(Debug, Clone, PartialEq)]
pub struct ExternalShadowStatementRequest {
    pub cypher: String,
    pub parameters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalShadowProjectGraphRequest {
    pub rel_type: Option<String>,
    pub expected_incoming_nodes: Vec<u64>,
    pub include_communities: bool,
    pub include_hierarchical_communities: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExternalShadowProjectGraphReply {
    Ok(serde_json::Value),
    PrimaryOnly { reason: Option<String> },
}

pub trait ExternalShadowProtocolBackend {
    fn engine_kind(&self) -> &'static str;

    fn wrapper_identity(&self) -> Option<&str> {
        None
    }

    fn capabilities(&self) -> Vec<&'static str> {
        REQUIRED_EXTERNAL_SHADOW_CAPABILITIES.to_vec()
    }

    fn execute(&mut self, statement: ExternalShadowStatementRequest) -> Result<QueryOutput>;

    fn execute_session(
        &mut self,
        statements: Vec<ExternalShadowStatementRequest>,
    ) -> Result<Vec<QueryOutput>> {
        statements
            .into_iter()
            .map(|statement| self.execute(statement))
            .collect()
    }

    fn project_graph(
        &mut self,
        _request: ExternalShadowProjectGraphRequest,
    ) -> Result<ExternalShadowProjectGraphReply> {
        Ok(ExternalShadowProjectGraphReply::PrimaryOnly {
            reason: Some("projected graph metadata is not exposed".to_string()),
        })
    }
}

pub struct ExternalShadowProtocolServer<B> {
    backend: B,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShadowStatementRole {
    Read,
    Mutation,
    Statement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalShadowReady {
    pub protocol_version: u64,
    pub capabilities: Vec<String>,
    pub engine_kind: Option<String>,
    pub wrapper_identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExternalShadowTraceSummary {
    pub trace_record_count: u64,
    pub request_events: u64,
    pub response_events: u64,
    pub error_events: u64,
    pub invalid_lines: u64,
    pub completed_request_count: u64,
    pub pending_request_count: u64,
    pub request_op_counts: BTreeMap<String, u64>,
    pub response_op_counts: BTreeMap<String, u64>,
    pub error_op_counts: BTreeMap<String, u64>,
    pub pending_op_counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternalShadowTraceHealth {
    pub present: bool,
    pub complete: bool,
    pub summary_available: Option<bool>,
    pub request_count_matches: Option<bool>,
    pub pending_request_count: Option<u64>,
}

pub struct ExternalShadowCommand {
    name: String,
    child: Child,
    stdin: ChildStdin,
    stdout: ExternalShadowStdout,
    stderr: ExternalShadowStderr,
    trace: Option<File>,
    next_trace_sequence: u64,
    request_timeout: Duration,
}

struct ExternalShadowStdout {
    receiver: mpsc::Receiver<std::result::Result<String, String>>,
    join: Option<JoinHandle<()>>,
}

enum ExternalShadowStdoutRead {
    Line(String),
    Closed,
    Timeout,
    Error(String),
}

struct ExternalShadowStderr {
    tail: Arc<Mutex<String>>,
    join: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExternalShadowErrorClass {
    Parse,
    Semantic,
    Storage,
    Execution,
}

impl<B> ExternalShadowProtocolServer<B>
where
    B: ExternalShadowProtocolBackend,
{
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn run_json_lines<R, W>(&mut self, reader: R, mut writer: W) -> Result<()>
    where
        R: BufRead,
        W: Write,
    {
        for line in reader.lines() {
            let line = line.map_err(|error| {
                SkeinError::Execution(format!("failed to read shadow request: {error}"))
            })?;
            let response = match serde_json::from_str::<serde_json::Value>(&line) {
                Ok(request) => self.handle_request(&request),
                Err(error) => json_error("parse", format!("invalid JSON: {error}")),
            };
            writeln!(writer, "{response}").map_err(|error| {
                SkeinError::Execution(format!("failed to write shadow response: {error}"))
            })?;
            writer.flush().map_err(|error| {
                SkeinError::Execution(format!("failed to flush shadow response: {error}"))
            })?;
        }
        Ok(())
    }

    pub fn handle_request(&mut self, request: &serde_json::Value) -> serde_json::Value {
        if let Err(error) = validate_protocol_version(request) {
            return error;
        }
        match request.get("op").and_then(serde_json::Value::as_str) {
            Some("ready") => self.handle_ready(),
            Some("execute") => self.handle_execute(request),
            Some("execute_session") => self.handle_execute_session(request),
            Some("project_graph") => self.handle_project_graph(request),
            Some(other) => json_error("semantic", format!("unknown shadow op '{other}'")),
            None => json_error("semantic", "shadow request missing op"),
        }
    }

    fn handle_ready(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": {
                "protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
                "engine_kind": self.backend.engine_kind(),
                "wrapper_identity": self.backend.wrapper_identity(),
                "capabilities": self.backend.capabilities(),
            }
        })
    }

    fn handle_execute(&mut self, request: &serde_json::Value) -> serde_json::Value {
        let Some(statement) = statement_from_request(request) else {
            return json_error("semantic", "execute request missing cypher");
        };
        match self.backend.execute(statement) {
            Ok(output) => json_output(output),
            Err(error) => json_error_from_skein(error),
        }
    }

    fn handle_execute_session(&mut self, request: &serde_json::Value) -> serde_json::Value {
        let Some(statements) = request
            .get("statements")
            .and_then(serde_json::Value::as_array)
        else {
            return json_error("semantic", "execute_session request missing statements");
        };
        let mut decoded = Vec::with_capacity(statements.len());
        for statement in statements {
            let Some(statement) = statement_from_request(statement) else {
                return json_error("semantic", "session statement missing cypher");
            };
            decoded.push(statement);
        }

        match self.backend.execute_session(decoded) {
            Ok(outputs) => serde_json::json!({
                "ok": {
                    "outputs": outputs.into_iter().map(rows_json).collect::<Vec<_>>()
                }
            }),
            Err(error) => json_error_from_skein(error),
        }
    }

    fn handle_project_graph(&mut self, request: &serde_json::Value) -> serde_json::Value {
        let request = project_graph_request_from_json(request);
        match self.backend.project_graph(request) {
            Ok(ExternalShadowProjectGraphReply::Ok(output)) => serde_json::json!({ "ok": output }),
            Ok(ExternalShadowProjectGraphReply::PrimaryOnly { reason }) => {
                let mut response = serde_json::Map::from_iter([(
                    "primary_only".to_string(),
                    serde_json::Value::Bool(true),
                )]);
                if let Some(reason) = reason {
                    response.insert("reason".to_string(), serde_json::Value::String(reason));
                }
                serde_json::Value::Object(response)
            }
            Err(error) => json_error_from_skein(error),
        }
    }
}

pub fn external_shadow_value_from_json(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Execution(format!(
                    "unsupported JSON number: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(external_shadow_value_from_json)
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => {
            if values.len() == 1
                && let Some(serde_json::Value::String(encoded)) = values.get("$binary")
            {
                return decode_binary_hex(encoded).map(Value::Binary);
            }
            if values.len() == 1
                && let Some(serde_json::Value::String(encoded)) = values.get("$uuid")
            {
                return skein_core::Uuid::parse_str(encoded)
                    .map(Value::Uuid)
                    .map_err(|error| {
                        SkeinError::Semantic(format!("invalid external UUID: {error}"))
                    });
            }
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), external_shadow_value_from_json(value)?)))
                .collect::<Result<BTreeMap<_, _>>>()
                .map(Value::Map)
        }
    }
}

pub fn external_shadow_json_from_value(value: Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(value),
        Value::Int(value) => serde_json::Value::Number(value.into()),
        Value::Float(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(value) => serde_json::Value::String(value),
        Value::Binary(value) => serde_json::json!({ "$binary": encode_binary_hex(&value) }),
        Value::Uuid(value) => serde_json::json!({ "$uuid": value.to_string() }),
        Value::List(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(external_shadow_json_from_value)
                .collect(),
        ),
        Value::Map(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, external_shadow_json_from_value(value)))
                .collect(),
        ),
    }
}

fn validate_protocol_version(
    request: &serde_json::Value,
) -> std::result::Result<(), serde_json::Value> {
    match request
        .get("protocol_version")
        .and_then(serde_json::Value::as_u64)
    {
        Some(EXTERNAL_SHADOW_PROTOCOL_VERSION) => Ok(()),
        Some(version) => Err(json_error(
            "execution",
            format!(
                "unsupported shadow protocol version {version}; expected {EXTERNAL_SHADOW_PROTOCOL_VERSION}"
            ),
        )),
        None => Err(json_error(
            "execution",
            "shadow request missing protocol_version",
        )),
    }
}

fn statement_from_request(request: &serde_json::Value) -> Option<ExternalShadowStatementRequest> {
    let cypher = request.get("cypher")?.as_str()?.to_string();
    let parameters = request
        .get("parameters")
        .and_then(serde_json::Value::as_object)
        .map(|parameters| {
            parameters
                .iter()
                .map(|(key, value)| Ok((key.clone(), external_shadow_value_from_json(value)?)))
                .collect::<Result<BTreeMap<_, _>>>()
        })
        .transpose()
        .ok()?
        .unwrap_or_default();
    Some(ExternalShadowStatementRequest { cypher, parameters })
}

fn project_graph_request_from_json(
    request: &serde_json::Value,
) -> ExternalShadowProjectGraphRequest {
    ExternalShadowProjectGraphRequest {
        rel_type: request
            .get("rel_type")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        expected_incoming_nodes: request
            .get("expected_incoming_nodes")
            .and_then(serde_json::Value::as_array)
            .map(|nodes| {
                nodes
                    .iter()
                    .filter_map(serde_json::Value::as_u64)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        include_communities: request
            .get("include_communities")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        include_hierarchical_communities: request
            .get("include_hierarchical_communities")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
    }
}

fn json_output(output: QueryOutput) -> serde_json::Value {
    serde_json::json!({ "ok": rows_json(output) })
}

fn rows_json(output: QueryOutput) -> serde_json::Value {
    serde_json::json!({
        "rows": output
            .rows
            .into_iter()
            .map(|row| {
                serde_json::Value::Object(
                    row.iter()
                        .map(|(key, value)| {
                            (
                                key.to_string(),
                                external_shadow_json_from_value(value.clone()),
                            )
                        })
                        .collect(),
                )
            })
            .collect::<Vec<_>>()
    })
}

fn json_error(class: &str, message: impl ToString) -> serde_json::Value {
    serde_json::json!({
        "error": {
            "class": class,
            "message": message.to_string(),
        }
    })
}

fn json_error_from_skein(error: SkeinError) -> serde_json::Value {
    match error {
        SkeinError::Parse(message) => json_error("parse", message),
        SkeinError::Semantic(message) => json_error("semantic", message),
        SkeinError::Storage(message) => json_error("storage", message),
        SkeinError::StorageIntegrity(message) => json_error("storage", message),
        SkeinError::Execution(message) => json_error("execution", message),
        error @ SkeinError::AppendSequenceExhausted { .. } => json_error("storage", error),
        SkeinError::CapabilityUnavailable { capability } => {
            json_error("capability_unavailable", capability.as_str())
        }
    }
}

impl ExternalShadowCommand {
    pub fn spawn(
        name: impl Into<String>,
        program: impl AsRef<std::ffi::OsStr>,
        args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
    ) -> Result<Self> {
        Self::spawn_inner(
            name,
            program,
            args,
            None::<&Path>,
            default_external_shadow_request_timeout(),
        )
    }

    pub fn spawn_with_trace_path(
        name: impl Into<String>,
        program: impl AsRef<std::ffi::OsStr>,
        args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
        trace_path: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::spawn_inner(
            name,
            program,
            args,
            Some(trace_path.as_ref()),
            default_external_shadow_request_timeout(),
        )
    }

    pub fn spawn_with_request_timeout(
        name: impl Into<String>,
        program: impl AsRef<std::ffi::OsStr>,
        args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
        request_timeout: Duration,
    ) -> Result<Self> {
        Self::spawn_inner(name, program, args, None::<&Path>, request_timeout)
    }

    pub fn spawn_with_trace_path_and_request_timeout(
        name: impl Into<String>,
        program: impl AsRef<std::ffi::OsStr>,
        args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
        trace_path: impl AsRef<Path>,
        request_timeout: Duration,
    ) -> Result<Self> {
        Self::spawn_inner(
            name,
            program,
            args,
            Some(trace_path.as_ref()),
            request_timeout,
        )
    }

    fn spawn_inner(
        name: impl Into<String>,
        program: impl AsRef<std::ffi::OsStr>,
        args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
        trace_path: Option<&Path>,
        request_timeout: Duration,
    ) -> Result<Self> {
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|error| {
            SkeinError::Execution(format!("failed to spawn external shadow engine: {error}"))
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            SkeinError::Execution("external shadow engine did not expose stdin".to_string())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            SkeinError::Execution("external shadow engine did not expose stdout".to_string())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            SkeinError::Execution("external shadow engine did not expose stderr".to_string())
        })?;
        let trace = trace_path
            .map(|path| {
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(path)
                    .map_err(|error| {
                        SkeinError::Execution(format!(
                            "failed to open external shadow trace: {error}"
                        ))
                    })
            })
            .transpose()?;
        Ok(Self {
            name: name.into(),
            child,
            stdin,
            stdout: ExternalShadowStdout::start(stdout),
            stderr: ExternalShadowStderr::start(stderr),
            trace,
            next_trace_sequence: 1,
            request_timeout,
        })
    }

    pub fn require_ready(&mut self) -> Result<ExternalShadowReady> {
        let response = self.request(serde_json::json!({
            "op": "ready",
            "required_protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "required_capabilities": REQUIRED_EXTERNAL_SHADOW_CAPABILITIES,
        }))?;
        decode_external_ready_response(&self.name, response)
    }

    pub fn request_count(&self) -> u64 {
        self.next_trace_sequence.saturating_sub(1)
    }

    fn request(&mut self, mut request: serde_json::Value) -> Result<serde_json::Value> {
        let trace_sequence = self.next_trace_sequence;
        self.next_trace_sequence += 1;
        {
            let object = request.as_object_mut().ok_or_else(|| {
                SkeinError::Execution("external shadow request must be a JSON object".to_string())
            })?;
            object.insert(
                "protocol_version".to_string(),
                serde_json::Value::Number(EXTERNAL_SHADOW_PROTOCOL_VERSION.into()),
            );
            object.insert(
                "request_id".to_string(),
                serde_json::Value::Number(trace_sequence.into()),
            );
        }
        self.trace_event(trace_sequence, "request", &request);
        let line = serde_json::to_string(&request).map_err(|error| {
            SkeinError::Execution(format!("failed to encode shadow request: {error}"))
        })?;
        writeln!(self.stdin, "{line}").map_err(|error| {
            let message = format!(
                "failed to write request to shadow engine '{}': {error}",
                self.name
            );
            self.trace_error(trace_sequence, &message);
            self.request_error(message)
        })?;
        self.stdin.flush().map_err(|error| {
            let message = format!(
                "failed to flush request to shadow engine '{}': {error}",
                self.name
            );
            self.trace_error(trace_sequence, &message);
            self.request_error(message)
        })?;

        let response = match self.stdout.read_line(self.request_timeout) {
            ExternalShadowStdoutRead::Line(response) => response,
            ExternalShadowStdoutRead::Closed => {
                let status = self.shadow_child_status_after_stdout_close();
                self.stderr.finish_reading();
                let message = format!("shadow engine '{}' closed stdout{status}", self.name);
                self.trace_error(trace_sequence, &message);
                return Err(self.request_error(message));
            }
            ExternalShadowStdoutRead::Timeout => {
                let status = self.kill_shadow_child_status();
                self.stderr.finish_reading();
                let message = format!(
                    "shadow engine '{}' did not return a response within {} ms{status}",
                    self.name,
                    self.request_timeout.as_millis()
                );
                self.trace_error(trace_sequence, &message);
                return Err(self.request_error(message));
            }
            ExternalShadowStdoutRead::Error(error) => {
                let message = format!(
                    "failed to read response from shadow engine '{}': {error}",
                    self.name
                );
                self.trace_error(trace_sequence, &message);
                return Err(self.request_error(message));
            }
        };
        let raw_response = response.trim_end();
        let response = serde_json::from_str(raw_response).map_err(|error| {
            let raw_tail = external_shadow_stdout_tail(raw_response);
            let message = format!(
                "shadow engine '{}' returned invalid JSON: {error}; stdout line tail: {raw_tail}",
                self.name,
            );
            self.trace_error(trace_sequence, &message);
            self.request_error(message)
        })?;
        self.trace_event(trace_sequence, "response", &response);
        if let Err(error) =
            validate_external_response_request_id(&self.name, trace_sequence, &response)
        {
            self.trace_error(trace_sequence, &error.to_string());
            return Err(error);
        }
        Ok(response)
    }

    fn request_error(&self, message: String) -> SkeinError {
        match self.stderr.tail() {
            Some(stderr) => SkeinError::Execution(format!("{message}; stderr tail: {stderr}")),
            None => SkeinError::Execution(message),
        }
    }

    fn shadow_child_status_after_stdout_close(&mut self) -> String {
        match self.child.try_wait() {
            Ok(Some(status)) => format!("; child status: {status}"),
            Ok(None) => {
                let killed_status = self.kill_shadow_child_status();
                format!("; child was still running after stdout close{killed_status}")
            }
            Err(error) => format!("; failed to read child status: {error}"),
        }
    }

    fn kill_shadow_child_status(&mut self) -> String {
        let _ = self.child.kill();
        match self.child.wait() {
            Ok(status) => format!("; child status: {status}"),
            Err(error) => format!("; failed to wait for child status: {error}"),
        }
    }

    fn trace_event(&mut self, sequence: u64, event: &str, payload: &serde_json::Value) {
        let Some(trace) = self.trace.as_mut() else {
            return;
        };
        let record = serde_json::json!({
            "sequence": sequence,
            "event": event,
            "payload": payload,
        });
        let _ = writeln!(trace, "{record}");
        let _ = trace.flush();
    }

    fn trace_error(&mut self, sequence: u64, message: &str) {
        let payload = serde_json::json!({
            "message": message,
            "stderr_tail": self.stderr.tail(),
        });
        self.trace_event(sequence, "error", &payload);
    }
}

impl Drop for ExternalShadowCommand {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn external_shadow_trace_report_json(
    trace_path: &str,
    request_count: u64,
) -> serde_json::Value {
    const REDACTED_TRACE_PATH: &str = "<redacted>";

    match summarize_external_shadow_trace(trace_path) {
        Ok(summary) => serde_json::json!({
            "path": REDACTED_TRACE_PATH,
            "path_redacted": true,
            "request_count": request_count,
            "summary_available": true,
            "trace_record_count": summary.trace_record_count,
            "request_events": summary.request_events,
            "response_events": summary.response_events,
            "error_events": summary.error_events,
            "invalid_lines": summary.invalid_lines,
            "completed_request_count": summary.completed_request_count,
            "pending_request_count": summary.pending_request_count,
            "request_op_counts": summary.request_op_counts,
            "response_op_counts": summary.response_op_counts,
            "error_op_counts": summary.error_op_counts,
            "pending_op_counts": summary.pending_op_counts,
        }),
        Err(_error) => serde_json::json!({
            "path": REDACTED_TRACE_PATH,
            "path_redacted": true,
            "request_count": request_count,
            "summary_available": false,
            "summary_error": "trace summary unavailable",
            "summary_error_code": "trace_summary_unavailable",
        }),
    }
}

pub fn external_shadow_trace_health_from_bundle(
    bundle: &serde_json::Value,
) -> ExternalShadowTraceHealth {
    let Some(trace) = bundle.get("shadow_trace") else {
        return ExternalShadowTraceHealth {
            present: false,
            complete: true,
            summary_available: None,
            request_count_matches: None,
            pending_request_count: None,
        };
    };
    let summary_available = trace
        .get("summary_available")
        .and_then(serde_json::Value::as_bool);
    let request_count = trace
        .get("request_count")
        .and_then(serde_json::Value::as_u64);
    let request_events = trace
        .get("request_events")
        .and_then(serde_json::Value::as_u64);
    let request_count_matches = request_count.zip(request_events).map(|(a, b)| a == b);
    let pending_request_count = trace
        .get("pending_request_count")
        .and_then(serde_json::Value::as_u64);
    let complete = summary_available == Some(true)
        && request_count_matches == Some(true)
        && pending_request_count == Some(0);
    ExternalShadowTraceHealth {
        present: true,
        complete,
        summary_available,
        request_count_matches,
        pending_request_count,
    }
}

pub fn external_shadow_ready_missing_capabilities(
    ready: Option<&ExternalShadowReady>,
) -> Vec<&'static str> {
    let Some(ready) = ready else {
        return REQUIRED_EXTERNAL_SHADOW_CAPABILITIES.to_vec();
    };
    REQUIRED_EXTERNAL_SHADOW_CAPABILITIES
        .iter()
        .copied()
        .filter(|capability| !ready.capabilities.iter().any(|value| value == capability))
        .collect()
}

fn summarize_external_shadow_trace(trace_path: &str) -> Result<ExternalShadowTraceSummary> {
    let file = File::open(trace_path).map_err(|error| {
        SkeinError::Execution(format!("failed to open external shadow trace: {error}"))
    })?;
    let mut summary = ExternalShadowTraceSummary::default();
    let mut request_sequences = std::collections::BTreeSet::new();
    let mut request_ops_by_sequence = BTreeMap::new();
    let mut terminal_sequences = std::collections::BTreeSet::new();
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else {
            summary.invalid_lines += 1;
            continue;
        };
        let Ok(record) = serde_json::from_str::<serde_json::Value>(&line) else {
            summary.invalid_lines += 1;
            continue;
        };
        summary.trace_record_count += 1;
        let sequence = record.get("sequence").and_then(serde_json::Value::as_u64);
        match record.get("event").and_then(serde_json::Value::as_str) {
            Some("request") => {
                summary.request_events += 1;
                let op = trace_record_op(&record);
                increment_trace_op_count(&mut summary.request_op_counts, &op);
                if let Some(sequence) = sequence {
                    request_sequences.insert(sequence);
                    request_ops_by_sequence.insert(sequence, op);
                }
            }
            Some("response") => {
                summary.response_events += 1;
                if let Some(sequence) = sequence {
                    terminal_sequences.insert(sequence);
                    let op = request_ops_by_sequence
                        .get(&sequence)
                        .map(String::as_str)
                        .unwrap_or("unknown");
                    increment_trace_op_count(&mut summary.response_op_counts, op);
                } else {
                    increment_trace_op_count(&mut summary.response_op_counts, "unknown");
                }
            }
            Some("error") => {
                summary.error_events += 1;
                if let Some(sequence) = sequence {
                    terminal_sequences.insert(sequence);
                    let op = request_ops_by_sequence
                        .get(&sequence)
                        .map(String::as_str)
                        .unwrap_or("unknown");
                    increment_trace_op_count(&mut summary.error_op_counts, op);
                } else {
                    increment_trace_op_count(&mut summary.error_op_counts, "unknown");
                }
            }
            _ => summary.invalid_lines += 1,
        }
    }
    summary.completed_request_count =
        request_sequences.intersection(&terminal_sequences).count() as u64;
    summary.pending_request_count =
        request_sequences.len() as u64 - summary.completed_request_count;
    for sequence in request_sequences.difference(&terminal_sequences) {
        let op = request_ops_by_sequence
            .get(sequence)
            .map(String::as_str)
            .unwrap_or("unknown");
        increment_trace_op_count(&mut summary.pending_op_counts, op);
    }
    Ok(summary)
}

fn trace_record_op(record: &serde_json::Value) -> String {
    record
        .get("payload")
        .and_then(|payload| payload.get("op"))
        .and_then(serde_json::Value::as_str)
        .filter(|op| !op.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

fn increment_trace_op_count(counts: &mut BTreeMap<String, u64>, op: &str) {
    *counts.entry(op.to_string()).or_insert(0) += 1;
}

impl ExternalShadowStdout {
    fn start(stdout: ChildStdout) -> Self {
        let (sender, receiver) = mpsc::channel();
        let join = thread::spawn(move || {
            let mut stdout = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match stdout.read_line(&mut line) {
                    Ok(0) => break,
                    Ok(_) => {
                        if sender.send(Ok(line)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error.to_string()));
                        break;
                    }
                }
            }
        });
        Self {
            receiver,
            join: Some(join),
        }
    }

    fn read_line(&self, timeout: Duration) -> ExternalShadowStdoutRead {
        match self.receiver.recv_timeout(timeout) {
            Ok(Ok(line)) => ExternalShadowStdoutRead::Line(line),
            Ok(Err(error)) => ExternalShadowStdoutRead::Error(error),
            Err(mpsc::RecvTimeoutError::Timeout) => ExternalShadowStdoutRead::Timeout,
            Err(mpsc::RecvTimeoutError::Disconnected) => ExternalShadowStdoutRead::Closed,
        }
    }
}

impl Drop for ExternalShadowStdout {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl ExternalShadowStderr {
    fn start(mut stderr: ChildStderr) -> Self {
        let tail = Arc::new(Mutex::new(String::new()));
        let thread_tail = Arc::clone(&tail);
        let join = thread::spawn(move || {
            let mut chunk = [0_u8; 1024];
            loop {
                match stderr.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(bytes) => {
                        let text = String::from_utf8_lossy(&chunk[..bytes]);
                        let mut tail = thread_tail.lock().unwrap();
                        tail.push_str(&text);
                        if tail.len() > EXTERNAL_SHADOW_STDERR_TAIL_BYTES {
                            let mut drain_to = tail.len() - EXTERNAL_SHADOW_STDERR_TAIL_BYTES;
                            while !tail.is_char_boundary(drain_to) {
                                drain_to += 1;
                            }
                            tail.drain(..drain_to);
                        }
                    }
                }
            }
        });
        Self {
            tail,
            join: Some(join),
        }
    }

    fn tail(&self) -> Option<String> {
        let tail = self.tail.lock().unwrap();
        let tail = tail.trim();
        if tail.is_empty() {
            None
        } else {
            Some(tail.to_string())
        }
    }

    fn finish_reading(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for ExternalShadowStderr {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl CompatibilityShadowEngine for ExternalShadowCommand {
    fn name(&self) -> &str {
        &self.name
    }

    fn execute(&mut self, statement: &CypherFixtureStatement) -> Result<QueryOutput> {
        self.execute_with_context(statement, ShadowRequestContext::default_execute())
    }

    fn execute_with_context(
        &mut self,
        statement: &CypherFixtureStatement,
        context: ShadowRequestContext,
    ) -> Result<QueryOutput> {
        let response = self.request(json_from_statement_with_role(
            "execute",
            statement,
            inferred_shadow_statement_role(statement),
            context,
        ))?;
        decode_external_query_response(&self.name, response)
    }

    fn execute_session(
        &mut self,
        statements: &[CypherFixtureStatement],
    ) -> Result<Vec<QueryOutput>> {
        self.execute_session_with_context(statements, ShadowRequestContext::default_execute())
    }

    fn execute_session_with_context(
        &mut self,
        statements: &[CypherFixtureStatement],
        context: ShadowRequestContext,
    ) -> Result<Vec<QueryOutput>> {
        let statement_count = statements.len();
        let session_access = shadow_session_access_as_str(statements);
        let statements = statements
            .iter()
            .enumerate()
            .map(|(index, statement)| {
                json_from_statement(
                    statement,
                    ShadowStatementRole::Statement,
                    ShadowRequestContext {
                        fixture: context.fixture,
                        check: context.check,
                        phase: ShadowRequestPhase::Statement,
                        statement_index: Some(index),
                    },
                )
            })
            .collect::<Vec<_>>();
        let response = self.request(serde_json::json!({
            "op": "execute_session",
            "access": session_access,
            "context": json_from_shadow_request_context(context),
            "statements": statements,
        }))?;
        let outputs = decode_external_session_response(&self.name, response)?;
        if outputs.len() != statement_count {
            return Err(SkeinError::Execution(format!(
                "shadow engine '{}' session returned {} outputs for {} statements",
                self.name,
                outputs.len(),
                statement_count
            )));
        }
        Ok(outputs)
    }

    fn project_graph(
        &mut self,
        check: &ProjectedGraphFixtureCheck,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        self.project_graph_with_context(check, ShadowRequestContext::default_project_graph())
    }

    fn project_graph_with_context(
        &mut self,
        check: &ProjectedGraphFixtureCheck,
        context: ShadowRequestContext,
    ) -> Result<Option<ProjectedGraphShadowOutput>> {
        match self.project_graph_result_with_context(check, context)? {
            ProjectedGraphShadowResult::Output(output) => Ok(Some(output)),
            ProjectedGraphShadowResult::PrimaryOnly { .. } => Ok(None),
        }
    }

    fn project_graph_result_with_context(
        &mut self,
        check: &ProjectedGraphFixtureCheck,
        context: ShadowRequestContext,
    ) -> Result<ProjectedGraphShadowResult> {
        let response = self.request(serde_json::json!({
            "op": "project_graph",
            "context": json_from_shadow_request_context(context),
            "rel_type": check.rel_type,
            "expected_incoming_nodes": check
                .expected_incoming
                .iter()
                .map(|(node, _)| *node)
                .collect::<Vec<_>>(),
            "include_communities": !check.expected_communities.is_empty(),
            "include_hierarchical_communities": !check.expected_hierarchical_communities.is_empty(),
        }))?;
        decode_external_projected_graph_response(&self.name, response)
    }
}

fn json_object_from_parameters(parameters: &BTreeMap<String, Value>) -> serde_json::Value {
    serde_json::Value::Object(
        parameters
            .iter()
            .map(|(key, value)| (key.clone(), json_from_value(value)))
            .collect(),
    )
}

fn json_from_statement_with_role(
    op: &str,
    statement: &CypherFixtureStatement,
    role: ShadowStatementRole,
    context: ShadowRequestContext,
) -> serde_json::Value {
    serde_json::json!({
        "op": op,
        "role": shadow_statement_role_as_str(role),
        "access": shadow_statement_access_as_str(statement),
        "context": json_from_shadow_request_context(context),
        "cypher": statement.cypher,
        "parameters": json_object_from_parameters(&statement.parameters),
    })
}

fn json_from_statement(
    statement: &CypherFixtureStatement,
    role: ShadowStatementRole,
    context: ShadowRequestContext,
) -> serde_json::Value {
    serde_json::json!({
        "role": shadow_statement_role_as_str(role),
        "access": shadow_statement_access_as_str(statement),
        "context": json_from_shadow_request_context(context),
        "cypher": statement.cypher,
        "parameters": json_object_from_parameters(&statement.parameters),
    })
}

fn inferred_shadow_statement_role(statement: &CypherFixtureStatement) -> ShadowStatementRole {
    if is_mutation_statement(&statement.cypher) {
        ShadowStatementRole::Mutation
    } else {
        ShadowStatementRole::Read
    }
}

impl ShadowRequestContext<'_> {
    fn default_execute() -> Self {
        Self {
            fixture: "",
            check: None,
            phase: ShadowRequestPhase::Statement,
            statement_index: None,
        }
    }

    fn default_project_graph() -> Self {
        Self {
            fixture: "",
            check: None,
            phase: ShadowRequestPhase::ProjectGraph,
            statement_index: None,
        }
    }
}

fn json_from_shadow_request_context(context: ShadowRequestContext) -> serde_json::Value {
    serde_json::json!({
        "fixture": context.fixture,
        "check": context.check,
        "phase": shadow_request_phase_as_str(context.phase),
        "statement_index": context.statement_index,
    })
}

fn shadow_request_phase_as_str(phase: ShadowRequestPhase) -> &'static str {
    match phase {
        ShadowRequestPhase::FixtureSetup => "fixture_setup",
        ShadowRequestPhase::CheckSetup => "check_setup",
        ShadowRequestPhase::Statement => "statement",
        ShadowRequestPhase::Session => "session",
        ShadowRequestPhase::Effect => "effect",
        ShadowRequestPhase::ProjectGraph => "project_graph",
    }
}

fn shadow_statement_role_as_str(role: ShadowStatementRole) -> &'static str {
    match role {
        ShadowStatementRole::Read => "read",
        ShadowStatementRole::Mutation => "mutation",
        ShadowStatementRole::Statement => "statement",
    }
}

fn shadow_statement_access_as_str(statement: &CypherFixtureStatement) -> &'static str {
    if is_mutation_statement(&statement.cypher) {
        "mutation"
    } else {
        "read"
    }
}

fn shadow_session_access_as_str(statements: &[CypherFixtureStatement]) -> &'static str {
    if statements
        .iter()
        .any(|statement| is_mutation_statement(&statement.cypher))
    {
        "mutation"
    } else {
        "read"
    }
}

fn json_from_value(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::Value::Number((*value).into()),
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(value) => serde_json::Value::String(value.clone()),
        Value::Binary(value) => serde_json::json!({ "$binary": encode_binary_hex(value) }),
        Value::Uuid(value) => serde_json::json!({ "$uuid": value.to_string() }),
        Value::List(values) => {
            serde_json::Value::Array(values.iter().map(json_from_value).collect())
        }
        Value::Map(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_from_value(value)))
                .collect(),
        ),
    }
}

fn encode_binary_hex(value: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        write!(encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

fn decode_binary_hex(encoded: &str) -> Result<Vec<u8>> {
    if !encoded.len().is_multiple_of(2) {
        return Err(SkeinError::Execution(
            "external shadow binary has an odd number of hex digits".to_string(),
        ));
    }
    encoded
        .as_bytes()
        .chunks_exact(2)
        .map(|digits| {
            let high = decode_binary_hex_digit(digits[0])?;
            let low = decode_binary_hex_digit(digits[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn decode_binary_hex_digit(digit: u8) -> Result<u8> {
    match digit {
        b'0'..=b'9' => Ok(digit - b'0'),
        b'a'..=b'f' => Ok(digit - b'a' + 10),
        b'A'..=b'F' => Ok(digit - b'A' + 10),
        _ => Err(SkeinError::Execution(format!(
            "external shadow binary contains invalid hex digit {:?}",
            char::from(digit)
        ))),
    }
}

pub(super) fn decode_external_query_response(
    engine_name: &str,
    response: serde_json::Value,
) -> Result<QueryOutput> {
    validate_external_ok_error_shape(engine_name, &response, "query")?;
    if let Some(error) = response.get("error") {
        return Err(error_from_external_response(engine_name, error));
    }
    let ok = response.get("ok").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' response missing 'ok' or 'error'"
        ))
    })?;
    let rows = ok.get("rows").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' query response missing rows"
        ))
    })?;
    Ok(QueryOutput {
        rows: rows_from_json(engine_name, rows)?.into(),
    })
}

fn validate_external_response_request_id(
    engine_name: &str,
    expected_request_id: u64,
    response: &serde_json::Value,
) -> Result<()> {
    let Some(request_id) = response.get("request_id") else {
        return Ok(());
    };
    let actual_request_id = request_id.as_u64().ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' response request_id must be an unsigned integer"
        ))
    })?;
    if actual_request_id != expected_request_id {
        return Err(SkeinError::Execution(format!(
            "shadow engine '{engine_name}' response request_id {actual_request_id} did not match request_id {expected_request_id}"
        )));
    }
    Ok(())
}

fn validate_external_ok_error_shape(
    engine_name: &str,
    response: &serde_json::Value,
    response_kind: &str,
) -> Result<()> {
    let shape_count =
        usize::from(response.get("ok").is_some()) + usize::from(response.get("error").is_some());
    if shape_count > 1 {
        return Err(SkeinError::Execution(format!(
            "shadow engine '{engine_name}' {response_kind} response must contain only one of 'ok' or 'error'"
        )));
    }
    if shape_count == 0 {
        return Err(SkeinError::Execution(format!(
            "shadow engine '{engine_name}' {response_kind} response missing 'ok' or 'error'"
        )));
    }
    Ok(())
}

pub(super) fn decode_external_ready_response(
    engine_name: &str,
    response: serde_json::Value,
) -> Result<ExternalShadowReady> {
    validate_external_ok_error_shape(engine_name, &response, "ready")?;
    if let Some(error) = response.get("error") {
        return Err(error_from_external_response(engine_name, error));
    }
    let ok = response.get("ok").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' ready response missing 'ok' or 'error'"
        ))
    })?;
    let protocol_version = required_u64(engine_name, ok, "protocol_version")?;
    if protocol_version != EXTERNAL_SHADOW_PROTOCOL_VERSION {
        return Err(SkeinError::Execution(format!(
            "shadow engine '{engine_name}' ready protocol_version {protocol_version} did not match expected {EXTERNAL_SHADOW_PROTOCOL_VERSION}"
        )));
    }
    let capabilities = required_string_array(engine_name, ok, "capabilities")?;
    for capability in REQUIRED_EXTERNAL_SHADOW_CAPABILITIES {
        if !capabilities.iter().any(|value| value == capability) {
            return Err(SkeinError::Execution(format!(
                "shadow engine '{engine_name}' ready response missing required capability '{capability}'"
            )));
        }
    }
    let engine_kind = optional_external_string(engine_name, ok, "engine_kind")?;
    let wrapper_identity = optional_external_string(engine_name, ok, "wrapper_identity")?;
    Ok(ExternalShadowReady {
        protocol_version,
        capabilities,
        engine_kind,
        wrapper_identity,
    })
}

fn external_shadow_stdout_tail(response: &str) -> &str {
    if response.len() <= EXTERNAL_SHADOW_STDOUT_TAIL_BYTES {
        return response;
    }
    let mut start = response.len() - EXTERNAL_SHADOW_STDOUT_TAIL_BYTES;
    while !response.is_char_boundary(start) {
        start += 1;
    }
    &response[start..]
}

fn default_external_shadow_request_timeout() -> Duration {
    Duration::from_millis(DEFAULT_EXTERNAL_SHADOW_REQUEST_TIMEOUT_MS)
}

pub(super) fn decode_external_session_response(
    engine_name: &str,
    response: serde_json::Value,
) -> Result<Vec<QueryOutput>> {
    validate_external_ok_error_shape(engine_name, &response, "session")?;
    if let Some(error) = response.get("error") {
        return Err(error_from_external_response(engine_name, error));
    }
    let ok = response.get("ok").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' session response missing 'ok' or 'error'"
        ))
    })?;
    let outputs = ok
        .get("outputs")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' session response missing outputs"
            ))
        })?;
    outputs
        .iter()
        .enumerate()
        .map(|(index, output)| {
            let rows = output.get("rows").ok_or_else(|| {
                SkeinError::Execution(format!(
                    "shadow engine '{engine_name}' session output {index} missing rows"
                ))
            })?;
            Ok(QueryOutput {
                rows: rows_from_json(engine_name, rows)
                    .map_err(|error| {
                    SkeinError::Execution(format!(
                        "shadow engine '{engine_name}' session output {index} row decoding failed: {error}"
                    ))
                })?
                    .into(),
            })
        })
        .collect()
}

pub(super) fn decode_external_projected_graph_response(
    engine_name: &str,
    response: serde_json::Value,
) -> Result<ProjectedGraphShadowResult> {
    let primary_only = match response.get("primary_only") {
        None => false,
        Some(serde_json::Value::Bool(value)) => *value,
        Some(_) => {
            return Err(SkeinError::Execution(format!(
                "shadow engine '{engine_name}' projected graph response field 'primary_only' must be a boolean"
            )));
        }
    };
    let shape_count = usize::from(primary_only)
        + usize::from(response.get("ok").is_some())
        + usize::from(response.get("error").is_some());
    if shape_count > 1 {
        return Err(SkeinError::Execution(format!(
            "shadow engine '{engine_name}' projected graph response must contain only one of 'ok', 'error', or primary_only"
        )));
    }
    if primary_only {
        return Ok(ProjectedGraphShadowResult::PrimaryOnly {
            reason: optional_external_string(engine_name, &response, "reason")?,
        });
    }
    if let Some(error) = response.get("error") {
        return Err(error_from_external_response(engine_name, error));
    }
    let ok = response.get("ok").ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' projected graph response missing 'ok', 'error', or primary_only"
        ))
    })?;
    Ok(ProjectedGraphShadowResult::Output(
        ProjectedGraphShadowOutput {
            node_count: required_usize(engine_name, ok, "node_count")?,
            edge_count: required_usize(engine_name, ok, "edge_count")?,
            incoming: tuple_vec_u64_list(engine_name, ok, "incoming")?,
            communities: tuple_vec_u64_u64(engine_name, ok, "communities")?,
            hierarchical_communities: tuple_vec_usize_u64_u64(
                engine_name,
                ok,
                "hierarchical_communities",
            )?,
            page_rank_scores: tuple_vec_u64_f64(engine_name, ok, "page_rank_scores")?,
            page_rank_top_node: optional_u64(engine_name, ok, "page_rank_top_node")?,
        },
    ))
}

fn error_from_external_response(engine_name: &str, error: &serde_json::Value) -> SkeinError {
    let class = error
        .get("class")
        .and_then(serde_json::Value::as_str)
        .and_then(ExternalShadowErrorClass::decode)
        .unwrap_or(ExternalShadowErrorClass::Execution);
    let message = error
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("external shadow engine error");
    let message = format!("shadow engine '{engine_name}': {message}");
    match class {
        ExternalShadowErrorClass::Parse => SkeinError::Parse(message),
        ExternalShadowErrorClass::Semantic => SkeinError::Semantic(message),
        ExternalShadowErrorClass::Storage => SkeinError::Storage(message),
        ExternalShadowErrorClass::Execution => SkeinError::Execution(message),
    }
}

impl ExternalShadowErrorClass {
    fn decode(value: &str) -> Option<Self> {
        match value {
            "parse" => Some(Self::Parse),
            "semantic" => Some(Self::Semantic),
            "storage" => Some(Self::Storage),
            "execution" => Some(Self::Execution),
            _ => None,
        }
    }
}

fn rows_from_json(engine_name: &str, value: &serde_json::Value) -> Result<Vec<Row>> {
    let rows = value.as_array().ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' rows must be an array"
        ))
    })?;
    rows.iter()
        .map(|row| row_from_json(engine_name, row))
        .collect()
}

fn row_from_json(engine_name: &str, value: &serde_json::Value) -> Result<Row> {
    let object = value.as_object().ok_or_else(|| {
        SkeinError::Execution(format!(
            "shadow engine '{engine_name}' row must be an object"
        ))
    })?;
    object
        .iter()
        .map(|(key, value)| Ok((key.clone(), value_from_json(engine_name, value)?)))
        .collect()
}

fn value_from_json(engine_name: &str, value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float(value))
            } else {
                Err(SkeinError::Execution(format!(
                    "shadow engine '{engine_name}' returned unsupported number: {value}"
                )))
            }
        }
        serde_json::Value::String(value) => Ok(Value::String(value.clone())),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| value_from_json(engine_name, value))
            .collect::<Result<Vec<_>>>()
            .map(Value::List),
        serde_json::Value::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), value_from_json(engine_name, value)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(Value::Map),
    }
}

fn required_usize(engine_name: &str, object: &serde_json::Value, field: &str) -> Result<usize> {
    required_u64(engine_name, object, field).and_then(|value| {
        usize::try_from(value).map_err(|_| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' exceeds usize"
            ))
        })
    })
}

fn required_u64(engine_name: &str, object: &serde_json::Value, field: &str) -> Result<u64> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' must be a u64"
            ))
        })
}

fn optional_u64(engine_name: &str, object: &serde_json::Value, field: &str) -> Result<Option<u64>> {
    match object.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' must be a u64 or null"
            ))
        }),
    }
}

fn optional_external_string(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Option<String>> {
    match object.get(field) {
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                SkeinError::Execution(format!(
                    "shadow engine '{engine_name}' field '{field}' must be a string or null"
                ))
            }),
    }
}

fn required_string_array(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<String>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            value.as_str().map(str::to_string).ok_or_else(|| {
                SkeinError::Execution(format!(
                    "shadow engine '{engine_name}' field '{field}' item {index} must be a string"
                ))
            })
        })
        .collect()
}

fn tuple_vec_u64_list(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(u64, Vec<u64>)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 2)?;
            let key = item[0]
                .as_u64()
                .ok_or_else(|| tuple_error(engine_name, field))?;
            let values = item[1]
                .as_array()
                .ok_or_else(|| tuple_error(engine_name, field))?
                .iter()
                .map(|value| {
                    value
                        .as_u64()
                        .ok_or_else(|| tuple_error(engine_name, field))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((key, values))
        })
        .collect()
}

fn tuple_vec_u64_u64(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(u64, u64)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 2)?;
            Ok((
                item[0]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
                item[1]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
            ))
        })
        .collect()
}

fn tuple_vec_usize_u64_u64(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(usize, u64, u64)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 3)?;
            Ok((
                usize::try_from(
                    item[0]
                        .as_u64()
                        .ok_or_else(|| tuple_error(engine_name, field))?,
                )
                .map_err(|_| tuple_error(engine_name, field))?,
                item[1]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
                item[2]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
            ))
        })
        .collect()
}

fn tuple_vec_u64_f64(
    engine_name: &str,
    object: &serde_json::Value,
    field: &str,
) -> Result<Vec<(u64, f64)>> {
    let values = required_array(engine_name, object, field)?;
    values
        .iter()
        .map(|item| {
            let item = tuple_array(engine_name, field, item, 2)?;
            Ok((
                item[0]
                    .as_u64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
                item[1]
                    .as_f64()
                    .ok_or_else(|| tuple_error(engine_name, field))?,
            ))
        })
        .collect()
}

fn required_array<'a>(
    engine_name: &str,
    object: &'a serde_json::Value,
    field: &str,
) -> Result<&'a Vec<serde_json::Value>> {
    object
        .get(field)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            SkeinError::Execution(format!(
                "shadow engine '{engine_name}' field '{field}' must be an array"
            ))
        })
}

fn tuple_array<'a>(
    engine_name: &str,
    field: &str,
    value: &'a serde_json::Value,
    len: usize,
) -> Result<&'a Vec<serde_json::Value>> {
    let values = value
        .as_array()
        .ok_or_else(|| tuple_error(engine_name, field))?;
    if values.len() != len {
        return Err(tuple_error(engine_name, field));
    }
    Ok(values)
}

fn tuple_error(engine_name: &str, field: &str) -> SkeinError {
    SkeinError::Execution(format!(
        "shadow engine '{engine_name}' field '{field}' has invalid tuple shape"
    ))
}

#[cfg(test)]
mod protocol_server_tests {
    use super::*;

    #[derive(Default)]
    struct StubPreviousWrapperBackend {
        executed: Vec<String>,
    }

    impl ExternalShadowProtocolBackend for StubPreviousWrapperBackend {
        fn engine_kind(&self) -> &'static str {
            "previous_wrapper"
        }

        fn execute(&mut self, statement: ExternalShadowStatementRequest) -> Result<QueryOutput> {
            self.executed.push(statement.cypher);
            let mut row = Row::new();
            row.insert("value".to_string(), Value::Int(self.executed.len() as i64));
            Ok(QueryOutput {
                rows: vec![row].into(),
            })
        }
    }

    #[test]
    fn protocol_server_reports_previous_wrapper_ready_shape() {
        let mut server = ExternalShadowProtocolServer::new(StubPreviousWrapperBackend::default());

        let response = server.handle_request(&serde_json::json!({
            "protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "ready"
        }));

        assert_eq!(response["ok"]["engine_kind"], "previous_wrapper");
        assert_eq!(
            response["ok"]["capabilities"],
            serde_json::json!(["execute", "execute_session", "project_graph"])
        );
    }

    #[test]
    fn protocol_server_runs_default_session_in_order() {
        let mut server = ExternalShadowProtocolServer::new(StubPreviousWrapperBackend::default());

        let response = server.handle_request(&serde_json::json!({
            "protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "execute_session",
            "statements": [
                { "cypher": "RETURN 1 AS value", "parameters": {} },
                { "cypher": "RETURN 2 AS value", "parameters": {} }
            ]
        }));

        assert_eq!(response["ok"]["outputs"][0]["rows"][0]["value"], 1);
        assert_eq!(response["ok"]["outputs"][1]["rows"][0]["value"], 2);
    }

    #[test]
    fn protocol_server_defaults_project_graph_to_primary_only() {
        let mut server = ExternalShadowProtocolServer::new(StubPreviousWrapperBackend::default());

        let response = server.handle_request(&serde_json::json!({
            "protocol_version": EXTERNAL_SHADOW_PROTOCOL_VERSION,
            "op": "project_graph"
        }));

        assert_eq!(response["primary_only"], true);
        assert_eq!(
            response["reason"],
            "projected graph metadata is not exposed"
        );
    }

    #[test]
    fn trace_report_redacts_unavailable_trace_errors() {
        let trace_path = "redaction-target-shadow-trace.jsonl";

        let report = external_shadow_trace_report_json(trace_path, 1);
        let encoded = serde_json::to_string(&report).unwrap();

        assert_eq!(report["path"], "<redacted>");
        assert_eq!(report["path_redacted"], true);
        assert_eq!(report["summary_available"], false);
        assert_eq!(report["summary_error"], "trace summary unavailable");
        assert_eq!(report["summary_error_code"], "trace_summary_unavailable");
        assert!(!encoded.contains(trace_path));
    }
}
