// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Pessimistic two-transaction campaign.
//!
//! Each transaction runs on its own worker thread so a lock wait is observable.
//! The reference model below is independent of the production lock manager: it
//! encodes the documented pessimistic contract for this one-table schema and
//! predicts, for every scheduled statement, whether it completes or waits, the
//! rows it returns, which error class it raises, and the visible state after
//! every commit.
//!
//! Contract encoded (HawDB `docs/STORAGE.md` and `EMBEDDED_RUNTIME_SPEC.md`,
//! concurrent transaction section, with the lock spans of the SQL lock
//! derivation):
//! - plain `SELECT` reads the private snapshot and takes no lock;
//! - `SELECT ... FOR SHARE|UPDATE` without a predicate takes a shared or
//!   exclusive lock on the whole primary-key range;
//! - `UPDATE`/`DELETE ... WHERE id = $` and `INSERT` take an exclusive point;
//! - a non-key predicate write takes an exclusive table lock;
//! - a Cypher property write of an existing node takes an exclusive lock on
//!   that node (plus label coverage shared by both writers); a Cypher read
//!   takes no lock;
//! - locks are held until commit or rollback; an owner's own locks never block
//!   it, and a request conflicts with the other transaction's held locks and
//!   with its pending (waiting) request;
//! - a waiter whose new wait closes a cycle is the deadlock victim: it is
//!   aborted and all of its locks are released;
//! - after a newly granted, previously uncovered lock, a transaction whose
//!   snapshot epoch is stale refreshes its snapshot if it has no successful
//!   statement yet, and otherwise aborts with a retryable serialization error;
//!   a graph statement never refreshes: it aborts on any stale new lock;
//! - commit rebases the transaction's successful writes onto the current
//!   visible state, in statement order.

use crate::case::{
    apply, interleave, Case, Mode, Operation, Program, Random, Rows, FORMAT_VERSION,
};
use crate::engine::{Failure, Observation};
use hawdb::{ConcurrentDatabase, ConcurrentTransactionOptions, Database, QueryOutput, Value};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::JoinHandle;
use std::time::Duration;

const READ: &str = "SELECT id, value FROM records ORDER BY id";
const GRAPH_READ: &str = "MATCH (n:Record) RETURN n.id AS id, n.value AS value ORDER BY n.id";
/// Lock waits must outlive every probe; a real timeout would be a harness error.
const LOCK_TIMEOUT: Duration = Duration::from_secs(30);
/// A statement predicted to wait must not answer within this probe. A shorter
/// probe raises throughput but can miss a wait that ends too early; it cannot
/// turn a correct run into a mismatch.
static WAIT_PROBE_MS: AtomicU64 = AtomicU64::new(200);

pub fn set_wait_probe(milliseconds: u64) {
    WAIT_PROBE_MS.store(milliseconds, Ordering::Relaxed);
}

fn wait_probe() -> Duration {
    Duration::from_millis(WAIT_PROBE_MS.load(Ordering::Relaxed))
}
/// A statement predicted to finish must answer within this bound.
const COMPLETION_BOUND: Duration = Duration::from_secs(10);

pub const FAMILIES: [&str; 11] = [
    "same_key_wait",
    "disjoint_keys",
    "duplicate_insert",
    "table_vs_point",
    "share_upgrade_deadlock",
    "range_vs_insert",
    "delete_vs_insert",
    "rollback_release",
    "graph_same_node",
    "graph_disjoint_nodes",
    "graph_sql_deadlock",
];

// ---------------------------------------------------------------- generation

pub fn generate(seed: u64, index: u64) -> Case {
    let mut random = Random::new(seed, index);
    let initial: Rows = (0..4).map(|id| (id, (random.next() % 20) as i64)).collect();
    let key = (random.next() % 4) as i64;
    let other = (key + 1) % 4;
    let value = (random.next() % 100) as i64;
    let update = |id| Operation::Update { id, value };
    let share = Operation::LockingRead { exclusive: false };
    let graph = |id| Operation::GraphUpdate { id, value };
    let (left, right) = match index % FAMILIES.len() as u64 {
        0 | 7 => (
            vec![update(key)],
            vec![Operation::Update {
                id: key,
                value: value + 1,
            }],
        ),
        1 => (vec![update(key)], vec![update(other)]),
        2 => (
            vec![Operation::Insert { id: 4, value }],
            vec![Operation::Insert {
                id: 4,
                value: value + 1,
            }],
        ),
        3 => (
            vec![Operation::BroadUpdate {
                minimum: (random.next() % 20) as i64,
                value,
            }],
            vec![update(key)],
        ),
        4 => (vec![share.clone(), update(key)], vec![share, update(other)]),
        5 => (
            vec![Operation::LockingRead { exclusive: true }],
            vec![Operation::Insert { id: 4, value }],
        ),
        6 => (
            vec![Operation::Delete { id: key }],
            vec![
                Operation::Delete { id: key },
                Operation::Insert { id: key, value },
            ],
        ),
        8 => (
            vec![graph(key)],
            vec![Operation::GraphUpdate {
                id: key,
                value: value + 1,
            }],
        ),
        9 => (vec![graph(key)], vec![graph(other)]),
        _ => (
            vec![graph(key), update(other)],
            vec![update(other), graph(key)],
        ),
    };
    // A leading plain read is a successful statement: it decides whether a
    // later wait refreshes the snapshot or aborts the transaction.
    let transactions = [left, right].map(|operations| {
        let mut sequence = Vec::new();
        if random.next().is_multiple_of(2) {
            sequence.push(Operation::Read);
        }
        for op in operations {
            let read = if op.is_graph() {
                Operation::GraphRead
            } else {
                Operation::Read
            };
            sequence.push(op);
            sequence.push(read);
        }
        Program {
            operations: sequence,
            rollback: false,
        }
    });
    let mut case = Case {
        format_version: FORMAT_VERSION,
        seed,
        index,
        mode: Mode::Pessimistic,
        initial,
        transactions,
        schedule: Vec::new(),
    };
    if index % FAMILIES.len() as u64 == 7 || random.next().is_multiple_of(8) {
        case.transactions[(random.next() % 2) as usize].rollback = true;
    }
    interleave(&mut case, &mut random);
    case
}

// ------------------------------------------------------------ reference model

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Resource {
    Point(i64),
    KeyRange,
    Table,
    Node(i64),
}

impl Resource {
    fn overlaps(self, other: Self) -> bool {
        match (self, other) {
            (Self::Point(left), Self::Point(right)) | (Self::Node(left), Self::Node(right)) => {
                left == right
            }
            (Self::Node(_), _) | (_, Self::Node(_)) => false,
            _ => true,
        }
    }

    fn contains(self, other: Self) -> bool {
        match (self, other) {
            (Self::Table, Self::Table | Self::KeyRange | Self::Point(_))
            | (Self::KeyRange, Self::KeyRange | Self::Point(_)) => true,
            (Self::Point(left), Self::Point(right)) | (Self::Node(left), Self::Node(right)) => {
                left == right
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Lock {
    resource: Resource,
    exclusive: bool,
}

impl Lock {
    /// `graph` is the transaction's private node snapshot: graph locks are
    /// derived from the nodes a statement matches there.
    fn for_operation(op: &Operation, graph: &Rows) -> Option<Self> {
        let (resource, exclusive) = match *op {
            Operation::Read | Operation::GraphRead => return None,
            Operation::GraphUpdate { id, .. } if graph.contains_key(&id) => {
                (Resource::Node(id), true)
            }
            Operation::GraphUpdate { .. } => return None,
            Operation::LockingRead { exclusive } => (Resource::KeyRange, exclusive),
            Operation::Update { id, .. }
            | Operation::Delete { id }
            | Operation::Insert { id, .. } => (Resource::Point(id), true),
            Operation::BroadUpdate { .. } => (Resource::Table, true),
        };
        Some(Self {
            resource,
            exclusive,
        })
    }

    fn conflicts(self, other: Self) -> bool {
        (self.exclusive || other.exclusive) && self.resource.overlaps(other.resource)
    }

    fn covers(self, request: Self) -> bool {
        self.resource.contains(request.resource) && (self.exclusive || !request.exclusive)
    }
}

/// Expected statement or terminal outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Rows(Rows),
    Ok,
    Duplicate,
    Serialization,
    Deadlock,
    /// The transaction was aborted earlier; any further call must fail.
    Aborted,
    /// An error outside the classes above.
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Active,
    Waiting,
    Aborted,
    Finished,
}

struct Transaction {
    state: State,
    private: Rows,
    graph: Rows,
    base_epoch: u64,
    successful: usize,
    held: Vec<Lock>,
    pending: Option<(Operation, Lock)>,
    writes: Vec<Operation>,
}

pub struct Model {
    visible: Rows,
    visible_graph: Rows,
    epoch: u64,
    txs: [Transaction; 2],
}

/// What the scheduler should observe after one action.
pub struct Prediction {
    /// `None`: the action waits for a lock.
    pub outcome: Option<Outcome>,
    /// The other transaction's pending statement finished as a consequence.
    pub woken: Option<Outcome>,
}

impl Model {
    pub fn new(initial: &Rows) -> Self {
        let tx = || Transaction {
            state: State::Active,
            private: initial.clone(),
            graph: initial.clone(),
            base_epoch: 0,
            successful: 0,
            held: Vec::new(),
            pending: None,
            writes: Vec::new(),
        };
        Self {
            visible: initial.clone(),
            visible_graph: initial.clone(),
            epoch: 0,
            txs: [tx(), tx()],
        }
    }

    pub fn visible(&self) -> &Rows {
        &self.visible
    }

    pub fn visible_graph(&self) -> &Rows {
        &self.visible_graph
    }

    pub fn is_waiting(&self, tx: usize) -> bool {
        self.txs[tx].state == State::Waiting
    }

    pub fn is_aborted(&self, tx: usize) -> bool {
        self.txs[tx].state == State::Aborted
    }

    fn blocked(&self, tx: usize, request: Lock) -> bool {
        let other = &self.txs[1 - tx];
        other.held.iter().any(|held| held.conflicts(request))
            || other
                .pending
                .as_ref()
                .is_some_and(|(_, waiting)| waiting.conflicts(request))
    }

    fn abort(&mut self, tx: usize) {
        let tx = &mut self.txs[tx];
        tx.state = State::Aborted;
        tx.held.clear();
        tx.pending = None;
        tx.writes.clear();
    }

    /// Runs a statement whose lock (if any) is granted now.
    fn granted(&mut self, tx: usize, op: &Operation, lock: Option<Lock>) -> Outcome {
        if let Some(lock) = lock
            && !self.txs[tx].held.iter().any(|held| held.covers(lock))
        {
            self.txs[tx].held.push(lock);
            if self.txs[tx].base_epoch != self.epoch {
                if self.txs[tx].successful > 0 || op.is_graph() {
                    self.abort(tx);
                    return Outcome::Serialization;
                }
                self.txs[tx].private = self.visible.clone();
                self.txs[tx].graph = self.visible_graph.clone();
                self.txs[tx].base_epoch = self.epoch;
            }
        }
        let current = &mut self.txs[tx];
        match *op {
            Operation::GraphRead => {
                current.successful += 1;
                return Outcome::Rows(current.graph.clone());
            }
            Operation::GraphUpdate { id, value } => {
                current.successful += 1;
                if let Some(node) = current.graph.get_mut(&id) {
                    *node = value;
                    current.writes.push(op.clone());
                }
                return Outcome::Ok;
            }
            _ => {}
        }
        if let Operation::Insert { id, .. } = *op
            && current.private.contains_key(&id)
        {
            return Outcome::Duplicate;
        }
        apply(&mut current.private, op).expect("duplicate insert handled above");
        current.successful += 1;
        match op {
            Operation::Read | Operation::LockingRead { .. } => {
                Outcome::Rows(current.private.clone())
            }
            write => {
                current.writes.push(write.clone());
                Outcome::Ok
            }
        }
    }

    pub fn execute(&mut self, tx: usize, op: &Operation) -> Prediction {
        if self.txs[tx].state == State::Aborted {
            return Prediction {
                outcome: Some(Outcome::Aborted),
                woken: None,
            };
        }
        let lock = Lock::for_operation(op, &self.txs[tx].graph);
        let needs_wait = lock.is_some_and(|lock| {
            !self.txs[tx].held.iter().any(|held| held.covers(lock)) && self.blocked(tx, lock)
        });
        if !needs_wait {
            return Prediction {
                outcome: Some(self.granted(tx, op, lock)),
                woken: None,
            };
        }
        let lock = lock.expect("only a lock request can wait");
        let other = 1 - tx;
        // The other transaction waits on us; our new wait closes the cycle.
        let closes_cycle = self.txs[other]
            .pending
            .as_ref()
            .is_some_and(|(_, waiting)| {
                self.txs[tx]
                    .held
                    .iter()
                    .any(|held| held.conflicts(*waiting))
            });
        if closes_cycle {
            self.abort(tx);
            return Prediction {
                outcome: Some(Outcome::Deadlock),
                woken: self.wake(other),
            };
        }
        self.txs[tx].state = State::Waiting;
        self.txs[tx].pending = Some((op.clone(), lock));
        Prediction {
            outcome: None,
            woken: None,
        }
    }

    fn wake(&mut self, tx: usize) -> Option<Outcome> {
        let (op, lock) = self.txs[tx].pending.clone()?;
        if self.blocked(tx, lock) {
            return None;
        }
        self.txs[tx].pending = None;
        self.txs[tx].state = State::Active;
        Some(self.granted(tx, &op, Some(lock)))
    }

    pub fn terminate(&mut self, tx: usize, rollback: bool) -> Prediction {
        let outcome = match self.txs[tx].state {
            State::Aborted if rollback => Outcome::Ok,
            State::Aborted => Outcome::Aborted,
            _ => {
                if !rollback {
                    let writes = std::mem::take(&mut self.txs[tx].writes);
                    for write in &writes {
                        if let Operation::GraphUpdate { id, value } = *write {
                            self.visible_graph.insert(id, value);
                        } else {
                            apply(&mut self.visible, write)
                                .expect("locks exclude duplicate inserts");
                        }
                    }
                    if !writes.is_empty() {
                        self.epoch += 1;
                    }
                }
                Outcome::Ok
            }
        };
        let current = &mut self.txs[tx];
        current.state = State::Finished;
        current.held.clear();
        current.pending = None;
        current.writes.clear();
        Prediction {
            outcome: Some(outcome),
            woken: self.wake(1 - tx),
        }
    }
}

// ------------------------------------------------------------------- executor

enum Command {
    Execute(Operation),
    Finish { rollback: bool },
}

type Reply = Result<Option<Rows>, String>;

struct Worker {
    commands: Sender<Command>,
    replies: Receiver<Reply>,
    handle: Option<JoinHandle<()>>,
}

fn rows(output: QueryOutput) -> Result<Rows, String> {
    let mut result = Rows::new();
    for row in output.rows {
        let (Some(Value::Int(id)), Some(Value::Int(value))) = (row.get("id"), row.get("value"))
        else {
            return Err(format!("unexpected typed row: {row:?}"));
        };
        if row.len() != 2 || result.insert(*id, *value).is_some() {
            return Err("unexpected column or duplicate primary key in result".into());
        }
    }
    Ok(result)
}

impl Worker {
    /// Begins the transaction on its own thread before any scheduled step.
    fn start(db: ConcurrentDatabase) -> Result<Self, String> {
        let (commands, inbox) = mpsc::channel::<Command>();
        let (outbox, replies) = mpsc::channel::<Reply>();
        let handle = std::thread::spawn(move || {
            let options = ConcurrentTransactionOptions::pessimistic(LOCK_TIMEOUT);
            let mut transaction = match db.begin_transaction(options) {
                Ok(transaction) => {
                    let _ = outbox.send(Ok(None));
                    transaction
                }
                Err(error) => {
                    let _ = outbox.send(Err(error.to_string()));
                    return;
                }
            };
            while let Ok(command) = inbox.recv() {
                match command {
                    Command::Execute(op) => {
                        let (text, parameters) = op.sql();
                        let output = if op.is_graph() {
                            let mut named = std::collections::BTreeMap::new();
                            if let [value, id] = parameters[..] {
                                named.insert("value".to_string(), Value::Int(value));
                                named.insert("id".to_string(), Value::Int(id));
                            }
                            transaction.query_with_params(text, &named)
                        } else {
                            let parameters =
                                parameters.into_iter().map(Value::Int).collect::<Vec<_>>();
                            transaction.query_sql_with_params(text, &parameters)
                        };
                        let reply = output
                            .map_err(|error| error.to_string())
                            .and_then(|output| match op {
                                Operation::Read
                                | Operation::LockingRead { .. }
                                | Operation::GraphRead => rows(output).map(Some),
                                _ => Ok(None),
                            });
                        if outbox.send(reply).is_err() {
                            return;
                        }
                    }
                    Command::Finish { rollback } => {
                        let reply = if rollback {
                            transaction.rollback();
                            Ok(None)
                        } else {
                            transaction
                                .commit()
                                .map(|_| None)
                                .map_err(|error| error.to_string())
                        };
                        let _ = outbox.send(reply);
                        return;
                    }
                }
            }
        });
        let worker = Self {
            commands,
            replies,
            handle: Some(handle),
        };
        match worker.replies.recv_timeout(COMPLETION_BOUND) {
            Ok(Ok(_)) => Ok(worker),
            Ok(Err(error)) => Err(error),
            Err(error) => Err(format!("worker did not begin: {error}")),
        }
    }

    fn send(&self, command: Command) -> Result<(), String> {
        self.commands
            .send(command)
            .map_err(|_| "worker exited".to_string())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // Unblock a waiting worker by finishing the peer first; the scheduler
        // only drops workers after every reply or on failure.
        let _ = self.commands.send(Command::Finish { rollback: true });
        if let Some(handle) = self.handle.take()
            && handle.is_finished()
        {
            let _ = handle.join();
        }
    }
}

fn classify(error: &str) -> Outcome {
    // A call on an already aborted transaction repeats the abort reason.
    if error.contains("is aborted") || error.contains("already finished") {
        Outcome::Aborted
    } else if error.contains("deadlock detected") {
        Outcome::Deadlock
    } else if error.contains("snapshot changed") && error.contains("retry the transaction") {
        Outcome::Serialization
    } else if error.contains("duplicate primary key") {
        Outcome::Duplicate
    } else {
        Outcome::Other
    }
}

fn observed(reply: Reply) -> (Outcome, Option<String>) {
    match reply {
        Ok(Some(rows)) => (Outcome::Rows(rows), None),
        Ok(None) => (Outcome::Ok, None),
        Err(error) => (classify(&error), Some(error)),
    }
}

fn failure(kind: &str, step: usize, detail: impl ToString) -> Failure {
    Failure {
        kind: kind.into(),
        step,
        detail: detail.to_string(),
    }
}

fn compare(
    observation: &mut Observation,
    step: usize,
    tx: usize,
    expected: &Outcome,
    reply: Reply,
) -> Result<(), Failure> {
    let (actual, error) = observed(reply);
    observation.trace.push(serde_json::json!({
        "step": step, "tx": tx, "expected_outcome": expected, "outcome": actual, "error": error,
    }));
    // Any error is acceptable for a call on an already aborted transaction,
    // but an unexpected error must never pass as an expected class.
    let matches = actual == *expected || (*expected == Outcome::Aborted && error.is_some());
    if matches {
        Ok(())
    } else {
        Err(failure(
            "statement_outcome",
            step,
            format!(
                "T{} expected {expected:?}, got {actual:?} ({error:?})",
                tx + 1
            ),
        ))
    }
}

fn await_reply(worker: &Worker, step: usize, tx: usize) -> Result<Reply, Failure> {
    match worker.replies.recv_timeout(COMPLETION_BOUND) {
        Ok(reply) => Ok(reply),
        Err(RecvTimeoutError::Timeout) => Err(failure(
            "unexpected_wait",
            step,
            format!("T{} did not finish within {COMPLETION_BOUND:?}", tx + 1),
        )),
        Err(RecvTimeoutError::Disconnected) => Err(failure("engine_error", step, "worker exited")),
    }
}

fn check_visible(
    db: &ConcurrentDatabase,
    model: &Model,
    step: usize,
    observation: &mut Observation,
) -> Result<(), Failure> {
    let actual = db
        .query_sql(READ)
        .map_err(|e| failure("engine_error", step, e))
        .and_then(|output| rows(output).map_err(|e| failure("engine_error", step, e)))?;
    let expected = model.visible();
    let graph = db
        .query(GRAPH_READ)
        .map_err(|e| failure("engine_error", step, e))
        .and_then(|output| rows(output).map_err(|e| failure("engine_error", step, e)))?;
    let expected_graph = model.visible_graph();
    observation.trace.push(serde_json::json!({
        "step": step, "visible": actual, "expected_visible": expected,
        "visible_graph": graph, "expected_visible_graph": expected_graph,
    }));
    if &actual != expected {
        return Err(failure(
            "visible_rows",
            step,
            format!("expected {expected:?}, got {actual:?}"),
        ));
    }
    if &graph != expected_graph {
        return Err(failure(
            "visible_nodes",
            step,
            format!("expected {expected_graph:?}, got {graph:?}"),
        ));
    }
    Ok(())
}

/// Adds this observation's outcome classes to campaign totals.
pub fn tally(observation: &Observation, totals: &mut std::collections::BTreeMap<String, u64>) {
    for event in &observation.trace {
        if event["expected_wait"].as_bool() == Some(true) {
            *totals.entry("waits".into()).or_default() += 1;
        }
        if let Some(outcome) = event.get("outcome") {
            let class = outcome
                .as_str()
                .map(str::to_string)
                .or_else(|| outcome.as_object().and_then(|o| o.keys().next().cloned()))
                .unwrap_or_default();
            *totals.entry(class).or_default() += 1;
        }
    }
}

/// Orders schedule entries: an entry for a waiting transaction is deferred
/// until the lock wait ends, preserving each transaction's program order.
fn next_entry(remaining: &mut Vec<(usize, usize)>, model: &Model) -> Option<(usize, usize)> {
    let position = remaining
        .iter()
        .position(|&(_, tx)| !model.is_waiting(tx))?;
    Some(remaining.remove(position))
}

pub fn run(case: &Case, observation: &mut Observation) -> Result<(), Failure> {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE records (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)")
        .map_err(|e| failure("engine_error", 0, e))?;
    for (&id, &value) in &case.initial {
        database
            .query_sql_with_params(
                "INSERT INTO records (id, value) VALUES ($1, $2)",
                &[Value::Int(id), Value::Int(value)],
            )
            .map_err(|e| failure("engine_error", 0, e))?;
    }
    for (&id, &value) in &case.initial {
        let parameters = std::collections::BTreeMap::from([
            ("id".to_string(), Value::Int(id)),
            ("value".to_string(), Value::Int(value)),
        ]);
        database
            .query_with_params("CREATE (:Record {id: $id, value: $value})", &parameters)
            .map_err(|e| failure("engine_error", 0, e))?;
    }
    let db = database.into_concurrent();
    let workers = [
        Worker::start(db.clone()).map_err(|e| failure("engine_error", 0, e))?,
        Worker::start(db.clone()).map_err(|e| failure("engine_error", 0, e))?,
    ];
    let pinned = db
        .begin_read_transaction()
        .map_err(|e| failure("engine_error", 0, e))?;
    let mut model = Model::new(&case.initial);
    let mut positions = [0; 2];
    let mut pending: [Option<usize>; 2] = [None, None];
    let mut remaining: Vec<_> = case.schedule.iter().copied().enumerate().collect();
    while let Some((step, tx)) = next_entry(&mut remaining, &model) {
        let program = &case.transactions[tx];
        let op = program.operations.get(positions[tx]).cloned();
        positions[tx] += 1;
        let prediction = match &op {
            // Statements of an aborted transaction are not sent; only its terminal is.
            Some(_) if model.is_aborted(tx) => {
                observation
                    .trace
                    .push(serde_json::json!({"step": step, "tx": tx, "skipped": "aborted"}));
                continue;
            }
            Some(op) => {
                workers[tx]
                    .send(Command::Execute(op.clone()))
                    .map_err(|e| failure("engine_error", step, e))?;
                model.execute(tx, op)
            }
            None => {
                workers[tx]
                    .send(Command::Finish {
                        rollback: program.rollback,
                    })
                    .map_err(|e| failure("engine_error", step, e))?;
                model.terminate(tx, program.rollback)
            }
        };
        observation.trace.push(serde_json::json!({
            "step": step, "tx": tx, "operation": op, "action": op.is_none().then_some(if program.rollback { "rollback" } else { "commit" }),
            "expected_wait": prediction.outcome.is_none(),
        }));
        match &prediction.outcome {
            None => match workers[tx].replies.recv_timeout(wait_probe()) {
                Err(RecvTimeoutError::Timeout) => pending[tx] = Some(step),
                Ok(reply) => {
                    return Err(failure(
                        "missing_wait",
                        step,
                        format!(
                            "T{} expected to wait for a lock, got {:?}",
                            tx + 1,
                            observed(reply).0
                        ),
                    ));
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(failure("engine_error", step, "worker exited"));
                }
            },
            Some(expected) => {
                let reply = await_reply(&workers[tx], step, tx)?;
                compare(observation, step, tx, expected, reply)?;
            }
        }
        let other = 1 - tx;
        match (prediction.woken, pending[other]) {
            (Some(expected), Some(_)) => {
                pending[other] = None;
                let reply = await_reply(&workers[other], step, other)?;
                compare(observation, step, other, &expected, reply)?;
            }
            (None, Some(_)) => {
                if let Ok(reply) = workers[other].replies.recv_timeout(wait_probe()) {
                    return Err(failure(
                        "missing_wait",
                        step,
                        format!(
                            "T{} expected to keep waiting, got {:?}",
                            other + 1,
                            observed(reply).0
                        ),
                    ));
                }
            }
            (Some(_), None) => {
                return Err(failure(
                    "harness_error",
                    step,
                    "model woke a transaction that was not waiting",
                ));
            }
            (None, None) => {}
        }
        check_visible(&db, &model, step, observation)?;
        let original = rows(
            pinned
                .query_sql(READ)
                .map_err(|e| failure("engine_error", step, e))?,
        )
        .map_err(|e| failure("engine_error", step, e))?;
        if original != case.initial {
            return Err(failure(
                "pinned_rows",
                step,
                format!("expected {:?}, got {original:?}", case.initial),
            ));
        }
    }
    if !remaining.is_empty() || pending.iter().any(Option::is_some) {
        return Err(failure(
            "harness_error",
            case.schedule.len().saturating_sub(1),
            "schedule ended while a transaction was still waiting",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(pairs: &[(i64, i64)]) -> Rows {
        pairs.iter().copied().collect()
    }

    #[test]
    fn same_key_writer_waits_and_refreshes_without_prior_statement() {
        let mut model = Model::new(&rows(&[(1, 10)]));
        let first = Operation::Update { id: 1, value: 20 };
        assert_eq!(model.execute(0, &first).outcome, Some(Outcome::Ok));
        assert!(model
            .execute(1, &Operation::Update { id: 1, value: 30 })
            .outcome
            .is_none());
        let commit = model.terminate(0, false);
        assert_eq!(commit.woken, Some(Outcome::Ok));
        assert_eq!(
            model.execute(1, &Operation::Read).outcome,
            Some(Outcome::Rows(rows(&[(1, 30)])))
        );
        model.terminate(1, false);
        assert_eq!(model.visible(), &rows(&[(1, 30)]));
    }

    #[test]
    fn stale_waiter_with_a_prior_statement_aborts() {
        let mut model = Model::new(&rows(&[(1, 10)]));
        assert!(matches!(
            model.execute(1, &Operation::Read).outcome,
            Some(Outcome::Rows(_))
        ));
        model.execute(0, &Operation::Update { id: 1, value: 20 });
        assert!(model
            .execute(1, &Operation::Update { id: 1, value: 30 })
            .outcome
            .is_none());
        assert_eq!(
            model.terminate(0, false).woken,
            Some(Outcome::Serialization)
        );
        assert_eq!(model.terminate(1, false).outcome, Some(Outcome::Aborted));
        assert_eq!(model.visible(), &rows(&[(1, 20)]));
    }

    #[test]
    fn shared_upgrade_cycle_aborts_the_closing_waiter() {
        let mut model = Model::new(&rows(&[(0, 1), (1, 2)]));
        let share = Operation::LockingRead { exclusive: false };
        model.execute(0, &share);
        model.execute(1, &share);
        assert!(model
            .execute(0, &Operation::Update { id: 0, value: 5 })
            .outcome
            .is_none());
        let closing = model.execute(1, &Operation::Update { id: 1, value: 6 });
        assert_eq!(closing.outcome, Some(Outcome::Deadlock));
        assert_eq!(closing.woken, Some(Outcome::Ok));
    }

    #[test]
    fn disjoint_points_do_not_wait_and_a_waiting_request_blocks_later_requests() {
        let mut model = Model::new(&rows(&[(0, 1), (1, 2)]));
        assert_eq!(
            model
                .execute(0, &Operation::Update { id: 0, value: 5 })
                .outcome,
            Some(Outcome::Ok)
        );
        assert_eq!(
            model
                .execute(1, &Operation::Update { id: 1, value: 6 })
                .outcome,
            Some(Outcome::Ok)
        );
        let none = Rows::new();
        let lock = Lock::for_operation(
            &Operation::BroadUpdate {
                minimum: 0,
                value: 0,
            },
            &none,
        )
        .unwrap();
        assert!(
            Lock::for_operation(&Operation::Insert { id: 9, value: 0 }, &none)
                .unwrap()
                .conflicts(lock)
        );
        let node = Lock::for_operation(
            &Operation::GraphUpdate { id: 0, value: 1 },
            &rows(&[(0, 0)]),
        )
        .unwrap();
        assert!(
            !node.conflicts(lock),
            "graph nodes and relational tables are disjoint"
        );
        assert!(Lock::for_operation(
            &Operation::GraphUpdate { id: 5, value: 1 },
            &rows(&[(0, 0)])
        )
        .is_none());
    }

    #[test]
    fn graph_waiter_aborts_even_without_a_prior_statement() {
        let mut model = Model::new(&rows(&[(1, 10)]));
        model.execute(0, &Operation::GraphUpdate { id: 1, value: 20 });
        assert!(model
            .execute(1, &Operation::GraphUpdate { id: 1, value: 30 })
            .outcome
            .is_none());
        assert_eq!(
            model.terminate(0, false).woken,
            Some(Outcome::Serialization)
        );
        assert_eq!(model.visible_graph(), &rows(&[(1, 20)]));
    }

    #[test]
    fn generated_pessimistic_cases_validate() {
        for index in 0..256 {
            let case = generate(7, index);
            case.validate().unwrap();
            assert_eq!(case.mode, Mode::Pessimistic);
        }
    }
}
