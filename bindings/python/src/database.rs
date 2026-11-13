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

use std::ops::Range;
use std::path::{Path, PathBuf};

use hawdb::{
    DatabaseConfig, DatabaseReadTransaction, DurabilityPolicy, EmbeddedDeploymentProfile,
    HawDBEmbedded, HawDBEmbeddedOpenOptions, QueryOutput, QueryRowRef, QueryRows,
    SqlStatementResult, Value,
};
use pyo3::exceptions::{PyIndexError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySequenceMethods, PyString, PyTuple};

use crate::errors::{format_embedded_error, format_hawdb_error, StorageError};
use crate::value::{
    parameter_sequence, py_dict_to_params, py_list_to_params, py_sequence_to_params, value_to_py,
};

/// An open HawDB database.
///
/// Follows the embedded-database idiom (`sqlite3`, `duckdb`): each
/// `execute` call runs one statement and commits on success. The
/// embedded facade applies its own query admission and result budgets,
/// so statements keep HawDB's bounded-resource behavior.
///
/// `Database()` opens an empty in-memory database; `Database(path)` opens
/// a durable project directory.
#[pyclass(module = "hawdb", name = "Database", unsendable)]
pub struct Database {
    inner: Option<HawDBEmbedded>,
    path: Option<PathBuf>,
}

impl Database {
    fn open_impl(
        path: Option<PathBuf>,
        read_only: bool,
        max_read_result_rows: Option<usize>,
        max_read_result_payload_bytes: Option<usize>,
        namespace_durability_anchor: Option<PathBuf>,
    ) -> PyResult<Self> {
        let with_open_options = |mut config: DatabaseConfig| {
            config.namespace_durability_anchor = namespace_durability_anchor.clone();
            if let Some(rows) = max_read_result_rows {
                config.max_read_result_rows = Some(rows);
            }
            if let Some(bytes) = max_read_result_payload_bytes {
                config.max_read_result_payload_bytes = Some(bytes);
            }
            config
        };
        let Some(path) = path else {
            if read_only {
                return Err(PyValueError::new_err("read_only requires a database path"));
            }
            if namespace_durability_anchor.is_some() {
                return Err(PyValueError::new_err(
                    "namespace_durability_anchor requires a database path",
                ));
            }
            return Ok(Self {
                inner: Some(HawDBEmbedded::open_in_memory_with_config(
                    with_open_options(DatabaseConfig::default()),
                )),
                path: None,
            });
        };
        let path = prepare_database_path(&path, read_only)?;
        let inner = if read_only {
            let config = with_open_options(DatabaseConfig {
                read_only: true,
                recovery_mode: hawdb::RecoveryMode::Strict,
                ..Default::default()
            });
            HawDBEmbedded::open_with_options(HawDBEmbeddedOpenOptions {
                path: path.clone(),
                config,
                durability: DurabilityPolicy::default(),
                deployment_profile: EmbeddedDeploymentProfile::SharedHost,
                storage_device: None,
                storage_io: None,
                resource_snapshot: None,
                runtime_governor_config: None,
            })
        } else {
            let options = HawDBEmbeddedOpenOptions::new(path.clone());
            let config = with_open_options(options.config);
            HawDBEmbedded::open_with_options(HawDBEmbeddedOpenOptions { config, ..options })
        };
        match inner {
            Ok(inner) => Ok(Self {
                inner: Some(inner),
                path: Some(path),
            }),
            Err(error) => Err(format_hawdb_error(&error)),
        }
    }

    fn required(&mut self) -> PyResult<&mut HawDBEmbedded> {
        self.inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("database is closed"))
    }

    fn execute_batch(
        slf: Py<Self>,
        py: Python<'_>,
        rows: &Bound<'_, PyAny>,
        execute: impl FnOnce(&mut Transaction) -> PyResult<()>,
    ) -> PyResult<()> {
        slf.borrow_mut(py).required()?;
        if parameter_sequence(rows)?.is_empty()? {
            return Ok(());
        }
        let mut transaction = Self::transaction(slf, py)?;
        execute(&mut transaction)?;
        transaction.commit(py)?;
        Ok(())
    }
}

fn prepare_database_path(path: &Path, read_only: bool) -> PyResult<PathBuf> {
    if path.as_os_str().is_empty() {
        return Err(PyValueError::new_err(
            "database path is empty; pass no path for an in-memory database",
        ));
    }
    if path == Path::new(":memory:") {
        return Err(PyValueError::new_err(
            "\":memory:\" is not supported; pass no path for an in-memory database",
        ));
    }
    // Keep the handle's path reusable after chdir without resolving symlinks.
    let path = std::path::absolute(path).map_err(|error| {
        StorageError::new_err(format!(
            "cannot resolve database path {}: {error}",
            path.display()
        ))
    })?;
    match path.metadata() {
        Ok(metadata) if !metadata.is_dir() => {
            return Err(StorageError::new_err(format!(
                "database path is not a directory: {}",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Windows can report NotFound when an ancestor is a regular file.
            // Existence alone does not establish that the parent is a directory.
            if let Some(parent) = path.parent() {
                match parent.metadata() {
                    Ok(metadata) if !metadata.is_dir() => {
                        return Err(StorageError::new_err(format!(
                            "parent of database path {} is not a directory: {}",
                            path.display(),
                            parent.display()
                        )));
                    }
                    Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                        return Err(StorageError::new_err(format!(
                            "cannot inspect parent of database path {}: {error}",
                            path.display()
                        )));
                    }
                    Err(_) if !read_only => {
                        return Err(StorageError::new_err(format!(
                            "parent directory does not exist: {}",
                            parent.display()
                        )));
                    }
                    _ => {}
                }
            }
        }
        Err(error) => {
            return Err(StorageError::new_err(format!(
                "cannot inspect database path {}: {error}",
                path.display()
            )));
        }
        Ok(_) => {}
    }
    Ok(path)
}

#[pymethods]
impl Database {
    #[new]
    #[pyo3(signature = (path = None, read_only = false, *, max_read_result_rows = None, max_read_result_payload_bytes = None, namespace_durability_anchor = None))]
    fn new(
        path: Option<PathBuf>,
        read_only: bool,
        max_read_result_rows: Option<usize>,
        max_read_result_payload_bytes: Option<usize>,
        namespace_durability_anchor: Option<PathBuf>,
    ) -> PyResult<Self> {
        Self::open_impl(
            path,
            read_only,
            max_read_result_rows,
            max_read_result_payload_bytes,
            namespace_durability_anchor,
        )
    }

    /// Absolute project path this database is bound to, without resolving
    /// symlinks, or None for an in-memory database.
    #[getter]
    fn path(&self) -> Option<String> {
        self.path.as_ref().map(|path| path.display().to_string())
    }

    /// Whether the database still has a live handle.
    #[getter]
    fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    /// Runs one Cypher statement and returns the materialized result.
    ///
    /// `params` is an optional dict of `$name` parameters; values may be
    /// None/bool/int/float/str/bytes/uuid.UUID/list/tuple/dict.
    #[pyo3(signature = (cypher, params = None))]
    fn execute(
        &mut self,
        py: Python<'_>,
        cypher: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<QueryResult> {
        #[cfg(feature = "boundary-profiling")]
        let conversion =
            crate::boundary_profile::PhaseTimer::start(crate::boundary_profile::Phase::Parameters);
        let params = match params {
            Some(dict) => py_dict_to_params(dict)?,
            None => Default::default(),
        };
        #[cfg(feature = "boundary-profiling")]
        drop(conversion);
        let database = self.required()?;
        #[cfg(feature = "boundary-profiling")]
        let engine =
            crate::boundary_profile::PhaseTimer::start(crate::boundary_profile::Phase::Engine);
        let output = py
            .detach(|| database.query_with_params_admitted(cypher, &params))
            .map_err(|error| format_embedded_error(&error))?;
        #[cfg(feature = "boundary-profiling")]
        drop(engine);
        Ok(QueryResult::from_output(output))
    }

    /// Returns the physical Cypher plan without executing the statement.
    #[pyo3(signature = (cypher, params = None))]
    fn explain(
        &mut self,
        py: Python<'_>,
        cypher: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let params = params
            .map(py_dict_to_params)
            .transpose()?
            .unwrap_or_default();
        let database = self.required()?;
        if database.transaction_active() {
            return Err(PyRuntimeError::new_err(
                "a transaction is open on this database",
            ));
        }
        py.detach(move || {
            database
                .database()
                .explain_query_with_params(cypher, &params)
                .map(|output| output.physical_plan.explain(0))
        })
        .map_err(|error| format_hawdb_error(&error))
    }

    /// Experimental retained numeric query; unsupported shapes refuse without
    /// an owned-result fallback. Parameter conversion remains input work.
    #[pyo3(signature = (cypher, params = None, *, options = None))]
    fn execute_retained(
        &mut self,
        py: Python<'_>,
        cypher: &str,
        params: Option<&Bound<'_, PyDict>>,
        options: Option<PyRef<'_, crate::retained::RetainedOptions>>,
    ) -> PyResult<crate::retained::RetainedCursor> {
        let options = match options {
            Some(options) => options.inner,
            None => crate::retained::options(py, None, None, None, false, false)?,
        };
        let params = params
            .map(py_dict_to_params)
            .transpose()?
            .unwrap_or_default();
        let module = py.import("hawdb._hawdb")?.into_any().unbind();
        let database = self.required()?;
        let cursor = py
            .detach(move || database.query_with_params_retained(cypher, &params, options))
            .map_err(|error| crate::retained::error(py, &error))?;
        Ok(crate::retained::RetainedCursor::new(cursor, module))
    }

    /// Indexed complete-node views; recursive payload stays in native owners.
    #[pyo3(signature = (cypher, params = None, *, options = None))]
    fn execute_retained_node(
        &mut self,
        py: Python<'_>,
        cypher: &str,
        params: Option<&Bound<'_, PyDict>>,
        options: Option<PyRef<'_, crate::retained::RetainedOptions>>,
    ) -> PyResult<crate::retained::node::RetainedNodeCursor> {
        let options = options.map_or_else(
            || crate::retained::options(py, None, None, None, true, false),
            |options| Ok(options.inner),
        )?;
        let options = crate::retained::node::node_options(py, options)?;
        let params = params
            .map(py_dict_to_params)
            .transpose()?
            .unwrap_or_default();
        let module = py.import("hawdb._hawdb")?.into_any().unbind();
        let database = self.required()?;
        let cursor = py
            .detach(move || database.query_with_params_retained_node(cypher, &params, options))
            .map_err(|source| crate::retained::error(py, &source))?;
        Ok(crate::retained::node::RetainedNodeCursor::new(
            cursor, module,
        ))
    }

    /// Explicit resource observation; interpreter/application RSS is separate.
    fn retained_snapshot_copy<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let database = self
            .inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("database is closed"))?;
        let Some(snapshot) = database.database().retained_result_snapshot() else {
            return Ok(None);
        };
        let result = PyDict::new(py);
        result.set_item("budget_bytes", snapshot.budget_bytes)?;
        result.set_item("handle_limit", snapshot.handle_limit)?;
        result.set_item("retained_bytes", snapshot.retained_bytes)?;
        result.set_item("peak_retained_bytes", snapshot.peak_retained_bytes)?;
        result.set_item("buffer_owners", snapshot.buffer_owners)?;
        result.set_item("view_handles", snapshot.view_handles)?;
        result.set_item("peak_view_handles", snapshot.peak_view_handles)?;
        result.set_item("backpressure_events", snapshot.backpressure_events)?;
        Ok(Some(result))
    }

    /// Runs one SQL statement and returns the materialized result.
    ///
    /// `params` is an optional list of positional parameters with the same
    /// supported value types as `execute`. For an INSERT, UPDATE or DELETE
    /// on a RowPage table, `affected_rows` of the result is the committed
    /// row count.
    #[pyo3(signature = (sql, params = None))]
    fn execute_sql(
        &mut self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyList>>,
    ) -> PyResult<QueryResult> {
        #[cfg(feature = "boundary-profiling")]
        let conversion =
            crate::boundary_profile::PhaseTimer::start(crate::boundary_profile::Phase::Parameters);
        let values = match params {
            Some(list) => py_list_to_params(list)?,
            None => Vec::new(),
        };
        #[cfg(feature = "boundary-profiling")]
        drop(conversion);
        let database = self.required()?;
        if database.transaction_active() {
            return Err(PyRuntimeError::new_err(
                "a transaction is open on this database",
            ));
        }
        #[cfg(feature = "boundary-profiling")]
        let engine =
            crate::boundary_profile::PhaseTimer::start(crate::boundary_profile::Phase::Engine);
        let result = py
            .detach(|| {
                database
                    .database_mut()
                    .query_sql_with_result_and_params(sql, &values)
            })
            .map_err(|error| format_hawdb_error(&error))?;
        #[cfg(feature = "boundary-profiling")]
        drop(engine);
        Ok(QueryResult::from_sql_result(result))
    }

    /// Runs one Cypher statement for each parameter dict in one transaction.
    /// Returns None; an empty sequence opens no transaction.
    fn executemany(
        slf: Py<Self>,
        py: Python<'_>,
        cypher: &str,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        Self::execute_batch(slf, py, rows, |transaction| {
            transaction.executemany(py, cypher, rows)
        })
    }

    /// Runs one SQL statement for each positional parameter sequence in one transaction.
    /// Returns None; an empty sequence opens no transaction.
    fn executemany_sql(
        slf: Py<Self>,
        py: Python<'_>,
        sql: &str,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        Self::execute_batch(slf, py, rows, |transaction| {
            transaction.executemany_sql(py, sql, rows)
        })
    }

    /// Begins an explicit multi-statement transaction.
    ///
    /// `with db.transaction() as tx:` runs `tx.execute`/`tx.execute_sql`
    /// inside one transaction: leaving the block without an exception
    /// commits, an exception rolls back and re-raises. Nested calls on the
    /// same database fail while a transaction is open.
    fn transaction(slf: Py<Self>, py: Python<'_>) -> PyResult<Transaction> {
        {
            let mut database = slf.borrow_mut(py);
            database
                .required()?
                .begin_transaction()
                .map_err(|error| format_hawdb_error(&error))?;
        }
        Ok(Transaction {
            db: slf,
            active: true,
        })
    }

    /// Pins the current committed state for a stable read scope.
    ///
    /// `with db.read_transaction() as tx:` runs `tx.execute`/`tx.execute_sql`
    /// against one snapshot; write statements raise the engine's
    /// read-transaction error.
    fn read_transaction(&mut self) -> PyResult<ReadTransaction> {
        let inner = self
            .required()?
            .begin_read_transaction()
            .map_err(|error| format_hawdb_error(&error))?;
        Ok(ReadTransaction { inner: Some(inner) })
    }

    /// Closes the database handle. Safe to call more than once.
    fn close(&mut self) {
        self.inner = None;
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, pyo3::types::PyAny>,
        _exc_value: &Bound<'_, pyo3::types::PyAny>,
        _traceback: &Bound<'_, pyo3::types::PyAny>,
    ) -> bool {
        self.close();
        false
    }

    fn __repr__(&self) -> String {
        let state = if self.inner.is_some() {
            "open"
        } else {
            "closed"
        };
        match &self.path {
            Some(path) => format!("hawdb.Database({:?}, {})", path.display(), state),
            None => format!("hawdb.Database(<in-memory>, {})", state),
        }
    }
}

/// Rows of one statement result.
///
/// The engine's result is materialized when the query returns, and fetching
/// never touches the engine again. Rows become Python objects only as they
/// are fetched: each fetch builds new dicts (or tuples) that the caller owns,
/// so the result never keeps a Python copy of a row it has handed out.
#[pyclass(module = "hawdb", name = "QueryResult")]
pub struct QueryResult {
    /// The engine's rows, moved out of its `QueryOutput` without a copy.
    /// They stay alive, and keep their result-memory charge, until the
    /// result is dropped; fetches convert from them.
    rows: QueryRows,
    cursor: usize,
    affected_rows: Option<usize>,
}

impl QueryResult {
    fn from_output(output: QueryOutput) -> Self {
        Self {
            rows: output.rows,
            cursor: 0,
            affected_rows: None,
        }
    }

    /// Converts a SQL statement result, keeping its RowPage mutation count.
    fn from_sql_result(result: SqlStatementResult) -> Self {
        Self {
            affected_rows: result.mutation.map(|mutation| mutation.affected_rows),
            ..Self::from_output(result.output)
        }
    }

    /// Converts rows `range` with `convert`, which receives the row's values
    /// in column order and the interned column names.
    fn convert_rows<'py, T>(
        &self,
        py: Python<'py>,
        range: Range<usize>,
        mut convert: impl FnMut(&[Bound<'py, PyString>], QueryRowRef<'_>) -> PyResult<T>,
    ) -> PyResult<Vec<T>> {
        #[cfg(feature = "boundary-profiling")]
        let _conversion =
            crate::boundary_profile::PhaseTimer::start(crate::boundary_profile::Phase::Results);
        // One key object per column, shared by every dict of this fetch.
        let names = self
            .rows
            .schema()
            .columns()
            .iter()
            .map(|name| PyString::new(py, name))
            .collect::<Vec<_>>();
        range
            .map(|index| {
                let row = self.rows.row(index).expect("row ordinal is in range");
                convert(&names, row)
            })
            .collect()
    }

    fn dicts<'py>(
        &self,
        py: Python<'py>,
        range: Range<usize>,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        self.convert_rows(py, range, |names, row| {
            let dict = PyDict::new(py);
            for (name, value) in names.iter().zip(row.values()) {
                dict.set_item(name, value_to_py(py, value)?)?;
            }
            Ok(dict)
        })
    }

    fn tuples<'py>(
        &self,
        py: Python<'py>,
        range: Range<usize>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        self.convert_rows(py, range, |_, row| {
            PyTuple::new(
                py,
                row.values()
                    .map(|value| value_to_py(py, value))
                    .collect::<PyResult<Vec<_>>>()?,
            )
        })
    }

    /// The next `size` rows from the cursor, clamped to the row count.
    fn next_range(&self, size: usize) -> Range<usize> {
        self.cursor..self.cursor.saturating_add(size).min(self.rows.len())
    }
}

#[pymethods]
impl QueryResult {
    /// Column names of the result, in declaration order.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.rows.schema().columns().to_vec()
    }

    /// Number of rows in the result, fetched or not.
    #[getter]
    fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Rows inserted, updated or deleted by a SQL INSERT, UPDATE or DELETE
    /// on a RowPage table, or None for any other statement (queries, DDL,
    /// branch commands, Strict Append writes, Cypher). Conflict no-ops of
    /// `ON CONFLICT DO NOTHING` are not counted. Inside a transaction the
    /// count is of staged rows, published on commit.
    #[getter]
    fn affected_rows(&self) -> Option<usize> {
        self.affected_rows
    }

    /// Returns the next row as a new dict, or None when exhausted.
    fn fetchone<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        Ok(self.fetchmany(py, 1)?.pop())
    }

    /// Returns up to `size` rows starting at the cursor, as new dicts.
    #[pyo3(signature = (size = 1))]
    fn fetchmany<'py>(
        &mut self,
        py: Python<'py>,
        size: usize,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let range = self.next_range(size);
        let rows = self.dicts(py, range.clone())?;
        self.cursor = range.end;
        Ok(rows)
    }

    /// Returns up to `size` rows starting at the cursor, as new tuples in
    /// column order. Shares the cursor with the dict fetches.
    #[pyo3(signature = (size = 1))]
    fn fetchmany_tuples<'py>(
        &mut self,
        py: Python<'py>,
        size: usize,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let range = self.next_range(size);
        let rows = self.tuples(py, range.clone())?;
        self.cursor = range.end;
        Ok(rows)
    }

    /// Returns every remaining row from the cursor, as new dicts.
    fn fetchall<'py>(&mut self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
        self.fetchmany(py, usize::MAX)
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        self.fetchone(py)
    }

    /// The first non-null value of column `column` in the whole result,
    /// independent of the fetch cursor, or None when every value is null.
    ///
    /// Scans the engine's values and converts only the one it returns; the
    /// DB-API derives `description` type codes from it.
    fn _first_non_null(&self, py: Python<'_>, column: usize) -> PyResult<Option<Py<PyAny>>> {
        if column >= self.rows.schema().len() {
            return Err(PyIndexError::new_err("column index out of range"));
        }
        self.rows
            .value_rows()
            .map(|values| &values[column])
            .find(|value| !matches!(value, Value::Null))
            .map(|value| value_to_py(py, value))
            .transpose()
    }

    /// Notebook HTML table of the first rows; the plain `repr` is unchanged.
    ///
    /// Always renders from the first row, independent of the fetch cursor.
    /// The row cap lives in `hawdb._display`; only rows within it are
    /// converted, so large results do not build a full list to draw a few rows.
    fn _repr_html_(&self, py: Python<'_>) -> PyResult<String> {
        let display = py.import("hawdb._display")?;
        let limit: usize = display.getattr("DISPLAY_ROW_LIMIT")?.extract()?;
        let rows = self.dicts(py, 0..limit.min(self.rows.len()))?;
        display
            .getattr("render_html")?
            .call1((self.columns(), rows, self.rows.len()))?
            .extract()
    }

    /// Rows `start..stop` for notebook display, as new dicts, independent of
    /// the fetch cursor.
    ///
    /// `stop` defaults to the row count; both bounds are clamped to it.
    #[pyo3(signature = (start = 0, stop = None))]
    fn _display_rows<'py>(
        &self,
        py: Python<'py>,
        start: usize,
        stop: Option<usize>,
    ) -> PyResult<Vec<Bound<'py, PyDict>>> {
        let stop = stop.unwrap_or(self.rows.len()).min(self.rows.len());
        self.dicts(py, start.min(stop)..stop)
    }

    fn __repr__(&self) -> String {
        format!(
            "hawdb.QueryResult(columns={:?}, rows={})",
            self.rows.schema().columns(),
            self.rows.len()
        )
    }
}

/// An open multi-statement transaction on a [`Database`].
///
/// Obtained from `db.transaction()`. Statements run inside the engine's
/// single user transaction and see their own writes. `commit()` publishes
/// all staged mutations; `rollback()` discards them. Leaving a `with` block
/// commits on success and rolls back on exception. Calling into the parent
/// `Database` while a transaction is open raises `RuntimeError`.
#[pyclass(module = "hawdb", name = "Transaction", unsendable)]
pub struct Transaction {
    db: Py<Database>,
    active: bool,
}

impl Transaction {
    fn required(&mut self) -> PyResult<()> {
        if self.active {
            Ok(())
        } else {
            Err(PyRuntimeError::new_err("transaction is closed"))
        }
    }

    /// Rolls the transaction back if it is still open; used by `__exit__`
    /// on the exception path and by `rollback`.
    fn abandon(&mut self, py: Python<'_>) -> PyResult<()> {
        if !self.active {
            return Ok(());
        }
        self.active = false;
        self.db
            .borrow_mut(py)
            .required()?
            .rollback_transaction()
            .map_err(|error| format_hawdb_error(&error))
    }

    fn execute_batch(
        &mut self,
        py: Python<'_>,
        rows: &Bound<'_, PyAny>,
        mut execute: impl FnMut(&mut Self, &Bound<'_, PyAny>) -> PyResult<()>,
    ) -> PyResult<()> {
        self.required()?;
        let result = (|| {
            for row in parameter_sequence(rows)?.try_iter()? {
                execute(self, &row?)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            // There are no savepoints: a caught batch error must not leave a
            // committable prefix in the caller's transaction.
            if let Err(rollback_error) = self.abandon(py) {
                rollback_error.set_cause(py, Some(error));
                return Err(rollback_error);
            }
            return Err(error);
        }
        Ok(())
    }
}

#[pymethods]
impl Transaction {
    /// Runs one Cypher statement inside the transaction. Same signature and
    /// parameter types as `Database.execute`.
    #[pyo3(signature = (cypher, params = None))]
    fn execute(
        &mut self,
        py: Python<'_>,
        cypher: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<QueryResult> {
        let params = match params {
            Some(dict) => py_dict_to_params(dict)?,
            None => Default::default(),
        };
        self.required()?;
        let mut db = self.db.borrow_mut(py);
        let embedded = db.required()?;
        let output = py
            .detach(|| embedded.transaction_query_with_params(cypher, &params))
            .map_err(|error| format_hawdb_error(&error))?;
        Ok(QueryResult::from_output(output))
    }

    /// Runs one SQL statement inside the transaction. Same signature and
    /// parameter types as `Database.execute_sql`. `affected_rows` of the
    /// result counts the rows the statement staged; they are published only
    /// when the transaction commits.
    #[pyo3(signature = (sql, params = None))]
    fn execute_sql(
        &mut self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyList>>,
    ) -> PyResult<QueryResult> {
        let values = match params {
            Some(list) => py_list_to_params(list)?,
            None => Vec::new(),
        };
        self.required()?;
        let mut db = self.db.borrow_mut(py);
        let embedded = db.required()?;
        let result = py
            .detach(|| embedded.transaction_query_sql_with_result_and_params(sql, &values))
            .map_err(|error| format_hawdb_error(&error))?;
        Ok(QueryResult::from_sql_result(result))
    }

    /// Runs a batch of Cypher bindings without committing the outer transaction.
    /// Returns None; any batch failure rolls back and closes the transaction.
    fn executemany(
        &mut self,
        py: Python<'_>,
        cypher: &str,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.execute_batch(py, rows, |transaction, row| {
            let params = py_dict_to_params(row.cast::<PyDict>()?)?;
            let mut db = transaction.db.borrow_mut(py);
            let embedded = db.required()?;
            py.detach(|| embedded.transaction_query_with_params(cypher, &params))
                .map(|_| ())
                .map_err(|error| format_hawdb_error(&error))
        })
    }

    /// Runs a batch of SQL bindings without committing the outer transaction.
    /// Returns None; any batch failure rolls back and closes the transaction.
    fn executemany_sql(
        &mut self,
        py: Python<'_>,
        sql: &str,
        rows: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.execute_batch(py, rows, |transaction, row| {
            let params = py_sequence_to_params(parameter_sequence(row)?)?;
            let mut db = transaction.db.borrow_mut(py);
            let embedded = db.required()?;
            py.detach(|| embedded.transaction_query_sql_with_params(sql, &params))
                .map(|_| ())
                .map_err(|error| format_hawdb_error(&error))
        })
    }

    /// Publishes every staged statement as one durable commit and closes
    /// the transaction.
    fn commit(&mut self, py: Python<'_>) -> PyResult<QueryResult> {
        self.required()?;
        // A failed commit abandons the transaction like
        // `DatabaseTransaction::commit`; the transaction is over either way.
        self.active = false;
        let mut db = self.db.borrow_mut(py);
        let embedded = db.required()?;
        let output = py
            .detach(|| embedded.commit_transaction())
            .map_err(|error| format_hawdb_error(&error))?;
        Ok(QueryResult::from_output(output))
    }

    /// Abandons the transaction without committing. Safe to call once.
    fn rollback(&mut self, py: Python<'_>) -> PyResult<()> {
        self.abandon(py)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        py: Python<'_>,
        exc_type: &Bound<'_, pyo3::types::PyAny>,
        _exc_value: &Bound<'_, pyo3::types::PyAny>,
        _traceback: &Bound<'_, pyo3::types::PyAny>,
    ) -> PyResult<bool> {
        if !self.active {
            return Ok(false);
        }
        if exc_type.is_none() {
            self.commit(py)?;
        } else {
            self.abandon(py)?;
        }
        Ok(false)
    }

    fn __repr__(&self) -> String {
        let state = if self.active { "open" } else { "closed" };
        format!("hawdb.Transaction({state})")
    }
}

impl Drop for Transaction {
    /// A transaction dropped outside its `with` block rolls back so the
    /// parent database does not stay locked out. Skipped when the
    /// interpreter has already finalized.
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let _ = Python::try_attach(|py| {
            if let Ok(mut db) = self.db.try_borrow_mut(py)
                && let Some(inner) = db.inner.as_mut()
            {
                let _ = inner.rollback_transaction();
            }
        });
    }
}

/// A stable read-only scope on a [`Database`].
///
/// Obtained from `db.read_transaction()`. Statements run against the
/// committed state pinned at `begin_read_transaction`; write statements
/// raise the engine's read-transaction error. Leaving the `with` block (or
/// `close()`) releases the snapshot.
#[pyclass(module = "hawdb", name = "ReadTransaction", unsendable)]
pub struct ReadTransaction {
    inner: Option<DatabaseReadTransaction>,
}

impl ReadTransaction {
    fn required(&mut self) -> PyResult<&mut DatabaseReadTransaction> {
        self.inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("read transaction is closed"))
    }
}

#[pymethods]
impl ReadTransaction {
    /// Runs one Cypher statement against the pinned snapshot. Same
    /// signature and parameter types as `Database.execute`.
    #[pyo3(signature = (cypher, params = None))]
    fn execute(
        &mut self,
        py: Python<'_>,
        cypher: &str,
        params: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<QueryResult> {
        let params = match params {
            Some(dict) => py_dict_to_params(dict)?,
            None => Default::default(),
        };
        let transaction = self.required()?;
        let output = py
            .detach(|| transaction.query_with_params(cypher, &params))
            .map_err(|error| format_hawdb_error(&error))?;
        Ok(QueryResult::from_output(output))
    }

    /// Runs one SQL statement against the pinned snapshot. Same signature
    /// and parameter types as `Database.execute_sql`.
    #[pyo3(signature = (sql, params = None))]
    fn execute_sql(
        &mut self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyList>>,
    ) -> PyResult<QueryResult> {
        let values = match params {
            Some(list) => py_list_to_params(list)?,
            None => Vec::new(),
        };
        let transaction = self.required()?;
        let output = py
            .detach(|| transaction.query_sql_with_params(sql, &values))
            .map_err(|error| format_hawdb_error(&error))?;
        Ok(QueryResult::from_output(output))
    }

    /// Releases the pinned snapshot. Safe to call more than once.
    fn close(&mut self) {
        self.inner = None;
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: &Bound<'_, pyo3::types::PyAny>,
        _exc_value: &Bound<'_, pyo3::types::PyAny>,
        _traceback: &Bound<'_, pyo3::types::PyAny>,
    ) -> bool {
        self.close();
        false
    }

    fn __repr__(&self) -> String {
        let state = if self.inner.is_some() {
            "open"
        } else {
            "closed"
        };
        format!("hawdb.ReadTransaction({state})")
    }
}

/// Opens a HawDB database, creating a durable project at `path` if needed.
///
/// Called without `path` it returns an empty in-memory database: no project
/// directory is created and closing the handle discards all data. Unlike
/// sqlite3, `":memory:"` is not an in-memory path and raises ValueError, as does
/// an empty path. Filesystem path errors raise StorageError.
/// `read_only=True` requires a path and opens an existing database read-only.
/// Optional read budgets pass through to the engine; None keeps its defaults.
/// A namespace_durability_anchor is an existing ancestor whose name and ancestry
/// the host guarantees durable and stable for the handle's lifetime. Namespace
/// synchronization includes that boundary. None keeps the engine's mount walk.
#[pyfunction]
#[pyo3(signature = (path = None, read_only = false, *, max_read_result_rows = None, max_read_result_payload_bytes = None, namespace_durability_anchor = None))]
pub fn open(
    path: Option<PathBuf>,
    read_only: bool,
    max_read_result_rows: Option<usize>,
    max_read_result_payload_bytes: Option<usize>,
    namespace_durability_anchor: Option<PathBuf>,
) -> PyResult<Database> {
    Database::open_impl(
        path,
        read_only,
        max_read_result_rows,
        max_read_result_payload_bytes,
        namespace_durability_anchor,
    )
}
