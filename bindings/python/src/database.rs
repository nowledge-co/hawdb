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

use std::path::PathBuf;

use hawdb::{
    DatabaseConfig, DatabaseReadTransaction, DurabilityPolicy, EmbeddedDeploymentProfile,
    HawDBEmbedded, HawDBEmbeddedOpenOptions, QueryOutput, Value,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::errors::{format_embedded_error, format_hawdb_error, Error};
use crate::value::{py_dict_to_params, py_list_to_params, value_to_py};

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
    fn open_impl(path: Option<PathBuf>, read_only: bool) -> PyResult<Self> {
        let Some(path) = path else {
            if read_only {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "read_only requires a database path",
                ));
            }
            return Ok(Self {
                inner: Some(HawDBEmbedded::open_in_memory()),
                path: None,
            });
        };
        let inner = if read_only {
            let config = DatabaseConfig {
                read_only: true,
                ..Default::default()
            };
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
            HawDBEmbedded::open(&path)
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
}

#[pymethods]
impl Database {
    #[new]
    #[pyo3(signature = (path = None, read_only = false))]
    fn new(path: Option<PathBuf>, read_only: bool) -> PyResult<Self> {
        Self::open_impl(path, read_only)
    }

    /// Project path this database is bound to, or None for an in-memory
    /// database.
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
        let params = match params {
            Some(dict) => py_dict_to_params(dict)?,
            None => Default::default(),
        };
        let database = self.required()?;
        let output = py
            .detach(|| database.query_with_params_admitted(cypher, &params))
            .map_err(|error| format_embedded_error(&error))?;
        QueryResult::from_output(py, output)
    }

    /// Runs one SQL statement and returns the materialized result.
    ///
    /// `params` is an optional list of positional parameters with the same
    /// supported value types as `execute`.
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
        let database = self.required()?;
        if database.transaction_active() {
            return Err(PyRuntimeError::new_err(
                "a transaction is open on this database",
            ));
        }
        let output = py
            .detach(|| database.database_mut().query_sql_with_params(sql, &values))
            .map_err(|error| format_hawdb_error(&error))?;
        QueryResult::from_output(py, output)
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

/// Materialized rows of one statement result.
///
/// Rows are materialized eagerly when the query returns; iterating or
/// fetching never touches the engine again.
#[pyclass(module = "hawdb", name = "QueryResult")]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Py<PyDict>>,
    cursor: usize,
}

impl QueryResult {
    fn from_output(py: Python<'_>, output: QueryOutput) -> PyResult<Self> {
        let columns = output.schema().columns().to_vec();
        let mut rows = Vec::new();
        for row in output.value_rows() {
            let dict = PyDict::new(py);
            for (index, column) in columns.iter().enumerate() {
                let value = row.get(index).cloned().unwrap_or(Value::Null);
                dict.set_item(column, value_to_py(py, &value)?)?;
            }
            rows.push(dict.unbind());
        }
        Ok(Self {
            columns,
            rows,
            cursor: 0,
        })
    }
}

#[pymethods]
impl QueryResult {
    /// Column names of the result, in declaration order.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Number of materialized rows.
    #[getter]
    fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns the next row as a dict, or None when exhausted.
    fn fetchone(&mut self, py: Python<'_>) -> Option<Py<PyDict>> {
        let row = self.rows.get(self.cursor).map(|row| row.clone_ref(py));
        if row.is_some() {
            self.cursor += 1;
        }
        row
    }

    /// Returns up to `size` rows starting at the cursor.
    #[pyo3(signature = (size = 1))]
    fn fetchmany(&mut self, py: Python<'_>, size: usize) -> Vec<Py<PyDict>> {
        let end = (self.cursor + size).min(self.rows.len());
        let rows = self.rows[self.cursor..end]
            .iter()
            .map(|row| row.clone_ref(py))
            .collect();
        self.cursor = end;
        rows
    }

    /// Returns every remaining row from the cursor.
    fn fetchall(&mut self, py: Python<'_>) -> Vec<Py<PyDict>> {
        let rows = self.rows[self.cursor..]
            .iter()
            .map(|row| row.clone_ref(py))
            .collect();
        self.cursor = self.rows.len();
        rows
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> Option<Py<PyDict>> {
        let row = self.rows.get(self.cursor).map(|row| row.clone_ref(py));
        if row.is_some() {
            self.cursor += 1;
        }
        row
    }

    fn __repr__(&self) -> String {
        format!(
            "hawdb.QueryResult(columns={:?}, rows={})",
            self.columns,
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
        QueryResult::from_output(py, output)
    }

    /// Runs one SQL statement inside the transaction. Same signature and
    /// parameter types as `Database.execute_sql`.
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
        let output = py
            .detach(|| embedded.transaction_query_sql_with_params(sql, &values))
            .map_err(|error| format_hawdb_error(&error))?;
        QueryResult::from_output(py, output)
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
        QueryResult::from_output(py, output)
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
        QueryResult::from_output(py, output)
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
        QueryResult::from_output(py, output)
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
/// directory is created and closing the handle discards all data.
/// `read_only=True` requires a path and opens an existing database read-only.
#[pyfunction]
#[pyo3(signature = (path = None, read_only = false))]
pub fn open(path: Option<PathBuf>, read_only: bool) -> PyResult<Database> {
    if let Some(path) = &path
        && !read_only
        && !path.exists()
    {
        // HawDB creates the database on open; surface a hint only when the
        // parent directory is missing so a typo fails loudly instead of
        // creating a stray directory tree.
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            return Err(Error::new_err(format!(
                "parent directory does not exist: {}",
                parent.display()
            )));
        }
    }
    Database::open_impl(path, read_only)
}
