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

mod database;
mod errors;
mod value;

use pyo3::prelude::*;

pub use database::{open, Database, QueryResult};
pub use errors::register_exceptions;

// The `hawdb` Python package is a mixed layout: the public package is the
// hand-written `python/hawdb/` directory, and this native module is built as
// the private `hawdb._hawdb` submodule (`module-name` in pyproject.toml).
#[pymodule]
fn _hawdb(module: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = module.py();

    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    module.add_class::<Database>()?;
    module.add_class::<QueryResult>()?;
    module.add_function(wrap_pyfunction!(open, module)?)?;

    let exceptions = PyModule::new(py, "hawdb.exceptions")?;
    register_exceptions(py, &exceptions)?;
    module.add_submodule(&exceptions)?;
    // Make `hawdb.exceptions` importable as a package-level module.
    py.import("sys")?
        .getattr("modules")?
        .set_item("hawdb.exceptions", &exceptions)?;

    Ok(())
}
