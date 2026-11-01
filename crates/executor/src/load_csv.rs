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

//! `LOAD CSV` file access policy and record-to-row conversion.
//!
//! Files are read only beneath an explicitly configured import directory.
//! Records are streamed through a fixed-size buffer; no plan or cache retains
//! a resolved location or any file contents.

mod reader;
#[cfg(test)]
mod tests;

pub use reader::{CsvField, CsvRecordReader, CSV_READ_BUFFER_BYTES};

use hawdb_core::{HawDBError, Result, Value};
use hawdb_plan_cypher::{CsvLocation, LoadCsvSource};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Resource bounds for one `LOAD CSV` source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CsvImportLimits {
    /// Maximum bytes read from one file, checked before opening against the
    /// file length and again while streaming.
    pub max_file_bytes: u64,
    /// Maximum field bytes in one record, excluding quotes and separators.
    pub max_record_bytes: usize,
    /// Maximum fields in one record, including the header record.
    pub max_fields_per_record: usize,
}

impl Default for CsvImportLimits {
    fn default() -> Self {
        Self {
            max_file_bytes: 4 * 1024 * 1024 * 1024,
            max_record_bytes: 4 * 1024 * 1024,
            max_fields_per_record: 4096,
        }
    }
}

/// The directory `LOAD CSV` may read from, and its per-file limits.
///
/// Without a policy, `LOAD CSV` is refused. With one, the canonical path of
/// every source, after resolving `..` components and symbolic links, must be
/// a regular file inside the canonical import root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CsvImportPolicy {
    root: PathBuf,
    limits: CsvImportLimits,
}

impl CsvImportPolicy {
    pub fn new(root: impl Into<PathBuf>, limits: CsvImportLimits) -> Self {
        Self {
            root: root.into(),
            limits,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn limits(&self) -> CsvImportLimits {
        self.limits
    }

    /// Resolves `location` to a canonical file path inside the import root.
    ///
    /// Errors that depend only on the statement are `Semantic`: a malformed
    /// or non-local location and a location outside the import directory
    /// (after resolving symbolic links, or lexically when the target does not
    /// exist). Filesystem conditions observed at run time are `Execution`: an
    /// import root or source that cannot be accessed.
    pub fn resolve(&self, location: &str) -> Result<PathBuf> {
        let root = std::fs::canonicalize(&self.root).map_err(|error| {
            HawDBError::Execution(format!(
                "LOAD CSV import root '{}' cannot be resolved: {error}",
                self.root.display()
            ))
        })?;
        if !root.is_dir() {
            return Err(HawDBError::Execution(format!(
                "LOAD CSV import root '{}' is not a directory",
                root.display()
            )));
        }
        let path = local_path(location)?;
        let path = if path.is_absolute() {
            path
        } else {
            root.join(path)
        };
        let canonical = std::fs::canonicalize(&path).map_err(|error| {
            if lexically_outside(&path, &root, &self.root) {
                outside_root(location)
            } else {
                HawDBError::Execution(format!("LOAD CSV cannot access '{location}': {error}"))
            }
        })?;
        if !canonical.starts_with(&root) {
            return Err(outside_root(location));
        }
        Ok(canonical)
    }

    /// Opens `source` for streaming after resolving and validating its location.
    ///
    /// The source must be a regular file before it is opened: opening a FIFO
    /// or device could block or never end. The opened handle is checked again
    /// for a file replaced between the two system calls.
    pub fn open(
        &self,
        source: &LoadCsvSource,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<CsvRows<File>> {
        let location = source_location(source, parameters)?;
        let path = self.resolve(location)?;
        let inspect = |metadata: std::io::Result<std::fs::Metadata>| {
            let metadata = metadata.map_err(|error| {
                HawDBError::Execution(format!("LOAD CSV cannot inspect '{location}': {error}"))
            })?;
            if !metadata.is_file() {
                return Err(HawDBError::Execution(format!(
                    "LOAD CSV source '{location}' is not a regular file"
                )));
            }
            if metadata.len() > self.limits.max_file_bytes {
                return Err(HawDBError::Execution(format!(
                    "LOAD CSV source '{location}' has {} bytes, exceeding max_file_bytes {}",
                    metadata.len(),
                    self.limits.max_file_bytes
                )));
            }
            Ok(())
        };
        inspect(std::fs::metadata(&path))?;
        let file = File::open(&path).map_err(|error| {
            HawDBError::Execution(format!("LOAD CSV cannot open '{location}': {error}"))
        })?;
        inspect(file.metadata())?;
        CsvRows::new(file, source, self.limits)
    }
}

fn outside_root(location: &str) -> HawDBError {
    HawDBError::Semantic(format!(
        "LOAD CSV source '{location}' is outside the configured import directory"
    ))
}

/// Whether `path`, with `.` and `..` applied lexically, lies outside both the
/// canonical and the configured import root. Used only for a target that
/// cannot be canonicalized, so a missing file outside the root reports the
/// policy violation rather than its absence.
fn lexically_outside(path: &Path, canonical_root: &Path, configured_root: &Path) -> bool {
    let normalize = |path: &Path| {
        let mut normalized = PathBuf::new();
        for component in path.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    normalized.pop();
                }
                component => normalized.push(component),
            }
        }
        normalized
    };
    let path = normalize(path);
    let configured = std::path::absolute(configured_root)
        .map(|root| normalize(&root))
        .ok();
    !path.starts_with(canonical_root)
        && configured.is_none_or(|configured| !path.starts_with(configured))
}

/// Opens a `LOAD CSV` source, refusing when no import policy is configured.
pub fn open_load_csv(
    policy: Option<&CsvImportPolicy>,
    source: &LoadCsvSource,
    parameters: &BTreeMap<String, Value>,
) -> Result<CsvRows<File>> {
    policy
        .ok_or_else(|| {
            HawDBError::Semantic(
                "LOAD CSV is disabled: set DatabaseConfig::csv_import_root to an import directory"
                    .to_string(),
            )
        })?
        .open(source, parameters)
}

fn source_location<'a>(
    source: &'a LoadCsvSource,
    parameters: &'a BTreeMap<String, Value>,
) -> Result<&'a str> {
    match &source.location {
        CsvLocation::Literal(location) => Ok(location),
        CsvLocation::Parameter(name) => match parameters.get(name) {
            Some(Value::String(location)) => Ok(location),
            Some(_) => Err(HawDBError::Semantic(format!(
                "LOAD CSV source parameter '${name}' must be a string"
            ))),
            None => Err(HawDBError::Semantic(format!("missing parameter '${name}'"))),
        },
    }
}

/// Converts a `file:` URL or plain path to a filesystem path. `file:` URL
/// paths are relative to the import directory, as in Neo4j. Every other URL
/// scheme, including `http` and `https`, is refused.
fn local_path(location: &str) -> Result<PathBuf> {
    if location.is_empty() || location.contains('\0') {
        return Err(HawDBError::Semantic(
            "LOAD CSV source must be a non-empty file path".to_string(),
        ));
    }
    let Some(scheme) = url_scheme(location) else {
        return Ok(PathBuf::from(location));
    };
    if !scheme.eq_ignore_ascii_case("file") {
        return Err(HawDBError::Semantic(format!(
            "LOAD CSV supports only local files; '{scheme}' URLs are not supported"
        )));
    }
    let rest = &location[scheme.len() + 1..];
    let path = if let Some(authority_path) = rest.strip_prefix("//") {
        let (host, path) = authority_path
            .find('/')
            .map_or((authority_path, ""), |index| authority_path.split_at(index));
        if !host.is_empty() && !host.eq_ignore_ascii_case("localhost") {
            return Err(HawDBError::Semantic(format!(
                "LOAD CSV file URL host '{host}' is not supported; use file:///path relative to the import directory"
            )));
        }
        path
    } else {
        rest
    };
    if path.contains(['?', '#']) {
        return Err(HawDBError::Semantic(
            "LOAD CSV file URLs must not contain a query or fragment".to_string(),
        ));
    }
    if !path.starts_with('/') {
        return Err(HawDBError::Semantic(
            "LOAD CSV file URLs must have the form file:///path".to_string(),
        ));
    }
    let decoded = percent_decode(path)?;
    // Like Neo4j, `file:///x.csv` names `x.csv` inside the import directory;
    // the URL path is never an absolute filesystem path.
    Ok(PathBuf::from(decoded.trim_start_matches('/')))
}

/// Returns the URL scheme when `location` starts with `scheme:`. A single
/// letter is a Windows drive, not a scheme.
fn url_scheme(location: &str) -> Option<&str> {
    let (scheme, _) = location.split_once(':')?;
    let mut bytes = scheme.bytes();
    let first = bytes.next()?;
    (first.is_ascii_alphabetic()
        && scheme.len() > 1
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.')))
    .then_some(scheme)
}

fn percent_decode(text: &str) -> Result<String> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = text
                .get(index + 1..index + 3)
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or_else(|| {
                    HawDBError::Semantic("LOAD CSV file URL has an invalid percent escape".into())
                })?;
            if hex == 0 {
                return Err(HawDBError::Semantic(
                    "LOAD CSV file URL must not contain NUL".to_string(),
                ));
            }
            decoded.push(hex);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map_err(|_| HawDBError::Semantic("LOAD CSV file URL is not valid UTF-8".to_string()))
}

/// Streams the rows `LOAD CSV` binds to its variable.
///
/// Without headers a row is a list of strings. With headers it is a map from
/// each header to the field in the same position. An empty unquoted field
/// and a field missing from a short record are `NULL`; a quoted empty field
/// is the empty string. Fields beyond the header count are ignored.
pub struct CsvRows<R> {
    reader: CsvRecordReader<R>,
    headers: Option<Vec<String>>,
    with_headers: bool,
}

impl<R: Read> CsvRows<R> {
    pub fn new(input: R, source: &LoadCsvSource, limits: CsvImportLimits) -> Result<Self> {
        Ok(Self {
            reader: CsvRecordReader::new(input, source.field_terminator, limits)?,
            headers: None,
            with_headers: source.with_headers,
        })
    }

    pub fn next_row(&mut self) -> Result<Option<Value>> {
        if self.with_headers && self.headers.is_none() {
            let Some(header) = self.reader.next_record()? else {
                return Ok(None);
            };
            let mut seen = std::collections::BTreeSet::new();
            let headers = header
                .into_iter()
                .map(|name| {
                    let name = name.unwrap_or_default();
                    if !seen.insert(name.clone()) {
                        return Err(HawDBError::Execution(format!(
                            "LOAD CSV header repeats column '{name}'"
                        )));
                    }
                    Ok(name)
                })
                .collect::<Result<Vec<_>>>()?;
            self.headers = Some(headers);
        }
        let Some(record) = self.reader.next_record()? else {
            return Ok(None);
        };
        Ok(Some(match &self.headers {
            None => Value::List(record.into_iter().map(field_value).collect()),
            Some(headers) => {
                let mut fields = record.into_iter();
                Value::Map(
                    headers
                        .iter()
                        .map(|name| (name.clone(), fields.next().map_or(Value::Null, field_value)))
                        .collect(),
                )
            }
        }))
    }

    /// Data records returned so far, excluding a header record.
    pub fn rows_read(&self) -> u64 {
        self.reader
            .records()
            .saturating_sub(u64::from(self.headers.is_some()))
    }
}

fn field_value(field: CsvField) -> Value {
    field.map_or(Value::Null, Value::String)
}
