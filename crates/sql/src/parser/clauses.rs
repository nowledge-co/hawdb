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

//! Recognized unsupported clause diagnostics have a stable, payload-free priority.
use hawdb_core::{HawDBError, Result};

pub(super) const CREATE_CLAUSES: &[&str] = &[
    "OR REPLACE",
    "TEMPORARY",
    "EXTERNAL",
    "DYNAMIC",
    "GLOBAL",
    "TRANSIENT",
    "VOLATILE",
    "ICEBERG",
    "Hive distribution",
    "Hive format",
    "STORED AS",
    "LOCATION",
    "AS query",
    "WITHOUT ROWID",
    "LIKE",
    "CLONE",
    "VERSION",
    "COMMENT",
    "ON COMMIT",
    "ON CLUSTER",
    "PRIMARY KEY expression",
    "ORDER BY",
    "PARTITION BY",
    "CLUSTER BY",
    "CLUSTERED BY",
    "INHERITS",
    "PARTITION OF",
    "FOR VALUES",
    "STRICT",
    "COPY GRANTS",
    "ENABLE_SCHEMA_EVOLUTION",
    "CHANGE_TRACKING",
    "DATA_RETENTION_TIME_IN_DAYS",
    "MAX_DATA_EXTENSION_TIME_IN_DAYS",
    "DEFAULT_DDL_COLLATION",
    "WITH AGGREGATION POLICY",
    "WITH ROW ACCESS POLICY",
    "WITH TAG",
    "EXTERNAL_VOLUME",
    "BASE_LOCATION",
    "CATALOG",
    "CATALOG_SYNC",
    "STORAGE_SERIALIZATION_POLICY",
    "TARGET_LAG",
    "WAREHOUSE",
    "REFRESH_MODE",
    "INITIALIZE",
    "REQUIRE USER",
    "UNLOGGED",
    "SNAPSHOT",
    "WITH STORAGE LIFECYCLE POLICY",
    "WITH CONNECTION",
    "DISTSTYLE",
    "DISTKEY",
    "SORTKEY",
    "BACKUP",
    "MULTISET",
    "FALLBACK",
    "WITH DATA",
];

pub(super) const INSERT_CLAUSES: &[&str] = &[
    "optimizer hint",
    "OR conflict action",
    "IGNORE",
    "missing INTO",
    "table alias",
    "OVERWRITE",
    "SET assignments",
    "PARTITION",
    "columns after PARTITION",
    "TABLE keyword",
    "REPLACE INTO",
    "priority",
    "row alias",
    "SETTINGS",
    "FORMAT",
    "OUTPUT",
    "multi-table type",
    "multi-table INTO",
    "multi-table WHEN",
    "multi-table ELSE",
];

pub(super) fn reject(statement: &'static str, present: &[&'static str]) -> Result<()> {
    let priority = match statement {
        "CREATE TABLE" => CREATE_CLAUSES,
        "INSERT" => INSERT_CLAUSES,
        _ => unreachable!("qualified statement diagnostics"),
    };
    let rank = |clause: &str| {
        priority
            .iter()
            .position(|name| *name == if clause == "LOCAL" { "GLOBAL" } else { clause })
            .unwrap_or(priority.len())
    };
    if let Some(clause) = present.iter().min_by_key(|clause| rank(clause)) {
        return Err(HawDBError::Semantic(format!(
            "unsupported PostgreSQL {statement} clause: {clause}"
        )));
    }
    Ok(())
}
