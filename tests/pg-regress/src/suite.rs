// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Read;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const UPSTREAM: &str = "3d6a828938a5fa0444275d3d2f67b64ec3199eb7";
pub(crate) type Result<T> = std::result::Result<T, String>;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Suite {
    pub format_version: u32,
    pub postgres_version: i32,
    pub postgres_commit: String,
    pub sources: Vec<Source>,
    pub cases: Vec<Case>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Source {
    pub file: String,
    pub upstream: String,
    pub sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Case {
    pub id: String,
    pub group: String,
    pub steps: Vec<Step>,
    #[serde(default)]
    pub unsupported: Option<String>,
}

fn yes() -> bool {
    true
}
fn execute() -> String {
    "execute".into()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Step {
    pub sql: String,
    pub source: Range,
    #[serde(default)]
    pub execute_sql: Option<String>,
    #[serde(default)]
    pub adaptation: Option<String>,
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default = "execute")]
    pub action: String,
    #[serde(default)]
    pub isolation: Option<String>,
    #[serde(default)]
    pub ordered: bool,
    #[serde(default = "yes")]
    pub compare_column_names: bool,
    #[serde(default)]
    pub expected_error: Option<ExpectedError>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Range {
    pub file: String,
    pub first_line: usize,
    pub last_line: usize,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExpectedError {
    pub pg_sqlstate: String,
    pub hawdb_kind: String,
}

pub(crate) fn digest(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut hash = Sha256::new();
    let mut bytes = [0; 65536];
    loop {
        let len = file.read(&mut bytes).map_err(|e| e.to_string())?;
        if len == 0 {
            break;
        }
        hash.update(&bytes[..len]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

impl Suite {
    pub(crate) fn load(root: &Path) -> Result<Self> {
        let bytes = fs::read(root.join("data/cases.json")).map_err(|e| e.to_string())?;
        let suite: Self = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if suite.format_version != 1
            || suite.postgres_version != 180000
            || suite.postgres_commit != UPSTREAM
        {
            return Err("suite must pin PostgreSQL REL_18_0".into());
        }
        let mut sources = BTreeMap::new();
        for source in &suite.sources {
            if Path::new(&source.file)
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
            {
                return Err("source path must stay inside the test project".into());
            }
            let path = root.join(&source.file);
            if digest(&path)? != source.sha256 {
                return Err(format!("source checksum mismatch: {}", source.file));
            }
            if sources
                .insert(
                    source.file.as_str(),
                    fs::read_to_string(path).map_err(|e| e.to_string())?,
                )
                .is_some()
            {
                return Err("duplicate source path".into());
            }
        }
        let mut ids = BTreeSet::new();
        for case in &suite.cases {
            if !ids.insert(&case.id) {
                return Err("duplicate case ID".into());
            }
            if case.unsupported.is_some() {
                continue;
            }
            if case.steps.is_empty() {
                return Err("supported case has no statements".into());
            }
            for step in &case.steps {
                let text = sources
                    .get(step.source.file.as_str())
                    .ok_or("unknown source file")?;
                let lines = text.split_inclusive('\n').collect::<Vec<_>>();
                let start = step.source.first_line;
                let end = step.source.last_line;
                if start == 0 || end < start || end > lines.len() {
                    return Err("invalid source line range".into());
                }
                if !lines[start - 1..end].concat().contains(&step.sql) {
                    return Err(format!("SQL changed from upstream in {}", case.id));
                }
                let control = step.sql.trim().trim_end_matches(';').to_uppercase();
                match step.action.as_str() {
                    "begin" => {
                        let isolation = match step.isolation.as_deref() {
                            Some("serializable") => "SERIALIZABLE",
                            Some("repeatable_read") => "REPEATABLE READ",
                            _ => return Err("unknown transaction isolation".into()),
                        };
                        if control != format!("BEGIN ISOLATION LEVEL {isolation}")
                            && control != format!("BEGIN TRANSACTION ISOLATION LEVEL {isolation}")
                        {
                            return Err("source SQL and mapped isolation differ".into());
                        }
                    }
                    "commit" | "rollback" if control == step.action.to_uppercase() => {}
                    "execute" => {}
                    _ => return Err("source SQL and transaction action differ".into()),
                }
                if step.execute_sql.is_some()
                    && (step.adaptation.as_deref().is_none_or(str::is_empty)
                        || !(control.starts_with("CREATE TABLE")
                            || control.starts_with("CREATE TEMP TABLE")
                            || control.starts_with("INSERT INTO")))
                {
                    return Err("fixture SQL adaptation requires explicit setup provenance".into());
                }
                if !step.compare_column_names
                    && step.adaptation.as_deref().is_none_or(str::is_empty)
                {
                    return Err("column label adaptation needs an explanation".into());
                }
            }
        }
        Ok(suite)
    }

    pub(crate) fn select(&self, schedule: &Path, filter: &str) -> Result<Vec<&Case>> {
        let text = fs::read_to_string(schedule).map_err(|e| e.to_string())?;
        let mut groups = BTreeSet::new();
        let mut cases = Vec::new();
        for line in text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
        {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let ["test:", group] = fields.as_slice() else {
                return Err("schedule requires one serial test: GROUP per line".into());
            };
            if !groups.insert(*group) || !self.cases.iter().any(|case| case.group == *group) {
                return Err("duplicate or unknown schedule group".into());
            }
            cases.extend(
                self.cases
                    .iter()
                    .filter(|case| case.group == *group && case.id.contains(filter)),
            );
        }
        if cases.is_empty() {
            return Err("selection matched zero cases".into());
        }
        Ok(cases)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_provenance_and_nonempty_selection() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let suite = Suite::load(root).unwrap();
        assert_eq!(
            suite
                .select(&root.join("data/schedule"), "boolean/scalar")
                .unwrap()
                .len(),
            3
        );
        assert!(suite
            .select(&root.join("data/schedule"), "no-such-case")
            .is_err());
    }

    #[test]
    fn corrupted_source_is_refused() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let suite = Suite::load(root).unwrap();
        let temp =
            std::env::temp_dir().join(format!("hawdb-pg18-source-control-{}", std::process::id()));
        fs::create_dir(&temp).unwrap();
        fs::create_dir_all(temp.join("data")).unwrap();
        fs::copy(root.join("data/cases.json"), temp.join("data/cases.json")).unwrap();
        for source in &suite.sources {
            let path = temp.join(&source.file);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::copy(root.join(&source.file), path).unwrap();
        }
        fs::write(temp.join("data/sql/boolean.sql"), "SELECT 12345;").unwrap();
        assert!(Suite::load(&temp)
            .err()
            .unwrap()
            .contains("checksum mismatch"));
        fs::remove_dir_all(temp).unwrap();
    }
}
