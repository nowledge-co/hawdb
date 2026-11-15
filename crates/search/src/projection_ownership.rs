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

//! Durable record of a projection's externally owned kinds (#1814).
//!
//! A host that supplies the documents of some kinds itself (its own text,
//! metadata and embeddings) must keep graph changefeed catch-up from
//! re-deriving and replacing them. The set is passed at open and recorded in
//! one sidecar file in the projection directory, beside the existing
//! `.reindex_needed` markers. It is deliberately not a selector manifest
//! field: readers from 0.31 through 0.33 refuse unknown manifest fields,
//! while they ignore this file (it parses as no artifact, does not start with
//! the `search_` prefix that headless-root checks refuse, and matches no
//! scratch pattern), so the projection stays readable after a downgrade.
//!
//! Recovery rule: when no record exists, a maintenance open records the
//! requested set (this also restores a record that an older binary's
//! operator removed). When a record exists and differs from the requested
//! set, the open is refused with
//! [`HawDBError::SearchProjectionOwnershipMismatch`]; nothing diverges
//! silently between, for example, a preparation flow and a runtime.

use crate::SearchProjectionKind;
use hawdb_core::{HawDBError, Result};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::Path;

/// File name of the record inside the projection directory.
pub const MARKER: &str = ".externally_owned_search_kinds";
const HEADER: &str = "hawdb externally owned search kinds v1";
/// Header, six kind names and their separators fit comfortably.
const MAX_MARKER_BYTES: u64 = 4096;

/// Whether `name` is the record or one of its write temporaries. Generation
/// cleanup never treats these as candidates.
pub(crate) fn is_record_name(name: &str) -> bool {
    name.strip_prefix(MARKER)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(".tmp."))
}

/// Reads the recorded set, or `None` when the projection has no record.
pub fn read_recorded_kinds(projection: &Path) -> Result<Option<BTreeSet<SearchProjectionKind>>> {
    let path = projection.join(MARKER);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut text = String::new();
    file.take(MAX_MARKER_BYTES + 1).read_to_string(&mut text)?;
    if text.len() as u64 > MAX_MARKER_BYTES {
        return Err(malformed(projection, "exceeds its size bound"));
    }
    let mut lines = text.lines();
    if lines.next() != Some(HEADER) {
        return Err(malformed(projection, "has an unknown header"));
    }
    let mut kinds = BTreeSet::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let kind = SearchProjectionKind::from_name(line)
            .ok_or_else(|| malformed(projection, "names an unknown kind"))?;
        if !kinds.insert(kind) {
            return Err(malformed(projection, "repeats a kind"));
        }
    }
    Ok(Some(kinds))
}

/// Compares the requested set with the record and, when `may_record` and no
/// record exists, durably records a non-empty requested set.
///
/// An empty request against an absent record writes nothing, so projections
/// that never opt in keep their exact directory contents.
pub fn reconcile(
    projection: &Path,
    requested: &BTreeSet<SearchProjectionKind>,
    may_record: bool,
) -> Result<()> {
    match read_recorded_kinds(projection)? {
        Some(recorded) if &recorded == requested => Ok(()),
        Some(recorded) => Err(HawDBError::SearchProjectionOwnershipMismatch {
            projection: projection.to_path_buf(),
            recorded: names(&recorded),
            requested: names(requested),
        }),
        None if requested.is_empty() || !may_record => Ok(()),
        None => write_record(projection, requested),
    }
}

fn write_record(projection: &Path, kinds: &BTreeSet<SearchProjectionKind>) -> Result<()> {
    let mut text = String::from(HEADER);
    for kind in kinds {
        text.push('\n');
        text.push_str(kind.as_str());
    }
    text.push('\n');
    let target = projection.join(MARKER);
    let temporary = projection.join(format!("{MARKER}.tmp.{}", std::process::id()));
    let written = (|| -> std::io::Result<()> {
        let mut file = File::create(&temporary)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        hawdb_storage::durability::durable_replace_file(&temporary, &target)
    })();
    if written.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    Ok(written?)
}

fn names(kinds: &BTreeSet<SearchProjectionKind>) -> Vec<String> {
    kinds.iter().map(|kind| kind.as_str().to_string()).collect()
}

fn malformed(projection: &Path, problem: &str) -> HawDBError {
    HawDBError::StorageIntegrity(format!(
        "externally owned search kinds record {} {problem}",
        projection.join(MARKER).display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory(name: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hawdb-projection-ownership-{name}-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn records_once_and_refuses_a_different_set() {
        let root = directory("record");
        let owned = BTreeSet::from([SearchProjectionKind::Memory, SearchProjectionKind::Entity]);
        reconcile(&root, &BTreeSet::new(), true).unwrap();
        assert!(
            !root.join(MARKER).exists(),
            "an empty request writes nothing"
        );
        reconcile(&root, &owned, false).unwrap();
        assert!(!root.join(MARKER).exists(), "a reader open writes nothing");
        reconcile(&root, &owned, true).unwrap();
        assert_eq!(
            fs::read_to_string(root.join(MARKER)).unwrap(),
            format!("{HEADER}\nmemory\nentity\n")
        );
        reconcile(&root, &owned, true).unwrap();
        for requested in [
            BTreeSet::new(),
            BTreeSet::from([SearchProjectionKind::Memory]),
        ] {
            let error = reconcile(&root, &requested, true).unwrap_err();
            assert_eq!(
                error,
                HawDBError::SearchProjectionOwnershipMismatch {
                    projection: root.clone(),
                    recorded: vec!["memory".into(), "entity".into()],
                    requested: names(&requested),
                }
            );
        }
        assert_eq!(read_recorded_kinds(&root).unwrap(), Some(owned.clone()));
        // A record removed out of band is restored from the requested set.
        fs::remove_file(root.join(MARKER)).unwrap();
        reconcile(&root, &owned, true).unwrap();
        assert_eq!(read_recorded_kinds(&root).unwrap(), Some(owned));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn refuses_a_malformed_record() {
        let root = directory("malformed");
        for text in [
            "memory\n",
            &format!("{HEADER}\nmemories\n"),
            &format!("{HEADER}\nmemory\nmemory\n"),
        ] {
            fs::write(root.join(MARKER), text).unwrap();
            assert!(matches!(
                reconcile(&root, &BTreeSet::new(), true),
                Err(HawDBError::StorageIntegrity(_))
            ));
        }
        fs::remove_dir_all(root).unwrap();
    }
}
