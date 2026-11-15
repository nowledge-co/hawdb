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

//! Durable records of a projection's externally owned kinds (#1814).
//!
//! A host that supplies the documents of some kinds itself (its own text,
//! metadata and embeddings) must keep graph changefeed catch-up from
//! re-deriving and replacing them. Two sidecar files in the projection
//! directory, beside the existing `.reindex_needed` markers, carry this:
//!
//! - [`MARKER`] records the set passed at open;
//! - [`ROWS_MARKER`] records whether the host must (re)supply its rows, as a
//!   monotonically increasing epoch.
//!
//! Neither is a selector manifest field: readers from 0.31 through 0.33
//! refuse unknown manifest fields, while they ignore these files (they parse
//! as no artifact, do not start with the `search_` prefix that headless-root
//! checks refuse, and match no scratch pattern), so the projection stays
//! readable after a downgrade.
//!
//! Recovery rule: when no set record exists, a maintenance open records the
//! requested set and raises the rows-required epoch, because nothing proves
//! the host's rows are present (a new projection, or a record removed out of
//! band). When a record exists and differs from the requested set, the open
//! is refused with [`HawDBError::SearchProjectionOwnershipMismatch`]; nothing
//! diverges silently between, for example, a preparation flow and a runtime.
//! An unparsable record is the typed, non-poisoning
//! [`HawDBError::SearchProjectionOwnershipMarkerCorrupt`]; [`reset`] rewrites
//! both records and raises the epoch.

use crate::SearchProjectionKind;
use hawdb_core::{HawDBError, Result};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::Path;

/// File name of the set record inside the projection directory.
pub const MARKER: &str = ".externally_owned_search_kinds";
/// File name of the rows-required state inside the projection directory.
pub const ROWS_MARKER: &str = ".externally_owned_rows_required";
const HEADER: &str = "hawdb externally owned search kinds v1";
const ROWS_HEADER: &str = "hawdb externally owned rows v1";
/// Headers, six kind names, an epoch and their separators fit comfortably.
const MAX_MARKER_BYTES: u64 = 4096;

/// Whether `name` is a record or one of its write temporaries. Generation
/// cleanup never treats these as candidates.
pub(crate) fn is_record_name(name: &str) -> bool {
    [MARKER, ROWS_MARKER].iter().any(|marker| {
        name.strip_prefix(marker)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(".tmp."))
    })
}

/// Whether the host must (re)supply the documents of its externally owned
/// kinds.
///
/// `epoch` only grows. It is raised whenever HawDB cannot prove the host's
/// documents are present: when ownership is first recorded for a projection
/// directory, or when a rebuild could not carry the previous documents over.
/// The host re-pushes its rows, then acknowledges the epoch it observed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExternallyOwnedRowsState {
    pub epoch: u64,
    pub required: bool,
}

impl ExternallyOwnedRowsState {
    /// The epoch the host must acknowledge, while rows are required.
    pub fn required_epoch(self) -> Option<u64> {
        self.required.then_some(self.epoch)
    }

    /// The next raised state.
    pub fn raised(self) -> Self {
        Self {
            epoch: self.epoch.saturating_add(1),
            required: true,
        }
    }
}

/// Reads the recorded set, or `None` when the projection has no record.
pub fn read_recorded_kinds(projection: &Path) -> Result<Option<BTreeSet<SearchProjectionKind>>> {
    let Some(text) = read_bounded(projection, MARKER)? else {
        return Ok(None);
    };
    let mut lines = text.lines();
    if lines.next() != Some(HEADER) {
        return Err(malformed(projection, MARKER, "has an unknown header"));
    }
    let mut kinds = BTreeSet::new();
    for line in lines.filter(|line| !line.is_empty()) {
        let kind = SearchProjectionKind::from_name(line)
            .ok_or_else(|| malformed(projection, MARKER, "names an unknown kind"))?;
        if !kinds.insert(kind) {
            return Err(malformed(projection, MARKER, "repeats a kind"));
        }
    }
    Ok(Some(kinds))
}

/// Reads the rows-required state; an absent file is epoch 0, not required.
pub fn read_rows_state(projection: &Path) -> Result<ExternallyOwnedRowsState> {
    let Some(text) = read_bounded(projection, ROWS_MARKER)? else {
        return Ok(ExternallyOwnedRowsState::default());
    };
    let bad = |problem| malformed(projection, ROWS_MARKER, problem);
    let mut lines = text.lines();
    if lines.next() != Some(ROWS_HEADER) {
        return Err(bad("has an unknown header"));
    }
    let epoch = lines
        .next()
        .and_then(|line| line.strip_prefix("epoch "))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| bad("has no epoch"))?;
    let required = match lines.next() {
        Some("required") => true,
        Some("restored") => false,
        _ => return Err(bad("has no state")),
    };
    if lines.any(|line| !line.is_empty()) {
        return Err(bad("has trailing lines"));
    }
    Ok(ExternallyOwnedRowsState { epoch, required })
}

/// Durably writes `state`.
pub fn write_rows_state(projection: &Path, state: ExternallyOwnedRowsState) -> Result<()> {
    let text = format!(
        "{ROWS_HEADER}\nepoch {}\n{}\n",
        state.epoch,
        if state.required {
            "required"
        } else {
            "restored"
        }
    );
    write_durably(projection, ROWS_MARKER, &text)
}

/// Raises the rows-required epoch and returns it.
pub fn raise_rows_required(projection: &Path) -> Result<u64> {
    let raised = read_rows_state(projection)?.raised();
    write_rows_state(projection, raised)?;
    Ok(raised.epoch)
}

/// The raised state that follows `previous`. An unreadable previous state
/// jumps to the current time in microseconds, which exceeds every epoch a
/// counter could have reached, so epochs never repeat.
pub fn raised_after(previous: Result<ExternallyOwnedRowsState>) -> ExternallyOwnedRowsState {
    match previous {
        Ok(state) => state.raised(),
        Err(_) => ExternallyOwnedRowsState {
            epoch: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX))
                .unwrap_or(u64::MAX >> 1)
                .max(1),
            required: true,
        },
    }
}

/// Rewrites both records from `kinds` regardless of their current content,
/// raising the rows-required epoch. This is the recovery for an unreadable
/// record ([`HawDBError::SearchProjectionOwnershipMarkerCorrupt`]).
pub fn reset(projection: &Path, kinds: &BTreeSet<SearchProjectionKind>) -> Result<u64> {
    let raised = raised_after(read_rows_state(projection));
    write_rows_state(projection, raised)?;
    write_record(projection, kinds)?;
    Ok(raised.epoch)
}

/// Clears the requirement only if `epoch` is still the required epoch.
///
/// Returns `false`, changing nothing, when no rows are required or `epoch`
/// is stale because a later rebuild raised a newer one.
pub fn acknowledge_rows_restored(projection: &Path, epoch: u64) -> Result<bool> {
    let state = read_rows_state(projection)?;
    if state.required_epoch() != Some(epoch) {
        return Ok(false);
    }
    write_rows_state(
        projection,
        ExternallyOwnedRowsState {
            epoch,
            required: false,
        },
    )?;
    Ok(true)
}

/// Compares the requested set with the record and, when `may_record` and no
/// record exists, durably records a non-empty requested set after raising
/// the rows-required epoch.
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
        None => {
            // Raise first: a crash before the set is recorded raises again.
            raise_rows_required(projection)?;
            write_record(projection, requested)
        }
    }
}

/// Durably records `kinds` without consulting an existing record.
pub fn write_record(projection: &Path, kinds: &BTreeSet<SearchProjectionKind>) -> Result<()> {
    let mut text = String::from(HEADER);
    for kind in kinds {
        text.push('\n');
        text.push_str(kind.as_str());
    }
    text.push('\n');
    write_durably(projection, MARKER, &text)
}

fn read_bounded(projection: &Path, name: &str) -> Result<Option<String>> {
    let file = match File::open(projection.join(name)) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut text = String::new();
    file.take(MAX_MARKER_BYTES + 1).read_to_string(&mut text)?;
    if text.len() as u64 > MAX_MARKER_BYTES {
        return Err(malformed(projection, name, "exceeds its size bound"));
    }
    Ok(Some(text))
}

fn write_durably(projection: &Path, name: &str, text: &str) -> Result<()> {
    let target = projection.join(name);
    let temporary = projection.join(format!("{name}.tmp.{}", std::process::id()));
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

fn malformed(projection: &Path, name: &str, problem: &str) -> HawDBError {
    HawDBError::SearchProjectionOwnershipMarkerCorrupt {
        projection: projection.to_path_buf(),
        marker: name.to_string(),
        message: problem.to_string(),
    }
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
        // Nothing proves the host's rows are present yet.
        assert_eq!(read_rows_state(&root).unwrap().required_epoch(), Some(1));
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
        // A record removed out of band is restored and raises a newer epoch.
        fs::remove_file(root.join(MARKER)).unwrap();
        reconcile(&root, &owned, true).unwrap();
        assert_eq!(read_recorded_kinds(&root).unwrap(), Some(owned));
        assert_eq!(read_rows_state(&root).unwrap().required_epoch(), Some(2));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acknowledgement_clears_only_the_current_epoch() {
        let root = directory("acknowledge");
        assert_eq!(
            read_rows_state(&root).unwrap(),
            ExternallyOwnedRowsState::default()
        );
        assert!(!acknowledge_rows_restored(&root, 0).unwrap());
        assert_eq!(raise_rows_required(&root).unwrap(), 1);
        assert_eq!(raise_rows_required(&root).unwrap(), 2);
        assert!(
            !acknowledge_rows_restored(&root, 1).unwrap(),
            "a stale epoch changes nothing"
        );
        assert_eq!(read_rows_state(&root).unwrap().required_epoch(), Some(2));
        assert!(acknowledge_rows_restored(&root, 2).unwrap());
        assert_eq!(
            read_rows_state(&root).unwrap(),
            ExternallyOwnedRowsState {
                epoch: 2,
                required: false
            }
        );
        assert!(!acknowledge_rows_restored(&root, 2).unwrap());
        assert_eq!(
            raise_rows_required(&root).unwrap(),
            3,
            "epochs never repeat"
        );
        for text in [
            "epoch 1\nrequired\n",
            &format!("{ROWS_HEADER}\nepoch x\nrequired\n"),
            &format!("{ROWS_HEADER}\nepoch 1\nmaybe\n"),
        ] {
            fs::write(root.join(ROWS_MARKER), text).unwrap();
            assert!(matches!(
                read_rows_state(&root),
                Err(HawDBError::SearchProjectionOwnershipMarkerCorrupt { .. })
            ));
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reset_rewrites_unreadable_records_and_raises_past_every_epoch() {
        let root = directory("reset");
        let owned = BTreeSet::from([SearchProjectionKind::Memory]);
        fs::write(root.join(MARKER), "garbage").unwrap();
        fs::write(root.join(ROWS_MARKER), "garbage").unwrap();
        assert!(matches!(
            reconcile(&root, &owned, true),
            Err(HawDBError::SearchProjectionOwnershipMarkerCorrupt { ref marker, .. }) if marker == MARKER
        ));
        let epoch = reset(&root, &owned).unwrap();
        assert!(
            epoch > 1_000_000_000,
            "time-based epoch after an unreadable state"
        );
        assert_eq!(read_recorded_kinds(&root).unwrap(), Some(owned.clone()));
        assert_eq!(
            read_rows_state(&root).unwrap().required_epoch(),
            Some(epoch)
        );
        assert_eq!(reset(&root, &owned).unwrap(), epoch + 1);
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
                Err(HawDBError::SearchProjectionOwnershipMarkerCorrupt { .. })
            ));
        }
        fs::remove_dir_all(root).unwrap();
    }
}
