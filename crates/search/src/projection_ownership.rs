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
//! - [`ROWS_MARKER`] records whether the host must (re)supply its rows, fenced
//!   by a projection incarnation and an epoch within that incarnation.
//!
//! Neither is a selector manifest field: readers from 0.31 through 0.33
//! refuse unknown manifest fields, while they ignore these files (they parse
//! as no artifact, do not start with the `search_` prefix that headless-root
//! checks refuse, and match no scratch pattern), so the projection stays
//! readable after a downgrade.
//!
//! Recovery rule: when no set record exists, a maintenance open records the
//! requested set and creates the rows-required fence, because nothing proves
//! the host's rows are present (a new projection, or a record removed out of
//! band). When a record exists and differs from the requested set, the open
//! is refused with [`HawDBError::SearchProjectionOwnershipMismatch`]; nothing
//! diverges silently between, for example, a preparation flow and a runtime.
//! An unparsable record is the typed, non-poisoning
//! [`HawDBError::SearchProjectionOwnershipMarkerCorrupt`]; [`reset`] rewrites
//! both records with a fresh incarnation.

use crate::SearchProjectionKind;
use hawdb_core::{HawDBError, Result, Uuid};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::Path;

/// File name of the set record inside the projection directory.
pub const MARKER: &str = ".externally_owned_search_kinds";
/// File name of the rows-required state inside the projection directory.
pub const ROWS_MARKER: &str = ".externally_owned_rows_required";
const HEADER: &str = "hawdb externally owned search kinds v1";
const ROWS_HEADER: &str = "hawdb externally owned rows v2";
/// Headers, six kind names, an incarnation, an epoch and their separators fit comfortably.
const MAX_MARKER_BYTES: u64 = 4096;

/// Whether `name` is a record or one of its write temporaries. Generation
/// cleanup never treats these as candidates.
pub(crate) fn is_record_name(name: &str) -> bool {
    [MARKER, ROWS_MARKER].iter().any(|marker| {
        name.strip_prefix(marker)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(".tmp."))
    })
}

/// The fencing token the host must acknowledge after supplying its externally
/// owned rows.
///
/// `incarnation` identifies one durable ownership-state lifetime. `epoch` only
/// grows within that incarnation. A rebuild into a different projection or an
/// explicit recovery reset receives a fresh incarnation, so an acknowledgement
/// retained from an older projection can never clear the new requirement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternallyOwnedRowsToken {
    pub incarnation: Uuid,
    pub epoch: u64,
}

/// Whether the host must (re)supply the documents of its externally owned
/// kinds in one projection incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExternallyOwnedRowsState {
    pub token: ExternallyOwnedRowsToken,
    pub required: bool,
}

impl ExternallyOwnedRowsState {
    /// The token the host must acknowledge, while rows are required.
    pub fn required_token(self) -> Option<ExternallyOwnedRowsToken> {
        self.required.then_some(self.token)
    }

    /// The next raised state in this incarnation.
    fn raised(self, projection: &Path) -> Result<Self> {
        let epoch = self.token.epoch.checked_add(1).ok_or_else(|| {
            malformed(
                projection,
                ROWS_MARKER,
                "has exhausted its acknowledgement epoch",
            )
        })?;
        Ok(Self {
            token: ExternallyOwnedRowsToken {
                incarnation: self.token.incarnation,
                epoch,
            },
            required: true,
        })
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

/// Reads the rows-required state, or `None` when the file is absent.
pub fn read_rows_state(projection: &Path) -> Result<Option<ExternallyOwnedRowsState>> {
    let Some(text) = read_bounded(projection, ROWS_MARKER)? else {
        return Ok(None);
    };
    let bad = |problem| malformed(projection, ROWS_MARKER, problem);
    let mut lines = text.lines();
    if lines.next() != Some(ROWS_HEADER) {
        return Err(bad("has an unknown header"));
    }
    let incarnation = lines
        .next()
        .and_then(|line| line.strip_prefix("incarnation "))
        .and_then(|value| value.parse::<Uuid>().ok())
        .filter(|value| !value.is_nil())
        .ok_or_else(|| bad("has no valid incarnation"))?;
    let epoch = lines
        .next()
        .and_then(|line| line.strip_prefix("epoch "))
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| bad("has no epoch"))?;
    let required = match lines.next() {
        Some("required") => true,
        Some("restored") => false,
        _ => return Err(bad("has no state")),
    };
    if lines.any(|line| !line.is_empty()) {
        return Err(bad("has trailing lines"));
    }
    Ok(Some(ExternallyOwnedRowsState {
        token: ExternallyOwnedRowsToken { incarnation, epoch },
        required,
    }))
}

/// Durably writes `state`.
pub fn write_rows_state(projection: &Path, state: ExternallyOwnedRowsState) -> Result<()> {
    let text = format!(
        "{ROWS_HEADER}\nincarnation {}\nepoch {}\n{}\n",
        state.token.incarnation,
        state.token.epoch,
        if state.required {
            "required"
        } else {
            "restored"
        }
    );
    write_durably(projection, ROWS_MARKER, &text)
}

/// Raises the rows-required fence and returns its new token.
///
/// An absent state starts a fresh projection incarnation. Callers opening an
/// already-owned projection must use [`require_rows_state`] first so a removed
/// sidecar cannot look like an unowned projection.
pub fn raise_rows_required(projection: &Path) -> Result<ExternallyOwnedRowsToken> {
    let raised = match read_rows_state(projection)? {
        Some(state) => state.raised(projection)?,
        None => new_rows_state()?,
    };
    write_rows_state(projection, raised)?;
    Ok(raised.token)
}

/// Requires the state of a projection that already records externally owned
/// kinds. Missing state is corruption, never an implicit restored state.
pub fn require_rows_state(projection: &Path) -> Result<ExternallyOwnedRowsState> {
    read_rows_state(projection)?.ok_or_else(|| malformed(projection, ROWS_MARKER, "is missing"))
}

/// Rewrites both records from `kinds` regardless of their current content,
/// creating a fresh projection incarnation. This is the explicit recovery for
/// an unreadable or missing record
/// ([`HawDBError::SearchProjectionOwnershipMarkerCorrupt`]).
pub fn reset(
    projection: &Path,
    kinds: &BTreeSet<SearchProjectionKind>,
) -> Result<ExternallyOwnedRowsToken> {
    let state = new_rows_state()?;
    write_rows_state(projection, state)?;
    write_record(projection, kinds)?;
    Ok(state.token)
}

/// Clears the requirement only if `token` is still the required fence.
///
/// Returns `false`, changing nothing, when no rows are required or `token` is
/// stale because a later raise or a different projection incarnation replaced
/// it.
pub fn acknowledge_rows_restored(
    projection: &Path,
    token: ExternallyOwnedRowsToken,
) -> Result<bool> {
    let state = require_rows_state(projection)?;
    if state.required_token() != Some(token) {
        return Ok(false);
    }
    write_rows_state(
        projection,
        ExternallyOwnedRowsState {
            token,
            required: false,
        },
    )?;
    Ok(true)
}

/// Compares the requested set with the record and, when `may_record` and no
/// record exists, durably records a non-empty requested set after creating or
/// raising the rows-required fence.
///
/// An empty request against an absent record writes nothing, so projections
/// that never opt in keep their exact directory contents.
pub fn reconcile(
    projection: &Path,
    requested: &BTreeSet<SearchProjectionKind>,
    may_record: bool,
) -> Result<()> {
    match read_recorded_kinds(projection)? {
        Some(recorded) if &recorded == requested => {
            if requested.is_empty() {
                Ok(())
            } else {
                require_rows_state(projection).map(|_| ())
            }
        }
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

/// Observes the ownership of a projection opened with a non-empty
/// `requested` set, after [`reconcile`] accepted that open.
///
/// Returns the rows state when the record equals `requested`, and
/// [`HawDBError::SearchProjectionOwnershipMismatch`] when it differs. Without
/// a record it returns `Ok(None)` for an open that could not record
/// (`recorded_at_open` false: the projection is not opted in yet, and a later
/// writable open may record it), and the typed corruption error for an open
/// that recorded or verified the set, because the record was then removed
/// out of band and the rows must never look restored.
pub fn observe(
    projection: &Path,
    requested: &BTreeSet<SearchProjectionKind>,
    recorded_at_open: bool,
) -> Result<Option<ExternallyOwnedRowsState>> {
    match read_recorded_kinds(projection)? {
        Some(recorded) if &recorded == requested => require_rows_state(projection).map(Some),
        Some(recorded) => Err(HawDBError::SearchProjectionOwnershipMismatch {
            projection: projection.to_path_buf(),
            recorded: names(&recorded),
            requested: names(requested),
        }),
        None if recorded_at_open => Err(malformed(projection, MARKER, "is missing")),
        None => Ok(None),
    }
}

fn new_rows_state() -> Result<ExternallyOwnedRowsState> {
    Ok(ExternallyOwnedRowsState {
        token: ExternallyOwnedRowsToken {
            incarnation: hawdb_core::generate_uuidv7()?,
            epoch: 1,
        },
        required: true,
    })
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

    fn required_token(root: &Path) -> ExternallyOwnedRowsToken {
        require_rows_state(root).unwrap().required_token().unwrap()
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
        let first = required_token(&root);
        assert_eq!(first.epoch, 1);
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
        let second = required_token(&root);
        assert_eq!(second.incarnation, first.incarnation);
        assert_eq!(second.epoch, 2);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acknowledgement_clears_only_the_current_token() {
        let root = directory("acknowledge");
        assert_eq!(read_rows_state(&root).unwrap(), None);
        let first = raise_rows_required(&root).unwrap();
        let second = raise_rows_required(&root).unwrap();
        assert_eq!(second.incarnation, first.incarnation);
        assert_eq!(second.epoch, 2);
        assert!(
            !acknowledge_rows_restored(&root, first).unwrap(),
            "a stale token changes nothing"
        );
        assert_eq!(required_token(&root), second);
        assert!(acknowledge_rows_restored(&root, second).unwrap());
        assert_eq!(
            read_rows_state(&root).unwrap(),
            Some(ExternallyOwnedRowsState {
                token: second,
                required: false
            })
        );
        assert!(!acknowledge_rows_restored(&root, second).unwrap());
        let third = raise_rows_required(&root).unwrap();
        assert_eq!(third.incarnation, second.incarnation);
        assert_eq!(third.epoch, 3, "epochs never repeat in an incarnation");
        for text in [
            "epoch 1\nrequired\n",
            &format!("{ROWS_HEADER}\nincarnation x\nepoch 1\nrequired\n"),
            &format!(
                "{ROWS_HEADER}\nincarnation {}\nepoch x\nrequired\n",
                first.incarnation
            ),
            &format!(
                "{ROWS_HEADER}\nincarnation {}\nepoch 1\nmaybe\n",
                first.incarnation
            ),
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
    fn missing_rows_state_fails_closed_and_reset_rejects_the_old_token() {
        let root = directory("missing-rows");
        let owned = BTreeSet::from([SearchProjectionKind::Memory]);
        reconcile(&root, &owned, true).unwrap();
        let old = required_token(&root);
        fs::remove_file(root.join(ROWS_MARKER)).unwrap();
        assert!(matches!(
            reconcile(&root, &owned, true),
            Err(HawDBError::SearchProjectionOwnershipMarkerCorrupt { ref marker, .. }) if marker == ROWS_MARKER
        ));

        let replacement = reset(&root, &owned).unwrap();
        assert_ne!(replacement.incarnation, old.incarnation);
        assert_eq!(replacement.epoch, 1);
        assert!(!acknowledge_rows_restored(&root, old).unwrap());
        assert_eq!(required_token(&root), replacement);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn reset_rewrites_unreadable_records_with_a_fresh_incarnation() {
        let root = directory("reset");
        let owned = BTreeSet::from([SearchProjectionKind::Memory]);
        let old = reset(&root, &owned).unwrap();
        assert!(acknowledge_rows_restored(&root, old).unwrap());
        fs::write(root.join(ROWS_MARKER), "garbage").unwrap();
        assert!(matches!(
            reconcile(&root, &owned, true),
            Err(HawDBError::SearchProjectionOwnershipMarkerCorrupt { ref marker, .. }) if marker == ROWS_MARKER
        ));
        let replacement = reset(&root, &owned).unwrap();
        assert_ne!(replacement.incarnation, old.incarnation);
        assert_eq!(replacement.epoch, 1);
        assert_eq!(read_recorded_kinds(&root).unwrap(), Some(owned.clone()));
        assert_eq!(required_token(&root), replacement);
        assert!(!acknowledge_rows_restored(&root, old).unwrap());
        let next = reset(&root, &owned).unwrap();
        assert_ne!(next.incarnation, replacement.incarnation);
        assert_eq!(next.epoch, 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn acknowledgement_identity_does_not_depend_on_incarnation_order() {
        let root = directory("non-monotonic-incarnation");
        let old = ExternallyOwnedRowsToken {
            incarnation: Uuid::from_u128(2),
            epoch: 1,
        };
        let replacement = ExternallyOwnedRowsToken {
            // A lower UUID models an identity created after clock rollback.
            incarnation: Uuid::from_u128(1),
            epoch: 1,
        };
        write_rows_state(
            &root,
            ExternallyOwnedRowsState {
                token: replacement,
                required: true,
            },
        )
        .unwrap();

        assert!(!acknowledge_rows_restored(&root, old).unwrap());
        assert_eq!(required_token(&root), replacement);
        assert!(acknowledge_rows_restored(&root, replacement).unwrap());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn observe_follows_the_record_written_after_open() {
        let root = directory("observe");
        let owned = BTreeSet::from([SearchProjectionKind::Memory]);
        // A reader open of an unrecorded projection is not opted in yet.
        reconcile(&root, &owned, false).unwrap();
        assert_eq!(observe(&root, &owned, false).unwrap(), None);
        assert!(matches!(
            observe(&root, &owned, true),
            Err(HawDBError::SearchProjectionOwnershipMarkerCorrupt { ref marker, .. }) if marker == MARKER
        ));
        // A writer records the set later; the reader sees its fence.
        reconcile(&root, &owned, true).unwrap();
        let state = observe(&root, &owned, false).unwrap().unwrap();
        assert_eq!(state.required_token(), Some(required_token(&root)));
        let other = BTreeSet::from([SearchProjectionKind::Entity]);
        assert!(matches!(
            observe(&root, &other, false),
            Err(HawDBError::SearchProjectionOwnershipMismatch { .. })
        ));
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
