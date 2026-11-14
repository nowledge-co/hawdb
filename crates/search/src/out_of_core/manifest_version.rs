// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Reader-version gate for the search projection selector manifest (#1763).
//!
//! The selector (`search_projection_manifest.hawdb`) is decoded strictly
//! (`deny_unknown_fields`), so a field written by a newer HawDB used to fail
//! an older reader as `HawDBError::Storage`, which hosts treat as corruption.
//! Selectors that need more than the base reader now carry
//! `min_reader_version`. A reader first checks that field without decoding
//! the rest of the body and refuses a newer selector with the typed
//! `HawDBError::UnsupportedStorageFormat`. Only then does it decode strictly.
//!
//! The selector gates its whole closure: the resident snapshot, layouts,
//! lexical manifests and mutation runs are all reached through it. The bump
//! policy is documented in `docs/SEARCH_PROJECTION_MANIFEST_VERSION.md`.

use crate::checksum_bytes;
use crate::error::{HawDBError, Result};
use hawdb_core::StorageFormatError;
use serde::Deserialize;

/// The `format` named by `StorageFormatError` for a refused selector.
pub(super) const FORMAT_NAME: &str = "search projection manifest";

/// Version 1: every selector field written before #1763. Selectors at this
/// version omit both version fields, so they stay byte-identical to, and
/// readable by, releases that predate the version fields.
pub(super) const BASE_VERSION: u16 = 1;

/// Version 2 adds `content_source_graph_commit_epoch` (#1756), written only by
/// a metadata-only epoch advance, together with the version fields.
pub(super) const CONTENT_EPOCH_VERSION: u16 = 2;

/// The highest selector version this reader decodes. It is also the
/// `format_version` this writer stamps on a selector above the base version.
pub(super) const READER_VERSION: u16 = CONTENT_EPOCH_VERSION;

pub(super) const fn base_version() -> u16 {
    BASE_VERSION
}

pub(super) fn is_base_version(version: &u16) -> bool {
    *version == BASE_VERSION
}

/// Every top-level envelope field except `body` is fixed by
/// `SearchOutOfCoreManifestEnvelope`; the writer is `serde_json`'s compact
/// serializer, so the body is the bytes between these markers.
const BODY_PREFIX: &[u8] = b"{\"body\":";
const CHECKSUM_MARKER: &[u8] = b",\"checksum\":";

/// Only the version fields. No `deny_unknown_fields`: a newer writer's other
/// fields are ignored here and rejected by the strict decode that follows.
#[derive(Deserialize)]
struct VersionProbe {
    #[serde(default)]
    min_reader_version: Option<u64>,
}

/// Refuse a selector that requires a newer reader, before its fields are
/// decoded. The refusal is reported only for an intact envelope whose
/// checksum matches; anything else is left to the strict decode, which keeps
/// reporting malformed or damaged selectors as storage failures.
pub(super) fn check_reader_version(bytes: &[u8], supported: u16) -> Result<()> {
    let Some((body, checksum)) = split_envelope(bytes) else {
        return Ok(());
    };
    if checksum_bytes(body) != checksum {
        return Ok(());
    }
    let Ok(probe) = serde_json::from_slice::<VersionProbe>(body) else {
        return Ok(());
    };
    let required = probe.min_reader_version.unwrap_or(u64::from(BASE_VERSION));
    if required > u64::from(supported) {
        return Err(HawDBError::UnsupportedStorageFormat(
            StorageFormatError::UnsupportedVersion {
                format: FORMAT_NAME,
                found: u16::try_from(required).unwrap_or(u16::MAX),
                supported,
            },
        ));
    }
    Ok(())
}

/// The body bytes and the envelope checksum, or `None` when the bytes are not
/// a canonical selector envelope. The checksum is the last top-level member,
/// so the last marker is the envelope's even when nested records (mutation
/// runs) carry a `checksum` of their own.
fn split_envelope(bytes: &[u8]) -> Option<(&[u8], u64)> {
    let rest = bytes.strip_prefix(BODY_PREFIX)?;
    let marker = rest
        .windows(CHECKSUM_MARKER.len())
        .rposition(|window| window == CHECKSUM_MARKER)?;
    let digits = rest[marker + CHECKSUM_MARKER.len()..].strip_suffix(b"}")?;
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let checksum = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some((&rest[..marker], checksum))
}

#[cfg(test)]
mod tests;
