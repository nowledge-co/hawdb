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

use std::fmt::{Display, Formatter};

// The `format` reported by `StorageFormatError`. Hosts that react differently
// to different formats compare `StorageFormatError::format()` against these
// constants, never against the message text.

/// The storage manifest and checkpoint `version` line (`hawdb-storage-v<N>`).
pub const STORAGE_MANIFEST_FORMAT: &str = "storage manifest";
/// Relational WAL and full-checkpoint envelopes.
pub const RELATIONAL_DURABLE_CODEC_FORMAT: &str = "relational durable codec";
/// Relational schema records, including materialized-view expression tags.
pub const RELATIONAL_SCHEMA_FORMAT: &str = "relational schema";
/// Relational row-page publications.
pub const RELATIONAL_ROW_PAGE_FORMAT: &str =
    "relational row-page format (recreate the development database)";
/// Embedded append WAL records.
/// Relational checkpoint layouts this reader cannot open in the requested
/// residency or index configuration, such as checkpoints written without a
/// full-row relational file (#1737) or without a persistent relational index
/// generation required by Authoritative indexes.
pub const RELATIONAL_CHECKPOINT_LAYOUT_FORMAT: &str = "relational checkpoint layout";
pub const APPEND_WAL_FORMAT: &str = "append WAL";
/// Logical dumps (`docs/LOGICAL_DUMP.md`).
pub const LOGICAL_DUMP_FORMAT: &str = "logical dump";
/// The search projection selector manifest
/// (`docs/SEARCH_PROJECTION_MANIFEST_VERSION.md`).
pub const SEARCH_PROJECTION_MANIFEST_FORMAT: &str = "search projection manifest";

/// An intact storage envelope requires a different reader capability.
/// Refusal does not authorize repair, truncation, or database recreation.
///
/// `format` is always one of the `*_FORMAT` constants exported next to this
/// type, such as [`SEARCH_PROJECTION_MANIFEST_FORMAT`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageFormatError {
    UnsupportedVersion {
        format: &'static str,
        found: u16,
        supported: u16,
    },
    UnsupportedFeature {
        format: &'static str,
        feature: &'static str,
        tag: u64,
    },
}

impl StorageFormatError {
    /// The refused format: one of the exported `*_FORMAT` constants.
    pub const fn format(&self) -> &'static str {
        match self {
            Self::UnsupportedVersion { format, .. } | Self::UnsupportedFeature { format, .. } => {
                format
            }
        }
    }
}

impl Display for StorageFormatError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion {
                format,
                found,
                supported,
            } => write!(
                formatter,
                "unsupported {format} version {found}; this reader supports version {supported}"
            ),
            Self::UnsupportedFeature {
                format,
                feature,
                tag,
            } => write!(
                formatter,
                "unsupported {format} feature {feature} tag {tag}"
            ),
        }
    }
}

impl std::error::Error for StorageFormatError {}

pub(super) fn find_storage_format_error(
    error: &(dyn std::error::Error + 'static),
) -> Option<StorageFormatError> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<StorageFormatError>() {
            return Some(error.clone());
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>()
            && let Some(inner) = error.get_ref()
            && let Some(error) = find_storage_format_error(inner)
        {
            return Some(error);
        }
        current = error.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HawDBError;

    #[test]
    fn format_accessor_reports_the_exported_constant() {
        let names = [
            STORAGE_MANIFEST_FORMAT,
            RELATIONAL_DURABLE_CODEC_FORMAT,
            RELATIONAL_SCHEMA_FORMAT,
            RELATIONAL_ROW_PAGE_FORMAT,
            RELATIONAL_CHECKPOINT_LAYOUT_FORMAT,
            APPEND_WAL_FORMAT,
            LOGICAL_DUMP_FORMAT,
            SEARCH_PROJECTION_MANIFEST_FORMAT,
        ];
        let unique = names.iter().collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), names.len());
        let version = StorageFormatError::UnsupportedVersion {
            format: SEARCH_PROJECTION_MANIFEST_FORMAT,
            found: 3,
            supported: 2,
        };
        assert_eq!(version.format(), SEARCH_PROJECTION_MANIFEST_FORMAT);
        let feature = StorageFormatError::UnsupportedFeature {
            format: RELATIONAL_SCHEMA_FORMAT,
            feature: "materialized view expression",
            tag: 6,
        };
        assert_eq!(feature.format(), RELATIONAL_SCHEMA_FORMAT);
    }

    #[test]
    fn storage_wrappers_preserve_compatibility_refusal() {
        for error in [
            StorageFormatError::UnsupportedVersion {
                format: RELATIONAL_DURABLE_CODEC_FORMAT,
                found: 1,
                supported: 2,
            },
            StorageFormatError::UnsupportedFeature {
                format: RELATIONAL_SCHEMA_FORMAT,
                feature: "materialized view expression",
                tag: 6,
            },
        ] {
            let wrapped = std::io::Error::other(std::io::Error::other(error.clone()));
            assert!([RELATIONAL_DURABLE_CODEC_FORMAT, RELATIONAL_SCHEMA_FORMAT]
                .contains(&error.format()));
            assert_eq!(
                HawDBError::from_storage_error_with_context(wrapped, "open failed"),
                HawDBError::UnsupportedStorageFormat(error),
            );
        }
        assert_eq!(
            HawDBError::from_storage_error_with_context(
                std::io::Error::other("checksum mismatch"),
                "open failed",
            ),
            HawDBError::Storage("open failed: checksum mismatch".into()),
        );
    }
}
