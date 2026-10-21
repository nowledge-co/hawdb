// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Immutable artifact families have distinct numeric namespaces. In particular,
//! a mutation run must not alias a content segment with the same generation.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ArtifactKey {
    Content(u64),
    Lexical(u64),
    RaBitQ(u64),
    MutationRun(u64),
}

impl ArtifactKey {
    pub(crate) fn generation(self) -> u64 {
        match self {
            Self::Content(id) | Self::Lexical(id) | Self::RaBitQ(id) | Self::MutationRun(id) => id,
        }
    }

    /// Accept canonical generated components only. Quarantine suffixes belong
    /// to cleanup discovery and are never valid manifest source references.
    pub(crate) fn from_file_name(name: &str) -> Option<Self> {
        const CONTENT: &[&str] = &[
            "search_projection_segments.",
            "search_projection_segment_payloads.",
            "search_projection_metadata_payloads.",
            "search_projection_vector_payloads.",
            "search_projection_out_of_core_layout.",
        ];
        if let Some(id) = CONTENT.iter().find_map(|prefix| generation(name, prefix)) {
            return Some(Self::Content(id));
        }
        generation(name, "search_lexical.manifest.")
            .or_else(|| generation(name, "search_lexical."))
            .map(Self::Lexical)
            .or_else(|| generation(name, "search_rabitq.").map(Self::RaBitQ))
            .or_else(|| generation(name, "search_projection_mutation_run.").map(Self::MutationRun))
    }
}

fn generation(name: &str, prefix: &str) -> Option<u64> {
    let digits = name.strip_prefix(prefix)?.strip_suffix(".hawdb")?;
    if digits.is_empty()
        || digits.len() > 1 && digits.starts_with('0')
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_names_preserve_artifact_family_identity() {
        for name in [
            "search_projection_segments.17.hawdb",
            "search_projection_segment_payloads.17.hawdb",
            "search_projection_metadata_payloads.17.hawdb",
            "search_projection_vector_payloads.17.hawdb",
            "search_projection_out_of_core_layout.17.hawdb",
        ] {
            assert_eq!(
                ArtifactKey::from_file_name(name),
                Some(ArtifactKey::Content(17))
            );
        }
        for (name, key) in [
            ("search_lexical.manifest.17.hawdb", ArtifactKey::Lexical(17)),
            ("search_lexical.17.hawdb", ArtifactKey::Lexical(17)),
            ("search_rabitq.17.hawdb", ArtifactKey::RaBitQ(17)),
            (
                "search_projection_mutation_run.17.hawdb",
                ArtifactKey::MutationRun(17),
            ),
        ] {
            assert_eq!(ArtifactKey::from_file_name(name), Some(key));
            assert_ne!(key, ArtifactKey::Content(17));
        }
        assert_ne!(ArtifactKey::MutationRun(17), ArtifactKey::Lexical(17));
        assert_ne!(ArtifactKey::MutationRun(17), ArtifactKey::RaBitQ(17));
    }

    #[test]
    fn only_canonical_source_components_are_artifact_keys() {
        for suffix in ["", "01", "+1", "-1", "١", "18446744073709551616", "1/2"] {
            assert_eq!(
                ArtifactKey::from_file_name(&format!("search_lexical.{suffix}.hawdb")),
                None
            );
        }
        for name in [
            "../search_lexical.1.hawdb",
            "search_lexical.1.hawdb.quarantine",
            "search_lexical.1.hawdb.extra",
        ] {
            assert_eq!(ArtifactKey::from_file_name(name), None);
        }
        assert_eq!(
            ArtifactKey::from_file_name("search_lexical.0.hawdb"),
            Some(ArtifactKey::Lexical(0))
        );
        assert_eq!(
            ArtifactKey::from_file_name("search_lexical.18446744073709551615.hawdb"),
            Some(ArtifactKey::Lexical(u64::MAX))
        );
    }
}
