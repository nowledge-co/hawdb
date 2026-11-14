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

/// Typed class of a relational refusal, so callers need not parse its text.
///
/// The storage crate's relational error reports this as the `source()` of its
/// constraint and schema variants, which lets
/// [`HawDBError::from_storage_error`](crate::HawDBError::from_storage_error)
/// preserve it across the crate boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelationalErrorKind {
    /// A row violates a declared constraint, such as a duplicate primary or
    /// unique key, a NULL in a NOT NULL column, or a missing foreign-key target.
    Constraint,
    /// A statement or value does not fit the relational schema, such as a
    /// value of the wrong type for its column.
    Schema,
}

impl RelationalErrorKind {
    /// Stable lowercase name, as bindings expose it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Constraint => "constraint",
            Self::Schema => "schema",
        }
    }
}

impl Display for RelationalErrorKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Constraint => "relational constraint violation",
            Self::Schema => "relational schema error",
        })
    }
}

impl std::error::Error for RelationalErrorKind {}

/// The layer that reported a relational refusal. It selects the message
/// prefix, `storage error:` or `execution error:`, and lets bindings keep the
/// error type that layer has always produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RelationalErrorSurface {
    /// Reported through storage, as an autocommit statement's commit does.
    Storage,
    /// Reported by a statement inside an explicit transaction.
    Execution,
}

impl RelationalErrorSurface {
    pub(super) const fn prefix(self) -> &'static str {
        match self {
            Self::Storage => "storage error",
            Self::Execution => "execution error",
        }
    }
}

/// Finds a relational refusal kind in `error`'s source chain, including
/// through the boxed cause of an `io::Error`, which `source` can skip.
pub fn find_relational_error_kind(
    error: &(dyn std::error::Error + 'static),
) -> Option<RelationalErrorKind> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(kind) = error.downcast_ref::<RelationalErrorKind>() {
            return Some(*kind);
        }
        if let Some(kind) = error
            .downcast_ref::<crate::HawDBError>()
            .and_then(crate::HawDBError::relational_error_kind)
        {
            return Some(kind);
        }
        if let Some(error) = error.downcast_ref::<std::io::Error>()
            && let Some(inner) = error.get_ref()
            && let Some(kind) = find_relational_error_kind(inner)
        {
            return Some(kind);
        }
        current = error.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HawDBError;

    /// Stands in for the storage crate's relational error, which core cannot name.
    #[derive(Debug)]
    struct Refusal(RelationalErrorKind, &'static str);

    impl Display for Refusal {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "{}: {}", self.0, self.1)
        }
    }

    impl std::error::Error for Refusal {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(match self.0 {
                RelationalErrorKind::Constraint => &RelationalErrorKind::Constraint,
                RelationalErrorKind::Schema => &RelationalErrorKind::Schema,
            })
        }
    }

    #[test]
    fn storage_conversion_keeps_the_kind_and_the_storage_text() {
        for (kind, detail) in [
            (RelationalErrorKind::Constraint, "duplicate primary key 1"),
            (RelationalErrorKind::Schema, "column id expects Int64"),
        ] {
            let text = format!("storage error: {kind}: {detail}");
            let direct = HawDBError::from_storage_error(Refusal(kind, detail));
            assert_eq!(direct.relational_error_kind(), Some(kind));
            assert_eq!(direct.to_string(), text);
            // Through an io::Error wrapper, and once more after conversion.
            let wrapped =
                HawDBError::from_storage_error(std::io::Error::other(Refusal(kind, detail)));
            assert_eq!(wrapped, direct);
            assert_eq!(HawDBError::from_storage_error(direct.clone()), direct);
            let context = HawDBError::from_storage_error_with_context(Refusal(kind, detail), "ctx");
            assert_eq!(context.relational_error_kind(), Some(kind));
            assert_eq!(
                context.to_string(),
                format!("storage error: ctx: {kind}: {detail}")
            );
        }
    }

    #[test]
    fn execution_surface_keeps_the_execution_text() {
        let error = HawDBError::relational(
            RelationalErrorKind::Constraint,
            RelationalErrorSurface::Execution,
            "relational constraint violation: duplicate",
        );
        assert_eq!(
            error.to_string(),
            "execution error: relational constraint violation: duplicate"
        );
        assert_eq!(
            error.relational_error_kind(),
            Some(RelationalErrorKind::Constraint)
        );
        assert_eq!(
            error.relational_error_surface(),
            Some(RelationalErrorSurface::Execution)
        );
    }

    #[test]
    fn other_storage_errors_have_no_relational_kind() {
        let error = HawDBError::from_storage_error(std::io::Error::other("disk"));
        assert!(matches!(error, HawDBError::Storage(_)));
        assert_eq!(error.relational_error_kind(), None);
        assert_eq!(
            HawDBError::Execution("x".into()).relational_error_kind(),
            None
        );
    }
}
