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

use std::ops::{Deref, DerefMut, Range};

/// A half-open byte range in the original UTF-8 query text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSpan {
    pub start: usize,
    pub end: usize,
}

impl SourceSpan {
    pub fn range(self) -> Range<usize> {
        self.start..self.end
    }

    pub fn text(self, source: &str) -> Option<&str> {
        source.get(self.range())
    }
}

/// Syntax with optional original-source provenance.
///
/// Equality compares syntax only. The parser must compare repeated parameters
/// and literals independently of where they occur, and whitespace does not
/// change query semantics. Compare `span` explicitly when comparing locations.
/// A synthetic node has no source range; it never borrows another node's range.
#[derive(Debug, Clone)]
pub struct AstNode<T> {
    pub kind: T,
    pub span: Option<SourceSpan>,
}

impl<T> AstNode<T> {
    pub fn from_source(kind: T, span: SourceSpan) -> Self {
        Self {
            kind,
            span: Some(span),
        }
    }

    pub fn synthetic(kind: T) -> Self {
        Self { kind, span: None }
    }
}

impl<T: PartialEq> PartialEq for AstNode<T> {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl<T: Eq> Eq for AstNode<T> {}

impl<T> Deref for AstNode<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.kind
    }
}

impl<T> DerefMut for AstNode<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.kind
    }
}
