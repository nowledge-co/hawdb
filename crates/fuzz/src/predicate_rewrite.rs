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

pub(crate) const PREDICATE_REWRITE_SHAPES: [&str; 6] = [
    "double_negation",
    "conjunction_idempotence",
    "disjunction_idempotence",
    "null_totality",
    "conjunction_absorption",
    "disjunction_absorption",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PredicateRewriteKind {
    DoubleNegation,
    ConjunctionIdempotence,
    DisjunctionIdempotence,
    NullTotality,
    ConjunctionAbsorption,
    DisjunctionAbsorption,
}

impl PredicateRewriteKind {
    pub(crate) const fn for_case(index: usize) -> Self {
        match index % PREDICATE_REWRITE_SHAPES.len() {
            0 => Self::DoubleNegation,
            1 => Self::ConjunctionIdempotence,
            2 => Self::DisjunctionIdempotence,
            3 => Self::NullTotality,
            4 => Self::ConjunctionAbsorption,
            _ => Self::DisjunctionAbsorption,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::DoubleNegation => PREDICATE_REWRITE_SHAPES[0],
            Self::ConjunctionIdempotence => PREDICATE_REWRITE_SHAPES[1],
            Self::DisjunctionIdempotence => PREDICATE_REWRITE_SHAPES[2],
            Self::NullTotality => PREDICATE_REWRITE_SHAPES[3],
            Self::ConjunctionAbsorption => PREDICATE_REWRITE_SHAPES[4],
            Self::DisjunctionAbsorption => PREDICATE_REWRITE_SHAPES[5],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_selection_covers_every_rewrite_deterministically() {
        let observed = (0..PREDICATE_REWRITE_SHAPES.len())
            .map(|index| PredicateRewriteKind::for_case(index).as_str())
            .collect::<Vec<_>>();

        assert_eq!(observed, PREDICATE_REWRITE_SHAPES);
        assert_eq!(PredicateRewriteKind::for_case(6).as_str(), observed[0]);
    }
}
