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

//! Fits an analyzed lexical query to the bounded scored-term cap.
//!
//! Scoring work and stream memory grow with every scored term, so a query
//! keeps at most `LexicalProjectionConfig.max_query_terms` of them. Longer
//! queries retain their most selective terms: the lowest corpus document
//! frequency (the highest BM25 IDF) first, then ascending term bytes. Terms
//! absent from the corpus rank last because they cannot contribute a score.
//! The reduction is deterministic and reported; it is never a prefix cut.

use super::*;

/// How many analyzed terms a lexical query produced and how many it scored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryTermSelection {
    pub analyzed: usize,
    pub kept: usize,
    pub limit: NonZeroUsize,
}

impl QueryTermSelection {
    pub(crate) const fn unchanged(analyzed: usize, limit: NonZeroUsize) -> Self {
        Self {
            analyzed,
            kept: analyzed,
            limit,
        }
    }

    pub(crate) const fn dropped(&self) -> usize {
        self.analyzed - self.kept
    }

    pub(crate) const fn reduced(&self) -> bool {
        self.kept < self.analyzed
    }
}

impl std::fmt::Display for QueryTermSelection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "lexical query scored the {} most selective of {} analyzed terms; \
             dropped {} less selective terms (max_query_terms {})",
            self.kept,
            self.analyzed,
            self.dropped(),
            self.limit
        )
    }
}

/// Retains at most `limit` terms, preferring the lowest document frequency.
///
/// `document_frequency` must report the corpus statistics BM25 will use, so
/// the retained terms score exactly as a query containing only them.
pub(crate) fn retain_most_selective_terms<T: Borrow<str> + Ord>(
    terms: &mut BTreeSet<T>,
    limit: NonZeroUsize,
    document_frequency: impl Fn(&str) -> u64,
    context: Option<QueryContext<'_>>,
) -> Result<QueryTermSelection> {
    let analyzed = terms.len();
    if analyzed <= limit.get() {
        return Ok(QueryTermSelection::unchanged(analyzed, limit));
    }
    query_context::check(context)?;
    let rank = |term: &str| match document_frequency(term) {
        0 => u64::MAX,
        frequency => frequency,
    };
    let longest = terms
        .iter()
        .map(query_term)
        .map(str::len)
        .max()
        .unwrap_or(0);
    let _memory = context
        .map(|context| {
            analyzed
                .checked_mul(std::mem::size_of::<(u64, &str)>())
                .and_then(|bytes| bytes.checked_add(longest))
                .ok_or_else(|| HawDBError::Execution("query term ranking size overflow".into()))
                .and_then(|bytes| context.memory.reserve(bytes))
        })
        .transpose()?;
    let (cutoff_rank, cutoff_term) = {
        let mut ranked = terms
            .iter()
            .map(query_term)
            .map(|term| (rank(term), term))
            .collect::<Vec<_>>();
        ranked.sort_unstable();
        let (frequency, term) = ranked[limit.get() - 1];
        (frequency, term.to_owned())
    };
    query_context::check(context)?;
    terms
        .retain(|term| (rank(term.borrow()), term.borrow()) <= (cutoff_rank, cutoff_term.as_str()));
    debug_assert_eq!(terms.len(), limit.get());
    query_context::check(context)?;
    Ok(QueryTermSelection {
        analyzed,
        kept: terms.len(),
        limit,
    })
}

impl LexicalProjectionReader {
    /// Scored-term cap for one query; analyzed terms beyond it are reduced.
    pub(crate) const fn max_query_terms(&self) -> NonZeroUsize {
        self.config.max_query_terms
    }

    /// Fits a single-generation query to the scored-term cap using the
    /// generation's document frequencies projected through `delta`.
    pub(crate) fn fit_query_terms<T: Borrow<str> + Ord>(
        &self,
        terms: &mut BTreeSet<T>,
        delta: &LexicalMiniDelta,
        max_term_bytes: NonZeroU64,
        context: Option<QueryContext<'_>>,
    ) -> Result<QueryTermSelection> {
        let limit = self.config.max_query_terms;
        if terms.len() <= limit.get() {
            return Ok(QueryTermSelection::unchanged(terms.len(), limit));
        }
        let accounted;
        let local;
        let statistics = match context {
            Some(context) => {
                accounted =
                    AccountedCorpusStatistics::aggregate([self], &*terms, max_term_bytes, context)?;
                &accounted.statistics
            }
            None => {
                local = self.query_statistics_for_terms(&*terms, max_term_bytes, None)?;
                &local
            }
        };
        retain_most_selective_terms(
            terms,
            limit,
            |term| {
                delta.projected_document_frequency(term, statistics.document_frequency(term)) as u64
            },
            context,
        )
    }
}
