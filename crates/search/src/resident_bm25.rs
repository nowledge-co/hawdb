// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Exact resident fallback statistics for one query's terms. The caller chooses
//! the corpus; ordinary search keeps its existing filtered-document domain.

use crate::analyzer_stream::Control;
use crate::resident_text_seed::count_document_tokens;
use crate::{Result, SearchAnalyzerLexicon, SearchDocument, BM25_B, BM25_K1};
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Scorer<'a> {
    query_terms: BTreeMap<&'a str, usize>,
    document_frequency: Vec<usize>,
    frequencies: Vec<usize>,
    document_count: usize,
    average_document_len: f64,
}

impl<'a> Scorer<'a> {
    pub(super) fn from_documents<'d>(
        terms: &'a BTreeSet<String>,
        documents: impl Iterator<Item = &'d SearchDocument>,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<Self> {
        let mut scorer = Self {
            query_terms: terms
                .iter()
                .enumerate()
                .map(|(index, term)| (term.as_str(), index))
                .collect(),
            document_frequency: vec![0; terms.len()],
            frequencies: vec![0; terms.len()],
            document_count: 0,
            average_document_len: 0.0,
        };
        let mut total_len = 0;
        for document in documents {
            scorer.document_count += 1;
            total_len += scorer.count(document, analyzer)?;
            for (frequency, df) in scorer
                .frequencies
                .iter()
                .zip(&mut scorer.document_frequency)
            {
                // Repetition and field weights affect TF/length, never DF.
                if *frequency > 0 {
                    *df += 1;
                }
            }
        }
        if scorer.document_count != 0 {
            scorer.average_document_len = total_len as f64 / scorer.document_count as f64;
        }
        Ok(scorer)
    }

    fn count(
        &mut self,
        document: &SearchDocument,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<usize> {
        self.frequencies.fill(0);
        count_document_tokens(
            document,
            analyzer,
            &self.query_terms,
            &mut self.frequencies,
            Control::default(),
        )
    }

    pub(super) fn score(
        &mut self,
        document: &SearchDocument,
        analyzer: &SearchAnalyzerLexicon,
    ) -> Result<f64> {
        if self.query_terms.is_empty() || self.document_count == 0 {
            return Ok(0.0);
        }
        let document_len = self.count(document, analyzer)?;
        if document_len == 0 {
            return Ok(0.0);
        }
        let document_len = document_len as f64;
        let average_document_len = self.average_document_len.max(1.0);
        let mut score = 0.0;
        // BTreeMap iteration preserves the legacy query BTreeSet's summation
        // order, including roundoff at tied ranking boundaries.
        for index in self.query_terms.values() {
            let frequency = self.frequencies[*index];
            let document_frequency = self.document_frequency[*index];
            if frequency == 0 || document_frequency == 0 {
                continue;
            }
            let idf = (1.0
                + (self.document_count as f64 - document_frequency as f64 + 0.5)
                    / (document_frequency as f64 + 0.5))
                .ln();
            let frequency = frequency as f64;
            let denominator =
                frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * document_len / average_document_len);
            score += idf * (frequency * (BM25_K1 + 1.0)) / denominator;
        }
        Ok(score)
    }
}

#[cfg(test)]
mod tests;
