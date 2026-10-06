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

//! Logical fields supply events independently of their physical body storage.

use super::*;
use crate::analyzer_stream::Control;

pub(crate) trait DocumentSource {
    fn id(&self) -> &str;
    fn admit_source(&self, config: LexicalProjectionConfig) -> Result<()>;
    fn visit_tokens(
        &self,
        analyzer: &SearchAnalyzerLexicon,
        control: Control<'_>,
        emit: &mut dyn FnMut(u8, usize, Term, TokenOccurrence) -> Result<()>,
    ) -> Result<()>;
}

impl DocumentSource for SearchDocument {
    fn id(&self) -> &str {
        &self.id
    }

    fn admit_source(&self, config: LexicalProjectionConfig) -> Result<()> {
        admit_document_source(self, config)
    }

    fn visit_tokens(
        &self,
        analyzer: &SearchAnalyzerLexicon,
        control: Control<'_>,
        emit: &mut dyn FnMut(u8, usize, Term, TokenOccurrence) -> Result<()>,
    ) -> Result<()> {
        for (field, (text, weight)) in document_token_fields(self).enumerate() {
            let field = u8::try_from(field).expect("at most six analysis fields");
            crate::analyzer_stream::visit_admitted_token_list(
                text,
                analyzer,
                control,
                |term, occurrence| emit(field, weight, term, occurrence),
            )?;
        }
        Ok(())
    }
}

pub(crate) fn admit_streamed_source(
    header: crate::document_encoding::Header<'_>,
    body_bytes: u64,
    config: LexicalProjectionConfig,
) -> Result<()> {
    let overflow = || HawDBError::Storage("streamed document source length overflow".into());
    let base = body_bytes
        .checked_add(header.title.len() as u64)
        .ok_or_else(overflow)?;
    let bytes = header
        .metadata
        .iter()
        .try_fold(base, |bytes, (key, value)| {
            bytes
                .checked_add(key.len() as u64)?
                .checked_add(value.len() as u64)
        })
        .ok_or_else(overflow)?;
    if bytes > config.max_document_source_bytes.get() {
        return Err(HawDBError::Storage(
            "streamed document source bytes exceed lexical admission".into(),
        ));
    }
    Ok(())
}

pub(crate) fn visit_streamed_source(
    header: crate::document_encoding::Header<'_>,
    body: &mut impl std::io::Read,
    body_bytes: u64,
    analyzer: &SearchAnalyzerLexicon,
    control: Control<'_>,
    emit: &mut dyn FnMut(u8, usize, Term, TokenOccurrence) -> Result<()>,
) -> Result<()> {
    crate::analyzer_stream::visit_admitted_token_list(
        header.title,
        analyzer,
        control,
        |term, occurrence| emit(0, crate::TITLE_TERM_FREQUENCY_WEIGHT, term, occurrence),
    )?;
    let bytes = crate::analyzer_stream::reader::visit_reader(
        body,
        analyzer,
        control,
        body_bytes,
        usize::try_from(body_bytes).unwrap_or(usize::MAX),
        |term, occurrence| emit(1, 1, term, occurrence),
    )?;
    if bytes != body_bytes {
        return Err(HawDBError::Storage("streamed body length changed".into()));
    }
    for (field, (text, weight)) in
        crate::analyzer_stream::metadata_token_fields(header.metadata).enumerate()
    {
        let field = u8::try_from(field + 2).expect("at most six analysis fields");
        crate::analyzer_stream::visit_admitted_token_list(
            text,
            analyzer,
            control,
            |term, occurrence| emit(field, weight, term, occurrence),
        )?;
    }
    Ok(())
}
