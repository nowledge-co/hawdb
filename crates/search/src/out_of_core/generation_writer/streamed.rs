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

use super::*;
use crate::analyzer_stream::Control;
use crate::document_encoding::streamed;
use crate::{SearchDocumentBody, SearchDocumentHeader};
use std::io::Read;

impl SearchOutOfCoreGenerationWriter {
    /// Captures one UTF-8 body exactly once into this private generation.
    ///
    /// The caller pins the source version until return. Declared length and the
    /// optional CRC32c checksum are verified before accepting the record. Input,
    /// cancellation or admission failures poison the writer; no partial record
    /// can be published. The caller retains no body-sized allocation obligation.
    pub fn push_reader(
        &mut self,
        header: SearchDocumentHeader,
        mut body: impl Read,
        source: SearchDocumentBody,
    ) -> Result<()> {
        if self.poisoned {
            return Err(HawDBError::Storage(
                "search generation writer is poisoned after an earlier input failure".into(),
            ));
        }
        let result = (|| {
            checkpoint(&self.task_context)?;
            let header = self.memory.admit_header(header)?;
            crate::lexical_projection::source::admit_streamed_source(
                header.header(),
                source.bytes,
                LexicalProjectionConfig {
                    max_document_source_bytes: self.options.lexical_max_document_source_bytes,
                    ..LexicalProjectionConfig::default()
                },
            )?;
            let record_bytes = streamed::record_len(header.header(), source.bytes)?;
            let prepared = self.prepare_record(header.header(), record_bytes)?;
            let spool = self.spool.as_mut().ok_or_else(|| {
                HawDBError::Storage("search generation spool is already closed".into())
            })?;
            let receipt = streamed::write_frame(
                spool,
                header.header(),
                &mut body,
                source,
                self.options.max_record_bytes.get(),
                Control {
                    memory: Some(&self.memory),
                    task: Some(&self.task_context),
                    ..Control::default()
                },
            )?;
            checkpoint(&self.task_context)?;
            self.documents_digest
                .add_record(receipt.checksum, receipt.bytes);
            self.needs_chinese_analyzer |= receipt.needs_chinese
                || std::iter::once((header.title.as_str(), 1))
                    .chain(crate::analyzer_stream::metadata_token_fields(
                        &header.metadata,
                    ))
                    .any(|(text, _)| text.chars().any(crate::cjk_tokenizer::is_han_search_char));
            self.commit_record(header.header, record_bytes, prepared);
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
}
