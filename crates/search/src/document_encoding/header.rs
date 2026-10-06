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
use std::borrow::Borrow;
use std::collections::BTreeMap;

#[derive(Clone, Copy)]
pub(crate) struct Header<'a> {
    pub(crate) id: &'a str,
    pub(crate) title: &'a str,
    pub(crate) embedding: Option<&'a [f32]>,
    pub(crate) metadata: &'a BTreeMap<String, String>,
}

impl<'a> Header<'a> {
    pub(crate) fn field(self, key: &str) -> Option<&'a str> {
        match key {
            crate::SEARCH_DOCUMENT_ID_FIELD => Some(self.id),
            "space_id" => Some(
                self.metadata
                    .get(key)
                    .map(String::as_str)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(crate::DEFAULT_SPACE_ID),
            ),
            _ => self.metadata.get(key).map(String::as_str),
        }
    }
}

pub(crate) trait HeaderSource {
    fn header(&self) -> Header<'_>;
}

pub(crate) trait RecordSource: HeaderSource {
    fn encoded_len(&self, task: Option<&hawdb_core::RuntimeTaskContext>) -> Result<usize>;
    fn write_encoded(&self, output: &mut impl io::Write) -> io::Result<()>;
}

impl<T: Borrow<SearchDocument>> HeaderSource for T {
    fn header(&self) -> Header<'_> {
        let document = self.borrow();
        Header {
            id: &document.id,
            title: &document.title,
            embedding: document.embedding.as_deref(),
            metadata: &document.metadata,
        }
    }
}

impl<T: Borrow<SearchDocument>> RecordSource for T {
    fn encoded_len(&self, task: Option<&hawdb_core::RuntimeTaskContext>) -> Result<usize> {
        Ok(DocumentEncoding::new_with_context(self.borrow(), task)?.len())
    }
    fn write_encoded(&self, output: &mut impl io::Write) -> io::Result<()> {
        DocumentEncoding::new_with_context(self.borrow(), None)
            .map_err(io::Error::other)?
            .write_to(output)
    }
}

impl HeaderSource for crate::SearchDocumentHeader {
    fn header(&self) -> Header<'_> {
        Header {
            id: &self.id,
            title: &self.title,
            embedding: self.embedding.as_deref(),
            metadata: &self.metadata,
        }
    }
}

impl HeaderSource for crate::build_memory::AdmittedHeader {
    fn header(&self) -> Header<'_> {
        self.header.header()
    }
}
