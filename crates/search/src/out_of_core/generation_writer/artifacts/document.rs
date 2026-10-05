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

use super::super::spool::SpoolRecord;
use super::*;
use crate::document_encoding::{Header, HeaderSource, RecordSource};
use std::io;

pub(super) enum SegmentDocument {
    #[cfg(test)]
    Owned(AdmittedDocument),
    Spool(crate::build_memory::shared::Shared<SpoolRecord>),
}

impl HeaderSource for SegmentDocument {
    fn header(&self) -> Header<'_> {
        match self {
            #[cfg(test)]
            Self::Owned(document) => document.header(),
            Self::Spool(document) => document.header(),
        }
    }
}

impl RecordSource for SegmentDocument {
    fn encoded_len(&self, task: Option<&RuntimeTaskContext>) -> Result<usize> {
        match self {
            #[cfg(test)]
            Self::Owned(document) => document.encoded_len(task),
            Self::Spool(document) => document.encoded_len(task),
        }
    }
    fn write_encoded(&self, output: &mut impl Write) -> io::Result<()> {
        match self {
            #[cfg(test)]
            Self::Owned(document) => document.write_encoded(output),
            Self::Spool(document) => document.write_encoded(output),
        }
    }
}
