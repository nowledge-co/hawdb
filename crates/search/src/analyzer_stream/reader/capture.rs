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

//! One-shot capture proof. The caller owns private staging and publication.

use super::*;
use hawdb_integrity::Crc32cHasher;
use std::io::Write;

#[derive(Debug, PartialEq, Eq)]
struct Receipt {
    bytes: u64,
    checksum: u64,
    needs_chinese: bool,
}

fn capture(
    source: &mut impl Read,
    private_stage: &mut impl Write,
    expected_bytes: u64,
    expected_checksum: Option<u64>,
    control: Control<'_>,
) -> Result<Receipt> {
    let mut digest = Crc32cHasher::new();
    let mut needs_chinese = false;
    let bytes = utf8::visit(source, control, expected_bytes, |text| {
        private_stage.write_all(text.as_bytes())?;
        digest.update(text.as_bytes());
        needs_chinese |= text.chars().any(crate::cjk_tokenizer::is_han_search_char);
        Ok(())
    })?;
    if bytes != expected_bytes {
        return Err(HawDBError::Storage(
            "streamed source length mismatch".into(),
        ));
    }
    let checksum = digest.finish();
    if expected_checksum.is_some_and(|expected| expected != checksum) {
        return Err(HawDBError::Storage(
            "streamed source checksum mismatch".into(),
        ));
    }
    control.check()?;
    private_stage.flush()?;
    control.check()?;
    Ok(Receipt {
        bytes,
        checksum,
        needs_chinese,
    })
}

#[cfg(test)]
mod tests;
