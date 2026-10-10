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

pub(crate) fn visit(
    reader: &mut impl Read,
    control: Control<'_>,
    max_source_bytes: u64,
    mut consume: impl FnMut(&str) -> Result<()>,
) -> Result<u64> {
    let _scratch = control
        .memory
        .map(|memory| memory.spool().reserve(BUFFER_BYTES + 4))
        .transpose()?;
    let mut total = 0u64;
    let mut buffer = [0u8; BUFFER_BYTES + 4];
    let mut pending = 0;
    loop {
        control.check()?;
        let count = match reader.read(&mut buffer[pending..pending + BUFFER_BYTES]) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        control.check()?;
        total = total
            .checked_add(count as u64)
            .filter(|bytes| *bytes <= max_source_bytes)
            .ok_or_else(|| {
                HawDBError::Storage("streamed field exceeds source byte admission".into())
            })?;
        let filled = pending + count;
        let (valid, incomplete) = match std::str::from_utf8(&buffer[..filled]) {
            Ok(_) => (filled, false),
            Err(error) if error.error_len().is_none() => (error.valid_up_to(), true),
            Err(_) => return Err(HawDBError::Storage("streamed field is not UTF-8".into())),
        };
        let text = std::str::from_utf8(&buffer[..valid]).expect("validated UTF-8 prefix");
        consume(text)?;
        pending = filled - valid;
        buffer.copy_within(valid..filled, 0);
        if count == 0 {
            if incomplete {
                return Err(HawDBError::Storage(
                    "streamed field ends inside UTF-8".into(),
                ));
            }
            return Ok(total);
        }
    }
}
