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

pub(crate) fn reference_decode(input: &str) -> Option<Vec<u8>> {
    fn nibble(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    if !input.len().is_multiple_of(2) {
        return None;
    }
    input
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            // Preserve the legacy unsigned radix parser's leading-plus pair.
            let high = if pair[0] == b'+' { 0 } else { nibble(pair[0])? };
            Some(high * 16 + nibble(pair[1])?)
        })
        .collect()
}

pub(crate) fn campaign_inputs() -> Vec<String> {
    let mut state = 0x0401_5afe_dec0_u64;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        state
    };
    (0..1024)
        .map(|case| {
            let mut input = (0..next() % 128)
                .map(|_| format!("{:02x}", (next() >> 32) as u8))
                .collect::<String>();
            match case % 8 {
                1 => input.make_ascii_uppercase(),
                2 => input.push_str("gg"),
                3 => input.push('f'),
                4 => input.insert_str(0, "a\u{e9}a"),
                5 => input.push_str("+0+f"),
                6 => input.truncate(input.len() / 2),
                7 => input.push('\u{1f980}'),
                _ => {}
            }
            input
        })
        .collect()
}
