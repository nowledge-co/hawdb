// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::io::Write;

#[test]
fn frame_fields_match_the_native_parser_for_pledged_and_streamed_frames() {
    for size in [0, 1, 255, 256, 65535, 65536, 300_000] {
        let input = vec![42; size];
        let mut writer = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
        writer.write_all(&input).unwrap();
        for encoded in [
            writer.finish().unwrap(),
            zstd::bulk::compress(&input, 3).unwrap(),
        ] {
            let header = parse_frame_header(&encoded).unwrap();
            assert!(!header.skippable);
            assert_eq!(
                header.content_bytes,
                zstd::zstd_safe::get_frame_content_size(&encoded).unwrap()
            );
            assert!(header.window_bytes >= size.min(1024) as u64);
            assert_eq!(header.block_bytes, header.window_bytes.min(128 * 1024));
            for len in 0..5 {
                assert!(parse_frame_header(&encoded[..len]).is_err());
            }
        }
    }
}

#[test]
fn malformed_headers_fail_without_native_contexts() {
    for magic in [0xfd2fb527u32, 0, u32::MAX] {
        assert!(parse_frame_header(&magic.to_le_bytes()).is_err());
    }
    let mut bytes = 0x184d2a50u32.to_le_bytes().to_vec();
    bytes.extend(7u32.to_le_bytes());
    assert!(parse_frame_header(&bytes).unwrap().skippable);
    assert!(parse_frame_header(&bytes[..7]).is_err());
    for size in [u64::MAX, u64::MAX - 1] {
        let mut bytes = vec![0x28, 0xb5, 0x2f, 0xfd, 0xe0];
        bytes.extend(size.to_le_bytes());
        assert!(parse_frame_header(&bytes).is_err());
    }
    assert!(parse_frame_header(&[0x28, 0xb5, 0x2f, 0xfd, 0x28, 0]).is_err());
}
