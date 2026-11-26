// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[test]
fn consumer_and_hawdb_share_the_qualified_native_library() {
    assert_eq!(zstd_sys::ZSTD_VERSION_NUMBER, 10600);
    assert_eq!(zstd::zstd_safe::version_number(), 10600);
    let input = b"independent consumer with one native zstd library";
    let encoded = hawdb_storage::compression::encode_all(&input[..], 3).unwrap();
    assert_eq!(zstd::stream::decode_all(encoded.as_slice()).unwrap(), input);
    let encoded = zstd::stream::encode_all(&input[..], 3).unwrap();
    let mut decoder = hawdb_storage::compression::Decoder::new(encoded.as_slice()).unwrap();
    let mut decoded = Vec::new();
    std::io::Read::read_to_end(&mut decoder, &mut decoded).unwrap();
    assert_eq!(decoded, input);
    assert!(
        hawdb_storage::compression::install_compression_backend(std::sync::Arc::new(
            hawdb_storage::compression::TestZstdBackend,
        ))
        .is_err()
    );
}
