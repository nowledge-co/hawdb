//! Source sidecar ownership lives in storage; GraphStore still owns publication.
pub use skein_storage::source_scan::SourceScanRow;
#[cfg(test)]
pub(super) use skein_storage::source_scan::SOURCE_SCAN_TARGET_ROWS;
pub(super) use skein_storage::source_scan::{
    build, decode_payload, load, write, SourceScanPublication, SOURCE_SCAN_ARTIFACT_ID,
    SOURCE_SCAN_DESCRIPTOR_FILE, SOURCE_SCAN_PAYLOAD_FILE,
};

#[cfg(test)]
mod tests {
    #[test]
    fn source_scan_row_preserves_facade_type_identity() {
        assert_eq!(
            std::any::TypeId::of::<crate::store::SourceScanRow>(),
            std::any::TypeId::of::<skein_storage::source_scan::SourceScanRow>()
        );
        let decode: fn(&[u8]) -> crate::Result<Vec<crate::store::SourceScanRow>> =
            super::decode_payload;
        assert!(std::ptr::fn_addr_eq(
            decode,
            skein_storage::source_scan::decode_payload as fn(_) -> _
        ));
    }
}
