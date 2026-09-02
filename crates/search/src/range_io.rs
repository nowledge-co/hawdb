use super::{
    checksum_bytes, decode_search_segment_documents, validate_search_segment_documents,
    SearchIndex, SearchPhysicalRangeRead, SearchPredicateSet, SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
    SEARCH_SEGMENT_PAYLOAD_FILE,
};
use crate::error::{Result, SkeinError};
use skein_qos::{IoConcurrencyBudget, StorageDeviceProfile};
use skein_storage::{
    FileSegmentRangeReader, SegmentReadExecutor, SegmentReadRange, SegmentReadScheduler,
};
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

const DESKTOP_SEARCH_RANGE_READ_MAX_WAVE_BYTES: u64 = 8 * 1024 * 1024;
const MOBILE_SEARCH_RANGE_READ_MAX_WAVE_BYTES: u64 = 2 * 1024 * 1024;
const INDEPENDENT_SEARCH_SEGMENT_MAX_COALESCED_BYTES: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchRangeReadConfig {
    pub io_depth: NonZeroUsize,
    pub max_wave_bytes: NonZeroU64,
}

impl SearchRangeReadConfig {
    pub fn new(io_depth: NonZeroUsize, max_wave_bytes: NonZeroU64) -> Self {
        Self {
            io_depth,
            max_wave_bytes,
        }
    }

    pub fn from_io_budget(io: IoConcurrencyBudget, max_wave_bytes: NonZeroU64) -> Self {
        Self::new(io.foreground_depth, max_wave_bytes)
    }

    pub fn desktop_bound(io: IoConcurrencyBudget) -> Self {
        Self::from_io_budget(
            io,
            NonZeroU64::new(DESKTOP_SEARCH_RANGE_READ_MAX_WAVE_BYTES)
                .expect("desktop search range-read wave budget is non-zero"),
        )
    }

    pub fn mobile_embedded(io: IoConcurrencyBudget) -> Self {
        Self::from_io_budget(
            io,
            NonZeroU64::new(MOBILE_SEARCH_RANGE_READ_MAX_WAVE_BYTES)
                .expect("mobile search range-read wave budget is non-zero"),
        )
    }
}

impl Default for SearchRangeReadConfig {
    fn default() -> Self {
        Self::desktop_bound(IoConcurrencyBudget::desktop_bound_for_device(
            StorageDeviceProfile::default(),
        ))
    }
}

impl SearchIndex {
    pub(super) fn read_pruned_search_segments(
        &self,
        predicates: &SearchPredicateSet,
    ) -> Result<Option<SearchPhysicalRangeRead>> {
        let (Some(path), Some(descriptor)) = (&self.path, &self.segment_descriptor) else {
            return Ok(None);
        };
        if predicates.is_empty() || !descriptor.matches_documents(&self.documents) {
            return Ok(None);
        }

        let matching_segments = descriptor
            .segments
            .iter()
            .filter(|segment| segment.may_match_predicates(predicates))
            .collect::<Vec<_>>();
        if matching_segments.len() == descriptor.segments.len() {
            return Ok(None);
        }
        if matching_segments.is_empty() {
            return Ok(Some(SearchPhysicalRangeRead::default()));
        }
        if descriptor
            .segments
            .iter()
            .any(|segment| segment.payload_range.is_none())
        {
            return Err(SkeinError::Storage(
                "search segment physical ranges are unavailable; checkpoint or rebuild the search projection"
                    .to_string(),
            ));
        }

        let ranges = matching_segments
            .iter()
            .map(|segment| {
                let range = segment
                    .payload_range
                    .expect("physical range presence was validated");
                SegmentReadRange::new(
                    range.artifact_id,
                    segment.segment_id,
                    range.offset,
                    NonZeroU64::new(range.length)
                        .expect("persisted physical payload ranges are non-empty"),
                )
            })
            .collect::<Vec<_>>();
        let schedule = SegmentReadScheduler::new(
            self.range_read_config.io_depth,
            NonZeroU64::new(INDEPENDENT_SEARCH_SEGMENT_MAX_COALESCED_BYTES)
                .expect("independent segment coalescing limit is non-zero"),
        )
        .schedule_with_wave_budget(ranges, self.range_read_config.max_wave_bytes);
        let mut reader = FileSegmentRangeReader::new();
        reader.register(
            SEARCH_SEGMENT_PAYLOAD_ARTIFACT_ID,
            path.join(SEARCH_SEGMENT_PAYLOAD_FILE),
        );
        let mut documents = BTreeMap::new();
        let report = SegmentReadExecutor::new(self.range_read_config.max_wave_bytes)
            .execute(&reader, &schedule, |payload| {
                let [segment_id] = payload.range.segment_ids.as_slice() else {
                    return Err(SkeinError::Storage(
                        "search range reader unexpectedly coalesced independent segment frames"
                            .to_string(),
                    ));
                };
                let segment_index = usize::try_from(*segment_id).map_err(|_| {
                    SkeinError::Storage(
                        "search range reader returned an unsupported segment id".to_string(),
                    )
                })?;
                let segment = descriptor.segments.get(segment_index).ok_or_else(|| {
                    SkeinError::Storage(format!(
                        "search range reader returned unknown segment id {segment_id}"
                    ))
                })?;
                let expected = segment
                    .payload_range
                    .expect("physical range presence was validated");
                let actual_checksum = checksum_bytes(&payload.bytes);
                if actual_checksum != expected.checksum {
                    return Err(SkeinError::Storage(format!(
                        "search segment {segment_id} payload checksum mismatch: expected {}, got {actual_checksum}",
                        expected.checksum
                    )));
                }
                let segment_documents = decode_search_segment_documents(&payload.bytes)?;
                validate_search_segment_documents(segment, &segment_documents)?;
                for document in segment_documents {
                    let document_id = document.id.clone();
                    if documents.insert(document_id.clone(), document).is_some() {
                        return Err(SkeinError::Storage(format!(
                            "search range reader returned duplicate document id {document_id}"
                        )));
                    }
                }
                Ok::<(), SkeinError>(())
            })
            .map_err(|error| {
                SkeinError::Storage(format!("search segment range execution failed: {error}"))
            })?;
        let expected_document_count = matching_segments
            .iter()
            .map(|segment| segment.document_count)
            .sum::<usize>();
        if documents.len() != expected_document_count {
            return Err(SkeinError::Storage(format!(
                "search range reader loaded {} documents, expected {expected_document_count}",
                documents.len()
            )));
        }
        Ok(Some(SearchPhysicalRangeRead {
            documents,
            range_count: report.range_count,
            bytes_read: report.bytes_read,
        }))
    }
}
