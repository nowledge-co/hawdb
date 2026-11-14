# Search projection selector version

Issue: `nowledge-co/hawdb#1763`.

The search projection selector (`search_projection_manifest.hawdb`) names the
active generation of a resident or out-of-core search projection and every
artifact in its closure. Readers decode it strictly
(`#[serde(deny_unknown_fields)]`), so a field written by a newer HawDB used to
fail an older reader with

```text
HawDBError::Storage("invalid search out-of-core manifest: unknown field ...")
```

Hosts treat `Storage` as a store fault and poison their runtime. The first such
field is `content_source_graph_commit_epoch`, written by a metadata-only epoch
advance ([`SEARCH_PROJECTION_EPOCH_ADVANCE.md`](SEARCH_PROJECTION_EPOCH_ADVANCE.md)).

## Format

The selector body has two optional fields:

| Field | Meaning |
| --- | --- |
| `min_reader_version` | The lowest selector version that understands every field present. |
| `format_version` | The selector version of the writer. Diagnostic only; it is never lower than `min_reader_version`. |

Both are omitted when `min_reader_version` is 1, and an absent field reads as
1. A version-1 selector is therefore byte-identical to one written before
#1763, and releases that predate the version fields keep reading it.

| Version | Adds |
| --- | --- |
| 1 | Every field written before #1763. |
| 2 | `content_source_graph_commit_epoch` (#1756), and the version fields themselves. |
| 3 | A segment's `descriptor_version` (#1746). It names a V4 segment descriptor, which can omit a field's value set. |

This reader supports version 3.

## Reading

A reader decodes the selector in two stages:

1. **Version gate.** The reader takes the body bytes from the envelope
   (`{"body":...,"checksum":N}`) and checks the envelope checksum over them.
   It then reads only `min_reader_version`, without `deny_unknown_fields`. If
   that version is above the one it supports, it returns
   `HawDBError::UnsupportedStorageFormat(StorageFormatError::UnsupportedVersion {
   format: SEARCH_PROJECTION_MANIFEST_FORMAT, found, supported })`. `found` is the
   selector's `min_reader_version`, saturated to `u16::MAX`. The message is
   `unsupported search projection manifest version 3; this reader supports
   version 2`.
2. **Strict decode.** Otherwise the reader decodes and validates the body
   exactly as before, with `deny_unknown_fields`.

The refusal applies only to an intact selector. A checksum mismatch, bytes that
are not a canonical envelope, or JSON that does not parse all fall through to
the strict decode, which keeps reporting them as `Storage`. So does an intact
selector whose versions are inconsistent: `min_reader_version` of 0, above the
reader's version, or above `format_version`. A supported version with an
unknown field also stays `Storage`: a writer that adds a field without raising
`min_reader_version` is broken.

Every selector read goes through this gate: `SearchOutOfCoreReader::open` and
refresh, `SearchIndex::open` and its alias repair, epoch advance, generation
publication, cleanup verification, Consumer candidate validation, and the
generation writer's active-selector discovery.

Writers never publish over a selector they refuse. A new-generation build, an
incremental update or a resident checkpoint whose directory holds a newer
selector fails with the same typed error, and the selector stays byte for byte
as the newer HawDB wrote it. A damaged selector is different: a standalone full
replacement may still rebuild over it, as before.

The selector gates its whole closure. The resident snapshot, segment
descriptors, layouts, lexical manifests, vector artifacts and mutation runs are
all reached through the selector, so a reader refuses before it opens any of
them.

The error is the same one HawDB returns for every other persistent format it
cannot read. A host tells the formats apart by comparing
`StorageFormatError::format()` with the constants exported from `hawdb` and
`hawdb_core`: `SEARCH_PROJECTION_MANIFEST_FORMAT` for this selector, and
`STORAGE_MANIFEST_FORMAT`, `RELATIONAL_DURABLE_CODEC_FORMAT`,
`RELATIONAL_SCHEMA_FORMAT`, `RELATIONAL_ROW_PAGE_FORMAT`, `APPEND_WAL_FORMAT`
and `LOGICAL_DUMP_FORMAT` for the graph, relational and dump formats. Do not
match the message text. Bindings already map it: the C ABI reports
`kind = "unsupported_storage_format"`, and Python raises `StorageError` with
`kind = "unsupported_storage_format"`, not `IntegrityError`. A host that sees
it should not poison, repair or delete the projection. It is intact; this
binary is too old for it. The host can upgrade HawDB, or rebuild the
projection into a new directory with this binary.

## Writing

Every writer stamps the selector from the fields present
(`SearchOutOfCoreManifestBody::stamp_reader_version`) before serializing it:

- `min_reader_version` is the highest version among the fields present. A
  field that is absent does not raise it.
- `format_version` is the writer's version when `min_reader_version` is above
  1, and 1 otherwise.

An epoch advance therefore writes version 2, because it writes
`content_source_graph_commit_epoch`. The next content publication, a resident
checkpoint or an out-of-core delta, drops that field and writes version 1
again. A host can downgrade after such a publication.

An out-of-core generation whose descriptor omits a value set writes version 3
(see [Segment value summaries](#segment-value-summaries)). The selector stays
at version 3 while any selected segment carries `descriptor_version`, so
publishing more content does not lower it.

## Bump policy

The selector `format` string (`HAWDB_SEARCH_OUT_OF_CORE_V4`) names the selector
family. Do not change it to evolve the selector: an older reader would report
the new string as an invalid header (`Storage`). Use the version fields.

When a change touches the selector or any artifact it reaches:

1. **New field.** Give it the next version, add it to the version table above
   and to `required_reader_version`, and raise `READER_VERSION`. Write it only
   when it carries information, with `skip_serializing_if` for its empty value,
   so selectors without it keep their lower version. The strict decode has no
   ignorable fields: if the field is present, `min_reader_version` must
   include its version, even if an older reader could ignore it safely.
2. **New meaning for an existing field or value.** Examples: a new enum variant
   in `vector_scoring`, a new `vector_index_binding` encoding, or a changed
   rule for which artifact a field names. Treat it like a new field. Raise
   `min_reader_version` whenever the selector uses the new meaning.
3. **Change to a referenced artifact.** This covers a new field in a layout,
   lexical manifest, mutation run or snapshot header, or a new artifact kind.
   Raise the selector's `min_reader_version` whenever the selector references
   such an artifact. Older readers then refuse at the selector, before they
   reach the artifact's own strict decode.
4. **No version change** is needed for a change that writes no new bytes:
   reader-side validation, query behavior, or a new writer that produces only
   fields an older reader already knows.

Raising `min_reader_version` does not make existing databases unreadable, so
it does not bump `PERSISTENT_FORMAT_VERSION`. A change that stops this reader
from reading selectors it used to read does bump it
(`crates/storage/src/persistent_format.rs`).

## Segment value summaries

Issue: `nowledge-co/hawdb#1746`.

Each segment descriptor entry keeps, per metadata field, the set of distinct
normalized values. The reader uses it to skip a segment for `Eq`, `In` and
`NotIn`. Every node property is also projection metadata, so before #1746 the
set for `content` (and other long text) held every document body, and an
out-of-core reader kept about one copy of the corpus text resident.

An out-of-core segment now keeps a field's set only within two bounds:

| Bound | Value |
| --- | --- |
| One normalized value | 256 bytes (`SEARCH_SEGMENT_VALUE_SUMMARY_MAX_VALUE_BYTES`) |
| Sum of distinct normalized values per field per segment | 8 KiB (`SEARCH_SEGMENT_VALUE_SUMMARY_MAX_FIELD_BYTES`) |

Above either bound the set is omitted for that segment. An omitted set proves
nothing, so `Eq`, `In` and `NotIn` on that field read the segment and filter
its rows. Presence counts and numeric and timestamp ranges are unaffected and
still skip segments. Truncated prefixes are not stored instead: a prefix of
per-document text is still one entry per document, so it would keep reader
memory proportional to the document count, and the sets serve only exact
equality, which is rare on body text.

The writer records an omitted set as `~` in the value column of a
`HAWDB_SEARCH_SEGMENTS_V4` descriptor, and the selector's segment entry carries
`descriptor_version: 4`. A descriptor without omitted sets is still written as
V3 with no `descriptor_version`, byte for byte as before. Readers accept `~`
only in a V4 descriptor, and refuse a segment whose `descriptor_version` does
not match its descriptor header.

A reader applies the same bounds while decoding a V1-V3 descriptor written
before #1746, without materializing the values it omits. Old projections
therefore open with bounded value summaries and need no rebuild. They still
read their larger descriptor file once at open.

Resident `SearchIndex` descriptors are unchanged: they keep complete value
sets, alongside the documents they already hold in memory.

## Limits

- The gate only helps readers that have it. HawDB 0.31.0 and 0.32.0 have no
  gate, and they fail any selector with a field they do not know as `Storage`.
  This includes every version-2 selector. Before downgrading to one of these
  releases, publish content after the last epoch advance (checkpoint or catch
  up), so that the selector is back at version 1.
- A version-3 selector names V4 descriptors, which HawDB releases before
  #1746 cannot read. They refuse the selector with the typed error above.
  Only a rebuild with the older binary returns such a projection to version 1.
- Selectors written by a development build between #1762 and #1763 can carry
  `content_source_graph_commit_epoch` without the version fields. This reader
  accepts them, and the next publication stamps the version.

## Tests

`out_of_core::manifest_version::tests` (`hawdb-search`, the existing unit lane)
covers:

- a version-3 selector with an unknown field, refused as the typed error by the
  decoder, `SearchOutOfCoreReader::open` and `SearchIndex::open`. The test
  asserts the error is not `Storage`, and that an oversized version saturates;
- damaged, non-canonical and version-inconsistent selectors, and a supported
  version with an unknown field, which all stay `Storage`;
- resident and out-of-core selectors without version fields, which still read
  and re-encode byte-identically;
- an epoch-advanced selector, which writes version 2, round-trips, reopens and
  chains, and which a version-1 reader with the gate refuses as the typed
  error;
- a resident checkpoint and an out-of-core delta after an advance, which write
  a version-1 selector without the version or content-epoch fields;
- a new-generation build, an incremental update from a reader opened earlier,
  and a resident checkpoint over a version-3 selector, which are all refused
  with the typed error and leave the selector and the directory unchanged.

Every refusal assertion also checks
`format() == SEARCH_PROJECTION_MANIFEST_FORMAT`.

Segment value summaries (#1746) are covered by
`out_of_core::tests::value_summary_bounds`,
`out_of_core::generation_writer::artifacts::descriptor::tests` and
`document_encoding::descriptor::decode::tests`:

- bodies above the bounds omit the set, while values at the exact value and
  field bounds are kept, including trimming and Unicode lowercase growth;
- an omitted set never skips a segment for `Eq`, `In` or `NotIn`, and filters
  on it return the same rows; kept sets and ranges still skip segments;
- a V4 generation writes version 3, a version-2 reader refuses it with the
  typed error, and a mismatched `descriptor_version` or a `~` outside V4 is a
  storage failure;
- a V3 descriptor with full bodies reopens with bounded summaries;
- reader-retained value bytes and admitted reader memory do not grow with the
  body length.

The C ABI (`bindings/ffi/src/error_tests.rs`) and Python
(`bindings/python/src/errors.rs`) tests pin the binding kinds for this error.

Local replay:
`cargo test --locked -p hawdb-search --lib manifest_version -- --test-threads=4`
and
`cargo test --locked -p hawdb-search --lib value_summar descriptor -- --test-threads=4`.
