# Per-Family Artifact Generations and Block-Framed Search Content Specification

## Status and scope

Status: **proposed design**. Nothing described here as "new" is implemented.
Tracking issue: [#1919](https://github.com/nowledge-co/hawdb/issues/1919), for
[#1641](https://github.com/nowledge-co/hawdb/issues/1641) (a
relational-only checkpoint re-encodes the whole unchanged graph) and
[#1917](https://github.com/nowledge-co/hawdb/issues/1917) (search hit hydration
decompresses and verifies the whole content segment). Related:
[#1521](https://github.com/nowledge-co/hawdb/issues/1521),
[#1593](https://github.com/nowledge-co/hawdb/issues/1593),
[#1440](https://github.com/nowledge-co/hawdb/issues/1440),
[#1844](https://github.com/nowledge-co/hawdb/issues/1844),
[#1902](https://github.com/nowledge-co/hawdb/issues/1902),
[#1903](https://github.com/nowledge-co/hawdb/issues/1903),
[#1904](https://github.com/nowledge-co/hawdb/issues/1904).

This specification defines one coordinated on-disk format round:

1. **Durable manifest v2.** Each durable data family records its own artifact
   generation, source commit epoch, family format version and artifact-set
   digest. A checkpoint rewrites only the families that changed. Every consumer
   resolves paths through the family references: loaders, recovery, scrub,
   backup and restore, closure and sealed-root publication, branch
   publication, reclamation, doctor and derived repair.
2. **Block-framed search content segments.** The search content payload gets
   independently compressed and checksummed blocks plus a block index, so hit
   hydration reads and verifies only the blocks it needs.

The two parts have separate version gates. The storage manifest gates part 1
and the search selector gates part 2. They are scheduled together so that
embedding hosts see a single format change (§10).

This document is exploratory until a maintainer accepts it. Its requirement
keywords are prospective acceptance criteria in the sense of
[`README.md`](README.md) "Exploratory proposals". On acceptance it becomes the
canonical contract for the manifest model, and [`../STORAGE.md`](../STORAGE.md)
must be updated in the same change that implements it.

### Goals

- G1. A checkpoint's write, hash and fsync cost is proportional to the families
  that changed, not to database size. The #1641 case (one DDL after an SF0.1
  graph import, 7.9–8.0 s) must not touch graph artifacts.
- G2. Integrity is no weaker than today. Every byte a reader trusts is still
  bound, through a chain of checksums, to the durable manifest that selected
  it. Generation stamps inside artifacts keep their role.
- G3. Reachability stays exact. Reclamation, branch closures and backups
  retain an artifact for as long as any retained manifest references it,
  however old its generation.
- G4. Future-proofing. New families, new family formats and newer manifests
  fail closed with the typed `UnsupportedStorageFormat` (#1903) before any
  family byte is interpreted.
- G5. Search hydration cost scales with the hits returned (O(k · block size)),
  not with segment or corpus size, with an explicit integrity contract.

### Non-goals

- Changing any family's internal encoding, except search content segments
  (§9). Canonical segments, adjacency blocks, descriptor pages and row pages
  keep their bytes.
- Removing the legacy `relational.<g>.hawdb` checkpoint (#1521). This spec
  makes it a carry-forward-capable family and leaves its removal to #1521.
- Moving search projections, projection generations, column-group shadows or
  vector projections into the durable manifest (see §4.2 for why).
- Incremental backups. The design enables them (§8.4) but does not specify
  them.

## 1. Terminology

- **Checkpoint generation (C-gen).** The number allocated by
  `next_checkpoint_generation` (`crates/storage/src/store/durable.rs:697-720`).
  It names `checkpoint.<g>.hawdb` and `wal.<g>.hawdb`. Today it equals
  `checkpoint_epoch`.
- **Checkpoint commit epoch (C).** `checkpoint_commit_epoch`: the commit epoch
  the checkpoint makes durable.
- **Family.** A set of artifacts written by one writer, read by one reader and
  bound by one manifest entry (§4).
- **Family generation (F-gen).** The C-gen of the checkpoint that wrote the
  family's root artifacts. It names those artifacts.
- **Family source epoch (E).** The commit epoch whose logical state the
  family's bytes represent.
- **Carry forward.** Publishing a manifest whose entry for a family names an
  F-gen older than the new C-gen, without reading, re-encoding or re-hashing
  that family's bytes.

## 2. Current model (today)

All paths are relative to the repository root, at `origin/main` `c5e7dea84`.

### 2.1 Durable manifest v1

- **Struct.** `DurableManifest` (`crates/storage/src/durable_manifest.rs:48-77`)
  is a flat struct:
  - checkpoint identity: `checkpoint_generation`, `checkpoint_epoch`,
    `checkpoint_commit_epoch`, `wal_generation`;
  - a len/CRC/SHA-256 triple for the checkpoint file, the canonical manifest,
    the property-spill manifest and the property-projection manifest;
  - structured bindings for canonical adjacency, relational rows, relational
    overflow, relational index and append tables.
- **Text encoding** (`durable_manifest.rs:1159-1439`):
  - a `HAWDB_MANIFEST_V1` header line, then a `version\thawdb-storage-v1` line;
  - one `key\tvalue` line per field, with `none` for absent values;
  - a `checksum\t<u64>` footer over the body.
- **Decoding:**
  - The header must be exactly V1 (`:813-817`).
  - `version` is checked by `validate_storage_version` (`:837`, `:1515-1535`).
    An intact `hawdb-storage-v<N>` other than v1 returns a typed
    `UnsupportedVersion`.
  - Unknown keys are rejected (`:1073-1077`). Every v1 key is required
    (`:1080-1142`).
- **Storage version.** `STORAGE_VERSION = "hawdb-storage-v1"` and
  `STORAGE_VERSION_REVISION = 1` (`:1454`, `:1509`). It has never been bumped.
- **Validation ties every family to the checkpoint** (`validate`, `:562-798`):
  - The adjacency binding must have `generation == checkpoint_epoch` and
    `source_commit_epoch == checkpoint_commit_epoch` (`:583-595`).
  - The same holds for relational row, overflow and append (`:662-715`) and
    for relational index (`:716-738`).
  - `wal_generation == checkpoint_epoch` (`:739-744`) and
    `checkpoint_generation == checkpoint_epoch` (`:745-752`).
  - Canonical, property spill and property projection have **no generation
    field at all**. Their files are always named from `checkpoint_generation`
    (`:469-512`).
- **Writer.** `checkpoint_manifest` (`crates/storage/src/store/durable/checkpoint.rs:303-378`)
  builds the manifest with `checkpoint_generation: Some(generation)`,
  `wal_generation: generation` and `checkpoint_epoch: generation`
  (`:339`, `:365-366`). Every family binding comes from the same preparation.

### 2.2 Path, generation and epoch derivation per family

| Family (today's name) | Files | Generation used for the path | Source-epoch check |
| --- | --- | --- | --- |
| Checkpoint (catalog, statistics, search change log) | `checkpoint.<g>.hawdb` | `checkpoint_generation` (`durable_manifest.rs:408-412`) | file `generation` and `commit_epoch` lines must equal the manifest (`store/graph_recovery.rs:369-485`) |
| Legacy relational checkpoint | `relational.<g>.hawdb` | `checkpoint_epoch` (`store/graph_recovery.rs:489`, `store/durable/scrub.rs:122-140`) | len/CRC/SHA bound inside the checkpoint file (`crates/storage/src/checkpoint.rs:208-219`) |
| Canonical segments | `canonical.<g>.hawdb`, `canonical.<g>.manifest.hawdb`, `canonical-segment-descriptors-<g>.{pages,root}.hawdb` | `checkpoint_generation` (`store/durable/artifacts.rs:513-518`, `:564`) | `canonical_manifest.source_commit_epoch == checkpoint_commit_epoch` (`artifacts.rs:541-546`) |
| Property spill (written by the canonical writer) | `properties.<g>.hawdb`, `.manifest`, `property-spill-descriptors-<g>.*` | `checkpoint_generation` (`artifacts.rs:597-600`, `:628-648`) | equality (`artifacts.rs:622-627`) |
| Property projection | `property-index.<g>.hawdb`, `.manifest`, `property-index-descriptors-<g>.*` | `checkpoint_generation` (`artifacts.rs:673-678`, `:714-728`) | equality (`artifacts.rs:697-704`) |
| Canonical adjacency | `adjacency.<g>.hawdb`, `adjacency-descriptors-<g>.{pages,root}.hawdb` | `checkpoint_generation`, and `binding.generation` must equal it (`artifacts.rs:749-759`) | the binding's epoch must equal the checkpoint's (`durable_manifest.rs:583-595`) |
| Relational row pages | `relational-row-pages-<g>.{pages,manifest}.hawdb`, `relational-row-root-<g>.{descriptors,keys}.hawdb` | `binding.generation` (`durable_manifest.rs:513-524`); reused older page files via `physical_generations` (`crates/storage/src/relational/row_page/publication.rs:212-214`) | `manifest.generation == checkpoint_epoch` (`store/relational_row_pages.rs:453-467`) |
| Relational overflow | `relational-overflow-<g>.{extents,manifest}.hawdb`, `relational-overflow-root-<g>.descriptors.hawdb` | `binding.generation`; older extents via descriptor `physical_generation` (`store/durable/backup.rs:189-219`) | equality (`durable_manifest.rs:662-715`) |
| Relational index shadow | `relational-index-shadow-<g>.{pages,manifest}.hawdb` | `binding.generation` | equality (`durable_manifest.rs:716-738`); an older index is classified `Stale` (`store/relational_index_shadow.rs:694-770`) |
| Append tables | `append-<g>.{segment,manifest}.hawdb` | `binding.generation`; segments carry their own generation (`store/durable/backup.rs:279-295`) | equality (`durable_manifest.rs:662-715`) |
| WAL | `wal.<g>.hawdb` | `wal_generation == checkpoint_epoch` (`durable_manifest.rs:739-744`) | n/a |

Name construction lives in `crates/storage/src/artifact_files.rs:26-62` and the
family modules (`canonical.rs:181,185`; `canonical_adjacency.rs:62,66`;
`property_spill.rs:58,62`; `property_projection.rs:65,69`;
`relational/row_page/publication.rs:53-65`;
`relational/overflow/publication.rs:41-49`; `relational/index_shadow.rs:93,97`;
`append_table/publication.rs:44,48`).

Names are parsed back by `storage_generation_for_file`
(`artifact_files.rs:180-205`).

### 2.3 Integrity bindings and generation stamps

Each graph family writes its generation (and often its source epoch) into its
own bytes as an integrity binding. Readers reject a mismatch.

| Artifact | Stamp written | Checked against |
| --- | --- | --- |
| Canonical segment header | generation (`crates/storage/src/canonical/checkpoint_flush.rs:139`) | canonical manifest generation (`canonical.rs:3500-3520`) |
| Canonical artifact header | generation | manifest generation (`canonical.rs:1715`) |
| Adjacency block header | generation (`canonical_adjacency.rs:1518-1526`) | descriptor (`canonical_adjacency.rs:2537`); artifact header vs binding (`:1822`, `:2428`) |
| Graph descriptor page | `physical_generation`, `source_commit_epoch` (`graph_descriptor_page.rs:228-229`) | reference and expected epoch (`graph_descriptor_page.rs:387-396`) |
| Graph descriptor root | generation, source epoch (`graph_descriptor_tree.rs:150-151`) | binding (`graph_descriptor_tree.rs:607-619`); adjacency root vs binding (`canonical_adjacency.rs:1879-1880`) |
| Property spill artifact and block | generation (`property_spill.rs:592`, `:843`) | `property_spill.rs:1375`; canonical cross-check (`canonical.rs:1672-1673`) |
| Property projection block | generation (`property_projection.rs:1879`) | manifest generation and source epoch (`store/durable/artifacts.rs:697-704`) |
| Row-page manifest | gen, sce, previous gen (`relational/row_page/publication/manifest.rs:135-137`) | binding (`store/format_admission.rs:90-108`) |
| Append manifest and segment | gen, sce (`append_table/publication/manifest_encoding.rs:180-181`; `append_table/segment.rs:327-328`) | binding (`store/format_admission.rs:109-134`) |

The #1641 measurement shows the effect of these stamps. Between two
checkpoints with no graph change, the adjacency file differs in about 1.1 M
bytes and the descriptor pages in about 4.4 M bytes, all of them generation
stamps.

### 2.4 Exhaustive consumers of `checkpoint_generation` and "checkpoint epoch as generation"

Every site below assumes that a family's generation equals the checkpoint
generation. Each must resolve through family references in v2. "Mixed" marks a
site that already combines `binding.generation` with the checkpoint generation
for files of the same family. Those sites are latent bugs once the two values
can differ.

| Area | Sites |
| --- | --- |
| Manifest codec/validation | `durable_manifest.rs:50`, `:408-412` (`checkpoint_path`), `:426-560` (`manifest_artifact_inputs_with_work`; canonical/spill/projection from checkpoint gen at `:469-512`), `:562-798` (`validate`), `:838-840`, `:1082`, `:1164` |
| Loaders | `store/durable/artifacts.rs:499-581` (canonical), `:583-657` (spill), `:659-737` (projection), `:739-807` (adjacency); `store/durable.rs:520-600` (open), `:587` (source-scan cache key `ManifestGeneration(checkpoint_epoch)`); `store/durable/checkpoint.rs:424-520` (`adopt_checkpoint_manifest`) |
| Checkpoint writers | `store/graph_checkpoint.rs:959-971` (one `generation` for all writers), `:1038-1088` (adjacency, projection), `:1093-1113` (relational checkpoint), `:1144-1290` (overflow and row), `:1345` (index), `:1351-1375` (checkpoint file, WAL); `store/graph_checkpoint/canonical_preparation.rs:51-77` (canonical and spill paths); `store/graph_checkpoint.rs:543` (overflow compaction) |
| Publication / source identity | `store/graph_checkpoint.rs:1388`, `:1542-1558`, `:1604-1610`; `store/graph_checkpoint_candidate.rs:59-80`, `:292-302`, `:378-392`, `:671-679`; `store/graph_checkpoint_candidate/reclamation.rs:50-63` |
| Recovery | `store/graph_recovery.rs:355-372`, `:463-492` (checkpoint-file generation equality, `relational.<g>` path), `:707-733`, `:785` (WAL cursor generation); `store/relational_row_pages.rs:453-467`; `store/relational_index_shadow.rs:694-770`; `relational/index_shadow.rs:638-709`; `relational/row_page/recovery.rs:55` |
| Scrub | `store/durable/scrub.rs:102-120` (checkpoint), `:122-140` (relational checkpoint via `checkpoint_epoch`), `:318-340` (canonical; requires decoded `artifact.generation == generation`), `:480-495` (spill), `:567-569` (projection); adjacency already uses `binding.generation` (`:403-470`) |
| Closure plan | `store/durable/backup.rs:59-66` (requires `checkpoint_generation`), `:129-142` (canonical), `:143-156` (adjacency pages from checkpoint gen: **mixed**), `:157-188` (spill, projection) |
| Backup | `store/durable/backup.rs:299-555` (`generation = self.checkpoint_epoch`; adjacency `:431-447` **mixed**); `crates/storage/src/backup.rs:245-275` (`BackupManifest` records one `generation`) |
| Restore validation | `store/backup.rs:80-96` (requires `checkpoint_generation == checkpoint_epoch == wal_generation == g`), `:189-229` (canonical), `:314-371` (adjacency **mixed**), `:414-485` (projection), `:648-688` (spill) |
| Reclamation | `store/durable/reclamation.rs:76-97` (`obsolete_generation_bytes`: anything older than `checkpoint_epoch - 1`), `:197-286` (`try_reclaim_old_generations`: retained = pins ∪ {current, previous}), `:396-473` (physical-generation expansion keyed by checkpoint gen); pins from `src/api/mod.rs:983-989` |
| Abandoned-future cleanup | `artifact_files.rs:214-244`, called with `checkpoint_epoch` (`store.rs:1993-1996`) |
| Doctor | `store/doctor.rs:874-896` (`validate_checkpoint_boundary`); sealed root vs manifest epoch (`store/doctor.rs:150`) |
| Format admission | `store/format_admission.rs:32-34` (checkpoint), relational checkpoint by checkpoint gen, `:90-134` (row and append via binding); `validate_sealed_formats` resolves the same names through `root.checkpoint_bindings` (`:138-166`) |
| Derived repair | `store/derived_repair.rs:115-130`, `:166-185`, `:230`, `:285`, `:376`, `:461-462`; `store/derived_repair/audit.rs:414-436` (`target_files(kind, generation)`); mount tolerance `store/immutable_root.rs:2854`, `:2894` |
| Branch runtime | `store/immutable_root.rs:83`, `:401`, `:1536`, `:1671`, `:1868` (`checkpoint_generation: durable.checkpoint_epoch`, used only for the sealed-WAL interval cap); `:2525-2545` (unchanged-manifest seal shortcut) |
| Readiness | `crates/readiness` has no reference to either name (verified by grep). Readiness consumes doctor and format-admission results only. |

### 2.5 Precedents the design builds on

- **Relational families already carry older bytes forward.**
  - A row-page root lists the `physical_generations` it references
    (`relational/row_page/publication.rs:212-214`).
  - Overflow descriptors carry a `physical_generation`.
  - The closure, backup, scrub and reclamation code already enumerates those
    older files through validated readers:
    - closure: `store/durable/backup.rs:189-264`;
    - reclamation: `store/durable/reclamation.rs:396-473`;
    - scrub: `store/durable/scrub.rs:668-713`.

  v2 extends this rule, "a newer root references older immutable files",
  from data pages to whole families.
- **The object layer is content-addressed.**
  - A sealed root binds `{relative_path, reference}` pairs
    (`crates/storage/src/sealed_root.rs:41-62`).
  - Objects live at `objects/<kind>/<sha-derived path>`
    (`immutable_object.rs:525-530`). Identical bytes publish as
    `PublishOutcome::Reused` (`immutable_object.rs:1524-1576`).
  - Object reclamation marks reachability from retained roots, not from
    generation numbers (`immutable_object.rs:722-830`;
    `branch_reclamation.rs:15-40`).
  - A binding such as `canonical.5.hawdb` inside a generation-9 root is
    therefore already legal.
- **The branching spec already requires closure-based reachability:**
  - "Single-directory generation arithmetic MUST NOT authorize deletion of
    shared branch objects" (`docs/specs/BRANCHING_STORAGE_SPEC.md:89-90`).
  - "Unreachable means proven absent from the complete current closure, not
    simply older than the active generation" (`:1676-1677`).
- **Search already has independent generations.** The out-of-core selector
  has its own `generation`, per-segment `generation`, `source_graph_commit_epoch`
  and `format_version`/`min_reader_version`, and refuses newer selectors with a
  typed error (`crates/search/src/out_of_core.rs:656-752`;
  `crates/search/src/out_of_core/manifest_version.rs:23-80`).
- **`graph_change_commit_epoch` (#1844)**
  - It advances when a commit's WAL record `changes_graph()`
    (`crates/storage/src/wal.rs:578-610`). The commit path is
    `store/graph_commit.rs:2496-2498`, with
    `store/relational_index_shadow.rs:674-676` for non-relational commits, and
    WAL replay is `store/graph_recovery.rs:1116-1118`.
  - It is not persisted. On open it is initialized to the checkpoint commit
    epoch (`store/graph_recovery.rs:421-426`).

### 2.6 Search content segments today

- **Payload file and descriptor segments.**
  - The payload file is `search_projection_segment_payloads.<g>.hawdb`
    (`crates/search/src/out_of_core/generation_writer/publication.rs:136-141`).
  - It holds back-to-back *descriptor segments* of up to 128 documents
    (`crates/search/src/lib.rs:357-360`). A segment is flushed early before
    exceeding `max_segment_uncompressed_bytes`
    (`out_of_core/generation_writer/artifacts.rs:275-281`).
- **Envelope.** Each segment is one `HAWDB_COMPRESSED_V1` envelope
  (`lib.rs:338`, `lib.rs:9122-9133`):
  - a text header with uncompressed and compressed CRC32C and lengths;
  - **one zstd stream** at level 3 (`lib.rs:339`).
  - The writer compresses into the tail and then shifts bytes to insert the
    header (`out_of_core/generation_writer/artifacts/encoding/file.rs:27-120`).
- **Body.**
  - The decompressed body is `HAWDB_SEARCH_SEGMENT_V1` followed by one `doc\t…`
    line per document in id order (`document_encoding/segment.rs:51-53,93-97`).
  - Each line includes a decimal-text embedding (`document_encoding.rs:154-201`).
  - There is no intra-segment offset index.
- **Descriptor.** The descriptor (`HAWDB_SEARCH_SEGMENTS_V3`,
  `document_encoding/descriptor.rs:123,171-178`) records for each segment
  `first/last_document_id`, `document_count` and a payload range
  `{offset, length, checksum}`. The checksum is a CRC32C over the whole
  envelope (`encoding/file.rs:105-115`).
- **Hydration pipeline.** Hydration of a hit
  (`out_of_core/hydration.rs:194-367`):
  - finds the segment by id range (`out_of_core.rs:4121-4184`);
  - streams the whole envelope through a range CRC, a compressed CRC, zstd and
    an inflated CRC;
  - fully decodes only the requested lines (`hydration.rs:421-507`);
  - drains the rest, because "Results remain private until the entire segment
    has passed integrity" (`hydration.rs:361-362`).
- **Contract and cost.**
  - The contract text is
    [`../STREAMING_SEARCH_HYDRATION.md`](../STREAMING_SEARCH_HYDRATION.md)
    lines 18-30.
  - #1917 measures 88–118 ms of hydration on BEIR SciFact and NFCorpus against
    1–13 ms of ranking.
  - *Inference:* SciFact (≈5.2 k documents, ≈41 segments) touches most
    segments for a top-100 query, so each query inflates close to the whole
    corpus, embeddings included.
- **No cache.** There is no cache for verified search content
  (`../SEARCH_MUTATION_SEGMENTS.md:226-241`).

## 3. Design summary

| Decision | Choice | Why |
| --- | --- | --- |
| Unit of reuse | A **family**: root artifacts plus all their descendants, under one manifest entry | Matches the existing writer/reader/binding boundaries (§2.2) and the closure families (`checkpoint_closure.rs:113-134`) |
| Paths | Still **derived** from `(family, F-gen)` with today's naming functions; not stored as strings | No renames, so v1 databases map one-to-one. It keeps one source of truth for names (closure bindings, backup whitelist, reclamation parser), and `BRANCHING_STORAGE_SPEC.md:936-946` already requires paths "from validated generation bindings" |
| F-gen allocation | Same monotonic counter as C-gen (F-gen = C-gen of the writing checkpoint) | Names never collide across families. Abandoned-future cleanup (`artifact_files.rs:214-244`) stays correct because every live F-gen ≤ C-gen |
| Stamps in bytes | Unchanged; they bind to the **F-gen** and the family source epoch | Carry-forward needs zero byte changes and integrity is unchanged (§6) |
| Proof of "no mutation" | Per-family mutation epochs fed by an exhaustive `WalOp` classification and fail-safe fences for non-WAL replacement | Compile-time exhaustiveness and a conservative default (§5.5) |
| Manifest encoding | Same line-oriented text with a checksum footer; repeated `family` records | Small, diffable, same codec infrastructure. v1 readers reach the `version` line first and refuse with a typed error (§5.3) |
| Search | Stays outside the durable manifest; block-framed content segments are gated by the search selector's `min_reader_version` | Search publication has its own commit point and consumer protocol (§4.2) |
| Old databases | v2 readers adopt v1 manifests in memory; the first v2 checkpoint writes v2 | v1 is exactly the v2 model with every F-gen equal to C-gen, so this is a reinterpretation, not a data migration (§10) |

## 4. Family model

### 4.1 Durable families (in the manifest)

Each family has a stable numeric tag (used in `UnsupportedFeature.tag`), a
stable name, a format identifier exported from `hawdb_core` and recorded in
`docs/persistent-format.lock`, and an initial v2 format version.

| Tag | Name | Artifacts | Initial format version | Depends on | Mutated by |
| --- | --- | --- | --- | --- | --- |
| 1 | `graph.canonical` | canonical artifact, manifest, segment descriptors; **property spill** artifact, manifest and descriptors (one writer, `canonical_preparation.rs:51-77`) | 1 | none | node/relationship create, delete and property set; `ProjectGraph`; non-WAL graph replacement |
| 2 | `graph.adjacency` | adjacency artifact, descriptor pages and root | 1 | `graph.canonical` (relationship count equality, `store/durable/checkpoint.rs:503-511`) | relationship create/delete; node delete that removes relationships; non-WAL graph replacement. Adjacency stores topology only (`canonical_adjacency.rs:221-249`), so **property sets do not mutate it** |
| 3 | `graph.property_projection` | projection artifact, manifest and descriptors | 1 | `graph.canonical` | node and relationship property set; label/type-relevant create and delete; index DDL (`CreateIndex`, `CreateCompositeIndex`, `CreateRangeIndex`, `CreateFullTextIndex`, drops); non-WAL graph replacement |
| 4 | `relational.checkpoint` | `relational.<g>.hawdb` (legacy full-row file, while it exists; #1521) | 6 (`RELATIONAL_DURABLE_CODEC_FORMAT` codec v6, `relational/codec.rs:72-74`) | none | any `Relational`/`RelationalSnapshot` op; relational compaction |
| 5 | `relational.row` | row root, descriptors, keys, manifest, page files (with older physical generations) | 5 (`ROW_PAGE_FORMAT_VERSION`, `relational/row_page/publication/manifest.rs:36`) | `relational.overflow` | relational row and schema ops; row compaction |
| 6 | `relational.overflow` | overflow root, descriptors, manifest, extents | 1 (`SKOVRM01`) | none | relational ops that write large values; overflow compaction |
| 7 | `relational.index` | index-shadow pages and manifest | 2 (`SKRIDXM1` v2) | `relational.row` | relational ops on indexed tables; index DDL |
| 8 | `append` | append manifest and segments | 2 (`SKAPMAN1` v2) | none | `Append` ops |

The checkpoint file (`checkpoint.<g>.hawdb`) and the WAL are not families. The
checkpoint file holds the catalog, statistics, next ids, the search change log
and `projected_graphs` metadata (`crates/storage/src/checkpoint.rs:47-60,
180-260`). It is small and every checkpoint rewrites it, so the checkpoint
record itself always names the current C-gen.

**Rule F1.** A family's format version is the version of the family layout as
a whole. It MUST be bumped whenever any artifact in the family changes in a way
a previous reader cannot decode. Internal header magics and versions remain as
a second, defensive check, and #1904 makes their newer-version refusals typed.

**Rule F2.** A family name and tag are never reused for a different layout.
Retiring a family (for example `relational.checkpoint` after #1521) removes its
entry; an absent entry means "the family is empty", as today's `none`
bindings do.

### 4.2 Families outside the durable manifest

These keep their own selectors and are **not** moved into the manifest:

- **Search out-of-core projections.** Selector
  `search_projection.out_of_core.manifest.hawdb` (`crates/search/src/out_of_core.rs:379`):
  - It has its own generation, `source_graph_commit_epoch`, rename-based commit
    point and min-reader gate.
  - Logical dumps treat it as rebuildable derived state
    (`docs/LOGICAL_DUMP.md:384-387`).
  - Folding it into the storage checkpoint would couple search publication to
    storage checkpoints, which the Consumer protocol avoids
    (`docs/CONSUMER_STATIONARY_PUBLICATION.md`).
  - §9 changes its content-segment format under its own gate.
- **Projection generations** (`projection-generations/`, own head and manifest
  protocol, `crates/storage/src/projection_generation.rs:15-21`) and
  **column-group shadows** (`store/graph_columnar_shadow.rs:866`, published
  after the checkpoint, `store/graph_checkpoint.rs:1594`).
- **Fixed-name sidecars.** The source scan (bound by `source_scan_commit_epoch`
  and descriptor checksum, `store/durable/artifacts.rs:122-144`), the
  projected-graph artifact file and `stable_ids.hawdb`. These are not
  generation-named today.
  - v2 records the source-scan epoch as before, and the source scan may be
    carried forward together with `graph.canonical` (§7.2).
  - Making the projected-graph file a family is listed as an open question
    (§14, Q7).

Rule F3 covers every one of these: an external family MUST NOT rely on durable
manifest reclamation to retain its files.

## 5. Durable manifest v2

### 5.1 Encoding

The text format, checksum footer and atomic replace (`durable_manifest.rs:1441-1451`)
are unchanged. Example:

```text
HAWDB_MANIFEST_V1
version	hawdb-storage-v2
checkpoint_generation	9
checkpoint_epoch	9
checkpoint_commit_epoch	1234
wal_generation	9
checkpoint_encoded_len	…
checkpoint_encoded_checksum	…
checkpoint_encoded_sha256	…
graph_change_commit_epoch	1100
oldest_reader_commit_epoch	none
safe_reclaim_commit_epoch	1234
wal_replay_start_lsn	…
next_lsn	…
source_scan_commit_epoch	1100
source_scan_descriptor_checksum	…
family	1	graph.canonical	format=1	generation=5	source_commit_epoch=1100	artifact_set_sha256=…
binding	1	manifest	len=…	crc32c=…	sha256=…
binding	1	spill_manifest	len=…	crc32c=…	sha256=…
family	2	graph.adjacency	format=1	generation=5	source_commit_epoch=1100	artifact_set_sha256=…
attr	2	relationship_count	1477965
attr	2	entry_count	2955930
binding	2	artifact	len=…	crc32c=…	sha256=…
binding	2	descriptor_root	len=…	crc32c=…	sha256=…
family	3	graph.property_projection	format=1	generation=5	source_commit_epoch=1100	artifact_set_sha256=…
binding	3	manifest	len=…	crc32c=…	sha256=…
family	5	relational.row	format=5	generation=9	source_commit_epoch=1234	artifact_set_sha256=…
attr	5	root_set_sha256	…
binding	5	manifest	len=…	crc32c=…	sha256=…
…
checksum	…
```

**Encoding rules:**

- E1. The first two lines MUST be `HAWDB_MANIFEST_V1` and
  `version\thawdb-storage-v2`.
  - The header is deliberately kept: a v1 reader checks the header first
    (`durable_manifest.rs:813-817`) and then meets `version` as the first
    field, which yields
    `UnsupportedVersion{format: STORAGE_MANIFEST_FORMAT, found: 2, supported: 1}`
    (`:837`, `:1515-1531`).
  - Changing the header would instead produce an untyped "missing the V1
    format header" error in every released reader.
- E2. Scalar keys appear once. `family` lines appear in ascending tag order,
  with at most one line per tag. `binding` and `attr` lines follow their
  `family` line, with names sorted. Any other ordering, a duplicate, or a
  `binding` or `attr` without a preceding `family` of the same tag is
  malformed (`StorageIntegrity`).
- E3. Each family declares a fixed schema of required binding names and
  attribute names. A missing required name is malformed. An unknown binding or
  attribute name inside a **known** family at a **supported** format version
  is malformed, because a newer layout must bump the family format.
- E4. All numbers are decimal `u64` and digests are lowercase hex SHA-256, as
  in v1.

### 5.2 Fields

**Scalar fields:**

- Kept from v1 with the same meaning: `checkpoint_generation`,
  `checkpoint_epoch`, `checkpoint_commit_epoch`, `wal_generation`, the
  checkpoint len/CRC/SHA-256 triple, `oldest_reader_commit_epoch`,
  `safe_reclaim_commit_epoch`, `wal_replay_start_lsn`, `next_lsn`, and the
  source-scan pair.
- New: `graph_change_commit_epoch` (§5.6).
- Removed: the flat canonical, spill and projection triples and every
  per-family `*_generation`/`*_source_commit_epoch` key. They move into
  family entries.

**Family entry** (`FamilyEntry`):

| Field | Type | Meaning |
| --- | --- | --- |
| `tag`, `name` | u16, string | Family identity (§4.1). Name and tag must agree. |
| `format` | u16 | Family format version (F1) |
| `generation` | u64 | F-gen; names the family root artifacts |
| `source_commit_epoch` | u64 | E: the commit epoch the bytes represent |
| `artifact_set_sha256` | SHA-256 | Family identity digest (below) |
| `binding <name>` | len, CRC32C, SHA-256 | Root artifacts bound directly by the manifest. The names are today's: canonical `manifest`, `spill_manifest`; adjacency `artifact`, `descriptor_root`; projection `manifest`; row, overflow, append `manifest`; index `page`, `manifest`; relational checkpoint `file` |
| `attr <name>` | u64 or digest | Family-specific scalars that v1 already stores: adjacency `relationship_count`, `entry_count`; row, overflow, append `root_set_sha256`; index `catalog_schema_sha256`, `root_set_sha256` |

**`artifact_set_sha256`** is SHA-256 over a canonical byte encoding of
`(tag, format, generation, source_commit_epoch, sorted [(binding name, len, sha256)], sorted [(attr name, value)])`.

- Roles:
  - (a) It is the cheap equality key that incremental closure (§8.2), backup
    (§8.4) and diagnostics use to decide "same family instance".
  - (b) It is recomputed on decode, so a hand-edited or bit-flipped entry that
    still passes the 64-bit footer checksum is rejected.
- Descendants (data pages, segment files, older physical generations) are not
  hashed into it. The root artifacts bind them transitively, as today.
- It is computed from manifest data only, so producing it costs nothing at
  checkpoint time.

### 5.3 Versioning and forward compatibility

- V1. `STORAGE_VERSION` becomes `hawdb-storage-v2` and
  `STORAGE_VERSION_REVISION = 2`. Readers accept revisions 1 and 2 (§10.1).
  Any other intact `hawdb-storage-v<N>` is a typed `UnsupportedVersion`. The
  checkpoint file's own `version` line (`crates/storage/src/checkpoint.rs:205`,
  `:783`) follows the same rule.
- V2. An unknown family tag is
  `UnsupportedFeature{format: STORAGE_MANIFEST_FORMAT, feature: "storage family", tag}`.
  - Readers MUST NOT ignore unknown families. Reclamation could not know such
    a family's files and would delete them (§8.1).
  - "Ignorable" families are deferred (§14, Q5).
- V3. A family `format` above the reader's supported version is
  `UnsupportedVersion{format: <family format id>, found, supported}`.
  - It is raised during manifest decode, before any family file is opened,
    and before WAL or repair decisions, consistent with
    `store/format_admission.rs:23-136`.
  - New format ids are added to `crates/core/src/error/storage_format.rs:22-44`:
    `GRAPH_CANONICAL_FORMAT`, `GRAPH_ADJACENCY_FORMAT` and
    `GRAPH_PROPERTY_PROJECTION_FORMAT`. The relational families reuse
    `RELATIONAL_ROW_PAGE_FORMAT` and `RELATIONAL_DURABLE_CODEC_FORMAT`.
  - New ids `RELATIONAL_OVERFLOW_FORMAT`, `RELATIONAL_INDEX_FORMAT` and
    `APPEND_TABLE_FORMAT` are added. The append family keeps `APPEND_WAL_FORMAT`
    for the WAL codec.
- V4. A reader MAY support several versions of one family (row pages already
  read v2–v5, `docs/STORAGE.md:90-94`). The writer writes the newest version
  only when it rewrites the family. A carried-forward family keeps its
  recorded version (§10.2).

### 5.4 Invariants

Writers MUST establish, and `DurableManifest::validate` MUST check, the
following. `C` is `checkpoint_commit_epoch` and `G` is `checkpoint_generation`.

- M1. `checkpoint_generation == checkpoint_epoch == wal_generation == G`, as
  today.
- M2. For every family entry: `0 < generation ≤ G` and
  `source_commit_epoch ≤ C`.
- M3. `generation == G` ⇔ the family was written by this checkpoint. For an
  entry with `generation < G`, the entry MUST be byte-identical (all fields)
  to the entry for the same tag in the manifest that published generation
  `generation`, and in every manifest since.
  - The writer guarantees this by copying the previous manifest's entry
    verbatim (§7).
  - Readers cannot check it without history. It is a writer obligation that
    power-loss and fuzz tests assert (§12).
- M4 (validity). An entry `(F, G_F, E)` is **valid for checkpoint epoch C** if
  and only if no committed mutation of family F has a commit epoch in `(E, C]`.
  "Mutation of F" is defined by the classification in §5.5.
- M5. Dependencies hold at the logical level. If F depends on D (§4.1), both
  entries describe the same logical state at C (both valid for C by M4). The
  cross-family checks that exist today remain mandatory:
  - canonical vs adjacency relationship count
    (`store/durable/checkpoint.rs:503-511`);
  - the row root's overflow root (`store/durable/artifacts.rs:476-491`).

  When the source epochs differ, a check MUST NOT require E_F == E_D.
- M6. Required families are unchanged from v1: `relational.row` and
  `relational.overflow` are required once a checkpoint exists
  (`durable_manifest.rs:761-768`). `graph.canonical` ⇔ `graph.adjacency` are
  present together (`:616-629`). `graph.property_projection` and the spill
  bindings require `graph.canonical` (`:639-661`).
- M7. `graph_change_commit_epoch ≤ C`. It is ≥ the maximum `source_commit_epoch`
  of the graph families when those were written by this checkpoint. More
  generally, it is an upper bound of the last graph-changing commit ≤ C (§5.6).
- M8. With no checkpoint (`checkpoint_generation = none`) there are no family
  entries, as in `durable_manifest.rs:770-795`.

### 5.5 Proving "no mutation" (M4)

The proof is inductive over published manifests. All state is in memory in the
`GraphStore` that prepares the checkpoint.

1. **Classification.** A new `WalOp::family_effects(&self) -> FamilySet`, next
   to `changes_graph` (`crates/storage/src/wal.rs:578-610`), is an
   **exhaustive** `match` with no wildcard arm. Adding a `WalOp` variant
   therefore fails to compile until its family effects are declared. The
   classification is conservative: an op that might affect a family's bytes
   is listed for that family. `Batch` is the union of its members.
2. **Mutation epochs.** `GraphStore` keeps `family_mutation_epoch: [u64; N]`.
   - Live commits set `family_mutation_epoch[F] = next_commit_epoch` for every
     F in the commit's `family_effects`, at the same point where
     `graph_change_commit_epoch` advances (`store/graph_commit.rs:2496-2498`,
     `store/relational_index_shadow.rs:674-676`).
   - WAL replay does the same (`store/graph_recovery.rs:1116-1118`).
3. **Initialization on open.** After the manifest is adopted,
   `family_mutation_epoch[F] = entry(F).source_commit_epoch`. This is sound by
   M4 applied to the adopted manifest: no mutation of F lies in `(E_F, C]`, so
   "last mutation ≤ E_F" holds. Replay then advances the epochs for the WAL
   suffix after C.
4. **Fences for non-WAL replacement.** A path that installs family state
   without a classified `WalOp` MUST call `force_family_rewrite(F)`. Examples:
   - initial or bulk import that replaces the base;
   - snapshot replacement;
   - derived repair;
   - relational row or overflow compaction (already a new row/overflow
     generation by construction);
   - restore;
   - branch creation or merge onto an existing store;
   - an in-memory store promoted to durable;
   - a change of a build configuration that alters a family's bytes, such as
     `DerivedArtifactBuildConfig` (`store/graph_checkpoint.rs:729-737`).

   Fail-safe rule: `family_rewrite_required` is `true` by default for every
   family on a `GraphStore` constructed by any path other than "adopt a
   published manifest" or "publish a checkpoint". A forgotten fence therefore
   costs performance, never correctness.
5. **Decision.** At checkpoint preparation from source commit epoch C, family
   F is carried forward if and only if all of the following hold:
   - `!family_rewrite_required[F]`;
   - `family_mutation_epoch[F] ≤ previous_entry(F).source_commit_epoch`;
   - the reader supports `previous_entry(F).format` (always true, since it was
     opened);
   - no forced-upgrade policy applies (§10.2);
   - every family F depends on is either carried forward too, or rebuilt in a
     way F does not consume (§7.2).
6. **Publication recheck.** Publication already refuses when the source moved
   (`store/graph_checkpoint.rs:1542-1558`). The carry-forward decision is
   captured in `PreparedCheckpoint` together with the mutation epochs it used.
   Publication MUST recheck that the epochs are unchanged, which already
   follows from the commit-epoch equality.

`changes_graph()` stays as defined by #1844. Each family's
`family_mutation_epoch` is finer than `changes_graph()`. For example,
`CreateNodeLabel` changes the graph catalog, which lives in the always-written
checkpoint file, but changes no family bytes.

### 5.6 Interaction with `graph_change_commit_epoch` (#1844)

v2 persists `graph_change_commit_epoch` in the manifest. On open,
`store/graph_recovery.rs:421-426` initializes the runtime value from the
manifest instead of from `checkpoint_commit_epoch`, and replay advances it as
today.

Why the field is persisted:

- Without it, carried-forward graph families would leave no durable record of
  the last graph change. A catalog-only graph op (for example `CreateNodeLabel`)
  in `(E_graph, C]` changes no family, so a value derived from family epochs
  could **regress** across reopen, which #1844 forbids.
- Persisting it makes the value exact across a clean reopen. This removes the
  "may advance once across a reopen" caveat (`graph_recovery.rs:421-425`).
  The `SyncOnCheckpoint` crash caveat is unchanged.

A v1 manifest adopted by a v2 reader uses `checkpoint_commit_epoch`, which is
today's behavior.

## 6. Generation stamps inside artifact bytes

The stamps (§2.3) stay unchanged in layout and meaning. Their binding target is
restated:

- S1. A stamp in a family artifact MUST equal that family entry's `generation`
  (F-gen). A source-epoch stamp MUST equal that entry's `source_commit_epoch`.
  The stamps never refer to the checkpoint generation of the manifest that
  currently selects the family.
- S2. Readers MUST take the expected generation and epoch from the family
  entry. Today's code takes them from `checkpoint_generation`
  (`store/durable/artifacts.rs:513`, `:597`, `:673`, `:749`) and
  `checkpoint_commit_epoch` (`:541`, `:622`, `:698`). Those reads become
  family reads.

Why integrity is preserved:

- **Misdirected or stale reads are still detected.** A stamp exists to catch
  bytes that are internally valid but belong to another generation, for
  example a stale file left by a crashed candidate or a page from another
  generation. The family entry names exactly one F-gen, so the stamp check
  still rejects every file except the one the manifest selected.
- **Names cannot be rewritten in place.**
  - A file name is reused only if it was never published:
    `next_checkpoint_generation` allocates a fresh C-gen
    (`store/durable.rs:697-720`), and abandoned-future cleanup removes
    unpublished generations above the published one (`artifact_files.rs:214-244`).
  - Once a manifest references `(F, G_F)`, the files under that name are
    immutable for as long as any retained manifest references them (§8.1).
- **The checksum chain is unchanged:** manifest footer and family digest, then
  root len/CRC/SHA-256, then the descriptor and segment digests inside the
  roots, then the data bytes.
- **Two generation spaces cannot be confused.** Every live F-gen is ≤ C-gen and
  each F-gen was itself a C-gen at the time it was written.

The rejected alternative (#1641 option B) was to drop the stamps and hard-link.
It would weaken misdirected-read detection and still require exact
reachability accounting (`BRANCHING_STORAGE_SPEC.md:800-804`).

## 7. Checkpoint flow

### 7.1 Preparation

`prepare_checkpoint_with_maintenance_inner` (`store/graph_checkpoint.rs:729-1450`)
changes as follows:

1. **Decide first.** Before any writer starts, compute the
   `CarryForwardPlan: FamilySet` from §5.5. Record it, together with the
   previous entries and the mutation epochs, in `PreparationPlan` so that a
   resumed or bounded preparation makes the same decision.
2. **Allocate C-gen** as today (`:959-968`). Rewritten families use F-gen = C-gen.
3. **Run only the writers of rewritten families:**
   - **canonical and spill** (`:1031-1033`; the decode and re-encode is ~53 %
     of the #1641 checkpoint);
   - **adjacency** (`:1036-1054`; ~29 %);
   - **property projection** (`:1057-1090`; ~17 %);
   - **relational checkpoint** (`:1093-1113`), **overflow and row**
     (`:1114-1314`), **index** (`:1344-1350`), **append** (`:976-994`).

   The decode of `relational.<g>` at `:1099-1113` is skipped when that family
   is carried forward. The adopted relational state is then the current
   in-memory state, which is identical by M4.
4. **Copy carried entries.** Carried-forward entries are copied verbatim from
   the previous manifest (M3).
5. **Always write the checkpoint file and prepare the WAL**
   (`:1351-1376`): C-gen advances on every checkpoint.
6. **Build the manifest** through a `checkpoint_manifest` that takes a
   `Vec<FamilyEntry>` instead of `CheckpointManifestArtifacts`
   (`store/durable/checkpoint.rs:303-378`), and validate it.

The projected-graph sidecar and the source scan are rewritten only when
`graph.canonical` is rewritten or `ProjectGraph` and catalog changes require
it. Otherwise they keep their previous bytes and the previous
`source_scan_commit_epoch` (`store/graph_checkpoint.rs:751-784`, `:995-1029`).

### 7.2 Carry-forward per family, including partial cases

| Change since the previous checkpoint | Rewritten | Carried forward | Expected saving vs today (from the #1641 profile) |
| --- | --- | --- | --- |
| Relational-only DML or DDL (#1641 bootstrap DDL) | relational families touched, checkpoint file | all graph families | ≈100 % of graph work (≈7.4 s at SF0.1) |
| Append-only | `append`, checkpoint file | graph, relational | graph and relational work |
| Graph property set | canonical (+spill), projection | adjacency, relational | ≈29 % (adjacency) |
| `CREATE INDEX` on graph properties | projection | canonical, adjacency, relational | ≈82 % (canonical + adjacency) |
| Relationship create or delete | canonical, adjacency, projection | relational | relational work |
| Graph-only change | graph families | relational families, including legacy `relational.<g>` (#1521 relief) | relational encode, decode and hash |

**Partial carry-forward and dependencies.**

- When `graph.canonical` is rebuilt but `graph.adjacency` is carried, the new
  canonical generation is cross-checked against the carried adjacency
  (relationship count, M5).
- When the projection is rebuilt over a carried canonical base (the
  `CREATE INDEX` row above), the projection builder reads records from the
  published canonical reader, as it does today in out-of-core mode
  (`store/graph_checkpoint.rs:791-819`). That read is a decode, not a
  re-encode.

### 7.3 Adoption and caches

`adopt_checkpoint_manifest` (`store/durable/checkpoint.rs:424-520`) and
`adopt_prepared_checkpoint_state_with_work_context`
(`store/graph_checkpoint.rs:1627-1676`) MUST keep the already-open reader
`Arc` of every carried-forward family instead of reopening it.

- Segment-cache keys include `manifest_generation` (`crates/storage/src/cache.rs:70-82`).
  They now carry the F-gen, so cached pages of a carried family stay warm
  across the checkpoint.
- Today every checkpoint changes that key and leaves the cache cold for the
  whole graph. *Inference:* this is a second-order performance gain.
- The source-scan reader's cache key (`store/durable.rs:587`) moves to the
  source-scan epoch.

### 7.4 Background candidates, private suffix replay and branch barriers

- `CheckpointSourceIdentity` (`store/graph_checkpoint_candidate.rs:59-80`)
  keeps `checkpoint_generation` as the C-gen.
- A candidate prepared on a `checkpoint_source()` clone
  (`store/graph_checkpoint.rs:634`) carries the mutation epochs and rewrite
  flags in the clone.
- The schema checkpoint barrier
  (`docs/specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md:1258-1269`) is an
  ordinary checkpoint under this model. It carries the graph forward, which is
  exactly the #1641 fix.

### 7.5 Forced rewrite

`checkpoint_with_options(CheckpointRewrite::All | Families(set))` forces
rewrites. Uses: format upgrade (§10.2), derived repair (§8.6), qualification
"verify" runs (§12).

Configuration `checkpoint_carry_forward: Enabled | Disabled` (default
`Enabled` once phase P5 qualifies; `Disabled` is a kill switch and is not
persisted).

## 8. Reachability and verification consumers

### 8.1 Data-root generation reclamation

`try_reclaim_old_generations` (`store/durable/reclamation.rs:197-286`) is
redefined as follows:

- R1. **Retained set.** Let `M` = {current manifest, previous manifest, every
  manifest pinned by a live reader}. Then
  `Retained = ⋃_{m∈M} ⋃_{F∈m} closure_generations(F, m.entry(F)) ∪ {m.C-gen for checkpoint and WAL files}`.
  - `closure_generations` is the entry's F-gen plus every older physical
    generation its validated reader enumerates.
  - It generalizes today's row, overflow and append expansion
    (`:396-473`), which is keyed by checkpoint generation today, to every
    family and keys it by F-gen.
- R2. **Classification.** File names are classified by a new
  `storage_family_for_file(name) -> Option<(FileClass, generation)>`.
  - `FileClass` is a family tag, checkpoint or WAL.
  - It replaces `storage_generation_for_file` (`artifact_files.rs:180-205`) for
    deletion decisions.
  - A file is deleted only if its class is known, its generation is below the
    current C-gen, and `(class, generation)` is not in `Retained`.
  - Unknown names are never deleted, as today.
- R3. **Pins.** Reader pins (`src/api/mod.rs:983-989`, consumed at
  `src/api/automatic_checkpoint.rs:2188-2196`) change from
  `BTreeSet<u64 /*C-gen*/>` to a set of pinned `Arc<DurableManifest>`
  snapshots, which each reader view already implies. `pinned_reader_generations`
  becomes `pinned_manifests`.
  - Today's rule stays: an open reader with an unknown pin set skips the
    data-root sweep (`:208-214`).
- R4. **Obsolete-byte reporting.** `obsolete_generation_bytes` (`:76-97`) uses
  the same `Retained` set. Today's `generation < checkpoint_epoch - 1`
  arithmetic would over-report every carried family as obsolete.
- R5. **Ordering.** R1–R4 MUST land before any writer can publish an entry with
  `generation < previous C-gen`. Otherwise today's sweep would delete a
  carried-forward family (phase P2 precedes P5, §11).

### 8.2 Checkpoint closure and sealed roots

`checkpoint_closure_plan_with_work` (`store/durable/backup.rs:59-297`) and
`manifest_artifact_inputs_with_work` (`durable_manifest.rs:426-560`) derive
every family's paths from `entry.generation`. The mixed adjacency-pages site
(`backup.rs:143-156`) is fixed by this.

The family sets of `CheckpointArtifactFamily` (`checkpoint_closure.rs:113-134`)
and `FamilyId` merge into one enum. The closure still requires every family to
be added or marked empty (`checkpoint_closure.rs:136-214`).

**Incremental closure (I1–I3).**

- I1. When publishing a root, read the previous verified root's durable
  manifest object (the root binds it, `sealed_root.rs:52-62`). For each family
  whose `artifact_set_sha256` equals the previous manifest's entry for the same
  tag:
  - enumerate the family's closure paths with its validated reader, which is a
    metadata read of descriptor roots and pages;
  - take each path's `ObjectReference` from the previous root's
    `checkpoint_bindings`.

  These files are not re-hashed and not re-staged.
- I2. The previous root must contain a binding for every enumerated path.
  Otherwise the family falls back to full digesting. The fallback is logged and
  counted in the publication report.
- I3. Only rewritten families, the checkpoint file and the manifest are
  digested and published (`checkpoint_closure.rs:54-99`;
  `immutable_object.rs:1348-1380`).
  - This makes closure publication proportional to change. It is the
    "incremental closure publication" that #1521 listed as separate work.
  - The existing unchanged-manifest shortcut
    (`store/immutable_root.rs:2525-2545`) becomes the degenerate case where
    every family matches.

Why the shortcut is sound:

- The previous root was verified when published: every object was hashed by
  `publish_file_with_work` or `validate_existing`.
- Published names are immutable (§6).
- The family digest equality proves the root bindings are identical.
- Bit rot in an already published object is scrub's responsibility, not the
  closure's (`immutable_object/published.rs:27-37`).

**Mount and branch publication.**

- Mounting (`store/immutable_root.rs:2821-2915`; `immutable_files.rs:312-342`)
  hard-links objects to `artifact_dir/relative_path`. That works unchanged
  because bindings are relative paths derived from F-gens.
- `validate_sealed_formats` (`store/format_admission.rs:138-166`) resolves
  family root names from the F-gen.
- Object reclamation is already reference-based (`immutable_object.rs:722-830`)
  and needs no change.
- The branch-runtime `checkpoint_generation` field
  (`store/immutable_root.rs:83`, `:1671`) stays the C-gen.

### 8.3 Scrub

`scrub.rs` verifies each family at its entry's F-gen and epoch:

- canonical: `:318-340`;
- spill: `:480-495`;
- projection: `:567-640`;
- relational checkpoint: `:122-140`.

The report gains, per family, `{tag, generation, source_commit_epoch, format, carried_forward: bool}`.

### 8.4 Backup and restore

- B1. The backup manifest (`crates/storage/src/backup.rs:245-275`) gets a v2
  that records the copied durable-manifest v2, so that family generations are
  explicit. Its single `generation` becomes the C-gen.
- B2. Backup (`store/durable/backup.rs:299-555`) copies each family's closure
  at its F-gen. The mixed adjacency site `:431-447` is fixed by this.
- B3. Restore validation (`store/backup.rs:69-1036`) replaces "all names at
  `{g}`" with per-family name and identity checks against the family entries:
  - the generation-equality requirement at `:80-87` is limited to the
    checkpoint file and WAL;
  - canonical `:189-229`, adjacency `:314-371`, projection `:414-485` and
    spill `:648-688` are checked per family;
  - the backup whitelist (`crates/storage/src/backup.rs:299-322`) is unchanged
    because names are unchanged.
- B4. *Enabled, not specified:* an incremental backup can skip a family whose
  `artifact_set_sha256` equals the destination's previous backup. This is
  listed as Q8.

### 8.5 Doctor, format admission and readiness

- `validate_checkpoint_boundary` (`store/doctor.rs:874-896`) is unchanged,
  since the checkpoint file is not a family.
- `validate_checkpoint_formats` (`store/format_admission.rs:23-136`) adds the
  manifest-level family format gate (V2/V3). It also covers the families it
  skips today: canonical, adjacency, spill, projection, overflow and index
  shadow. Those checks happen at manifest decode, so they need no file reads.
- Readiness inherits the typed refusals through doctor and format admission
  (`docs/STORAGE.md:126-130`).

### 8.6 Derived repair

`DerivedArtifactKind` (`crates/storage/src/derived_repair.rs:37-40`) maps to
families 2 and 3.

- **Repair rewrites only the damaged derived family**, using
  `CheckpointRewrite::Families`. All other families are carried forward.
- Today a rebuild requires `manifest.checkpoint_epoch == target_generation`
  and that every artifact is healthy (`store/derived_repair.rs:166-185`).
  That check becomes `entry(F).generation == target_generation` for the
  repaired families.
- `rebuildable_artifact_kind(path, generation)`
  (`store/derived_repair/audit.rs:414-436`; mount calls at
  `store/immutable_root.rs:2854`, `:2894`) receives the family's F-gen, so the
  mount tolerance still recognizes a carried-forward adjacency or projection.
- The repair protocol string (`derived_repair.rs:30`) is bumped to `-v3`.

### 8.7 Recovery

- `load_checkpoint` (`store/graph_recovery.rs:355-492`) is unchanged for the
  checkpoint file, which stays at the C-gen. It reads `relational.<g>` at the
  `relational.checkpoint` entry's F-gen.
- Row-page mount (`store/relational_row_pages.rs:453-467`) checks the
  `relational.row` entry instead of `checkpoint_epoch`.
- Index shadow (`store/relational_index_shadow.rs:694-770`) compares against
  the `relational.index` entry. Its `Stale` classification then means "entry
  missing or older than the row family requires", not "older than the
  checkpoint".
- WAL replay is unchanged (`store/graph_recovery.rs:785`) and advances the
  family mutation epochs (§5.5).

## 9. Block-framed search content segments (#1917)

### 9.1 Layout

A content descriptor segment encoded as `blocks1` is the following byte range
in the payload file:

```text
segment  := header | block[0] … block[n-1] | index | trailer
header   := magic "HDCBLK01" (8) | layout_version u16 = 1 | codec u8 (1 = zstd)
            | flags u8 (0) | dictionary_id u32 (0) | segment_id u64
            | generation u64 | reserved [u8; 32] (0)          -- 64 bytes
block[i] := zstd frame of the block body (no frame checksum; CRCs are in the index)
index    := block_count u32 | entries[block_count]
entry    := offset u64 (from segment start) | compressed_len u32 | uncompressed_len u32
            | compressed_crc32c u32 | uncompressed_crc32c u32 | doc_count u32
            | first_id_len u16 | first_id bytes | last_id_len u16 | last_id bytes
trailer  := index_offset u64 | index_len u32 | index_crc32c u32 | block_count u32
            | reserved u32 (0) | magic "HDCBLK01"                -- 32 bytes
```

**Block body.** A block body holds the same `doc\t…` lines as today, without
the `HAWDB_SEARCH_SEGMENT_V1` prefix. Documents are in strictly increasing id
order across the whole segment and are never split across blocks.

**Block sizing.**

- Target uncompressed block size: **16 KiB** by default. Writer config
  `content_block_target_bytes` accepts 4 KiB to 1 MiB.
- A block closes when adding the next line would exceed the target.
- A single document larger than the target forms its own block, bounded by the
  existing document limits.
- `max_content_block_uncompressed_bytes` (a reader admission bound, default
  `max(1 MiB, max_document_bytes)`) rejects any index entry above it before
  any read.

**Header fields.** `dictionary_id ≠ 0`, `codec ≠ 1` or non-zero reserved bytes
are
`UnsupportedFeature{format: SEARCH_PROJECTION_MANIFEST_FORMAT, feature: "search content block", tag}`.
This reserves room for trained zstd dictionaries or other codecs without
another layout version.

**Segment digest.** The descriptor's per-segment `payload_range.checksum`
(`crates/search/src/lib.rs:6533-6549`) is defined for `blocks1` as
`CRC32C(header || index || trailer)`.

- The index contains every block's compressed and uncompressed CRC, so this is
  a digest over the block digests.
- The whole payload file remains bound by the selector's SHA-256 object
  reference (`out_of_core/artifact_reference.rs:12-84`).

**Descriptor.** It moves to `HAWDB_SEARCH_SEGMENTS_V4`, which adds an
`encoding` column (`stream1` = today's envelope, `blocks1`). V1–V3 descriptors
remain readable (`lib.rs:8716-8721`).

### 9.2 Hydration read path

For a group of hits in one segment (`out_of_core/hydration/query.rs:71-110`):

1. **Read the segment index.** Read the trailer and the index (one positioned
   read when the index is small), or take the verified index from the cache
   (§9.4). Verify `CRC32C(header||index||trailer)` against the descriptor and
   check the structure:
   - blocks are contiguous and in range;
   - block id ranges are ordered and disjoint;
   - the sum of `doc_count` equals the descriptor's `document_count`;
   - the first and last ids match the descriptor.
2. **Find the blocks.** Binary-search the index for each requested id. An id
   outside every block's range is proven absent from this segment. This keeps
   today's "missing vs storage error" distinction
   (`STREAMING_SEARCH_HYDRATION.md:60-62`).
3. **Read and verify each needed block.**
   - Read `compressed_len` bytes. Adjacent needed blocks are coalesced into one
     read of up to 256 KiB, following the vector-sidecar precedent
     (`out_of_core/vector_segment.rs:20-23`).
   - Verify `compressed_crc32c`, decompress with the admitted decoder
     (`crates/search/src/build_memory/decoder.rs:15`), then verify
     `uncompressed_len` and `uncompressed_crc32c`.
   - Scan the block's lines with `summarize_document_line`
     (`hydration.rs:372-419`), checking order, count and first/last ids
     against the index entry, and fully decode only the requested ids.
4. **Release results** only after every block that contributed a result
   passed step 3.

**Integrity contract (replaces `STREAMING_SEARCH_HYDRATION.md:18-30` for
`blocks1`).**

- H1. Every released byte was verified by the chain selector → descriptor →
  segment digest → block CRCs.
- H2. A failure in any block read for this request fails the request
  (`Storage`). No partial success is exposed.
- H3. Blocks not read by a query are not verified by that query. Their
  integrity is guaranteed by:
  - publication-time verification of every installed file
    (`docs/SEARCH_SEGMENT_POWER_LOSS_QUALIFICATION.md:72-76`);
  - cleanup re-hashing (`docs/SEARCH_CLEANUP_OWNERSHIP.md:44-89`);
  - a new explicit **search deep verification** (`verify_search_projection`,
    also reachable from storage scrub) that verifies every block and the file
    SHA-256.

  This is the one deliberate narrowing of the current contract. It matches the
  storage page model, where each page is verified when read and scrub covers
  the rest (`crates/storage/src/immutable_object/published.rs:31-37`).
- H4. Whole-segment consumers verify every block plus the index-covers-range
  structure before releasing anything, so their guarantee is unchanged. These
  are compaction, mutation targets, the verified-body and selected-body readers,
  metadata queries and projection scans (`hydration/source.rs:149`,
  `selected_body.rs:234,256`, `verified_body.rs:157`,
  `generation_writer/compaction/streaming.rs:131`, `metadata_query.rs:65,357`,
  `projection_scan.rs:89`).

### 9.3 Writer

`encoding::append_segment` (`out_of_core/generation_writer/artifacts/encoding/file.rs:27-120`)
gains a `blocks1` path:

- It reserves the 64-byte header, then streams blocks, then writes the index
  and the trailer.
- The header has a fixed size, so no tail shift is needed.
- Memory: one block of uncompressed bytes, one compressed frame and an index
  builder. The index is bounded by `block_count × (36 + 2·max_id_len)` bytes,
  is charged to the existing build memory account, and is capped so that a
  segment's index stays below 1 MiB.

The metadata sidecar (`document_encoding/segment.rs:126-131`) uses the same
envelope and adopts `blocks1` in the same change. A sidecar per-document read
then has the same cost profile.

### 9.4 Cache

A new `RepresentationKind::SearchContentBlock` and
`RepresentationKind::SearchContentIndex` are added to the storage `SegmentCache`
(`crates/storage/src/cache.rs:37-50`, key `:76-82`). The key fields are:

- `store_id`: the search instance's store id;
- `manifest_generation`: the segment's generation;
- `segment_id`;
- `content_digest`: derived from the payload file's SHA-256 object reference,
  the block index and the block's `uncompressed_crc32c`.

Cached values are **verified decompressed block bodies** and **verified parsed
indexes**.

- **Budget.** Cache bytes are charged to the shared cache capacity, not to the
  query. A query charges only its transient buffers (one compressed frame, one
  decompressed block, decoder workspace) against `QueryMemoryAccount`
  (`out_of_core/query_memory.rs:43-60`; `hydration.rs:203-205`), plus result
  admission as today (`STREAMING_SEARCH_HYDRATION.md:32-42`).
- **Hits.** A cache hit skips I/O, CRC checks and decompression. A hit is
  trusted because it was verified at insertion and the key includes the
  payload object digest, so a different file can never alias it.
- **Opt-out.** Callers can opt out per query (`SearchHitHydration` options).
  The default is on, bounded by the existing cache capacity configuration.

### 9.5 Admission and I/O accounting

- The 8 KiB read granularity in `STREAMING_SEARCH_HYDRATION.md:15` becomes "at
  most one positioned read per coalesced block run, each ≤ 256 KiB".
- I/O-wave permits are per read, as today
  (`crates/search/src/build_memory/read/query.rs:28-143`).
- New metrics: `content_blocks_read`, `content_block_cache_hits`,
  `content_block_bytes_inflated`, `content_index_reads`. They complement
  `segment_range_reads` and `hydration_segment_bytes_read`
  (`hydration.rs:127-134`).

### 9.6 Search versioning

- Selector `format_version` and `min_reader_version` become **3** for any
  selector that references a `blocks1` segment, per the bump rule in
  `docs/SEARCH_PROJECTION_MANIFEST_VERSION.md:108-138`.
- Older readers refuse with
  `UnsupportedVersion{format: SEARCH_PROJECTION_MANIFEST_FORMAT}`
  (`out_of_core/manifest_version.rs:23-80`).
- New readers keep reading `stream1` segments. New segments, compaction output
  and rebuilds write `blocks1`, so `stream1` segments disappear as compaction
  proceeds.
- Dropping `stream1` reader support is a later, separate format decision
  (§14, Q10).

## 10. Compatibility and upgrade

### 10.1 Old databases (v1 manifests)

- U1. A v2 reader decodes a v1 manifest with the existing v1 decoder
  (`durable_manifest.rs:804-1157`) and maps it in memory to v2 entries:
  - every family present gets `generation = checkpoint_generation`;
  - `source_commit_epoch = checkpoint_commit_epoch`;
  - `format` is the version its header declares, which is 1 for graph
    families;
  - `artifact_set_sha256` is computed;
  - `graph_change_commit_epoch = checkpoint_commit_epoch`.

  These values are exactly v1's validated invariants (§2.1), so no file is
  read or rewritten.
- U2. Read-only opens of v1 databases never write.
- U3. The first checkpoint by a v2 writer publishes a v2 manifest. Every family
  of that first checkpoint MAY be carried forward under §5.5, since the mapped
  entries are valid.
- U4. No explicit upgrade command is required. `doctor` reports
  `manifest_revision` and per-family formats.
- U5. Backups taken by older releases (v1 backup manifest) restore through the
  same mapping.

### 10.2 Family format upgrades

When a release raises a family's writer version, a carried-forward family
keeps its old version until the next mutation (lazy upgrade). Two exceptions:

- `CheckpointRewrite::Families` forces an upgrade;
- a release MAY mark a family version as *deprecated*, so that the next
  checkpoint rewrites that family regardless of mutation.

A reader MUST keep decoding every family version it has not explicitly dropped
in a storage revision bump.

### 10.3 Forward compatibility and downgrade

- **Older readers refuse v2 manifests with a typed error.** A pre-v2 reader
  that includes #1903 refuses a v2 manifest with
  `UnsupportedVersion{storage manifest, found 2, supported 1}` (E1).
  - Releases before #1903 report it as an untyped storage error. This is
    documented as for row-page v5 (`docs/STORAGE.md:103-108`).
- **v2 readers refuse unknown families and newer family formats** with typed
  errors (V2/V3).
- **Downgrade is not supported** after the first v2 checkpoint. Use a logical
  dump (`docs/LOGICAL_DUMP.md`).
  - Writing v1 while no family is carried forward would keep downgrade
    possible longer, at the cost of two writers. This is the #1902 policy
    question, and it is left to the maintainer (§14, Q1).

### 10.4 `persistent-format.lock`

- Bump `PERSISTENT_FORMAT_VERSION` from 5 to 6
  (`crates/storage/src/persistent_format.rs:32`), because older binaries cannot
  open a v2 database.
- Regenerate the lock with `python3 scripts/check-persistent-format.py --update`.
- The new `STORAGE_VERSION`, the family format constants, the new `hawdb_core`
  format ids, `HDCBLK01` and the search selector version constant will appear
  in the diff. The checker matches `MAGIC|VERSION|FORMAT` names
  (`scripts/check-persistent-format.py:45-50`).
- *Gap found during this review:* `*_HEADER*` constants are not locked, for
  example `MANIFEST_HEADER_V1` (`durable_manifest.rs:1455`) and
  `CHECKPOINT_HEADER_V1` (`crates/storage/src/checkpoint.rs:46`). A small
  preliminary PR should add `HEADER` to the checker regex so that E1 is
  tripwired.

### 10.5 Fuzz and power-loss qualification impact

- **mvcc-fuzz** (`tools/mvcc-fuzz/`) and **shiro** (`tools/shiro/`; storage
  oracles in `row_page_oracle.rs`, `wal_tail_oracle.rs`, `append_oracle.rs`)
  gain a *carry-forward oracle*. After each checkpoint it asserts:
  - (a) M2–M4 against an independent model of family effects;
  - (b) for each carried family, the logical record stream equals a forced
    rebuild into a scratch generation;
  - (c) reopen equivalence.
- **Power-loss** (`crates/storage/src/power_loss.rs`;
  `src/api/tests/power_loss.rs:458-484` `publication_fault_plans`) gets the new
  cut points in §12. A new row is added to
  `docs/BRANCH_POWER_LOSS_QUALIFICATION.md:136-153`.
- **Error classes.** `crates/fuzz-contracts/src/lib.rs:155` already maps
  `UnsupportedStorageFormat`. New format ids need no fuzz-contract change.

## 11. Phased implementation plan

Each phase is one PR, or a small PR series, that ships and tests on its own.
P1–P3 change no bytes.

| Phase | Content | Format change | Ships value on its own |
| --- | --- | --- | --- |
| **P0** | This spec, tracking issue, `HEADER` constants in the lock checker (§10.4) | none | review baseline |
| **P1** | `FamilyId` registry merged with `CheckpointArtifactFamily`. In-memory `FamilyEntry` view over the v1 manifest. Route **every** §2.4 site through `entry(F).generation/source_commit_epoch` (still equal to C-gen). Fix the three mixed sites. Add `storage_family_for_file`. | none | removes latent mixed-generation bugs; one source of truth |
| **P2** | Reclamation by referenced family generations (R1–R4). Reader pins become manifest snapshots. Tests build manifests with older F-gens through a `#[cfg(test)]` constructor. | none | precise obsolete-byte reporting; prerequisite for P5 (R5) |
| **P3** | `WalOp::family_effects` (exhaustive), family mutation epochs, rewrite flags and fences. The checkpoint report gains `families_rewritten` and `families_eligible_for_carry_forward`, while still rewriting everything. Verify-mode test harness. | none | measures carry-forward eligibility on real workloads before any format risk |
| **P4** | Manifest v2 codec, v1 adoption, typed refusals (E1, V1–V3), new `hawdb_core` format ids, `graph_change_commit_epoch` persisted, backup manifest v2, restore, scrub, doctor and format admission per family, `PERSISTENT_FORMAT_VERSION` 6 and lock. The writer still rewrites every family. | **yes** (storage) | proves the format with zero carry-forward risk |
| **P5** | Enable carry-forward for graph families (canonical+spill, adjacency, projection) and partial combinations. Reader `Arc` reuse on adoption. Per-family derived repair. Power-loss and fuzz additions. | none beyond P4 | #1641 fix |
| **P6** | Incremental closure and sealed-root reuse per family (I1–I3) | none | branch-enabled checkpoints proportional to change |
| **P7** | Carry-forward for relational families (`relational.checkpoint`, index, append; row and overflow when untouched), coordinated with #1521 | none | graph-only checkpoints on large relational databases |
| **S1** | Search `blocks1` writer and reader, descriptor V4, selector v3, deep verification, contract-doc updates. Independent of P1–P7 and can land in parallel after P0. | **yes** (search) | #1917 fix |
| **S2** | Verified block and index cache, metrics | none | repeat-query latency |
| **S3** | In-repo BEIR-style hydration benchmark (extend `examples/component_profiles.rs`) | none | regression guard |

**Release coordination.** P4 and S1 SHOULD ship in the same release, so that
`PERSISTENT_FORMAT_VERSION` moves once and hosts see one format change. If they
cannot ship together, S1 alone does not bump `PERSISTENT_FORMAT_VERSION`
(search projections are rebuildable, `docs/SEARCH_PROJECTION_MANIFEST_VERSION.md`).

## 12. Test matrix

Every new test is listed in `docs/TEST_INDEX.md` in the same change
(`AGENTS.md:89-93`).

| Area | Cases | Phase |
| --- | --- | --- |
| Manifest codec | v2 round trip; ordering, duplicate and orphan `binding` lines; unknown family is a typed `UnsupportedFeature`; newer family format is a typed `UnsupportedVersion`; tampered entry fails the `artifact_set_sha256` check; v1 reader against a v2 manifest gives a typed v1-side refusal (golden-file test against a v1 decoder snapshot); v1 adoption equivalence | P4 |
| Invariants | M1–M8 property tests; M3 writer obligation (entry copied verbatim) checked against an oracle across random checkpoint sequences | P4–P5 |
| Classification | `family_effects` exhaustiveness (compile time); per-op golden table; fences default to rewrite (construct `GraphStore` through each constructor and assert all families require a rewrite) | P3 |
| Carry-forward correctness | relational-only, append-only, property-set, `CREATE INDEX`, relationship create/delete, `ProjectGraph`, catalog-only graph DDL; Materialized, OutOfCore and Auto residency; reopen after each; verify mode compares carried families with a forced rebuild | P5 |
| Crash and power loss | cut before and after the relational family write, the checkpoint file, WAL prepare and manifest publish, for a carry-forward checkpoint; cut during reclamation after a carry-forward with a pinned reader holding the previous manifest; persist-none, persist-all, reversed and isolated plans (`src/api/tests/power_loss.rs:458-484`); torn manifest; abandoned future generation cleanup with carried F-gens below C-gen | P4–P5 |
| Reclamation | carried family older than the previous C-gen is retained; dropped after the last referencing manifest is unpinned; unknown names untouched; obsolete-byte report equals the deletable set | P2, P5 |
| Branch and sealed root | publish a root with carried families (bindings point to older F-gen names); mount and reopen; incremental closure digests only rewritten families (counted); fallback path when a previous binding is missing; branch create, abandon and reclaim with shared carried objects; sealed-WAL interval cap unchanged | P5–P6 |
| Backup and restore | backup after carry-forward contains each family at its F-gen; restore validation rejects a mismatched F-gen file, a missing carried family and a renamed file; v1 backup restores into v2 | P4–P5 |
| Scrub and doctor | scrub reports per-family generations and finds corruption in a carried family; doctor typed refusal for unknown and newer family; derived repair of a carried adjacency rebuilds only adjacency | P4–P5 |
| mvcc-fuzz / shiro | carry-forward oracle (§10.5) under random interleavings of graph, relational, append and DDL ops with checkpoints, reopen and crash injection; required local gate per `AGENTS.md:139` | P3–P5 |
| Search `blocks1` | round trip; per-block CRC corruption fails only requests touching that block; index or trailer corruption fails the segment; absent id inside a block range vs outside all ranges; oversized-block admission; reserved-field and dictionary refusal; v2 selector reader refuses a v3 selector; mixed `stream1`/`blocks1` generations; compaction converts; power-loss replay (`docs/SEARCH_SEGMENT_POWER_LOSS_QUALIFICATION.md:13,136-137`); hydration fuzz target (`//crates/search:hawdb_search_hydration_fuzz_tests`); cache-key aliasing across generations | S1–S2 |

## 13. Expected performance and measurement

Measured numbers are quoted from the issues. Numbers marked *target* are
expectations that phases P5, P6 and S1 must confirm or revise.

| Workload | Today | Target | How to measure |
| --- | --- | --- | --- |
| LDBC SNB SF0.1 import, end to end | 36.8 s (#1641) | ≈29 s; the tool-side experiment in #1641 removing the checkpoint measured 28.9 s | the #1641 import harness (external; release build, frame pointers), two runs each |
| DDL-after-import checkpoint (bootstrap `CreateTable`) | 7.9–8.0 s (#1641) | < 0.2 s without branches; with a branch root, < 0.5 s after P6 (*target*) | wall time of `complete_required_relational_row_checkpoint` (`src/api/transactions.rs:1441`); report `families_rewritten`; `perf` share of `prepare_checkpoint_with_maintenance_inner` |
| `CREATE INDEX` on a graph property after import | ≈ full graph checkpoint | ≈17–20 % of today (projection only) (*target*) | same harness plus index DDL |
| Property-update checkpoint | full graph checkpoint | −25–30 % (adjacency carried) (*target*) | microbenchmark: SET on 1 % of nodes, then checkpoint |
| Checkpoint closure (branch root) | re-hashes all files (`checkpoint_closure.rs:54-99`) | bytes hashed ∝ rewritten families | publication report counters (digested bytes, reused bindings) |
| TPC-C SF1 load and run | 289 s load (#1521) | no regression (±2 %); P7 helps only if graph state exists | the #1521 py-tpcc harness |
| BEIR SciFact BM25 top-100, `Documents` hydration p50 | 123 ms (#1917) | ≤ 10 ms cold, ≤ 3 ms warm cache (*target*) | the #1917 harness; S3 adds an in-repo variant via `examples/component_profiles.rs` (`docs/COMPONENT_PROFILES.md:45-69`) |
| BEIR NFCorpus BM25 | 89 ms | ≤ 8 ms cold (*target*) | as above |
| Search payload size | baseline | ≤ +15 % compressed bytes at 16 KiB blocks (*target*); block size tuned on this | `payload_len` from the selector before and after a rebuild |

All runs record the commit, build profile, machine and the new counters
(`families_rewritten`, closure digested and reused bytes, `content_blocks_read`,
cache hits).

## 14. Open questions for the maintainer (@hawkingrei)

1. **Q1. Downgrade window (#1902).** After a v2 binary opens a database,
   should it write a v2 manifest unconditionally (single writer; this spec's
   default)? Or should it keep writing v1 until the first checkpoint that
   actually carries a family forward? The second option keeps downgrade
   possible longer but adds a second writer and test matrix.
2. **Q2. v1 adoption vs dump-only policy.** `docs/STORAGE.md:167-169` and
   `AGENTS.md:20` say development formats break without migrations. This
   spec's U1 is an in-memory reinterpretation, not a byte migration. Is that
   acceptable, or should a v2 reader refuse v1 and require a logical dump?
3. **Q3. `relational.<g>.hawdb` (#1521).** Is the legacy relational checkpoint
   still needed by any recovery or compatibility path? If not, family 4 should
   be retired in P7 instead of carried forward.
4. **Q4. Branch-root incremental closure trust.** I1–I3 reuse the previous
   root's references for families with an identical `artifact_set_sha256`,
   without re-hashing. Is relying on name immutability plus the previous
   root's verification acceptable under the branching spec's
   publisher-validation rule (`BRANCHING_STORAGE_SPEC.md:800-804`)? Or must
   reuse still re-validate the object (the cost #1521 measured)?
5. **Q5. Ignorable families.** Should v2 already reserve an "ignorable
   derived family" class? Older readers could then open a database with a
   newer derived family, at the cost of storing explicit file lists for it.
   This spec says no: fail closed.
6. **Q6. `graph_change_commit_epoch` location.** It is persisted in the
   manifest here (§5.6). Would you rather put it in the checkpoint file, next
   to `commit_epoch`?
7. **Q7. Projected-graph sidecar and source scan.** Should these fixed-name
   files become families with generation-named files in this round, or stay
   fixed-name and be rewritten only when the graph changes?
8. **Q8. Incremental backup.** Should B4 (skip unchanged families) be
   specified now or deferred?
9. **Q9. Search embedding text.** Content blocks still contain the
   decimal-text embedding, which the vector sidecar already stores. Should
   `blocks1` omit it, which needs a vector-sidecar dependency for `Documents`
   hydration? This would likely cut block bytes several-fold.
10. **Q10. `stream1` retirement.** When can readers drop single-stream search
    envelopes? This spec proposes the next search format bump after every
    supported writer emits `blocks1`.
11. **Q11. Default block size.** Is 16 KiB acceptable, pending S3
    measurements, or do you prefer 32 or 64 KiB for compression ratio?
12. **Q12. Release pairing.** Must P4 (storage v2) and S1 (search `blocks1`)
    ship in one release, or may S1 go first since it does not bump
    `PERSISTENT_FORMAT_VERSION`?

## 15. Risks

- **Reclamation ordering (R5).** Enabling carry-forward before
  family-reference reclamation would delete live files. P2 must precede P5,
  and P5 must assert at startup that the reclamation implementation reports
  family-aware mode.
- **Unclassified mutation paths.** A non-WAL path that changes family state
  without a fence would publish a stale family. This is mitigated by the
  default-rewrite constructor rule (§5.5 step 4), the verify mode, and the
  fuzz oracle.
- **Mixed-generation sites.** The three mixed sites (§2.4) show that silent
  coupling exists. P1 must remove every direct use of the checkpoint generation
  for family files; a grep-based CI check is recommended.
- **Contract narrowing for search (H3).** Unread corrupt blocks are no longer
  detected by queries. This is mitigated by deep verification, publish-time
  verification and cleanup re-hashing, and must be documented in
  `STREAMING_SEARCH_HYDRATION.md`.
- **Downgrade.** The first v2 checkpoint prevents downgrade (Q1).
