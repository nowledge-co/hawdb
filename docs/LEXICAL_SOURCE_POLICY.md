# Lexical Source Policy

`SearchLexicalSourcePolicy` is the host-selected admission limit for the UTF-8
bytes of one source document analyzed by the lexical projection. Its default is
4 MiB. The limit covers the complete `SearchDocument`: title, content, metadata
keys, and metadata values.

The policy is independent from encoded-record, input, analyzer workspace,
token, term, build-memory, spill, and query limits. Increasing it permits a
larger source only when those limits also admit the operation. In particular,
the public `SearchDocument` input and encoded spool record remain owned,
document-sized values. This policy does not provide a streaming input contract
or remove that resident input floor.

## Build and update lifecycle

Use `SearchOutOfCoreGenerationWriter::create_with_source_policy` to create a
generation with an explicit source limit. Use
`create_with_lexical_policies` when a non-default term policy is also required.

Open a generation with `SearchOutOfCoreReader::open_with_source_policy` or
`open_with_lexical_policies` before preparing updates. The reader captures the
host policy for every `prepare_delta` call. Later changes to the reader do not
alter an already prepared update.

Projection rows are converted to their complete materialized search documents
before lexical admission. Hosts must therefore leave room for generated graph
metadata as well as the row body when selecting a source limit.

## Ownership and failure behavior

The source policy is not stored in a generation manifest and is never inferred
from an artifact. An artifact cannot widen a host's future input admission.
Opening with a lower policy is valid because reopening does not reanalyze source
documents; an update under that lower policy fails before publication if either
hydrated or inserted documents exceed it.

`push` can stage a document that later exceeds a newly lowered source policy.
`finish` revalidates the complete stage and rejects publication atomically. The
previous active generation remains selected and the failed stage is cleaned up.
