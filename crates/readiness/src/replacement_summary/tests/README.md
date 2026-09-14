# Replacement summary baselines

`ready.json` and `blocked.json` contain the complete input and summary emitted
by `src/replacement_summary.rs` at main revision
`7fedeeee27472762959cb0e048922e351ed81f7e`, before the owner migration. They
were captured through its existing `production_ready_bundle` and
`blocked_bundle` test fixtures and default summary API. The temporary capture
test was removed after capture; the original fixture builders remain in
`../tests.rs` with their tests.

The regression compares the complete canonical JSON serialization, including
array order, null fields, blocker categories, next actions, and public protocol
identifiers. These snapshots are migration evidence, not a source of truth for
new behavior: intentional contract changes require explicit review of the
changed fields and their underlying readiness requirements.

The generated campaign checks raw contradictory evidence and bounded
presentation separately. Its complete 128-seed target remains local-only.
