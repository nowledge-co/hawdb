# Per-family test index entries

[`docs/TEST_INDEX.md`](../TEST_INDEX.md) is the overview of execution lanes,
shared owners and slow paths. New or changed **test-family entries** go here,
one file per family, so that concurrent pull requests do not edit the same
lines of one large file.

## Adding an entry

Create `docs/test-index/<area>-<family>.md` (lowercase, words separated by
`-`). Use this shape; the first four lines are checked by
`scripts/check-test-index.py`, which `prek` runs:

```markdown
# <What the family protects> (#<issue>)

Owner: `<test module or case path>` (several separated by `;`)
Lane: `<Bazel target>` (and why, if not an existing lane)
Replay: `<exact local command>` (several separated by `;`)

<Purpose and failure mode, exact feature/fixture scope, known slow paths or
issues, anything that changed lanes, profiles, timeouts or isolation.>
```

Update the family's file in the same change that adds, moves, optimizes or
re-scopes its tests. Edit `docs/TEST_INDEX.md` itself only for lane-level or
cross-family changes.
