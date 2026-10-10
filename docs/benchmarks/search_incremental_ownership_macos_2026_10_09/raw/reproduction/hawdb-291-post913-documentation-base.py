import hashlib
import json
import runpy
import subprocess
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-incremental-recovery-followup'
SOURCE = json.loads((BASE / 'hawdb-291-post913-source.json').read_text())
QUALIFIED = SOURCE['commit']


def git(*args):
    return subprocess.check_output(['git', *args], cwd=WORK, text=True).strip()


assert git('branch', '--show-current') == 'docs/291-incremental-qualification'
assert not git('status', '--porcelain')
head = git('rev-parse', 'HEAD')
main = git('rev-parse', 'origin/main')
assert head == main, 'documentation branch must start directly from current main'
subprocess.run(['git', 'merge-base', '--is-ancestor', QUALIFIED, head], cwd=WORK, check=True)
classifier = runpy.run_path(str(BASE / 'hawdb-291-post913-classify-main.py'))
classification = classifier['classify_revision'](WORK, QUALIFIED, head)
assert classification['search_runtime_benchmark_tests_and_external_dependencies_unchanged']
classified_paths = {row['path'] for row in classification['changes']}
assert all(hashlib.sha256((WORK / name).read_bytes()).hexdigest() == expected
           for name, expected in SOURCE['source_files_sha256'].items()
           if name not in classified_paths)
assert (WORK / '.bazelversion').read_text() == '9.3.0\n'
actual_bazel = subprocess.check_output(['bazel', '--version'], cwd=WORK, text=True).strip()
assert actual_bazel == 'bazel 9.3.0'
profiles_log = BASE / 'hawdb-291-post913-bazel-evidence/full-five-profiles.log'
assert profiles_log.read_text().splitlines()[0] == 'Starting local Bazel server (9.3.0) and connecting to it...'
receipt = {
    'state': 'passed', 'qualification_base': QUALIFIED, 'qualification_tree': SOURCE['integrated_tree'],
    'documentation_base': head, 'verified_current_origin_main': main,
    'runtime_and_benchmark_build_inputs_unchanged': classification['literal_build_inputs_unchanged'],
    'runtime_and_benchmark_semantics_unchanged': classification['search_runtime_benchmark_tests_and_external_dependencies_unchanged'],
    'working_tree_clean': True, 'changes': classification['changes'],
    'main_change_classification': classification,
    'toolchain_pin': {'actual_bazel': actual_bazel, 'local_search_run_uses_pinned_version': True,
                     'CI_scope': 'Final documentation PR required checks remain separate. Version-only metadata does not rebind measured binaries or resource results.'},
    'includes_PR979': True, 'includes_PR913': True,
    'qualification_gate': 'This binds source only. Current-main scale, reference, native and profile receipts must separately pass before publication.',
    'publication_gate': 'Fetch live main and classify runtime/test/build changes before delivery and issue closure.',
}
(BASE / 'hawdb-291-post913-documentation-base.json').write_text(json.dumps(receipt, indent=2) + '\n')
(BASE / 'hawdb-291-post913-main-change-classification.json').write_text(json.dumps(classification, indent=2) + '\n')
print(json.dumps({key: receipt[key] for key in ['state', 'qualification_base', 'documentation_base',
                  'runtime_and_benchmark_build_inputs_unchanged', 'runtime_and_benchmark_semantics_unchanged',
                  'working_tree_clean', 'includes_PR979', 'includes_PR913']}))
