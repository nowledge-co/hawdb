import hashlib
import json
import subprocess
import time
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-post913-qualification'
RECEIPT = BASE / 'hawdb-291-post913-pipeline.json'
SOURCE_PATH = BASE / 'hawdb-291-post913-source.json'
SOURCE = json.loads(SOURCE_PATH.read_text())
GATES = {
    'release': BASE / 'hawdb-291-post913-release.json',
    'compressed-reference': BASE / 'hawdb-291-post913-compressed-probe.json',
    'native-recovery': BASE / 'hawdb-291-post913-native.json',
    'full-five-search-profiles': BASE / 'hawdb-291-post913-bazel-evidence/receipt.json',
    'fresh-fuzz-and-shared-IO-regressions': BASE / 'hawdb-291-post913-bazel-phase-evidence/receipt.json',
}
data = {'state': 'waiting-for-prerequisites-and-original-matrix', 'commit': SOURCE['commit'],
        'source_receipt': str(SOURCE_PATH), 'started_unix_seconds': time.time(), 'steps': [],
        'scope': 'Fresh post-913 qualification. Preserve original594 pipeline and completed7cb native/profiles; serialize scale matrices; no historical cell substitutes.'}


def read(path):
    return json.loads(path.read_text()) if path.exists() else {}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save():
    temporary = RECEIPT.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.replace(RECEIPT)


def source_valid():
    return (
        subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORK, text=True).strip() == SOURCE['commit']
        and not subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORK, text=True)
        and all(digest(WORK / name) == expected for name, expected in SOURCE['source_files_sha256'].items())
    )


assert not RECEIPT.exists(), 'pipeline already recorded; do not duplicate'
try:
    save()
    while True:
        assert source_valid(), 'current-main worktree changed while waiting'
        statuses = {name: read(path).get('state', 'pending') for name, path in GATES.items()}
        data['prerequisites'] = statuses
        for name, status in statuses.items():
            if status in ['failed', 'source_changed']:
                raise RuntimeError('prerequisite failed: ' + name)
        original = read(BASE / 'hawdb-291-post-merge-verification.json')
        original_manifest = read(BASE / 'hawdb-291-incremental-repair-scale-evidence/manifest.json')
        scale_step = next((s for s in original.get('steps', []) if s['label'] == 'fresh-three-cell-scale'), {})
        cells = original_manifest.get('cells', [])
        data['original_matrix'] = {
            'pipeline_state': original.get('state'), 'step_state': scale_step.get('state'),
            'driver_pid': scale_step.get('pid'), 'exit_code': scale_step.get('exit_code'),
            'cells': [{'label': c['label'], 'state': c['state']} for c in cells],
        }
        terminal = (
            original.get('state') in ['passed', 'failed'] and scale_step.get('state') == 'finished'
            and [c['label'] for c in cells] == ['d81920-k10', 'd327680-k10', 'd327680-k100']
            and all(c['state'] == 'finished' for c in cells)
        )
        save()
        if all(status == 'passed' for status in statuses.values()) and terminal:
            break
        time.sleep(20)
    original_paths = [BASE / 'hawdb-291-post-merge-verification.json', BASE / 'hawdb-291-incremental-repair-scale-evidence/manifest.json']
    data['original_terminal_receipts'] = {str(p): digest(p) for p in original_paths}
    data['prerequisite_receipts'] = {name: {'path': str(p), 'sha256': digest(p)} for name, p in GATES.items()}
    script = BASE / 'hawdb-291-post913-scale-driver.py'
    log = BASE / 'hawdb-291-post913-scale-driver.log'
    command = ['python3', str(script)]
    step = {'label': 'fresh-three-cell-scale', 'state': 'running', 'command': command,
            'script_sha256': digest(script), 'log': str(log), 'started_unix_seconds': time.time()}
    data['state'] = 'fresh-three-cell-scale'
    data['steps'].append(step)
    save()
    with log.open('w') as output:
        process = subprocess.Popen(command, cwd=WORK, stdout=output, stderr=subprocess.STDOUT)
        step['pid'] = process.pid
        save()
        code = process.wait()
    step.update(state='finished', exit_code=code, elapsed_seconds=time.time() - step['started_unix_seconds'], log_sha256=digest(log))
    data['source_unchanged'] = source_valid()
    data['state'] = 'passed' if code == 0 and data['source_unchanged'] else 'failed'
    save()
except Exception as error:
    data.update(state='failed', error=str(error))
    save()
    raise
raise SystemExit(0 if data['state'] == 'passed' else 1)
