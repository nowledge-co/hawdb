import hashlib
import json
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-post913-qualification'
ROOT = BASE / 'hawdb-291-post913-bazel-phase-evidence'
SOURCE = json.loads((BASE / 'hawdb-291-post913-source.json').read_text())
BAZEL = ['bazel', '--output_base=/private/tmp/hawdb-291-post913-bazel']
data = {'state': 'waiting-for-old-profiles-and-fresh-native', 'commit': SOURCE['commit'], 'steps': [],
        'source_receipt': str(BASE / 'hawdb-291-post913-source.json')}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text()) if path.exists() else {}


def unchanged():
    return (
        subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORK, text=True).strip() == SOURCE['commit']
        and not subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORK, text=True)
        and all(sha(WORK / name) == value for name, value in SOURCE['source_files_sha256'].items())
    )


def save():
    path = ROOT / 'receipt.json'
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.replace(path)


def run(label, command):
    assert unchanged()
    log = ROOT / (label + '.log')
    row = {'label': label, 'state': 'running', 'command': command, 'log': str(log), 'started_unix_seconds': time.time()}
    data['steps'].append(row)
    data['state'] = label
    save()
    with log.open('w') as stream:
        process = subprocess.Popen(command, cwd=WORK, stdout=stream, stderr=subprocess.STDOUT)
        row['pid'] = process.pid
        save()
        code = process.wait()
    row.update(state='finished', exit_code=code, elapsed_seconds=time.time() - row['started_unix_seconds'], log_sha256=sha(log))
    save()
    return code


ROOT.mkdir(exist_ok=False)
try:
    assert unchanged()
    save()
    while True:
        old_profiles = read(BASE / 'hawdb-291-current-main-bazel-evidence/receipt.json')
        native = read(BASE / 'hawdb-291-post913-native.json')
        if native.get('state') in ['failed', 'source_changed']:
            raise RuntimeError('fresh post913 native verification failed')
        if old_profiles.get('state') in ['passed', 'failed'] and native.get('state') == 'passed':
            break
        assert unchanged()
        time.sleep(20)
    assert run('release-old-owned-bazel-server', ['bazel', '--output_base=/private/tmp/hawdb-fd-owner-bazel', 'shutdown']) == 0
    assert run('full-five-profiles', ['python3', str(BASE / 'hawdb-291-post913-bazel.py')]) == 0
    assert read(BASE / 'hawdb-291-post913-bazel-evidence/receipt.json')['state'] == 'passed'
    testlogs = Path(subprocess.check_output(BAZEL + ['info', 'bazel-testlogs'], cwd=WORK, text=True).strip())
    selections = [
        ('shared-fd-and-pinned-read', '//crates/storage:hawdb_storage_tests', ['file_descriptors::tests::'], 0),
        ('typed-readiness', '//crates/readiness:hawdb_readiness_tests', ['nowledge_mem_runtime_status::tests::'], 0),
        ('complete-root-recovery', '//:hawdb_unit_recovery_tests', [], 1),
        ('constrained-default-host', '//:hawdb_branch_project_open_tests', ['default_project_remains_usable_under_a_host_owned_descriptor_limit', '--exact'], 0),
        ('explicit-FD32-regression', '//:hawdb_branch_project_open_tests', ['live_writer_reclamation_retains_readers_descendants_and_budget_retries', '--exact'], 0),
    ]
    for label, target, filters, ignored in selections:
        command = BAZEL + ['test', target] + ['--test_arg=' + value for value in filters + ['--test-threads=1']] + ['--local_test_jobs=1', '--test_output=errors']
        code = run(label, command)
        package, name = target[2:].split(':', 1)
        destination = ROOT / label
        destination.mkdir()
        files = []
        for filename in ['test.log', 'test.xml']:
            path = destination / filename
            shutil.copyfile(testlogs / package / name / filename, path)
            files.append({'path': str(path), 'sha256': sha(path), 'bytes': path.stat().st_size})
        raw = (destination / 'test.log').read_text()
        counts = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
        xml = ET.parse(destination / 'test.xml').getroot()
        valid = code == 0 and bool(counts) and all(s[0] == 'ok' and int(s[1]) > 0 and int(s[2]) == 0 and int(s[3]) == ignored for s in counts)
        valid = valid and len(list(xml.iter('testcase'))) > 0 and not list(xml.iter('failure')) and not list(xml.iter('error'))
        row = data['steps'][-1]
        row.update(actual_test_summaries=counts, artifacts=files, actual_validation='passed' if valid else 'failed')
        save()
        assert valid, label
    initial_code = run('required96-fuzz', ['python3', str(BASE / 'hawdb-291-post913-fuzz.py')])
    initial_path = BASE / 'hawdb-291-post913-fuzz-evidence/receipt.json'
    initial = read(initial_path)
    assert initial.get('source_unchanged')
    if initial_code != 0:
        assert run('original-deadline-isolated-fuzz-retries', ['python3', str(BASE / 'hawdb-291-post913-resolve-fuzz.py')]) == 0
        resolved = read(BASE / 'hawdb-291-post913-fuzz-resolved/receipt.json')
        assert resolved['state'] == 'passed' and resolved['final_exit_code'] == 0
    else:
        assert initial['state'] == 'passed' and initial['exit_code'] == 0
    data['fuzz_outcome'] = 'original required command passed' if initial_code == 0 else 'original timeout retained; unchanged-deadline isolated retries and final required cached command passed'
    data['source_unchanged'] = unchanged()
    data['state'] = 'passed' if data['source_unchanged'] else 'failed'
    save()
except Exception as error:
    data.update(state='failed', error=str(error))
    save()
    raise
raise SystemExit(0 if data['state'] == 'passed' else 1)
