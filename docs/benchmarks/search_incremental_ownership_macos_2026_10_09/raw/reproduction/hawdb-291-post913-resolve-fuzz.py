import hashlib
import json
import re
import shutil
import subprocess
import time
from pathlib import Path

root = Path('/private/tmp/hawdb-291-post913-qualification')
initial_path = Path('/private/tmp/hawdb-291-post913-fuzz-evidence/receipt.json')
out = Path('/private/tmp/hawdb-291-post913-fuzz-resolved')
out.mkdir(parents=True, exist_ok=True)
receipt = out / 'receipt.json'
source_path = Path('/private/tmp/hawdb-291-post913-source.json')
source = json.loads(source_path.read_text())
base = ['bazel', '--output_base=/private/tmp/hawdb-291-post913-bazel']
data = {'state': 'waiting_for_initial_run', 'source_receipt': str(source_path), 'initial_receipt': str(initial_path), 'checks': []}

def save():
    receipt.write_text(json.dumps(data, indent=2) + '\n')

def unchanged():
    names = set(subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=root).decode().split('\0')) - {''}
    return names == set(source['source_files_sha256']) and all((root / n).is_file() and hashlib.sha256((root / n).read_bytes()).hexdigest() == h for n, h in source['source_files_sha256'].items())

def valid_summaries(summaries):
    return bool(summaries) and all(s[0] == 'ok' and int(s[1]) > 0 and int(s[2]) == 0 and int(s[3]) == 0 for s in summaries)

save()
while True:
    initial = json.loads(initial_path.read_text())
    if initial['state'] != 'running':
        break
    time.sleep(5)
assert initial['state'] == 'failed', initial['state']
assert initial['source_unchanged'] and unchanged()
assert initial['exit_code'] != 0
failed = []
for artifact in initial['artifacts']:
    if artifact['target'] != '//:hawdb_linux_ci_fuzz_smoke_test' and not valid_summaries(artifact.get('test_summaries', [])):
        failed.append(artifact)
assert failed and len(initial['artifacts']) == 96
for artifact in failed:
    paths = [Path(f['path']) for f in artifact['files'] if f['path'].endswith('test.log')]
    assert len(paths) == 1 and '-- Test timed out at ' in paths[0].read_text(), artifact['target']
data.update(state='isolated_retry', initial_exit_code=initial['exit_code'], initial_passed_targets=96 - len(failed), timeout_targets=[a['target'] for a in failed], initial_receipt_sha256=hashlib.sha256(initial_path.read_bytes()).hexdigest())
save()
testlogs = Path(subprocess.check_output(base + ['info', 'bazel-testlogs'], cwd=root, text=True).strip())
assert testlogs.is_absolute()
for artifact in failed:
    label = artifact['target']
    package, target = label[2:].split(':', 1)
    directory = Path(package) / target
    assert not directory.is_absolute() and '..' not in directory.parts
    destination = out / directory
    destination.mkdir(parents=True, exist_ok=True)
    log = destination / 'command.log'
    command = base + ['test', label, '--local_test_jobs=1', '--test_output=errors']
    check = {'target': label, 'state': 'running', 'command': command, 'log': str(log), 'unchanged_deadline': True}
    data['checks'].append(check)
    save()
    started = time.monotonic()
    with log.open('w') as stream:
        result = subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT)
    check.update(exit_code=result.returncode, elapsed_seconds=time.monotonic() - started, artifacts=[])
    save()
    for name in ('test.log', 'test.xml'):
        src = testlogs / directory / name
        assert src.is_file(), src
        dest = destination / name
        shutil.copyfile(src, dest)
        raw = dest.read_bytes()
        check['artifacts'].append({'path': str(dest), 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)})
        if name == 'test.log':
            check['test_summaries'] = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored', raw.decode(errors='replace'))
    check['state'] = 'passed' if result.returncode == 0 and valid_summaries(check.get('test_summaries', [])) and unchanged() else 'failed'
    save()
    print('isolated retry', label, check['state'], check.get('test_summaries'), flush=True)
    if check['state'] != 'passed':
        data['state'] = 'failed'
        save()
        raise SystemExit(1)

data['state'] = 'final_required_command'
save()
command = initial['command']
log = out / 'final-command.log'
started = time.monotonic()
with log.open('w') as stream:
    result = subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT)
raw = log.read_bytes()
data.update(final_command=command, final_log=str(log), final_exit_code=result.returncode, final_elapsed_seconds=time.monotonic() - started, final_log_sha256=hashlib.sha256(raw).hexdigest(), source_unchanged=unchanged())
data['execution_summary'] = re.findall(r'Executed (\d+) out of (\d+) tests?: (.+)', raw.decode(errors='replace'))
assert data['execution_summary'], raw[-2000:]
data['state'] = 'passed' if result.returncode == 0 and data['source_unchanged'] else 'failed'
data['qualification_scope'] = 'Keep the original complete-run timeout. Successful qualification combines its passing targets with fresh isolated retries at original seeds, caps and deadlines; the final exact required command may reuse cached results and is not claimed as 96 fresh successful executions.'
save()
print(data['state'], data['execution_summary'], flush=True)
raise SystemExit(0 if data['state'] == 'passed' else 1)
