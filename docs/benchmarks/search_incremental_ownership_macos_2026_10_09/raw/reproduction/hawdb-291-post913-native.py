import hashlib
import json
import os
import re
import subprocess
import time
from pathlib import Path

base = Path('/private/tmp')
root = base / 'hawdb-291-post913-qualification'
source_path = base / 'hawdb-291-post913-source.json'
source = json.loads(source_path.read_text())
receipt = base / 'hawdb-291-post913-native.json'
env = os.environ.copy()
env.update(CARGO_TARGET_DIR='/private/tmp/hawdb-291-incremental-repair-native-target', RUSTUP_TOOLCHAIN='1.97.1')
data = {'state': 'running', 'source_receipt': str(source_path), 'commit': source['commit'], 'checks': []}

def save():
    temp = receipt.with_suffix('.tmp')
    temp.write_text(json.dumps(data, indent=2) + '\n')
    temp.replace(receipt)

def unchanged():
    return subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True) == '' and all(hashlib.sha256((root / n).read_bytes()).hexdigest() == h for n, h in source['source_files_sha256'].items())

checks = [
    ('maintenance-facade', ['cargo', 'test', '--locked', '-p', 'hawdb', '--all-features', '--lib', 'incremental_maintenance', '--', '--test-threads=1']),
    ('rebuild-facade', ['cargo', 'test', '--locked', '-p', 'hawdb', '--all-features', '--lib', 'api::tests::search_projection_rebuild_facade::', '--', '--test-threads=1']),
    ('power-loss', ['cargo', 'test', '--locked', '-p', 'hawdb', '--all-features', '--lib', 'api::tests::power_loss::search_projection::', '--', '--test-threads=1', '--nocapture']),
]
data['state'] = 'waiting-for-original7cb-native'
assert unchanged()
save()
while json.loads((base / 'hawdb-291-current-main-native.json').read_text()).get('state') == 'running':
    time.sleep(20)
data['state'] = 'running'
save()
for label, command in checks:
    assert unchanged()
    log = base / ('hawdb-291-post913-' + label + '.log')
    row = {'label': label, 'state': 'running', 'command': command, 'log': str(log)}
    data['checks'].append(row)
    save()
    started = time.monotonic()
    with log.open('w') as output:
        run = subprocess.Popen(command, cwd=root, env=env, stdout=output, stderr=subprocess.STDOUT)
        row['pid'] = run.pid
        save()
        code = run.wait()
    text = log.read_text()
    counts = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)
    valid = code == 0 and counts and all(c[0] == 'ok' and int(c[1]) > 0 and int(c[2]) == int(c[3]) == 0 for c in counts)
    if label == 'power-loss':
        plans = re.findall(r'^search-power-(?:publication|initial)-v1 .*\bplans=(\d+)', text, re.M)
        row.update(cut_families=len(plans), fault_plans=sum(map(int, plans)))
        valid = valid and counts[-1] == ('ok', '11', '0', '0') and len(plans) == 30 and row['fault_plans'] > 0
    row.update(state='passed' if valid else 'failed', exit_code=code, elapsed_seconds=time.monotonic() - started, test_summaries=counts, log_sha256=hashlib.sha256(log.read_bytes()).hexdigest())
    save()
    print(label, row['state'], counts, flush=True)
    if not valid:
        data['state'] = 'failed'
        save()
        raise SystemExit(1)
data['source_unchanged'] = unchanged()
data['state'] = 'passed' if data['source_unchanged'] else 'source_changed'
save()
raise SystemExit(0 if data['state'] == 'passed' else 1)
