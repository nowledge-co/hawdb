import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-post913-qualification'
SOURCE = json.loads((BASE / 'hawdb-291-post913-source.json').read_text())
RECEIPT = BASE / 'hawdb-291-post913-required-checks.json'
data = {'state': 'waiting-for-native', 'commit': SOURCE['commit'], 'source_receipt': str(BASE / 'hawdb-291-post913-source.json'), 'checks': []}


def save():
    temporary = RECEIPT.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.replace(RECEIPT)


def unchanged():
    return not subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORK, text=True) and all(hashlib.sha256((WORK / name).read_bytes()).hexdigest() == value for name, value in SOURCE['source_files_sha256'].items())


assert not RECEIPT.exists()
try:
    save()
    while True:
        native = json.loads((BASE / 'hawdb-291-post913-native.json').read_text())
        if native['state'] in ['failed', 'source_changed']:
            raise RuntimeError('native source qualification failed')
        if native['state'] == 'passed':
            break
        assert unchanged()
        time.sleep(20)
    env = os.environ.copy()
    env.update(RUSTUP_TOOLCHAIN='1.97.1', CARGO_TARGET_DIR='/private/tmp/hawdb-291-incremental-repair-native-target')
    ar = '/Users/hawkingrei/.rustup/toolchains/1.97.1-aarch64-apple-darwin/lib/rustlib/aarch64-apple-darwin/bin/llvm-ar'
    linux = dict(env, CARGO_TARGET_DIR='/private/tmp/hawdb-979-linux-clippy-target',
                 CC_aarch64_unknown_linux_gnu='/private/tmp/hawdb-979-cross-tools/linux-cc',
                 CXX_aarch64_unknown_linux_gnu='/private/tmp/hawdb-979-cross-tools/linux-cxx',
                 AR_aarch64_unknown_linux_gnu=ar,
                 CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER='/private/tmp/hawdb-979-cross-tools/linux-cc')
    wasm = dict(env, CARGO_TARGET_DIR='/private/tmp/hawdb-979-wasm-clippy-target', CC_wasm32_unknown_unknown='clang', AR_wasm32_unknown_unknown=ar)
    checks = [
        ('fmt', ['cargo', 'fmt', '--all', '--', '--check'], env),
        ('strict-workspace', ['cargo', 'clippy', '--locked', '--workspace', '--all-targets', '--all-features', '--', '-D', 'warnings'], env),
        ('install-hooks', ['prek', 'install'], env),
        ('explicit-hooks', ['prek', 'run', '--all-files'], env),
        ('linux-cross-strict', ['cargo', 'clippy', '--locked', '--workspace', '--all-targets', '--all-features', '--target', 'aarch64-unknown-linux-gnu', '--', '-D', 'warnings'], linux),
        ('minimal-browser-strict', ['cargo', 'clippy', '--locked', '-p', 'hawdb', '--no-default-features', '--target', 'wasm32-unknown-unknown', '--lib', '--test', 'in_memory_portable', '--', '-D', 'warnings'], wasm),
    ]
    data['state'] = 'running'
    save()
    for label, command, task_env in checks:
        assert unchanged()
        path = BASE / ('hawdb-291-post913-' + label + '.log')
        row = {'label': label, 'state': 'running', 'command': command, 'log': str(path)}
        data['checks'].append(row)
        save()
        started = time.monotonic()
        with path.open('w') as stream:
            process = subprocess.Popen(command, cwd=WORK, env=task_env, stdout=stream, stderr=subprocess.STDOUT)
            row['pid'] = process.pid
            save()
            code = process.wait()
        row.update(state='passed' if code == 0 else 'failed', exit_code=code, elapsed_seconds=time.monotonic() - started, log_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        save()
        if code:
            raise RuntimeError(label + ' failed')
    data['source_unchanged'] = unchanged()
    data['state'] = 'passed' if data['source_unchanged'] else 'failed'
    save()
except Exception as error:
    data.update(state='failed', error=str(error))
    save()
    raise
raise SystemExit(0 if data['state'] == 'passed' else 1)
