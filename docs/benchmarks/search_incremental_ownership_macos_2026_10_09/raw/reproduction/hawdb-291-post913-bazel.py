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
ROOT = BASE / 'hawdb-291-post913-bazel-evidence'
SOURCE_PATH = BASE / 'hawdb-291-post913-source.json'
SOURCE = json.loads(SOURCE_PATH.read_text())
BAZEL = ['bazel', '--output_base=/private/tmp/hawdb-291-post913-bazel']
TARGETS = [
    '//crates/search:hawdb_search_tests',
    '//crates/search:hawdb_search_acl_tests',
    '//crates/search:hawdb_search_text_background_tests',
    '//crates/search:hawdb_search_text_only_tests',
    '//crates/search:hawdb_search_text_vector_tests',
]
COMMAND = BAZEL + ['test'] + TARGETS + ['--local_test_jobs=1', '--test_output=errors']
data = {'state': 'running', 'commit': SOURCE['commit'], 'source_receipt': str(SOURCE_PATH),
        'command': COMMAND, 'started_unix_seconds': time.time(), 'targets': []}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save():
    path = ROOT / 'receipt.json'
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, indent=2) + '\n')
    temporary.replace(path)


def unchanged():
    return (
        subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORK, text=True).strip() == SOURCE['commit']
        and subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORK, text=True) == ''
        and all(sha(WORK / name) == value for name, value in SOURCE['source_files_sha256'].items())
    )


ROOT.mkdir(exist_ok=False)
try:
    assert unchanged(), 'source differs from frozen current-main receipt'
    save()
    log = ROOT / 'full-five-profiles.log'
    with log.open('w') as stream:
        process = subprocess.Popen(COMMAND, cwd=WORK, stdout=stream, stderr=subprocess.STDOUT)
        data['pid'] = process.pid
        save()
        code = process.wait()
    data.update(exit_code=code, log=str(log), log_sha256=sha(log), elapsed_seconds=time.time() - data['started_unix_seconds'])
    save()
    info_log = ROOT / 'bazel-testlogs-info.log'
    info = subprocess.run(BAZEL + ['info', 'bazel-testlogs'], cwd=WORK, text=True, capture_output=True)
    info_log.write_text(info.stdout + info.stderr)
    data['info_exit_code'] = info.returncode
    if info.returncode:
        raise RuntimeError('cannot resolve actual Bazel testlogs directory')
    testlogs = Path(info.stdout.strip())
    assert testlogs.is_absolute() and testlogs.is_dir()
    data['bazel_testlogs_directory'] = str(testlogs)
    complete = True
    for target in TARGETS:
        package, name = target[2:].split(':', 1)
        original = testlogs / package / name
        archive = ROOT / 'testlogs' / package / name
        archive.mkdir(parents=True, exist_ok=False)
        row = {'target': target, 'artifacts': {}}
        data['targets'].append(row)
        for filename in ['test.log', 'test.xml']:
            source = original / filename
            destination = archive / filename
            shutil.copy2(source, destination)
            row['artifacts'][filename] = {'path': str(destination), 'sha256': sha(destination)}
        raw = (archive / 'test.log').read_text()
        summaries = re.findall(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;', raw)
        xml = ET.parse(archive / 'test.xml').getroot()
        row['test_summaries'] = summaries
        row['xml_cases'] = len(list(xml.iter('testcase')))
        row['xml_failures'] = len(list(xml.iter('failure'))) + len(list(xml.iter('error')))
        valid = bool(summaries) and all(s[0] == 'ok' and int(s[1]) > 0 and int(s[2]) == 0 and int(s[3]) == 20 for s in summaries)
        valid = valid and row['xml_cases'] > 0 and row['xml_failures'] == 0
        witnesses = {}
        for module, minimum in [('initial', 6), ('incremental_ownership', 6), ('compaction', 20)]:
            count = len(re.findall(r'^test out_of_core::generation_writer::tests::' + module + r'::[^\n]* \.\.\. ok$', raw, re.M))
            witnesses[module] = count
            valid = valid and count >= minimum
        for name in [
            'tier_configuration_bounds_normal_merges_without_starting_background_work',
            'crisis_merge_uses_a_bounded_pair_and_does_not_exceed_the_top_level',
            'crisis_merge_reduces_mixed_levels_and_preserves_mutations_and_pins',
            'crisis_merge_chooses_the_smallest_pair_before_a_mixed_level_pair',
            'cancelled_compaction_preserves_the_active_manifest',
            'staged_compaction_rejects_a_newer_active_generation',
        ]:
            present = re.search(r'^test out_of_core::generation_writer::tests::compaction::' + name + r' \.\.\. ok$', raw, re.M) is not None
            witnesses[name] = present
            valid = valid and present
        row.update(state='passed' if valid else 'failed', witnesses=witnesses)
        complete = complete and valid
        save()
    data['source_unchanged'] = unchanged()
    data['state'] = 'passed' if code == 0 and complete and data['source_unchanged'] else 'failed'
    save()
except Exception as error:
    data.update(state='failed', error=str(error))
    save()
    raise
raise SystemExit(0 if data['state'] == 'passed' else 1)
