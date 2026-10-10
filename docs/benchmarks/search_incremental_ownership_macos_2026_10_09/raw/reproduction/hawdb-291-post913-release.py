import hashlib,json,os,subprocess,time
from pathlib import Path
BASE=Path('/private/tmp')
WORKTREE=BASE/'hawdb-291-post913-qualification'
SOURCE_PATH=BASE/'hawdb-291-post913-source.json'
SOURCE=json.loads(SOURCE_PATH.read_text())
TARGET=BASE/'hawdb-291-post913-release-target'
RECEIPT=BASE/'hawdb-291-post913-release.json'
environment=os.environ.copy();environment['CARGO_TARGET_DIR']=str(TARGET);environment['RUSTUP_TOOLCHAIN']='1.97.1'
receipt={'state':'running','worktree':str(WORKTREE),'integrated_tree':SOURCE['integrated_tree'],'source_receipt':str(SOURCE_PATH),'source_receipt_sha256':hashlib.sha256(SOURCE_PATH.read_bytes()).hexdigest(),'target_directory':str(TARGET),'checks':[]}
def save():
    temporary=RECEIPT.with_suffix('.tmp');temporary.write_text(json.dumps(receipt,indent=2)+'\n');temporary.replace(RECEIPT)
def valid_source():
    return subprocess.check_output(['git','write-tree'],cwd=WORKTREE,text=True).strip()==SOURCE['integrated_tree'] and all(hashlib.sha256((WORKTREE/n).read_bytes()).hexdigest()==v for n,v in SOURCE['source_files_sha256'].items())
assert valid_source()
for label,command in [('library',['cargo','build','--locked','--release','--lib']),('bench',['cargo','bench','--locked','--bench','search_mutation','--no-run'])]:
    assert valid_source()
    log=BASE/('hawdb-291-post913-release-'+label+'.log');started=time.monotonic()
    row={'label':label,'state':'running','command':command,'log':str(log)};receipt['checks'].append(row);save()
    with log.open('wb') as stream:
        process=subprocess.Popen(command,cwd=WORKTREE,env=environment,stdout=stream,stderr=subprocess.STDOUT);row['pid']=process.pid;save();code=process.wait()
    row.update(state='finished',exit_code=code,elapsed_seconds=time.monotonic()-started,log_sha256=hashlib.sha256(log.read_bytes()).hexdigest());save();print(json.dumps(row),flush=True)
    if code:
        receipt['state']='failed';save();raise SystemExit(code)
receipt['source_unchanged']=valid_source();assert receipt['source_unchanged']
library=TARGET/'release/libhawdb.rlib';receipt['library']={'path':str(library),'sha256':hashlib.sha256(library.read_bytes()).hexdigest()}
receipt['bench_binaries']=[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in (TARGET/'release/deps').glob('search_mutation-*') if p.is_file() and p.suffix=='']
assert len(receipt['bench_binaries'])==1
receipt['state']='passed';save()
