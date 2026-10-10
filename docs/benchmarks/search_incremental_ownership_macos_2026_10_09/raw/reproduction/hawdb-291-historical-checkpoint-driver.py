import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import time

WORKTREE=Path('/private/tmp/hawdb-291-historical-checkpoint-review')
EVIDENCE=Path('/private/tmp/hawdb-291-historical-checkpoint-evidence')
SCRATCH=Path('/private/tmp/hawdb-291-historical-checkpoint-scratch')
EVIDENCE.mkdir(exist_ok=True)
SCRATCH.mkdir(exist_ok=True)
REVISION='8cbe16f8f76f3472149b971970bda7d7511da3b8'
BINARY=Path('/private/tmp/hawdb-291-historical-checkpoint-probe')
SOURCE=Path('/private/tmp/hawdb-291-historical-checkpoint-probe.rs')
COMPILE=json.loads(Path('/private/tmp/hawdb-291-historical-probe-compile.json').read_text())
SMOKE=json.loads(Path('/private/tmp/hawdb-291-historical-smoke-four-bit.json').read_text())
assert COMPILE['compile_exit_code']==SMOKE['exit_code']==0
assert COMPILE['rabitq_bit_width']==4
assert hashlib.sha256(SOURCE.read_bytes()).hexdigest()==COMPILE['source_sha256']
assert hashlib.sha256(BINARY.read_bytes()).hexdigest()==COMPILE['binary_sha256']
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=WORKTREE,text=True).strip()==REVISION
assert not subprocess.check_output(['git','status','--porcelain'],cwd=WORKTREE,text=True)
for row in COMPILE['libraries'].values():assert hashlib.sha256(Path(row['rlib']).read_bytes()).hexdigest()==row['sha256']
NAMES=['Cargo.lock','Cargo.toml','rust-toolchain.toml','crates/search/src/out_of_core.rs','crates/search/src/out_of_core/generation_writer.rs','crates/search/src/out_of_core/generation_writer/delta.rs','crates/search/src/out_of_core/generation_writer/artifacts.rs','crates/search/src/out_of_core/generation_writer/publication.rs','crates/search/src/out_of_core/generation_writer/spool.rs','crates/vector-projection/src/model.rs']
HASHES={name:hashlib.sha256((WORKTREE/name).read_bytes()).hexdigest() for name in NAMES}
soft,hard=resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE,(4096,hard))
fixture=SCRATCH/('d327680-k10-'+str(time.time_ns()))
command=[str(BINARY),str(fixture),'327680','10']
manifest={
 'state':'running','revision':REVISION,'git_tree':subprocess.check_output(['git','rev-parse','HEAD^{tree}'],cwd=WORKTREE,text=True).strip(),
 'command':command,'compiler':subprocess.check_output(['rustc','-Vv'],cwd=WORKTREE,text=True),'profile':'release optimized3/codegen-units1/thinLTO','compile_receipt':COMPILE,'wrapper_smoke':SMOKE,
 'source_files_sha256':HASHES,'wrapper_source_sha256':COMPILE['source_sha256'],'wrapper_rust_source':SOURCE.read_text(),
 'host_platform':subprocess.check_output(['sw_vers'],text=True),'host_model':subprocess.check_output(['sysctl','-n','hw.model'],text=True).strip(),'host_ram_bytes':int(subprocess.check_output(['sysctl','-n','hw.memsize'],text=True)),
 'os_fd_limits':resource.getrlimit(resource.RLIMIT_NOFILE),'process_inspection':'None; internal process counters and passive scratch sampling only.',
 'configuration':{'base_documents':327680,'seed_documents':32,'content_bytes':65536,'embedding_dimension':384,'rabitq_bit_width':4,'rabitq_segment_rows':1024,'rabitq_transform_seed':0x534b45494e565134,'touches':10,'lexical_build_memory_bytes':8388608,'max_segment_uncompressed_bytes':67108864,'historical_complete_operation_reservation':'API unavailable; no256MiB RSS qualification is claimed for this historical baseline','historical_project_descriptor_admission':'API unavailable; process soft limit4096'},
 'scope':'Actual historical full-generation rewrite afterK10 changed-text/vector replacements at20GiB base body shape. Same base+32seed logical rows as the candidate pre-replacement state, with seed rows included in historical initialization to avoid32unrelated full rewrites. Formats and locked dependencies are revision-specific. This strengthens bytes-before/after evidence; it is not controlled same-format throughput or RSS comparison.',
 'started_unix_seconds':time.time(),'fixture':str(fixture),'scratch_sampling_interval_seconds':10,
}
path=EVIDENCE/'manifest.json'
def save():
 tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(manifest,indent=2)+'\n');tmp.replace(path)
def size():
 logical=allocated=0
 for directory,_,names in os.walk(SCRATCH):
  for name in names:
   try:stat=(Path(directory)/name).stat()
   except FileNotFoundError:continue
   logical+=stat.st_size;allocated+=stat.st_blocks*512
 return logical,allocated
save()
stdout=EVIDENCE/'d327680-k10.stdout.log';stderr=EVIDENCE/'d327680-k10.stderr.log'
with stdout.open('wb') as out,stderr.open('wb') as err:
 process=subprocess.Popen(command,cwd=WORKTREE,stdout=out,stderr=err)
 manifest['pid']=process.pid
 logical_high=allocated_high=0
 while process.poll() is None:
  logical,allocated=size();logical_high=max(logical_high,logical);allocated_high=max(allocated_high,allocated)
  manifest.update(elapsed_seconds=time.time()-manifest['started_unix_seconds'],scratch_logical_highwater_bytes=logical_high,scratch_allocated_highwater_bytes=allocated_high)
  save();time.sleep(10)
 code=process.wait()
manifest.update(state='finished',exit_code=code,elapsed_seconds=time.time()-manifest['started_unix_seconds'],stdout_sha256=hashlib.sha256(stdout.read_bytes()).hexdigest(),stderr_sha256=hashlib.sha256(stderr.read_bytes()).hexdigest(),source_files_sha256_after={name:hashlib.sha256((WORKTREE/name).read_bytes()).hexdigest() for name in NAMES},working_tree_status_after=subprocess.check_output(['git','status','--porcelain'],cwd=WORKTREE,text=True))
lines=[json.loads(line[len('historical_checkpoint '):]) for line in stdout.read_text().splitlines() if line.startswith('historical_checkpoint ')]
if len(lines)==1:
 (EVIDENCE/'d327680-k10.result.json').write_text(json.dumps(lines[0],indent=2)+'\n')
manifest['source_unchanged']=manifest['source_files_sha256_after']==HASHES and not manifest['working_tree_status_after']
save();print(json.dumps(manifest),flush=True)
if code!=0:raise SystemExit(code)
if len(lines)!=1 or not manifest['source_unchanged']:raise SystemExit(1)
