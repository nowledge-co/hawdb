import hashlib,json,os,re,shutil,subprocess,time
from pathlib import Path
root=Path("/private/tmp/hawdb-291-post913-qualification")
output=Path("/private/tmp/hawdb-291-post913-fuzz-evidence");output.mkdir(parents=True,exist_ok=True)
receipt=output/"receipt.json"
source=json.loads(Path("/private/tmp/hawdb-291-post913-source.json").read_text())
def unchanged():
    names=set(subprocess.check_output(["git","ls-files","-z","--cached","--others","--exclude-standard"],cwd=root).decode().split("\0"))-{''}
    return names==set(source["source_files_sha256"]) and all((root/n).is_file() and hashlib.sha256((root/n).read_bytes()).hexdigest()==h for n,h in source["source_files_sha256"].items())
assert unchanged()
base=["bazel","--output_base=/private/tmp/hawdb-291-post913-bazel"]
testlogs=Path(subprocess.check_output(base+["info","bazel-testlogs"],cwd=root,text=True).strip())
assert testlogs.is_absolute()
query=base+["query","tests(//crates/fuzz:hawdb_fuzz_tests + //crates/fuzz:hawdb_fuzz_cli_tests + //:hawdb_linux_ci_fuzz_smoke_test)"]
q=subprocess.run(query,cwd=root,capture_output=True,text=True)
(output/"query.stdout").write_text(q.stdout);(output/"query.stderr").write_text(q.stderr)
assert q.returncode==0,q.stderr
labels=sorted(line for line in q.stdout.splitlines() if line.startswith("//"))
assert len(labels)==96,(len(labels),labels)
command=base+["test","//crates/fuzz:hawdb_fuzz_tests","//crates/fuzz:hawdb_fuzz_cli_tests","//:hawdb_linux_ci_fuzz_smoke_test","--local_test_jobs=1","--test_output=errors"]
log=output/"command.log"
data={"state":"running","source_receipt":"/private/tmp/hawdb-291-post913-source.json","query":query,"command":command,"targets":labels,"log":str(log)}
receipt.write_text(json.dumps(data,indent=2)+"\n")
print("running",len(labels),"targets",flush=True)
started=time.monotonic()
with log.open("w") as stream:run=subprocess.run(command,cwd=root,stdout=stream,stderr=subprocess.STDOUT)
data.update(exit_code=run.returncode,elapsed_seconds=time.monotonic()-started,source_unchanged=unchanged(),log_sha256=hashlib.sha256(log.read_bytes()).hexdigest(),artifacts=[],execution_summary=re.findall(r"Executed (\d+) out of (\d+) tests?: (.+)",log.read_text()))
receipt.write_text(json.dumps(data,indent=2)+"\n")
errors=[]
for label in labels:
    package,target=label[2:].split(":",1);directory=Path(package)/target;assert not directory.is_absolute() and ".." not in directory.parts;source_dir=testlogs/directory;dest=output/directory;dest.mkdir(parents=True,exist_ok=True)
    entry={"target":label,"files":[]}
    for name in ("test.log","test.xml"):
        s=source_dir/name;d=dest/name
        if not s.is_file():errors.append("missing "+str(s));continue
        shutil.copyfile(s,d);raw=d.read_bytes();entry["files"].append({"path":str(d),"sha256":hashlib.sha256(raw).hexdigest(),"bytes":len(raw)})
        if name=="test.log":
            summaries=re.findall(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored",raw.decode(errors="replace"));entry["test_summaries"]=summaries
            if label!="//:hawdb_linux_ci_fuzz_smoke_test" and (not summaries or any(s[0]!="ok" or int(s[1])==0 or int(s[2])!=0 or int(s[3])!=0 for s in summaries)):errors.append("invalid actual counts "+label)
    data["artifacts"].append(entry)
data["errors"]=errors;data["state"]="passed" if run.returncode==0 and data["source_unchanged"] and not errors else "failed"
receipt.write_text(json.dumps(data,indent=2)+"\n");print(data["state"],"exit",run.returncode,"errors",errors,flush=True)
raise SystemExit(0 if data["state"]=="passed" else 1)
