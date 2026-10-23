from pathlib import Path
import os,json,subprocess,hashlib,shutil,time
out=Path(__file__).resolve().parent;root=Path('/private/tmp/owner32-real-cargo-validity-v7');root.mkdir(exist_ok=False);target=root/'target';cargo='/Users/chroma/.rustup/toolchains/1.92.0-aarch64-apple-darwin/bin/cargo';env=dict(os.environ,RUSTC='/Users/chroma/.rustup/toolchains/1.92.0-aarch64-apple-darwin/bin/rustc',CARGO_PROFILE_RELEASE_DEBUG='0',CARGO_TARGET_DIR=str(target));records=[]
a=root/'epoch-a';(a/'benches').mkdir(parents=True);(a/'Cargo.toml').write_text('[package]\nname="freshness-witness"\nversion="0.1.0"\nedition="2021"\n[[bench]]\nname="witness"\nharness=false\n');(a/'benches/witness.rs').write_text('mod value; fn main(){println!("{}",value::value());}\n');(a/'benches/value.rs').write_text('pub fn value()->u64{1}\n')
for p in a.rglob('*'):
 if p.is_file():os.utime(p,ns=(1700000000000000000,1700000000000000000))
def build(folder,label):
 command=[cargo,'build','--release','--locked','--offline','--bench','witness','--message-format=json'];result=subprocess.run(command,cwd=folder,env=env,capture_output=True,timeout=30);(out/(label+'.stdout')).write_bytes(result.stdout);(out/(label+'.stderr')).write_bytes(result.stderr);assert result.returncode==0,result.stderr.decode();artifacts=[v for line in result.stdout.splitlines()if(v:=json.loads(line)).get('reason')=='compiler-artifact'and v['target']['name']=='witness'];assert len(artifacts)==1;artifact=artifacts[0];binary=Path(artifact['executable']);behavior=subprocess.check_output([binary],timeout=3).decode().strip();r=dict(label=label,source_root=str(folder),source_mtime_ns=(folder/'benches/value.rs').stat().st_mtime_ns,entry_mtime_ns=(folder/'benches/witness.rs').stat().st_mtime_ns,fresh=artifact['fresh'],binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),behavior=behavior);records.append(r);return r
subprocess.run([cargo,'generate-lockfile','--offline'],cwd=a,env=env,check=True,capture_output=True,timeout=10)
first=build(a,'baseline');b=root/'epoch-b';shutil.copytree(a,b,copy_function=shutil.copy2);value=b/'benches/value.rs';value.write_text('pub fn value()->u64{2}\n');os.utime(value,ns=(1700000000000000000,1700000000000000000));second=build(b,'relocated-preserved-mtime')
# Refresh both changed module and consuming benchmark, not content or target cache.
stamp=time.time_ns();os.utime(value,ns=(stamp,stamp));os.utime(b/'benches/witness.rs',ns=(stamp,stamp));third=build(b,'relocated-refreshed')
assert first['behavior']=='1'and third['behavior']=='2'and third['fresh']is False and third['binary_sha256']!=first['binary_sha256']
(out/'real-cargo-result.json').write_text(json.dumps(dict(records=records,stale_reproduced=second['behavior']=='1'and second['fresh']is True),indent=2)+'\n');print(json.dumps(records))
