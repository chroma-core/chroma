"""Offline native build recipe, separate from the780s scientific envelope."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()

def verify(source,manifest):
    for name,item in manifest['files'].items():
        path=source/name
        if path.stat().st_size!=item['bytes'] or digest(path)!=item['sha256']:raise ValueError('source changed: '+name)
    for name,target in manifest['symlinks'].items():
        path=source/name
        if not path.is_symlink() or os.readlink(path)!=target or not path.resolve().is_relative_to(source.resolve()):raise ValueError('source symlink differs')

def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True);p.add_argument('--target',required=True);p.add_argument('--output',required=True);p.add_argument('--toolchain',required=True);a=p.parse_args()
    if sys.platform!='linux':raise ValueError('Linux build recipe only')
    source=Path(a.source).resolve();tool=Path(a.toolchain).resolve();out=Path(a.output).resolve();out.mkdir(exist_ok=False)
    if digest(tool/'rustc')!='028fd60b0e0add5505c661cd3ccde91393d615c8729bd95d94ab1e91409b36a1':raise ValueError('pinned Linux1.92compiler differs')
    manifest=json.loads(Path(__file__).with_name('source-manifest.json').read_text());verify(source,manifest)
    if shutil.disk_usage(source).free<20*1024**3:raise ValueError('20GiB free floor')
    env=dict(os.environ,CARGO_TARGET_DIR=a.target,CARGO_PROFILE_RELEASE_DEBUG='0',CARGO_INCREMENTAL='0',RUSTC=str(tool/'rustc'))
    command=[str(tool/'cargo'),'build','--release','--locked','--offline','-j2','-p','chroma-index','--bench','hierarchical_spann_shard','--message-format=json']
    before=time.monotonic()
    with (out/'stdout').open('xb') as stdout,(out/'stderr').open('xb') as stderr:
        result=subprocess.run(command,cwd=source,env=env,stdout=stdout,stderr=stderr,timeout=600)
    receipt=dict(command=command,returncode=result.returncode,elapsed_seconds=time.monotonic()-before,compiler_sha256=digest(tool/'rustc'),source_manifest_sha256=digest(Path(__file__).with_name('source-manifest.json')))
    verify(source,manifest)
    if result.returncode==0:
        artifacts=[]
        for line in (out/'stdout').read_text().splitlines():
            try:item=json.loads(line)
            except ValueError:continue
            if item.get('reason')=='compiler-artifact' and item.get('target',{}).get('name')=='hierarchical_spann_shard' and item.get('executable'):artifacts.append(item)
        if len(artifacts)!=1 or artifacts[0]['profile']['opt_level']!='3':raise ValueError('ambiguous/nonoptimized compiler artifact')
        built=Path(artifacts[0]['executable']);shutil.copyfile(built,out/'native');(out/'native').chmod(0o755)
        receipt['binary']=dict(path=str(out/'native'),bytes=(out/'native').stat().st_size,sha256=digest(out/'native'))
        receipt['cargo_artifact']=artifacts[0]
    (out/'result.json').write_text(json.dumps(receipt,indent=2)+'\n');raise SystemExit(result.returncode)
if __name__=='__main__':main()
