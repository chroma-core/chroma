"""Assemble only immutable historical archive plus accepted frozen replacements."""
import hashlib,json,tarfile,tempfile,shutil,io
from pathlib import Path
HERE=Path(__file__).resolve().parent
ADAPTER=HERE.parent/'matched-reference-adapter-v1'
BASE=HERE.parents[2]/'reference-3m-20261017/reference-source.tar.gz'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def normalized(m):return {'files':{r['path']:{'bytes':r['bytes'],'sha256':r['sha256']}for r in m['members']if 'sha256'in r},'symlinks':{'rust/types/idl':'../../idl'}}
def main():
 hand=json.loads((ADAPTER/'HANDOFF.json').read_text());new=normalized(json.loads((ADAPTER/'source-manifest.json').read_text()));old=normalized(json.loads((ADAPTER/'base-manifest.json').read_text()))
 assert sha(BASE)==hand['base_archive_sha256']
 write(HERE/'source-manifest.json',new);write(HERE/'baseline-source-manifest.json',old)
 with tempfile.TemporaryDirectory()as tmp:
  root=Path(tmp)/'source';root.mkdir()
  with tarfile.open(BASE)as t:t.extractall(root)
  for r in hand['changed_files']:
   source=ADAPTER/'frozen-source'/r['path'];assert sha(source)==r['sha256'];dest=root/r['path'];dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source,dest)
  actual={p.relative_to(root).as_posix()for p in root.rglob('*')if p.is_file()and not p.is_symlink()}
  assert actual==set(new['files'])
  for n,r in new['files'].items():assert sha(root/n)==r['sha256']and(root/n).stat().st_size==r['bytes']
  assert {p.relative_to(root).as_posix():p.readlink().as_posix()for p in root.rglob('*')if p.is_symlink()}==new['symlinks']
  with tarfile.open(HERE/'source.tar.gz','w:gz',dereference=False)as t:
   for name in sorted(actual|set(new['symlinks'])):t.add(root/name,arcname=name,recursive=False)
  protocol=json.loads((HERE/'PROTOCOL.json').read_text());protocol['status']='Frozen local package awaiting root runtime review';protocol['source_plan']['source_archive_sha256']=sha(HERE/'source.tar.gz');protocol['source_plan']['source_manifest_sha256']=sha(HERE/'source-manifest.json');protocol['membership_gate']='Exactly two structured precommit/postreopen records require100000 unique IDs and current rows, zero duplicate current postings; saved bit digest separately checked. Precommit validation warms postings and its time is separate.';write(HERE/'PROTOCOL.json',protocol)
  write(HERE/'SOURCE-CONTRACT.json',{'accepted':True,'source_manifest_sha256':sha(HERE/'source-manifest.json'),'baseline_manifest_sha256':sha(HERE/'baseline-source-manifest.json'),'source_archive_sha256':sha(HERE/'source.tar.gz'),'changes':[r['path']for r in hand['changed_files']],'membership_uniqueness_review_sha256':sha(ADAPTER/'root-review.json'),'protocol_sha256':sha(HERE/'PROTOCOL.json')})
  files=['run_reference.py','supervisor_core.py','process_lifecycle.py','extract_package.py','test_reference.py','SOURCE-CONTRACT.json','PROTOCOL.json','source-manifest.json','baseline-source-manifest.json']
  with tarfile.open(HERE/'runtime-package.tar.gz','w:gz',dereference=False)as t:
   for name in files:t.add(HERE/name,arcname=name)
   for name in sorted(actual|set(new['symlinks'])):t.add(root/name,arcname='source/'+name,recursive=False)
 from extract_package import extract
 with tempfile.TemporaryDirectory()as tmp:extract(HERE/'runtime-package.tar.gz',Path(tmp)/'fresh')
 write(HERE/'package-validation.json',{'regular_source_files':len(new['files']),'symlinks':new['symlinks'],'exact_changed_files':[r['path']for r in hand['changed_files']],'full_package_extraction_and_inventory':'pass','package_sha256':sha(HERE/'runtime-package.tar.gz'),'package_bytes':(HERE/'runtime-package.tar.gz').stat().st_size})
if __name__=='__main__':main()
