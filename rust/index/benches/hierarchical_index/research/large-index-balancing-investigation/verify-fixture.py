import pathlib,json,hashlib
out=pathlib.Path('/home/ubuntu/benchmark-data/large-index-balancing-investigation')
p=pathlib.Path('/mnt/data/wikipedia-40m-current')
files=sorted((str(f.relative_to(p)),f.stat().st_size,f.stat().st_mtime_ns,f.stat().st_ino) for f in p.rglob('*') if f.is_file())
after={'checkpoint':json.loads((p/'checkpoint.json').read_text()),'files':files}
before=json.loads((out/'40m-before.json').read_text())
encoded=json.dumps(after)
(out/'40m-after.json').write_text(encoded)
assert json.loads(encoded)==before, '40M original fixture metadata changed'
result={'unchanged':True,'file_count':len(files),'total_file_bytes':sum(row[1] for row in files),'checkpoint_sha256':hashlib.sha256((p/'checkpoint.json').read_bytes()).hexdigest(),'manifest_sha256':hashlib.sha256(encoded.encode()).hexdigest()}
(out/'40m-preservation.json').write_text(json.dumps(result,indent=2))
print(result)
