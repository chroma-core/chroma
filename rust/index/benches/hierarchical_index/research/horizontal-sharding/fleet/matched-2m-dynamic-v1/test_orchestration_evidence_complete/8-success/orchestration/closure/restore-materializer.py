import json,sys,hashlib
from pathlib import Path
s=json.loads(Path(sys.argv[2]).read_text());p=Path(s['attempt_dir']);p.mkdir();
if s.get('fake_exit'):raise SystemExit(s['fake_exit'])
c=p/'canonical.f32';c.write_bytes(bytes(range(24)));d=dict(path=str(c),bytes=24,sha256=hashlib.sha256(c.read_bytes()).hexdigest());r=dict(status='complete',canonical=d,rows=3,dimension=2,expected_bytes=24,deadline_unix=s['deadline_unix'],min_free_bytes=s['min_free_bytes']);(p/'receipt.json').write_text(json.dumps(r));(p/'runtime-provenance.json').write_text(json.dumps(dict(r,fixture=True)));(p/'object-0000.json').write_text('{}')
