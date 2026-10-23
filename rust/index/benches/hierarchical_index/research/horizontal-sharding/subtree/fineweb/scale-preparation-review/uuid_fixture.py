"""Bounded local UUID metadata experiment; this does not process corpus vectors."""
import argparse,hashlib,heapq,json,os,resource,sqlite3,tempfile,time
from pathlib import Path

def unique_sqlite(source,root):
 db=sqlite3.connect(root/'index.sqlite');db.execute('PRAGMA cache_size=-8192');db.execute('CREATE TABLE docs(uuid TEXT PRIMARY KEY, row INTEGER NOT NULL UNIQUE) WITHOUT ROWID')
 try:
  with source.open() as f:
   batch=[]
   for line in f:
    row=json.loads(line);batch.append((row['source_uuid'],row['global_id']))
    if len(batch)==4096:db.executemany('INSERT INTO docs VALUES (?,?)',batch);db.commit();batch=[]
   if batch:db.executemany('INSERT INTO docs VALUES (?,?)',batch);db.commit()
 finally:db.close()

def unique_merge(source,root,run_rows=8192):
 runs=[]
 with source.open() as f:
  batch=[]
  def flush():
   path=root/('run-'+str(len(runs))+'.jsonl');runs.append(path)
   with path.open('w') as out:
    for value in sorted(batch):out.write(json.dumps(value)+'\n')
  for line in f:
   row=json.loads(line);batch.append((row['source_uuid'],row['global_id']))
   if len(batch)==run_rows:flush();batch=[]
  if batch:flush()
 streams=[p.open() for p in runs]
 try:
  merged=heapq.merge(*((tuple(json.loads(line)) for line in f) for f in streams));last=None;count=0
  for value in merged:
   if value[0]==last:raise ValueError('duplicate source UUID')
   last=value[0];count+=1
  return count
 finally:
  for f in streams:f.close()

def main():
 p=argparse.ArgumentParser();p.add_argument('--rows',type=int,default=100000);p.add_argument('--duplicate',action='store_true');p.add_argument('--method',choices=['sqlite','merge'],required=True);a=p.parse_args()
 if not 1<=a.rows<=200000:raise ValueError('fixture is limited to200000 metadata rows')
 with tempfile.TemporaryDirectory(prefix='uuid-review-') as d:
  root=Path(d);source=root/'source.jsonl'
  with source.open('w') as f:
   for i in range(a.rows):
    identity=0 if a.duplicate and i==a.rows-1 else i
    token=hashlib.blake2b(str(identity).encode(),digest_size=16).hexdigest()
    f.write(json.dumps(dict(global_id=i,source_uuid='urn:uuid:'+token))+'\n')
  start=time.monotonic();cpu=time.process_time();status='passed'
  try:(unique_sqlite if a.method=='sqlite' else unique_merge)(source,root)
  except (ValueError,sqlite3.IntegrityError):status='duplicate_rejected'
  report=dict(method=a.method,rows=a.rows,duplicate=a.duplicate,status=status,wall_s=time.monotonic()-start,cpu_s=time.process_time()-cpu,peak_rss_platform_units=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,source_bytes=source.stat().st_size,output_bytes=sum(x.stat().st_size for x in root.iterdir() if x!=source),fixture_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
  assert status==('duplicate_rejected' if a.duplicate and a.rows>1 else 'passed');print(json.dumps(report))
if __name__=='__main__':main()
