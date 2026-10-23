import pathlib,json,tarfile,hashlib
r=pathlib.Path('/home/ubuntu/owner3m');paths=[]
for d in [r,r/'cache1024',r/'smoke-1',r/'smoke-4',r/'four-reservation-check']:
 for p in d.iterdir():
  if p.is_file() and not p.is_symlink() and p.suffix in ['.json','.log','.time','.py','.sh'] and p.name not in ['queries.json','smoke-queries.json']:paths.append(p)
 for name in ['owner-1','owner-4']:
  for f in ['result.json','snapshot64.json']:
   p=d/name/f
   if p.is_file():paths.append(p)
x=json.loads((r/'four-reservation-check/result.json').read_text());v=x['after_save']['vector_store'];brief={'rows':x['census']['current_rows'],'status':x['status'],'add_seconds':x['add_seconds'],'balance_seconds':x['balance_seconds'],'placement_seconds':x['placement_seconds'],'save_seconds':x['save_seconds'],'audit_seconds':x['audit_seconds'],'split_working_peak_reserved_bytes':v['split_working_peak_reserved_bytes'],'split_working_rejections':v['split_working_rejections'],'process_memory':x['process_memory'],'owners':x['after_save']['owners'],'shape':x['after_save']['shape'],'full_records':x['after_save']['full_records'],'critical_wait_ns':x['after_save']['critical_wait_ns']}
p=r/'reservation-check-summary.json';p.write_text(json.dumps(brief,indent=2)+'\n');paths.append(p)
with tarfile.open(r/'small-evidence.tar.gz','w:gz') as t:
 for p in sorted(set(paths)):t.add(p,arcname=str(p.relative_to(r)),recursive=False)
p=r/'small-evidence.tar.gz';print(json.dumps({'archive_bytes':p.stat().st_size,'archive_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'reservation_check':{k:v for k,v in brief.items() if k not in ['shape','full_records']}}))
