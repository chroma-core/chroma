from run import P,H,B,ssh,put
import sys,json,shlex
import hashlib,time,concurrent.futures
sys.path.insert(0,str(P.parent));import lifecycle as l
who=l.identity();hosts=l.instances();assert all(v['State']['Name']=='running' for v in hosts.values())
worker='i-011ff180f03584f40';archives={}
ssh(worker,shlex.join(['mkdir','-p',B+'/source-snapshots']))
for name in ['source-epoch31v3.tar.gz','source-epoch31v4.tar.gz']:
 local=P.parent.parent/'packages'/name;h=hashlib.sha256(local.read_bytes()).hexdigest();dest=B+'/source-snapshots/'+name
 code_check="import pathlib,hashlib; p=pathlib.Path("+repr(dest)+"); print(hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else '')"
 if ssh(worker,'python3 -c '+shlex.quote(code_check)).strip()!=h:put(worker,local,dest)
 assert ssh(worker,shlex.join(['sha256sum',dest])).split()[0]==h;archives[name]=h
code='''import pathlib,json,hashlib,shutil,importlib.util
base=pathlib.Path('/home/ubuntu/spann-two-machine-v1/development30');active=[]
s=importlib.util.spec_from_file_location('g','/home/ubuntu/spann-two-machine-v1/epoch29c/remote-job.py');g=importlib.util.module_from_spec(s);s.loader.exec_module(g);ident=g.imds('INSTANCE')
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:args=[a.decode(errors='replace') for a in (p/'cmdline').read_bytes().split(b'\\0') if a]
 except (OSError,PermissionError):continue
 if args and ((args[0].startswith(str(base)+'/') and args[0].endswith('/two_machine_spann')) or (len(args)>1 and args[1]==str(base/'remote-run31.py')) or (args[0].endswith('/two_machine_reader') and any(str(base) in a for a in args[1:]))):active.append(int(p.name))
def digest(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for block in iter(lambda:f.read(8*1024*1024),b''):h.update(block)
 return h.hexdigest()
checkpoint=base/'checkpoint-2m'/'owner-OWNER.json';inp=pathlib.Path('/home/ubuntu/spann-two-machine-v1/epoch2/input/fineweb3m.f32')
print(json.dumps({'identity':ident,'active_owned_pids':active,'reader_sha256':digest(base/'epoch31reader/two_machine_reader') if (base/'epoch31reader/two_machine_reader').exists() else None,'binary_sha256':digest(base/'epoch31v4/two_machine_spann'),'checkpoint_sha256':digest(checkpoint),'input':{'bytes':inp.stat().st_size,'mtime_ns':inp.stat().st_mtime_ns,'ctime_ns':inp.stat().st_ctime_ns},'free_bytes':shutil.disk_usage(base).free}))
'''
guard=json.loads((P.parent/'window-epoch29c/start-intent.json').read_text());unit='spann-two-machine-'+guard['window_id']+'-stop.timer'
def one(i):
 owner=1 if i==worker else 2;v=json.loads(ssh(i,'python3 -c '+shlex.quote(code.replace('INSTANCE',i).replace('OWNER',str(owner))),timeout=60));assert not v['active_owned_pids'];assert v['binary_sha256']==json.loads((P/'epoch31v4-binary.json').read_text())['sha256'];m=json.loads((P.parent/'development30/checkpoint-manifest.json').read_text());assert v['checkpoint_sha256']==next(x['checkpoint_sha256'] for x in m['owners'] if x['owner']==owner);v['guard']=ssh(i,shlex.join(['sudo','systemctl','show',unit,'-p','ActiveState','-p','NextElapseUSecRealtime']));assert 'ActiveState=active' in v['guard'];v['api_state']=hosts[i]['State']['Name'];return i,v
with concurrent.futures.ThreadPoolExecutor(2) as pool:audits=dict(pool.map(one,H))

assert audits[worker]['reader_sha256']==json.loads((P/'reader-selection.json').read_text())['sha256']
result={'observed_unix':time.time(),'account':who['Account'],'region':'us-east-1','hosts':audits,'source_archives_on_worker':archives,'machines_left_running_per_user':True,'guard_deadline_unix':guard['deadline_unix']}
(P/'final-cloud-audit.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'states':{k:v['api_state'] for k,v in audits.items()},'active_owned_processes':0,'guard_deadline':guard['deadline_utc']}))
