from run import P,H,B,ssh,worker
import json,shlex,concurrent.futures,base64,time
code='''import pathlib,json,hashlib,base64,shutil
p=pathlib.Path('/home/ubuntu/spann-two-machine-v1/development30/checkpoint-3m-prebalance32/owner-OWNER.json');raw=p.read_bytes();h=hashlib.sha256(raw).hexdigest();assert h==p.with_suffix('.sha256').read_text().strip();x=json.loads(raw);assert x['next_input']==3000000 and x['owner']==OWNER
seen=bytearray(3000000//8);count=0
for node in x['nodes']:
 posting=node['postings']
 if posting is not None:
  for i in posting['ids']:
   assert 0<=i<3000000 and not seen[i//8]&(1<<(i%8));seen[i//8]|=1<<(i%8);count+=1
print(json.dumps({'owner':x['owner'],'next_input':x['next_input'],'input_sha256':x['input_sha256'],'checkpoint_sha256':h,'bytes':len(raw),'view_sha256':hashlib.sha256(json.dumps(x['view'],sort_keys=True,separators=(',',':')).encode()).hexdigest(),'nodes':len(x['nodes']),'vectors':count,'membership_bits':base64.b64encode(seen).decode(),'free_bytes':shutil.disk_usage(p.parent).free}))
'''
def one(i):return json.loads(ssh(i,'python3 -c '+shlex.quote(code.replace('OWNER','1' if i==worker else '2')),timeout=180))
with concurrent.futures.ThreadPoolExecutor(2) as pool:owners=list(pool.map(one,H))
a,b=[base64.b64decode(x.pop('membership_bits')) for x in owners];assert all((x&y)==0 and (x|y)==255 for x,y in zip(a,b));assert len({x['view_sha256'] for x in owners})==1;assert {x['input_sha256'] for x in owners}=={'18fb3c71c45bb70d28730f8edfdbd2ae90a3361b150a4871f09d870cb6684aba'}
(P/'checkpoint-3m-manifest.json').write_text(json.dumps({'status':'exact_disjoint_complete_3m_membership_and_identical_shared_view','observed_unix':time.time(),'owners':owners},indent=2)+'\n');print('CHECKPOINT_VALIDATED',[(x['owner'],x['bytes'],x['vectors']) for x in owners],flush=True)
