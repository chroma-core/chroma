"""Actual two-host correctness gate; run on separate driver with SSH access to workers."""
import argparse,json,pathlib,subprocess,struct,time,sys
from tools.hierarchical_sharding.transport import publish_manifest,query_all,ShardError
p=argparse.ArgumentParser();p.add_argument('--hosts',nargs=2,required=True);p.add_argument('--root',default='/mnt/spann');p.add_argument('--port-base',type=int,default=18080);p.add_argument('--binary',required=True);p.add_argument('--output',required=True);a=p.parse_args()
root=pathlib.Path(a.root);out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=True)
def remote(host,script):
 r=subprocess.run(['ssh',host,'python3','-'],input=script,text=True,capture_output=True,timeout=120)
 if r.returncode:raise RuntimeError(r.stderr)
 return r.stdout

def build(s,g,rows,resume=False):
 d=str(root/'smoke'/str(s));f=d+f'/input-{g}.f32';start=s+(4 if resume else 0)
 cmd=[a.binary,'build','--vectors',f,'--generation',str(g),'--source-id','two-host-smoke','--dimension','2','--global-id-start',str(start),'--id-stride','2','--row-count',str(len(rows)),'--checkpoint-dir',d,'--shard-id',str(s),'--metric','l2']+(['--resume'] if resume else [])
 script=f'import pathlib,struct,subprocess\np=pathlib.Path({d!r});p.mkdir(parents=True,exist_ok=True)\npathlib.Path({f!r}).write_bytes({struct.pack("<"+"f"*len(rows)*2,*(v for row in rows for v in row))!r})\nr=subprocess.run({cmd!r},capture_output=True,text=True,timeout=90);print(r.stdout);print(r.stderr);r.check_returncode()\n'
 (out/f'build-{s}-{g}.log').write_text(remote(a.hosts[s],script))
def endpoint(s,g):
 d=str(root/'smoke'/str(s));port=a.port_base+g;log=d+f'/server-{g}.log';manifest=d+f'/manifest-{g}.json'
 cmd=['python3','-m','tools.hierarchical_sharding.server','--manifest',manifest,'--checkpoint-dir',d,'--reader-binary',a.binary,'--host','0.0.0.0','--port',str(port)]
 script=f'import subprocess,json,pathlib,os\nf=open({log!r},"w");p=subprocess.Popen({cmd!r},stdout=f,stderr=f,start_new_session=True,cwd={str(root/"source")!r});pathlib.Path({(d+f"/server-{g}.pid")!r}).write_text(str(p.pid));print(pathlib.Path({manifest!r}).read_text())'
 shard=json.loads(remote(a.hosts[s],script));shard['endpoint']=f'http://{a.hosts[s].split("@")[-1]}:{port}';return shard
try:
 for s,rows in enumerate(([[0,0],[2,0]],[[1,0],[3,0]])):build(s,1,rows)
 old=dict(generation=1,dimension=2,metric='l2',total_vector_count=4,shards=[endpoint(s,1) for s in range(2)])
 publish_manifest(out/'collection-1.json',old);time.sleep(2)
 first=query_all(old,[0,0],4,rerank=4);assert first['ids']==[0,1,2,3] and first['distances']==[0,1,4,9],first
 for s,row in enumerate(([[.25,0]],[[.5,0]])):build(s,2,row,True)
 new=dict(generation=2,dimension=2,metric='l2',total_vector_count=6,shards=[endpoint(s,2) for s in range(2)])
 publish_manifest(out/'collection-2.json',new);time.sleep(2)
 second=query_all(new,[0,0],4,rerank=4);assert second['ids']==[0,4,5,1] and second['distances']==[0,.0625,.25,1],second
 assert query_all(old,[0,0],4,rerank=4)['ids']==[0,1,2,3]
 bad=json.loads(json.dumps(new));bad['shards'][0]['endpoint']='http://127.0.0.1:1'
 try:query_all(bad,[0,0],4,timeout=.5,rerank=4);raise AssertionError('missing shard silently succeeded')
 except ShardError:pass
 stale=json.loads(json.dumps(new));stale['shards'][0]['endpoint']=old['shards'][0]['endpoint']
 try:query_all(stale,[0,0],4,rerank=4);raise AssertionError('stale shard silently succeeded')
 except ShardError:pass
 incomplete=json.loads(json.dumps(new));incomplete['shards']=incomplete['shards'][:1]
 try:publish_manifest(out/'invalid.json',incomplete);raise AssertionError('incomplete publication succeeded')
 except ValueError:pass
 result=dict(status='passed',topology='two distinct worker hosts and separate driver',hosts=a.hosts,first=first,second=second,checks=['exact tiny IDs and distances','append','pinned old generation','missing shard fails closed','stale shard fails closed','incomplete publication rejected'])
 (out/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
finally:
 pass # Preserve readers and immutable checkpoints for inspection; fleet teardown owns node cleanup.
