import argparse,pathlib,subprocess,json
from tools.hierarchical_sharding.transport import publish_manifest
p=argparse.ArgumentParser();p.add_argument('--hosts',nargs='+',required=True);p.add_argument('--index',required=True);p.add_argument('--indexes',nargs='*');p.add_argument('--port-step',type=int,default=0);p.add_argument('--port',type=int,required=True);p.add_argument('--count',type=int,required=True);p.add_argument('--dimension',type=int,default=1024);p.add_argument('--metric',default='l2');p.add_argument('--output',required=True);a=p.parse_args();shards=[]
for ordinal,host in enumerate(a.hosts):
 index=a.indexes[ordinal] if a.indexes else a.index
 port=a.port+ordinal*a.port_step
 manifest=index+'/manifest-1.json'
 command=['python3','-m','tools.hierarchical_sharding.server','--manifest',manifest,'--checkpoint-dir',index,'--reader-binary','/mnt/spann/hierarchical_spann_shard','--host','0.0.0.0','--port',str(port),'--timeout','30']
 code=f'import subprocess,pathlib\nf=open({(index+"/server.log")!r},"w");p=subprocess.Popen({command!r},stdout=f,stderr=f,start_new_session=True,cwd="/mnt/spann/source");pathlib.Path({(index+"/server.pid")!r}).write_text(str(p.pid));print(pathlib.Path({manifest!r}).read_text())'
 r=subprocess.run(['ssh',host,'python3','-'],input=code,text=True,capture_output=True,check=True);s=json.loads(r.stdout);s['endpoint']='http://'+host.split('@')[-1]+':'+str(port);shards.append(s)
publish_manifest(a.output,dict(generation=1,dimension=a.dimension,metric=a.metric,total_vector_count=a.count,shards=shards))
