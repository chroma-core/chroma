import json,pathlib,subprocess
base=pathlib.Path('/mnt/spann/results/fineweb1m128-h2-a16-c16');rows=[]
for name in ('servers-cohost.json','servers-two-hosts.json'):rows+=json.load(open(base/name))
reports=[]
for host in sorted({r['host'] for r in rows}):
 code='''import json,os,pathlib,signal
rows=INPUT
out=[]
for r in rows:
 p=pathlib.Path('/proc')/str(r['pid'])
 if not p.exists():out.append(dict(pid=r['pid'],state='already-exited'));continue
 cmd=(p/'cmdline').read_bytes().replace(b'\\0',b' ').decode();assert r['config'] in cmd and 'subtree_server' in cmd,cmd
 pg=os.getpgid(r['pid']);assert pg==r['pid'];os.killpg(pg,signal.SIGTERM);out.append(dict(pid=r['pid'],pgid=pg,command=cmd,state='term-sent'))
print(json.dumps(out))
'''.replace('INPUT',repr([r for r in rows if r['host']==host]))
 res=subprocess.run(['ssh',host,'python3 -'],input=code,text=True,capture_output=True,check=True);reports.append(dict(host=host,processes=json.loads(res.stdout)))
(base/'adapters-stopped-after-load.json').write_text(json.dumps(reports));print(json.dumps(reports))
