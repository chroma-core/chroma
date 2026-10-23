"""Stop only recorded optional adapters after their query gate finishes."""
import pathlib,json,sys
sys.path.insert(0,'/mnt/spann/source')
from tools.hierarchical_sharding.subtree_orchestrate import ssh
case=pathlib.Path(sys.argv[1]);out=[]
for label in ('cohost','two-hosts'):
 for row in json.loads((case/('servers-'+label+'.json')).read_text()):
  code='''import pathlib,os,signal,json,time
pid=PID;config=CONFIG;p=pathlib.Path('/proc')/str(pid)
if not p.exists():print(json.dumps(dict(pid=pid,status='already-exited')))
else:
 args=(p/'cmdline').read_bytes().split(bytes([0]));assert config.encode() in args and b'tools.hierarchical_sharding.subtree_server' in args
 stat=(p/'stat').read_text().rsplit(')',1)[1].split();assert int(stat[2])==pid
 os.killpg(pid,signal.SIGTERM);time.sleep(.1);print(json.dumps(dict(pid=pid,status='owned-group-terminated',start_ticks=stat[19])))
'''.replace('PID',repr(row['pid'])).replace('CONFIG',repr(row['config']))
  out.append(dict(host=row['host'],outcome=json.loads(ssh(row['host'],code))))
(case/'stopped-adapters.json').write_text(json.dumps(out,indent=2));print(json.dumps(dict(groups=len(out),status='stopped')))
