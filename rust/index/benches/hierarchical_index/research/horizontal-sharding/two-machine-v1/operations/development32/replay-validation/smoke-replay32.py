import tempfile,pathlib,struct,hashlib,json,socket,subprocess,time
root=pathlib.Path(tempfile.mkdtemp(prefix='spann-balance-replay32-'))
binary='/private/tmp/chroma-single-shard-target/debug/deps/two_machine_spann-8575e116ae6b025d'
data=b''.join(struct.pack('<17f',*[i/113]*17) for i in range(64));(root/'input.f32').write_bytes(data)
(root/'input.json').write_text(json.dumps(dict(path=str(root/'input.f32'),rows=64,dimensions=17,stride_bytes=68,offset_bytes=0,first_id=0,file_bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),immutable_locators=['local-fixture'])))
(root/'token').write_text('local-replay-test-token-01234567890123456789')
def ports():
 ss=[socket.socket() for _ in range(4)]
 for s in ss:s.bind(('127.0.0.1',0))
 p=[s.getsockname()[1] for s in ss]
 for s in ss:s.close()
 return p
def run(name,extra):
 p=ports()
 common=['--run','3030','--input-descriptor',str(root/'input.json'),'--token-file',str(root/'token'),'--shutdown-at-unix',str(int(time.time())+60),'--rows','64','--initial-batch','64','--workers','2','--owner-budget','1000','--placement-start','1000','--split-threshold','16','--merge-threshold','4','--branching-factor','4','--construction-only','--reader-binary','/usr/bin/true']+extra
 def cmd(owner):
  a,b=(0,2) if owner==1 else (2,0)
  return [binary,'--mode','run' if owner==1 else 'serve','--owner',str(owner),'--output',str(root/f'{name}-{owner}'),'--data-bind',f'127.0.0.1:{p[a]}','--control-bind',f'127.0.0.1:{p[a+1]}','--peer-data',f'127.0.0.1:{p[b]}','--peer-control',f'127.0.0.1:{p[b+1]}']+common
 peer=subprocess.Popen(cmd(2),stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 try:
  for _ in range(200):
   ev=root/f'{name}-2/events.jsonl'
   if ev.exists() and 'server_ready' in ev.read_text():break
   if peer.poll() is not None:raise RuntimeError(peer.communicate())
   time.sleep(.02)
  r=subprocess.run(cmd(1),capture_output=True,text=True,timeout=40)
  assert r.returncode==0,(r.stdout,r.stderr)
  return json.loads((root/f'{name}-1/result.json').read_text())
 finally:peer.terminate();peer.communicate(timeout=5)
a=run('prepare',['--checkpoint-after-add-at','64','--checkpoint-directory',str(root/'checkpoint')]);assert a['mode']=='post_insertion_checkpoint'
b=run('replay',['--balance-only','--restore-checkpoint',str(root/'checkpoint')]);assert b['status']=='writer_validated' and b['mode']=='balance_only' and len(b['cycles'])==1 and b['cycles'][0]['add_seconds']==0
c=run('ordinary',[]);assert c['status']=='writer_validated' and c['mode']=='construction'
for flags in [['--balance-only'],['--balance-only','--restore-checkpoint','x','--checkpoint-directory','x'],['--checkpoint-after-add-at','64'],['--checkpoint-after-add-at','64','--checkpoint-directory','x','--replay-queries','1']]:
 base=[binary,'--mode','run','--owner','1','--run','3030','--input-descriptor',str(root/'input.json'),'--token-file',str(root/'token'),'--shutdown-at-unix',str(int(time.time())+60),'--output',str(root/'invalid'),'--data-bind','127.0.0.1:39001','--control-bind','127.0.0.1:39002','--peer-data','127.0.0.1:39003','--peer-control','127.0.0.1:39004','--owner-budget','1000','--construction-only','--reader-binary','/usr/bin/true']
 r=subprocess.run(base+flags,capture_output=True,text=True);assert r.returncode==2,(flags,r.stderr)
print(json.dumps({'directory':str(root),'checkpoint':a['status'],'replay':b['status'],'ordinary':c['status'],'replay_add_seconds':b['cycles'][0]['add_seconds'],'replay_cycles':len(b['cycles']),'invalid_cli_cases':4}))
