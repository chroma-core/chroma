#!/usr/bin/env python3
"""Build reviewed29, test actual durable storage and run one guarded3M candidate."""
import concurrent.futures,json,shlex,subprocess,time
from pathlib import Path
import lifecycle as l
P=Path(__file__).resolve().parent;W=P/'window-epoch29';A=P/'admission-epoch29.json'
BASE='/home/ubuntu/spann-two-machine-v1/epoch29'
KNOWN=Path('/private/tmp/spann-epoch29-known-hosts')
SSH=['-i','/Users/chroma/Documents/lyon.pem','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+str(KNOWN)]
def run(args,timeout=45,**kw):
 r=subprocess.run(args,capture_output=True,text=True,timeout=timeout,**kw)
 if r.returncode:raise RuntimeError(str(args[0])+': '+r.stderr[-4000:]+r.stdout[-2000:])
 return r.stdout
def ssh(ip,cmd,**kw):return run(['ssh',*SSH,'ubuntu@'+ip,cmd],**kw)
prices=json.loads((P/'r6i16-price-epoch29-measure.json').read_text());rates=[]
for raw in prices['PriceList']:
 for term in json.loads(raw)['terms']['OnDemand'].values():
  for d in term['priceDimensions'].values():
   if d['unit']=='Hrs':rates.append(float(d['pricePerUnit']['USD']))
assert rates and set(rates)=={4.032}
old=json.loads((P/'admission-epoch28.json').read_text())
for item in old['local_artifacts']:assert l.digest(Path(item['path']))==item['sha256'],item['path']
hashes={}
artifacts=[P/x for x in ['measure-epoch29.py','run-one-arm-epoch29.py','make-3m-spec-epoch29.py','remote-build-epoch29.py','build-manifest-epoch29-distributed.json','lifecycle.py','remote-guard.sh','remote-job-epoch29.py','r6i16-price-epoch29-measure.json']]+[P.parent/'packages/source-epoch29.tar.gz',P.parent/'local-tests/epoch29-reuse-validation.json',P.parent/'local-tests/EPOCH29-INDEPENDENT-REVIEW.md',P/'storage-config-epoch29.json',P/'owner_local_epoch29_test.py']
validation=json.loads((P.parent/'local-tests/epoch29-reuse-validation.json').read_text())
assert validation['suite']['passed']==266 and validation['suite']['failed']==0 and validation['native_build']['exit_code']==0
l.write(A,dict(root_authorized_execution=True,combined_review_passed=True,review_basis='Independent review accepts combined owner-direct saving, unchanged-block reuse and refresh correction; final native validation is bound.',window_seconds=2700,rates_usd_hour={i:4.032 for i in l.BINDINGS},compute_cap_usd=6.25,scope='Build reviewed29 writer/reader/test, real Rust S3 conditional smoke under180s, verify/copy binaries and inputs, one3M first+unchanged-repeat-save benchmark with both fresh readers, collect and stop.',local_artifacts=[dict(path=str(p),sha256=l.digest(p)) for p in artifacts]))
try:
 print(run(['python3',str(P/'lifecycle.py'),'start','--window',str(W),'--admission',str(A)]),flush=True)
 intent=json.loads((W/'start-intent.json').read_text());end=int(intent['deadline_unix']);wid='epoch29-measure-'+str(int(intent['observed_start_unix']));intent['window_id']=wid;l.write(W/'start-intent.json',intent)
 while time.time()<end-900:
  hosts=l.instances()
  if all(i['State']['Name']=='running' and i.get('PublicIpAddress') for i in hosts.values()):break
  time.sleep(3)
 else:raise RuntimeError('Hosts unavailable within preparation allowance')
 l.write(W/'running-readback.json',hosts)
 lines=[]
 for iid,h in hosts.items():
  key=(P/'window-epoch28'/('hostkey-'+iid+'.txt')).read_text().split();lines.append(h['PublicIpAddress']+' '+key[1]+' '+key[2])
 KNOWN.write_text('\n'.join(lines)+'\n')
 for iid,h in hosts.items():
  ip=h['PublicIpAddress']
  while time.time()<end-800:
   try:ssh(ip,'true');break
   except RuntimeError:time.sleep(3)
  else:raise RuntimeError('SSH not ready')
  (W/('remote-guard-'+iid+'.txt')).write_text(ssh(ip,f'SPANN_DEADLINE_UNIX={end} SPANN_WINDOW_ID={wid} SPANN_INSTANCE_ID={iid} bash -s',input=(P/'remote-guard.sh').read_text()))
 print('Both remote guards active',flush=True)
 worker=hosts['i-011ff180f03584f40']['PublicIpAddress'];peer=hosts['i-00268d1e1dfea0357']['PublicIpAddress']
 def check_binaries(ip):
  values=ssh(ip,'sha256sum '+BASE+'/build/two_machine_reader '+BASE+'/build/two_machine_spann').splitlines()
  actual={Path(line.split()[1]).name:line.split()[0] for line in values};assert actual==hashes,actual
  return actual
 ssh(worker,'mkdir '+BASE)
 for local,remote in [(P.parent/'packages/source-epoch29.tar.gz','source.tar.gz'),(P/'build-manifest-epoch29-distributed.json','build-manifest.json'),(P/'remote-build-epoch29.py','remote-build.py')]:
  run(['scp',*SSH,str(local),'ubuntu@'+worker+':'+BASE+'/'+remote],timeout=90)
 for ip in [worker,peer]:
  if ip==peer:ssh(ip,'mkdir '+BASE)
  run(['scp',*SSH,str(P/'remote-job-epoch29.py'),'ubuntu@'+ip+':'+BASE+'/remote-job.py'],timeout=45)
 archive_sha=l.digest(P.parent/'packages/source-epoch29.tar.gz')
 extract="import pathlib,hashlib,tarfile; p=pathlib.Path("+repr(BASE)+"); assert hashlib.sha256((p/'source.tar.gz').read_bytes()).hexdigest()=="+repr(archive_sha)+"; (p/'source').mkdir(); t=tarfile.open(p/'source.tar.gz'); assert all(m.isfile() and not m.name.startswith('/') and '..' not in pathlib.Path(m.name).parts for m in t.getmembers()); t.extractall(p/'source',filter='data')"
 ssh(worker,'python3 -c '+shlex.quote(extract))
 try:
  built=ssh(worker,'python3 '+BASE+'/remote-build.py --manifest '+BASE+'/build-manifest.json --output '+BASE+'/build --deadline '+str(end-1500),timeout=min(1150,int(end-time.time()-1470)))
  (W/'build.stdout').write_text(built)
 finally:
  collect="import pathlib,json; p=pathlib.Path("+repr(BASE+'/build')+"); print(json.dumps({str(f):f.read_text() for f in p.glob('*') if f.is_file() and f.stat().st_size<4000000 and f.suffix in ('.json','.stdout','.stderr')}))"
  (W/'build-receipts.json').write_text(ssh(worker,'python3 -c '+shlex.quote(collect)))
 receipts=json.loads((W/'build-receipts.json').read_text())
 for name in ['two_machine_reader','two_machine_spann']:
  status=json.loads(receipts[BASE+'/build/'+name+'.process.json']);assert status['exit_code']==0
  binary=json.loads(receipts[BASE+'/build/'+name+'.binary.json']);assert binary['compiler_artifact']['fresh'] is False
  hashes[name]=binary['sha256']
 print('Candidate29 Linux binaries built',flush=True)
 l.write(W/'worker-binaries-before-transfer.json',check_binaries(worker))
 ssh(peer,'test ! -e '+BASE+'/build/two_machine_reader && test ! -e '+BASE+'/build/two_machine_spann && mkdir -p '+BASE+'/build')
 for name in ['two_machine_reader','two_machine_spann']:
  run(['scp','-3',*SSH,'ubuntu@'+worker+':'+BASE+'/build/'+name,'ubuntu@'+peer+':'+BASE+'/build/'+name],timeout=90)
 ssh(peer,'chmod 755 '+BASE+'/build/two_machine_reader '+BASE+'/build/two_machine_spann')
 expected=json.loads((P/'window-epoch2/descriptor-3m-corrected.json').read_text())
 def check_host(item):
  iid,h=item;ip=h['PublicIpAddress'];actual=check_binaries(ip)
  script='import pathlib,json,hashlib; p=pathlib.Path('+repr(expected['path'])+'); h=hashlib.sha256(); f=p.open("rb"); '+ '\nfor b in iter(lambda:f.read(8*1024*1024),b""): h.update(b)\n'+ 'print(json.dumps({"bytes":p.stat().st_size,"sha256":h.hexdigest(),"descriptor":json.loads(p.with_name("3m.json").read_text())}))'
  value=json.loads(ssh(ip,'timeout 120 python3 -c '+shlex.quote(script),timeout=130))
  assert value['bytes']==expected['file_bytes'] and value['sha256']==expected['sha256'] and value['descriptor']==expected
  jobhash=ssh(ip,'sha256sum '+BASE+'/remote-job.py').split()[0];assert jobhash==l.digest(P/'remote-job-epoch29.py')
  value['binaries']=actual;value['supervisor_sha256']=jobhash;l.write(W/('runtime-check-'+iid+'.json'),value)
 with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(check_host,hosts.items()))
 l.write(P/'runtime-binaries-epoch29.json',hashes)
 for ip in [worker,peer]:
  run(['scp',*SSH,str(P/'storage-config-epoch29.json'),'ubuntu@'+ip+':'+BASE+'/storage-config.json'],timeout=45)
 test_receipt=json.loads(receipts[BASE+'/build/hierarchical_distributed.binary.json'])
 assert ssh(worker,'sha256sum '+BASE+'/build/hierarchical_distributed').split()[0]==test_receipt['sha256']
 smoke_config=(P/'storage-config-epoch29.json').read_text().strip()
 smoke_cmd=shlex.join(['env','AWS_REGION=us-east-1','AWS_DEFAULT_REGION=us-east-1','SPANN_DURABLE_SMOKE_CONFIG='+smoke_config,'timeout','180',BASE+'/build/hierarchical_distributed','--exact','hierarchical_index::distributed::generation_backend::tests::durable_s3_adapter_smoke','--ignored','--nocapture','--test-threads=1'])
 smoke=subprocess.run(['ssh',*SSH,'ubuntu@'+worker,smoke_cmd],capture_output=True,text=True,timeout=200)
 (W/'adapter-smoke.stdout').write_text(smoke.stdout);(W/'adapter-smoke.stderr').write_text(smoke.stderr);l.write(W/'adapter-smoke-process.json',{'exit_code':smoke.returncode,'command':smoke_cmd,'binary_sha256':test_receipt['sha256']})
 assert smoke.returncode==0,'Real S3 adapter smoke failed; do not start benchmark'
 print('Real Rust S3 adapter smoke passed',flush=True)
 print('Binaries, inputs and supervisors verified on both hosts',flush=True)
 assert time.time()+1260<end,'Insufficient time; never extend window'
 r=subprocess.run(['python3',str(P/'run-one-arm-epoch29.py'),'3m-distributed'],capture_output=True,text=True,timeout=1340)
 (W/'runner.stdout').write_text(r.stdout);(W/'runner.stderr').write_text(r.stderr);l.write(W/'runner-process.json',{'exit_code':r.returncode})
 print('Runner exited '+str(r.returncode),flush=True)
finally:
 if (W/'start-intent.json').exists() and not (W/'closed.json').exists():l.stop(W,'measure-stop');print('Both hosts API-stopped',flush=True)
