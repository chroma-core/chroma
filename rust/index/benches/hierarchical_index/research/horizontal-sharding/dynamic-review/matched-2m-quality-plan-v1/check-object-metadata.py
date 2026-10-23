import concurrent.futures,json,os,subprocess,datetime
from pathlib import Path
r=Path(__file__).resolve().parent
m=json.loads((r/'canonical-2m-input.json').read_text())
env=dict(os.environ,AWS_CONFIG_FILE='/private/tmp/spann-resume-aws-config',KUBECONFIG='/Users/chroma/.config/cops/kube/config',AWS_PAGER='')
base=['/opt/homebrew/bin/aws','--profile','spann-sandbox','--region','us-east-1']
def call(args):return json.loads(subprocess.check_output(base+args,env=env,text=True,timeout=30))
identity=call(['sts','get-caller-identity']);assert identity['Account']=='079819354890'
def check(c):
 a=c['artifact'];h=call(['s3api','head-object','--bucket',a['bucket'],'--key',a['key'],'--version-id',a['version_id'],'--expected-bucket-owner','079819354890'])
 assert h['ContentLength']==a['bytes'] and h['VersionId']==a['version_id']
 return dict(path=c['path'],response=h)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(check,m['chunks']))
(r/'live-object-metadata.json').write_text(json.dumps(dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),identity=identity,objects=results,scope='Read-only exact-version metadata; no corpus payload download or checksum validation'),indent=2)+'\n')
print('Verified metadata for',len(results),'version-pinned objects')
