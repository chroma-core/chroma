"""Pure metadata replay of exactly the sealed, consumed evaluation. No query execution."""
import hashlib,json,pathlib,subprocess,sys,time,platform,resource
from harness_common import descriptor,require,write
HERE=pathlib.Path(__file__).resolve().parent
EVIDENCE=HERE.parents[2]/'fleet/code4-posting-quality-repair-v2/small-evidence/runtime'
REMOTE='/mnt/spann/code4-posting-quality-v1/repair-v2/runtime-attempt1'

def main():
    original=EVIDENCE/'analysis-input.json';s=json.loads(original.read_text());require(set(s)=={'selection','evaluations','output'},'original analysis shape');require(set(s['evaluations'])=={'reference','dynamic1','dynamic4'},'all original evaluation arms');before={};remap={}
    for pin in [s['selection']]+list(s['evaluations'].values()):
        require(pathlib.PurePosixPath(pin['path']).parent==pathlib.PurePosixPath(REMOTE),'original remote namespace')
        p=EVIDENCE/pathlib.PurePosixPath(pin['path']).name;actual=descriptor(p);require(actual['bytes']==pin['bytes'] and actual['sha256']==pin['sha256'],'immutable retrieved analysis input');before[str(p)]=actual;remap[pin['path']]=actual
    require(json.loads((EVIDENCE/'selection.lock.json').read_text())==s['selection'],'original sealed selection descriptor')
    output=HERE/'inference.json';require(not output.exists(),'never overwrite recovered inference');local=dict(selection=remap[s['selection']['path']],evaluations={a:remap[p['path']] for a,p in s['evaluations'].items()},output=str(output));write(HERE/'analysis-input.local.json',local)
    artifacts=[original,EVIDENCE/'analysis.stderr',EVIDENCE/'analysis-process.json',EVIDENCE/'selection.lock.json',EVIDENCE/'result.json']
    write(HERE/'REMAPPING.json',dict(original_inputs={str(p):descriptor(p) for p in artifacts},remote_to_local=remap,derived_analysis_input=descriptor(HERE/'analysis-input.local.json'),scope='Only filesystem path substitution and output relocation. No evaluation values, policy, bootstrap or thresholds change. Evaluation already consumed; this is not a fresh evaluation.'))
    command=[sys.executable,str(HERE/'query_runtime.py'),'--analyze',str(HERE/'analysis-input.local.json')];cpu=resource.getrusage(resource.RUSAGE_CHILDREN);started=time.monotonic()
    with (HERE/'replay.stdout').open('xb') as stdout,(HERE/'replay.stderr').open('xb') as stderr:p=subprocess.run(command,stdout=stdout,stderr=stderr,timeout=20)
    after=resource.getrusage(resource.RUSAGE_CHILDREN);require(p.returncode==0,'fixed analyzer failed');require(output.stat().st_size<=65536,'bounded inference output');v=json.loads(output.read_text());require(json.loads((HERE/'replay.stdout').read_text())==v,'stdout/output equality')
    require(all(descriptor(path)==pin for path,pin in before.items()),'retained evaluation bytes changed')
    write(HERE/'replay-result.json',dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-started,python_version=platform.python_version(),user_cpu_seconds=after.ru_utime-cpu.ru_utime,system_cpu_seconds=after.ru_stime-cpu.ru_stime,inference=descriptor(output),stdout=descriptor(HERE/'replay.stdout'),stderr=descriptor(HERE/'replay.stderr'),input_remapping=descriptor(HERE/'REMAPPING.json'),outcome=v['outcome'],scope='Local recovery of prespecified inference only; original cloud run remains analysis-output failure. No query rerun, new policy, native source, corpus or cloud action.'))
    print(json.dumps(v,indent=2))
if __name__=='__main__':main()
