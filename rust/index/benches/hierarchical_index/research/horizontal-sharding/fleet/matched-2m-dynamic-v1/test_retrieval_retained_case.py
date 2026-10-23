"""Retain one actual local retrieval CLI check on the already-created FAKE case.

No orchestration rerun, network, native task, or lifecycle operation occurs.
"""
import io,json,subprocess,sys,tarfile
from pathlib import Path
from orchestration_contract import descriptor


def main():
    here=Path(__file__).resolve().parent
    cases=here/'test_orchestration_evidence_complete'
    source=next(p for p in cases.iterdir() if (p/'orchestration/result.json').is_file()
                and json.loads((p/'orchestration/result.json').read_text()).get('success'))
    assert json.loads((source/'admission.json').read_text())['expected']['cohort']['rows']==3
    out=here/'test_retrieval_retained_evidence';out.mkdir()
    manifest=source/'orchestration/retrieval-manifest.json';value=json.loads(manifest.read_text());wire=io.BytesIO()
    with tarfile.open(fileobj=wire,mode='w',format=tarfile.USTAR_FORMAT) as tar:
        for item in value['files']:
            path=source/'orchestration'/item['name']
            tar.add(path,arcname=item['name'],recursive=False)
    (out/'wire.tar').write_bytes(wire.getvalue())
    spec=dict(manifest=descriptor(manifest),admission=value['admission'],admission_copy=descriptor(source/'admission.json'),
        limits=value['limits'],deadline_unix=value['machine_stop_unix'],output=str(out/'retrieved'))
    (out/'spec.json').write_text(json.dumps(spec,indent=2)+'\n')
    command=[sys.executable,str(here/'retrieve-evidence.py'),'--spec',str(out/'spec.json')]
    p=subprocess.run(command,input=wire.getvalue(),capture_output=True,timeout=20)
    (out/'stdout').write_bytes(p.stdout);(out/'stderr').write_bytes(p.stderr)
    (out/'result.json').write_text(json.dumps(dict(command=command,returncode=p.returncode,
        source_case=str(source),wire=descriptor(out/'wire.tar'),scope='Retained tiny FAKE evidence only; no transfer/channel/controller'),indent=2)+'\n')
    assert p.returncode==0,p.stderr.decode()
    for item in value['files']:
        assert (out/'retrieved'/item['name']).read_bytes()==(source/'orchestration'/item['name']).read_bytes()


if __name__=='__main__':main()
