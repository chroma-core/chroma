"""Bounded retrieval tests use only small local bytes; no network transport."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import time
import unittest

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from orchestration_contract import descriptor
loader=importlib.util.spec_from_file_location('retrieval',HERE/'retrieve-evidence.py');retrieval=importlib.util.module_from_spec(loader);loader.loader.exec_module(retrieval)


def save(path,value):path.write_text(json.dumps(value));return descriptor(path)


def fixture(root, cap=100000):
    root=root.resolve();origin=root/'origin';origin.mkdir()
    limits=dict(max_wire_bytes=cap,max_archive_bytes=80000,max_sidecar_bytes=10000)
    approval=save(root/'admission.json',dict(decision='approved',epoch='matched-2m-dynamic-v1',retrieval_limits=limits,machine_stop_unix=time.time()+300))
    a=json.loads((root/'admission.json').read_text());files=[]
    for name in ('result.json','pre-collection.json'):
        (origin/name).write_text(json.dumps(dict(status='fake-work-failed',name=name)));files.append(dict(name=name,**descriptor(origin/name)))
    manifest=save(root/'retrieval-manifest.json',dict(format_version=1,epoch='matched-2m-dynamic-v1',admission=approval,root=str(origin),limits=limits,machine_stop_unix=a['machine_stop_unix'],files=files))
    return dict(manifest=manifest,admission=approval,admission_copy=approval,limits=limits,deadline_unix=time.time()+30,output=str(root/'retrieved')),files


def wire(files, kind='valid'):
    out=io.BytesIO()
    with tarfile.open(fileobj=out,mode='w',format=tarfile.PAX_FORMAT if kind=='pax' else tarfile.USTAR_FORMAT) as tar:
        for index,item in enumerate(files):
            if kind=='missing' and index==1:continue
            data=Path(item['path']).read_bytes();name=item['name']
            if index==0:
                if kind=='escape':name='../result.json'
                if kind=='unknown':name='data-blocks.raw'
                if kind=='pax':name='x'*101
                if kind=='bad-hash':data=b'!'+data[1:]
            info=tarfile.TarInfo(name);info.size=len(data)
            if kind=='symlink' and index==0:info.type=tarfile.SYMTYPE;info.linkname='/outside';info.size=0
            tar.addfile(info,io.BytesIO(data) if info.isreg() else None)
            if kind=='duplicate' and index==0:tar.addfile(info,io.BytesIO(data))
    data=out.getvalue()
    if kind=='truncated':data=data[:600]
    if kind=='trailing':data+=b'X'
    return data


class RetrievalContract(unittest.TestCase):
    def test_exact_failure_sidecars_retrieved_without_inner_extraction(self):
        with tempfile.TemporaryDirectory() as tmp:
            spec,files=fixture(Path(tmp));r=retrieval.retrieve(spec,io.BytesIO(wire(files)));self.assertEqual(r['status'],'retrieved');self.assertFalse(r['inner_archive_extracted'])
            for item in files:self.assertEqual((Path(spec['output'])/item['name']).read_bytes(),Path(item['path']).read_bytes())

    def test_unsafe_incomplete_corrupt_and_oversized_streams_fail_without_publication(self):
        for kind in ['escape','unknown','pax','bad-hash','symlink','duplicate','missing','truncated','trailing','wire-cap']:
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as tmp:
                spec,files=fixture(Path(tmp),1024 if kind=='wire-cap' else 100000)
                with self.assertRaises((ValueError,UnicodeError)):
                    retrieval.retrieve(spec,io.BytesIO(wire(files,kind)))
                self.assertFalse((Path(spec['output'])/'result.json').exists());self.assertFalse((Path(spec['output'])/'retrieval-complete.json').exists());self.assertTrue((Path(spec['output'])/'retrieval-failure.json').exists())

    def test_wrong_retained_admission_and_expired_deadline_reject_before_output(self):
        for kind in ['approval','expired','limits']:
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as tmp:
                spec,files=fixture(Path(tmp))
                if kind=='approval':
                    p=Path(spec['admission_copy']['path']);d=json.loads(p.read_text());d['decision']='draft';spec['admission_copy']=save(p,d)
                elif kind=='expired':spec['deadline_unix']=0
                else:spec['limits']=dict(spec['limits'],max_wire_bytes=100001)
                with self.assertRaises(ValueError):retrieval.retrieve(spec,io.BytesIO(wire(files)))
                self.assertFalse(Path(spec['output']).exists())


if __name__=='__main__':unittest.main(verbosity=2)
