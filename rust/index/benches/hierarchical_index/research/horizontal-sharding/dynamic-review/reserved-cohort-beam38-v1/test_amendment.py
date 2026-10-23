import copy,hashlib,importlib.util,json,os,sys,tempfile,time,unittest
from pathlib import Path
from harness_common import descriptor,write
import quality_analysis as a
HERE=Path(__file__).resolve().parent
sp=importlib.util.spec_from_file_location('amendment',HERE/'run-amended.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)
PRIOR=HERE.parent/'reserved-cohort-quality-v1/repair-v2';sys.path.insert(0,str(PRIOR));from test_quality import fake_reply,fake_snapshot,CENSUS


def fixture(root):
    original=root/'original';original.mkdir();out=root/'amended';out.mkdir();prep=dict(status='passed',queries={},ground_truth={},prefix_sha256='prefix',prefix_copy_bytes=50331648,query_logical_read_bytes=884736,row_provenance=[])
    for j,identity in enumerate(a.cohort_ids()):prep['row_provenance'].append(dict(canonical_global_id=identity,sha256=hashlib.sha256(str(identity).encode()).hexdigest(),split='calibration' if j%9==0 else 'evaluation'))
    for split in ['calibration','evaluation']:
        selected=[v for j,v in enumerate(a.cohort_ids()) if (j%9==0)==(split=='calibration')];queries=[dict(query_id='canonical-doc:'+str(i),canonical_global_id=i,vector=[i/2000000.]+[.1]*767,split=split) for i in selected];gt=[dict(query_id=q['query_id'],ids=list(range(100)),distances=[float(i) for i in range(100)]) for q in queries]
        for kind,values in [('queries',queries),('ground_truth',gt)]:
            path=original/(split+'-'+kind+'.jsonl');path.write_text(''.join(json.dumps(v)+'\n' for v in values));prep[kind][split]=descriptor(path)
    preppath=original/'preparation.json';write(preppath,prep);ids=['canonical-doc:'+str(v) for j,v in enumerate(a.cohort_ids()) if j%9==0]
    cells={};pins={}
    for arm in ['reference','dynamic4']:
        rows=[dict(query_id=i,recall_at_100=1.,structural=dict(internal_pruning=True),counts=dict(posting_rows_scored_before_dedup=450,centroid_distance_evaluations=3),elapsed_ms=.01) for i in ids];cell=dict(arm=arm,split='calibration',policy=m.CONTROL,measured=rows);path=original/(arm+'.json');write(path,cell);cells[arm]=cell;pins[arm]=descriptor(path)
    result=dict(status='completed',outcome='calibration-target-or-pruning-not-met',evaluation={},calibration={arm:[None,cell] for arm,cell in cells.items()});path=original/'result.json';write(path,result);pins['result']=descriptor(path)
    selection=dict(preparation=descriptor(preppath),bootstrap=a.BOOTSTRAP,decision=dict(eligible_for_evaluation=False,failures={'dynamic1':'original failure'},chosen={'reference':m.CONTROL,'dynamic4':m.CONTROL}),calibration_evidence={arm:[pin] for arm,pin in pins.items() if arm in cells},calibration_queries=prep['queries']['calibration'],calibration_gt=prep['ground_truth']['calibration'],evaluation_queries=prep['queries']['evaluation'],evaluation_gt=prep['ground_truth']['evaluation']);path=original/'selection.json';write(path,selection);pins['selection']=descriptor(path);path=original/'selection.lock.json';write(path,pins['selection']);pins['lock']=descriptor(path);pins['preparation']=descriptor(preppath)
    spec=dict(binary={'path':'unused'},vectors={'path':'fake-canonical'},restore_receipt={'path':'unused'},prefix_sha256='prefix',snapshots={'fixed':'fake'});admitted=dict(spec,protocol='reserved-cohort-quality-v1',output=str(original));path=original/'runtime-spec.json';write(path,admitted);pins['admitted_spec']=descriptor(path);path=original/'setup-spec.json';write(path,dict(source='fake-canonical',output=str(original)));pins['setup_spec']=descriptor(path);spec['original']=pins;return out,spec,prep


class Amendment(unittest.TestCase):
    def test_bound_lineage_one_candidate_lock_failure_and_no_reruns(self):
        for mode in ['success','candidate-negative','tamper','input-mismatch','prior-evaluation']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as td:
                root=Path(td);out,spec,prep=fixture(root);before={k:descriptor(v['path']) for k,v in spec['original'].items()}
                if mode=='input-mismatch':Path(spec['original']['preparation']['path']).write_text('{}')
                if mode=='prior-evaluation':
                    path=Path(spec['original']['result']['path']);value=json.loads(path.read_text());value['evaluation']={'reference':'already run'};path.write_text(json.dumps(value));spec['original']['result']=descriptor(path)
                if mode in ['input-mismatch','prior-evaluation']:
                    with self.assertRaises(ValueError):m.lineage(spec)
                    self.assertFalse(list(out.iterdir()));continue
                prep,values=m.lineage(spec);fake=root/'fake-native';eval_ids=[v for j,v in enumerate(a.cohort_ids()) if j%9!=0]
                script='''#!PYTHON
import sys,json
from pathlib import Path
sys.path.insert(0,PRIOR)
from test_quality import fake_reply
from harness_common import descriptor
for line in sys.stdin:
 request=json.loads(line);identity=int(request['request_id'].split(':')[-1]);is_eval=identity in EVAL_IDS
 if is_eval:
  path=Path(OUT)/'selection.json';assert descriptor(path)==json.loads((Path(OUT)/'selection.lock.json').read_text())
 reply=fake_reply(request)
 if MODE=='candidate-negative' and not is_eval:reply['ids']=list(range(90))+list(range(100,110))
 print(json.dumps(reply))
'''.replace('PYTHON',sys.executable).replace('PRIOR',repr(str(PRIOR))).replace('OUT',repr(str(out))).replace('EVAL_IDS',repr(eval_ids)).replace('MODE',repr(mode));fake.write_text(script);fake.chmod(0o755);calls=[]
                def run_cell(arm,policy,split,snapshot,census,queries,gt,remaining):
                    calls.append((arm,policy['beam'],split));value=m.previous.cell(fake,arm,policy,split,snapshot,census,queries,gt,out,remaining,dict(os.environ))
                    if mode=='tamper' and split=='evaluation':(out/'selection.json').write_text('{}')
                    return value
                snapshots={arm:fake_snapshot(root/arm) for arm in a.ARMS};censuses={arm:CENSUS for arm in a.ARMS}
                if mode=='tamper':
                    with self.assertRaises(ValueError):m.amended_cycle(out,spec,prep,values,snapshots,censuses,run_cell,lambda cap,end:cap)
                    self.assertEqual(calls,[('dynamic1',38,'calibration'),('reference',36,'evaluation')])
                else:
                    result=m.amended_cycle(out,spec,prep,values,snapshots,censuses,run_cell,lambda cap,end:cap)
                    expected=[('dynamic1',38,'calibration')]+([] if mode=='candidate-negative' else [('reference',36,'evaluation'),('dynamic1',38,'evaluation'),('dynamic4',36,'evaluation')]);self.assertEqual(calls,expected)
                    if mode=='candidate-negative':self.assertEqual(result['outcome'],'amended-calibration-target-or-pruning-not-met');self.assertFalse((out/'evaluation-gt.jsonl').exists());self.assertFalse(result['evaluation'])
                    else:self.assertEqual(set(result['evaluation']),set(a.ARMS));self.assertEqual(json.loads((out/'selection.json').read_text())['decision']['chosen']['dynamic1'],m.CANDIDATE)
                self.assertTrue((out/'selection.lock.json').exists());self.assertEqual(before,{k:descriptor(v['path']) for k,v in spec['original'].items()})

if __name__=='__main__':unittest.main()
