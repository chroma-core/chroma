import copy,importlib.util,json,pathlib,tempfile,unittest
from unittest.mock import patch
from harness_common import descriptor,write
P=pathlib.Path(__file__).with_name('run-quality-code4.py');S=importlib.util.spec_from_file_location('policy',P);m=importlib.util.module_from_spec(S);S.loader.exec_module(m)
class Policy(unittest.TestCase):
 def test_calibration_gate_locked_overlay_and_no_evaluation_on_failure(self):
  base=pathlib.Path(__file__).parents[2]/'fleet'
  rawpaths={a:(base/'reserved-cohort-beam38-v1/small-evidence/runtime-attempt1/calibration-dynamic1-beam38.stdout') if a=='dynamic1' else base/f'reserved-cohort-quality-v1/small-evidence/runtime-attempt1/calibration-{a}-beam36.stdout' for a in m.ARMS}
  prior={a:[json.loads(v) for v in p.read_text().splitlines()][32:] for a,p in rawpaths.items()}
  for case in ['pass','calibration-miss','lock-tamper']:
   with self.subTest(case=case),tempfile.TemporaryDirectory() as td:
    out=pathlib.Path(td);gtfile=out/'fake-gt.json';gtfile.write_text('[]');prep={'queries':{'calibration':descriptor(gtfile),'evaluation':descriptor(gtfile)},'ground_truth':{'calibration':descriptor(gtfile),'evaluation':descriptor(gtfile)}};spec={'original':{'preparation':descriptor(gtfile)},'build_binding':descriptor(gtfile),'snapshots':{a:{'manifest':{'sha256':'a'*64},'root_ref':{'id':9},'census':{'reachable_leaves':1}} for a in m.ARMS}};calls=[];loads=[]
    def load(prep,split,ids):
     loads.append(split)
     if split=='evaluation':self.assertTrue((out/'selection.lock.json').exists());self.assertTrue(json.loads((out/'selection.json').read_text())['decision']['eligible_for_evaluation'])
     return [{'query_id':str(i),'vector':[0.]*768} for i in ids],[]
    def cell(arm,policy,split,*args):
     calls.append((arm,split));path=out/('overlay-'+arm);path.mkdir(exist_ok=True);file=path/'leaf-1.code4'
     if split=='calibration':
      file.write_bytes(b'');write(path/'overlay.json',dict(policy='code4-residual-postings-v1',snapshot='a'*64,dimension=768,root=9,metric='InnerProduct',leaves=[dict(id=1,file=file.name,rows=0,unique_rows=0,bytes=0,sha256=descriptor(file)['sha256'])]))
     rawfile=out/f'{split}-{arm}.stdout';rawfile.write_text(''.join(json.dumps(v)+'\n' for v in prior[arm]*2));report=dict(created=split=='calibration',snapshot='a'*64,manifest_sha256=descriptor(path/'overlay.json')['sha256'],embedding_point_gets=0,explicit_centroid_gets=0,loader_centroid_gets=0,vector_observation=dict(access_overflow=False,block_access={},point_blocks={},batch_point_blocks={}))
     value=dict(raw_stdout=descriptor(rawfile),posting_overlay_report=report,measured=[dict(query_id=str(i),recall_at_100=.94 if case=='calibration-miss' and arm=='dynamic1' else 1.,structural={'internal_pruning':True},counts={'posting_rows_scored_before_dedup':1,'centroid_distance_evaluations':1},elapsed_ms=1.) for i in range(32)])
     write(out/f'{split}-{arm}-beam{policy["beam"]}-result.json',value)
     if split=='evaluation' and case=='lock-tamper':(out/'selection.json').write_text('{}')
     return value
    with patch.object(m.previous,'load_split',load):
     if case=='lock-tamper':
      with self.assertRaisesRegex(ValueError,'policy lock changed'):m.cycle(out,spec,prep,spec['snapshots'],{a:{} for a in m.ARMS},cell,lambda *a:10,prior)
      self.assertEqual(sum(v[1]=='evaluation' for v in calls),1)
     else:
      result=m.cycle(out,spec,prep,spec['snapshots'],{a:{} for a in m.ARMS},cell,lambda *a:10,prior)
      if case=='pass':self.assertEqual(len(result['evaluation']),3);self.assertEqual(loads,['calibration','evaluation'])
      else:self.assertEqual(result['evaluation'],{});self.assertEqual(loads,['calibration'])
     self.assertEqual(sum(v[1]=='calibration' for v in calls),3)
 def test_native_flag_and_response_binding(self):
  from types import SimpleNamespace
  base=pathlib.Path(__file__).parents[2]/'fleet'
  raw=[json.loads(v) for v in (base/'reserved-cohort-quality-v1/small-evidence/runtime-attempt1/calibration-reference-beam36.stdout').read_text().splitlines()]
  gt=[json.loads(v) for v in (base/'reserved-cohort-quality-v1/small-evidence/runtime-attempt1/calibration-gt.jsonl').read_text().splitlines()]
  manifest=json.loads((base/'matched-pruning-16k-v1/runtime-evidence/runtime-attempt1/reference/store/manifest.json').read_text())
  spec=json.loads(pathlib.Path(__file__).with_name('runtime-spec.template.json').read_text());manifest['_store']='/fake/immutable-store'
  for v in raw:v['posting_score_policy']='code4-residual-postings-v1';v['posting_overlay_report']={'manifest_sha256':'a'*64}
  with tempfile.TemporaryDirectory() as td:
   out=pathlib.Path(td)
   def fake(command,**kwargs):
    self.assertEqual(command[command.index('--posting-code4-overlay-dir')+1],str(out/'overlay-reference'))
    kwargs['stdout'].write((''.join(json.dumps(v)+'\n' for v in raw)).encode());return SimpleNamespace(returncode=0)
   queries=[{'query_id':v['query_id'],'vector':[0.]*768} for v in gt]
   with patch.object(m.previous.subprocess,'run',fake):
    result=m.previous.cell(pathlib.Path('/fake/native'),'reference',m.POLICIES['reference'],'calibration',manifest,spec['snapshots']['reference']['census'],queries,gt,out,15,{})
   self.assertEqual(len(result['measured']),32);self.assertEqual(result['posting_overlay_report']['manifest_sha256'],'a'*64)
if __name__=='__main__':unittest.main()
