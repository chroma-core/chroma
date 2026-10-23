import json,pathlib,subprocess,sys,tempfile,unittest
from harness_common import descriptor,write
from quality_analysis import BOOTSTRAP,ARMS,cohort_ids
class AnalyzerCLI(unittest.TestCase):
 def test_real_analysis_subprocess_serializes_prespecified_inference(self):
  script=pathlib.Path(__file__).with_name('query_runtime.py')
  help_result=subprocess.run([sys.executable,str(script),'--help'],capture_output=True,text=True,timeout=10);self.assertEqual(help_result.returncode,0)
  with tempfile.TemporaryDirectory() as td:
   root=pathlib.Path(td);policies={a:{'name':'code4-residual-FP-navigation-rerank4','beam':38 if a=='dynamic1' else 36} for a in ARMS};selection=root/'selection.json';write(selection,{'decision':{'eligible_for_evaluation':True,'chosen':policies},'bootstrap':BOOTSTRAP});pins={}
   ids=['canonical-doc:'+str(v) for j,v in enumerate(cohort_ids()) if j%9!=0]
   for arm in ARMS:
    p=root/(arm+'.json');write(p,{'split':'evaluation','arm':arm,'policy':policies[arm],'measured':[{'query_id':q,'recall_at_100':1.,'structural':{'internal_pruning':True}} for q in ids]});pins[arm]=descriptor(p)
   output=root/'inference.json';request=root/'analysis.json';write(request,{'selection':descriptor(selection),'evaluations':pins,'output':str(output)})
   before={p.name:descriptor(p) for p in root.iterdir()};result=subprocess.run([sys.executable,str(script),'--analyze',str(request)],capture_output=True,text=True,timeout=20);self.assertEqual(result.returncode,0,result.stderr);value=json.loads(output.read_text());self.assertEqual(json.loads(result.stdout),value);self.assertEqual(value['outcome'],'conditional-comparison-passed');self.assertEqual(value['queries'],256);self.assertEqual(value['bootstrap'],BOOTSTRAP);self.assertEqual(len(value['intervals']),5);self.assertEqual(value['pruning_threshold'],205);self.assertTrue(output.read_bytes().endswith(b'\n'));self.assertLess(output.stat().st_size,65536)
   for name,pin in before.items():self.assertEqual(descriptor(root/name),pin)
if __name__=='__main__':unittest.main()
