import copy,importlib.util,pathlib,unittest
p=pathlib.Path(__file__).with_name('run-fidelity.py');s=importlib.util.spec_from_file_location('fidelity',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Fidelity(unittest.TestCase):
 def test_score_boundary_and_tied_exclusion(self):
  rows=[[i,m.bits(float(i))] for i in range(399)]+[[399,m.bits(399.)],[400,m.bits(399.)],[401,m.bits(401.)]]
  t=dict(complete=True,id_cap=16384,pretrim_candidate_ids=list(range(402)),pretrim_candidate_score_bits=rows,retained_candidate_ids=list(range(400)),cutoff_score_bits=m.bits(399.),cutoff_tie_candidates=2,cutoff_tie_retained=1)
  self.assertEqual(m.score_trace({'query_stage_trace':t})[400],399.)
  for field,value in [('cutoff_tie_candidates',True),('cutoff_score_bits',m.bits(398.)),('complete',False),('pretrim_candidate_score_bits',rows+rows[:1])]:
   bad=copy.deepcopy(t);bad[field]=value
   with self.assertRaises(ValueError):m.score_trace({'query_stage_trace':bad})
if __name__=='__main__':unittest.main()
