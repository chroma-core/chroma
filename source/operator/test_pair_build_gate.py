import hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import build_validity,run_pair
class FreshDependencyTests(unittest.TestCase):
 def test_unfilled_source_contract_rejects(self):
  with tempfile.TemporaryDirectory()as d:
   pending=Path(d)/'pending';pending.write_text(json.dumps({'source_manifest_sha256':None,'changed_source_files':None}))
   with patch.object(run_pair,'CONFIG',pending):
    with self.assertRaisesRegex(ValueError,'unfilled'):run_pair.configuration_ready()
 def fixture(self,d,extra=False):
  control=Path(d).resolve();root=control/'source';root.mkdir();dep=build_validity.DEPENDENCIES[0];entry='rust/index/benches/hierarchical_spann_shard.rs';other='rust/unreviewed/src/lib.rs';names=list(build_validity.DEPENDENCIES)+[entry]+([other]if extra else[])
  files={}
  for name in names:
   p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'current');files[name]={'bytes':7,'sha256':build_validity.digest(p)}
  manifest={'files':files,'symlinks':{}};base={'files':dict(files),'symlinks':{}};
  for name in build_validity.DEPENDENCIES:base['files'][name]={'bytes':3,'sha256':'old'}
  if extra:base['files'][other]={'bytes':3,'sha256':'old'}
  m=control/'source-manifest.json';m.write_text(json.dumps(manifest));config=control/'config';changed=sorted(list(build_validity.DEPENDENCIES)+([other]if extra else[]));config.write_text(json.dumps({'source_manifest_sha256':build_validity.digest(m),'changed_source_files':changed}));return root,manifest,base,config,entry,dep
 def test_exact_dependency_and_consumer_refresh_preserve_bytes(self):
  with tempfile.TemporaryDirectory()as d:
   root,m,b,c,entry,dep=self.fixture(d);before={n:(root/n).read_bytes()for n in m['files']}
   with patch.object(build_validity,'CONFIG',c):result=build_validity.refresh(root,m,b)
   self.assertEqual(result['changed_source_files'],sorted(build_validity.DEPENDENCIES));self.assertEqual({r['path']for r in result['refreshed']},{entry,*build_validity.DEPENDENCIES})
   self.assertEqual(before,{n:(root/n).read_bytes()for n in m['files']})
 def test_unreviewed_dependency_rejects_before_touch(self):
  with tempfile.TemporaryDirectory()as d:
   root,m,b,c,entry,dep=self.fixture(d,True);times={n:(root/n).stat().st_mtime_ns for n in m['files']}
   with patch.object(build_validity,'CONFIG',c):
    with self.assertRaisesRegex(ValueError,'Unreviewed'):build_validity.refresh(root,m,b)
   self.assertEqual(times,{n:(root/n).stat().st_mtime_ns for n in m['files']})
 def test_dependency_freshness_is_mandatory_before_either_arm(self):
  for fresh in [True,None,False]:
   with self.subTest(fresh=fresh),tempfile.TemporaryDirectory()as d:
    control=Path(d).resolve();folder=control/'build';folder.mkdir();native=folder/'native';native.write_bytes(b'new');config=control/'config';config.write_text(json.dumps({'source_manifest_sha256':'expected','changed_source_files':sorted(build_validity.DEPENDENCIES),'old_binary_sha256':['old']}));refresh={'changed_source_files':sorted(build_validity.DEPENDENCIES)};(folder/'source-refresh.json').write_text(json.dumps(refresh));(folder/'result.json').write_text(json.dumps({'bench_artifact_fresh':False,'source_manifest_sha256':'expected','binary':str(native),'binary_sha256':run_pair.digest(native),'source_refresh':refresh}));bench={'reason':'compiler-artifact','target':{'name':'hierarchical_spann_shard'},'executable':str(native),'fresh':False};dep={'reason':'compiler-artifact','target':{'name':'chroma_blockstore','kind':['lib'],'src_path':str(control/'source/rust/blockstore/src/lib.rs')},'fresh':fresh,'profile':{'opt_level':'3','debuginfo':0}};(folder/'stdout.jsonl').write_text(json.dumps(bench)+'\n'+json.dumps(dep)+'\n')
    class Stage:
     def validate_artifact(self,artifacts):assert len(artifacts)==1 and artifacts[0]['fresh']is False
    with patch.object(run_pair,'CONFIG',config):
     if fresh is False:self.assertFalse(run_pair.validate_pair_build(control,Stage())['blockstore_artifact_fresh'])
     else:
      with self.assertRaisesRegex(ValueError,'dependency did not compile'):run_pair.validate_pair_build(control,Stage())
      self.assertFalse((control/'paired-build-validity.json').exists())
if __name__=='__main__':unittest.main()
