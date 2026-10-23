import json
from pathlib import Path
import struct
import tempfile
import unittest
from diagnostic_spec import create
from project_rounds import digest

class MappingTest(unittest.TestCase):
    def fixture(self, p, groups):
        jobs=[]; counts=[]
        for owner, ids in enumerate(groups):
            d=p/'build'/'indexes'/str(owner);d.mkdir(parents=True)
            mapping=d/'ids.u64';mapping.write_bytes(struct.pack('<'+'Q'*len(ids),*ids))
            h=digest(mapping)
            m=dict(source_id='s',metric='ip',dimension=1,vector_count=len(ids),generation=1,
                   id_mapping=dict(path='ids.u64',sha256=h,count=len(ids),bytes=8*len(ids)))
            (d/'manifest-1.json').write_text(json.dumps(m))
            jobs.append(dict(owner=owner,generation=1,id_map_sha256=h));counts.append(len(ids))
        (p/'projection').write_text(json.dumps(dict(rows=4,dimension=1,owners=2,owner_counts=counts,jobs=jobs)))
        (p/'queries').write_text(''.join(json.dumps(dict(query_id=str(i),vector=[float(i)]))+'\n' for i in range(16)))
        (p/'reference').write_text('{}\n')
    def test_valid_and_duplicate_partition(self):
        for groups,valid in [([[0,2],[1,3]],True),([[0,2],[2,3]],False),([[0],[1,2]],False)]:
            with tempfile.TemporaryDirectory() as tmp:
                p=Path(tmp);self.fixture(p,groups)
                args=(p/'projection',p/'build',p/'queries',p/'reference',p/'spec','s')
                if valid:
                    result=create(*args);self.assertEqual(result['global_rows'],4)
                    self.assertEqual(len(result['owners']),2)
                else:
                    with self.assertRaises(ValueError):create(*args)
                    self.assertFalse((p/'spec').exists())

if __name__=='__main__':unittest.main()
