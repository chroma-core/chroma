import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('wrapper', HERE/'run-and-collect.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def binding(path):
    data = path.read_bytes()
    return {'path':str(path),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}

class WrapperTests(unittest.TestCase):
    def test_collection_after_success_and_failure(self):
        for code in (0, 7, 9):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp)
                (root/'base.json').write_text(json.dumps({'deadline_unix':time.time()+10}))
                (root/'driver.py').write_text(f'raise SystemExit({code})\n')
                (root/'collection.json').write_text(json.dumps({'root':tmp,'deadline_unix':time.time()+140,'files':['base.json']}))
                args={'base':binding(root/'base.json'),'driver':binding(root/'driver.py'),'collector':binding(HERE/'collect-evidence.py'),'collection':binding(root/'collection.json'),'machine_stop_unix':time.time()+220,'output':str(root/'output')}
                if code == 9:
                    args['restore'] = {'script':binding(root/'driver.py'),'spec':binding(root/'base.json')}
                result=module.run(args)
                self.assertEqual(result['work'], None if code == 9 else code)
                if code == 9: self.assertEqual(result['restore'],9)
                self.assertEqual(result['collection'],0)
                self.assertTrue((root/'output/evidence.tar.receipt.json').exists())
                args['output']=str(root/'bad')
                args['machine_stop_unix']=time.time()+145
                with self.assertRaises(ValueError): module.run(args)

if __name__ == '__main__': unittest.main()
