import copy
import importlib.util
import json
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('matched', HERE/'run-matched.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
PLAN = HERE.parents[1]/'dynamic-review/matched-2m-quality-plan-v1'

class CohortTests(unittest.TestCase):
    def setUp(self):
        self.inputs = json.loads((PLAN/'matched-input-bindings.json').read_text())
        self.bindings = {'matched_inputs': {'path': str(PLAN/'matched-input-bindings.json')}, 'queries': self.inputs['queries']}
        self.prepare = {'queries': copy.deepcopy(self.inputs['queries'])}
        self.report = {'queries': [{'query_id': q, 'expected': 100, 'covered': 100} for q in self.inputs['query_ids']]}

    def test_actual_frozen_cohort(self):
        path, ids = module.verify_query_cohort(self.bindings, self.prepare)
        self.assertEqual(path, self.inputs['queries']['path'])
        module.verify_report_queries(self.report, ids)

    def test_wrong_preparation_path(self):
        self.prepare['queries']['path'] = '/missing/queries.jsonl'
        with self.assertRaises(ValueError):
            module.verify_query_cohort(self.bindings, self.prepare)

    def test_synthetic_ids_rejected(self):
        self.report['queries'][0]['query_id'] = '0'
        with self.assertRaises(ValueError):
            module.verify_report_queries(self.report, self.inputs['query_ids'])

    def test_order_rejected(self):
        self.report['queries'].reverse()
        with self.assertRaises(ValueError):
            module.verify_report_queries(self.report, self.inputs['query_ids'])

    def test_incomplete_coverage_rejected(self):
        self.report['queries'][0]['covered'] = 99
        with self.assertRaises(ValueError):
            module.verify_report_queries(self.report, self.inputs['query_ids'])

if __name__ == '__main__':
    unittest.main()
