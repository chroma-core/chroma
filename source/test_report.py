"""Validate the actual local native report and reject incomplete attribution."""
import copy
import importlib.util
import json
from pathlib import Path
import unittest
ROOT=Path(__file__).parent
spec=importlib.util.spec_from_file_location('balance_report',ROOT/'run-balance-cost.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class ReportContract(unittest.TestCase):
    def test_actual_native_cost_and_tampered_reconciliation(self):
        value=json.loads((ROOT/'local-native/profiled/result.json').read_text())
        for cut in value['cuts']:
            original=cut['balance_cost'];module.validate_cost(original)
            for field in ['exclusive_sum_ns','observed_window_ns','cli_balance_wall_ns','observation_boundary_extra_ns']:
                bad=copy.deepcopy(original);bad[field]+=1
                with self.assertRaises(ValueError):module.validate_cost(bad)
            bad=copy.deepcopy(original);bad['categories']['clustering']['exclusive_ns']+=1
            with self.assertRaises(ValueError):module.validate_cost(bad)
    def test_missing_scan_and_invalid_observation_reject(self):
        original=json.loads((ROOT/'local-native/profiled/result.json').read_text())['cuts'][0]['balance_cost']
        for mutate in [lambda v:v.update(valid=False),lambda v:v['work_event_deltas'].update({'owner-evaluation':0}),lambda v:v['categories']['clustering'].update(entries=0),lambda v:v['shape_after'].update(nodes=1)]:
            bad=copy.deepcopy(original);mutate(bad)
            with self.assertRaises(ValueError):module.validate_cost(bad)
if __name__=='__main__':unittest.main()
