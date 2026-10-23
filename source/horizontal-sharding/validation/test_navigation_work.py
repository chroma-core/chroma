import copy
import json
import unittest
from pathlib import Path

from navigation_work import validate_query_work_counts


def fixture():
    return {
        "expanded_internal_nodes": 2, "centroid_distance_evaluations": 4,
        "navigation_levels": 2, "peak_child_frontier": 2, "selected_leaves": 1,
        "posting_rows_scored_before_dedup": 1,
        "navigation_level_trace": {"complete": True, "level_cap": 128, "levels": [
            dict(level=1, expanded_internal=1, offered_internal=2, offered_leaf=0,
                 retained_internal=1, retained_leaf=0, discarded_internal=1, discarded_leaf=0),
            dict(level=2, expanded_internal=1, offered_internal=0, offered_leaf=2,
                 retained_internal=0, retained_leaf=1, discarded_internal=0, discarded_leaf=1),
        ]},
    }


class NavigationWorkValidation(unittest.TestCase):
    def test_internal_pruning_and_input_immutability(self):
        value = fixture(); before = copy.deepcopy(value)
        result = validate_query_work_counts(value, saved_internal_nodes=3, require_trace=True)
        self.assertTrue(result["internal_pruning"])
        self.assertEqual((result["discarded_internal"], result["discarded_leaf"]), (1, 1))
        self.assertNotIn("navigation_level_trace", result["scalar_counts"])
        self.assertEqual(value, before)

    def test_leaf_only_and_no_pruning_controls(self):
        value = fixture(); value["navigation_level_trace"]["levels"] = [value["navigation_level_trace"]["levels"][1]]
        value["navigation_level_trace"]["levels"][0]["level"] = 1
        value.update(expanded_internal_nodes=1, centroid_distance_evaluations=2, navigation_levels=1)
        self.assertFalse(validate_query_work_counts(value, saved_internal_nodes=1)["internal_pruning"])
        value["navigation_level_trace"]["levels"][0].update(retained_leaf=2, discarded_leaf=0)
        value["selected_leaves"] = 2
        self.assertFalse(validate_query_work_counts(value, saved_internal_nodes=1)["internal_pruning"])
        with self.assertRaises(ValueError):
            validate_query_work_counts(fixture(), saved_internal_nodes=1)
        self.assertFalse(validate_query_work_counts(fixture(), saved_internal_nodes=2)["internal_pruning"])

    def test_legacy_retained_counts_and_optional_null(self):
        path = Path(__file__).resolve().parents[1] / 'fleet/four-bit-navigation-quality-v1/runtime-evidence/runtime-attempt1/reference-four-bit.stdout'
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual(len(rows), 32)
        for row in rows:
            counts = row["query_work"]["counts"]
            self.assertFalse(validate_query_work_counts(counts, saved_internal_nodes=49)["trace_observed"])
            null = dict(counts, navigation_level_trace=None)
            self.assertIsNone(validate_query_work_counts(null, saved_internal_nodes=49)["internal_pruning"])
            with self.assertRaises(ValueError):
                validate_query_work_counts(null, saved_internal_nodes=49, require_trace=True)

    def test_root_leaf_empty_trace(self):
        value = dict(expanded_internal_nodes=0, centroid_distance_evaluations=0, navigation_levels=0,
                     peak_child_frontier=0, selected_leaves=1,
                     navigation_level_trace=dict(complete=True, level_cap=128, levels=[]))
        self.assertFalse(validate_query_work_counts(value, saved_internal_nodes=0)["internal_pruning"])

    def test_reject_malformed_and_incomplete(self):
        mutations = [
            lambda x: x.update(selected_leaves=True),
            lambda x: x.update(posting_rows_scored_before_dedup=-1),
            lambda x: x.update(extra={}),
            lambda x: x.update(extra=1.5),
            lambda x: x.pop("navigation_levels"),
            lambda x: x["navigation_level_trace"].update(complete=False),
            lambda x: x["navigation_level_trace"].update(complete=1),
            lambda x: x["navigation_level_trace"].update(level_cap=True),
            lambda x: x["navigation_level_trace"].update(level_cap=129),
            lambda x: x["navigation_level_trace"].update(levels=[]),
            lambda x: x["navigation_level_trace"]["levels"][0].update(offered_internal=3),
            lambda x: x["navigation_level_trace"]["levels"][0].update(discarded_internal=True),
            lambda x: x["navigation_level_trace"]["levels"][1].update(expanded_internal=2),
            lambda x: x["navigation_level_trace"]["levels"][1].update(level=3),
            lambda x: x.update(centroid_distance_evaluations=5),
            lambda x: x.update(expanded_internal_nodes=3),
            lambda x: x.update(peak_child_frontier=3),
            lambda x: x.update(selected_leaves=2),
            lambda x: x["navigation_level_trace"]["levels"][1].update(retained_internal=1, offered_internal=1),
            lambda x: x["navigation_level_trace"].update(extra=0),
            lambda x: x["navigation_level_trace"].update(levels=[x["navigation_level_trace"]["levels"][0]]*129),
        ]
        for mutation in mutations:
            value = fixture(); mutation(value)
            with self.subTest(mutation=mutations.index(mutation)), self.assertRaises(ValueError):
                validate_query_work_counts(value, saved_internal_nodes=3)
        for saved in (True, -1, 3.0):
            with self.assertRaises(ValueError): validate_query_work_counts(fixture(), saved_internal_nodes=saved)


if __name__ == '__main__':
    unittest.main()
