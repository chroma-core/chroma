import time
import struct
import tempfile
import unittest

from tools.hierarchical_sharding.benchmark import exact_shard, merge_exact_topk, run_load, split_queries


class BenchmarkTests(unittest.TestCase):
    def test_streamed_exact_reference_and_modulo_ids(self):
        with tempfile.NamedTemporaryFile() as source:
            source.write(struct.pack('<6f', 1, 0, 0, 1, -1, 0))
            source.flush()
            query = [{'query_id': 'q', 'vector': [1., 0.]}]
            for metric, expected in [('l2', [0., 2.]), ('ip', [0., 1.]), ('cosine', [0., 1.])]:
                result = exact_shard(source.name, query, 2, 2, metric, 1, 4)[0]
                self.assertEqual(result['ids'], [1, 5])
                self.assertEqual(result['distances'], expected)
            with self.assertRaises(ValueError):
                exact_shard(source.name, query, 4)

    def test_search_settings_forwarded(self):
        received = {}
        def backend(*args, **kwargs):
            received.update(kwargs)
            return {'ids': [0], 'generation': 1}
        _, summary = run_load({'generation': 1}, [{'query_id': 'q', 'vector': [0]}],
            {'q': [0]}, backend, k=1, query_settings={'beam_min': 8, 'beam_max': 64, 'rerank_factor': 10})
        self.assertEqual(received['beam_max'], 64)
        self.assertEqual(summary['query_settings']['rerank_factor'], 10)
        self.assertEqual(summary['rerank_budget']['per_shard_candidate_budget'], 10)
        with self.assertRaises(ValueError):
            run_load({'generation': 1}, [{'query_id': 'q', 'vector': [0]}], {'q': [0]}, backend, k=1, query_settings={'rerank_factor': 1})

    def test_exact_merge_global_order_and_ties(self):
        self.assertEqual(merge_exact_topk([
            {'ids': [9, 4], 'distances': [1., 2.]},
            {'ids': [2, 8], 'distances': [1., 3.]},
        ], 3)['ids'], [2, 9, 4])
        with self.assertRaises(ValueError):
            merge_exact_topk([{'ids': [2, 2], 'distances': [1., 2.]}], 2)

    def test_split_is_disjoint_and_reproducible(self):
        queries = [{'query_id': str(i), 'vector': [i]} for i in range(20)]
        a, b = split_queries(queries, 5, 10, 9)
        self.assertFalse({q['query_id'] for q in a} & {q['query_id'] for q in b})
        self.assertEqual((a, b), split_queries(queries, 5, 10, 9))
        with self.assertRaises(ValueError):
            split_queries(queries, 15, 10, 9)

    def test_errors_and_timeouts_are_in_latency_and_recall(self):
        def backend(manifest, vector, k, **kwargs):
            if vector[0] == 1:
                raise TimeoutError('shard deadline')
            if vector[0] == 2:
                raise RuntimeError('worker failed')
            return {'ids': [0, 1], 'generation': manifest['generation']}
        records, summary = run_load({'generation': 3},
            [{'query_id': str(i), 'vector': [i]} for i in range(3)],
            {str(i): [0, 1] for i in range(3)}, backend, k=2, concurrency=2)
        self.assertEqual(summary['requests'], 3)
        self.assertEqual(summary['timeouts'], 1)
        self.assertEqual(summary['errors'], 1)
        self.assertEqual(summary['mean_delivered_recall_at_k'], 1/3)
        self.assertIsNotNone(summary['latency_all_s']['p99'])
        self.assertEqual(len(records), 3)

    def test_late_response_is_timeout(self):
        def backend(manifest, vector, k, **kwargs):
            time.sleep(.01)
            return {'ids': [0], 'generation': 1}
        _, summary = run_load({'generation': 1}, [{'query_id': 'q', 'vector': [0]}],
                              {'q': [0]}, backend, k=1, timeout=.001)
        self.assertEqual(summary['timeouts'], 1)
        self.assertEqual(summary['successes'], 0)

    def test_generation_mismatch_is_error(self):
        _, summary = run_load({'generation': 1}, [{'query_id': 'q', 'vector': [0]}],
            {'q': [0]}, lambda *a, **kw: {'ids': [0], 'generation': 2}, k=1)
        self.assertEqual(summary['errors'], 1)


if __name__ == '__main__':
    unittest.main()
