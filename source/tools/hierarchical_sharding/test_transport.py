import copy
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .transport import ShardError, publish_manifest, query_all, validate_manifest


def manifest():
    return dict(generation=1, dimension=2, metric='l2', total_vector_count=4, shards=[dict(generation=1, dimension=2, metric='l2', shard_id=str(i), id_offset=i, id_stride=2, vector_count=2, source_id='test-fixture', construction={}, endpoint='http://127.0.0.1:1') for i in range(2)])


class Tests(unittest.TestCase):
    def test_ownership(self):
        value = manifest()
        validate_manifest(value)
        value['shards'][1]['id_offset'] = 0
        with self.assertRaises(ValueError):
            validate_manifest(value)

    def test_stale_manifest(self):
        value = manifest()
        value['shards'][1]['generation'] = 0
        with self.assertRaises(ValueError):
            validate_manifest(value)

    def test_truncated_corpus(self):
        value = manifest()
        value['total_vector_count'] = 6
        with self.assertRaises(ValueError):
            validate_manifest(value)

    def test_global_deadline(self):
        import time
        from unittest.mock import patch
        def hung(*args, **kwargs):
            time.sleep(0.3)
            raise TimeoutError('simulated hung shard')
        start = time.monotonic()
        with patch('urllib.request.urlopen', side_effect=hung):
            with self.assertRaises(TimeoutError):
                query_all(manifest(), [0, 0], 2, timeout=0.05, max_workers=1)
        self.assertLess(time.monotonic() - start, 0.2)

    def test_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'manifest.json'
            value = manifest()
            publish_manifest(path, value)
            publish_manifest(path, value)
            changed = copy.deepcopy(value)
            changed['shards'][0]['endpoint'] += '/other'
            with self.assertRaises(ValueError):
                publish_manifest(path, changed)
            self.assertEqual(json.loads(path.read_text()), value)

    def run_query(self, faulty=None, **settings):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                sid = body['shard_id']
                result = dict(generation=1, shard_id=sid, request_id=body['request_id'], rerank_factor=body['rerank_factor'], candidate_budget=body['k']*body['rerank_factor'], ids=[0, 2] if sid == '0' else [1, 3], distances=[1.0, 3.0] if sid == '0' else [1.0, 2.0])
                if faulty and sid == '1':
                    result.update(faulty)
                data = json.dumps(result).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(data)
            def log_message(self, *_):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            value = manifest()
            for shard in value['shards']:
                shard['endpoint'] = 'http://127.0.0.1:' + str(server.server_port)
            return query_all(value, [0, 0], 2, **settings)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_topk_and_ties(self):
        self.assertEqual(self.run_query()['ids'], [0, 1])

    def test_bad_results_fail_closed(self):
        for faulty in ({'rerank_factor': 99}, {'candidate_budget': 1}, {'generation': 0}, {'ids': []}, {'ids': [0, 3]}, {'ids': [1, 1]}, {'error': 'failed'}):
            with self.subTest(faulty=faulty), self.assertRaises(ShardError):
                self.run_query(faulty)

    def test_rerank_factor_contract(self):
        result = self.run_query(rerank_factor=2)
        self.assertEqual(result['rerank_factor'], 2)
        self.assertEqual(result['candidate_budget_per_shard'], 4)
        self.assertEqual(result['candidate_budget'], 8)
        self.assertEqual(len(result['shard_results']), 2)
        self.assertEqual(self.run_query(rerank=3)['rerank_factor'], 3)
        self.assertEqual(self.run_query(rerank=3, rerank_factor=3)['rerank_factor'], 3)
        for settings in ({'rerank_factor': 1}, {'rerank_factor': True}, {'rerank_factor': 10001}, {'tau': 1.5}, {'rerank': 3, 'rerank_factor': 4}):
            with self.subTest(settings=settings), self.assertRaises(ValueError):
                query_all(manifest(), [0, 0], 100, **settings)

    def test_unreachable_shard_fails(self):
        with self.assertRaises(ShardError):
            query_all(manifest(), [0, 0], 2, timeout=0.1)


if __name__ == '__main__':
    unittest.main()

class ReaderTests(unittest.TestCase):
    def test_checkpoint_manifest_identity(self):
        from .server import load_snapshot
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            actual = manifest()['shards'][0]
            (root / 'manifest-1.json').write_text(json.dumps(actual))
            advertised = dict(actual, endpoint='http://other-host:8080')
            path = root / 'advertised.json'
            path.write_text(json.dumps(advertised))
            # Checkpoint manifests never contain transport endpoint metadata.
            actual.pop('endpoint')
            (root / 'manifest-1.json').write_text(json.dumps(actual))
            self.assertEqual(load_snapshot(path, root), advertised)
            advertised['source_id'] = 'different-source'
            path.write_text(json.dumps(advertised))
            with self.assertRaises(ValueError):
                load_snapshot(path, root)

    def test_persistent_reader(self):
        import sys
        from .server import Reader
        program = "import sys,json\nfor line in sys.stdin:\n p=json.loads(line);print(json.dumps({'generation':p['generation'],'op':p['op']}),flush=True)"
        reader = Reader([sys.executable, '-u', '-c', program], 1)
        try:
            self.assertEqual(reader.query({'generation': 1}), {'generation': 1, 'op': 'query'})
            self.assertEqual(reader.query({'generation': 2, 'op': 'expand'}), {'generation': 2, 'op': 'expand'})
        finally:
            reader.close()

    def test_partial_response_timeout(self):
        import sys
        from .server import Reader
        program = "import sys,time\nsys.stdin.readline();sys.stdout.write('{');sys.stdout.flush();time.sleep(10)"
        reader = Reader([sys.executable, '-u', '-c', program], 0.1)
        try:
            with self.assertRaises(TimeoutError):
                reader.query({})
        finally:
            reader.close()
