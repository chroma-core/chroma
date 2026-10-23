"""Exercise real Rust checkpoints through two persistent HTTP shard readers."""
import argparse
import json
import socket
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from .transport import publish_manifest, query_all


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--reader-binary', required=True)
    args = parser.parse_args()
    processes = []
    with tempfile.TemporaryDirectory(prefix='hierarchical-http-') as directory:
        root = Path(directory)
        def build(shard, generation, rows, resume=False):
            vectors = root / ('input-' + str(shard) + '-' + str(generation) + '.f32')
            vectors.write_bytes(struct.pack('<' + 'f' * len(rows) * 2, *(value for row in rows for value in row)))
            start = shard if not resume else shard + 4
            command = [args.reader_binary, 'build', '--vectors', str(vectors), '--generation', str(generation), '--source-id', 'http-smoke', '--dimension', '2', '--global-id-start', str(start), '--id-stride', '2', '--row-count', str(len(rows)), '--checkpoint-dir', str(root / str(shard)), '--shard-id', str(shard), '--metric', 'l2']
            if resume:
                command.append('--resume')
            subprocess.run(command, check=True, capture_output=True, text=True, timeout=20)
        def endpoint(shard, generation):
            with socket.socket() as reservation:
                reservation.bind(('127.0.0.1', 0))
                port = reservation.getsockname()[1]
            manifest_path = root / str(shard) / ('manifest-' + str(generation) + '.json')
            process = subprocess.Popen([sys.executable, '-m', 'tools.hierarchical_sharding.server', '--manifest', str(manifest_path), '--checkpoint-dir', str(root / str(shard)), '--reader-binary', args.reader_binary, '--port', str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            processes.append(process)
            deadline = time.monotonic() + 15
            while True:
                if process.poll() is not None:
                    raise RuntimeError(process.stderr.read().decode())
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('HTTP adapter did not start')
                    time.sleep(0.05)
            manifest = json.loads(manifest_path.read_text())
            manifest['endpoint'] = 'http://127.0.0.1:' + str(port)
            return manifest
        def collection(generation, shards, count):
            value = dict(generation=generation, dimension=2, metric='l2', total_vector_count=count, shards=shards)
            path = root / ('collection-' + str(generation) + '.json')
            publish_manifest(path, value)
            return json.loads(path.read_text())
        try:
            build(0, 1, [[0, 0], [2, 0]])
            build(1, 1, [[1, 0], [3, 0]])
            old = collection(1, [endpoint(0, 1), endpoint(1, 1)], 4)
            result = query_all(old, [0, 0], 4, rerank=4)
            assert result['ids'] == [0, 1, 2, 3], result
            assert result['distances'] == [0, 1, 4, 9], result
            build(0, 2, [[0.25, 0]], resume=True)
            build(1, 2, [[0.5, 0]], resume=True)
            current = collection(2, [endpoint(0, 2), endpoint(1, 2)], 6)
            result = query_all(current, [0, 0], 4, rerank=4)
            assert result['ids'] == [0, 4, 5, 1], result
            assert result['distances'] == [0, 0.0625, 0.25, 1], result
            assert query_all(old, [0, 0], 4, rerank=4)['ids'] == [0, 1, 2, 3]
            print(json.dumps({'status': 'passed', 'shards': 2, 'generations': [1, 2], 'checks': ['exact global IDs and distances', 'append generation', 'old endpoints remain pinned']}))
        finally:
            for process in processes:
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                process.stderr.close()


if __name__ == '__main__':
    main()
