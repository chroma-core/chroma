"""A private-network HTTP adapter for one persistent Rust checkpoint reader."""
import argparse
import json
import os
import time
import selectors
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .transport import validate_response


class Reader:
    def __init__(self, command, timeout):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        self.timeout = timeout
        self.lock = threading.Lock()

    def query(self, payload):
        if not self.lock.acquire(timeout=self.timeout):
            raise TimeoutError('reader busy')
        try:
            if self.process.poll() is not None:
                raise RuntimeError('reader exited')
            self.process.stdin.write(json.dumps(dict(payload, op='query')) + '\n')
            self.process.stdin.flush()
            deadline = time.monotonic() + self.timeout
            data = bytearray()
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stdout, selectors.EVENT_READ)
                while b'\n' not in data:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        self.process.kill()
                        raise TimeoutError('reader timed out')
                    chunk = os.read(self.process.stdout.fileno(), 65536)
                    if not chunk:
                        raise RuntimeError('reader exited')
                    data.extend(chunk)
                    if len(data) > 16 * 1024 * 1024:
                        self.process.kill()
                        raise RuntimeError('reader response too large')
            if data.count(b'\n') != 1 or not data.endswith(b'\n'):
                self.process.kill()
                raise RuntimeError('reader emitted unexpected extra output')
            return json.loads(data)
        finally:
            self.lock.release()

    def close(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        self.process.stdin.close()
        self.process.stdout.close()


def serve(shard, reader, host='127.0.0.1', port=8080):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                self.connection.settimeout(reader.timeout)
                length = int(self.headers.get('Content-Length', '0'))
                if self.path != '/query' or not 0 < length <= 1024 * 1024:
                    raise ValueError('invalid request')
                payload = json.loads(self.rfile.read(length))
                for key in ('generation', 'shard_id', 'dimension', 'metric'):
                    if payload.get(key) != shard[key]:
                        raise ValueError('snapshot mismatch: ' + key)
                query, k = payload.get('query'), payload.get('k')
                import math
                if not isinstance(query, list) or len(query) != shard['dimension'] or any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in query):
                    raise ValueError('invalid query vector')
                if isinstance(k, bool) or not isinstance(k, int) or k <= 0:
                    raise ValueError('invalid k')
                result = reader.query(payload)
                if not isinstance(payload.get('request_id'), str) or result.get('request_id') != payload['request_id']:
                    raise ValueError('reader response request ID mismatch')
                validate_response(shard, result, k)
                status = 200
            except Exception as exc:
                status, result = 503, {'error': str(exc)}
            encoded = json.dumps(result).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)
        def log_message(self, *_args):
            pass
    class BoundedServer(ThreadingHTTPServer):
        slots = threading.BoundedSemaphore(16)
        def process_request(self, request, client_address):
            if not self.slots.acquire(blocking=False):
                self.shutdown_request(request)
                return
            try:
                super().process_request(request, client_address)
            except BaseException:
                self.slots.release()
                raise
        def process_request_thread(self, request, client_address):
            try:
                super().process_request_thread(request, client_address)
            finally:
                self.slots.release()
    server = BoundedServer((host, port), Handler)
    # One reader serializes requests. Bound waiting requests at the socket layer.
    server.request_queue_size = 16
    try:
        server.serve_forever()
    finally:
        server.server_close()
        reader.close()


def load_snapshot(manifest_path, checkpoint_dir):
    shard = json.loads(Path(manifest_path).read_text())
    actual = json.loads((Path(checkpoint_dir) / ('manifest-' + str(shard['generation']) + '.json')).read_text())
    advertised = {key: value for key, value in shard.items() if key != 'endpoint'}
    if advertised != actual:
        raise ValueError('advertised manifest does not match pinned checkpoint manifest')
    return shard


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--reader-binary', required=True)
    parser.add_argument('--checkpoint-dir', required=True)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--timeout', type=float, default=10)
    args = parser.parse_args()
    shard = load_snapshot(args.manifest, args.checkpoint_dir)
    reader = Reader([args.reader_binary, 'read', '--checkpoint-dir', args.checkpoint_dir, '--generation', str(shard['generation'])], args.timeout)
    serve(shard, reader, args.host, args.port)


if __name__ == '__main__':
    main()
