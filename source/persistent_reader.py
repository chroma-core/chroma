"""Existing bounded local-pipe reader; no HTTP server or network operations."""
import json,os,time,selectors,subprocess,threading
class Reader:
    def __init__(self, command, timeout):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        self.timeout = timeout
        self.lock = threading.Lock()

    def query(self, payload, timeout=None, metrics=None):
        timeout = self.timeout if timeout is None else min(timeout, self.timeout)
        deadline = time.monotonic() + timeout
        if timeout <= 0 or not self.lock.acquire(timeout=timeout):
            raise TimeoutError('reader busy')
        acquired = time.monotonic()
        if metrics is not None:
            metrics['queue_wait_ms'] = (acquired - (deadline-timeout))*1000
        try:
            if self.process.poll() is not None:
                raise RuntimeError('reader exited')
            encoded = json.dumps(dict(payload, op=payload.get('op', 'query'))) + '\n'
            serialized = time.monotonic()
            self.process.stdin.write(encoded)
            self.process.stdin.flush()
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
            received = time.monotonic()
            result = json.loads(data)
            decoded = time.monotonic()
            if metrics is not None:
                metrics.update(serialization_ms=(serialized-acquired)*1000,
                               pipe_roundtrip_ms=(received-serialized)*1000,
                               decode_ms=(decoded-received)*1000,
                               response_bytes=len(data), request_bytes=len(encoded.encode()))
            return result
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
