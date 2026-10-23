"""Host batches for pinned lower subtrees; this adapter performs no search policy."""
import argparse
import concurrent.futures
import json
import socket
import threading
import time
import math
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from .subtree_transport import PinnedReader,TreeError


class Host:
    def __init__(self, configuration):
        self.configuration=configuration
        self.readers={sid:PinnedReader(value,configuration.get('timeout',30)) for sid,value in configuration['subtrees'].items()}

    def batch(self,payload):
        if payload.get('tree_sha256')!=self.configuration['tree_sha256']:
            raise TreeError('request references a different immutable tree')
        requests=payload.get('requests')
        if not isinstance(requests,list) or not 0<len(requests)<=128:
            raise TreeError('batch must contain 1 through 128 operations')
        if any(item.get('subtree_id') not in self.readers for item in requests):
            raise TreeError('owner does not serve requested subtree')
        if len({item['subtree_id'] for item in requests})!=len(requests):
            raise TreeError('one operation per subtree is required in each owner batch')
        timeout=payload.get('timeout_s',self.configuration.get('timeout',30))
        if type(timeout) not in (float,int) or not math.isfinite(timeout) or timeout<=0:
            raise TreeError('positive batch deadline required')
        deadline=time.monotonic()+min(timeout,self.configuration.get('timeout',30))
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(16,len(requests))) as pool:
            responses=list(pool.map(lambda item:self.readers[item['subtree_id']].call(item,max(.001,deadline-time.monotonic())),requests))
        return dict(tree_sha256=self.configuration['tree_sha256'],request_id=payload.get('request_id'),
                    owner_id=self.configuration['owner_id'],actual_host=socket.gethostname(),responses=responses)

    def close(self):
        for reader in self.readers.values():reader.close()


def make_server(host,bind='127.0.0.1',port=8080):
    maximum=host.configuration.get('max_inflight_batches',16)
    if type(maximum) is not int or not 1<=maximum<=256:
        raise ValueError('max_inflight_batches must be within 1 through 256')
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                self.connection.settimeout(host.configuration.get('timeout',30))
                size=int(self.headers.get('Content-Length','0'))
                if self.path!='/batch' or not 0<size<=4*1024*1024:
                    raise TreeError('invalid owner batch request')
                raw=self.rfile.read(size)
                decode_started=time.monotonic()
                payload=json.loads(raw)
                decoded=time.monotonic()
                result=host.batch(payload);status=200
                completed=time.monotonic()
            except Exception as error:
                result={'error':str(error)};status=503
            encode_started=time.monotonic()
            encoded=json.dumps(result,allow_nan=False).encode()
            encoded_at=time.monotonic()
            self.send_response(status)
            if status==200 and host.configuration.get('profile_host',False):
                self.send_header('X-Adapter-Decode-Ms',str((decoded-decode_started)*1000))
                self.send_header('X-Adapter-Batch-Ms',str((completed-decoded)*1000))
                self.send_header('X-Adapter-Encode-Ms',str((encoded_at-encode_started)*1000))
            self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(encoded)));self.end_headers();self.wfile.write(encoded)
        def log_message(self,*_args):pass
    class Server(ThreadingHTTPServer):
        daemon_threads=True
        slots=threading.BoundedSemaphore(maximum)
        def process_request(self,request,address):
            if not self.slots.acquire(blocking=False):
                self.reject_busy(request)
                return
            try:super().process_request(request,address)
            except Exception:self.slots.release();raise
        def reject_busy(self,request):
            # Consume a bounded request before closing so unread body bytes do
            # not reset the connection and discard the overload response.
            deadline=time.monotonic()+.25
            try:
                headers=bytearray()
                received=0
                expected=None
                while time.monotonic()<deadline:
                    if expected is not None and received>=expected:
                        break
                    request.settimeout(max(.001,deadline-time.monotonic()))
                    maximum=16384-len(headers) if expected is None else min(65536,expected-received)
                    if maximum<=0:
                        break
                    chunk=request.recv(maximum)
                    if not chunk:
                        break
                    received+=len(chunk)
                    if expected is None:
                        headers.extend(chunk)
                        end=headers.find(b'\r\n\r\n')
                        if end>=0:
                            lengths=[line.split(b':',1)[1].strip() for line in headers[:end].split(b'\r\n') if line.lower().startswith(b'content-length:')]
                            length=int(lengths[0]) if len(lengths)==1 else 0
                            if not 0<=length<=4*1024*1024:
                                break
                            expected=end+4+length
                            headers.clear()
            except (OSError,ValueError):
                pass
            body=json.dumps(dict(error='owner admission limit reached',code='owner_busy',retryable=True)).encode()
            header=('HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: '+str(len(body))+'\r\nConnection: close\r\n\r\n').encode()
            try:
                request.settimeout(.05)
                request.sendall(header+body)
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
        def process_request_thread(self,request,address):
            try:super().process_request_thread(request,address)
            finally:self.slots.release()
    return Server((bind,port),Handler)


def serve(host,bind='127.0.0.1',port=8080):
    server=make_server(host,bind,port)
    try:server.serve_forever()
    finally:server.server_close();host.close()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',required=True);p.add_argument('--host',default='127.0.0.1');p.add_argument('--port',type=int,default=8080);a=p.parse_args()
    host=Host(json.loads(Path(a.config).read_text()));serve(host,a.host,a.port)

if __name__=='__main__':main()
