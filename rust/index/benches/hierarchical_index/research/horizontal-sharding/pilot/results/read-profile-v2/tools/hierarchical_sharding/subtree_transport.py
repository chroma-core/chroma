"""One fixed global frontier and candidate budget over pinned remote subtrees."""
import argparse
import collections
import concurrent.futures
import hashlib
import json
import math
import mmap
import socket
import struct
import time
import urllib.request
import uuid
from pathlib import Path
from .tree_manifest import content_hash, checked_artifact, load_model, load_tree, load_placement


class TreeError(RuntimeError):
    pass


def integer(value):
    return type(value) is int and value >= 0


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def distance(query, centroid, metric):
    if metric == 'l2':
        return sum((a-b)**2 for a, b in zip(query, centroid))
    dot = sum(a*b for a, b in zip(query, centroid))
    if metric == 'ip':
        return 1-dot
    norm = math.sqrt(sum(x*x for x in query)*sum(x*x for x in centroid))
    if norm == 0:
        raise ValueError('cosine navigation requires nonzero query and centroid')
    return 1-dot/norm


def node_key(node):
    return (node['subtree_id'], 'leaf' if node['is_leaf'] else 'internal', node['node_id'])


class HttpBackend:
    """One HTTP batch per owner for each coordinator phase."""
    def __init__(self, placement):
        self.placement = placement

    def call_many(self, items, tree_hash, timeout):
        grouped = collections.defaultdict(list)
        for index, item in enumerate(items):
            owner = self.placement['owners'][item['subtree_id']]
            grouped[(owner['owner_id'], owner['endpoint'])].append((index, item))
        deadline = time.monotonic()+timeout
        def call(owner, group):
            owner_id, endpoint = owner
            started = time.monotonic()
            request_id = str(uuid.uuid4())
            payload = dict(tree_sha256=tree_hash, request_id=request_id,
                           requests=[item for _, item in group],timeout_s=max(.001,deadline-time.monotonic()))
            request = urllib.request.Request(endpoint.rstrip('/')+'/batch', data=json.dumps(payload, allow_nan=False).encode(), headers={'Content-Type':'application/json'})
            encoded_at=time.monotonic()
            with urllib.request.urlopen(request, timeout=max(.001, deadline-time.monotonic())) as stream:
                headers_at=time.monotonic()
                host_timing={key:float(stream.headers['X-Adapter-'+header+'-Ms']) for key,header in [('decode_ms','Decode'),('batch_ms','Batch'),('encode_ms','Encode')] if stream.headers.get('X-Adapter-'+header+'-Ms') is not None}
                raw = stream.read(64*1024*1024+1)
            received_at=time.monotonic()
            if len(raw)>64*1024*1024:
                raise TreeError('owner response too large')
            result = json.loads(raw)
            if result.get('tree_sha256')!=tree_hash or result.get('request_id')!=request_id or result.get('owner_id')!=owner_id:
                raise TreeError('owner returned a mismatched pinned batch')
            rows = result.get('responses')
            if not isinstance(rows,list) or len(rows)!=len(group):
                raise TreeError('owner returned an incomplete batch')
            if time.monotonic()>deadline:
                raise TimeoutError('owner batch exceeded query deadline')
            trace = dict(owner_id=owner_id, endpoint=endpoint, actual_host=result.get('actual_host'),
                         roundtrips=1, operation_count=len(group), elapsed_s=time.monotonic()-started)
            if host_timing:
                trace['adapter_timing']=host_timing
                trace['client_timing']=dict(request_build_ms=(encoded_at-started)*1000,headers_wait_ms=(headers_at-encoded_at)*1000,response_read_ms=(received_at-headers_at)*1000,response_decode_validate_ms=(time.monotonic()-received_at)*1000,response_bytes=len(raw))
            return [(index,row) for (index,_),row in zip(group,rows)], trace
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(1,min(16,len(grouped))))
        futures = [pool.submit(call, owner, group) for owner,group in grouped.items()]
        try:
            done, pending = concurrent.futures.wait(futures, timeout=max(.001,deadline-time.monotonic()), return_when=concurrent.futures.FIRST_EXCEPTION)
            for future in done:
                future.result()
            if pending:
                raise TimeoutError('tree owner deadline exceeded')
            responses, traces = [None]*len(items), []
            for future in futures:
                pairs, trace = future.result(); traces.append(trace)
                for index,row in pairs:
                    responses[index] = row
            return responses, traces
        except Exception as error:
            raise TreeError('tree query failed closed: '+str(error)) from error
        finally:
            for future in futures:
                future.cancel()
            pool.shutdown(wait=False)


class PinnedReader:
    """The same persistent JSONL worker supports direct and HTTP transports."""
    def __init__(self, descriptor, timeout=30):
        from .server import Reader
        self.descriptor = descriptor
        path = Path(descriptor['manifest'])
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest!=descriptor['snapshot_sha256']:
            raise ValueError('configured lower manifest checksum mismatch')
        self.snapshot = json.loads(path.read_text())
        actual = Path(descriptor['checkpoint_dir'])/('manifest-'+str(self.snapshot['generation'])+'.json')
        if hashlib.sha256(actual.read_bytes()).hexdigest()!=digest:
            raise ValueError('lower checkpoint differs from configured pinned manifest')
        runtime_threads=descriptor.get('runtime_threads',2)
        if type(runtime_threads) is not int or not 1<=runtime_threads<=64:
            raise ValueError('runtime_threads must be within 1 through 64')
        self.reader = Reader(['env','TOKIO_WORKER_THREADS='+str(runtime_threads),descriptor['binary'],'read','--checkpoint-dir',descriptor['checkpoint_dir'],
                              '--generation',str(self.snapshot['generation'])], timeout)

    def call(self, item, timeout=None):
        if item.get('snapshot_sha256')!=self.descriptor['snapshot_sha256']:
            raise TreeError('request does not match pinned lower snapshot')
        payload = dict(item['payload'])
        if payload.get('op') not in ('describe_root','expand','score_selected_leaves','rerank_selected'):
            raise TreeError('unsupported subtree operation')
        payload['generation'] = self.snapshot['generation']
        payload['request_id'] = str(uuid.uuid4())
        metrics = {} if self.descriptor.get('profile_reader', False) else None
        result = self.reader.query(payload, timeout=timeout, metrics=metrics)
        if result.get('error'):
            raise TreeError('lower worker failed: '+str(result['error']))
        if result.get('request_id')!=payload['request_id'] or result.get('generation')!=self.snapshot['generation'] or result.get('shard_id')!=self.snapshot['shard_id']:
            raise TreeError('lower worker returned a mismatched generation or request')
        if metrics is not None:
            result['adapter_timing'] = metrics
        return dict(subtree_id=item['subtree_id'],snapshot_sha256=self.descriptor['snapshot_sha256'],result=result)

    def close(self):
        self.reader.close()


class LocalBackend:
    def __init__(self, descriptors, timeout=30):
        self.readers = {sid:PinnedReader(desc,timeout) for sid,desc in descriptors.items()}

    def call_many(self, items, tree_hash, timeout):
        started = time.monotonic()
        rows = [self.readers[item['subtree_id']].call(item, max(.001,timeout-(time.monotonic()-started))) for item in items]
        if time.monotonic()-started>timeout:
            raise TimeoutError('direct reference exceeded query deadline')
        return rows, [dict(owner_id='direct-reference', actual_host=socket.gethostname(), roundtrips=0,
                           operation_count=len(items), elapsed_s=time.monotonic()-started)]

    def close(self):
        for reader in self.readers.values():
            reader.close()


class TreeCoordinator:
    def __init__(self, tree, model, backend, mappings=None):
        self.tree, self.model, self.backend = tree, model, backend
        self.tree_hash = content_hash(tree)
        self.subtrees = {s['subtree_id']:s for s in tree['subtrees']}
        self.mappings = mappings or {}

    @classmethod
    def from_files(cls, tree_path, placement_path, backend=None):
        path = Path(tree_path); tree = load_tree(path)
        placement = load_placement(placement_path,tree)
        mappings = {}
        for subtree in tree['subtrees']:
            file = checked_artifact(subtree['local_to_global'],path.parent).open('rb')
            mappings[subtree['subtree_id']] = (file,mmap.mmap(file.fileno(),0,access=mmap.ACCESS_READ))
        return cls(tree,load_model(tree,path.parent),backend or HttpBackend(placement),mappings)

    def close(self):
        for file,mapping in self.mappings.values():
            mapping.close(); file.close()
        if hasattr(self.backend,'close'):
            self.backend.close()

    def mapped_id(self, sid, local_id):
        if not integer(local_id) or local_id>=self.subtrees[sid]['vector_count']:
            raise TreeError('candidate local ID outside its pinned subtree')
        if sid in self.mappings:
            mapping = self.mappings[sid]
            if isinstance(mapping,tuple):
                return struct.unpack_from('<Q',mapping[1],8*local_id)[0]
            return mapping[local_id]
        raise TreeError('explicit local-to-global mapping required')

    def _call(self, operations, timeout):
        items = [dict(subtree_id=sid,snapshot_sha256=self.subtrees[sid]['snapshot']['sha256'],payload=payload)
                 for sid,payload in operations]
        rows,traces = self.backend.call_many(items,self.tree_hash,timeout)
        if len(rows)!=len(items):
            raise TreeError('incomplete lower operation set')
        results = []
        for item,row in zip(items,rows):
            if row.get('subtree_id')!=item['subtree_id'] or row.get('snapshot_sha256')!=item['snapshot_sha256']:
                raise TreeError('lower operation returned a different pinned subtree')
            result = row.get('result')
            if not isinstance(result,dict) or result.get('error'):
                raise TreeError('lower operation failed')
            if result.get('generation')!=self.subtrees[item['subtree_id']]['generation']:
                raise TreeError('lower operation returned a stale generation')
            results.append(result)
        return results,traces

    def verify_roots(self, timeout=30):
        operations = [(sid,dict(op='describe_root')) for sid in self.subtrees]
        results,traces = self._call(operations,timeout)
        for (sid,_), result in zip(operations,results):
            expected = self.subtrees[sid]['root_ref']; actual = result['root']
            if actual.get('node_id')!=expected['id'] or actual.get('is_leaf')!=(expected['kind']=='leaf'):
                raise TreeError('opened lower root differs from committed tree')
            if result.get('dimension')!=self.tree['dimension'] or result.get('metric')!=self.tree['metric'] or result.get('vector_count')!=self.subtrees[sid]['vector_count']:
                raise TreeError('opened lower metadata differs from committed tree')
        return traces

    def query(self, query, k, *, beam_width=2, candidate_budget=1000, timeout=30):
        if len(query)!=self.tree['dimension'] or not all(finite(x) for x in query):
            raise ValueError('query dimension and finite values required')
        if not integer(k) or k<1 or k>self.tree['source']['row_count']:
            raise ValueError('k must be within the committed vector count')
        if beam_width is not None and (not integer(beam_width) or beam_width<1):
            raise ValueError('beam_width must be positive or None for exhaustive traversal')
        if not integer(candidate_budget) or candidate_budget<k:
            raise ValueError('global candidate budget must be at least k')
        if not finite(timeout) or timeout<=0:
            raise ValueError('positive timeout required')
        started = time.monotonic(); deadline = started+timeout
        levels,transport,leaves,workers = [],[],{},[]
        def remaining():
            left = deadline-time.monotonic()
            if left<=0:
                raise TimeoutError('tree query deadline exceeded')
            return left
        candidates = []
        for child in self.tree['upper_root']['children']:
            sid = child['subtree_id']; root = self.subtrees[sid]['root_ref']
            candidates.append(dict(subtree_id=sid,node_id=root['id'],is_leaf=root['kind']=='leaf',
                                   distance=distance(query,self.model['centroids'][child['centroid_index']],self.tree['metric']),parent='upper-root'))
        visited = set(); expanded = ['upper-root']
        for level in range(129):
            candidates.sort(key=lambda n:(n['distance'],node_key(n)))
            selected = candidates if beam_width is None else candidates[:beam_width]
            levels.append(dict(level=level,expanded=expanded,candidates=candidates,selected=selected,
                               selected_count=len(selected),beam_width=beam_width))
            frontier = []
            for node in selected:
                key = node_key(node)
                if key in visited:
                    raise TreeError('cycle or repeated selected child in navigation tree')
                visited.add(key)
                if node['is_leaf']:
                    leaves[key] = node
                else:
                    frontier.append(node)
            if not frontier:
                break
            grouped = collections.defaultdict(list)
            for node in frontier:
                grouped[node['subtree_id']].append(node['node_id'])
            operations = [(sid,dict(op='expand',nodes=nodes,query=query)) for sid,nodes in sorted(grouped.items())]
            results,traces = self._call(operations,remaining());transport.extend(traces)
            candidates = []; child_keys = set()
            for (sid,payload),result in zip(operations,results):
                workers.append(dict(subtree_id=sid,op='expand',result=result))
                children = result.get('children')
                if not isinstance(children,list):
                    raise TreeError('worker did not return expanded children')
                for child in children:
                    if child.get('parent_id') not in payload['nodes'] or not integer(child.get('node_id')) or type(child.get('is_leaf')) is not bool or not finite(child.get('distance')):
                        raise TreeError('invalid parent/child reference or routing distance')
                    node = dict(child,subtree_id=sid)
                    key = node_key(node)
                    if key in child_keys:
                        raise TreeError('child occurs more than once in one global level')
                    child_keys.add(key);candidates.append(node)
            expanded = [dict(subtree_id=n['subtree_id'],node_id=n['node_id']) for n in frontier]
            if not candidates:
                raise TreeError('internal frontier has no children')
        else:
            raise TreeError('tree exceeds navigation depth limit')
        grouped = collections.defaultdict(list)
        for node in leaves.values():
            grouped[node['subtree_id']].append(node['node_id'])
        operations = [(sid,dict(op='score_selected_leaves',nodes=sorted(nodes),query=query,candidate_budget=candidate_budget)) for sid,nodes in sorted(grouped.items())]
        results,traces = self._call(operations,remaining());transport.extend(traces)
        unique = {}
        returned_candidates = 0
        for (sid,_),result in zip(operations,results):
            workers.append(dict(subtree_id=sid,op='score_selected_leaves',result=result))
            rows = result.get('candidates')
            if not isinstance(rows,list) or len(rows)>candidate_budget:
                raise TreeError('worker exceeded its safe local candidate contribution')
            for candidate in rows:
                local = candidate.get('local_id'); ident = self.mapped_id(sid,local)
                if candidate.get('id')!=ident or not finite(candidate.get('distance')):
                    raise TreeError('candidate differs from explicit global ownership or has invalid distance')
                row = dict(candidate,subtree_id=sid)
                old = unique.get(ident)
                if old is None or (row['distance'],sid,local)<(old['distance'],old['subtree_id'],old['local_id']):
                    unique[ident] = row
                returned_candidates += 1
        selected = sorted(unique.values(),key=lambda c:(c['distance'],c['id']))[:candidate_budget]
        if len(selected)<k:
            raise TreeError('selected leaves contain too few unique live candidates')
        grouped = collections.defaultdict(list)
        for candidate in selected:
            grouped[candidate['subtree_id']].append(candidate['local_id'])
        operations = [(sid,dict(op='rerank_selected',local_ids=ids,query=query)) for sid,ids in sorted(grouped.items())]
        results,traces = self._call(operations,remaining());transport.extend(traces)
        exact = []
        for (sid,payload),result in zip(operations,results):
            workers.append(dict(subtree_id=sid,op='rerank_selected',result=result))
            rows = result.get('results')
            if not isinstance(rows,list) or len(rows)!=len(payload['local_ids']) or {r.get('local_id') for r in rows}!=set(payload['local_ids']):
                raise TreeError('exact reranking did not return every globally selected candidate once')
            for row in rows:
                if row.get('id')!=self.mapped_id(sid,row.get('local_id')) or not finite(row.get('distance')):
                    raise TreeError('invalid exact reranking result')
                exact.append(row)
        exact.sort(key=lambda c:(c['distance'],c['id']))
        if len({r['id'] for r in exact})!=len(exact):
            raise TreeError('duplicate exact global result')
        remaining()
        return dict(ids=[r['id'] for r in exact[:k]],distances=[r['distance'] for r in exact[:k]],
                    tree_sha256=self.tree_hash,generation=self.tree['tree_generation'],policy='fixed-global-beam',
                    beam_width=beam_width,global_candidate_budget=candidate_budget,global_candidate_count=len(selected),
                    candidate_contributions=returned_candidates,reached_leaves=[dict(subtree_id=n['subtree_id'],node_id=n['node_id']) for n in leaves.values()],
                    levels=levels,selected_candidates=selected,transport=transport,worker_operations=workers,
                    actual_hosts=sorted({t['actual_host'] for t in transport if t.get('actual_host')}),
                    roundtrips=sum(t['roundtrips'] for t in transport),elapsed_s=time.monotonic()-started)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--tree',required=True);p.add_argument('--placement',required=True);p.add_argument('--queries',required=True);p.add_argument('--output',required=True);p.add_argument('--k',type=int,default=10);p.add_argument('--beam-width',type=int,default=2);p.add_argument('--exhaustive',action='store_true');p.add_argument('--candidate-budget',type=int,default=1000);a=p.parse_args()
    coordinator=TreeCoordinator.from_files(a.tree,a.placement)
    try:
        coordinator.verify_roots()
        with open(a.output,'w') as output:
            for line in open(a.queries):
                query=json.loads(line);result=coordinator.query(query['vector'],a.k,beam_width=None if a.exhaustive else a.beam_width,candidate_budget=a.candidate_budget)
                output.write(json.dumps(dict(query_id=query['query_id'],result=result),allow_nan=False)+'\n')
    finally:coordinator.close()

if __name__=='__main__':main()
