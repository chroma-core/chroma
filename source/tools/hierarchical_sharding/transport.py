"""Fail-closed HTTP fanout. Distances use the index's smaller-is-better convention."""
import concurrent.futures
import json
import math
import os
import tempfile
import time
import socket
import urllib.error
import uuid
import urllib.request
from pathlib import Path


class ShardError(RuntimeError):
    pass


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def validate_manifest(manifest):
    if not _integer(manifest.get('generation')) or manifest['generation'] < 0:
        raise ValueError('generation must be a nonnegative integer')
    if not _integer(manifest.get('dimension')) or manifest['dimension'] <= 0:
        raise ValueError('dimension must be positive')
    if manifest.get('metric') not in ('l2', 'cosine', 'ip'):
        raise ValueError('unsupported metric')
    shards = manifest.get('shards')
    if not isinstance(shards, list) or not shards:
        raise ValueError('at least one shard is required')
    seen = set()
    residues = set()
    stride = len(shards)
    total = manifest.get('total_vector_count')
    if not _integer(total) or total < 0:
        raise ValueError('explicit total_vector_count is required')
    construction = shards[0].get('construction')
    source_identity = shards[0].get('source_identity')
    source_id = shards[0].get('source_id')
    if not isinstance(source_id, str) or not source_id or not isinstance(construction, dict):
        raise ValueError('source_id and construction configuration are required')
    for shard in shards:
        if shard.get('construction') != construction or shard.get('source_identity') != source_identity or shard.get('source_id') != source_id:
            raise ValueError('shard construction or source identity mismatch')
        sid = shard.get('shard_id')
        if not isinstance(sid, str) or not sid or sid in seen:
            raise ValueError('shard ownership must be unique')
        seen.add(sid)
        for field in ('generation', 'dimension', 'metric'):
            if shard.get(field) != manifest[field]:
                raise ValueError('shard snapshot mismatch: ' + field)
        offset = shard.get('id_offset')
        if shard.get('id_stride') != stride or not _integer(offset) or not 0 <= offset < stride or offset in residues:
            raise ValueError('shards must own distinct residues of the fixed shard count')
        residues.add(offset)
        expected = max(0, (total - 1 - offset) // stride + 1)
        if not _integer(shard.get('vector_count')) or shard['vector_count'] != expected:
            raise ValueError('shard counts must cover a dense global ID prefix')
        if not isinstance(shard.get('endpoint'), str) or not shard['endpoint'].startswith(('http://', 'https://')):
            raise ValueError('shard endpoint required')
    return manifest


def publish_manifest(path, manifest):
    """Publish an already complete snapshot atomically; same generation is idempotent."""
    validate_manifest(manifest)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        previous = json.loads(path.read_text())
        if previous['generation'] > manifest['generation']:
            raise ValueError('cannot replace a newer generation')
        if previous['generation'] == manifest['generation']:
            if previous != manifest:
                raise ValueError('generation is immutable')
            return
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.manifest-')
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(manifest, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def validate_response(shard, response, k):
    if not isinstance(response, dict) or response.get('error'):
        raise ShardError('shard returned an error or malformed response')
    if response.get('generation') != shard['generation'] or response.get('shard_id') != shard['shard_id']:
        raise ShardError('shard returned a stale or foreign snapshot')
    ids, distances = response.get('ids'), response.get('distances')
    expected = min(k, shard['vector_count'])
    if not isinstance(ids, list) or not isinstance(distances, list) or len(ids) != expected or len(distances) != expected:
        raise ShardError('shard returned incomplete results')
    if len(set(ids)) != len(ids):
        raise ShardError('duplicate result IDs')
    for ident, distance in zip(ids, distances):
        if not _integer(ident) or ident < shard['id_offset'] or (ident - shard['id_offset']) % shard['id_stride'] != 0 or (ident - shard['id_offset']) // shard['id_stride'] >= shard['vector_count']:
            raise ShardError('result outside shard ownership')
        if isinstance(distance, bool) or not isinstance(distance, (float, int)) or not math.isfinite(distance):
            raise ShardError('invalid result distance')
    return list(zip(ids, distances))


def query_all(manifest, query, k, timeout=10, max_workers=16, beam_min=10, beam_max=256, tau=None, rerank=None, rerank_factor=None):
    validate_manifest(manifest)
    if not _integer(k) or k <= 0 or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0 or not _integer(max_workers) or max_workers <= 0:
        raise ValueError('k, timeout, and max_workers must be positive')
    if not _integer(beam_min) or not _integer(beam_max) or not 0 < beam_min <= beam_max:
        raise ValueError('beam bounds must be positive and ordered')
    if rerank is not None and rerank_factor is not None and rerank != rerank_factor:
        raise ValueError('rerank and rerank_factor disagree; both denote a multiplier')
    factor = rerank_factor if rerank_factor is not None else (rerank if rerank is not None else 10)
    if not _integer(factor) or not 2 <= factor <= 10000:
        raise ValueError('rerank_factor must be an integer from 2 through 10000')
    if tau is not None:
        raise ValueError('tau is fixed at 2.0 by this reader and cannot be overridden')
    if len(query) != manifest['dimension'] or any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) for v in query):
        raise ValueError('query must contain finite values matching dimension')
    def request(shard):
        body = {key: shard[key] for key in ('generation', 'shard_id', 'dimension', 'metric')}
        body.update(query=query, k=k, rerank_factor=factor, beam_min=beam_min, beam_max=beam_max)
        body['request_id'] = str(uuid.uuid4())
        req = urllib.request.Request(shard['endpoint'].rstrip('/') + '/query', data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
        try:
            started = time.monotonic()
            with urllib.request.urlopen(req, timeout=timeout) as response:
                payload = response.read(16 * 1024 * 1024 + 1)
            if len(payload) > 16 * 1024 * 1024:
                raise ShardError('oversized shard response')
            result = json.loads(payload)
            if result.get('request_id') != body['request_id']:
                raise ShardError('shard response request ID mismatch')
            if result.get('rerank_factor') != factor or result.get('candidate_budget') != k * factor:
                raise ShardError('shard returned a mismatched rerank factor or candidate budget')
            result['transport_elapsed_s'] = time.monotonic() - started
            return validate_response(shard, result, k), result
        except Exception as exc:
            if isinstance(exc, (TimeoutError, socket.timeout)) or isinstance(getattr(exc, 'reason', None), (TimeoutError, socket.timeout)):
                raise TimeoutError('query timed out for shard ' + shard['shard_id']) from exc
            raise ShardError('query failed for shard ' + shard['shard_id']) from exc
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(manifest['shards'])))
    futures = [executor.submit(request, shard) for shard in manifest['shards']]
    try:
        done, pending = concurrent.futures.wait(futures, timeout=timeout, return_when=concurrent.futures.FIRST_EXCEPTION)
        for future in done:
            future.result()
        if pending:
            raise TimeoutError('all-shard query deadline exceeded')
        groups = [future.result() for future in futures]
    finally:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False)
    candidates = [entry for group, _ in groups for entry in group]
    candidates.sort(key=lambda pair: (pair[1], pair[0]))
    best = candidates[:k]
    result = {'ids': [p[0] for p in best], 'distances': [p[1] for p in best], 'generation': manifest['generation'], 'shards_queried': len(groups), 'rerank_factor': factor, 'candidate_budget_per_shard': k * factor, 'candidate_budget': len(groups) * k * factor, 'shard_results': [payload for _, payload in groups]}
    for metric in ('physical_bytes_fetched', 'logical_vectors_scored'):
        values = [payload.get(metric) for _, payload in groups]
        if all(isinstance(v, (float, int)) and not isinstance(v, bool) and math.isfinite(v) and v >= 0 for v in values):
            result[metric] = sum(values)
    return result
