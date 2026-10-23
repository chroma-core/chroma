"""Reproducible closed-loop load driver; measurements include failed requests."""
import argparse
import concurrent.futures
import importlib
import heapq
import struct
import json
import math
import random
import threading
import time
from pathlib import Path


def merge_exact_topk(shards, k):
    """Merge exact shard candidates using ascending distance and global-ID ties.

    Every shard must provide at least min(k, shard population) exact candidates.
    Similarities must first be converted to ascending distances.
    """
    if k < 1:
        raise ValueError("k must be positive")
    candidates = {}
    for shard in shards:
        if len(shard['ids']) != len(shard['distances']):
            raise ValueError('candidate lengths differ')
        for ident, distance in zip(shard['ids'], shard['distances']):
            if not math.isfinite(distance):
                raise ValueError('nonfinite exact distance')
            if ident in candidates:
                raise ValueError('duplicate global ID across exact candidates')
            candidates[ident] = distance
    pairs = sorted(candidates.items(), key=lambda pair: (pair[1], pair[0]))[:k]
    return {'ids': [p[0] for p in pairs], 'distances': [p[1] for p in pairs]}


def exact_shard(path, queries, dimension, k=100, metric='l2', id_offset=0, id_stride=1):
    """Slow tiny-corpus oracle: stream little-endian f32, accumulate in float64.

    Memory is O(number of queries * k + dimension). Returns squared L2,
    one minus inner product, or one minus cosine similarity. Rejects zero norms.
    """
    if dimension < 1 or k < 1 or metric not in ('l2', 'ip', 'cosine') or id_offset < 0 or id_stride < 1:
        raise ValueError('invalid exact reference settings')
    vectors = [q['vector'] for q in queries]
    if any(len(q) != dimension or any(not math.isfinite(x) for x in q) for q in vectors):
        raise ValueError('invalid query vectors')
    norms = [sum(x*x for x in q) for q in vectors]
    if metric == 'cosine' and any(n == 0 for n in norms):
        raise ValueError('cosine requires nonzero vectors')
    heaps = [[] for _ in queries]
    decoder = struct.Struct('<' + str(dimension) + 'f')
    with open(path, 'rb') as source:
        row = 0
        while True:
            data = source.read(decoder.size)
            if not data:
                break
            if len(data) != decoder.size:
                raise ValueError('truncated f32 row')
            vector = decoder.unpack(data)
            norm = sum(x*x for x in vector)
            if any(not math.isfinite(x) for x in vector) or (metric == 'cosine' and norm == 0):
                raise ValueError('invalid corpus vector')
            ident = id_offset + row * id_stride
            for query, qnorm, heap in zip(vectors, norms, heaps):
                if metric == 'l2':
                    distance = sum((x-y)**2 for x, y in zip(query, vector))
                else:
                    dot = sum(x*y for x, y in zip(query, vector))
                    distance = 1-dot if metric == 'ip' else 1-dot/math.sqrt(norm*qnorm)
                item = (-distance, -ident)
                if len(heap) < k:
                    heapq.heappush(heap, item)
                elif item > heap[0]:
                    heapq.heapreplace(heap, item)
            row += 1
    results = []
    for query, heap in zip(queries, heaps):
        pairs = sorted((-d, -ident) for d, ident in heap)
        results.append({'query_id': query['query_id'], 'ids': [i for _, i in pairs], 'distances': [d for d, _ in pairs]})
    return results


def split_queries(rows, calibration_count, test_count, seed):
    if calibration_count < 1 or test_count < 1 or calibration_count + test_count > len(rows):
        raise ValueError('require enough distinct queries for positive calibration/test sizes')
    ids = [row['query_id'] for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError('duplicate query IDs')
    order = list(rows)
    random.Random(seed).shuffle(order)
    return order[:calibration_count], order[calibration_count:calibration_count + test_count]


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    return values[max(0, math.ceil(fraction * len(values)) - 1)]


def summarize(records, elapsed_s):
    successes = [r for r in records if r['status'] == 'ok']
    recalls = [r['recall_at_k'] for r in successes if r.get('recall_at_k') is not None]
    # Failed requests contribute zero to delivered recall, exposing partial outages.
    delivered = [r.get('recall_at_k') or 0 for r in records]
    return {
        'requests': len(records), 'successes': len(successes),
        'errors': sum(r['status'] == 'error' for r in records),
        'timeouts': sum(r['status'] == 'timeout' for r in records),
        'elapsed_s': elapsed_s,
        'offered_qps': len(records) / elapsed_s if elapsed_s else None,
        'successful_qps': len(successes) / elapsed_s if elapsed_s else None,
        'latency_all_s': {name: percentile([r['latency_s'] for r in records], p)
                          for name, p in [('p50', .5), ('p95', .95), ('p99', .99)]},
        'latency_success_s': {name: percentile([r['latency_s'] for r in successes], p)
                              for name, p in [('p50', .5), ('p95', .95), ('p99', .99)]},
        'mean_success_recall_at_k': sum(recalls) / len(recalls) if recalls else None,
        'mean_delivered_recall_at_k': sum(delivered) / len(delivered) if delivered else None,
    }


def run_load(manifest, queries, reference, query_fn, *, k=100, concurrency=1,
             timeout=30, repetitions=1, seed=1729, regime='warm', shard_workers=16, query_settings=None):
    settings = query_settings or {}
    factor = settings.get('rerank_factor', settings.get('rerank'))
    if factor is not None and (type(factor) is not int or factor < 2):
        raise ValueError('rerank_factor must be an integer >= 2; it multiplies k')
    if 'rerank_factor' in settings and 'rerank' in settings and settings['rerank_factor'] != settings['rerank']:
        raise ValueError('conflicting rerank factor aliases')
    if min(k, concurrency, repetitions, shard_workers) < 1 or timeout <= 0 or not queries:
        raise ValueError('positive load parameters and nonempty queries required')
    if len({q['query_id'] for q in queries}) != len(queries):
        raise ValueError('duplicate query IDs')
    for q in queries:
        truth = reference[q['query_id']]
        if len(truth) < k or len(set(truth[:k])) != k:
            raise ValueError('reference must provide k distinct global IDs')
    work = [(repeat, q) for repeat in range(repetitions) for q in queries]
    random.Random(seed).shuffle(work)
    iterator = iter(enumerate(work))
    lock = threading.Lock()
    records = []
    started = time.perf_counter()

    def worker():
        while True:
            with lock:
                item = next(iterator, None)
            if item is None:
                return
            sequence, (repeat, q) = item
            begin = time.perf_counter()
            row = {'sequence': sequence, 'query_id': q['query_id'], 'repeat': repeat,
                   'regime': regime, 'start_offset_s': begin - started, 'status': 'ok'}
            try:
                result = query_fn(manifest, q['vector'], k, timeout=timeout, max_workers=shard_workers, **(query_settings or {}))
                ids = result['ids']
                if len(ids) != k or len(set(ids)) != k:
                    raise ValueError('response must contain k distinct global IDs')
                if result.get('generation') != manifest['generation']:
                    raise ValueError('response generation mismatch')
                row['recall_at_k'] = len(set(ids) & set(reference[q['query_id']][:k])) / k
                row['result'] = result
            except Exception as exc:
                row['status'] = 'timeout' if isinstance(exc, TimeoutError) else 'error'
                row['error_type'] = type(exc).__name__
                row['error'] = str(exc)
            row['latency_s'] = time.perf_counter() - begin
            if row['latency_s'] > timeout:
                row['status'] = 'timeout'
                row['error'] = 'request exceeded total deadline (backend must enforce cancellation)'
                row.pop('recall_at_k', None)
            with lock:
                records.append(row)
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker) for _ in range(concurrency)]
        for future in futures:
            future.result()
    elapsed = time.perf_counter() - started
    summary = summarize(records, elapsed)
    summary.update({'load_model': 'closed_loop', 'concurrency': concurrency, 'k': k,
                    'repetitions': repetitions, 'seed': seed, 'regime': regime,
                    'generation': manifest['generation'], 'timeout_s': timeout, 'query_settings': settings,
                    'rerank_budget': {'factor': factor, 'per_shard_candidate_budget': k * factor if factor is not None else None,
                        'aggregate_candidate_budget': len(manifest['shards']) * k * factor if factor is not None and 'shards' in manifest else None,
                        'boundary': 'configured candidate budget; actual vectors scored unavailable unless emitted by backend'}})
    return sorted(records, key=lambda r: r['sequence']), summary


def read_jsonl(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line.strip()]


def write_jsonl(path, rows):
    Path(path).write_text(''.join(json.dumps(row, allow_nan=False) + '\n' for row in rows))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    exact = sub.add_parser('exact-shard')
    for name in ('vectors', 'queries', 'output'):
        exact.add_argument('--' + name, required=True)
    exact.add_argument('--dimension', type=int, required=True)
    exact.add_argument('--k', type=int, default=100)
    exact.add_argument('--metric', choices=('l2', 'ip', 'cosine'), default='l2')
    exact.add_argument('--id-offset', type=int, default=0)
    exact.add_argument('--id-stride', type=int, default=1)
    split = sub.add_parser('split')
    split.add_argument('--queries', required=True)
    split.add_argument('--calibration-count', type=int, required=True)
    split.add_argument('--test-count', type=int, required=True)
    split.add_argument('--seed', type=int, default=1729)
    split.add_argument('--output-prefix', required=True)
    merge = sub.add_parser('merge-exact')
    merge.add_argument('--shards', nargs='+', required=True)
    merge.add_argument('--k', type=int, default=100)
    merge.add_argument('--output', required=True)
    run = sub.add_parser('run')
    for name in ['manifest', 'queries', 'reference', 'output-prefix']:
        run.add_argument('--' + name, required=True)
    run.add_argument('--query-function', default='tools.hierarchical_sharding.transport:query_all')
    for name, default in [('k', 100), ('concurrency', 1), ('repetitions', 1), ('seed', 1729), ('shard-workers', 16)]:
        run.add_argument('--' + name, type=int, default=default)
    run.add_argument('--query-settings', default='{}', help='JSON search settings forwarded to backend')
    run.add_argument('--timeout', type=float, default=30)
    run.add_argument('--regime', required=True, help='Named externally prepared cache regime; does not flush caches')
    args = parser.parse_args()
    if args.command == 'exact-shard':
        write_jsonl(args.output, exact_shard(args.vectors, read_jsonl(args.queries), args.dimension, args.k, args.metric, args.id_offset, args.id_stride))
    elif args.command == 'split':
        calibration, test = split_queries(read_jsonl(args.queries), args.calibration_count, args.test_count, args.seed)
        write_jsonl(args.output_prefix + '.calibration.jsonl', calibration)
        write_jsonl(args.output_prefix + '.test.jsonl', test)
    elif args.command == 'merge-exact':
        shards = [{row['query_id']: row for row in read_jsonl(path)} for path in args.shards]
        if any(set(s) != set(shards[0]) for s in shards):
            raise ValueError('exact shard query sets differ')
        write_jsonl(args.output, [dict(query_id=qid, **merge_exact_topk([s[qid] for s in shards], args.k)) for qid in shards[0]])
    else:
        module, function = args.query_function.split(':')
        records, summary = run_load(json.loads(Path(args.manifest).read_text()), read_jsonl(args.queries),
            {r['query_id']: r['ids'] for r in read_jsonl(args.reference)}, getattr(importlib.import_module(module), function),
            k=args.k, concurrency=args.concurrency, timeout=args.timeout, repetitions=args.repetitions,
            seed=args.seed, regime=args.regime, shard_workers=args.shard_workers, query_settings=json.loads(args.query_settings))
        write_jsonl(args.output_prefix + '.queries.jsonl', records)
        Path(args.output_prefix + '.summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')


if __name__ == '__main__':
    main()
