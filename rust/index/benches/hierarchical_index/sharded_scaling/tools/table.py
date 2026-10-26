"""Print one row per configuration and pass for a reader bench run.

The columns follow a query's path: what it reached and scanned, how long it
took end to end, where that time went (navigation, each owner's fetch, scan,
selection and rerank, the remote request's overhead, the merge), what it cost
in CPU, bytes and heap, and how warm the caches were. Phase times are medians
over the pass's queries, so they need not add up to the median latency.

Usage: python3 table.py RESULTS_DIR [--markdown]
"""
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path


def med(values):
    values = [v for v in values if v is not None]
    return st.median(values) if values else None


def fmt(v, digits=2):
    if v is None:
        return '-'
    if isinstance(v, str):
        return v
    if abs(v) >= 1000:
        return f'{v:,.0f}'
    return f'{v:.{digits}f}'


def rows(results_dir):
    summary = json.loads((results_dir / 'summary.json').read_text())
    records = defaultdict(list)
    for line in (results_dir / 'queries.jsonl').read_text().splitlines():
        r = json.loads(line)
        records[(r['config'], r['pass'])].append(r)
    # A configuration can run the same pass name twice (cold,warm,cold).
    # Records are written in pass order, so the k-th summary entry for a
    # (configuration, pass) takes the k-th equal share of its records, and
    # its pass is shown as `cold#2`.
    repeats = defaultdict(int)
    for entry in summary['results']:
        repeats[(entry['config']['name'], entry['pass'])] += 1
    seen = defaultdict(int)
    for entry in summary['results']:
        c, s = entry['config'], entry['summary']
        key = (c['name'], entry['pass'])
        k, n = seen[key], repeats[key]
        seen[key] += 1
        share = len(records[key]) // n
        recs = records[key][k * share:(k + 1) * share]
        if k:
            entry = dict(entry, **{'pass': f"{entry['pass']}#{k + 1}"})
        parts = defaultdict(list)
        for r in recs:
            for p in (r.get('owners') or {}).get('parts', []):
                parts['remote' if p['remote'] else 'local'].append(p)
        owners = bool(parts)
        ms = lambda ns: None if ns is None else ns / 1e6
        phase = lambda kind, key: ms(med(p['timing'][key] for p in parts[kind]))
        # Load-pass records fetched from spread runs keep only timing and
        # recall fields, so per-query work columns are empty for them.
        vectors = med(r['work']['vectors'] for r in recs if 'work' in r)
        scan_ns = med(sum(p['timing']['scan'] for p in r['owners']['parts']) for r in recs) if owners else None
        lat = s['latency_ms'] or {}
        g = lambda key, stat='p50': (s.get(key) or {}).get(stat)
        b = lambda key: (s['bytes'].get(key) or {}).get('mean')
        remote = parts['remote']
        breakdowns = [(r.get('owners') or {}).get('remote_breakdown_seconds') for r in recs]
        breakdowns = [b for b in breakdowns if b]
        brk = lambda *keys: (med(sum(b.get(k) or 0 for k in keys) * 1e3 for b in breakdowns)
                             if breakdowns else None)
        # Bytes each machine read per query: postings scanned plus full-vector
        # rows reranked, plus the routing codes navigation scored on the query
        # machine. Fetched bytes are the share that came over the network.
        def per_query(kind, keys):
            out = []
            for r in recs:
                ps = [p for p in (r.get('owners') or {}).get('parts', []) if p['remote'] == (kind == 'remote')]
                out.append(sum(p['work'].get(k) or 0 for p in ps for k in keys))
            return st.mean(out) / 1e6 if out else None
        read_keys = ['posting_bytes_scanned', 'rerank_row_bytes']
        fetch_keys = ['posting_bytes_fetched', 'vector_store_bytes_loaded']
        nav_mb = (b('navigation_codes_read') or 0) / 1e6
        if owners:
            coord_mb = per_query('local', read_keys) + nav_mb
            peer_mb = per_query('remote', read_keys) if remote else 0.0
            coord_fetch = per_query('local', fetch_keys)
            peer_fetch = per_query('remote', fetch_keys) if remote else 0.0
        else:
            coord_mb = ((b('postings_scanned') or 0) + (b('full_vector_rows_read') or 0)) / 1e6 + nav_mb
            peer_mb, coord_fetch, peer_fetch = 0.0, (b('posting_bytes_fetched') or 0) / 1e6, 0.0
        walk = s.get('walk') or {}
        levels = {}
        for lv in walk.get('levels') or []:
            n = lv['level']
            levels[f'L{n} beam'] = (f"{lv['kept']:.0f}/{lv['scored']:.0f}" if lv.get('kept') is not None else '-')
            levels[f'L{n} reach'] = fmt(lv.get('reach'), 4)
        yield {
            'config': c['name'],
            'pass': entry['pass'],
            f'R@{c["k"]}': fmt(s['recall'], 4),
            'opt R': fmt(walk.get('optimal_recall'), 4),
            'leaf reach': fmt(walk.get('leaf_reach'), 4),
            **levels,
            'leaves': fmt(g('leaves', 'mean'), 0),
            'local/remote leaves': fmt(f"{med(p['leaves'] for p in parts['local']) or 0:.0f}/"
                                       f"{med(p['leaves'] for p in remote) or 0:.0f}") if owners else '-',
            'vectors': fmt(vectors, 0),
            'reranked': fmt(g('vectors_reranked', 'mean'), 0),
            'loaded MB': fmt(coord_mb + peer_mb, 1),
            'coord MB': fmt(coord_mb, 1),
            'peer MB': fmt(peer_mb, 1),
            'coord fetched MB': fmt(coord_fetch, 2),
            'peer fetched MB': fmt(peer_fetch, 2),
            'p50 ms': fmt(lat.get('p50')),
            'p99 ms': fmt(lat.get('p99')),
            'max ms': fmt(lat.get('max')),
            'nav ms': fmt(g('navigation_ms')),
            'local fetch': fmt(phase('local', 'fetch_wait')),
            'local scan': fmt(phase('local', 'scan')),
            'local select': fmt(phase('local', 'select')),
            'local rerank': fmt(phase('local', 'rerank')),
            'local total': fmt(phase('local', 'total')),
            'remote request': fmt(ms(med(p['request_ns'] for p in remote))),
            'peer fetch': fmt(phase('remote', 'fetch_wait')),
            'peer scan': fmt(phase('remote', 'scan')),
            'peer select': fmt(phase('remote', 'select')),
            'peer rerank': fmt(phase('remote', 'rerank')),
            'peer total': fmt(phase('remote', 'total')),
            'rpc overhead': fmt(ms(med(p['request_ns'] - p['timing']['total'] for p in remote))),
            # Where the remote request's overhead goes (builds with the RPC
            # breakdown): waiting on the query machine for a request thread
            # (a new thread per request before commit df4932e9f, a
            # persistent scan worker from it on), thread handoffs on the owner,
            # and the rest (network plus waking blocked threads).
            'req spawn': fmt(brk('client_spawn')),
            'owner handoffs': fmt(brk('server_decode_queue', 'server_dispatch', 'server_worker_wake')),
            'wire+wake': fmt(brk('unattributed')),
            'merge ms': fmt(g('merge_ms'), 3),
            'ns/vector': fmt(scan_ns / vectors if scan_ns and vectors else None, 1),
            'cpu ms': fmt(g('cpu_ms')),
            'peer cpu ms': fmt(g('remote_cpu_ms')),
            'scanned MB': fmt(b('postings_scanned') / 1e6 if b('postings_scanned') is not None else None, 1),
            'cold-equiv MB': fmt(b('cold_equivalent') / 1e6 if b('cold_equivalent') is not None else None, 1),
            'fetched MB': fmt(b('posting_bytes_fetched') / 1e6 if b('posting_bytes_fetched') is not None else None, 1),
            # Vector-store rerank: blocks loaded from storage and blocks read.
            'vec store MB': fmt(b('vector_store_bytes_loaded') / 1e6 if b('vector_store_bytes_loaded') is not None else None, 1),
            'vec blocks': fmt(b('vector_store_blocks_touched'), 1),
            'm2m KB': fmt(b('machine_to_machine') / 1e3 if b('machine_to_machine') is not None else None, 1),
            'heap KB': fmt(g('heap_peak_bytes') / 1e3 if g('heap_peak_bytes') is not None else None, 0),
            'peer heap KB': fmt(g('remote_heap_peak_bytes') / 1e3 if g('remote_heap_peak_bytes') is not None else None, 0),
            'cache hits': fmt(g('cache_hits', 'mean'), 1),
            'qps': fmt(s.get('reader_queries_per_second'), 1),
        }


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    results_dir = Path(sys.argv[1])
    table = list(rows(results_dir))
    # Runs that mix k values have one recall column per k; a row shows '-'
    # under the other k's column.
    columns = []
    for r in table:
        columns += [c for c in r if c not in columns]
    recalls = [c for c in columns if c.startswith('R@')]
    rest = [c for c in columns if not c.startswith('R@')]
    columns = rest[:2] + recalls + rest[2:]
    table = [{c: r.get(c, '-') for c in columns} for r in table]
    widths = {c: max(len(c), *(len(str(r[c])) for r in table)) for c in columns}
    if '--markdown' in sys.argv:
        print('| ' + ' | '.join(columns) + ' |')
        print('|' + '|'.join(' --- ' for _ in columns) + '|')
        for r in table:
            print('| ' + ' | '.join(str(r[c]) for c in columns) + ' |')
        return
    print('  '.join(c.rjust(widths[c]) for c in columns))
    for r in table:
        print('  '.join(str(r[c]).rjust(widths[c]) for c in columns))


if __name__ == '__main__':
    main()
