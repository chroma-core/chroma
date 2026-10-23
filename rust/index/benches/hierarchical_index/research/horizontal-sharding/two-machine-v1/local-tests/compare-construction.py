"""Compare retained reference, prior candidate and updated candidate without summing nested timings."""
import argparse
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location('summary', Path(__file__).with_name('summarize-500k.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def metrics(path):
    summary = module.summarize(path)
    if isinstance(summary['gross_build_save'], dict):
        summary['gross_build_save'] = summary['gross_build_save']['wall_seconds']
    result = json.loads(Path(path).read_text())
    counters = {}
    for cycle in summary['cycles']:
        for key, value in cycle['work'].items():
            if isinstance(value, (int, float)):
                counters[key] = counters.get(key, 0) + value
    insertion = []
    if not summary['reference']:
        for cycle in result['cycles']:
            before = next(o for o in cycle['before_add'] if o['owner'] == 1)
            after = next(o for o in cycle['after_add'] if o['owner'] == 1)
            scopes = {}
            for name, fields in after['work']['inclusive_scopes'].items():
                old = before['work']['inclusive_scopes'].get(name, {})
                scopes[name] = {key: value - old.get(key, 0) for key, value in fields.items() if not key.startswith('max_')}
            insertion.append(dict(rows=cycle['rows_after'], wall_seconds=cycle['add_seconds'], owner_cpu_seconds=after['process_cpu_seconds'] - before['process_cpu_seconds'], distances=after['distances'] - before['distances'], inclusive_scopes=scopes))
    return dict(summary=summary, balance_counters=counters, insertion=insertion)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('reference', type=Path)
    parser.add_argument('previous', type=Path)
    parser.add_argument('candidate', type=Path)
    args = parser.parse_args()
    arms = {name: metrics(getattr(args, name)) for name in ('reference', 'previous', 'candidate')}
    assert len({arm['summary']['rows'] for arm in arms.values()}) == 1
    assert len({tuple(c['rows'] for c in arm['summary']['cycles']) for arm in arms.values()}) == 1
    comparisons = {}
    for key in ('add_seconds', 'balance_seconds', 'cpu_seconds', 'kernel_cpu_seconds', 'non_kernel_cpu_seconds'):
        ref, old, new = (arms[name]['summary']['totals'][key] for name in ('reference', 'previous', 'candidate'))
        comparisons[key] = dict(reference=ref, previous=old, candidate=new, candidate_over_reference=new/ref if ref else None, previous_over_candidate=old/new if new else None, reduction_fraction=(old-new)/old if old else None)
    for key in ('save_seconds', 'gross_build_save'):
        ref, old, new = (arms[name]['summary'][key] for name in ('reference', 'previous', 'candidate'))
        comparisons[key] = dict(reference=ref, previous=old, candidate=new, candidate_over_reference=new/ref if ref else None, previous_over_candidate=old/new if new else None, reduction_fraction=(old-new)/old if old else None)
    print(json.dumps(dict(arms=arms, comparisons=comparisons, caveats=['Use each source closure to identify the changed mechanisms; independently constructed trees prevent exact causal attribution of elapsed differences.', 'Trees are independently constructed; compare work counts as well as elapsed time.', 'Inclusive operation durations overlap and are not additive phase costs.', 'Construction CPU summary requires initial-owner construction before final redistribution.']), indent=2))


if __name__ == '__main__':
    main()
