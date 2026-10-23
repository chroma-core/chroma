"""Summarize sampled CPU evidence; percentages are not wall-time attribution."""
import argparse,json
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('evidence',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
rows=[]
for path in sorted(a.evidence.glob('0*/*.perf-phases.json')):
 d=json.loads(path.read_text());r=json.loads((path.parent/'result.json').read_text())
 row={'run':path.parent.name,'arm':r['arm'],'profile':str(path),'samples':d['samples'],'unknown_leaf_samples':d['unknown_leaf_samples'],'lost':d['lost_samples_reported'],'unparsed':d['unparsed_nonempty_lines'],'phases':{}}
 for phase in ['add','balance','save']:
  g=d['groups'][phase];leaf=dict(g['top_leaf_symbols']);inc=dict(g['top_inclusive_symbols']);count=g['samples']
  arc=sum(n for s,n in leaf.items() if s.startswith('clone<lock_api::rwlock::RwLock<parking_lot::raw_rwlock::RawRwLock, core::option::Option<single_shard_attribution::hierarchical_index::common::TreeNode>>'))
  entry={'samples':count,'unknown':g['unknown_leaf_samples'],'resident_arc_clone_leaf_samples_in_top80':arc,'resident_arc_clone_sample_percent':100*arc/count,'navigation_inclusive_samples':inc.get('navigate_f32_excluding (inlined)'),'node_store_get_inclusive_samples':inc.get('get<single_shard_attribution::hierarchical_index::common::TreeNode> (inlined)'),'reassign_many_inclusive_samples':inc.get('reassign_many (inlined)'),'leaf_split_inclusive_samples':inc.get('split_leaf (inlined)'),'clustering_split_inclusive_samples':inc.get('split (inlined)'),'fma_leaf_samples':leaf.get('_mm512_fmadd_ps (inlined)'),'byte_copy_leaf_samples':leaf.get('copy_nonoverlapping<u8> (inlined)')}
  row['phases'][phase]=entry
 rows.append(row)
assert len(rows)==4,'Require four profiles before complete summary'
a.output.write_text(json.dumps({'runs':rows,'limits':['CPU sampling estimates execution share, not exclusive wall time or waiting.','Inclusive symbols overlap and must not be added.','Only top80 symbols per category are available; missing is not proof of zero.','Event windows include nearby diagnostic snapshots; bounds are approximate.']},indent=2)+'\n')
print(json.dumps(rows,indent=2))
