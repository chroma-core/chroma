from pathlib import Path
import json,hashlib
p=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/dynamic-global-default-authority-v1');raw=p/'repair-v3/local-tests-attempt1/default-construction-result.json';d=json.loads(raw.read_text());loc={0:(0,0,1)};direct=[];versioned=[];cross_eligible=[];later=[];registered={};frames=[]
for index,e in enumerate(d['events']):
 k=e['kind']
 if k=='leaf_registered':
  id=e['id'];prev=loc.get(id)
  if prev is not None and prev[0]!=e['owner']:
   record={'position':index,'id':id,'before':prev,'after':[e['owner'],e['leaf'],e['version']]}
   (direct if prev[2]==e['version'] else versioned).append(record)
  loc[id]=(e['owner'],e['leaf'],e['version'])
 elif k=='split':
  for side in ['left','right']:
   for id in e[side+'_ids']:
    prev=loc[id];loc[id]=(e['owner'],e[side],prev[2])
 elif k=='move':
  r=e['reply'];ids=r.get('ids',[id for id,(owner,leaf,version) in loc.items() if owner==r['from'] and leaf==r['leaf']]);assert len(ids)==r['live'];
  for id in ids:
   old=loc[id];assert old[:2]==(r['from'],r['leaf']);loc[id]=(r['to'],old[1],old[2])
 elif k=='neighbor-begin':frames.append((e['split_owner'],e['owner'],e['leaf']))
 elif k=='neighbor-end':frames.pop()
 elif k=='owner-evaluation' and e['neighbor'] and e['eligible']:
  source,owner,leaf=frames[-1];assert owner==e['owner'] and leaf==e['leaf'];
  if owner!=source:cross_eligible.append({'position':index,'id':e['id'],'split_owner':source,'source_owner':owner,'leaf':leaf,'current':e['current']})
 elif k=='registered':registered[e['id']]=(index,e['current'])
 elif k=='before_scrub':
  for id,stored,current in e['versions'] or []:
   assert loc[id][2]==current
   if id in registered and registered[id][0]<index and registered[id][1]==current and stored!=current:later.append({'position':index,'id':id,'stored':stored,'current':current,'registration_position':registered[id][0]})
assert not frames and set(loc)==set(range(225));owners={o for o,l,v in loc.values()};assert len(owners)==1;final_owner=next(iter(owners));assert d["owner_counters"][str(final_owner)]["current_rows"]==225;leaf=d['census225']['leaves'][0]['leaf_id'];assert all(l==leaf for o,l,v in loc.values());
result={'kind':'offline replay of retained semantic events, not additional runtime','source':str(raw),'sha256':hashlib.sha256(raw.read_bytes()).hexdigest(),'cross_owner_neighbor_eligible':cross_eligible,'cross_owner_preserved_version_registration':direct,'cross_owner_bumped_version_registration':versioned,'later_version_visibility':later,'final_ids':len(loc),'final_leaf':leaf,'final_owner':final_owner,'all_final_locations_match_complete_census':True};(p/'DERIVED-TRANSFERS-V3.json').write_text(json.dumps(result,indent=2)+'\n');print({k:len(v) for k,v in result.items()if isinstance(v,list)})
