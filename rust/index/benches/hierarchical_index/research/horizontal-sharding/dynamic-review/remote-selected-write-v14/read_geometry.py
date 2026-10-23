"""Inspect only the two already constructed tiny saved trees."""
import hashlib,json,os,struct,sys,time
from pathlib import Path
from tools.hierarchical_sharding.server import Reader
from tools.hierarchical_sharding.dynamic_runtime import inventory

binary=Path(sys.argv[1]);expected=sys.argv[2];output=Path(sys.argv[3]);assert not output.exists();output.mkdir()
assert hashlib.sha256(binary.read_bytes()).hexdigest()==expected
receipt=Path('/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/dynamic-review/remote-selected-write-v12/geometry-attempt2/receipt.json')
source=json.loads(receipt.read_text());parent=source['constructed_parent'];before={sid:inventory(o['checkpoint_dir']) for sid,o in parent['owners'].items()};raw=[];nodes={};exits=[];started=time.monotonic()
def f32(v):return struct.unpack('<f',struct.pack('<f',v))[0]
def distance(a,b):
    result=0.
    for x,y in zip(a,b):
        d=f32(f32(x)-f32(y));result=f32(result+f32(d*d))
    return result
try:
    for subtree in parent['tree']['subtrees']:
        sid=subtree['subtree_id'];owner=parent['owners'][sid];manifest=Path(owner['checkpoint_dir'])/('manifest-'+str(subtree['generation'])+'.json')
        reader=Reader([str(binary),'read','--checkpoint-dir',owner['checkpoint_dir'],'--generation',str(subtree['generation'])],5)
        try:
            def request(payload):
                assert time.monotonic()-started<30
                value=reader.query(payload);raw.append(dict(owner=sid,request=payload,response=value));return value
            root=request(dict(op='describe_root',request_id='root'))['root']['node_id'];pending=[root];seen=set();all_nodes=[]
            while pending:
                assert len(seen)+len(pending)<=128 and len(pending)==len(set(pending))
                result=request(dict(op='describe_fp_nodes',nodes=pending,request_id='nodes-'+str(len(seen))))
                assert 'error' not in result and result['diagnostic_only'] is True and result['dimension']==4
                descriptions=result['nodes'];assert [n['node_id'] for n in descriptions]==pending
                following=[]
                for node in descriptions:
                    assert node['node_id'] not in seen and len(node['centroid_bits'])==4
                    seen.add(node['node_id']);all_nodes.append(node);following.extend(node['children'])
                assert not seen.intersection(following);pending=following
            nodes[sid]=all_nodes
            for label,ids in [('duplicate',[root,root]),('empty',[]),('oversize',[root]*129),('missing',[4294967295])]:
                result=request(dict(op='describe_fp_nodes',nodes=ids,request_id=label));assert 'error' in result
            assert 'error' not in request(dict(op='describe_fp_nodes',nodes=[root],request_id='retry'))
        finally:
            reader.close();exits.append(dict(owner=sid,returncode=reader.process.returncode));assert type(reader.process.returncode) is int
    scores=[]
    for sid,descriptions in nodes.items():
        for node in descriptions:
            if node['is_leaf']:
                center=[struct.unpack('<f',struct.pack('<I',b))[0] for b in node['centroid_bits']]
                scores.append(dict(owner=sid,node_id=node['node_id'],centroid=center,distance=distance(source['vectors'][0],center)))
    winner=min(scores,key=lambda x:(x['distance'],x['owner'],x['node_id']));route=source['routes'][0]
    assert (winner['owner'],winner['node_id'],winner['distance'])==(route['subtree_id'],route['leaf_id'],route['distance'])
    assert winner['owner']!=source['nearest_upper']
    for sid,files in before.items():assert inventory(parent['owners'][sid]['checkpoint_dir'])==files
    (output/'receipt.json').write_text(json.dumps(dict(status='passed',scope='read-only actual FP node geometry; no writer/session/socket',binary_sha256=expected,source_receipt_sha256=hashlib.sha256(receipt.read_bytes()).hexdigest(),nodes=nodes,leaf_scores=scores,winner=winner,exits=exits,wall_seconds=time.monotonic()-started),indent=2)+'\n')
finally:
    (output/'raw.json').write_text(json.dumps(raw,indent=2)+'\n')
