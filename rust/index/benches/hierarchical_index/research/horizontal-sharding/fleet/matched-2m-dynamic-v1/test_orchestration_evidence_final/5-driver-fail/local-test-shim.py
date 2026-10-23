import sys,json,importlib.util
sys.path.insert(0,'/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/fleet/matched-2m-dynamic-v1')
p=importlib.util.spec_from_file_location('o','/Users/chroma/Documents/ChatGPT/Hierarchical SPANN/horizontal-sharding/fleet/matched-2m-dynamic-v1/orchestrate-restored.py');m=importlib.util.module_from_spec(p);p.loader.exec_module(m)
s=json.load(open(sys.argv[1]));r=m.orchestrate(s,_cohort={'rows': 3, 'dimension': 2, 'metric': 'ip', 'source_id': 'fake-three'});print(json.dumps(r));raise SystemExit(not r['success'])
