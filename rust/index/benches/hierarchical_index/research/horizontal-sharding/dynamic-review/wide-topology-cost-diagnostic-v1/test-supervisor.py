import importlib.util,pathlib,tempfile,time,sys,json
p=pathlib.Path(__file__).with_name('supervise-local.py');s=importlib.util.spec_from_file_location('supervisor',p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
with tempfile.TemporaryDirectory() as directory:
 root=pathlib.Path(directory)
 for name,code,seconds,probe,expected in [('success','print("ok")',2,lambda _:1024,None),('deadline','import time;time.sleep(2)',.15,lambda _:1024,'deadline'),('rss','import time;time.sleep(2)',2,lambda _:2048,'sampled-rss-limit')]:
  r=m.supervise([sys.executable,'-c',code],root/name,root/'absent',time.monotonic()+seconds,1024 if name=='rss' else 1024**3,100000,probe=probe);assert r['guard_failure']==expected,(name,r);assert r['child_user_seconds']>=0 and r['terminal_child_max_rss_bytes']>0
 try:m.supervise([sys.executable,'-c','raise Exception("must not spawn")'],root/'expired',root/'absent',time.monotonic()-1,1024,100000);raise AssertionError('expired admitted')
 except ValueError:pass
 assert not (root/'expired').exists()
 fixture={'after_save':{'shape':{'reachable_nodes':9},'pending_balance':0,'counts':{'splits':1},'live_topology':{'evictions':1,'durable_root_commits':2}},'census':{'current_rows':64,'vector_bits_verified':True,'query_exact_distance_bits_verified':True},'native_before_serialize_seconds':10}
 for key in ['input_seconds','setup_seconds','add_seconds','balance_seconds','placement_seconds','save_seconds','query_open_seconds','query_seconds','audit_seconds']:fixture[key]=1
 assert m.classify(fixture,True,8,64)['status']=='supported-mechanism';fixture['after_save']['shape']['reachable_nodes']=8;assert m.classify(fixture,True,8,64)['status']=='inconclusive';fixture['native_before_serialize_seconds']=8;assert m.classify(fixture,True,8,64)['unbucketed_before_serialize_seconds']==-1
print(json.dumps({'status':'passed','guard_cases':3,'structural_cases':3,'no_native16k_execution':True}))
