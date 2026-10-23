"""Real imports, saved admission, persistent pipe execution and CLI serialization."""
import copy,importlib.util,json,pathlib,subprocess,sys,tempfile,unittest
from harness_common import descriptor,write
HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('cost',HERE/'run-cost.py');cost=importlib.util.module_from_spec(spec);spec.loader.exec_module(cost)
FIXTURES=HERE/'fixtures'

class CostIntegration(unittest.TestCase):
    def test_real_modules_startup_pipe_and_summary_cli(self):
        # Actual retained, vector-free reply supplies the real native response shape.
        reply=cost.load(FIXTURES/'native-response.json')
        reply['query_stage_trace']=None;reply['query_stage_trace_policy']=None;reply['query_work']['counts']['navigation_level_trace']=None
        template=cost.load(HERE/'runtime-spec.template.json')
        actual_ids=cost.load(FIXTURES/'evaluation-query-ids.json')
        self.assertEqual(actual_ids,['canonical-doc:'+str(v) for j,v in enumerate(cost.cohort_ids()) if j%9!=0])
        # The real admission helper checks exact16K mapping, roots and census for each arm.
        with tempfile.TemporaryDirectory() as td:
            root=pathlib.Path(td);new=copy.deepcopy(template)
            from admission import input_snapshot
            for arm in cost.ARMS:
                store=root/arm;store.mkdir();mapping=store/'id-map-1.u64le';mapping.write_bytes(b''.join(i.to_bytes(8,'little') for i in range(16384)))
                s=new['snapshots'][arm];m=dict(generation=1,dimension=768,metric='ip',vector_count=16384,shard_id='global',block_ids=s['roots'],integrity_audit=s['census'],id_mapping=dict(sha256=descriptor(mapping)['sha256'],count=16384,bytes=131072,path=mapping.name))
                path=store/'manifest.json';write(path,m);s['manifest']=descriptor(path);s['mapping']=descriptor(mapping)
                actual,inventory=input_snapshot(s,9999999999);self.assertEqual(actual,m);self.assertEqual(len(inventory['files']),2)
            # Full real admission uses local fake metadata and empty-code leaves, not mocks.
            for arm in cost.ARMS:
                s=new['snapshots'][arm];overlay=root/('overlay-'+arm);overlay.mkdir()
                leaves=[]
                for i in range(s['census']['reachable_leaves']):
                    code=overlay/f'leaf-{i}.code4';code.write_bytes(b'');pin=descriptor(code)
                    leaves.append(dict(id=i,file=code.name,rows=0,unique_rows=0,bytes=0,sha256=pin['sha256']))
                write(overlay/'overlay.json',dict(policy='code4-residual-postings-v1',snapshot=s['manifest']['sha256'],dimension=768,metric='InnerProduct',root=s['root_ref']['id'],leaves=leaves))
                from admission import overlay_closure
                new['overlays'][arm]=overlay_closure(overlay,s)
                prior_file=root/(arm+'-baseline.json');write(prior_file,dict(arm=arm,measured=[dict(query_id=v) for v in actual_ids]));new['baseline'][arm]=descriptor(prior_file)
            build=root/'build.json';binary=root/'native';binary.write_bytes(b'fake-not-executed');new['binary']=descriptor(binary)
            write(build,dict(source_manifest_sha256=new['source_manifest_sha256'],binary=new['binary']));new['build']=descriptor(build)
            query_file=root/'queries.jsonl';query_file.write_text(''.join(json.dumps(dict(query_id=v,vector=[0.0]*768))+'\n' for v in actual_ids));new['queries']=descriptor(query_file)
            selection=root/'selection.json';write(selection,dict(snapshots=new['snapshots'],overlays=new['overlays']));new['selection']=descriptor(selection)
            lock=root/'selection.lock.json';write(lock,new['selection']);new['selection_lock']=descriptor(lock)
            admitted=cost.admit(new,9999999999);self.assertEqual([q['query_id'] for q in admitted[4]],actual_ids)
            original_queries=query_file.read_bytes()
            query_file.write_text(''.join(json.dumps(dict(query_id=v.removeprefix('canonical-doc:'),vector=[0.0]*768))+'\n' for v in actual_ids));new['queries']=descriptor(query_file)
            with self.assertRaisesRegex(ValueError,'frozen 256-query order'):cost.admit(new,9999999999)
            query_file.write_bytes(original_queries);new['queries']=descriptor(query_file)
            (root/'overlay-dynamic4/leaf-0.code4').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError,'overlay leaf digest'):cost.admit(new,9999999999)
            # Use the retained manifest's identity plus its exact generation audit.
            s=template['snapshots']['reference'];snapshot={k:reply['query_navigation_snapshot'][k] for k in reply['query_navigation_snapshot']};snapshot['integrity_audit']=s['census']
            prior=cost.load(FIXTURES/'prior-query.json')
            queries=[dict(query_id=v,vector=[0.0]*768) for v in actual_ids]
            prior_rows=[dict(prior,query_id=v) for v in actual_ids]
            # Small fake source emits genuine native-shaped metadata, never invokes native/AWS.
            sample=root/'reply.json';write(sample,reply)
            self.assertEqual(cost.freeze_threads(),dict(TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1'))
            emitter=root/'fake_reader.py';emitter.write_text('import sys,json,os\nassert os.environ["TOKIO_WORKER_THREADS"]==os.environ["RAYON_NUM_THREADS"]=="4" and os.environ["OPENBLAS_NUM_THREADS"]==os.environ["OMP_NUM_THREADS"]==os.environ["MKL_NUM_THREADS"]=="1"\nv=json.load(open(sys.argv[1]))\nfor line in sys.stdin:\n q=json.loads(line);v["request_id"]=q["request_id"];print(json.dumps(v),flush=True)\n')
            result=cost.run_arm([sys.executable,str(emitter),str(sample)],'reference',snapshot,template['overlays']['reference'],queries,dict(measured=prior_rows),root,30)
            self.assertEqual(len(result['rows']),1024);self.assertEqual(result['process']['returncode'],0);self.assertEqual(result['summary']['native_query_means_ms']['count'],256)
            inp=root/'summary-input.json';write(inp,dict(input=descriptor(root/'reference-result.json'),output=str(root/'summary.json')))
            p=subprocess.run([sys.executable,str(HERE/'run-cost.py'),'--summarize',str(inp)],capture_output=True,text=True,timeout=10)
            self.assertEqual(p.returncode,0,p.stderr);self.assertEqual(json.loads(p.stdout),cost.load(root/'summary.json'));self.assertEqual(cost.load(root/'summary.json'),result['summary'])
            bad=copy.deepcopy(reply);bad['ids'][0]+=1
            request=dict(request_id=reply['request_id'])
            with self.assertRaisesRegex(ValueError,'same IDs'):cost.validate_response(bad,request,prior,snapshot,template['overlays']['reference'])
            self.assertNotIn('--trace-query-stages',cost.command('native',s,template['overlays']['reference']))

    def test_inference_and_cli_fail_closed(self):
        cost.check_inference(cost.load(HERE/'recovered-inference.json'))
        bad=cost.load(HERE/'recovered-inference.json');bad['outcome']='accepted'
        with self.assertRaises(ValueError):cost.check_inference(bad)
        p=subprocess.run([sys.executable,str(HERE/'run-cost.py'),'--help'],capture_output=True,text=True,timeout=5);self.assertEqual(p.returncode,0)
        p=subprocess.run([sys.executable,str(HERE/'run-cost.py'),'--spec',str(HERE/'runtime-spec.template.json')],capture_output=True,text=True,timeout=5)
        self.assertNotEqual(p.returncode,0)  # Linux gate or null deadline; never starts a native child.

if __name__=='__main__':unittest.main()
