"""Tiny two-agent selected-write proof; no production or physical-host claim."""
import copy
import json
import os
from pathlib import Path
import secrets
import struct
import subprocess
import threading
import unittest
import uuid
import urllib.error
from unittest.mock import patch
from .tree_manifest import artifact,canonical_bytes,checked_artifact,file_hash
from .test_compactor_registration import CapturedAuthority
from .test_compactor_remote_topology import retained_context,advance


def retained_current_capture(prepared_path):
    """Capture the published fake-authority state retained by an earlier gate."""
    prepared=json.loads(Path(prepared_path).read_text());context=copy.deepcopy(prepared['context']);context['parent']=copy.deepcopy(prepared['candidate_state'])
    # A placement preserves the namespace construction snapshot. The current
    # Collection binding is separate and comes from the registered target.
    current=context['parent']['collection'];binding=context['record_collection_binding']
    binding['wire']['version']=current['version'];binding['wire']['log_position']=current['log_position']
    binding['total_records_post_compaction']=current['total_records_post_compaction'];binding['size_bytes_post_compaction']=current['size_bytes_post_compaction']
    context['compaction']=dict(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=current['log_position'],target_total_records=current['total_records_post_compaction'],target_logical_size_bytes=current['size_bytes_post_compaction'])
    from .compactor_namespace import current_view
    current_view(context)
    return context


@unittest.skipUnless(os.environ.get('SPANN_SELECTED_NATIVE'),'reviewed source and binaries required')
class NativeRemoteSelectedWrite(unittest.TestCase):
    def test_actual_materializer_global_route_direct_save_one_proposal(self):
        from .dynamic_agent import Agent,make_server
        from .dynamic_remote import AgentClient
        from .dynamic_runtime import PRODUCER_SOURCE_SHA256,inventory
        from .dynamic_write import prepare_global_append
        from .dynamic_remote_write import RemoteWriteExecution
        from .compactor_remote_topology import prepare_transferred_placement,submit_transferred_placement,reconcile_transferred_placement
        from .compactor_namespace import current_view,prepare_current_namespace_export,ExportedNamespaceAuthority,SavedNamespaceProducer,NamespacePreparation
        from .compactor_registration import CompactorRegistration
        from .compactor_topology import _capture_health
        root=Path(os.environ['SPANN_SELECTED_OUTPUT']).resolve();root.mkdir()
        context=retained_current_capture(os.environ['SPANN_SELECTED_PARENT']);reader=os.environ['SPANN_DYNAMIC_BINARY'];writer=os.environ['SPANN_SELECTED_WRITER'];auditor=Path(os.environ['SPANN_SELECTED_AUDITOR']);store=os.environ['SPANN_SELECTED_RECORD_STORE']
        self.assertEqual(file_hash(writer),os.environ['SPANN_SELECTED_WRITER_SHA']);self.assertEqual(file_hash(auditor),os.environ['SPANN_SELECTED_AUDITOR_SHA'])
        self.assertEqual(context['parent']['collection']['total_records_post_compaction'],18)
        agents=[];servers=[];threads=[];clients={};executions=[];generations=[];audits=[];cleanup=[]
        def audit(target,namespace,directory):
            directory=Path(directory);directory.mkdir();input_path=directory/'input.json';result=directory/'result.json';input_path.write_bytes(canonical_bytes(dict(capture=target,namespace=namespace,store=store,output=str(result))))
            run=subprocess.run([str(auditor),'--ignored','--exact','audit_current_namespace_view','--nocapture'],env=dict(os.environ,SPANN_NAMESPACE_VIEW_INPUT=str(input_path)),capture_output=True,timeout=10);(directory/'stdout').write_bytes(run.stdout);(directory/'stderr').write_bytes(run.stderr);self.assertEqual(run.returncode,0,run.stderr.decode());report=json.loads(result.read_text());self.assertEqual(report['namespace'],namespace);self.assertEqual(report['collection_binding'],target['record_collection_binding']);audits.append(artifact(result));return artifact(result)
        try:
            for worker in ['worker-a','worker-b']:
                scope=root/worker;scope.mkdir();token=scope/'token';token.write_text(secrets.token_hex(32));token.chmod(0o600);identity=dict(instanceId='loopback-'+worker,accountId='fixture',region='fixture',privateIp='127.0.0.1')
                config=dict(run_id='selected-'+worker,worker_id=worker,expected_identity=identity,binary=reader,binary_sha256=file_hash(reader),selected_writer_binary=writer,selected_writer_sha256=file_hash(writer),producer_sha256=PRODUCER_SOURCE_SHA256,token_file=str(token),scope_root=str(scope));agent=Agent(config,identity_reader=lambda identity=identity:identity);agents.append(agent);server=make_server(agent);servers.append(server);thread=threading.Thread(target=server.serve_forever);thread.start();threads.append(thread);clients[worker]=AgentClient('http://127.0.0.1:'+str(server.server_address[1]),token.read_text(),config['run_id'],timeout=10,worker_id=worker,instance_id=identity['instanceId'])
            delegate=CapturedAuthority(copy.deepcopy(context),[]);view=current_view(context);old_bundle=json.loads(checked_artifact(view['registered_bundle'],'.').read_text());assignments={sid:w for sid,w in zip(sorted(old_bundle['deployment']['owners']),clients)}
            placement,generation,ledger=prepare_transferred_placement(delegate,context['parent']['collection']['collection_id'],clients,assignments,root/'placement',reader,audit);generations.append(generation)
            self.assertEqual(submit_transferred_placement(delegate,placement,generation,audit)['status'],'pending-or-unknown');delegate.state=copy.deepcopy(placement['candidate_state']);self.assertEqual(reconcile_transferred_placement(delegate,placement)['status'],'committed')
            context=advance(context,placement);context['compaction'].update(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=24,target_total_records=20,target_logical_size_bytes=320)
            delegate=CapturedAuthority(copy.deepcopy(context),[]);view=current_view(context);parent=json.loads(checked_artifact(view['registered_bundle'],'.').read_text())['deployment'];before={sid:inventory(o['checkpoint_dir']) for sid,o in parent['owners'].items()}
            # The first row is nearer one upper centroid but its better lower
            # leaf is in the other subtree. The second row ties the two roots.
            rows=[dict(log_offset=23+i,user_id='remote-selected-'+str(i),embedding_bits=list(struct.unpack('<IIII',struct.pack('<ffff',*v)))) for i,v in enumerate([[8.25,1.,-0.,0.],[8.,4097.,-0.,0.]])]
            input_art=prepare_current_namespace_export(delegate,context['parent']['collection']['collection_id'],view['registered_bundle'],rows,store,root/'record-input')
            env=dict(os.environ,SPANN_NAMESPACE_CURRENT_INPUT=input_art['path'],SPANN_NAMESPACE_CURRENT_OUTPUT=str(root/'record-export'))
            run=subprocess.run([str(auditor),'--ignored','--exact','export_current_namespace_append','--nocapture'],env=env,capture_output=True,timeout=30);(root/'export.stdout').write_bytes(run.stdout);(root/'export.stderr').write_bytes(run.stderr);self.assertEqual(run.returncode,0,run.stderr.decode())
            export=artifact(root/'record-export/export.json');proof=artifact(root/'record-export/export.receipt.json');runtime=json.loads(checked_artifact(proof,'.').read_text())['runtime_binary'];self.assertEqual(runtime['sha256'],file_hash(auditor));actual=json.loads(checked_artifact(export,'.').read_text());self.assertEqual([(r['record_offset_id'],r['global_id']) for r in actual['captured']['rows']],[(19,18),(20,19)])
            # Compare the unchanged local command in both writer epochs against
            # the same logical roots/aliases. Only physical tuples are removed
            # from this private oracle view; no authority is published here.
            oracle_parent=copy.deepcopy(parent)
            for owner in oracle_parent['owners'].values():owner.pop('remote')
            oracles=[]
            for label,binary in [('baseline',os.environ['SPANN_WRITE_PLANNER_BINARY']),('extracted',writer)]:
                prepare_global_append(binary,reader,oracle_parent,actual['candidate_canonical']['path'],root/('oracle-'+label),timeout=30)
                oracles.append(json.loads((root/('oracle-'+label)/'write-plan.stdout').read_text()))
            self.assertEqual(oracles[0]['routes'],oracles[1]['routes'])
            def composer(path,delegate):
                source=ExportedNamespaceAuthority(delegate,export,proof,runtime);authority=NamespacePreparation(source,SavedNamespaceProducer(source,root/(path+'-namespace')))
                execution=RemoteWriteExecution(parent,context,export,clients,lambda:_capture_health(delegate,context));executions.append(execution)
                return CompactorRegistration(authority,context['parent']['collection']['collection_id'],writer,reader,root/path,vector_execution=execution),execution
            failed,failed_execution=composer('failed-private',delegate);original=clients['worker-a'].rpc
            def wrong_leaf(payload):
                if payload['op']=='selected-write-command' and payload['command']['op']=='register':
                    payload=copy.deepcopy(payload);payload['command']['leaf']=0xffffffff
                return original(payload)
            # Apply the same deterministic fault on either owner: the global
            # coordinator chooses the owner, and the worker rejects the leaf.
            originals={w:c.rpc for w,c in clients.items()}
            def corrupt(w):
                def call(payload):
                    if payload['op']=='selected-write-command' and payload['command']['op']=='register':payload=copy.deepcopy(payload);payload['command']['leaf']=0xffffffff
                    return originals[w](payload)
                return call
            with patch.object(clients['worker-a'],'rpc',side_effect=corrupt('worker-a')),patch.object(clients['worker-b'],'rpc',side_effect=corrupt('worker-b')):
                with self.assertRaises(urllib.error.HTTPError) as rejected:failed.prepare(str(uuid.uuid4()),failed.task_spec())
                self.assertEqual(rejected.exception.code,503);body=rejected.exception.read(16384);self.assertIn(b'selected reached leaf/ID/grant differs',body);(root/'rejected-leaf-response.json').write_bytes(body)
            self.assertEqual(delegate.starts,0);self.assertFalse((root/'failed-private/hierarchy-bundle.json').exists());generation.verify()
            # A rejected mutation remains explicitly unreconciled in its ledger.
            with self.assertRaisesRegex(ValueError,'unreconciled'):failed_execution.close()
            session,execution=composer('private',delegate);reply=session.prepare(str(uuid.uuid4()),session.task_spec());self.assertEqual(reply['routes'],oracles[0]['routes'])
            model=json.loads(checked_artifact(parent['tree']['model'],'.').read_text());nearest=min(parent['tree']['subtrees'],key=lambda s:sum((a-b)**2 for a,b in zip([8.25,1.,0.,0.],model['centroids'][s['centroid_index']])) )['subtree_id'];self.assertNotEqual(reply['routes'][0]['subtree_id'],nearest)
            protocol=[json.loads(line) for line in (root/'private/private-writers/write-commands.jsonl').read_text().splitlines()]
            roots=[r['response']['root']['distance_bits'] for r in protocol if r.get('row_id')==19 and r.get('command',{}).get('op')=='root'];self.assertEqual(len(roots),2);self.assertEqual(roots[0],roots[1])
            final=json.loads(checked_artifact(reply['descriptor'],'.').read_text());candidate=json.loads(checked_artifact(final['hierarchy_metadata']['namespace_root'],'.').read_text());mapping=json.loads(checked_artifact(candidate['mapping'],'.').read_text());old=json.loads(checked_artifact(view['namespace']['mapping'],'.').read_text());self.assertEqual(mapping[:18],old);self.assertEqual([(r['record_offset_id'],r['global_id']) for r in mapping[-2:]],[(19,18),(20,19)])
            stale_delegate=CapturedAuthority(copy.deepcopy(context),[]);stale_delegate.state['collection']['version']+=1
            stale_source=ExportedNamespaceAuthority(stale_delegate,export,proof,runtime);stale_session=copy.copy(session);stale_session.authority=NamespacePreparation(stale_source,SavedNamespaceProducer(stale_source,root/'unused-stale-namespace'))
            with self.assertRaisesRegex(ValueError,'parent changed'):stale_session.submit()
            self.assertEqual(stale_delegate.starts,0);self.assertIsNone(session.attempt)
            saved_leaf_responses=[]
            for route in reply['routes']:
                sid=route['subtree_id'];slot=next(s for s in final['deployment']['tree']['subtrees'] if s['subtree_id']==sid);remote=final['deployment']['owners'][sid]['remote'];worker=remote['worker_id'];request_id=uuid.uuid4().hex
                payload=dict(tree_sha256=final['deployment']['tree_sha256'],request_id=request_id,timeout_s=5,requests=[dict(subtree_id=sid,snapshot_sha256=slot['snapshot']['sha256'],payload=dict(op='score_selected_leaves',query=[0.]*4,nodes=[route['leaf_id']],candidate_budget=128))])
                response=clients[worker].rpc(dict(op='probe-generation',generation_id=execution.generation.generations[worker],payload=payload));hits=response['responses'][0]['result']['candidates'];self.assertEqual(sum(h['id']==route['id'] and h['local_id']==route['local_id'] for h in hits),1);saved_leaf_responses.append(dict(route=route,request=payload,response=response))
            (root/'saved-leaves.json').write_bytes(canonical_bytes(saved_leaf_responses))
            self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(delegate.starts,1);delegate.state=copy.deepcopy(session.candidate_state);self.assertEqual(session.reconcile()['status'],'committed')
            for sid,files in before.items():self.assertEqual(inventory(parent['owners'][sid]['checkpoint_dir']),files)
            self.assertFalse((root/'current.json').exists());(root/'receipt.json').write_bytes(canonical_bytes(dict(export=export,proof=proof,input=input_art,descriptor=reply['descriptor'],routes=reply['routes'],oracle_routes=oracles[0]['routes'],mapping=candidate['mapping'],audits=audits,parent_inventory=before,no_second_pointer=True,scope='two loopback agents, actual materializer/record/vector writes, fake collection authority')))
        finally:
            # Known failed attempts retain unreconciled ledgers. All actual
            # children are still owned and closed by their agent below.
            for execution in reversed(executions):
                try:execution.close()
                except Exception as error:cleanup.append(repr(error))
            for generation in generations:
                try:generation.close()
                except Exception as error:cleanup.append(repr(error))
            for server in servers:server.shutdown();server.server_close()
            for thread in threads:thread.join(2)
            for agent in agents:agent.close()
            (root/'cleanup.json').write_bytes(canonical_bytes(dict(known_unreconciled_attempts=cleanup,threads_alive=[t.is_alive() for t in threads],writer_children=[dict(state=s['state'],returncode=None if s['channel'] is None else s['channel'].process.returncode) for a in agents for s in a.write_sessions.sessions.values()])))
            self.assertFalse(any(t.is_alive() for t in threads));self.assertTrue(all(s['channel'] is None or s['channel'].process.returncode is not None for a in agents for s in a.write_sessions.sessions.values()))


@unittest.skipUnless(os.environ.get('SPANN_SELECTED_GEOMETRY'),'reviewed private geometry preflight required')
class NativeWriteGeometry(unittest.TestCase):
    def test_retained_reassignment_has_cross_boundary_write_destination(self):
        from .dynamic_write import prepare_global_append
        from .dynamic_runtime import inventory
        from .tree_manifest import content_hash
        root=Path(os.environ['SPANN_SELECTED_GEOMETRY_OUTPUT']).resolve();root.mkdir()
        capture=retained_current_capture(os.environ['SPANN_SELECTED_PARENT'])
        from .compactor_namespace import current_view
        view=current_view(capture);bundle=json.loads(checked_artifact(view['registered_bundle'],'.').read_text());parent=bundle['deployment']
        self.assertEqual(parent['tree']['source']['row_count'],18);self.assertEqual(parent['tree']['dimension'],4)
        self.assertFalse(any(o.get('remote') for o in parent['owners'].values()))
        before={sid:inventory(o['checkpoint_dir']) for sid,o in parent['owners'].items()}
        vectors=[[8.25,1.,-0.,0.],[8.,4097.,-0.,0.]];canonical=root/'canonical.f32'
        canonical.write_bytes(checked_artifact(parent['canonical_vectors'],'.').read_bytes()+b''.join(struct.pack('<ffff',*v) for v in vectors))
        candidate=prepare_global_append(os.environ['SPANN_WRITE_PLANNER_BINARY'],os.environ['SPANN_DYNAMIC_BINARY'],parent,canonical,root/'private-oracle',timeout=30,max_rows=128,max_bytes=8*1024*1024)
        plan=json.loads((root/'private-oracle/write-plan.stdout').read_text());model=json.loads(checked_artifact(parent['tree']['model'],'.').read_text())
        aliases={s['subtree_id']:model['centroids'][s['centroid_index']] for s in parent['tree']['subtrees']}
        nearest=min(aliases,key=lambda sid:sum((a-b)**2 for a,b in zip(vectors[0],aliases[sid])))
        def f32(v):return struct.unpack('<f',struct.pack('<f',v))[0]
        def distance(a,b):
            result=0.
            for x,y in zip(a,b):
                d=f32(f32(x)-f32(y));result=f32(result+f32(d*d))
            return result
        ties={sid:distance(vectors[1],center) for sid,center in aliases.items()}
        upper_f32={sid:distance(vectors[0],center) for sid,center in aliases.items()};nearest_f32=min(upper_f32,key=lambda sid:(upper_f32[sid],sid));self.assertEqual(nearest,nearest_f32)
        receipt=dict(scope='private writer route geometry only; no materializer or collection publication',parent_bundle=view['registered_bundle'],parent_deployment_sha256=content_hash(parent),canonical=artifact(canonical),vectors=vectors,aliases=aliases,nearest_upper=nearest,nearest_upper_f32=nearest_f32,upper_f32_distances=upper_f32,routes=plan['routes'],saved=plan['saved'],tied_root_f32_distances=ties,cross_boundary=plan['routes'][0]['subtree_id']!=nearest,parent_inventory=before)
        (root/'receipt.json').write_bytes(canonical_bytes(receipt))
        for sid,files in before.items():self.assertEqual(inventory(parent['owners'][sid]['checkpoint_dir']),files)
        self.assertEqual(len(set(ties.values())),1);self.assertEqual(next(iter(ties.values())),16777230.)
        self.assertTrue(receipt['cross_boundary'],'retained geometry does not establish requested counterexample')
        self.assertFalse((root/'current.json').exists())
