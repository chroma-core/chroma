"""Two loopback agents exercise the existing bounded transport, not physical hosts."""
import copy
import json
import os
from pathlib import Path
import secrets
import subprocess
import struct
import sys
import tempfile
import threading
import unittest
import uuid
from unittest.mock import patch
from .compactor_remote_topology import TransportLedger,TrackedClient,TrackedGeneration,RemoteTopologyExecution,prepare_transferred_placement,submit_transferred_placement,reconcile_transferred_placement
from .compactor_namespace import current_view
from .compactor_topology import prepare_namespace_topology,submit_namespace_topology,reconcile_namespace_topology,_capture_health
from .dynamic_topology import split_request,merge_request,reassign_request,owned_ids
from .tree_manifest import artifact,canonical_bytes,checked_artifact,file_hash
from .test_compactor_registration import CapturedAuthority


class TransportIntentContract(unittest.TestCase):
    def test_generation_handle_is_owned_before_rpc_and_unknown_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            parent={'fence':1};ledger=TransportLedger(Path(directory)/'transport',parent)
            class Client:
                worker_id='a';instance_id='i';run_id='run';calls=0
                def rpc(self,payload):
                    self.calls+=1;self.payload=payload;self.asserted=json.loads(ledger.path.read_text())['operations'][-1]['status']=='started';raise TimeoutError('reply unknown')
            client=Client();tracked=TrackedClient(client,ledger)
            with self.assertRaises(TimeoutError):tracked.rpc(dict(op='prepare-generation',generation_id='owned-gen',subtrees=[]))
            self.assertTrue(client.asserted);self.assertEqual(client.calls,1);self.assertEqual(ledger.state['operations'][0]['generation_id'],'owned-gen');self.assertEqual(ledger.state['operations'][0]['status'],'unreconciled')


    def test_remote_readiness_rejects_reentry_before_rpc(self):
        execution=RemoteTopologyExecution.__new__(RemoteTopologyExecution);execution.readiness_started=True
        with self.assertRaisesRegex(ValueError,'single-use'):execution.ready({})


def retained_context(prepared_path):
    prepared=json.loads(Path(prepared_path).read_text());context=copy.deepcopy(prepared['context']);context['parent']=copy.deepcopy(prepared['candidate_state']);bundle=json.loads(checked_artifact(prepared['descriptor'],'.').read_text());ns=json.loads(checked_artifact(bundle['hierarchy_metadata']['namespace_root'],'.').read_text());context['record_collection_binding']=copy.deepcopy(ns['collection']);c=context['parent']['collection'];context['compaction']=dict(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=c['log_position'],target_total_records=c['total_records_post_compaction'],target_logical_size_bytes=c['size_bytes_post_compaction']);current_view(context);return context


def advance(context,prepared):
    result=copy.deepcopy(context);result['parent']=copy.deepcopy(prepared['candidate_state']);result['record_collection_binding']['wire']['version']+=1;result['compaction']['job_id']=str(uuid.uuid4());result['compaction']['session_id']=str(uuid.uuid4());current_view(result);return result


@unittest.skipUnless(os.environ.get('SPANN_REMOTE_TOPOLOGY_AUDITOR'),'reviewed native fixture required')
class NativeRemoteTopology(unittest.TestCase):
    def test_two_agents_transfer_merge_reassign_one_collection(self):
        from .dynamic_agent import Agent,make_server
        from .dynamic_remote import AgentClient
        from .dynamic_runtime import PRODUCER_SOURCE_SHA256,inventory
        root=Path(os.environ['SPANN_REMOTE_TOPOLOGY_OUTPUT']).resolve();root.mkdir();context=retained_context(os.environ['SPANN_REMOTE_TOPOLOGY_PARENT']);self.assertEqual(context['parent']['collection']['total_records_post_compaction'],18)
        auditor_binary=Path(os.environ['SPANN_REMOTE_TOPOLOGY_AUDITOR']);self.assertEqual(file_hash(auditor_binary),os.environ['SPANN_REMOTE_TOPOLOGY_AUDITOR_SHA']);reader=os.environ['SPANN_DYNAMIC_BINARY'];store=os.environ['SPANN_REMOTE_TOPOLOGY_RECORD_STORE'];record_before=inventory(store);agents=[];servers=[];threads=[];clients={};generations=[];executions=[];audits=[];cleanup=[];steps=[];children={}
        def auditor(target,namespace,directory):
            directory=Path(directory);directory.mkdir();input_path=directory/'input.json';output=directory/'result.json';input_path.write_bytes(canonical_bytes(dict(capture=target,namespace=namespace,store=store,output=str(output))))
            run=subprocess.run([str(auditor_binary),'--ignored','--exact','audit_current_namespace_view','--nocapture'],env=dict(os.environ,SPANN_NAMESPACE_VIEW_INPUT=str(input_path)),capture_output=True,timeout=10);(directory/'stdout').write_bytes(run.stdout);(directory/'stderr').write_bytes(run.stderr);self.assertEqual(run.returncode,0,run.stderr.decode());result=json.loads(output.read_text());self.assertEqual(result['namespace'],namespace);ns=json.loads(checked_artifact(namespace,'.').read_text());self.assertEqual(result['rows'],json.loads(checked_artifact(ns['mapping'],'.').read_text()));self.assertEqual(result['record_files'],ns['record_files']);self.assertEqual(result['collection_binding'],target['record_collection_binding']);vector=next(s for s in target['parent']['segments'] if s['segment_id']==target['vector_segment_id']);self.assertEqual(result['current_bundle'],json.loads(vector['file_paths']['hierarchical_spann_bundle_v1'][0]));audits.append(artifact(output));return artifact(output)
        def remember_children():
            for agent in agents:
                for generation in agent.generations.values():
                    if generation['host'] is not None:
                        for reader in generation['host'].readers.values():children[reader.reader.process.pid]=reader.reader.process
        def commit(authority,prepared,submit,reconcile):
            self.assertEqual(submit()['status'],'pending-or-unknown');self.assertEqual(submit()['status'],'pending-or-unknown');self.assertEqual(authority.starts,1);authority.state=copy.deepcopy(prepared['candidate_state']);self.assertEqual(reconcile()['status'],'committed')
        try:
            for worker in ('worker-a','worker-b'):
                scope=root/worker;scope.mkdir(mode=0o700);token=scope/'token';token.write_text(secrets.token_hex(32));token.chmod(0o600);identity=dict(instanceId='injected-loopback-'+worker,accountId='fixture',region='fixture',privateIp='127.0.0.1');config=dict(run_id='fixture-'+worker,worker_id=worker,expected_identity=identity,binary=reader,binary_sha256=file_hash(reader),producer_sha256=PRODUCER_SOURCE_SHA256,token_file=str(token),scope_root=str(scope));agent=Agent(config,identity_reader=lambda identity=identity:identity);agents.append(agent);server=make_server(agent);servers.append(server);thread=threading.Thread(target=server.serve_forever);thread.start();threads.append(thread);clients[worker]=AgentClient('http://127.0.0.1:'+str(server.server_address[1]),token.read_text(),config['run_id'],timeout=10,worker_id=worker,instance_id=identity['instanceId'])
            stale=CapturedAuthority(copy.deepcopy(context),[]);stale.state['collection']['version']+=1
            with self.assertRaisesRegex(ValueError,'stale'):prepare_transferred_placement(stale,context['parent']['collection']['collection_id'],clients,{},root/'stale-transfer',reader,auditor)
            self.assertFalse((root/'stale-transfer').exists());self.assertTrue(all(not a.reservations for a in agents))
            authority=CapturedAuthority(copy.deepcopy(context),[]);view=current_view(context);before=json.loads(checked_artifact(view['registered_bundle'],'.').read_text());assignments={sid:worker for sid,worker in zip(sorted(before['deployment']['owners']),('worker-a','worker-b'))};placement,generation,ledger=prepare_transferred_placement(authority,context['parent']['collection']['collection_id'],clients,assignments,root/'placement',reader,auditor);generations.append(generation);remember_children()
            placed=json.loads(checked_artifact(placement['descriptor'],'.').read_text());self.assertEqual(placed['hierarchy_metadata'],before['hierarchy_metadata']);self.assertEqual(placed['deployment']['tree'],before['deployment']['tree']);commit(authority,placement,lambda:submit_transferred_placement(authority,placement,generation,auditor),lambda:reconcile_transferred_placement(authority,placement));context=advance(context,placement);steps.append(dict(operation='verified-transfer-placement',prepared=artifact(root/'placement/prepared.json')))
            identity={k:current_view(context)['namespace'][k] for k in ('mapping','identity_source','canonical_vectors','record_files')}
            for operation in ('split','merge','reassign'):
                view=current_view(context);parent=json.loads(checked_artifact(view['registered_bundle'],'.').read_text())['deployment'];authority=CapturedAuthority(copy.deepcopy(context),[])
                if operation=='split':
                    sid=next(s for s,o in parent['owners'].items() if o['worker_id']=='worker-a');slot=next(s for s in parent['tree']['subtrees'] if s['subtree_id']==sid);ids=list(owned_ids(slot));replacement=split_request(parent,sid,ids[:len(ids)//2],'left','right');destinations={'left':'worker-a','right':'worker-a'}
                elif operation=='merge':
                    other=next(s for s,o in parent['owners'].items() if o['worker_id']=='worker-b');replacement=merge_request(parent,['left',other],'cross-merged');destinations={'cross-merged':'worker-b'}
                    # Corrupt a downloaded archive before admission to import.
                    failed=root/'corrupt-transfer';execution=RemoteTopologyExecution(parent,clients,destinations,failed/'transport',lambda:_capture_health(authority,context));executions.append(execution);reservations=[copy.deepcopy(a.reservations) for a in agents];download=clients['worker-a'].download
                    def corrupt(*args):
                        path=download(*args);payload=path.read_bytes();path.write_bytes(bytes([payload[0]^1])+payload[1:]);return path
                    with patch.object(clients['worker-a'],'download',side_effect=corrupt):
                        with self.assertRaisesRegex(ValueError,'wire digest'):prepare_namespace_topology(authority,context['parent']['collection']['collection_id'],replacement,failed,reader,auditor,timeout=10,execution=execution)
                    execution.close();self.assertFalse((failed/'prepared.json').exists());self.assertEqual(authority.starts,0);self.assertEqual([a.reservations for a in agents],reservations);generation.verify()
                else:
                    ids=list(owned_ids(next(s for s in parent['tree']['subtrees'] if s['subtree_id']=='cross-merged')));replacement=reassign_request(parent,'cross-merged','right',[ids[0]]);destinations={'cross-merged':'worker-b','right':'worker-a'}
                output=root/operation;execution=RemoteTopologyExecution(parent,clients,destinations,output/'transport',lambda authority=authority,context=context:_capture_health(authority,context));executions.append(execution);prepared=prepare_namespace_topology(authority,context['parent']['collection']['collection_id'],replacement,output,reader,auditor,timeout=10,execution=execution);remember_children();bundle=json.loads(checked_artifact(prepared['descriptor'],'.').read_text());ns=json.loads(checked_artifact(bundle['hierarchy_metadata']['namespace_root'],'.').read_text())
                for key,value in identity.items():self.assertEqual(ns[key],value)
                if operation=='reassign':
                    stale=CapturedAuthority(copy.deepcopy(context),[]);stale.state['collection']['version']+=1
                    with self.assertRaisesRegex(ValueError,'stale'):submit_namespace_topology(stale,prepared,auditor,execution=execution)
                    self.assertEqual(stale.starts,0);execution.close();output=root/'reassign-fresh';execution=RemoteTopologyExecution(parent,clients,destinations,output/'transport',lambda authority=authority,context=context:_capture_health(authority,context));executions.append(execution);prepared=prepare_namespace_topology(authority,context['parent']['collection']['collection_id'],replacement,output,reader,auditor,timeout=10,execution=execution);remember_children();bundle=json.loads(checked_artifact(prepared['descriptor'],'.').read_text());ns=json.loads(checked_artifact(bundle['hierarchy_metadata']['namespace_root'],'.').read_text())
                    for key,value in identity.items():self.assertEqual(ns[key],value)
                commit(authority,prepared,lambda:submit_namespace_topology(authority,prepared,auditor,execution=execution),lambda:reconcile_namespace_topology(authority,prepared));steps.append(dict(operation=operation,prepared=artifact(output/'prepared.json')));context=advance(context,prepared)
                # Read every actual agent copy through its prepared generation.
                results=[];raw=[]
                for subtree in bundle['deployment']['tree']['subtrees']:
                    sid=subtree['subtree_id'];owner=bundle['deployment']['owners'][sid];remote=owner['remote'];worker=remote['worker_id'];request_id=uuid.uuid4().hex;payload=dict(tree_sha256=bundle['deployment']['tree_sha256'],request_id=request_id,timeout_s=10,requests=[dict(subtree_id=sid,snapshot_sha256=subtree['snapshot']['sha256'],payload=dict(op='rerank_selected',query=[0.,0.,0.,0.],local_ids=list(range(subtree['vector_count']))))]);response=clients[worker].rpc(dict(op='probe-generation',generation_id=execution.generation.generations[worker],payload=payload));self.assertEqual(response['request_id'],request_id);raw.append(dict(worker=worker,generation_id=execution.generation.generations[worker],request=payload,response=response));results.extend(response['responses'][0]['result']['results'])
                self.assertEqual(sorted(r['id'] for r in results),list(range(18)));canonical=checked_artifact(ns['canonical_vectors'],'.').read_bytes();self.assertEqual(bundle['deployment']['tree']['metric'],'l2')
                for hit in results:
                    values=struct.unpack_from('<ffff',canonical,hit['id']*16);self.assertAlmostEqual(hit['distance'],sum(v*v for v in values),places=4)
                (output/'remote-rerank.json').write_bytes(canonical_bytes(dict(results=results,raw=raw)))
            self.assertEqual(inventory(store),record_before);self.assertFalse((root/'current.json').exists());(root/'receipt.json').write_bytes(canonical_bytes(dict(steps=steps,audits=audits,identity=identity,record_inventory=record_before,no_second_pointer=True,scope='two loopback agents, injected identities and fake collection authority')))
        finally:
            active=sys.exc_info()[1]
            for execution in reversed(executions):
                try:execution.close()
                except BaseException as error:cleanup.append(repr(error))
            for generation in reversed(generations):
                try:generation.close()
                except BaseException as error:cleanup.append(repr(error))
            for server in servers:server.shutdown();server.server_close()
            for thread in threads:thread.join(2)
            for agent in agents:agent.close()
            report=dict(status='unreconciled' if cleanup or any(t.is_alive() for t in threads) or any(a.generations for a in agents) or any(p.poll() is None for p in children.values()) else 'reconciled',errors=cleanup,server_threads_alive=[t.is_alive() for t in threads],agent_generations=[len(a.generations) for a in agents],owned_reader_exit_codes={str(pid):process.poll() for pid,process in children.items()});(root/'cleanup.json').write_bytes(canonical_bytes(report))
            if report['status']!='reconciled':
                if active is not None:active.add_note('owned cleanup unreconciled: '+repr(report))
                else:self.fail('owned cleanup unreconciled: '+repr(report))
