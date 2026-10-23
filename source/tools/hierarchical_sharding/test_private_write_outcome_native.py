"""One native failure through in-process agent dispatch; no HTTP/authentication claim."""
import copy,json,os,secrets,struct,subprocess,time,unittest,uuid
from pathlib import Path
from unittest.mock import patch
from .tree_manifest import artifact,canonical_bytes,checked_artifact,file_hash
from .test_dynamic_remote_write_native import retained_current_capture
from .test_compactor_registration import CapturedAuthority
from .test_compactor_remote_topology import advance

class DirectAgentClient:
    """Call actual agent handlers directly; preserve the tracked request bytes."""
    def __init__(self,agent,log):
        self.agent=agent;self.run_id=agent.run_id;self.worker_id=agent.worker_id;self.instance_id=agent.identity['instanceId'];self.log=Path(log)
    def rpc(self,payload):
        raw=canonical_bytes(payload)
        if len(raw)>256*1024:raise ValueError('direct test request exceeds admission')
        try:
            result=self.agent.execute(json.loads(raw));encoded=canonical_bytes(result)
            if len(encoded)>256*1024:raise ValueError('direct test response exceeds admission')
            row=dict(request=payload,result=result);return result
        except BaseException as error:
            row=dict(request=payload,error=repr(error));raise
        finally:
            with self.log.open('ab') as stream:stream.write(canonical_bytes(row))
    def upload(self,replica,archive,receipt,subtree):
        path=Path(archive)
        if path.stat().st_size!=receipt['bytes'] or not 0<receipt['bytes']<=32*1024**2 or file_hash(path)!=receipt['sha256']:raise ValueError('direct test archive differs')
        ticket=self.rpc(dict(op='prepare-import',replica_id=replica,receipt=receipt,subtree_id=subtree['subtree_id'],generation=subtree['generation'],snapshot_sha256=subtree['snapshot']['sha256']))['ticket']
        with path.open('rb') as stream:return self.agent.receive(ticket,stream,receipt['bytes'])
    def download(self,export_id,receipt,target):
        path=self.agent.exports[export_id]
        if path.stat().st_size!=receipt['bytes'] or not 0<receipt['bytes']<=32*1024**2 or file_hash(path)!=receipt['sha256']:raise ValueError('direct test export differs')
        with path.open('rb') as source,Path(target).open('xb') as output:
            remaining=receipt['bytes']
            while remaining:
                chunk=source.read(min(65536,remaining))
                if not chunk:raise ValueError('truncated direct test archive')
                output.write(chunk);remaining-=len(chunk)
            if source.read(1):raise ValueError('oversized direct test archive')
        if file_hash(target)!=receipt['sha256']:raise ValueError('direct test download digest differs')
        return Path(target)

@unittest.skipUnless(os.environ.get('SPANN_PRIVATE_DISCARD_NATIVE'),'exact reviewed native failure fixture required')
class NativePrivateDiscard(unittest.TestCase):
    def test_native_failed_precommit_discard_has_exact_unknown_effect_receipt(self):
        from .dynamic_agent import Agent
        from .dynamic_runtime import PRODUCER_SOURCE_SHA256,inventory
        from .dynamic_remote_write import RemoteWriteExecution,RemoteWriteCoordinator
        from .compactor_namespace import current_view,prepare_current_namespace_export,ExportedNamespaceAuthority,SavedNamespaceProducer,NamespacePreparation
        from .compactor_registration import CompactorRegistration
        from .compactor_remote_topology import prepare_transferred_placement,submit_transferred_placement,reconcile_transferred_placement
        from .compactor_topology import _capture_health
        from .private_write_outcome import CLASSIFICATION
        root=Path(os.environ['SPANN_SELECTED_OUTPUT']);root.mkdir();reader=os.environ['SPANN_DYNAMIC_BINARY'];writer=os.environ['SPANN_SELECTED_WRITER'];auditor=Path(os.environ['SPANN_SELECTED_AUDITOR'])
        self.assertEqual(file_hash(writer),os.environ['SPANN_SELECTED_WRITER_SHA']);self.assertEqual(file_hash(auditor),os.environ['SPANN_SELECTED_AUDITOR_SHA'])
        context=retained_current_capture(os.environ['SPANN_SELECTED_PARENT']);view=current_view(context);parent=json.loads(checked_artifact(view['registered_bundle'],'.').read_bytes())['deployment']
        self.assertEqual(parent['tree']['source']['row_count'],18);self.assertEqual(parent['tree']['dimension'],4);self.assertTrue(all(not o.get('remote') for o in parent['owners'].values()))
        original={s:inventory(o['checkpoint_dir']) for s,o in parent['owners'].items()};old_ledger=Path(os.environ['SPANN_DISCARD_V15_LEDGER']);old_ledger_sha=file_hash(old_ledger)
        agents=[];clients={};generation=None;execution=None;audits=[];started=time.monotonic()
        def audit(target,namespace,directory):
            directory=Path(directory);directory.mkdir(parents=True,exist_ok=False);out=directory/'report.json';input_path=directory/'input.json';input_path.write_bytes(canonical_bytes(dict(format_version=1,context=target,namespace_root=namespace,record_store=os.environ['SPANN_SELECTED_RECORD_STORE'],output=str(out))))
            run=subprocess.run([str(auditor),'--ignored','--exact','audit_current_namespace_view','--nocapture'],env=dict(os.environ,SPANN_NAMESPACE_VIEW_INPUT=str(input_path)),capture_output=True,timeout=10);(directory/'stdout').write_bytes(run.stdout);(directory/'stderr').write_bytes(run.stderr);self.assertEqual(run.returncode,0,run.stderr.decode());report=json.loads(out.read_bytes());self.assertEqual(report['namespace'],namespace);self.assertEqual(report['collection_binding'],target['record_collection_binding']);audits.append(artifact(out));return artifact(out)
        try:
            for worker in ['worker-a','worker-b']:
                scope=root/worker;scope.mkdir();token=scope/'token';token.write_text(secrets.token_hex(32));token.chmod(0o600);identity=dict(instanceId='inprocess-'+worker,accountId='fixture',region='fixture',privateIp='127.0.0.1')
                config=dict(run_id='discard-'+worker,worker_id=worker,expected_identity=identity,binary=reader,binary_sha256=file_hash(reader),selected_writer_binary=writer,selected_writer_sha256=file_hash(writer),producer_sha256=PRODUCER_SOURCE_SHA256,token_file=str(token),scope_root=str(scope));agent=Agent(config,identity_reader=lambda identity=identity:identity);agents.append(agent);clients[worker]=DirectAgentClient(agent,root/(worker+'-rpc.jsonl'))
            delegate=CapturedAuthority(copy.deepcopy(context),[]);placement,generation,_=prepare_transferred_placement(delegate,context['parent']['collection']['collection_id'],clients,dict(zip(sorted(parent['owners']),clients)),root/'placement',reader,audit)
            self.assertEqual(submit_transferred_placement(delegate,placement,generation,audit)['status'],'pending-or-unknown');delegate.state=copy.deepcopy(placement['candidate_state']);self.assertEqual(reconcile_transferred_placement(delegate,placement)['status'],'committed')
            context=advance(context,placement);context['compaction'].update(job_id=str(uuid.uuid4()),session_id=str(uuid.uuid4()),pulled_log_offset=24,target_total_records=20,target_logical_size_bytes=320)
            delegate=CapturedAuthority(copy.deepcopy(context),[]);view=current_view(context);parent=json.loads(checked_artifact(view['registered_bundle'],'.').read_bytes())['deployment']
            rows=[dict(log_offset=23+i,user_id='discard-new-'+str(i),embedding_bits=list(struct.unpack('<IIII',struct.pack('<ffff',*v)))) for i,v in enumerate([[7.25,1.,-0.,0.],[8.,4097.,-0.,0.]])]
            source_input=prepare_current_namespace_export(delegate,context['parent']['collection']['collection_id'],view['registered_bundle'],rows,os.environ['SPANN_SELECTED_RECORD_STORE'],root/'record-input')
            run=subprocess.run([str(auditor),'--ignored','--exact','export_current_namespace_append','--nocapture'],env=dict(os.environ,SPANN_NAMESPACE_CURRENT_INPUT=source_input['path'],SPANN_NAMESPACE_CURRENT_OUTPUT=str(root/'record-export')),capture_output=True,timeout=30);(root/'export.stdout').write_bytes(run.stdout);(root/'export.stderr').write_bytes(run.stderr);self.assertEqual(run.returncode,0,run.stderr.decode())
            export=artifact(root/'record-export/export.json');proof=artifact(root/'record-export/export.receipt.json');runtime=json.loads(checked_artifact(proof,'.').read_bytes())['runtime_binary'];self.assertEqual(runtime['sha256'],file_hash(auditor))
            source=ExportedNamespaceAuthority(delegate,export,proof,runtime);authority=NamespacePreparation(source,SavedNamespaceProducer(source,root/'unused-namespace'))
            execution=RemoteWriteExecution(parent,context,export,clients,lambda:_capture_health(delegate,context));session=CompactorRegistration(authority,context['parent']['collection']['collection_id'],writer,reader,root/'failed-private',vector_execution=execution)
            original_run=RemoteWriteCoordinator.run
            def corrupt_before_intent(coordinator):
                call=coordinator.call
                def invoke(sid,handle,command,row):
                    if command['op']=='register':command=dict(command,leaf=4294967295)
                    return call(sid,handle,command,row)
                coordinator.call=invoke;return original_run(coordinator)
            with patch.object(RemoteWriteCoordinator,'run',corrupt_before_intent):
                with self.assertRaisesRegex(ValueError,'selected reached leaf/ID/grant differs'):session.prepare(str(uuid.uuid4()),session.task_spec())
            self.assertEqual(delegate.starts,0);self.assertIsNone(session.reply);self.assertIsNone(session.attempt);self.assertIsNone(execution.generation);self.assertFalse((root/'failed-private/hierarchy-bundle.json').exists())
            self.assertEqual(execution.ledger.state['preparation_state'],'failed-before-candidate');self.assertTrue(execution.poisoned)
            uncertain=[r for r in execution.ledger.state['operations'] if r['status']=='unreconciled'];self.assertEqual(len(uncertain),1);self.assertEqual(uncertain[0]['kind'],'selected-write-command')
            with self.assertRaisesRegex(ValueError,'unreconciled'):execution.close()
            before_reconcile=copy.deepcopy(execution.ledger.state);classification=execution.reconcile_discarded_private();self.assertEqual(classification,dict(classification=CLASSIFICATION,operations=1));execution.close()
            resolved=[r for r in execution.ledger.state['operations'] if r['status']==CLASSIFICATION];self.assertEqual(len(resolved),1);outcome=resolved[0]['discard_receipt'];self.assertIs(outcome['commit_started'],False);self.assertEqual(outcome['returncode'],1)
            self.assertEqual(delegate.starts,0);self.assertIsNone(session.reply);self.assertTrue(execution.poisoned)
            for agent in agents:
                for row in agent.write_sessions.sessions.values():
                    state=json.loads(row['command_journal'].read_bytes());self.assertIs(state['commit_started'],False);self.assertNotIn(row['replica_id'],agent.replicas);self.assertTrue(row['channel'].closed);self.assertIsInstance(row['channel'].process.returncode,int)
            for sid,files in original.items():self.assertEqual(inventory(placement['transport_parent']['owners'][sid]['checkpoint_dir']),files)
            self.assertEqual(file_hash(old_ledger),old_ledger_sha);self.assertFalse((root/'current.json').exists());generation.verify()
            (root/'receipt.json').write_bytes(canonical_bytes(dict(scope='actual native private failure through in-process agent dispatch; no HTTP/prod authority',classification=classification,before_reconcile=before_reconcile,after_reconcile=execution.ledger.state,audits=audits,materialized_export=export,materialized_proof=proof,old_v15_ledger_sha256=old_ledger_sha,parent_inventory=original,append_registration_calls=delegate.starts,no_second_pointer=True,wall_seconds=time.monotonic()-started)))
        finally:
            cleanup=[]
            if execution is not None:
                try:execution.close()
                except Exception as error:cleanup.append(repr(error))
            if generation is not None:
                try:generation.close()
                except Exception as error:cleanup.append(repr(error))
            for agent in agents:agent.close()
            children=[dict(returncode=row['channel'].process.returncode,threads_stopped=row['channel'].closed) for agent in agents for row in agent.write_sessions.sessions.values() if row['channel'] is not None]
            (root/'cleanup.json').write_bytes(canonical_bytes(dict(errors=cleanup,children=children,generations=[len(a.generations) for a in agents])))
            self.assertFalse(cleanup);self.assertTrue(all(type(c['returncode']) is int and c['threads_stopped'] for c in children))
