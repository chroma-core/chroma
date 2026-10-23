"""A bounded, single-owner private writer channel used by the local agent.

The caller supplies a previously checked specification and binary. This module
owns command sequencing and process lifetime; it does not authorize routing or
publish a collection. An uncertain response permanently poisons the channel.
"""
import json
import hashlib
import os
from pathlib import Path
import queue
import subprocess
import threading
from .tree_manifest import canonical_bytes


def sealed_spec_bytes(path, expected_sha256):
    """Return the same bounded bytes whose digest identifies the executed spec."""
    with Path(path).open('rb') as stream:raw=stream.read(64*1024+1)
    if len(raw)>64*1024 or hashlib.sha256(raw).hexdigest()!=expected_sha256:
        raise ValueError('executed specification changed before sealing')
    return raw


class PrivateWriteChannel:
    FRAME_BYTES = 64 * 1024
    REPLY_BYTES = 256 * 1024
    MAX_COMMANDS = 256

    def __init__(self, binary, spec_path, spec, stderr_path, timeout=5):
        self.spec = spec
        self.timeout = timeout
        self.sequence = 0
        self.poisoned = False
        self.terminal = False
        self.closed = False
        self.closing = False
        self.cleanup_failure = None
        self.responses = queue.Queue(maxsize=2)
        import time
        self.started_unix = time.time()
        self.argv = [str(binary), 'selected-write-session', '--spec', str(spec_path)]
        self.protocol_bytes = 0
        self.protocol_path = Path(str(stderr_path)+'.protocol.jsonl')
        self.stderr = Path(stderr_path).open('xb')
        try:
            self.process = subprocess.Popen(self.argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, bufsize=0, env=dict(os.environ,TOKIO_WORKER_THREADS='2'))
        except BaseException:
            self.stderr.close()
            raise
        try:
            self.thread = threading.Thread(target=self._receive, daemon=True)
            self.thread.start()
        except BaseException:
            self.close()
            raise

    def _receive(self):
        try:
            while True:
                raw = self.process.stdout.readline(self.REPLY_BYTES + 1)
                if not raw:
                    self.responses.put(EOFError('private writer closed before a response'), timeout=self.timeout)
                    return
                if len(raw) > self.REPLY_BYTES or not raw.endswith(b'\n'):
                    self.responses.put(ValueError('private writer response exceeds admission'), timeout=self.timeout)
                    return
                self.responses.put(raw, timeout=self.timeout)
        except BaseException as error:
            try:self.responses.put_nowait(error)
            except queue.Full:pass

    def call(self, command, row_id=None):
        if self.closed or self.poisoned or self.terminal:
            raise ValueError('private writer is closed, poisoned, or terminal')
        self.sequence += 1
        if self.sequence > self.MAX_COMMANDS:
            self.poisoned = True
            raise ValueError('private writer command admission exceeded')
        spec = self.spec
        row_tag = 'all' if row_id is None else str(row_id)
        request = dict(request_id=f"{spec['session_id']}:{spec['attempt_id']}:{row_tag}:{self.sequence}", session_id=spec['session_id'], attempt_id=spec['attempt_id'], authorization=spec['authorization'], ownership_generation=spec['ownership_generation'], sequence=self.sequence, row_id=row_id, command=command)
        import time
        deadline = time.monotonic() + self.timeout
        try:
            raw = canonical_bytes(request)
            if len(raw) > self.FRAME_BYTES:
                raise ValueError('private writer request exceeds admission')
            self._retain('request', raw)
            self._send(raw, deadline)
            response = self.responses.get(timeout=max(0, deadline-time.monotonic()))
            if isinstance(response, BaseException):
                raise response
            self._retain('response', response)
            response = json.loads(response)
            owner = spec['base']['owners'][0]
            expected = dict(request_id=request['request_id'], sequence=self.sequence, row_id=row_id, session_id=spec['session_id'], attempt_id=spec['attempt_id'], parent_sha256=spec['base']['parent_sha256'], ownership_generation=spec['ownership_generation'], source_snapshot_sha256=owner['manifest_sha256'], source_map_sha256=spec['source_map_sha256'])
            if set(response) != set(expected) | {'result'} or any(response[key] != value or type(response[key]) is not type(value) for key, value in expected.items()):
                raise ValueError('private writer response scope differs')
            result = response['result']
            if not isinstance(result, dict) or result.get('op') == 'failed':
                raise ValueError('private writer rejected command: ' + str(result))
            expected_op = {'register': 'registered', 'commit': 'saved', 'abort': 'aborted'}.get(command['op'], command['op'])
            if result.get('op') != expected_op:
                raise ValueError('private writer response operation differs')
            self.terminal = expected_op in ('saved', 'aborted')
            return result
        except BaseException:
            self.poisoned = True
            raise

    def _send(self, raw, deadline):
        import time
        completed = queue.Queue(maxsize=1)
        def write():
            try:
                remaining = memoryview(raw)
                while remaining:
                    written = self.process.stdin.write(remaining)
                    if not written:raise EOFError('private writer input closed')
                    remaining = remaining[written:]
                self.process.stdin.flush()
                completed.put(None)
            except BaseException as error:completed.put(error)
        self.writer_thread = threading.Thread(target=write, daemon=True)
        self.writer_thread.start()
        result = completed.get(timeout=max(0, deadline-time.monotonic()))
        if result is not None:raise result

    def _retain(self, kind, raw):
        import base64
        if not hasattr(self, 'protocol_path'):
            return  # Pure in-memory contract tests do not own a child journal.
        encoded = canonical_bytes(dict(kind=kind, raw_b64=base64.b64encode(raw).decode()))
        if self.protocol_bytes + len(encoded) > 8*1024*1024:
            raise ValueError('private writer protocol journal exceeds admission')
        with self.protocol_path.open('ab') as stream:
            stream.write(encoded);stream.flush()
        self.protocol_bytes += len(encoded)

    def close(self):
        if self.closed:return
        if getattr(self, 'cleanup_failure', None) is not None:
            raise ValueError('prior private writer cleanup remains unreconciled: '+self.cleanup_failure)
        if getattr(self, 'closing', False):
            raise ValueError('private writer cleanup is already in flight')
        self.closing = True
        try:
            try:self.process.stdin.close()
            except (BrokenPipeError, OSError):pass
            try:self.process.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=self.timeout)
            if type(self.process.returncode) is not int:
                raise ValueError('private writer terminal exit is not reconciled')
            self.process.stdout.close();self.stderr.close()
            for name in ('thread','writer_thread'):
                thread=getattr(self,name,None)
                if thread is not None and thread.ident is not None:
                    thread.join(self.timeout)
                    if thread.is_alive():raise ValueError('private writer I/O thread requires reconciliation')
            self.closed = True
        except BaseException as error:
            self.poisoned = True
            self.cleanup_failure = repr(error)
            raise ValueError('private writer cleanup is unreconciled: '+repr(error)) from error
        finally:self.closing = False


class AgentWriteSessions:
    """Own private writer children inside an existing authenticated agent.

    The job context is a trusted coordinator capture, not a new authority API.
    Every command still carries the captured job/session/attempt binding.
    """
    def __init__(self, agent):
        self.agent = agent
        self.sessions = {}

    def prepare(self, payload):
        import base64
        import copy
        import shutil
        import uuid
        from .dynamic_agent import name
        from .dynamic_content import VERIFIED_WRITERS
        from .dynamic_runtime import inventory, copy_checkpoint
        from .tree_manifest import content_hash, file_hash, _atomic_write
        a = self.agent
        binary = Path(a.config.get('selected_writer_binary', ''))
        expected = a.config.get('selected_writer_sha256')
        if expected not in VERIFIED_WRITERS or file_hash(binary) != expected:
            raise ValueError('an independently admitted selected writer is required')
        handle = name(payload['handle'])
        replica = name(payload['replica_id'])
        with a.lock:
            if handle in self.sessions or len(self.sessions) >= 8:
                raise ValueError('fresh bounded private write session required')
            self.sessions[handle] = dict(state='reserved', channel=None, replica_id=replica, job_id=payload['job_id'], lock=threading.Lock(), failed=threading.Event())
        row = self.sessions[handle]
        row['lock'].acquire()
        try:
            a.reserve_replica(replica, 'selected-write')
            source = a.metadata(payload['source_replica_id'])
            if source != payload['source_metadata']:
                raise ValueError('selected writer source capture differs')
            spec = copy.deepcopy(payload['spec'])
            context = payload['captured_context']
            job = context['compaction']
            from .compactor_registration import HIERARCHY_BUNDLE
            vector_segments = [v for v in context['parent']['segments'] if v['segment_id'] == context['vector_segment_id']]
            if len(vector_segments) != 1:
                raise ValueError('exact current vector segment required')
            refs = vector_segments[0]['file_paths'].get(HIERARCHY_BUNDLE)
            if not isinstance(refs, list) or len(refs) != 1:
                raise ValueError('one captured registered bundle required')
            registered_reference = json.loads(refs[0])
            uuid.UUID(job['job_id']); uuid.UUID(job['session_id'])
            if (payload['job_id'] != job['job_id'] or spec['session_id'] != job['session_id']
                    or spec['attempt_id'] != handle or spec['source_replica_id'] != payload['source_replica_id']
                    or spec['worker_id'] != a.worker_id or spec['instance_id'] != a.identity['instanceId']
                    or spec['agent_run_id'] != a.run_id or spec['collection_version'] != context['parent']['collection']['version']):
                raise ValueError('captured job, physical source, or parent differs')
            if len(spec['base']['owners']) != 1 or spec['base']['owners'][0]['subtree_id'] != source['subtree']['subtree_id']:
                raise ValueError('one captured logical owner required')
            inputs = a.root/'inputs'/replica
            inputs.mkdir()
            keys = {'parent_deployment_file', 'registered_bundle_file', 'materialized_export_file', 'model_file', 'canonical_vectors', 'append_vectors'}
            if set(payload['artifacts']) != keys:
                raise ValueError('exact private input artifact set required')
            total = 0
            for key in sorted(keys):
                raw = base64.b64decode(payload['artifacts'][key], validate=True)
                total += len(raw)
                if len(raw) > 2*1024*1024 or total > 4*1024*1024:
                    raise ValueError('private writer input bytes exceed admission')
                path = inputs/key
                _atomic_write(path, raw)
                if key in ('canonical_vectors', 'append_vectors'):
                    spec['base'][key] = str(path)
                else:
                    spec[key] = str(path)
            parent = json.loads((inputs/'parent_deployment_file').read_bytes())
            bundle = json.loads((inputs/'registered_bundle_file').read_bytes())
            export = json.loads((inputs/'materialized_export_file').read_bytes())
            if (content_hash(parent) != spec['base']['parent_sha256'] or bundle['deployment'] != parent
                    or export['registered_bundle'] != registered_reference or spec['registered_bundle_sha256'] != registered_reference['sha256']
                    or export['captured']['collection_version'] != context['parent']['collection']['version']
                    or export['captured']['pulled_log_offset'] != job['pulled_log_offset']
                    or export['captured']['parent_log_position'] != context['parent']['collection']['log_position']
                    or export['captured']['rows'] != spec['rows']):
                raise ValueError('actual materializer export is not captured current input')
            remote = parent['owners'][source['subtree']['subtree_id']]['remote']
            if any(remote[key] != value for key, value in dict(worker_id=a.worker_id, instance_id=a.identity['instanceId'], agent_run_id=a.run_id, replica_id=payload['source_replica_id']).items()):
                raise ValueError('parent physical source tuple differs')
            directory = a.root/'replicas'/replica
            copy_checkpoint(source['owner']['checkpoint_dir'], directory, 32*1024*1024)
            clone_inventory = inventory(directory)
            if clone_inventory != source['inventory'] or content_hash(clone_inventory) != source['owner']['inventory_sha256']:
                raise ValueError('private clone full inventory differs before launch')
            # Persist the exact pre-marker copy proof; Rust checks its own roots/map.
            proof = dict(job_id=job['job_id'], session_id=job['session_id'], attempt_id=handle, source_replica_id=payload['source_replica_id'], source_inventory_sha256=source['owner']['inventory_sha256'], clone_inventory=clone_inventory)
            owner = spec['base']['owners'][0]
            owner['checkpoint_dir'] = str(directory)
            _atomic_write(directory/'global-write-private.json',canonical_bytes(dict(parent_sha256=spec['base']['parent_sha256'], manifest_sha256=owner['manifest_sha256'], subtree_id=owner['subtree_id'])))
            path = inputs/'selected-write-spec.json'
            spec_bytes=canonical_bytes(spec);spec_sha=hashlib.sha256(spec_bytes).hexdigest()
            proof['spec_sha256']=spec_sha
            _atomic_write(inputs/'clone-proof.json',canonical_bytes(proof))
            _atomic_write(path,spec_bytes)
            row.update(state='launching', spec=spec, spec_sha256=spec_sha, job_id=job['job_id'], inputs=inputs, source_metadata=source)
            if row['failed'].is_set():raise ValueError('concurrent request poisoned private preparation')
            row['command_scope']=dict(job_id=job['job_id'],session_id=job['session_id'],handle=handle,replica_id=replica,spec_sha256=spec_sha)
            row['command_journal']=inputs/'command-intents.json'
            row['commands']=[];row['commit_started']=False;row['intent_persistence_failed']=False
            self._persist_command_intents(row)
            row['channel'] = PrivateWriteChannel(binary, path, spec, inputs/'selected-write.stderr')
            info = row['channel'].call(dict(op='info'))
            if info.get('spec_sha256')!=spec_sha:raise ValueError('writer consumed a different specification')
            if row['failed'].is_set():raise ValueError('concurrent request poisoned private preparation')
            row['state'] = 'active'
            a.record('selected-write-prepared', handle=handle, replica_id=replica, job_id=job['job_id'], clone_inventory_sha256=content_hash(clone_inventory))
            return dict(handle=handle, replica_id=replica, info=info, clone_proof=proof)
        except BaseException:
            row['failed'].set()
            row['state'] = 'poisoned'
            if row['channel'] is not None:
                row['channel'].close()
            raise
        finally:row['lock'].release()

    @staticmethod
    def _outcome_bytes(path):
        with Path(path).open('rb') as stream:raw=stream.read(128*1024+1)
        if len(raw)>128*1024:raise ValueError('private outcome evidence exceeds admission')
        return raw

    def _persist_command_intents(self,row):
        from .tree_manifest import _atomic_write
        try:
            _atomic_write(row['command_journal'],canonical_bytes(dict(scope=row['command_scope'],commands=row['commands'],commit_started=row['commit_started'])))
        except BaseException:
            row['intent_persistence_failed']=True
            raise

    def _retain_discard(self,row):
        from .tree_manifest import _atomic_write,content_hash
        from .private_write_outcome import CLASSIFICATION
        # Absence of a durable intent is uncertainty, never evidence of no write.
        if (not row['failed'].is_set() or row.get('commit_started',True)
                or row.get('intent_persistence_failed',True)):
            return
        journal=self._outcome_bytes(row['command_journal'])
        if len(journal)>128*1024:raise ValueError('private command journal exceeds admission')
        state=json.loads(journal)
        if canonical_bytes(state)!=canonical_bytes(dict(scope=row['command_scope'],commands=row['commands'],commit_started=False)):
            raise ValueError('persisted private command intent differs')
        a=self.agent;replica=row['replica_id'];channel=row['channel']
        with a.lock:
            reservation=a.reservations.get(replica)
            if (reservation!=dict(operation='selected-write',state='failed') or replica in a.replicas
                    or any(replica in g.get('replica_ids',set()) for g in a.generations.values())):
                return
            if not channel.closed or type(channel.process.returncode) is not int:
                return
            receipt=dict(classification=CLASSIFICATION,scope=state['scope'],commands=state['commands'],commit_started=False,
                         failed=True,threads_stopped=True,returncode=channel.process.returncode,quarantined=True,
                         eligible_replica=False,eligible_generation=False,command_journal_sha256=content_hash(state))
            path=row['inputs']/'discarded-private-outcome.json'
            _atomic_write(path,canonical_bytes(receipt));row['discard_receipt']=path

    def outcome(self,payload):
        row=self.sessions[payload['handle']]
        if payload['job_id']!=row['job_id']:raise ValueError('exact private outcome job required')
        if not row['lock'].acquire(timeout=5):raise ValueError('private outcome still in flight')
        try:
            if row['state']!='closed' or 'discard_receipt' not in row:
                raise ValueError('private operation outcome remains unknown')
            raw=self._outcome_bytes(row['discard_receipt'])
            if len(raw)>128*1024:raise ValueError('private outcome exceeds admission')
            receipt=json.loads(raw)
            from .private_write_outcome import validate_terminal_fields
            validate_terminal_fields(receipt)
            # A closed failed session cannot admit new commands or reuse its reservation.
            if (not row['failed'].is_set() or row['commit_started'] or row['intent_persistence_failed']
                    or not row['channel'].closed or type(row['channel'].process.returncode) is not int):
                raise ValueError('private outcome lacks terminal evidence')
            with self.agent.lock:
                replica=row['replica_id']
                if (self.agent.reservations.get(replica)!=dict(operation='selected-write',state='failed')
                        or replica in self.agent.replicas or any(replica in g.get('replica_ids',set()) for g in self.agent.generations.values())):
                    raise ValueError('private namespace is not quarantined')
            from .tree_manifest import content_hash
            journal=self._outcome_bytes(row['command_journal'])
            if len(journal)>128*1024:raise ValueError('private command journal exceeds admission')
            state=json.loads(journal)
            expected=dict(scope=row['command_scope'],commands=row['commands'],commit_started=False)
            if (canonical_bytes(state)!=canonical_bytes(expected)
                    or canonical_bytes(receipt['scope'])!=canonical_bytes(row['command_scope'])
                    or canonical_bytes(receipt['commands'])!=canonical_bytes(state['commands'])
                    or receipt['command_journal_sha256']!=content_hash(state)
                    or type(receipt['returncode']) is not int or receipt['returncode']!=row['channel'].process.returncode):
                raise ValueError('private outcome scope or durable command evidence changed')
            return receipt
        finally:row['lock'].release()

    def command(self, payload):
        row = self.sessions[payload['handle']]
        if not row['lock'].acquire(blocking=False):
            row['failed'].set()
            raise ValueError('one in-flight writer command required; attempt poisoned')
        try:return self._command_locked(payload, row)
        finally:row['lock'].release()

    def status(self, payload):
        row = self.sessions[payload['handle']]
        if payload['job_id'] != row['job_id'] or row['failed'].is_set() or row['state'] not in ('active','saved','aborted'):
            raise ValueError('private writer health is failed or unavailable')
        return dict(handle=payload['handle'],state=row['state'])

    def _command_locked(self, payload, row):
        a = self.agent
        try:
            if row['state'] != 'active' or row['failed'].is_set() or payload['job_id'] != row['job_id']:
                raise ValueError('active exact-job writer required')
            from .private_write_outcome import command_identity
            identity=command_identity(payload)
            if len(row['commands'])>=256 or any(c['operation_id']==identity['operation_id'] for c in row['commands']):
                raise ValueError('fresh bounded command operation required; no mutation replay')
            row['commands'].append(identity)
            if payload['command'].get('op')=='commit':row['commit_started']=True
            self._persist_command_intents(row)
            result = row['channel'].call(payload['command'], payload.get('row_id'))
            if row['failed'].is_set():raise ValueError('concurrent writer command poisoned attempt')
            if result['op'] in ('saved', 'aborted'):
                row['channel'].close()
                if row['channel'].process.returncode != 0:
                    raise ValueError('private writer did not exit successfully')
                row['state'] = result['op']
                if result['op'] == 'saved':
                    spec = row['spec']; owner = spec['base']['owners'][0]
                    if result.get('spec_sha256')!=row['spec_sha256']:raise ValueError('saved writer consumed a different specification')
                    self._seal(row,result)
                    if row['failed'].is_set():raise ValueError('concurrent writer command poisoned attempt')
                    metadata = a.register(row['replica_id'], owner['checkpoint_dir'], owner['subtree_id'], owner['generation']+1)
                    a.reservations[row['replica_id']]['state'] = 'ready'
                    result = dict(result, metadata=metadata)
            if row['failed'].is_set():raise ValueError('concurrent writer command poisoned attempt')
            return result
        except BaseException:
            row['failed'].set()
            row['state'] = 'poisoned'
            row['channel'].close()
            a.reservations[row['replica_id']]['state'] = 'failed'
            raise

    def close_one(self, payload):
        row = self.sessions[payload['handle']]
        if payload['job_id'] != row.get('job_id'):
            raise ValueError('owned exact-job cleanup required')
        if not row['lock'].acquire(timeout=5):
            row['failed'].set()
            raise ValueError('in-flight owned writer cleanup requires reconciliation')
        try:
            channel = row['channel']
            if channel is not None:
                channel.close()
                if not channel.closed or type(channel.process.returncode) is not int:
                    raise ValueError('writer terminal process/threads remain unreconciled')
            row['state'] = 'closed'
            self._retain_discard(row)
            return dict(handle=payload['handle'], state='closed', started=channel is not None, returncode=None if channel is None else channel.process.returncode, threads_stopped=True)
        except BaseException:
            row['failed'].set();row['state']='cleanup-unreconciled'
            raise
        finally:row['lock'].release()

    def _seal(self, row, result):
        import shutil
        import time
        from .dynamic_content import mapped_digest, PROOF_NAME
        from .dynamic_runtime import inventory, PRODUCER_SOURCE_SHA256
        from .tree_manifest import artifact, content_hash, file_hash, _atomic_write
        spec = row['spec']; owner = spec['base']['owners'][0]
        directory = Path(owner['checkpoint_dir'])
        saved_spec = directory/'selected-write-spec.json'
        spec_bytes=sealed_spec_bytes(row['inputs']/'selected-write-spec.json',row['spec_sha256'])
        _atomic_write(saved_spec,spec_bytes)
        _atomic_write(directory/'selected-write-result.json',canonical_bytes(result))
        manifest = directory/f"manifest-{owner['generation']+1}.json"
        snapshot = json.loads(manifest.read_bytes())
        mapping = directory/snapshot['id_mapping']['path']
        digest, count = mapped_digest(directory/'construction-input.f32', mapping, spec['base']['dimension'], spec['base']['metric'])
        inputs = {}
        for key, filename in [('vectors','construction-input.f32'),('map','construction-input.u64le')]:
            desc = artifact(directory/filename)
            inputs[key] = dict(file=filename, bytes=desc['bytes'], sha256=desc['sha256'])
        channel = row['channel']
        proof = dict(format_version=1, execution_kind='selected-write-session', consumed_spec_sha256=row['spec_sha256'], producer_source_sha256=PRODUCER_SOURCE_SHA256,
                     binary_sha256=file_hash(self.agent.config['selected_writer_binary']), argv=channel.argv, pid=channel.process.pid,
                     started_unix=channel.started_unix, finished_unix=time.time(), returncode=channel.process.returncode,
                     inputs=inputs, mapped_normalized_sha256=digest, snapshot_sha256=file_hash(manifest), map_sha256=file_hash(mapping),
                     dimension=spec['base']['dimension'], metric=spec['base']['metric'], output_inventory_sha256=content_hash(inventory(directory)),
                     compactor_spec=dict(file=saved_spec.name, bytes=saved_spec.stat().st_size, sha256=file_hash(saved_spec)))
        if count != snapshot['vector_count']:
            raise ValueError('selected writer input coverage differs')
        _atomic_write(directory/PROOF_NAME, canonical_bytes(proof))

    def close(self):
        errors = []
        for row in self.sessions.values():
            if row['channel'] is not None:
                try: row['channel'].close()
                except Exception as error: errors.append(str(error))
        if errors:
            raise ValueError('owned writer cleanup requires reconciliation: ' + '; '.join(errors))
