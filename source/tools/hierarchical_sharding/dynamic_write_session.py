"""A bounded, single-owner private writer channel used by the local agent.

The caller supplies a previously checked specification and binary. This module
owns command sequencing and process lifetime; it does not authorize routing or
publish a collection. An uncertain response permanently poisons the channel.
"""
import json
from pathlib import Path
import queue
import subprocess
import threading
from .tree_manifest import canonical_bytes


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
        self.responses = queue.Queue(maxsize=2)
        import time
        self.started_unix = time.time()
        self.argv = [str(binary), 'selected-write-session', '--spec', str(spec_path)]
        self.protocol_bytes = 0
        self.protocol_path = Path(str(stderr_path)+'.protocol.jsonl')
        self.stderr = Path(stderr_path).open('xb')
        try:
            self.process = subprocess.Popen(self.argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr)
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
        try:
            raw = canonical_bytes(request)
            if len(raw) > self.FRAME_BYTES:
                raise ValueError('private writer request exceeds admission')
            self._retain('request', raw)
            self.process.stdin.write(raw)
            self.process.stdin.flush()
            response = self.responses.get(timeout=self.timeout)
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
        if self.closed:
            return
        self.closed = True
        try:
            try:self.process.stdin.close()
            except (BrokenPipeError, OSError):pass
            try:
                self.process.wait(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=self.timeout)
        finally:
            self.process.stdout.close()
            self.stderr.close()
            thread=getattr(self,'thread',None)
            if thread is not None and thread.ident is not None:
                thread.join(self.timeout)
                if thread.is_alive():raise ValueError('private writer receive thread requires reconciliation')


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
            self.sessions[handle] = dict(state='reserved', channel=None, replica_id=replica, job_id=payload['job_id'])
        row = self.sessions[handle]
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
            _atomic_write(inputs/'clone-proof.json',canonical_bytes(proof))
            owner = spec['base']['owners'][0]
            owner['checkpoint_dir'] = str(directory)
            _atomic_write(directory/'global-write-private.json',canonical_bytes(dict(parent_sha256=spec['base']['parent_sha256'], manifest_sha256=owner['manifest_sha256'], subtree_id=owner['subtree_id'])))
            path = inputs/'selected-write-spec.json'
            _atomic_write(path,canonical_bytes(spec))
            row.update(state='launching', spec=spec, job_id=job['job_id'], inputs=inputs, source_metadata=source)
            row['channel'] = PrivateWriteChannel(binary, path, spec, inputs/'selected-write.stderr')
            info = row['channel'].call(dict(op='info'))
            row['state'] = 'active'
            a.record('selected-write-prepared', handle=handle, replica_id=replica, job_id=job['job_id'], clone_inventory_sha256=content_hash(clone_inventory))
            return dict(handle=handle, replica_id=replica, info=info, clone_proof=proof)
        except BaseException:
            row['state'] = 'poisoned'
            if row['channel'] is not None:
                row['channel'].close()
            raise

    def command(self, payload):
        a = self.agent
        row = self.sessions[payload['handle']]
        if row['state'] != 'active' or payload['job_id'] != row['job_id']:
            raise ValueError('active exact-job writer required')
        try:
            result = row['channel'].call(payload['command'], payload.get('row_id'))
            if result['op'] in ('saved', 'aborted'):
                row['channel'].close()
                if row['channel'].process.returncode != 0:
                    raise ValueError('private writer did not exit successfully')
                row['state'] = result['op']
                if result['op'] == 'saved':
                    spec = row['spec']; owner = spec['base']['owners'][0]
                    self._seal(row)
                    metadata = a.register(row['replica_id'], owner['checkpoint_dir'], owner['subtree_id'], owner['generation']+1)
                    a.reservations[row['replica_id']]['state'] = 'ready'
                    result = dict(result, metadata=metadata)
            return result
        except BaseException:
            row['state'] = 'poisoned'
            row['channel'].close()
            a.reservations[row['replica_id']]['state'] = 'failed'
            raise

    def close_one(self, payload):
        row = self.sessions[payload['handle']]
        if payload['job_id'] != row.get('job_id'):
            raise ValueError('owned exact-job cleanup required')
        channel = row['channel']
        if channel is not None:
            channel.close()
        row['state'] = 'closed'
        return dict(handle=payload['handle'], state='closed', returncode=None if channel is None else channel.process.returncode)

    def _seal(self, row):
        import shutil
        import time
        from .dynamic_content import mapped_digest, PROOF_NAME
        from .dynamic_runtime import inventory, PRODUCER_SOURCE_SHA256
        from .tree_manifest import artifact, content_hash, file_hash, _atomic_write
        spec = row['spec']; owner = spec['base']['owners'][0]
        directory = Path(owner['checkpoint_dir'])
        saved_spec = directory/'selected-write-spec.json'
        shutil.copyfile(row['inputs']/'selected-write-spec.json', saved_spec)
        manifest = directory/f"manifest-{owner['generation']+1}.json"
        snapshot = json.loads(manifest.read_bytes())
        mapping = directory/snapshot['id_mapping']['path']
        digest, count = mapped_digest(directory/'construction-input.f32', mapping, spec['base']['dimension'], spec['base']['metric'])
        inputs = {}
        for key, filename in [('vectors','construction-input.f32'),('map','construction-input.u64le')]:
            desc = artifact(directory/filename)
            inputs[key] = dict(file=filename, bytes=desc['bytes'], sha256=desc['sha256'])
        channel = row['channel']
        proof = dict(format_version=1, execution_kind='selected-write-session', producer_source_sha256=PRODUCER_SOURCE_SHA256,
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
