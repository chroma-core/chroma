"""Coordinate private owner sessions against one captured published topology.

This component selects one destination using the writer's actual global beam.
It never rebuilds a lower tree and never publishes an independent pointer.
"""
import math
import struct
from .tree_manifest import content_hash
from .write_frontier import Node, destination


def f32_bits(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError('finite scalar distance required')
    return struct.unpack('<I', struct.pack('<f', value))[0]


def total_key(value):
    bits = f32_bits(value)
    return (~bits & 0xffffffff) if bits & 0x80000000 else bits | 0x80000000


def child(owner, value):
    if not isinstance(value, dict) or set(value) != {'parent_id', 'node_id', 'is_leaf', 'distance', 'distance_bits'}:
        raise ValueError('exact writer child schema required')
    for key in ('parent_id', 'node_id', 'distance_bits'):
        if type(value[key]) is not int or not 0 <= value[key] <= 0xffffffff:
            raise ValueError('writer child integer differs')
    if type(value['is_leaf']) is not bool or f32_bits(value['distance']) != value['distance_bits']:
        raise ValueError('writer child precision or kind differs')
    return value['parent_id'], Node(owner, value['node_id'], value['is_leaf'], value['distance'])


class RemoteWriteCoordinator:
    """A single-use route/register ledger for already admitted owner channels."""
    def __init__(self, sessions, info, topology_sha256, old_counts, rows, call, record):
        if not 1 <= len(sessions) <= 8 or set(sessions) != set(info) or set(sessions) != set(old_counts):
            raise ValueError('complete bounded owner set required')
        if len(rows) != 2:
            raise ValueError('two actual materialized rows required')
        if any(not isinstance(o,str) or not o or not isinstance(sessions[o],str) or not sessions[o] or type(old_counts[o]) is not int or old_counts[o]<0 for o in sessions):
            raise ValueError('strict owner handles and old local counts required')
        first=sum(old_counts.values())
        for i,row in enumerate(rows):
            if (not isinstance(row,dict) or type(row.get('global_id')) is not int or row['global_id']!=first+i
                    or not isinstance(row.get('embedding_bits'),list) or not 1<=len(row['embedding_bits'])<=16
                    or any(type(v) is not int or not 0<=v<=0xffffffff or not math.isfinite(struct.unpack('<f',struct.pack('<I',v))[0]) for v in row['embedding_bits'])):
                raise ValueError('exact contiguous actual row identities and bits required')
        if len(rows[0]['embedding_bits'])!=len(rows[1]['embedding_bits']):raise ValueError('row dimensions differ')
        self.sessions = sessions
        self.rows = rows
        self.call = call
        self.record = record
        self.topology_sha256 = topology_sha256
        self.counts = dict(old_counts)
        self.routes = []
        self.poisoned = False
        self.finished = False
        policies = []
        widths = []
        for owner in sorted(sessions):
            value = info[owner]
            if set(value) != {'op', 'policy', 'local_level_widths', 'old_local_count'} or value['op'] != 'info' or value['old_local_count'] != old_counts[owner]:
                raise ValueError('owner info differs')
            local = value['local_level_widths']
            if not isinstance(local, list) or not 1 <= len(local) <= 128 or local[0] != 1 or any(type(n) is not int or not 0 <= n <= 100000 for n in local):
                raise ValueError('bounded owner widths required')
            policies.append(content_hash(value['policy']))
            widths.append(local)
        if len(set(policies)) != 1:
            raise ValueError('owners disagree on actual writer policy')
        self.widths = [sum(w[i] if i < len(w) else 0 for w in widths) for i in range(max(map(len, widths)))]

    def _call(self, owner, command, row_id=None):
        value = self.call(owner, self.sessions[owner], command, row_id)
        self.record(dict(owner=owner, row_id=row_id, command=command, response=value))
        return value

    def run(self):
        if self.poisoned or self.finished or self.routes:
            raise ValueError('fresh single-use write coordinator required')
        try:
            for row in self.rows:
                row_id = row['global_id']
                roots = []
                for owner in sorted(self.sessions):
                    reply = self._call(owner, dict(op='root'), row_id)
                    if set(reply) != {'op', 'root'} or reply['op'] != 'root':
                        raise ValueError('root response differs')
                    _, root = child(owner, reply['root'])
                    roots.append(root)
                def expand(owner, parents, topology):
                    if topology != self.topology_sha256:
                        raise ValueError('route topology differs')
                    reply = self._call(owner, dict(op='expand', parents=list(parents)), row_id)
                    if set(reply) != {'op', 'children'} or reply['op'] != 'expand' or not isinstance(reply['children'], list) or len(reply['children']) > 1024:
                        raise ValueError('bounded expansion response required')
                    return [child(owner, value) for value in reply['children']]
                def count(level, ordered):
                    command = dict(op='count', level=level, global_level_widths=self.widths, sorted_distance_bits=[f32_bits(n.distance) for n in ordered])
                    values = []
                    for owner in sorted(self.sessions):
                        reply = self._call(owner, command, row_id)
                        if set(reply) != {'op', 'count'} or reply['op'] != 'count' or type(reply['count']) is not int:
                            raise ValueError('actual global beam response differs')
                        values.append(reply['count'])
                    if len(set(values)) != 1:
                        raise ValueError('owners disagree on global beam')
                    return values[0]
                winner, trace = destination(roots, expand, count, topology_hash=self.topology_sha256, distance_key=total_key)
                local = self.counts[winner.owner]
                grant = dict(global_id=row_id, embedding_bits=row['embedding_bits'], topology_sha256=self.topology_sha256, owner=winner.owner, leaf=winner.node_id, local_id=local, distance_bits=f32_bits(winner.distance))
                reply = self._call(winner.owner, dict(op='register', leaf=winner.node_id, local_id=local, distance_bits=f32_bits(winner.distance), route_sha256=content_hash(grant)), row_id)
                if reply != dict(op='registered', global_id=row_id, local_id=local, leaf_id=winner.node_id, route_sha256=content_hash(grant)):
                    raise ValueError('selected destination registration differs')
                self.routes.append(dict(id=row_id, subtree_id=winner.owner, leaf_id=winner.node_id, local_id=local, distance=winner.distance))
                self.counts[winner.owner] += 1
                self.record(dict(global_id=row_id, grant=grant, trace=[dict(level=t['level'], candidates=[n.__dict__ for n in t['candidates']], selected=[n.__dict__ for n in t['selected']]) for t in trace]))
            self.finished = True
            return self.routes
        except BaseException:
            self.poisoned = True
            raise


class RemoteWriteExecution:
    """Prepare a tiny append through owner agents and retain full local mirrors.

    Mirrors preserve the existing full validation oracle. They are a fixture
    mechanism and do not establish a production storage or transport policy.
    """
    def __init__(self, parent, context, materialized_export, clients, health):
        import copy
        self.parent = copy.deepcopy(parent)
        self.context = copy.deepcopy(context)
        self.materialized_export = copy.deepcopy(materialized_export)
        self.raw_clients = dict(clients)
        self.health_callback = health
        self.started = False
        self.poisoned = False
        self.generation = None
        self.clients = {}
        self.sessions = {}
        self.ledger = None

    def verify(self):
        if self.poisoned:
            raise ValueError('remote write attempt is poisoned')
        self.health_callback()
        if self.generation is not None:
            self.generation.verify()

    def prepare(self, planner_binary, reader_binary, parent, canonical_vectors, output, context):
        import base64
        import copy
        import json
        from pathlib import Path
        import uuid
        from .compactor_materialized import checked
        from .compactor_namespace import current_view
        from .compactor_remote_topology import TransportLedger, TrackedClient, TrackedGeneration, _metadata, _owner
        from .dynamic_deployment import validate_record, resolved_tree, verify_canonical_append, make_record
        from .dynamic_runtime import probe_reader
        from .dynamic_topology import owned_ids
        from .dynamic_write import VERIFIED_WRITE_PLANNERS, validate_plan
        from .tree_manifest import artifact, file_hash, canonical_bytes, _atomic_write, load_model
        if self.started or content_hash(context) != content_hash(self.context) or content_hash(parent) != content_hash(self.parent):
            raise ValueError('fresh exact captured remote write execution required')
        self.started = True
        try:
            self.verify(); validate_record(parent)
            if file_hash(planner_binary) not in VERIFIED_WRITE_PLANNERS:
                raise ValueError('admitted selected writer required')
            view = current_view(context)
            export_path = checked(self.materialized_export, 2*1024*1024)
            export_raw = export_path.read_bytes(); export = json.loads(export_raw)
            if export['registered_bundle'] != view['registered_bundle'] or export['captured']['parent_deployment_sha256'] != content_hash(parent):
                raise ValueError('actual export is not captured current parent')
            rows = export['captured']['rows']
            tree = resolved_tree(parent); old = tree['source']['row_count']
            if len(rows) != 2 or [r['global_id'] for r in rows] != [old, old+1] or old+2 > 128 or tree['dimension'] > 16 or not 1 <= len(self.raw_clients) <= 2:
                raise ValueError('tiny actual two-row remote append required')
            source = verify_canonical_append(parent['canonical_vectors'], artifact(canonical_vectors), tree['dimension'])
            output = Path(output); output.mkdir(parents=True, exist_ok=False)
            canonical = output/'canonical.f32'; canonical.write_bytes(source.read_bytes())
            append = output/'append.f32'; append.write_bytes(canonical.read_bytes()[old*tree['dimension']*4:])
            if len(append.read_bytes()) != 2*tree['dimension']*4:
                raise ValueError('exact candidate vector suffix required')
            self.ledger = TransportLedger(output/'transport', parent)
            self.clients = {w: TrackedClient(c, self.ledger) for w,c in self.raw_clients.items()}
            model = load_model(tree)
            old_maps = {s['subtree_id']: list(owned_ids(s)) for s in tree['subtrees']}
            infos = {}; sources = {}; sid_workers = {}
            current_bundle_raw = checked(view['registered_bundle'], 32*1024*1024).read_bytes()
            artifacts = dict(parent_deployment_file=canonical_bytes(parent), registered_bundle_file=current_bundle_raw, materialized_export_file=export_raw, model_file=checked(tree['model'],2*1024*1024).read_bytes(), canonical_vectors=canonical.read_bytes(), append_vectors=append.read_bytes())
            artifacts = {k: base64.b64encode(v).decode() for k,v in artifacts.items()}
            for slot in sorted(tree['subtrees'], key=lambda s:s['subtree_id']):
                sid = slot['subtree_id']; owner = parent['owners'][sid]; remote = owner['remote']; worker = remote['worker_id']; client = self.clients[worker]
                if client.instance_id != remote['instance_id'] or client.run_id != remote['agent_run_id']:
                    raise ValueError('captured owner physical identity differs')
                metadata = client.rpc(dict(op='metadata', replica_id=remote['replica_id']))
                _metadata(metadata, slot, owner['inventory_sha256'])
                handle = uuid.uuid4().hex; replica = 'write-'+uuid.uuid4().hex
                self.sessions[sid] = handle; sid_workers[sid] = worker; sources[sid] = metadata
                # Caller-chosen identities are durable before the mutating RPC.
                self.ledger.state.setdefault('write_sessions', {})[sid] = dict(worker=worker, handle=handle, replica_id=replica, state='reserved')
                self.ledger.save()
                spec = dict(base=dict(parent_sha256=content_hash(parent), source_id=tree['source']['source_id'], dimension=tree['dimension'], metric=tree['metric'], old_count=old,
                                      canonical_vectors='private', canonical_sha256=file_hash(canonical), append_vectors='private', append_sha256=file_hash(append), max_rows=128,
                                      owners=[dict(subtree_id=sid, checkpoint_dir='private', generation=slot['generation'], manifest_sha256=slot['snapshot']['sha256'], centroid=model['centroids'][slot['centroid_index']])]),
                            authorization=uuid.uuid4().hex, session_id=context['compaction']['session_id'], attempt_id=handle, collection_version=context['parent']['collection']['version'],
                            parent_log_position=context['parent']['collection']['log_position'], pulled_log_offset=context['compaction']['pulled_log_offset'], ownership_generation=parent['fence'], source_map_sha256=slot['local_to_global']['sha256'],
                            parent_deployment_file='private', registered_bundle_file='private', registered_bundle_sha256=view['registered_bundle']['sha256'], materialized_export_file='private', materialized_export_sha256=self.materialized_export['sha256'],
                            model_file='private', model_sha256=tree['model']['sha256'], worker_id=worker, instance_id=client.instance_id, agent_run_id=client.run_id, source_replica_id=remote['replica_id'], rows=rows)
                result = client.rpc(dict(op='prepare-selected-write', handle=handle, replica_id=replica, job_id=context['compaction']['job_id'], captured_context=context, source_replica_id=remote['replica_id'], source_metadata=metadata, spec=spec, artifacts=artifacts))
                if result['handle'] != handle or result['replica_id'] != replica or result['clone_proof']['source_inventory_sha256'] != owner['inventory_sha256']:
                    raise ValueError('private clone response differs')
                infos[sid] = result['info']
            def call(sid, handle, command, row_id):
                self.verify()
                return self.clients[sid_workers[sid]].rpc(dict(op='selected-write-command', handle=handle, job_id=context['compaction']['job_id'], command=command, row_id=row_id))
            trace_path = output/'write-commands.jsonl'
            def record(value):
                with trace_path.open('ab') as stream:
                    stream.write(canonical_bytes(value)); stream.flush()
            coordinator = RemoteWriteCoordinator(self.sessions, infos, content_hash(parent), {s:len(v) for s,v in old_maps.items()}, rows, call, record)
            routes = coordinator.run(); maps = copy.deepcopy(old_maps); saved = []; updated = copy.deepcopy(tree); placements = copy.deepcopy(parent['owners'])
            for route in routes: maps[route['subtree_id']].append(route['id'])
            for sid in sorted(self.sessions):
                changed = maps[sid] != old_maps[sid]
                result = call(sid, self.sessions[sid], dict(op='commit', routes=routes) if changed else dict(op='abort'), None)
                self.ledger.state['write_sessions'][sid]['state'] = result['op']; self.ledger.save()
                if not changed: continue
                metadata = result['metadata']; receipt = result['receipt']; saved.append(receipt)
                previous = next(s for s in tree['subtrees'] if s['subtree_id']==sid)
                if (receipt.get('subtree_id') != sid or receipt.get('parent_sha256') != content_hash(parent)
                        or receipt.get('parent_manifest_sha256') != previous['snapshot']['sha256']
                        or receipt.get('generation') != previous['generation']+1
                        or receipt.get('snapshot_sha256') != metadata['subtree']['snapshot']['sha256']
                        or receipt.get('selected_leaf_memberships') != [r for r in routes if r['subtree_id']==sid]
                        or receipt.get('exact_vector_rows_verified') != len(maps[sid])
                        or receipt.get('all_selected_leaf_memberships_verified') is not True):
                    raise ValueError('saved leaf receipt differs from coordinated single ownership')
                directory = output/('index-'+sid); client = self.clients[sid_workers[sid]]; replica = self.ledger.state['write_sessions'][sid]['replica_id']
                client.mirror(replica, metadata['owner']['inventory_sha256'], directory, output/(sid+'.tar'))
                slot = copy.deepcopy(metadata['subtree']); slot['snapshot'] = artifact(directory/Path(slot['snapshot']['path']).name); slot['local_to_global'] = artifact(directory/Path(slot['local_to_global']['path']).name)
                previous = next(s for s in tree['subtrees'] if s['subtree_id']==sid); slot['centroid_index'] = previous['centroid_index']
                if slot['snapshot']['sha256'] != metadata['subtree']['snapshot']['sha256'] or slot['local_to_global']['sha256'] != metadata['subtree']['local_to_global']['sha256']:
                    raise ValueError('saved selected writer mirror differs')
                placements[sid] = _owner(directory, slot, sid_workers[sid], replica, client)
                probe_reader(reader_binary,sid,placements[sid],slot,5)
                updated['subtrees'][next(i for i,s in enumerate(updated['subtrees']) if s['subtree_id']==sid)] = slot
            plan = dict(status='saved-private-global-writes', durable=True, parent_sha256=content_hash(parent), append_sha256=file_hash(append), owners=[dict(subtree_id=s,ids=maps[s]) for s in sorted(maps)], routes=routes, saved=saved)
            validate_plan(plan,tree,old_maps,content_hash(parent),file_hash(append),old+2)
            _atomic_write(output/'write-plan.stdout',canonical_bytes(plan))
            updated['tree_generation'] += 1; updated['source']['row_count'] = old+2
            pages = trie = None
            additions = {s:maps[s][len(old_maps[s]):] for s in maps}
            if 'ownership_pages' in parent:
                from . import ownership_pages
                from .dynamic_deployment import validate_ownership_pages
                pages = ownership_pages.append(validate_ownership_pages(parent), additions, output/'ownership-pages')
            if 'ownership_trie' in parent:
                from . import ownership_trie
                from .dynamic_deployment import validate_ownership_trie
                trie,_,_ = ownership_trie.append(validate_ownership_trie(parent), additions, output/'ownership-trie')
            candidate = make_record(updated, placements, parent, 'compaction', canonical_vectors=artifact(canonical), ownership_pages=pages, ownership_trie=trie)
            validate_record(candidate); self.verify()
            self.generation = TrackedGeneration(candidate,self.clients,self.ledger)
            self.verify()
            return candidate
        except BaseException:
            self.poisoned = True
            raise

    def close(self):
        errors = []
        if self.generation is not None:
            try: self.generation.close()
            except Exception as error: errors.append(repr(error))
        if self.ledger is not None:
            for sid, row in self.ledger.state.get('write_sessions', {}).items():
                if row.get('cleanup') == 'closed':continue
                try:
                    response=self.clients[row['worker']].rpc(dict(op='close-selected-write',handle=row['handle'],job_id=self.context['compaction']['job_id']))
                    if response.get('state') != 'closed' or response.get('handle') != row['handle']:raise ValueError('owned writer close response differs')
                    row['cleanup']='closed';self.ledger.save()
                except Exception as error:errors.append(repr(error))
        if self.ledger is not None and any(r['status']=='unreconciled' for r in self.ledger.state['operations']):
            errors.append('unknown mutating agent replies require reconciliation')
        if errors:
            raise ValueError('remote write cleanup is unreconciled: ' + '; '.join(errors))
