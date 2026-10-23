"""Recorded-window compactor placement with durable conservative reservations.

This opt-in local control helper uses recorded observations, not a production
telemetry collector. Resource-inventory providers must exclude reserved copies
until reconciliation or identify their exact physical artifact IDs. Failed and
completed reservations remain charged; no timeout silently frees retained data.
"""
import copy
import fcntl
import math
from pathlib import Path
import time
from .tree_manifest import canonical_bytes,content_hash,file_hash,_atomic_write
from .dynamic_deployment import load_deployment,run_transaction
from .dynamic_runtime import inventory,prepare_migration
from .ownership_pages import _path

POLICY='recorded-cpu-window-one-move-v1'
DEFAULT_POLICY=dict(min_peak_drop=.02,min_relative_drop=.1,cooldown_ns=30_000_000_000,
                    max_observation_age_ns=60_000_000_000,max_plan_age_ns=300_000_000_000,
                    migration_cpu_cores=.01,control_storage_bytes=1024*1024)


def _int(value,maximum=2**63-1,minimum=0):
    if type(value) is not int or not minimum<=value<=maximum:raise ValueError('bounded nonnegative integer required')
    return value


def _number(value):
    if type(value) not in (int,float) or not math.isfinite(value) or value<0:raise ValueError('nonnegative finite quantity required')
    return value


def _identity(value):
    if not isinstance(value,str) or not 1<=len(value)<=256:raise ValueError('bounded resource identity required')
    return value


def replica_id(owner):
    return content_hash(dict(checkpoint_dir=owner['checkpoint_dir'],inventory_sha256=owner['inventory_sha256']))


def empty_reservations():return dict(format='balance-reservations-v2',entries={})


def reservation_snapshot(pointer):
    import json
    path=Path(pointer).parent/'balance-reservations.json'
    return json.loads(path.read_bytes()) if path.exists() else empty_reservations()


def _book(book):
    if not isinstance(book,dict) or book.get('format')!='balance-reservations-v2' or not isinstance(book.get('entries'),dict) or len(book['entries'])>128:raise ValueError('invalid bounded reservation book; older attempts require explicit reconciliation')
    for key,row in book['entries'].items():
        _identity(key);_identity(row['worker_id']);_identity(row['instance_id']);_identity(row['artifact_id'])
        for field in ('replica_storage_bytes','control_storage_bytes','memory_bytes','active_jobs'):_int(row[field])
        _number(row['cpu_cores'])
        source=row['retained_source']
        for field in ('worker_id','instance_id','artifact_id'):_identity(source[field])
        _int(source['storage_bytes'])
    return book


def _inputs(record,observations,resources,policy,now_ns,book):
    _int(now_ns);_book(book)
    if set(policy)!=set(DEFAULT_POLICY):raise ValueError('explicit complete balance policy required')
    for field in ('cooldown_ns','max_observation_age_ns','max_plan_age_ns','control_storage_bytes'):_int(policy[field])
    for field in ('min_peak_drop','min_relative_drop','migration_cpu_cores'):_number(policy[field])
    if not 0<policy['min_peak_drop']<=1 or not 0<policy['min_relative_drop']<=1:raise ValueError('positive hysteresis thresholds required')
    if observations.get('format')!='measured-compactor-window-v1' or observations.get('deployment_sha256')!=content_hash(record) or type(observations.get('fence')) is not int or observations['fence']!=record['fence']:raise ValueError('observation does not pin complete deployment and fence')
    start=_int(observations['window_start_ns']);end=_int(observations['window_end_ns'])
    if not start<end<=now_ns or now_ns-end>policy['max_observation_age_ns']:raise ValueError('future or expired observation window')
    if resources.get('format')!='compactor-resource-inventory-v1' or type(resources.get('revision')) is not int or resources['revision']<1:raise ValueError('versioned resource inventory required')
    workers={}
    for worker in resources['workers']:
        wid=_identity(worker['worker_id'])
        if wid in workers:raise ValueError('duplicate worker')
        _identity(worker['instance_id']);_identity(worker['cpu_class']);_int(worker['cpu_capacity_cores'],1024,1)
        _path(worker['checkpoint_root'])
        _int(worker['storage_capacity_logical_bytes']);_int(worker['memory_capacity_bytes']);_int(worker['max_active_jobs'],128,1);_int(worker['max_subtrees'],128,1)
        workers[wid]=worker
    if not 1<=len(workers)<=16 or len({w['cpu_class'] for w in workers.values()})!=1:raise ValueError('bounded homogeneous CPU-class workers required')
    slots={s['subtree_id']:s for s in record['tree']['subtrees']};samples={}
    if not 1<=len(slots)<=128:raise ValueError('bounded owner set required')
    for sample in observations['samples']:
        sid=sample['subtree_id']
        if sid in samples or sid not in slots:raise ValueError('duplicate or unknown sample owner')
        subtree=slots[sid];owner=record['owners'][sid];worker=workers.get(owner['worker_id'])
        expected=dict(snapshot_sha256=subtree['snapshot']['sha256'],map_sha256=subtree['local_to_global']['sha256'],ownership_revision=subtree['ownership_revision'],worker_id=owner['worker_id'],storage_artifact_id=replica_id(owner))
        if worker is None or sample.get('instance_id')!=worker['instance_id'] or any(sample.get(k)!=v for k,v in expected.items()):raise ValueError('sample owner/snapshot/physical instance binding differs')
        if _path(worker['checkpoint_root']) not in _path(owner['checkpoint_dir']).parents:raise ValueError('active owner is outside declared worker checkpoint scope')
        if owner.get('remote') and owner['remote']['instance_id']!=sample['instance_id']:raise ValueError('sample differs from committed remote instance')
        for field in ('cpu_ns','replica_storage_logical_bytes','next_job_memory_bytes'):_int(sample[field])
        receipt=sample.get('measurement_receipt')
        if not isinstance(receipt,dict) or sample.get('measurement_receipt_sha256')!=content_hash(receipt):raise ValueError('recorded measurement receipt hash required')
        user=_int(receipt['user_cpu_ns']);system=_int(receipt['system_cpu_ns'])
        if user+system!=sample['cpu_ns'] or not start<=_int(receipt['started_ns'])<_int(receipt['finished_ns'])<=end:raise ValueError('sample CPU units or receipt window differ')
        if sample.get('memory_estimate_kind') not in ('declared-conservative','measured-upper-bound'):raise ValueError('explicit next-job memory estimate kind required')
        samples[sid]=sample
    if set(samples)!=set(slots):raise ValueError('observation must cover every owner')
    history=record.get('balance_state',{}).get('last_moves',{})
    for sid,row in history.items():
        _identity(sid);_int(row['reservation_started_ns']);_int(row['cooldown_until_ns'])
        if row['reservation_started_ns']>now_ns:raise ValueError('published reservation starts in the future')
    return workers,samples,end-start


def _usage(workers,book):
    used={}
    for wid,worker in workers.items():
        for key,limit in [('storage_artifacts',4096),('resident_processes',1024),('inflight',128)]:
            if not isinstance(worker.get(key),list) or len(worker[key])>limit:raise ValueError('bounded explicit resource inventory required')
        artifacts={};memory=jobs=0;cpu=0.
        for row in worker['storage_artifacts']:
            key=_identity(row['artifact_id']);size=_int(row['logical_bytes'])
            if key in artifacts and artifacts[key]!=size:raise ValueError('same physical artifact has inconsistent byte size')
            artifacts[key]=size
        processes=set()
        for process in worker['resident_processes']:
            key=_identity(process['process_id'])
            if key in processes:raise ValueError('resident process must appear once')
            processes.add(key);memory+=_int(process['memory_bytes']);jobs+=_int(process['active_jobs'],128)
        storage=sum(artifacts.values());inflight=set()
        for row in worker['inflight']:
            key=_identity(row['reservation_id'])
            if key in inflight:raise ValueError('duplicate in-flight reservation')
            inflight.add(key);storage+=_int(row['remaining_storage_bytes']);memory+=_int(row['memory_bytes']);jobs+=_int(row['active_jobs'],128);cpu+=_number(row['cpu_cores'])
        for key,row in sorted(book['entries'].items()):
            if row['worker_id']!=wid:continue
            if row['instance_id']!=worker['instance_id']:raise ValueError('retained reservation physical instance changed')
            if row['artifact_id'] in artifacts:
                if artifacts[row['artifact_id']]!=row['replica_storage_bytes']:raise ValueError('reconciled copy size differs')
            else:storage+=row['replica_storage_bytes'];artifacts[row['artifact_id']]=row['replica_storage_bytes']
            storage+=row['control_storage_bytes'];memory+=row['memory_bytes'];jobs+=row['active_jobs'];cpu+=row['cpu_cores']
        for row in book['entries'].values():
            source=row['retained_source']
            if source['worker_id']!=wid:continue
            if source['instance_id']!=worker['instance_id']:raise ValueError('retained source physical instance changed')
            if source['artifact_id'] in artifacts:
                if artifacts[source['artifact_id']]!=source['storage_bytes']:raise ValueError('retained source size differs')
            else:storage+=source['storage_bytes'];artifacts[source['artifact_id']]=source['storage_bytes']
        if storage>worker['storage_capacity_logical_bytes'] or memory>worker['memory_capacity_bytes'] or jobs>worker['max_active_jobs']:raise ValueError('existing retained/inflight resources exceed capacity')
        _number(cpu)
        used[wid]=dict(storage_logical_bytes=storage,memory_bytes=memory,active_jobs=jobs,reserved_cpu_cores=cpu,artifact_ids=artifacts)
    if any(row['worker_id'] not in workers or row['retained_source']['worker_id'] not in workers for row in book['entries'].values()):raise ValueError('retained reservation worker missing from inventory')
    return used


def plan_balance(record,observations,resources,policy,now_ns,reservations=None):
    book=reservations if reservations is not None else empty_reservations();workers,samples,window=_inputs(record,observations,resources,policy,now_ns,book);used=_usage(workers,book)
    owners={sid:row['worker_id'] for sid,row in record['owners'].items()};loads={wid:used[wid]['reserved_cpu_cores'] for wid in workers};counts={wid:0 for wid in workers}
    for sid in sorted(samples):
        wid=owners[sid];loads[wid]+=samples[sid]['cpu_ns']/window;counts[wid]+=1
        if counts[wid]>workers[wid]['max_subtrees']:raise ValueError('current owner count exceeds worker capacity')
        artifact=samples[sid]['storage_artifact_id']
        if used[wid]['artifact_ids'].get(artifact)!=samples[sid]['replica_storage_logical_bytes']:raise ValueError('active snapshot logical bytes are absent from resource inventory')
    peak=max(loads[w]/workers[w]['cpu_capacity_cores'] for w in workers);options=[];blocked=dict(cooldown=0,capacity=0,hysteresis=0)
    history=record.get('balance_state',{}).get('last_moves',{})
    for sid,sample in sorted(samples.items()):
        source=owners[sid]
        if sid in history and now_ns<history[sid]['cooldown_until_ns']:blocked['cooldown']+=1;continue
        for target,worker in sorted(workers.items()):
            if target==source:continue
            reservation=dict(worker_id=target,instance_id=worker['instance_id'],replica_storage_bytes=sample['replica_storage_logical_bytes'],control_storage_bytes=policy['control_storage_bytes'],memory_bytes=sample['next_job_memory_bytes'],active_jobs=1,cpu_cores=policy['migration_cpu_cores'])
            after=dict(loads);after[source]-=sample['cpu_ns']/window;after[target]+=sample['cpu_ns']/window+policy['migration_cpu_cores']
            u=used[target]
            if counts[target]+1>worker['max_subtrees'] or u['storage_logical_bytes']+reservation['replica_storage_bytes']+reservation['control_storage_bytes']>worker['storage_capacity_logical_bytes'] or u['memory_bytes']+reservation['memory_bytes']>worker['memory_capacity_bytes'] or u['active_jobs']+1>worker['max_active_jobs'] or after[target]>worker['cpu_capacity_cores']:
                blocked['capacity']+=1;continue
            next_peak=max(after[w]/workers[w]['cpu_capacity_cores'] for w in workers);drop=peak-next_peak
            if drop<policy['min_peak_drop'] or peak==0 or drop/peak<policy['min_relative_drop']:blocked['hysteresis']+=1;continue
            options.append((next_peak,sid,target,after,reservation))
    plan=dict(policy=POLICY,planner_sha256=file_hash(Path(__file__)),parent_sha256=content_hash(record),fence=record['fence'],observations_sha256=content_hash(observations),inventory_sha256=content_hash(resources),inventory_revision=resources['revision'],reservations_sha256=content_hash(book),policy_sha256=content_hash(policy),decision_ns=now_ns,expires_ns=_int(now_ns+policy['max_plan_age_ns']),before_peak_utilization=peak,estimated_cores=loads,blocked=blocked,status='no-move',assignments=owners,moves=[])
    if options:
        next_peak,sid,target,after,reservation=min(options,key=lambda row:row[:3]);plan.update(status='move',after_peak_utilization=next_peak,after_estimated_cores=after,reservation=reservation,moves=[dict(subtree_id=sid,source=owners[sid],target=target)]);plan['assignments']=dict(owners,**{sid:target})
    return plan


def publish_balance(pointer,parent,plan,observations,policy,inventory_provider,destination,probe,now_provider=time.time_ns):
    """Reserve, verify/copy, probe and CAS one move; retain every failed attempt."""
    import json
    parent=copy.deepcopy(parent);plan=copy.deepcopy(plan)
    pointer=_path(str(pointer));destination=_path(str(destination));bookpath=pointer.parent/'balance-reservations.json'
    with open(str(bookpath)+'.lock','a+b') as held:
        fcntl.flock(held,fcntl.LOCK_EX);book=_book(reservation_snapshot(pointer));resources=inventory_provider();now=now_provider()
        if content_hash(load_deployment(pointer))!=content_hash(parent) or plan.get('parent_sha256')!=content_hash(parent):raise ValueError('balance plan has stale complete deployment')
        _inputs(parent,observations,resources,policy,now,book)
        if now<plan['decision_ns'] or now>plan['expires_ns']:raise ValueError('balance plan expired or future')
        expected=plan_balance(parent,observations,resources,policy,plan['decision_ns'],book)
        if content_hash(expected)!=content_hash(plan) or plan['status']!='move':raise ValueError('balance plan/resource reservations changed or no beneficial move')
        if len(book['entries'])>=128:raise ValueError('reservation reconciliation required before further moves')
        move=plan['moves'][0];sid=move['subtree_id'];source=parent['owners'][sid]
        target=next(w for w in resources['workers'] if w['worker_id']==move['target'])
        if _path(target['checkpoint_root']) not in destination.parents or destination.exists():raise ValueError('fresh destination must remain inside declared target checkpoint scope')
        if source.get('remote'):raise ValueError('local balancing cannot claim remote replica transfer')
        sample=next(row for row in observations['samples'] if row['subtree_id']==sid)
        rows=inventory(source['checkpoint_dir'])
        if content_hash(rows)!=source['inventory_sha256'] or sum(row['bytes'] for row in rows.values())!=sample['replica_storage_logical_bytes']:raise ValueError('measured source replica bytes or sealed inventory differ')
        digest=content_hash(plan);entry=dict(plan['reservation'],artifact_id=content_hash(dict(checkpoint_dir=str(destination),inventory_sha256=source['inventory_sha256'])),destination=str(destination),status='preparing',parent_sha256=content_hash(parent),retained_source=dict(worker_id=source['worker_id'],instance_id=sample['instance_id'],artifact_id=sample['storage_artifact_id'],storage_bytes=sample['replica_storage_logical_bytes']))
        if digest in book['entries']:raise ValueError('balance attempt already reserved; reconcile instead of retrying')
        book=copy.deepcopy(book);book['entries'][digest]=entry;_atomic_write(bookpath,canonical_bytes(book));reserved_hash=content_hash(book)
        def health():
            current=inventory_provider();clock=now_provider()
            if content_hash(observations)!=plan['observations_sha256'] or content_hash(policy)!=plan['policy_sha256'] or file_hash(Path(__file__))!=plan['planner_sha256']:raise ValueError('balance observation/policy/planner binding changed before publication')
            if content_hash(current)!=plan['inventory_sha256'] or content_hash(reservation_snapshot(pointer))!=reserved_hash:raise ValueError('source/target reservation inventory changed before publication')
            if not plan['decision_ns']<=clock<=plan['expires_ns']:raise ValueError('balance plan expired before publication')
            _inputs(parent,observations,current,policy,clock,empty_reservations())
        def prepare(pinned):
            health();record=prepare_migration(pinned,plan['assignments'],{sid:destination},sample['replica_storage_logical_bytes'])
            state=copy.deepcopy(parent.get('balance_state',dict(last_moves={})));state['last_moves'][sid]=dict(reservation_started_ns=now,cooldown_until_ns=_int(plan['expires_ns']+policy['cooldown_ns']),source=move['source'],worker_id=move['target'],plan_sha256=digest)
            state['last_observations_sha256']=plan['observations_sha256'];state['last_inventory_sha256']=plan['inventory_sha256'];record['balance_state']=state
            return record
        try:
            receipt=run_transaction(pointer,parent,prepare,probe,Path(str(destination)+'.balance-transaction.json'),parameters=dict(operation='recorded-load-whole-subtree',plan=plan,destination=str(destination)),health=health)
            entry['status']='published-retained';entry['publication']=receipt
            return receipt
        except BaseException:
            entry['status']='reconciliation-required-retained';raise
        finally:
            # Failure to persist terminal classification leaves the original
            # durable full reservation charged; it never frees partial data.
            book['entries'][digest]=entry;_atomic_write(bookpath,canonical_bytes(book))
