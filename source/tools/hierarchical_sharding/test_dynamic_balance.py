"""Recorded-load decisions and real unchanged-subtree publication fixtures."""
import copy
import json
import os
from pathlib import Path
import resource
import subprocess
import time
import unittest
from unittest.mock import patch
from . import dynamic_balance as balance
from .tree_manifest import content_hash
from .dynamic_runtime import inventory,DeploymentReader
from .dynamic_deployment import make_record,publish_deployment,load_deployment
from .test_dynamic_write import make_parent,evidence_case


def inputs(record,base,cpus,now=2_000_000_000,receipts=None):
    policy=dict(balance.DEFAULT_POLICY,migration_cpu_cores=0.,control_storage_bytes=0,min_peak_drop=.001)
    workers=[]
    for wid,folder in [('local','source'),('other','target')]:
        workers.append(dict(worker_id=wid,instance_id='single-host-fixture-'+wid,cpu_class='native-local-fixture',cpu_capacity_cores=2,checkpoint_root=str(base/folder),storage_capacity_logical_bytes=64*1024**2,memory_capacity_bytes=1024**3,max_active_jobs=4,max_subtrees=128,storage_artifacts=[],resident_processes=[],inflight=[]))
    start=now-1_000_000_000 if receipts is None else min(r['started_ns'] for r in receipts.values())-1
    observations=dict(format='measured-compactor-window-v1',deployment_sha256=content_hash(record),fence=record['fence'],window_start_ns=start,window_end_ns=now,samples=[])
    slots={s['subtree_id']:s for s in record['tree']['subtrees']}
    for sid,owner in sorted(record['owners'].items()):
        worker=next(w for w in workers if w['worker_id']==owner['worker_id']);subtree=slots[sid]
        directory=Path(owner['checkpoint_dir'])
        size=sum(r['bytes'] for r in inventory(directory).values()) if directory.exists() else 100
        receipt=receipts[sid] if receipts is not None else dict(kind='controlled-unit-fixture',user_cpu_ns=cpus[sid],system_cpu_ns=0,started_ns=start,finished_ns=now)
        sample=dict(subtree_id=sid,worker_id=worker['worker_id'],instance_id=worker['instance_id'],snapshot_sha256=subtree['snapshot']['sha256'],map_sha256=subtree['local_to_global']['sha256'],ownership_revision=subtree['ownership_revision'],storage_artifact_id=balance.replica_id(owner),replica_storage_logical_bytes=size,next_job_memory_bytes=64*1024**2,memory_estimate_kind='declared-conservative',cpu_ns=receipt['user_cpu_ns']+receipt['system_cpu_ns'],measurement_receipt=receipt,measurement_receipt_sha256=content_hash(receipt))
        observations['samples'].append(sample);worker['storage_artifacts'].append(dict(artifact_id=sample['storage_artifact_id'],logical_bytes=size))
    return observations,dict(format='compactor-resource-inventory-v1',revision=1,workers=workers),policy


class BalancePolicy(unittest.TestCase):
    def setUp(self):
        self.temp=__import__('tempfile').TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.base=Path(self.temp.name).resolve()
        self.record=dict(fence=1,tree=dict(subtrees=[]),owners={})
        for i in range(4):
            sid='s'+str(i);worker='other' if i==3 else 'local'
            self.record['tree']['subtrees'].append(dict(subtree_id=sid,snapshot=dict(sha256=str(i)*64),local_to_global=dict(sha256='a'*64),ownership_revision='b'*64))
            self.record['owners'][sid]=dict(worker_id=worker,checkpoint_dir=str(self.base/('target' if worker=='other' else 'source')/sid),inventory_sha256='c'*64)
        self.now=2_000_000_000
        self.observations,self.resources,self.policy=inputs(self.record,self.base,dict(s0=400000000,s1=300000000,s2=100000000,s3=100000000),self.now)

    def test_beneficial_move_deterministic_and_noise_rejected(self):
        plan=balance.plan_balance(self.record,self.observations,self.resources,self.policy,self.now)
        self.assertEqual(plan['status'],'move');self.assertLess(plan['after_peak_utilization'],plan['before_peak_utilization'])
        reordered=copy.deepcopy(self.resources);reordered['workers'].reverse();obs=copy.deepcopy(self.observations);obs['samples'].reverse()
        self.assertEqual(plan['moves'],balance.plan_balance(self.record,obs,reordered,self.policy,self.now)['moves'])
        strict=dict(self.policy,min_relative_drop=.99)
        self.assertEqual(balance.plan_balance(self.record,self.observations,self.resources,strict,self.now)['status'],'no-move')

    def test_cooldown_prevents_immediate_reverse_and_stable_window_does_not_oscillate(self):
        plan=balance.plan_balance(self.record,self.observations,self.resources,self.policy,self.now);sid=plan['moves'][0]['subtree_id']
        moved=copy.deepcopy(self.record);moved['fence']=2;moved['owners'][sid]['worker_id']='other';moved['owners'][sid]['checkpoint_dir']=str(self.base/'target'/sid)
        obs,res,policy=inputs(moved,self.base,dict(s0=400000000,s1=300000000,s2=100000000,s3=100000000),self.now+1)
        self.assertEqual(balance.plan_balance(moved,obs,res,policy,self.now+1)['status'],'no-move')
        # New skew would favor reversing the just-moved owner without cooldown.
        changed={s:100000000 for s in moved['owners']};changed[sid]=400000000;changed['s3']=500000000
        obs,res,policy=inputs(moved,self.base,changed,self.now+1)
        uncooled=balance.plan_balance(moved,obs,res,policy,self.now+1);self.assertEqual(uncooled['moves'][0]['subtree_id'],sid)
        moved['balance_state']=dict(last_moves={sid:dict(reservation_started_ns=self.now,cooldown_until_ns=self.now+policy['cooldown_ns'])})
        obs['deployment_sha256']=content_hash(moved)
        cooled=balance.plan_balance(moved,obs,res,policy,self.now+1)
        self.assertGreater(cooled['blocked']['cooldown'],0);self.assertNotIn(sid,[m['subtree_id'] for m in cooled['moves']])

    def test_retained_storage_inflight_jobs_and_process_memory_remain_charged(self):
        resources=copy.deepcopy(self.resources);target=next(w for w in resources['workers'] if w['worker_id']=='other')
        target['inflight']=[dict(reservation_id='pending-output',remaining_storage_bytes=target['storage_capacity_logical_bytes']-100,memory_bytes=0,active_jobs=0,cpu_cores=0)]
        # Existing target snapshot costs100bytes, so no new fullcopy fits.
        self.assertEqual(balance.plan_balance(self.record,self.observations,resources,self.policy,self.now)['blocked']['capacity'],3)
        resources=copy.deepcopy(self.resources);target=next(w for w in resources['workers'] if w['worker_id']=='other')
        target['resident_processes']=[dict(process_id=str(i),memory_bytes=600*1024**2,active_jobs=1,snapshot_sha256='same') for i in range(2)]
        with self.assertRaisesRegex(ValueError,'exceed capacity'):balance.plan_balance(self.record,self.observations,resources,self.policy,self.now)

    def test_stale_window_instance_revision_and_cpu_unit_mismatch_reject(self):
        for mutate in [lambda o:o.update(deployment_sha256='stale'),lambda o:o.update(fence=True),lambda o:o.update(window_end_ns=self.now+1),lambda o:o['samples'].pop(),lambda o:o['samples'][0].update(instance_id='wrong'),lambda o:o['samples'][0].update(cpu_ns=True),lambda o:o['samples'][0].update(cpu_ns=1)]:
            obs=copy.deepcopy(self.observations);mutate(obs)
            with self.assertRaises(ValueError):balance.plan_balance(self.record,obs,self.resources,self.policy,self.now)

    def test_source_and_target_artifacts_stay_charged_without_double_counting_catalog_overlap(self):
        resources=copy.deepcopy(self.resources);source=next(w for w in resources['workers'] if w['worker_id']=='local');target=next(w for w in resources['workers'] if w['worker_id']=='other')
        asset=source['storage_artifacts'][0];entry=dict(worker_id='other',instance_id=target['instance_id'],artifact_id='new-physical-copy',replica_storage_bytes=100,control_storage_bytes=0,memory_bytes=1,active_jobs=1,cpu_cores=0,retained_source=dict(worker_id='local',instance_id=source['instance_id'],artifact_id=asset['artifact_id'],storage_bytes=asset['logical_bytes']))
        book=balance.empty_reservations();book['entries']['plan']=entry;balance._book(book)
        before=balance._usage({w['worker_id']:w for w in resources['workers']},book)
        source['storage_artifacts'].remove(asset);target['storage_artifacts'].append(dict(artifact_id='new-physical-copy',logical_bytes=100))
        after=balance._usage({w['worker_id']:w for w in resources['workers']},book)
        self.assertEqual(after['local']['storage_logical_bytes'],before['local']['storage_logical_bytes'])
        self.assertEqual(after['other']['storage_logical_bytes'],before['other']['storage_logical_bytes'])


@unittest.skipUnless(os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native writer required')
class RealBalance(unittest.TestCase):
    def fixture(self,root):
        receipts={};original=subprocess.Popen
        def spawn(argv,*args,**kwargs):
            measured=len(argv)>1 and argv[1]=='build';before=resource.getrusage(resource.RUSAGE_CHILDREN);started=time.time_ns();process=original(argv,*args,**kwargs)
            if measured:
                wait=process.wait;sid=argv[argv.index('--shard-id')+1]
                def observed_wait(*a,**kw):
                    result=wait(*a,**kw)
                    if sid not in receipts:
                        after=resource.getrusage(resource.RUSAGE_CHILDREN)
                        receipts[sid]=dict(kind='actual-native-build-child-rusage-delta',pid=process.pid,argv=argv,started_ns=started,finished_ns=time.time_ns(),user_cpu_ns=round(max(0,after.ru_utime-before.ru_utime)*1e9),system_cpu_ns=round(max(0,after.ru_stime-before.ru_stime)*1e9))
                    return result
                process.wait=observed_wait
            return process
        binary=os.environ['SPANN_DYNAMIC_BINARY']
        with patch('tools.hierarchical_sharding.dynamic_runtime.subprocess.Popen',side_effect=spawn):parent,pointer,_,probe=make_parent(root/'source',binary,page_rows=4,trie_fanout=4)
        (root/'target').mkdir();now=time.time_ns();obs,res,policy=inputs(parent,root,{},now,receipts)
        policy.update(min_peak_drop=.000001,control_storage_bytes=65536,max_plan_age_ns=30_000_000_000)
        self.assertTrue(all(s['cpu_ns']>0 for s in obs['samples']))
        (root/'observations.json').write_text(json.dumps(obs,indent=2)+'\n');(root/'resources.json').write_text(json.dumps(res,indent=2)+'\n')
        return binary,parent,pointer,probe,obs,res,policy,now

    def test_actual_observed_build_work_moves_whole_snapshot_and_preserves_query(self):
        with evidence_case('measured-balance-success') as root:
            binary,parent,pointer,probe,obs,res,policy,now=self.fixture(root)
            plan=balance.plan_balance(parent,obs,res,policy,now);self.assertEqual(plan['status'],'move')
            old=DeploymentReader(pointer,binary);before=old.query([0,0,0,0],16,beam_width=None,candidate_budget=16)
            destination=root/'target'/'moved-owner'
            try:
                balance.publish_balance(pointer,parent,plan,obs,policy,lambda:copy.deepcopy(res),destination,probe)
                current=load_deployment(pointer);self.assertEqual(current['tree'],parent['tree']);self.assertEqual(current['ownership_trie'],parent['ownership_trie'])
                new=DeploymentReader(pointer,binary)
                try:
                    after=new.query([0,0,0,0],16,beam_width=None,candidate_budget=16)
                    for key in ('ids','distances','levels','selected_candidates'):self.assertEqual(after[key],before[key])
                finally:new.close()
                self.assertEqual(old.query([0,0,0,0],16,beam_width=None,candidate_budget=16)['ids'],before['ids'])
                self.assertEqual(balance.reservation_snapshot(pointer)['entries'][content_hash(plan)]['status'],'published-retained')
                sid=plan['moves'][0]['subtree_id'];self.assertTrue(Path(parent['owners'][sid]['checkpoint_dir']).exists())
                (root/'plan-and-publication.json').write_text(json.dumps(dict(plan=plan,current_sha256=content_hash(current),before=before,after=after,reservations=balance.reservation_snapshot(pointer)),indent=2)+'\n')
            finally:old.close()

    def test_reservation_inventory_change_after_copy_blocks_cas_and_retains_resources(self):
        with evidence_case('measured-balance-resource-failure') as root:
            _,parent,pointer,probe,obs,res,policy,now=self.fixture(root);head=pointer.read_bytes();plan=balance.plan_balance(parent,obs,res,policy,now);calls=[]
            def provider():
                calls.append(1);value=copy.deepcopy(res)
                if len(calls)>=3:value['revision']+=1
                return value
            destination=root/'target'/'retained-partial'
            with self.assertRaisesRegex(ValueError,'inventory changed'):balance.publish_balance(pointer,parent,plan,obs,policy,provider,destination,probe)
            self.assertEqual(pointer.read_bytes(),head);self.assertTrue(destination.exists())
            book=balance.reservation_snapshot(pointer);self.assertEqual(book['entries'][content_hash(plan)]['status'],'reconciliation-required-retained')
            self.assertEqual(json.loads(Path(str(destination)+'.balance-transaction.json').read_text())['status'],'failed_unpublished')
            # Existing failed-copy/job reservations prevent another copy under a one-job limit.
            for worker in res['workers']:
                if worker['worker_id']==plan['moves'][0]['target']:worker['max_active_jobs']=1
            retry=balance.plan_balance(parent,obs,res,policy,time.time_ns(),book)
            self.assertEqual(retry['status'],'no-move')

    def test_stale_complete_deployment_rejects_before_copy(self):
        with evidence_case('measured-balance-stale') as root:
            _,parent,pointer,probe,obs,res,policy,now=self.fixture(root);plan=balance.plan_balance(parent,obs,res,policy,now)
            winner=make_record(parent['tree'],parent['owners'],parent);publish_deployment(pointer,winner,content_hash(parent),probe);head=pointer.read_bytes();destination=root/'target'/'never-created'
            with self.assertRaisesRegex(ValueError,'stale complete deployment'):balance.publish_balance(pointer,parent,plan,obs,policy,lambda:res,destination,probe)
            self.assertEqual(pointer.read_bytes(),head);self.assertFalse(destination.exists())


if __name__=='__main__':unittest.main()
