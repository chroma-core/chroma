"""Race separately authorized placement/topology publications in two processes."""
from pathlib import Path
import queue
import time
from .dynamic_deployment import content_hash,load_deployment,run_transaction
from .dynamic_remote import AgentClient,RemoteGeneration
from .tree_manifest import canonical_bytes,_atomic_write


def child(pointer,record,generations,specs,entries,barrier,stop,journal,result_queue):
    from .dynamic_twohost_control import control
    clients={worker:AgentClient(**spec) for worker,spec in specs.items()}
    serving=RemoteGeneration.__new__(RemoteGeneration);serving.record=record;serving.clients=clients;serving.generations=generations;serving.closed=False
    serving.subtree_workers={sid:owner['worker_id'] for sid,owner in record['owners'].items()}
    outcome=dict(candidate_sha256=content_hash(record),operation=record['operation'])
    def health():
        if stop.is_set():raise ValueError('parent observed peer health failure')
        for entry in entries:
            if control('poll',entry,5).get('status')!='running':raise ValueError('worker terminal blocks racing publication')
    try:
        barrier.wait(timeout=20);health()
        parent=load_deployment(pointer)
        # A contender never substitutes a newer parent after losing the race.
        if content_hash(parent)!=record['parent_sha256']:raise ValueError('deployment compare-and-swap conflict')
        receipt=run_transaction(pointer,parent,lambda _:record,serving.probe,journal,health=health)
        outcome.update(status='published',receipt=receipt)
    except BaseException as error:outcome.update(status='rejected',error=type(error).__name__+': '+str(error))
    _atomic_write(Path(str(journal)+'.outcome.json'),canonical_bytes(outcome));result_queue.put(outcome)


def run(pointer,candidates,clients,entries,output,health):
    import multiprocessing
    output=Path(output);output.mkdir(exist_ok=False)
    if len(candidates)!=2 or {record['operation'] for record,_ in candidates}!={'placement','topology'}:raise ValueError('one placement and one topology contender required')
    parents={record['parent_sha256'] for record,_ in candidates}
    if len(parents)!=1 or parents!={content_hash(load_deployment(pointer))}:raise ValueError('both contenders must retain the same current authorization')
    context=multiprocessing.get_context('spawn');barrier=context.Barrier(3);stop=context.Event();results=context.Queue();processes=[]
    specs={worker:dict(endpoint=client.endpoint,token=client.token,run_id=client.run_id,timeout=client.timeout,worker_id=client.worker_id,instance_id=client.instance_id) for worker,client in clients.items()}
    entries=[{key:value for key,value in entry.items() if key!='token'} for entry in entries]
    try:
        for index,(record,serving) in enumerate(candidates):
            process=context.Process(target=child,args=(str(pointer),record,serving.generations,specs,entries,barrier,stop,str(output/('contender-'+str(index)+'.json')),results));process.start();processes.append(process)
        barrier.wait(timeout=20);deadline=time.monotonic()+60
        while any(process.is_alive() for process in processes):
            try:health()
            except BaseException:stop.set();raise
            if time.monotonic()>deadline:stop.set();raise TimeoutError('bounded separate-process publication race expired')
            for process in processes:process.join(.1)
        if any(process.exitcode!=0 for process in processes):raise ValueError('contender process failed without a classified outcome')
        outcomes=[results.get(timeout=2) for _ in processes]
        winners=[row for row in outcomes if row['status']=='published'];losers=[row for row in outcomes if row['status']=='rejected']
        if len(winners)!=1 or len(losers)!=1 or 'compare-and-swap' not in losers[0].get('error',''):raise AssertionError('race must admit one winner and reject one stale parent')
        current=load_deployment(pointer)
        if content_hash(current)!=winners[0]['candidate_sha256']:raise AssertionError('authority head differs from durable winner')
        report=dict(status='passed',outcomes=outcomes,winner_sha256=content_hash(current),separate_processes=len(processes))
        _atomic_write(output/'race.json',canonical_bytes(report));return current,report
    finally:
        stop.set()
        for process in processes:
            if process.is_alive():process.terminate()
            process.join(2)
            if process.is_alive():process.kill();process.join(2)
        results.close();results.join_thread()
