"""Append one frozen ownership slot while retaining the old committed tree."""
import argparse
import json
import pathlib
import threading
import time
from .subtree_pilot import build_case,start_hosts
from .subtree_transport import TreeCoordinator
from .tree_manifest import publish,load_published,content_hash


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--old-case',required=True);p.add_argument('--prepared',required=True);p.add_argument('--case',required=True);p.add_argument('--hosts',nargs='+',required=True);p.add_argument('--port',type=int,required=True);p.add_argument('--query',required=True);a=p.parse_args()
    old_path=pathlib.Path(a.old_case);old_case=json.loads((old_path/'case.json').read_text());old=TreeCoordinator.from_files(old_case['tree'],old_path/'placement-two-hosts.json');query=json.loads(pathlib.Path(a.query).read_text().splitlines()[0])['vector']
    before=old.query(query,10,beam_width=2,candidate_budget=1000)
    readings=[];stop=threading.Event()
    def read_old():
        while not stop.is_set() and len(readings)<200:
            started=time.time()
            try:
                row=old.query(query,10,beam_width=2,candidate_budget=1000)
                readings.append(dict(started_unix=started,finished_unix=time.time(),status='ok',tree_sha256=row['tree_sha256'],generation=row['generation'],ids=row['ids']))
            except Exception as error:
                readings.append(dict(started_unix=started,finished_unix=time.time(),status='error',error=str(error)));break
            stop.wait(.025)
    thread=threading.Thread(target=read_old);thread.start()
    try:
        built=build_case(a.prepared,a.case,a.hosts,generation=2,resume_case=old_path/'case.json',threads=1)
        stop.set();thread.join(35)
        if thread.is_alive() or not readings or any(r['status']!='ok' or r['tree_sha256']!=before['tree_sha256'] or r['ids']!=before['ids'] for r in readings):raise AssertionError('old tree failed or changed during append')
        case=pathlib.Path(a.case);(case/'old-readings.json').write_text(json.dumps(readings)+'\n');new_tree=json.loads(pathlib.Path(built['tree']).read_text());old_tree=json.loads(pathlib.Path(old_case['tree']).read_text())
        changed=[s['subtree_id'] for s in new_tree['subtrees'] if s['generation']==2]
        if len(changed)!=1:raise AssertionError('sparse append must change one lower snapshot')
        unchanged=[s for s in new_tree['subtrees'] if s['generation']==1]
        for row in unchanged:
            previous=next(s for s in old_tree['subtrees'] if s['subtree_id']==row['subtree_id'])
            if row['snapshot']['sha256']!=previous['snapshot']['sha256']:raise AssertionError('unchanged owner snapshot was rewritten')
        start_hosts(a.case,a.port)
        current=TreeCoordinator.from_files(built['tree'],case/'placement-two-hosts.json')
        try:
            current.verify_roots()
            pointer=publish(old_path/'current.json',new_tree,expected_old_hash=old_case['tree_sha256'],base=case)
            committed,_=load_published(old_path/'current.json')
            if content_hash(committed)!=built['tree_sha256']:raise AssertionError('published tree differs from append result')
            new=current.query(query,10001,beam_width=None,candidate_budget=10001)
            if set(new['ids'])!=set(range(10001)):raise AssertionError('appended tree does not expose the complete dense global ID prefix')
            old_again=old.query(query,10,beam_width=2,candidate_budget=1000)
            if old_again['ids']!=before['ids'] or old_again['tree_sha256']!=before['tree_sha256']:raise AssertionError('old tree changed after publication')
            report=dict(status='passed',old_tree_sha256=old_case['tree_sha256'],new_tree_sha256=built['tree_sha256'],changed_subtrees=changed,lower_generations={s['subtree_id']:s['generation'] for s in new_tree['subtrees']},old_readings=readings,pointer=pointer,checks=['frozen upper model and boundary','one changed owner','unchanged lower snapshot reused','old tree serves throughout append and after publication','complete10001 global IDs through exhaustive traversal'],new_result=new)
            (case/'append-check.json').write_text(json.dumps(report)+'\n');print(json.dumps({k:v for k,v in report.items() if k not in ('new_result','old_readings','pointer')}))
        finally:current.close()
    finally:
        stop.set();thread.join(35);old.close()

if __name__=='__main__':main()
