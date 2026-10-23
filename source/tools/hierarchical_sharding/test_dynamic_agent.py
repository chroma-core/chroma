"""The worker reserves reader generations before opening persistent children."""
import threading
import unittest
from unittest.mock import patch
from .dynamic_agent import Agent


class GenerationAdmission(unittest.TestCase):
    def agent(self):
        agent=Agent.__new__(Agent)
        agent.generations={};agent.lock=threading.RLock();agent.worker_id='worker';agent.binary='/unused'
        return agent
    def test_preparing_generation_rejects_queries_pins_and_retirement(self):
        agent=self.agent();agent.generations['pending']=dict(host=None,pins=set(),inflight=0)
        for operation in ('pin','batch','probe-generation','retire'):
            with self.subTest(operation=operation),self.assertRaisesRegex(ValueError,'not ready'):
                agent.execute(dict(op=operation,generation_id='pending',reader_id='reader',payload={}))
    def test_duplicate_reservation_does_not_open_readers(self):
        agent=self.agent();agent.generations['pending']=dict(host=None,pins=set(),inflight=0)
        with patch('tools.hierarchical_sharding.dynamic_agent.PinnedReader') as reader:
            with self.assertRaisesRegex(ValueError,'fresh bounded'):
                agent.execute(dict(op='prepare-generation',generation_id='pending',subtrees=[dict(subtree_id='a')]))
            reader.assert_not_called()
    def test_failed_preparation_closes_host_and_releases_reservation(self):
        agent=self.agent();agent.metadata=lambda _: (_ for _ in ()).throw(ValueError('missing replica'))
        with patch('tools.hierarchical_sharding.dynamic_agent.Host.close') as close:
            with self.assertRaisesRegex(ValueError,'missing replica'):
                agent.execute(dict(op='prepare-generation',generation_id='next',tree_sha256='x',subtrees=[dict(subtree_id='a',replica_id='absent')]))
            close.assert_called_once()
        self.assertEqual(agent.generations,{})


if __name__=='__main__':unittest.main()

class ReplicaAdmission(unittest.TestCase):
    def test_build_import_race_has_one_namespace_owner(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as temp:
            agent=Agent.__new__(Agent);agent.root=Path(temp);agent.replicas={};agent.reservations={};agent.lock=threading.RLock()
            barrier=threading.Barrier(2);outcomes=[]
            def attempt(operation):
                barrier.wait()
                try:agent.reserve_replica('same',operation);outcomes.append('won')
                except ValueError:outcomes.append('rejected')
            threads=[threading.Thread(target=attempt,args=(operation,)) for operation in ('build','import')]
            for thread in threads:thread.start()
            for thread in threads:thread.join(2);self.assertFalse(thread.is_alive())
            self.assertEqual(sorted(outcomes),['rejected','won'])
            agent.reservations['same']['state']='failed'
            with self.assertRaisesRegex(ValueError,'reservation'):agent.reserve_replica('same','retry')
    def test_duplicate_import_reservation_and_count_are_bounded(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as temp:
            agent=Agent.__new__(Agent);agent.root=Path(temp);agent.replicas={};agent.reservations={};agent.lock=threading.RLock()
            agent.reserve_replica('first','import')
            with self.assertRaises(ValueError):agent.reserve_replica('first','import')
            for index in range(31):agent.reserve_replica('replica-'+str(index),'build')
            with self.assertRaises(ValueError):agent.reserve_replica('excess','import')
