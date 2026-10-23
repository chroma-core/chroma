"""Pure protocol/route contracts; these tests do not start Rust or agents."""
import io
import queue
import unittest
from .dynamic_remote_write import RemoteWriteCoordinator, f32_bits, total_key
from .dynamic_write_session import PrivateWriteChannel


def node(parent, ident, leaf, distance):
    return dict(parent_id=parent,node_id=ident,is_leaf=leaf,distance=distance,distance_bits=f32_bits(distance))


class RemoteWriteContract(unittest.TestCase):
    def fixture(self, fail=False):
        calls=[]; journal=[]
        def call(owner, handle, command, row):
            calls.append((owner,command,row))
            op=command['op']
            if op=='root':return dict(op=op,root=node(0,0,False,0. if owner=='a' else 10.))
            if op=='expand':return dict(op=op,children=[node(0,1,True,3. if owner=='a' else -0. if row==18 else 0.)])
            if op=='count':return dict(op=op,count=2)
            if fail:raise EOFError('uncertain registration acknowledgement')
            return dict(op='registered',global_id=row,local_id=command['local_id'],leaf_id=command['leaf'],route_sha256=command['route_sha256'])
        info={o:dict(op='info',policy=dict(tau=1.,min=1,max=2,level_taus=[],level_min_pcts=[]),local_level_widths=[1,1],old_local_count=9) for o in ['a','b']}
        coordinator=RemoteWriteCoordinator(dict(a='s1',b='s2'),info,'frozen',dict(a=9,b=9),[dict(global_id=i,embedding_bits=[0]) for i in [18,19]],call,journal.append)
        return coordinator,calls,journal

    def test_actual_global_count_selects_across_upper_boundary_once(self):
        c,calls,journal=self.fixture();routes=c.run()
        self.assertEqual([r['subtree_id'] for r in routes],['b','b'])
        self.assertEqual([r['local_id'] for r in routes],[9,10])
        self.assertEqual(len([v for v in calls if v[1]['op']=='register']),2)
        self.assertTrue(all(v[1]['global_level_widths']==[2,2] for v in calls if v[1]['op']=='count'))
        self.assertLess(total_key(-0.),total_key(0.))
        with self.assertRaises(ValueError):c.run()

    def test_equal_distance_owner_tie_and_signed_zero_match_explicit_policy(self):
        c,_,_=self.fixture();original=c.call
        def tied(owner,handle,command,row):
            value=original(owner,handle,command,row)
            if command['op']=='expand':value['children']=[node(0,1,True,-0. if row==19 and owner=='b' else 0.)]
            return value
        c.call=tied
        self.assertEqual([r['subtree_id'] for r in c.run()],['a','b'])

    def test_uncertain_register_poisons_without_replay(self):
        c,calls,_=self.fixture(True)
        with self.assertRaises(EOFError):c.run()
        n=len(calls)
        with self.assertRaises(ValueError):c.run()
        self.assertEqual(len(calls),n)

    def test_every_child_is_validated_before_global_pruning(self):
        c,_,_=self.fixture();original=c.call
        def invalid(owner,handle,command,row):
            value=original(owner,handle,command,row)
            if command['op']=='expand' and owner=='a':value['children'].append(node(0,2,True,float('inf')))
            return value
        c.call=invalid
        with self.assertRaises(ValueError):c.run()

    def test_channel_wrong_scope_poison_prevents_later_write(self):
        c=PrivateWriteChannel.__new__(PrivateWriteChannel)
        c.closed=c.poisoned=c.terminal=False;c.sequence=0;c.timeout=.01
        c.spec=dict(session_id='s',attempt_id='a',authorization='auth',ownership_generation=2,base=dict(parent_sha256='p',owners=[dict(manifest_sha256='m')]),source_map_sha256='map')
        c.responses=queue.Queue();c.responses.put(b'{}\n')
        c.process=type('Process',(),dict(stdin=io.BytesIO()))()
        with self.assertRaises(ValueError):c.call(dict(op='info'))
        size=len(c.process.stdin.getvalue())
        with self.assertRaises(ValueError):c.call(dict(op='info'))
        self.assertEqual(len(c.process.stdin.getvalue()),size)
