"""Fake-only admission/order checks for the parent's lifecycle draft.

Importing this test module runs no lifecycle code and creates no processes.
The draft is compiled from captured bytes only inside a fully mocked harness.
"""
import copy
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import time
import types
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
BOOT = 'vol-07951d3573187a716'
DATA = 'vol-0123456789abcdef0'
INSTANCE = 'i-00268d1e1dfea0357'


class MemoryPath:
    filesystem = None
    def __init__(self, path):
        self.path = str(path)
    def __str__(self):
        return self.path
    def __truediv__(self, child):
        return MemoryPath(str(PurePosixPath(self.path) / child))
    def resolve(self):
        return self
    @property
    def parent(self):
        return MemoryPath(str(PurePosixPath(self.path).parent))
    def exists(self):
        return self.path in self.filesystem.files
    def read_text(self):
        return self.filesystem.files[self.path]
    def write_text(self, text):
        self.filesystem.files[self.path] = text
        self.filesystem.events.append(('write', self.path))
        return len(text)
    def open(self, mode):
        if mode != 'a':
            raise AssertionError('Unexpected filesystem opening: ' + mode)
        self.filesystem.events.append(('open-log', self.path))
        return io.StringIO()


class FakeWorld:
    def __init__(self):
        self.files, self.events = {}, []
        self.clock = 10_000.0
        self.account = '079819354890'
        self.instance = dict(InstanceId=INSTANCE, State=dict(Name='stopped'),
            InstanceType='r6i.2xlarge', RootDeviceName='/dev/sda1',
            Placement=dict(AvailabilityZone='us-east-1a'),
            Tags=[dict(Key='purpose', Value='hierarchical-spann-sharding'),
                  dict(Key='role', Value='driver')],
            BlockDeviceMappings=[dict(DeviceName='/dev/sda1', Ebs=dict(VolumeId=BOOT, Status='attached')),
                                 dict(DeviceName='/dev/sdf', Ebs=dict(VolumeId=DATA, Status='attached'))])
        self.volume = dict(VolumeId=DATA, Size=64, VolumeType='gp3', Encrypted=True,
            State='in-use', AvailabilityZone='us-east-1a',
            Tags=[dict(Key='purpose', Value='hierarchical-spann-sharding'),
                  dict(Key='epoch', Value='matched-2m-dynamic-v1')],
            Attachments=[dict(InstanceId=INSTANCE, State='attached', Device='/dev/sdf')])
        self.ready = 'matching'
        self.pid = 4242
        self.child_exit = None

    def sleep(self, seconds):
        self.events.append(('sleep', seconds))
        self.clock += seconds

    def run(self, command, **kwargs):
        # Every subprocess.run is intercepted. No command is executed.
        if command[:5] != ['/opt/homebrew/bin/aws', '--profile', 'spann-sandbox', '--region', 'us-east-1']:
            raise AssertionError('Unexpected subprocess: ' + repr(command))
        args = command[5:]
        self.events.append(('aws', tuple(args)))
        if args == ['sts', 'get-caller-identity']:
            value = dict(Account=self.account, Arn='arn:fake:test')
        elif args == ['ec2', 'describe-instances', '--instance-ids', INSTANCE]:
            value = dict(Reservations=[dict(Instances=[copy.deepcopy(self.instance)])])
        elif args == ['ec2', 'describe-volumes', '--volume-ids', DATA]:
            value = dict(Volumes=[copy.deepcopy(self.volume)])
        elif args == ['ec2', 'start-instances', '--instance-ids', INSTANCE]:
            value = dict(StartingInstances=[dict(InstanceId=INSTANCE)])
        elif args == ['ec2', 'stop-instances', '--instance-ids', INSTANCE]:
            value = dict(StoppingInstances=[dict(InstanceId=INSTANCE)])
        else:
            raise AssertionError('Unexpected AWS operation: ' + repr(args))
        return types.SimpleNamespace(returncode=0, stdout=json.dumps(value), stderr='')

    def popen(self, command, **kwargs):
        # No real child/watchdog/session is created.
        self.events.append(('watchdog-spawn', tuple(command), kwargs.get('start_new_session')))
        if command[-2] != 'watch':
            raise AssertionError('Unexpected Popen operation')
        deadline = float(command[-1])
        if self.ready != 'missing':
            ready = dict(pid=self.pid, deadline=deadline)
            if self.ready == 'wrong-pid':
                ready['pid'] += 1
            if self.ready == 'wrong-deadline':
                ready['deadline'] -= 1
            self.files['/fake/watchdog-ready.json'] = json.dumps(ready)
            self.events.append(('READY', ready))
        return types.SimpleNamespace(pid=self.pid, poll=lambda: self.child_exit)

    def starts(self):
        return [x for x in self.events if x[0] == 'aws' and x[1][:2] == ('ec2', 'start-instances')]

    def execute(self, source, args=None, name='__main__'):
        MemoryPath.filesystem = self
        namespace = dict(__name__=name, __file__='/fake/lifecycle.py')
        error = None
        with patch('pathlib.Path', MemoryPath), patch.object(subprocess, 'run', self.run), \
             patch.object(subprocess, 'Popen', self.popen), patch.object(time, 'time', lambda: self.clock), \
             patch.object(time, 'sleep', self.sleep), patch.object(sys, 'argv', args or ['lifecycle.py', 'start', DATA]):
            try:
                exec(compile(source, '/fake/lifecycle.py', 'exec'), namespace)
            except BaseException as exc:
                error = exc
        return error


class LifecycleContract(unittest.TestCase):
    source = None
    observations = []
    @classmethod
    def setUpClass(cls):
        cls.source = (HERE / 'lifecycle.py').read_bytes()
        cls.source_sha256 = hashlib.sha256(cls.source).hexdigest()

    def observe(self, world, error):
        self.observations.append(dict(test=self.id(), error_type=type(error).__name__ if error else None,
                                      error=str(error) if error else None, events=world.events,
                                      files=dict(world.files)))

    def rejected(self, mutate):
        world = FakeWorld()
        mutate(world)
        error = world.execute(self.source)
        self.observe(world, error)
        self.assertIsNotNone(error, 'Invalid resource was admitted')
        self.assertEqual(world.starts(), [], 'AWS start occurs before invalid resource rejection')
        self.assertFalse(any(x[0] == 'watchdog-spawn' for x in world.events),
                         'Resource rejection should precede watchdog launch')

    def test_wrong_account_type_instance_state_and_identity_reject(self):
        for change in [lambda w: setattr(w, 'account', '000000000000'),
                       lambda w: w.instance.update(InstanceType='r6i.4xlarge'),
                       lambda w: w.instance.update(InstanceId='i-other'),
                       lambda w: w.instance['State'].update(Name='running')]:
            with self.subTest(change=repr(change)):
                self.rejected(change)

    def test_wrong_instance_tags_reject(self):
        for key in ['purpose', 'role']:
            with self.subTest(tag=key):
                self.rejected(lambda w: w.instance['Tags'].append(dict(Key=key, Value='wrong')))

    def test_wrong_boot_or_missing_extra_volume_reject(self):
        changes = [lambda w: w.instance['BlockDeviceMappings'][0]['Ebs'].update(VolumeId='vol-wrong-boot'),
                   lambda w: w.instance['BlockDeviceMappings'].pop(),
                   lambda w: w.instance['BlockDeviceMappings'].append(dict(DeviceName='/dev/sdg', Ebs=dict(VolumeId='vol-third')))]
        for change in changes:
            with self.subTest(change=repr(change)):
                self.rejected(change)

    def test_duplicate_extra_mapping_is_not_exactly_two_volumes(self):
        self.rejected(lambda w: w.instance['BlockDeviceMappings'].append(copy.deepcopy(w.instance['BlockDeviceMappings'][1])))

    def test_boot_volume_must_be_the_root_device(self):
        self.rejected(lambda w: w.instance.update(RootDeviceName='/dev/sdf'))

    def test_volume_response_id_must_match_requested_volume(self):
        self.rejected(lambda w: w.volume.update(VolumeId='vol-different-response'))

    def test_wrong_volume_size_type_encryption_and_zone_reject(self):
        for change in [lambda w: w.volume.update(Size=63),
                       lambda w: w.volume.update(VolumeType='gp2'),
                       lambda w: w.volume.update(Encrypted=False),
                       lambda w: w.volume.update(AvailabilityZone='us-east-1b')]:
            with self.subTest(change=repr(change)):
                self.rejected(change)

    def test_wrong_experiment_tags_reject(self):
        for key in ['purpose', 'epoch']:
            with self.subTest(tag=key):
                self.rejected(lambda w: w.volume['Tags'].append(dict(Key=key, Value='wrong')))

    def test_wrong_missing_duplicate_attachment_reject(self):
        changes = [lambda w: w.volume.update(Attachments=[]),
                   lambda w: w.volume['Attachments'][0].update(InstanceId='i-other'),
                   lambda w: w.volume['Attachments'][0].update(State='attaching'),
                   lambda w: w.volume['Attachments'].append(copy.deepcopy(w.volume['Attachments'][0]))]
        for change in changes:
            with self.subTest(change=repr(change)):
                self.rejected(change)

    def test_launch_epoch_reuse_rejects_before_any_aws(self):
        world = FakeWorld()
        world.files['/fake/start-attempt.json'] = '{}'
        error = world.execute(self.source)
        self.observe(world, error)
        self.assertIsNotNone(error)
        self.assertEqual(world.events, [])

    def test_bad_or_missing_ready_and_dead_watchdog_reject_before_start(self):
        for mode in ['missing', 'wrong-pid', 'wrong-deadline', 'dead']:
            world = FakeWorld()
            if mode == 'dead':
                world.child_exit = 1
            else:
                world.ready = mode
            error = world.execute(self.source)
            self.observe(world, error)
            with self.subTest(mode=mode):
                self.assertIsNotNone(error)
                self.assertEqual(world.starts(), [])
                self.assertNotIn('/fake/start-attempt.json', world.files)

    def test_accepted_ready_precedes_start_with_exact_fixed_1200s_deadline(self):
        world = FakeWorld()
        error = world.execute(self.source)
        self.observe(world, error)
        self.assertIsNone(error)
        self.assertEqual(len(world.starts()), 1)
        ready = json.loads(world.files['/fake/watchdog-ready.json'])
        attempt = json.loads(world.files['/fake/start-attempt.json'])
        self.assertEqual(ready, dict(pid=world.pid, deadline=11200.0))
        self.assertEqual(attempt['deadline_unix'], 11200.0)
        self.assertEqual(attempt['watchdog_pid'], world.pid)
        ready_at = next(i for i, e in enumerate(world.events) if e[0] == 'READY')
        record_at = world.events.index(('write', '/fake/start-attempt.json'))
        start_at = next(i for i, e in enumerate(world.events) if e in world.starts())
        self.assertLess(ready_at, record_at)
        self.assertLess(record_at, start_at)
        self.assertTrue(all(e[1][1] in ('describe-instances', 'describe-volumes', 'start-instances')
                            for e in world.events if e[0] == 'aws' and e[1][0] == 'ec2'))
        self.assertEqual(attempt['instance']['BlockDeviceMappings'], world.instance['BlockDeviceMappings'])

    def test_watch_branch_uses_fixed_deadline_and_only_stops_the_fixed_instance(self):
        world = FakeWorld()
        error = world.execute(self.source, ['lifecycle.py', 'watch', '11200'])
        self.observe(world, error)
        self.assertIsInstance(error, SystemExit)
        self.assertEqual(error.code, 0)
        self.assertEqual(world.clock, 11200.0)
        self.assertEqual(world.starts(), [])
        self.assertIn(('aws', ('ec2', 'stop-instances', '--instance-ids', INSTANCE)), world.events)
        self.assertFalse(any(e[0] == 'watchdog-spawn' for e in world.events))

    def test_import_does_not_execute_lifecycle_or_require_cli_arguments(self):
        for args in [['unittest'], ['lifecycle.py', 'start', DATA]]:
            world = FakeWorld()
            error = world.execute(self.source, args, name='imported_lifecycle')
            self.observe(world, error)
            with self.subTest(args=args):
                self.assertIsNone(error, 'Import requires command-line arguments')
                self.assertEqual(world.events, [], 'Import executes lifecycle operations')


if __name__ == '__main__':
    started = time.monotonic()
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(LifecycleContract))
    output = dict(passed=result.wasSuccessful(), tests=result.testsRun,
                  failures=len(result.failures), errors=len(result.errors),
                  elapsed_seconds=time.monotonic() - started,
                  source_sha256=LifecycleContract.source_sha256,
                  current_source_sha256=hashlib.sha256((HERE/'lifecycle.py').read_bytes()).hexdigest(),
                  scope='All AWS, process, sleep/time and filesystem writes replaced with in-memory fakes; no operational subprocess/watchdog/network runs',
                  observations=LifecycleContract.observations)
    # This is the only new result file written by the test harness.
    (HERE/'test_lifecycle_result_attempt2.json').write_text(json.dumps(output, indent=2)+'\n')
    (HERE/'test_lifecycle_source_attempt2.json').write_text(json.dumps(dict(sha256=LifecycleContract.source_sha256,
        source=LifecycleContract.source.decode()), indent=2)+'\n')
    raise SystemExit(not result.wasSuccessful())
