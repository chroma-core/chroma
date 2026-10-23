"""Tiny FAKE-source correctness checks; no boto client, credentials, or transfers."""
import base64
import copy
import io
import json
from pathlib import Path
import struct
import tempfile
import time
import unittest
from unittest.mock import patch
import zlib
import materialize as m


def descriptor(data, version):
    sha = m.digest(data)
    crc = base64.b64encode(struct.pack('>I', zlib.crc32(data) & 0xffffffff)).decode()
    return dict(bucket=m.BUCKET, expected_bucket_owner=m.ACCOUNT, key='sha256/' + sha[:2] + '/' + sha,
                bytes=len(data), sha256=sha, version_id=version, crc32=crc, encryption='AES256',
                transport_checksum_field='ChecksumCRC32', transport_checksum_value=crc)


def fixture():
    data = [struct.pack('<6f', -0., 1., 2., 3., 4., 5.),
            struct.pack('<8f', 6., 7., 8., 9., 10., 11., 12., 13.)]
    chunks = [dict(start_id=0, take_rows=3, full_chunk_rows=3, path='cohort/a.f32',
                   artifact=descriptor(data[0], 'version-a')),
              dict(start_id=3, take_rows=2, full_chunk_rows=4, path='cohort/b.f32',
                   artifact=descriptor(data[1], 'version-b'))]
    plan = dict(status='fake source test only', observed_utc='not live', rows=5, dimension=2,
                vector_bytes=40, whole_object_bytes=56, last_chunk_take_rows=2,
                sources={}, chunks=chunks, limits='tiny fake source, not runtime cohort')
    return plan, data, data[0] + data[1][:16]


class FakeTransport:
    def __init__(self, plan, data):
        self.plan, self.data = plan, data
        self.calls, self.bodies = [], []
        self.bucket_ok = True
        self.head_patch, self.get_patch = {}, {}
        self.replace_body = {}

    def verify_bucket(self):
        self.calls.append(('bucket', m.BUCKET, m.ACCOUNT))
        m.require(self.bucket_ok, 'fake bucket owner rejected')

    def response(self, a):
        return dict(ContentLength=a['bytes'], VersionId=a['version_id'],
                    ServerSideEncryption='AES256', Metadata={'content-sha256': a['sha256']},
                    ChecksumCRC32=a['crc32'])

    def index(self, a):
        return next(i for i, c in enumerate(self.plan['chunks']) if c['artifact'] == a)

    def head(self, a):
        self.calls.append(('head', copy.deepcopy(a)))
        return dict(self.response(a), **self.head_patch)

    def get(self, a):
        i = self.index(a)
        self.calls.append(('get', copy.deepcopy(a)))
        body = io.BytesIO(self.replace_body.get(i, self.data[i]))
        self.bodies.append(body)
        return dict(self.response(a), **self.get_patch, Body=body)


class MaterializerContract(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.plan, self.data, self.expected = fixture()
        self.transport = FakeTransport(self.plan, self.data)
        self.guard = m.Guard(time.time() + 60, 1)

    def tearDown(self):
        self.temp.cleanup()

    def run_core(self, plan=None, transport=None, guard=None, name='attempt'):
        return m.materialize(plan or self.plan, self.root / name, transport or self.transport,
                             guard or self.guard, m.digest(json.dumps(self.plan).encode()))

    def assert_failed_private(self):
        out = self.root / 'attempt'
        self.assertFalse((out / 'canonical.f32').exists())
        self.assertEqual(json.loads((out / 'failure.json').read_text())['status'], 'failed')
        self.assertTrue(all(x.closed for x in self.transport.bodies))

    def test_exact_order_extent_full_source_retention_and_negative_zero(self):
        receipt = self.run_core()
        self.assertEqual((self.root / 'attempt/canonical.f32').read_bytes(), self.expected)
        self.assertEqual(receipt['canonical']['bytes'], 40)
        self.assertEqual(receipt['canonical']['sha256'], m.digest(self.expected))
        self.assertEqual(self.expected[:4], struct.pack('<f', -0.))
        self.assertEqual(receipt['status'], 'complete')
        for i, proof in enumerate(receipt['objects']):
            self.assertEqual(Path(proof['path']).read_bytes(), self.data[i])
            self.assertEqual(proof['sha256'], self.plan['chunks'][i]['artifact']['sha256'])
        self.assertEqual([x[0] for x in self.transport.calls], ['bucket', 'head', 'get', 'head', 'get'])
        self.assertTrue(all(x.closed for x in self.transport.bodies))
        with self.assertRaises(ValueError):
            self.run_core()
        self.assertEqual((self.root / 'attempt/canonical.f32').read_bytes(), self.expected)

    def test_plan_rejects_gaps_order_bool_path_extent_and_owner_before_transport(self):
        changes = [lambda p: p['chunks'][1].update(start_id=4),
                   lambda p: p['chunks'].reverse(),
                   lambda p: p['chunks'][0].update(start_id=False),
                   lambda p: p['chunks'][0].update(path='../bad.f32'),
                   lambda p: p['chunks'][0].update(path='/bad.f32'),
                   lambda p: p['chunks'][1].update(path='cohort/a.f32'),
                   lambda p: p.update(vector_bytes=39),
                   lambda p: p['chunks'][0]['artifact'].update(bytes=23),
                   lambda p: p['chunks'][0]['artifact'].update(expected_bucket_owner='000000000000'),
                   lambda p: p['chunks'][0]['artifact'].update(version_id='null'),
                   lambda p: p['chunks'][0]['artifact'].update(key='unversioned/path'),
                   lambda p: p['chunks'][0].update(take_rows=2)]
        for change in changes:
            plan = copy.deepcopy(self.plan)
            change(plan)
            with self.subTest(change=repr(change)), self.assertRaises(ValueError):
                self.run_core(plan=plan)
            self.assertFalse((self.root / 'attempt').exists())
        self.assertEqual(self.transport.calls, [])

    def test_bucket_and_response_version_length_metadata_checksum_fail_closed(self):
        for i, field in enumerate(['VersionId', 'ContentLength', 'Metadata', 'ChecksumCRC32', 'ServerSideEncryption']):
            t = FakeTransport(self.plan, self.data)
            t.head_patch = {field: {'content-sha256': '0' * 64} if field == 'Metadata' else 'wrong'}
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.run_core(transport=t, name='response' + str(i))
            self.assertNotIn('get', [c[0] for c in t.calls])
        self.transport.bucket_ok = False
        with self.assertRaises(ValueError):
            self.run_core()
        self.assert_failed_private()
        self.assertEqual(len(self.transport.calls), 1)

    def test_get_version_mismatch_closes_body_before_writing_source(self):
        self.transport.get_patch = {'VersionId': 'unapproved-version'}
        with self.assertRaises(ValueError):
            self.run_core()
        self.assert_failed_private()
        self.assertEqual(list((self.root / 'attempt/source-objects').iterdir()), [])

    def test_full_hash_checks_discarded_tail_and_preserves_partial_failure(self):
        # Corruption is outside the taken last prefix, and still must reject.
        self.transport.replace_body[1] = self.data[1][:-1] + bytes([self.data[1][-1] ^ 1])
        with self.assertRaises(ValueError):
            self.run_core()
        self.assert_failed_private()
        self.assertEqual((self.root / 'attempt/canonical.f32.partial').read_bytes(), self.data[0])
        self.assertTrue(any(p.name.endswith('.partial') for p in (self.root / 'attempt/source-objects').iterdir()))
        first = json.loads((self.root / 'attempt/object-0000.json').read_text())
        self.assertEqual(Path(first['path']).read_bytes(), self.data[0])

    def test_short_and_oversized_body_fail_closed(self):
        for i, data in enumerate([self.data[0][:-1], self.data[0] + b'x']):
            t = FakeTransport(self.plan, self.data)
            t.replace_body[0] = data
            with self.subTest(length=len(data)), self.assertRaises(ValueError):
                self.run_core(transport=t, name='length' + str(i))
            self.assertTrue(all(x.closed for x in t.bodies))
            self.assertFalse((self.root / ('length' + str(i)) / 'canonical.f32').exists())

    def test_changed_source_on_slicing_is_not_published(self):
        original = m.restore_object
        def changed(*args):
            source, proof = original(*args)
            source.write_bytes(b'!' + source.read_bytes()[1:])
            return source, proof
        with patch.object(m, 'restore_object', changed), self.assertRaises(ValueError):
            self.run_core()
        self.assert_failed_private()

    def test_external_deadline_disk_guard_and_linux_gate(self):
        with self.assertRaises(TimeoutError):
            m.Guard(time.time() - 1, 1)
        guard = m.Guard(time.time() + 60, 100, free=lambda _: 155)
        with self.assertRaises(ValueError):
            self.run_core(guard=guard)
        self.assertFalse((self.root / 'attempt').exists())
        with patch.object(m.sys, 'platform', 'darwin'), self.assertRaises(ValueError):
            m.runtime(self.root / 'does-not-exist.json')
        with patch.object(m.sys, 'platform', 'darwin'), self.assertRaises(ValueError):
            m.LinuxS3Transport('test-role', self.guard)

    def test_deadline_during_body_read_closes_source_without_publication(self):
        clock = [10.]
        guard = m.Guard(20., 1, now=lambda: clock[0])
        original = self.transport.get
        def get(artifact):
            response = original(artifact)
            body = response['Body']
            read = body.read
            def expired_read(size):
                result = read(size)
                clock[0] = 21.
                return result
            body.read = expired_read
            return response
        self.transport.get = get
        with self.assertRaises(TimeoutError):
            self.run_core(guard=guard)
        self.assert_failed_private()
        self.assertEqual((self.root / 'attempt/canonical.f32.partial').stat().st_size, 0)

    def test_transport_owner_version_parameters_and_strict_json(self):
        transport = object.__new__(m.LinuxS3Transport)
        a = self.plan['chunks'][0]['artifact']
        p = transport.parameters(a)
        self.assertEqual(p, dict(Bucket=m.BUCKET, ExpectedBucketOwner=m.ACCOUNT,
                                Key=a['key'], VersionId=a['version_id'], ChecksumMode='ENABLED'))
        for data in [b'{"x":1,"x":2}', b'{"x":NaN}']:
            with self.assertRaises(ValueError):
                m.unique_json(data)


if __name__ == '__main__':
    unittest.main(verbosity=2)
