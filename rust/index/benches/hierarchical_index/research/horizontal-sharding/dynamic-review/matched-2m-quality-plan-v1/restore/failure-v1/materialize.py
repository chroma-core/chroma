"""Restore exact version-pinned source objects, then write the declared row prefix.

The CLI is Linux-only and fixed to the admitted 2M plan. Pure functions accept
an explicit transport so local tests use tiny fake objects without AWS access.
"""
from __future__ import annotations
import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import struct
import sys
import time
import zlib

ACCOUNT = '079819354890'
BUCKET = 'spann-artifacts-079819354890-20261014-ecfcca4d'
PLAN_SHA256 = 'dd8efb14d493ef6b564b3c66d0f7198415b879c1aed932eb6ba3fe4581af8d53'
EXPECTED_ROWS, EXPECTED_DIMENSION, EXPECTED_BYTES = 2_000_000, 768, 6_144_000_000
BUFFER = 8 * 1024 * 1024


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, name, minimum=1):
    require(type(value) is int and value >= minimum, f'{name} must be an integer >= {minimum}')
    return value


def digest(data):
    return hashlib.sha256(data).hexdigest()


def hash_string(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def unique_json(data):
    def pairs(items):
        out = {}
        for key, value in items:
            require(key not in out, f'duplicate JSON field: {key}')
            out[key] = value
        return out
    return json.loads(data, object_pairs_hook=pairs,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON number')))


def fields(value, required, optional=()):
    require(type(value) is dict and set(required) <= set(value)
            and set(value) <= set(required) | set(optional), 'missing or unknown fields')


def safe_relative(value):
    require(type(value) is str and 0 < len(value) <= 1024 and '\\' not in value
            and all(ord(c) >= 32 for c in value), 'invalid logical source path')
    p = PurePosixPath(value)
    require(not p.is_absolute() and '..' not in p.parts and str(p) == value
            and p.suffix == '.f32', 'source path is not a canonical relative f32 path')


def validate_plan(plan):
    """Validate extents before side effects; no row count is inferred from paths."""
    fields(plan, ('status', 'observed_utc', 'rows', 'dimension', 'vector_bytes',
                  'whole_object_bytes', 'last_chunk_take_rows', 'sources', 'chunks', 'limits'))
    rows = integer(plan['rows'], 'rows')
    dimension = integer(plan['dimension'], 'dimension')
    require(rows <= 2_000_000 and dimension <= 4096, 'cohort cap exceeded')
    require(integer(plan['vector_bytes'], 'vector bytes') == rows * dimension * 4,
            'canonical extent differs')
    require(type(plan['chunks']) is list and 0 < len(plan['chunks']) <= 256, 'chunk count cap')
    at, whole, paths, objects = 0, 0, set(), set()
    for index, chunk in enumerate(plan['chunks']):
        fields(chunk, ('start_id', 'take_rows', 'full_chunk_rows', 'path', 'artifact'))
        require(integer(chunk['start_id'], 'start_id', 0) == at, 'chunk gap, overlap, or order mismatch')
        take = integer(chunk['take_rows'], 'take_rows')
        full = integer(chunk['full_chunk_rows'], 'full_chunk_rows')
        require(take <= full and (take == full or index == len(plan['chunks']) - 1),
                'only the last chunk can be partial')
        safe_relative(chunk['path'])
        require(chunk['path'] not in paths, 'duplicate logical source path')
        paths.add(chunk['path'])
        a = chunk['artifact']
        fields(a, ('bucket', 'bytes', 'crc32', 'encryption', 'expected_bucket_owner',
                   'key', 'sha256', 'transport_checksum_field', 'transport_checksum_value', 'version_id'))
        require(a['bucket'] == BUCKET and a['expected_bucket_owner'] == ACCOUNT,
                'artifact sandbox bucket/owner differs')
        require(hash_string(a['sha256']) and a['key'] == 'sha256/' + a['sha256'][:2] + '/' + a['sha256'],
                'artifact key is not its declared content hash')
        require(type(a['version_id']) is str and 0 < len(a['version_id']) <= 1024
                and a['version_id'] != 'null' and all(ord(c) >= 32 for c in a['version_id']),
                'artifact version is missing or invalid')
        require(integer(a['bytes'], 'whole object bytes') == full * dimension * 4,
                'whole object extent differs from rows')
        require(a['encryption'] == 'AES256' and a['transport_checksum_field'] == 'ChecksumCRC32'
                and a['transport_checksum_value'] == a['crc32'], 'artifact transport contract differs')
        try:
            require(len(base64.b64decode(a['crc32'], validate=True)) == 4, 'CRC32 length differs')
        except Exception as exc:
            raise ValueError('invalid CRC32 encoding') from exc
        identity = (a['bucket'], a['key'], a['version_id'])
        require(identity not in objects, 'duplicate source object')
        objects.add(identity)
        at += take
        whole += a['bytes']
    require(at == rows and whole == integer(plan['whole_object_bytes'], 'whole object bytes'),
            'declared total extent differs')
    require(integer(plan['last_chunk_take_rows'], 'last chunk take') == plan['chunks'][-1]['take_rows'],
            'last chunk extent differs')
    return rows * dimension * 4 + whole


class Guard:
    """Cooperative I/O guards; the CLI also arms its externally fixed deadline."""
    def __init__(self, deadline_unix, min_free_bytes, now=time.time, free=None):
        require(type(deadline_unix) in (int, float) and math.isfinite(deadline_unix), 'invalid deadline')
        self.deadline = deadline_unix
        self.reserve = integer(min_free_bytes, 'minimum free bytes')
        self.now = now
        self.free = free or (lambda p: shutil.disk_usage(p).free)
        self.check()

    def check(self, path=None, additional_bytes=0):
        if self.now() >= self.deadline:
            raise TimeoutError('fixed absolute materialization deadline reached')
        if path is not None:
            require(self.free(path) >= self.reserve + additional_bytes, 'disk reserve guard rejected write')


def verify_response(response, artifact):
    require(type(response.get('ContentLength')) is int
            and response['ContentLength'] == artifact['bytes'], 'object response length differs')
    require(response.get('VersionId') == artifact['version_id'], 'object response version differs')
    require(response.get('ServerSideEncryption') == 'AES256', 'object encryption differs')
    require(response.get('Metadata', {}).get('content-sha256') == artifact['sha256'],
            'object content metadata differs')
    require(response.get('ChecksumCRC32') == artifact['crc32'], 'object response CRC32 differs')


def publish_noclobber(partial, final):
    os.link(partial, final)
    partial.unlink()
    fd = os.open(final.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json_new(path, value):
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def restore_object(chunk, index, root, transport, guard):
    """Never slice an unverified object; failures retain their private partial."""
    artifact = chunk['artifact']
    guard.check(root, artifact['bytes'])
    head = transport.head(artifact)
    guard.check(root)
    verify_response(head, artifact)
    response = transport.get(artifact)
    body = response['Body']
    partial = root / f'{index:04d}-{artifact["sha256"]}.f32.partial'
    final = partial.with_suffix('')
    try:
        verify_response(response, artifact)
        sha, crc, count = hashlib.sha256(), 0, 0
        with partial.open('xb') as output:
            while True:
                guard.check(root)
                block = body.read(min(BUFFER, artifact['bytes'] - count + 1))
                guard.check(root)
                if not block:
                    break
                require(type(block) is bytes, 'transport body did not return bytes')
                count += len(block)
                require(count <= artifact['bytes'], 'object body exceeds whole object extent')
                guard.check(root, len(block))
                output.write(block)
                sha.update(block)
                crc = zlib.crc32(block, crc)
            output.flush()
            os.fsync(output.fileno())
        actual_crc = base64.b64encode(struct.pack('>I', crc & 0xffffffff)).decode()
        require(count == artifact['bytes'] and sha.hexdigest() == artifact['sha256']
                and actual_crc == artifact['crc32'], 'full object checksum or length differs')
        require(partial.stat().st_size == count, 'stored object length differs')
        publish_noclobber(partial, final)
        return final, dict(logical_path=chunk['path'], path=str(final), bytes=count,
                           sha256=sha.hexdigest(), crc32=actual_crc, version_id=artifact['version_id'])
    finally:
        body.close()


def materialize(plan, attempt, transport, guard, plan_sha256):
    """Transport-independent core. Only tests inject fake sources; CLI cannot."""
    required_bytes = validate_plan(plan)
    require(type(attempt) is Path and attempt.is_absolute(), 'attempt path must be absolute')
    require(not attempt.exists() and not attempt.is_symlink(), 'attempt path must be new')
    require(attempt.parent.is_dir(), 'attempt parent must already exist')
    guard.check(attempt.parent, required_bytes)
    attempt.mkdir(mode=0o700)
    objects = attempt / 'source-objects'
    objects.mkdir(mode=0o700)
    receipt = dict(format_version=1, status='incomplete', plan_sha256=plan_sha256,
                   deadline_unix=guard.deadline, min_free_bytes=guard.reserve, objects=[],
                   rows=plan['rows'], dimension=plan['dimension'], expected_bytes=plan['vector_bytes'])
    partial, final = attempt / 'canonical.f32.partial', attempt / 'canonical.f32'
    started = time.monotonic()
    try:
        transport.verify_bucket()
        guard.check(attempt)
        sha, size = hashlib.sha256(), 0
        with partial.open('xb') as output:
            for index, chunk in enumerate(plan['chunks']):
                source, proof = restore_object(chunk, index, objects, transport, guard)
                receipt['objects'].append(proof)
                # Read all source bytes again while taking only the declared prefix.
                # A changed source invalidates the private output before publication.
                full_sha = hashlib.sha256()
                selected = chunk['take_rows'] * plan['dimension'] * 4
                source_count, taken = 0, 0
                with source.open('rb') as stream:
                    while True:
                        guard.check(attempt)
                        block = stream.read(BUFFER)
                        guard.check(attempt)
                        if not block:
                            break
                        source_count += len(block)
                        require(source_count <= chunk['artifact']['bytes'], 'source grew before slicing')
                        full_sha.update(block)
                        take = block[:max(0, min(len(block), selected - taken))]
                        if take:
                            guard.check(attempt, len(take))
                            output.write(take)
                            sha.update(take)
                            taken += len(take)
                            size += len(take)
                require(source_count == chunk['artifact']['bytes']
                        and full_sha.hexdigest() == chunk['artifact']['sha256']
                        and taken == selected, 'verified source changed while slicing')
                proof['selected_rows'] = chunk['take_rows']
                proof['selected_bytes'] = taken
                write_json_new(attempt / f'object-{index:04d}.json', proof)
            output.flush()
            os.fsync(output.fileno())
        require(size == plan['vector_bytes'] and partial.stat().st_size == size,
                'final canonical extent differs')
        guard.check(attempt)
        publish_noclobber(partial, final)
        receipt.update(status='complete', canonical=dict(path=str(final), bytes=size, sha256=sha.hexdigest()),
                       elapsed_seconds=time.monotonic() - started,
                       scope='All full source objects retained. SHA computed over exact selected bytes; no float recoding.')
        write_json_new(attempt / 'receipt.json', receipt)
        return receipt
    except BaseException as exc:
        receipt.update(status='failed', error_type=type(exc).__name__, error=str(exc),
                       elapsed_seconds=time.monotonic() - started)
        # Preserve source files, partial canonical bytes, and completed object receipts.
        write_json_new(attempt / 'failure.json', receipt)
        raise


class LinuxS3Transport:
    """No retry or alternate source. Every call binds owner and exact version."""
    def __init__(self, expected_role, guard):
        require(sys.platform == 'linux', 'real restore is Linux-only')
        require(type(expected_role) is str and re.fullmatch('[A-Za-z0-9+=,.@_-]{1,128}', expected_role),
                'explicit sandbox instance role required')
        guard.check()
        import boto3
        from botocore.config import Config
        config = Config(connect_timeout=10, read_timeout=30, retries={'total_max_attempts': 1})
        self.s3 = boto3.client('s3', region_name='us-east-1', config=config)
        identity = boto3.client('sts', region_name='us-east-1', config=config).get_caller_identity()
        require(identity.get('Account') == ACCOUNT and identity.get('Arn', '').startswith(
            f'arn:aws:sts::{ACCOUNT}:assumed-role/{expected_role}/'), 'sandbox instance identity differs')
        self.identity = identity
        guard.check()

    def verify_bucket(self):
        response = self.s3.get_bucket_location(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT)
        require(response.get('LocationConstraint') in (None, 'us-east-1'), 'sandbox bucket region differs')

    def parameters(self, artifact):
        require(artifact['bucket'] == BUCKET and artifact['expected_bucket_owner'] == ACCOUNT,
                'sandbox bucket/owner differs')
        return dict(Bucket=BUCKET, ExpectedBucketOwner=ACCOUNT, Key=artifact['key'],
                    VersionId=artifact['version_id'], ChecksumMode='ENABLED')

    def head(self, artifact):
        return self.s3.head_object(**self.parameters(artifact))

    def get(self, artifact):
        return self.s3.get_object(**self.parameters(artifact))


def runtime(spec_path):
    require(sys.platform == 'linux', 'real restore is Linux-only')
    with Path(spec_path).open('rb') as stream:
        raw = stream.read(1024 * 1024 + 1)
    require(len(raw) <= 1024 * 1024, 'spec byte cap')
    spec = unique_json(raw)
    fields(spec, ('format_version', 'plan', 'attempt_dir', 'deadline_unix',
                  'min_free_bytes', 'expected_instance_role'))
    require(type(spec['format_version']) is int and spec['format_version'] == 1, 'unsupported format version')
    fields(spec['plan'], ('path', 'bytes', 'sha256'))
    require(spec['plan']['sha256'] == PLAN_SHA256, 'only the accepted canonical2M plan is admitted')
    guard = Guard(spec['deadline_unix'], spec['min_free_bytes'])
    # Task-local alarm uses the external fixed deadline, never a duration extension.
    def expired(signum, frame):
        raise TimeoutError('fixed absolute materialization deadline reached')
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, max(0.001, spec['deadline_unix'] - time.time()))
    try:
        with Path(spec['plan']['path']).open('rb') as stream:
            plan_raw = stream.read(1024 * 1024 + 1)
        require(len(plan_raw) <= 1024 * 1024 and type(spec['plan']['bytes']) is int
                and len(plan_raw) == spec['plan']['bytes'] and digest(plan_raw) == PLAN_SHA256,
                'pinned plan bytes differ')
        plan = unique_json(plan_raw)
        validate_plan(plan)
        require((plan['rows'], plan['dimension'], plan['vector_bytes'])
                == (EXPECTED_ROWS, EXPECTED_DIMENSION, EXPECTED_BYTES), 'accepted cohort differs')
        transport = LinuxS3Transport(spec['expected_instance_role'], guard)
        result = materialize(plan, Path(spec['attempt_dir']), transport, guard, PLAN_SHA256)
        # Completion includes exact materializer/spec provenance without credential material.
        result['spec_sha256'] = digest(raw)
        result['materializer_sha256'] = digest(Path(__file__).read_bytes())
        result['aws_identity'] = transport.identity
        write_json_new(Path(spec['attempt_dir']) / 'runtime-provenance.json', result)
        print(json.dumps(result))
        return result
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    runtime(parser.parse_args().spec)
