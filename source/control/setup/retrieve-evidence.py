"""Verify a bounded exact-file TAR stream supplied by the parent's transport.

This opens no network connection and runs no transfer command. The outer stream
contains only named evidence/receipt/result files. The inner evidence tar is
retained as bytes and never extracted here.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from orchestration_contract import (EPOCH, MAX_WIRE, SIDECARS, checked, descriptor,
                                    load_json, require, write_new)


def octal(field):
    value = field.strip(b'\0 ')
    require(not value or all(48 <= c <= 55 for c in value), 'non-octal TAR field')
    return int(value or b'0', 8)


class Wire:
    def __init__(self, source, cap, deadline):
        self.source, self.cap, self.deadline, self.count = source, cap, deadline, 0
    def read(self, count):
        require(time.time() < self.deadline, 'fixed retrieval deadline exhausted')
        require(count <= 1024*1024, 'unexpected oversized read request')
        data = self.source.read(min(count, self.cap-self.count+1))
        self.count += len(data)
        require(self.count <= self.cap, 'retrieval wire byte cap exceeded')
        require(time.time() < self.deadline, 'fixed retrieval deadline exhausted')
        return data
    def exact(self, count):
        result = bytearray()
        while len(result) < count:
            data = self.read(min(count-len(result), 1024*1024))
            require(bool(data), 'truncated retrieval stream')
            result.extend(data)
        return bytes(result)


def retrieve(spec, stream):
    manifest_path = checked(spec['manifest'])
    manifest = load_json(manifest_path)
    require(manifest.get('format_version') == 1 and manifest.get('epoch') == EPOCH, 'retrieval manifest scope')
    require(manifest['admission'] == spec['admission'], 'retrieval admission differs')
    approval = load_json(checked(spec['admission_copy']))
    require(all(spec['admission_copy'][k] == spec['admission'][k] for k in ('bytes','sha256'))
            and approval.get('decision') == 'approved' and approval.get('epoch') == EPOCH,
            'retained external admission copy differs')
    # Admission bytes may be retained locally with different file path; the externally
    # supplied exact descriptor is the trust anchor, not a new approval generated here.
    limits = manifest['limits']
    require(limits == spec['limits'] == approval['retrieval_limits']
            and type(limits['max_wire_bytes']) is int and 0 < limits['max_wire_bytes'] <= MAX_WIRE
            and type(limits['max_archive_bytes']) is int and 0 < limits['max_archive_bytes'] <= 68*1024**2
            and type(limits['max_sidecar_bytes']) is int and 0 < limits['max_sidecar_bytes'] <= 1024**2
            and approval['machine_stop_unix'] == manifest['machine_stop_unix'], 'retrieval limits differ')
    require(time.time() < spec['deadline_unix'] <= manifest['machine_stop_unix'], 'retrieval deadline differs')
    records = manifest['files']
    require(1 <= len(records) <= 6, 'retrieval file count cap')
    root = Path(manifest['root'])
    expected = {}
    for item in records:
        name = item['name']
        require(name in SIDECARS | {'evidence.tar'} and name not in expected, 'unexpected/duplicate retrieval file')
        require(Path(item['path']) == root/name and type(item['bytes']) is int and item['bytes'] >= 0,
                'retrieval source path/extent differs')
        cap = limits['max_archive_bytes'] if name == 'evidence.tar' else limits['max_sidecar_bytes']
        require(item['bytes'] <= cap, 'retrieval member cap exceeded')
        expected[name] = item
    require({'result.json','pre-collection.json'} <= set(expected), 'terminal result/status required')
    require(('evidence.tar' in expected) == ('evidence.tar.receipt.json' in expected), 'archive/receipt pairing differs')
    target = Path(spec['output'])
    require(target.is_absolute() and not target.exists(), 'fresh retrieval output required')
    target.mkdir(mode=0o700)
    write_new(target/'retrieval-request.json', dict(manifest=spec['manifest'], sources=records,
        limits=limits, deadline_unix=spec['deadline_unix'], mode='Exact listed flat regular TAR members only; no recursive copy'))
    wire = Wire(stream, limits['max_wire_bytes'], spec['deadline_unix'])
    seen = set()
    try:
        while True:
            header = wire.exact(512)
            if header == bytes(512):
                require(wire.exact(512) == bytes(512), 'TAR end marker differs')
                break
            require(octal(header[148:156]) == sum(header[:148]) + 8*32 + sum(header[156:]), 'TAR header checksum differs')
            require(header[156:157] in (b'0', b'\0') and header[345:500].strip(b'\0') == b'',
                    'only flat regular TAR files allowed; no PAX/link/directory/prefix')
            name = header[:100].split(b'\0',1)[0].decode('ascii')
            require(name in expected and name not in seen, 'unexpected or duplicate TAR member')
            size = octal(header[124:136])
            item = expected[name]
            require(size == item['bytes'], 'TAR member length differs from pinned manifest')
            h, remaining = hashlib.sha256(), size
            with (target/(name+'.partial')).open('xb') as output:
                while remaining:
                    block = wire.exact(min(remaining, 1024*1024))
                    output.write(block)
                    h.update(block)
                    remaining -= len(block)
                output.flush()
                os.fsync(output.fileno())
            require(h.hexdigest() == item['sha256'], 'retrieved file checksum differs')
            require(wire.exact((-size) % 512) == bytes((-size) % 512), 'TAR padding differs')
            seen.add(name)
        require(seen == set(expected), 'retrieval stream omits required members')
        # TAR writers can append zero record padding, never another archive/data.
        while True:
            tail = wire.read(1024*1024)
            if not tail:
                break
            require(not any(tail), 'nonzero trailing retrieval bytes')
        if 'evidence.tar' in seen:
            receipt = load_json(target/'evidence.tar.receipt.json.partial')
            require(receipt['archive']['bytes'] == expected['evidence.tar']['bytes']
                    and receipt['archive']['sha256'] == expected['evidence.tar']['sha256'], 'outside archive receipt differs')
        for name in expected:
            os.link(target/(name+'.partial'), target/name)
            (target/(name+'.partial')).unlink()
        result = dict(status='retrieved', wire_bytes=wire.count, files=records, manifest=spec['manifest'],
                      deadline_unix=spec['deadline_unix'], inner_archive_extracted=False)
        write_new(target/'retrieval-complete.json', result)
        return result
    except Exception as exc:
        write_new(target/'retrieval-failure.json', dict(error=repr(exc), wire_bytes=wire.count,
                                                       verified_members=sorted(seen)))
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    print(json.dumps(retrieve(load_json(args.spec), sys.stdin.buffer)))
