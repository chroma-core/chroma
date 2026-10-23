#!/usr/bin/env python3
"""Test conditional creation on a new task key during an admitted cloud window."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import time
import generation_transfer as transfer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefix', required=True)
    parser.add_argument('--receipt', type=Path, required=True)
    parser.add_argument('--shutdown-at-unix', type=int, required=True)
    parser.add_argument('--profile')
    args = parser.parse_args()
    if not time.time() < args.shutdown_at_unix <= time.time() + 5400:
        raise ValueError('Fresh bounded execution window required')
    if args.receipt.exists():
        raise ValueError('Preserve the existing probe receipt')
    prefix = transfer.relative(args.prefix)
    key = prefix + '/conditional-publication-probe.json'
    s3, identity = transfer.client(args.profile)
    first = b'{"probe":"first immutable publication"}\n'
    second = b'{"probe":"conflicting publication"}\n'
    receipt = {'identity': identity, 'bucket': transfer.BUCKET, 'key': key, 'status': 'pending'}
    transfer.atomic_json(args.receipt, receipt)
    def put(body):
        transfer.check_deadline(args.shutdown_at_unix)
        return s3.put_object(Bucket=transfer.BUCKET, Key=key, Body=body, ContentLength=len(body), IfNoneMatch='*', ExpectedBucketOwner=transfer.ACCOUNT, ChecksumSHA256=base64.b64encode(hashlib.sha256(body).digest()).decode())
    created = put(first)
    version = created.get('VersionId')
    if not version or version == 'null':
        raise RuntimeError('Probe requires exact-version publication')
    receipt.update(first_version_id=version, first_bytes=len(first), first_sha256=hashlib.sha256(first).hexdigest())
    transfer.atomic_json(args.receipt, receipt)
    try:
        put(second)
    except Exception as error:
        response = getattr(error, 'response', {})
        if response.get('ResponseMetadata', {}).get('HTTPStatusCode') != 412:
            raise
        receipt['conflict_http_status'] = 412
    else:
        receipt['status'] = 'failed_conflicting_write_was_accepted'
        transfer.atomic_json(args.receipt, receipt)
        raise RuntimeError('Conditional publication accepted conflicting bytes')
    transfer.check_deadline(args.shutdown_at_unix)
    observed = s3.get_object(Bucket=transfer.BUCKET, Key=key, ExpectedBucketOwner=transfer.ACCOUNT)
    try:
        body = observed['Body'].read(1024)
    finally:
        observed['Body'].close()
    if body != first or observed.get('VersionId') != version:
        raise RuntimeError('Conflict changed the current immutable publication')
    receipt.update(status='conditional_creation_verified', current_version_id=observed['VersionId'], retained_storage_bytes=len(first))
    transfer.atomic_json(args.receipt, receipt)
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
