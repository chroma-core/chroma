"""Fetch exactly the accepted first100K FineWeb chunk, never the full2Mcohort."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import time
import zlib

import boto3
from botocore.config import Config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--destination', required=True, type=Path)
    parser.add_argument('--deadline', required=True, type=float)
    args = parser.parse_args()
    if args.deadline - time.time() < 180:
        raise TimeoutError('Prefix fetch cannot fit preservation reserve')
    destination = args.destination
    if destination.exists() or destination.with_suffix('.partial').exists():
        raise ValueError('Fresh prefix destination required')
    destination.parent.mkdir(parents=True, exist_ok=True)
    client = boto3.client('s3', region_name='us-east-1', config=Config(connect_timeout=5, read_timeout=15, retries={'max_attempts': 0}))
    response = client.get_object(Bucket='spann-artifacts-079819354890-20261014-ecfcca4d',
        Key='sha256/5b/5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c',
        VersionId='jUCrGweB7USCWUrqbH119b6ETuY2aETC', ExpectedBucketOwner='079819354890', ChecksumMode='ENABLED')
    if response['ContentLength'] != 307200000 or response['VersionId'] != 'jUCrGweB7USCWUrqbH119b6ETuY2aETC' or response.get('ServerSideEncryption') != 'AES256':
        raise ValueError('Pinned object response differs')
    partial = destination.with_suffix('.partial')
    digest = hashlib.sha256()
    crc = 0
    count = 0
    with partial.open('xb') as stream:
        while True:
            if time.time() >= args.deadline - 180:
                raise TimeoutError('Fetch preservation reserve reached')
            block = response['Body'].read(1048576)
            if not block:
                break
            stream.write(block)
            digest.update(block)
            crc = zlib.crc32(block, crc)
            count += len(block)
            if count > 307200000:
                raise ValueError('Unexpected object growth')
        stream.flush()
        os.fsync(stream.fileno())
    checksum = base64.b64encode((crc & 0xffffffff).to_bytes(4, 'big')).decode()
    if count != 307200000 or digest.hexdigest() != '5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c' or checksum != 'xEFdoQ==':
        raise ValueError('Whole prefix payload checksum differs; partial retained')
    partial.rename(destination)
    receipt = {'path': str(destination), 'bytes': count, 'sha256': digest.hexdigest(),
               'crc32': checksum, 'version_id': response['VersionId'], 'encryption': response['ServerSideEncryption'],
               'scope': 'Entire standalone first100K sourcechunk verified; same prefix used by canonical2M concatenation. No corpus downloaded to Mac.'}
    with destination.with_suffix('.receipt.json').open('x') as stream:
        json.dump(receipt, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())


if __name__ == '__main__':
    main()
