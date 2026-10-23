"""Materialize a fresh tiny fixture after worker release and source review.

This creates local files only. It neither contacts workers nor launches agents.
The resulting ten-minute clock starts here and cannot be renewed in place.
"""
import json
import os
from pathlib import Path
import secrets
import shutil
import time
from .dynamic_archive import export_checkpoint
from .dynamic_agent import actual_identity
from .dynamic_runtime import PRODUCER_SOURCE_SHA256,durable_directory,producer_bundle_sha256
from .dynamic_ssh import authenticated_key_map,write_known_hosts,ORIGINAL_RECEIPT_SHA256
from .tree_manifest import canonical_bytes,file_hash,_atomic_write


BINARY_SHA256='7e53d0ff0936546aa5d55ca653a2a524cfe23fd53c6134499ff130bbb0a0d158'


def prepare(root,evidence,original_receipt,ssh_key,binary='/mnt/spann/hierarchical_spann_shard-memory-v4',identity_reader=actual_identity,fault=None):
    root=Path(root);evidence=Path(evidence);original_receipt=Path(original_receipt)
    if not root.is_absolute() or root.resolve()!=root or not str(root).startswith('/mnt/spann/dynamic-twohost-v1/'):raise ValueError('fresh sandbox driver root required')
    identity=identity_reader()
    if identity.get('instanceId')!='i-00268d1e1dfea0357' or identity.get('accountId')!='079819354890' or identity.get('region')!='us-east-1':raise ValueError('actual sandbox driver differs')
    if file_hash(binary)!=BINARY_SHA256 or producer_bundle_sha256()!=PRODUCER_SOURCE_SHA256:raise ValueError('immutable driver binary or producer changed')
    authenticated=authenticated_key_map(evidence)
    if file_hash(original_receipt)!=ORIGINAL_RECEIPT_SHA256:raise ValueError('original key receipt differs')
    original=json.loads(original_receipt.read_text())
    root.mkdir(mode=0o700,parents=True,exist_ok=False)
    source=root/'source';source.mkdir(mode=0o700);current=Path(__file__).resolve().parents[2];source_hashes={}
    for path in sorted((current/'tools'/'hierarchical_sharding').rglob('*.py')):
        relative=path.relative_to(current);target=source/relative;target.parent.mkdir(parents=True,exist_ok=True)
        before=file_hash(path);shutil.copyfile(path,target)
        if file_hash(path)!=before or file_hash(target)!=before:raise ValueError('source changes while freezing')
        source_hashes[str(relative)]=before
    durable_directory(source);archive=root/'source.tar';archive_receipt=export_checkpoint(source,archive)
    copied=root/'host-key-evidence';copied.mkdir()
    for name in ('attestation-receipt-v1.json','caller-v1.json','worker0-attested-console-v2.json','worker1-attested-console-v2.json'):
        shutil.copyfile(evidence/name,copied/name)
    authenticated_key_map(copied)
    original_copy=root/'original-host-key-receipt.json';shutil.copyfile(original_receipt,original_copy)
    known=root/'known_hosts';known_sha=write_known_hosts(original_copy,ORIGINAL_RECEIPT_SHA256,authenticated,known,identity['instanceId'])
    run_id='dynamic-'+secrets.token_hex(8);workers={};started=time.time();deadline=started+600
    for index,worker in enumerate(('worker-a','worker-b')):
        row=original['rows'][index];remote=Path('/mnt/spann/dynamic-twohost-v1')/run_id/worker
        expected=dict(instanceId=row['instance_id'],accountId='079819354890',region='us-east-1',privateIp=row['host'])
        workers[worker]=dict(run_id=run_id+'-'+worker,host=row['host'],expected_identity=expected,source_root=str(remote/'source'),config_path=str(remote/'host-config.json'),output_path=str(remote/'host-supervisor'),ready_path=str(remote/'agent-ready.json'),known_hosts=str(known),known_hosts_sha256=known_sha,ssh_key=str(Path(ssh_key).resolve()),token=secrets.token_hex(32))
    if fault not in (None,'lease-expiry-before-topology'):raise ValueError('unknown fixture fault')
    config=dict(fault=fault,run_id=run_id,created_unix=started,deadline_unix=deadline,root=str(root/'execution'),driver_scope=str(root),driver_identity=identity,workers=workers,host_key_evidence_directory=str(copied),original_host_key_receipt=str(original_copy),source_archive=str(archive),source_archive_sha256=archive_receipt['sha256'],source_sha256=source_hashes,binary=str(binary),binary_sha256=BINARY_SHA256,producer_sha256=PRODUCER_SOURCE_SHA256)
    config_path=root/'config-private.json';_atomic_write(config_path,canonical_bytes(config));durable_directory(root)
    receipt=dict(status='materialized-not-launched',config_path=str(config_path),config_sha256=file_hash(config_path),run_id=run_id,created_unix=started,deadline_unix=deadline,source_archive_sha256=archive_receipt['sha256'],source_file_count=len(source_hashes),binary_sha256=BINARY_SHA256,worker_instances=[row['expected_identity']['instanceId'] for row in workers.values()],secret_files_excluded_from_evidence=['config-private.json','worker token files'])
    _atomic_write(root/'materialization.json',canonical_bytes(receipt));return receipt


def main():
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',required=True);parser.add_argument('--evidence',required=True);parser.add_argument('--original-receipt',required=True);parser.add_argument('--ssh-key',required=True);parser.add_argument('--fault',choices=['lease-expiry-before-topology']);args=parser.parse_args()
    print(json.dumps(prepare(args.root,args.evidence,args.original_receipt,args.ssh_key,fault=args.fault)))


if __name__=='__main__':main()
