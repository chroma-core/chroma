"""Exact-host SSH arguments for a separately admitted sandbox fixture.

Admission consumes preserved control-plane evidence before creating a known
hosts file. This helper neither discovers keys nor starts remote processes.
"""
import base64
import hashlib
import ipaddress
from pathlib import Path
import re
from .tree_manifest import file_hash


def write_known_hosts(receipt,expected_sha256,authenticated_keys,target,driver_instance):
    """Match every receipt key to independently authenticated EC2 evidence.

    authenticated_keys is supplied by the control-plane admission step, not by
    ssh-keyscan or a query made through the connection being authenticated.
    """
    import json
    receipt=Path(receipt)
    if file_hash(receipt)!=expected_sha256:raise ValueError('host-key receipt digest differs')
    rows=json.loads(receipt.read_text())
    if rows.get('account')!='079819354890' or rows.get('driver_instance_id')!=driver_instance:raise ValueError('sandbox account or exact driver identity differs')
    if len(rows.get('rows',[]))!=2 or len({row['host'] for row in rows['rows']})!=2:raise ValueError('two distinct workers required')
    lines=[]
    for row in rows['rows']:
        host=str(ipaddress.ip_address(row['host']))
        parts=row['actual_ed25519_public_key'].split()
        if len(parts)<2 or parts[0]!='ssh-ed25519':raise ValueError('exact Ed25519 key required')
        blob=base64.b64decode(parts[1],validate=True)
        if len(blob)!=51 or blob[:19]!=b'\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20':raise ValueError('invalid Ed25519 public-key encoding')
        expected=authenticated_keys.get(row['instance_id'])
        if expected!=dict(account='079819354890',private_ip=host,key_type=parts[0],key_base64=parts[1]):raise ValueError('worker key lacks matching authenticated control-plane evidence')
        lines.append(host+' '+parts[0]+' '+parts[1]+'\n')
    target=Path(target)
    with target.open('x') as output:output.write(''.join(lines));output.flush();__import__('os').fsync(output.fileno())
    target.chmod(0o600)
    return file_hash(target)


def ssh_argv(host,key,known_hosts):
    ipaddress.ip_address(host)
    key=Path(key);known_hosts=Path(known_hosts)
    if not key.is_absolute() or not known_hosts.is_absolute() or not key.is_file() or not known_hosts.is_file():raise ValueError('absolute admitted SSH key paths required')
    if key.stat().st_mode&0o077:raise ValueError('private SSH key permissions required')
    return ['ssh','-i',str(key),'-o','BatchMode=yes','-o','IdentitiesOnly=yes','-o','StrictHostKeyChecking=yes','-o','HostKeyAlgorithms=ssh-ed25519','-o','UserKnownHostsFile='+str(known_hosts),'-o','GlobalKnownHostsFile=/dev/null','-o','ControlMaster=no','-o','ControlPath=none','-o','ForwardAgent=no','-o','ConnectTimeout=5','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2','ubuntu@'+host]


CONTROL_RECEIPT_SHA256='b728b8669078e7fec3adef7ec00cc113f2ebcf143555d67ddc42a7c0606e4b2c'
ORIGINAL_RECEIPT_SHA256='cc682f0109225ec1bbb92242b4ddb3488248b0f06fcbd594f140c27bd5016d2a'


def authenticated_key_map(evidence_directory):
    """Verify the preserved exact-epoch EC2 API receipts before using keys."""
    import json
    root=Path(evidence_directory);receipt_path=root/'attestation-receipt-v1.json'
    if file_hash(receipt_path)!=CONTROL_RECEIPT_SHA256:raise ValueError('control-plane admission receipt differs')
    receipt=json.loads(receipt_path.read_text());caller=root/'caller-v1.json'
    if receipt.get('status')!='passed' or receipt.get('original_receipt_sha256')!=ORIGINAL_RECEIPT_SHA256 or file_hash(caller)!=receipt['caller_sha256'] or json.loads(caller.read_text()).get('Account')!='079819354890':raise ValueError('authenticated AWS account provenance differs')
    result={}
    for row in receipt['rows']:
        path=root/Path(row['authenticated_console_file']).name
        if file_hash(path)!=row['authenticated_console_sha256']:raise ValueError('authenticated console bytes differ')
        console=json.loads(path.read_text())
        line='SPANN_HOSTKEY_ATTESTATION_V1 nonce='+row['nonce']+' key='+row['public_key']
        if console.get('InstanceId')!=row['instance_id'] or line not in console.get('Output','').splitlines():raise ValueError('fresh exact-instance console attestation is missing')
        kind,key=row['public_key'].split()
        result[row['instance_id']]=dict(account=receipt['account'],private_ip=row['private_ip'],key_type=kind,key_base64=key)
    return result
