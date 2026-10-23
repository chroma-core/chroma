"""Create local, unadmitted specifications. This script performs no cloud operations."""
import argparse
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import time

OPS = Path(__file__).resolve().parents[2] / 'operations'
BASE = '/home/ubuntu/spann-two-machine-v1'
WORKER = 'i-011ff180f03584f40'
PEER = 'i-00268d1e1dfea0357'
HASHES = {
    'two_machine_spann': '1cc7228294dd3418f06969549d94cbbd4d216cea1f262d8e2340dbcbfd31e902',
    'two_machine_reader': 'fb09190fc2adf6720bd6c13f8e1ae639351e66adce2a9885364a12724a4da14c',
    'reference': '8566a052e2fe0908188862c5ede2b98215664236198f6ab40a43193248016fb6',
}

def generate(a, now):
    if not re.fullmatch(r'two500k-[a-z0-9-]{1,40}', a.window_id):
        raise ValueError('Use a fresh two500k- window name.')
    if not now < a.job_deadline <= now + 590 or not a.job_deadline <= a.window_deadline - 10 or a.window_deadline > now + 1800:
        raise ValueError('Use fresh job/window deadlines within 590/1800 seconds.')
    nets = [ipaddress.ip_network(n) for n in ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16')]
    for value in (a.worker_ip, a.peer_ip):
        ip = ipaddress.ip_address(value)
        if ip.version != 4 or not any(ip in n for n in nets):
            raise ValueError('Fresh verified private IPv4 addresses are required.')
    if a.worker_ip == a.peer_ip:
        raise ValueError('Two distinct hosts are required.')
    receipts = [json.loads(Path(p).read_text()) for p in (a.worker_prefix_receipt, a.peer_prefix_receipt)]
    for receipt, instance in zip(receipts, (WORKER, PEER)):
        ident = receipt['identity']
        d = receipt['descriptor']
        if (ident['accountId'], ident['region'], ident['instanceId']) != ('079819354890', 'us-east-1', instance):
            raise ValueError('Prefix receipt has the wrong sandbox identity.')
        if (receipt['rows'], receipt['prefix_bytes'], d['rows'], d['dimensions'], d['stride_bytes'], d['offset_bytes'], d['first_id'], d['file_bytes']) != (500000,1536000000,500000,768,3072,0,0,1536000000):
            raise ValueError('Prefix descriptor does not describe the exact 500K input.')
        if d['sha256'] != receipt['prefix_sha256'] or not re.fullmatch(r'[0-9a-f]{64}', d['sha256']):
            raise ValueError('Prefix hash binding is invalid.')
        if d['path'] != BASE + '/epoch14/input/fineweb500k.f32':
            raise ValueError('Use the retained main-experiment prefix.')
    digest = receipts[0]['prefix_sha256']
    if digest != receipts[1]['prefix_sha256']:
        raise ValueError('The hosts have different input prefixes.')
    specs = {}
    for arm, mode, owner in [('reference','run',1),('distributed','run',1),('distributed','serve',2)]:
        template = OPS / ('window-epoch12/100k-reference-ackfix/run.json' if arm == 'reference' else 'window-epoch13/100k-distributed/run.json')
        v = json.loads(template.read_text())
        v.pop('profile_gate', None)
        name = f'500k-early-{arm}-owner{owner}'
        v.update(combined_review_passed=False, window_id=a.window_id, job_id=name, instance_id=WORKER if owner == 1 else PEER, mode=mode, deadline_unix=a.job_deadline, window_deadline_unix=a.window_deadline, workload_timeout_seconds=590, minimum_free_bytes=4*1024**3)
        v.update(binary=BASE + ('/epoch12b/build/reference.bin' if arm == 'reference' else '/epoch13/build/two_machine_spann'), binary_sha256=HASHES['reference' if arm == 'reference' else 'two_machine_spann'], reader=BASE+'/epoch13/build/two_machine_reader', reader_sha256=HASHES['two_machine_reader'])
        args = v['args']
        args.update({'--rows':500000, '--workers':32})
        output = BASE + '/' + a.window_id + '/' + name
        if arm == 'reference':
            args.update({'--input':receipts[0]['descriptor']['path'], '--input-sha256':digest, '--output-dir':output, '--initial-batch-rows':100000, '--batch-transition-rows':1000000, '--later-batch-rows':1000000})
        else:
            host, other = (a.worker_ip,a.peer_ip) if owner == 1 else (a.peer_ip,a.worker_ip)
            args.update({'--mode':mode,'--owner':owner,'--run':int.from_bytes(hashlib.sha256(a.window_id.encode()).digest()[:16],'big'),'--output':output,'--input-descriptor':BASE+'/epoch14/input/500k.json','--owner-budget':400000,'--first-owner-budget':80000,'--shutdown-at-unix':a.job_deadline,'--reader-binary':v['reader'],'--data-bind':host+':9100','--control-bind':host+':9101','--peer-data':other+':9100','--peer-control':other+':9101','--initial-batch':100000,'--transition':1000000,'--later-batch':1000000})
        v['provenance'] = {'arm':name,'replicates':1,'input_sha256':digest,'input_bytes':1536000000,'external_profile':False,'placement':'Redistribute after first100K; construct next400K with both owners.','admission':'Prepared locally only; exact operational review and fresh guards required.'}
        specs[name] = v
    return specs

if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    for field in ('window-id','worker-ip','peer-ip','worker-prefix-receipt','peer-prefix-receipt','output'):
        p.add_argument('--'+field, required=True)
    for field in ('window-deadline','job-deadline'):
        p.add_argument('--'+field, required=True, type=int)
    a = p.parse_args()
    specs = generate(a, time.time())
    output = Path(a.output)
    output.mkdir(parents=True, exist_ok=False)
    for name, spec in specs.items():
        body = (json.dumps(spec, indent=2, sort_keys=True)+'\n').encode()
        path = output / (name+'.json')
        with path.open('xb') as f:
            f.write(body)
        path.with_suffix('.json.sha256').write_text(hashlib.sha256(body).hexdigest()+'  '+path.name+'\n')
    print(json.dumps({'status':'prepared_not_admitted','output':str(output),'jobs':list(specs)}))
