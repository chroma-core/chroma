"""Read-only identity/capacity check; never formats or mounts devices."""
import argparse,json,subprocess
from pathlib import Path

def check(spec):
    mount=Path(spec['mount']).resolve(strict=True)
    data=json.loads(subprocess.check_output(['findmnt','--json','--target',str(mount),'--output','SOURCE,TARGET,FSTYPE'],text=True))['filesystems']
    if len(data)!=1 or Path(data[0]['target']).resolve()!=mount or data[0]['fstype']!='ext4':
        raise ValueError('exact dedicated ext4 mount required')
    device=Path(data[0]['source']).resolve(strict=True)
    nodes=json.loads(subprocess.check_output(['lsblk','--json','--nodeps','--output','PATH,SERIAL,TYPE,SIZE','--bytes',str(device)],text=True))['blockdevices']
    if len(nodes)!=1:raise ValueError('one whole device required')
    node=nodes[0]
    if node['type']!='disk' or node['serial'].strip().replace('-','')!=spec['volume_id'].replace('-',''):
        raise ValueError('volume identity mismatch')
    if int(node['size'])!=64*1024**3:raise ValueError('volume size differs')
    if subprocess.check_output(['findmnt','-n','-o','SOURCE','/'],text=True).strip()==str(device):
        raise ValueError('root device prohibited')
    for entry in mount.iterdir():
        if entry.name!='lost+found' or not entry.is_dir() or any(entry.iterdir()):
            raise ValueError('new filesystem contains unexpected data')
    import shutil
    free=shutil.disk_usage(mount).free
    if free<spec['required_free_bytes']:raise ValueError('insufficient free space')
    return dict(volume_id=spec['volume_id'],device=str(device),mount=str(mount),free_bytes=free)
if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--spec',required=True)
    print(json.dumps(check(json.loads(Path(a.parse_args().spec).read_text()))))
