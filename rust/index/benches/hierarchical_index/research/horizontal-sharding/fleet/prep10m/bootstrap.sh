#!/bin/bash
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
mkdir -p /var/lib/spann-prep-evidence
cat >/usr/local/sbin/spann-prep-hard-stop <<'STOP'
#!/bin/bash
set +e
# Preserve only bounded small proof files; scratch remains reconstructible.
timeout 20 python3 - <<'PYSMALL'
from pathlib import Path
import shutil,json,time
out=Path('/var/lib/spann-prep-evidence/final');out.mkdir(parents=True,exist_ok=True);used=0;copied=[];skipped=[]
for root in [Path('/mnt/spann/fineweb10m-reports-v1'),Path('/mnt/spann/preflight')]:
 if not root.exists():continue
 for p in root.rglob('*'):
  if not p.is_file() or p.suffix not in ('.json','.jsonl','.log'):continue
  size=p.stat().st_size
  if size>8*1024**2 or used+size>64*1024**2:skipped.append(str(p));continue
  dest=out/root.name/p.relative_to(root);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,dest);used+=size;copied.append(str(p))
(out/'stop-preservation.json').write_text(json.dumps(dict(utc_epoch=time.time(),copied=copied,skipped=skipped,bytes=used,policy='hard stop proceeds even if preservation fails; unarchived scratch is reconstructible')))
PYSMALL
timeout 5 sync
date -u +%FT%TZ >/var/lib/spann-prep-evidence/hard-stop-invoked
shutdown -h now
STOP
chmod +x /usr/local/sbin/spann-prep-hard-stop
remaining=$((1792032580-$(date -u +%s)))
if ((remaining<=0)); then /usr/local/sbin/spann-prep-hard-stop; exit 1; fi
systemd-run --unit=spann-prep-hard-stop --on-active="${remaining}s" /usr/local/sbin/spann-prep-hard-stop
echo 1792032580 >/var/lib/spann-prep-evidence/absolute-stop-epoch
apt-get update
apt-get install -y python3-venv python3-pip curl unzip jq nvme-cli
mkdir -p /mnt/spann
mapfile -t disks < <(lsblk -dnpo NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')
((${#disks[@]}==1))
scratch=${disks[0]}
test -z "$(lsblk -nro MOUNTPOINT "$scratch" | tr -d '[:space:]')"
test -z "$(wipefs -n "$scratch")"
mkfs.ext4 -F "$scratch"
mount "$scratch" /mnt/spann
chown ubuntu:ubuntu /mnt/spann /var/lib/spann-prep-evidence
sudo -u ubuntu python3 -m venv /mnt/spann/pilot-venv
sudo -u ubuntu /mnt/spann/pilot-venv/bin/pip install numpy==2.5.4 pyarrow==26.0.0
curl --proto '=https' --tlsv1.2 -fsS https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o /tmp/awscli.zip
unzip -q /tmp/awscli.zip -d /tmp/awscli-install
/tmp/awscli-install/aws/install
date -u +%FT%TZ >/var/lib/spann-prep-evidence/bootstrap-ready
