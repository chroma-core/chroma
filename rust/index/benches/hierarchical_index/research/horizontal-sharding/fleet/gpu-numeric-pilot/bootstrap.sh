#!/bin/bash
set -euo pipefail
token=$(curl -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
identity=$(curl -fsS -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document)
python3 -c 'import json,sys;r=json.loads(sys.argv[1]);assert r["accountId"]=="079819354890" and r["instanceType"]=="g6.2xlarge" and r["imageId"]=="ami-0a24e16849196176a"' "$identity"
mkdir -p /var/lib/spann-gpu-results
cat >/usr/local/sbin/spann-gpu-preserve-stop <<'GUARD'
#!/bin/bash
mkdir -p /var/lib/spann-gpu-results
for name in gpu-runtime-evidence gpu-pilot-result-v1; do
 if test -d /mnt/spann/$name; then cp -a /mnt/spann/$name /var/lib/spann-gpu-results/ || logger 'SPANN GPU: preservation failed; stopping at cost deadline'; fi
done
sync
shutdown -h now
GUARD
chmod +x /usr/local/sbin/spann-gpu-preserve-stop
cat >/etc/systemd/system/spann-gpu-stop.service <<'SERVICE'
[Unit]
Description=Preserve bounded numeric pilot evidence and stop GPU
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/spann-gpu-preserve-stop
SERVICE
cat >/etc/systemd/system/spann-gpu-stop.timer <<'TIMER'
[Unit]
Description=Absolute thirty-minute sandbox GPU cost guard
[Timer]
OnCalendar=2026-10-14 23:32:47 UTC
Persistent=true
Unit=spann-gpu-stop.service
[Install]
WantedBy=timers.target
TIMER
systemctl daemon-reload
systemctl enable --now spann-gpu-stop.timer
if test "$(date +%s)" -ge "$(date -d '2026-10-14 23:32:47 UTC' +%s)"; then /usr/local/sbin/spann-gpu-preserve-stop;exit;fi
mkdir -p /mnt/spann
mapfile -t disks < <(lsblk -dnpo NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')
if ((${#disks[@]}==1)); then
 disk=${disks[0]};test -z "$(lsblk -nro MOUNTPOINT "$disk" | tr -d '[:space:]')";test -z "$(wipefs -n "$disk")";mkfs.ext4 -F "$disk";mount "$disk" /mnt/spann
fi
chown ubuntu:ubuntu /mnt/spann
mkdir -p /mnt/spann/gpu-runtime-evidence
chown ubuntu:ubuntu /mnt/spann/gpu-runtime-evidence
printf '%s\n' "$identity" >/mnt/spann/gpu-runtime-evidence/instance-identity.json
systemctl list-timers spann-gpu-stop.timer --no-pager >/mnt/spann/gpu-runtime-evidence/stop-guard.txt
nvidia-smi -q -x >/mnt/spann/gpu-runtime-evidence/bootstrap-nvidia-smi.xml
