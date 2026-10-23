#!/usr/bin/env bash
set -euo pipefail
sudo systemd-run --unit=spann-100k-owner32-run2-stop --on-calendar='@1792282112' --timer-property=AccuracySec=1s /sbin/shutdown -h now
sudo systemctl is-active spann-100k-owner32-run2-stop.timer
sudo systemctl show spann-100k-owner32-run2-stop.timer -p NextElapseUSecRealtime
# Retire only prior closed task timers after new independent guard is active.
sudo systemctl stop owner3m-20261017-stop.timer 2>/dev/null || true
readonly token=$(curl --noproxy '*' -fsS --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl --noproxy '*' -fsS --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document
lscpu
lscpu -p=CPU,CORE,SOCKET
lsblk -o NAME,SERIAL,SIZE,FSTYPE,MOUNTPOINTS
df -B1 /
free -b
ls -ld /mnt/spann /mnt/spann-matched-2m-dynamic-v1 /home/ubuntu/.rustup/toolchains/1.92.0-x86_64-unknown-linux-gnu /home/ubuntu/owner3m /home/ubuntu/reference3m 2>/dev/null || true
find /home/ubuntu /mnt -maxdepth 4 -type f \( -name canonical.f32 -o -name '*fineweb*' -o -name '*stop*.log' \) -printf '%p %s\n' 2>/dev/null | head -40
pgrep -af 'hierarchical_spann|supervise.py|cargo build|rustc|run-matched' || true
