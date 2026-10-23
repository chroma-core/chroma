#!/bin/bash
set -euo pipefail
mkdir -p /mnt/spann/results /var/lib/spann-load-results
cat >/usr/local/sbin/spann-load-stop <<'STOP'
#!/bin/bash
cp -a /mnt/spann/results/. /var/lib/spann-load-results/ || true
sync
shutdown -h now
STOP
chmod +x /usr/local/sbin/spann-load-stop
printf '[Unit]\nDescription=Bounded sandbox load driver\n[Service]\nType=oneshot\nExecStart=/usr/local/sbin/spann-load-stop\n' >/etc/systemd/system/spann-load-stop.service
printf '[Timer]\nOnCalendar=2026-10-15 03:01:58 UTC\nPersistent=true\nUnit=spann-load-stop.service\n[Install]\nWantedBy=timers.target\n' >/etc/systemd/system/spann-load-stop.timer
systemctl daemon-reload
systemctl enable --now spann-load-stop.timer
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv awscli
chown -R ubuntu:ubuntu /mnt/spann
