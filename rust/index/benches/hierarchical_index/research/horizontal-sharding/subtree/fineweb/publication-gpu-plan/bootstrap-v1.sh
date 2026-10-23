#!/bin/bash
set -euo pipefail
mkdir -p /mnt/spann
cat > /mnt/spann/deadline.json <<'JSON'
{"started_epoch": 1792026008.833053, "stop_epoch": 1792029608.833053, "stop_utc": "2026-10-15T02:00:08.833053+00:00"}
JSON
cat > /etc/systemd/system/spann-publication-stop.service <<'UNIT'
[Unit]
Description=Absolute bounded sandbox GPU stop
[Service]
Type=oneshot
ExecStart=/sbin/shutdown -h now
UNIT
cat > /etc/systemd/system/spann-publication-stop.timer <<'UNIT'
[Unit]
Description=Absolute one-hour sandbox GPU deadline
[Timer]
OnCalendar=2026-10-15 02:00:08 UTC
AccuracySec=1s
Persistent=true
Unit=spann-publication-stop.service
[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable --now spann-publication-stop.timer
touch /mnt/spann/bootstrap-ready
