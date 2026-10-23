set -eu
sudo systemd-run --unit=owner3m-20261017-stop --on-calendar="2026-10-17 23:50:37 UTC" --timer-property=AccuracySec=1s /sbin/shutdown -h now
systemctl is-active owner3m-20261017-stop.timer
df -h /
free -h
