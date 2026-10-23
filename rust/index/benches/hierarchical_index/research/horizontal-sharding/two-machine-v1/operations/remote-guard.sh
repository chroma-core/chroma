#!/usr/bin/env bash
# Install before payload upload or execution on each freshly verified machine.
set -euo pipefail
: "${SPANN_DEADLINE_UNIX:?fixed absolute window deadline required}"
: "${SPANN_WINDOW_ID:?new lowercase window identifier required}"
: "${SPANN_INSTANCE_ID:?exact approved instance binding required}"
[[ "$SPANN_WINDOW_ID" =~ ^[a-z0-9-]+$ ]]
[[ "$SPANN_DEADLINE_UNIX" =~ ^[0-9]+$ ]]
case "$SPANN_INSTANCE_ID" in
    i-011ff180f03584f40|i-00268d1e1dfea0357) ;;
    *) exit 1 ;;
esac
now=$(date +%s)
test "$now" -lt "$SPANN_DEADLINE_UNIX"
test "$((SPANN_DEADLINE_UNIX-now))" -le 5400
token=$(curl --noproxy '*' -fsS --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
doc=$(curl --noproxy '*' -fsS --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document)
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["accountId"]=="079819354890" and d["instanceId"]==sys.argv[1] and d["region"]=="us-east-1"' "$SPANN_INSTANCE_ID" <<< "$doc"
if pgrep -af '(^|/)(cargo|rustc|single_shard_matched|single_shard_attribution|two_machine_spann|two_machine_correctness|two_machine_reader|perf|remote-run[.]py|materialize-input[.]py)( |$)'; then
    echo 'Competing task job found; stop before scheduling machine shutdown.' >&2
    exit 1
fi
unit="spann-two-machine-$SPANN_WINDOW_ID-stop"
sudo systemd-run --unit="$unit" --on-calendar="@$SPANN_DEADLINE_UNIX" --timer-property=AccuracySec=1s /sbin/shutdown -h now
sudo systemctl is-active --quiet "$unit.timer"
sudo systemctl show "$unit.timer" -p NextElapseUSecRealtime
printf '%s\n' "$doc"
sudo systemctl list-timers --all --no-pager 'spann-*' '*owner3m*'
