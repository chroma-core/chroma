#!/usr/bin/env bash
# Invoke on the freshly verified task worker before any archive transfer.
set -euo pipefail
: "${SPANN_DEADLINE_UNIX:?fixed local-window absolute epoch required}"
: "${SPANN_WINDOW_ID:?new simple lowercase window identifier required}"
[[ "$SPANN_WINDOW_ID" =~ ^[a-z0-9-]+$ ]]
[[ "$SPANN_DEADLINE_UNIX" =~ ^[0-9]+$ ]]
test "$(date +%s)" -lt "$SPANN_DEADLINE_UNIX"
token=$(curl --noproxy '*' -fsS --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
doc=$(curl --noproxy '*' -fsS --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/dynamic/instance-identity/document)
python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["accountId"]=="079819354890" and d["instanceId"]=="i-011ff180f03584f40" and d["region"]=="us-east-1"' <<< "$doc"
# This dedicated instance must have no active competing construction/build job.
if pgrep -af '(^|/)(cargo|rustc|single_shard_matched|hierarchical_spann_profile_quantized)( |$)' ; then
    echo 'Competing job found; do not schedule machine shutdown.' >&2
    exit 1
fi
unit="spann-single-shard-$SPANN_WINDOW_ID-stop"
sudo systemd-run --unit="$unit" --on-calendar="@$SPANN_DEADLINE_UNIX" --timer-property=AccuracySec=1s /sbin/shutdown -h now
sudo systemctl is-active --quiet "$unit.timer"
sudo systemctl show "$unit.timer" -p NextElapseUSecRealtime
printf '%s\n' "$doc"
# Closed historical timers must only be retired explicitly after this timer is active.
sudo systemctl list-timers --all --no-pager 'spann-*' '*owner3m*'
