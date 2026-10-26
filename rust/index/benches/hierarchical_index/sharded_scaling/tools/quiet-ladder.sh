#!/bin/bash
# Rerun the ladder benchmarks that carry latency and CPU figures on a quiet
# builder: the 1M, 10M, 30M and 100M grids, the 10M, 30M and 100M wide-beam
# grids, and the 100M internal-level cap sweep. The first ladder runs shared
# this machine with the build, the saves and each other, so their timings are
# noisy. These runs start after every save has exited and run one at a time;
# each records the load average before and after. Inputs, binaries, pinning
# and arguments match bench-ladder.sh, so recall and work counts should repeat
# the first runs exactly. Results go to reader-dev/runs/lad2q-*/0-A.
# Usage (on the builder): setsid nohup /data/quiet-ladder.sh > /data/quiet-ladder.log 2>&1 &
R=/home/ubuntu/spann-two-machine-v1/reader-dev
B=$R/bin/ladder-reader-1633b24ad2a7
COMMIT=1633b24ad
L=/data/ladder2
# Match the save script and writer by their command's start, so shells whose
# command text merely mentions them do not count as activity.
while pgrep -f "^(/bin/bash |bash )?/data/(ladder-save|bench-ladder)[a-z-]*\.sh|^/data/bin/ladder-writer" > /dev/null; do sleep 60; done
echo "== machine quiet $(date -u +%H:%M:%S) load $(cut -d' ' -f1-3 /proc/loadavg)"
pgrep -f "[c]pu_dma_latency" > /dev/null || sudo setsid nohup python3 -c "import os,struct,time; f=os.open('/dev/cpu_dma_latency', os.O_WRONLY); os.write(f, struct.pack('i', 10)); time.sleep(8*3600)" > /dev/null 2>&1 < /dev/null &
run_one() { # label rows config [extra args...]
  label=$1; n=$2; config=$3; shift 3
  d=$R/runs/$label
  if test -f $d/0-A/summary.json; then echo "== $label already done"; return; fi
  test -e $d && mv $d $d.failed-$(date +%s)
  mkdir -p $d/inputs
  cp /data/queries/dev1000.jsonl $d/inputs/queries.jsonl
  cp /data/gt/merged/dev-$n.exact.jsonl $d/inputs/groundtruth.jsonl
  cp /data/configs/$config $d/inputs/config.json
  cp $L/save-$n/storage.json $d/inputs/storage.json
  python3 -c "import json;print(json.dumps(json.load(open('$L/save-$n/owner1/result.json'))['saved_descriptor'],indent=2))" > $d/inputs/generation.json
  bin=$B
  test -f $L/parts-$n && bin=$R/bin/ladder-reader-c1eef3ca52c6
  argv=(taskset -c 0-27 $bin --mode bench --store $d/unused-store --storage-config $d/inputs/storage.json
    --descriptor $d/inputs/generation.json --expected-rows $n --vector-cache-bytes 51539607552
    --queries $d/inputs/queries.jsonl --groundtruth $d/inputs/groundtruth.jsonl --config $d/inputs/config.json
    --passes cold,warm --cold-threads 32 --output $d/0-A --vector-path /data/fineweb100m.f32
    --posting-cache-bytes=68719476736 --reader owners --receipt-dir /run/spann-reader-dev --full-vectors corpus
    --gt-leaf-map /run/spann-reader-dev/gt-leaf-map-lad2-$((n / 1000000))m.json "$@")
  # Sizes saved with one owner are read as two subtree parts, as in bench-ladder.sh.
  test -f $L/parts-$n && argv+=(--parts "$(cat $L/parts-$n)")
  load_before=$(cut -d' ' -f1-3 /proc/loadavg)
  echo "== $label $(date -u +%H:%M:%S) load $load_before"
  began=$(date +%s)
  SPANN_STORAGE_RUNTIME_WORKERS=16 "${argv[@]}" > $d/0-A.stdout 2> $d/0-A.stderr
  code=$?
  load_after=$(cut -d' ' -f1-3 /proc/loadavg)
  others=$(ps -eo pid,pcpu,comm --sort=-pcpu | awk 'NR>1 && $2>50' | grep -v ladder-reader | head -5 | tr '\n' ';')
  python3 -c "
import json,sys,subprocess
json.dump(dict(label='$label', rows=$n, binary='$bin', sha256=subprocess.run(['sha256sum','$bin'],capture_output=True,text=True).stdout.split()[0],
  commit='$COMMIT', argv=sys.argv[1:], started_unix=$began, seconds=$(date +%s)-$began, exit_code=$code,
  cpu_latency_hold_us=10, concurrent_work='none: quiet machine, one run at a time',
  load_before='$load_before', load_after='$load_after', other_busy_processes_after='$others'),
  open('$d/run.json','w'), indent=1)" "${argv[@]}"
  test $code = 0 || echo "!! $label exit $code"
}
for m in 1 10 30 100; do run_one lad2q-grid-${m}m $((m * 1000000)) phase0-grid.json; done
for m in 10 30 100; do run_one lad2q-wide-${m}m $((m * 1000000)) ladder-grid-wide.json; done
run_one lad2q-levelcap-100m 100000000 ladder-levelcap-100m.json
echo "== done $(date -u +%H:%M:%S)"
