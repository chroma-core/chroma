#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/read-scoring-10m-merged-22eb704
while [[ ! -f "$r/exit-status" ]]; do sleep 10; done
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
for variant in baseline candidate; do
 source=/home/ubuntu/chroma-read-scoring-10m-check-$variant
 mkdir "$source"
 if [[ $variant == baseline ]]; then archive=/home/ubuntu/benchmark-data/wikipedia-10m-code.tar; else archive=/home/ubuntu/benchmark-data/read-scoring-10m-merged.tar; fi
 tar -xf "$archive" -C "$source"
 python3 - "$source" <<'PY'
from pathlib import Path
import sys
p=Path(sys.argv[1])/'rust/index/benches/hierarchical_spann_profile_quantized.rs'
s=p.read_text()
old='''                        checkpoint_queries
                            .par_iter()
                            .map(|gt| {'''
assert s.count(old)==1
s=s.replace(old,'''                        checkpoint_queries
                            .par_iter()
                            .enumerate()
                            .map(|(query_index, gt)| {''')
needle='''                                let r100 = recall_at_k(&result_ids, &gt.neighbors, 100);'''
assert s.count(needle)==1
s=s.replace(needle,'''                                let result_bits: Vec<_> = results.iter().map(|(id, score)| (*id, score.to_bits())).collect();
                                eprintln!("READ_RESULT phase={} query={} {:?}", phase, query_index, result_bits);
''' + needle)
p.write_text(s)
PY
 diff -u /home/ubuntu/chroma-wikipedia-10m/rust/index/benches/hierarchical_spann_profile_quantized.rs "$source/rust/index/benches/hierarchical_spann_profile_quantized.rs" > "$r/check-$variant.patch" || test "$?" = 1
 cd "$source"
 cargo clean -p chroma-index --release > "$r/check-$variant-clean.log" 2>&1
 cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$r/check-$variant-build.log" 2>&1
 binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
 cp "$binary" "$r/check-$variant-binary"
 export HOME="$r/home"
 export HF_HOME=/mnt/data/huggingface
 command=("$r/check-$variant-binary" --dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --threads 32 --balance-threads 32 --write-navigation fp --fp-npa --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 100 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false --resume --no-commit --save-dir /mnt/data/wikipedia-10m-scaling-snapshots-20261008/10000000)
 printf '%q ' "${command[@]}" > "$r/check-$variant.command"; printf '\n' >> "$r/check-$variant.command"
 /usr/bin/time -v -o "$r/check-$variant.time" "${command[@]}" > "$r/check-$variant.log" 2> "$r/check-$variant.stderr"
 echo "$(date -u +%FT%TZ) Correctness$variant complete"
done
sha256sum "$r/check-"*-binary > "$r/check-binaries.sha256"
python3 - "$r" <<'PY'
import ast,re,sys,json
from pathlib import Path
r=Path(sys.argv[1]); outputs=[]
for variant in ['baseline','candidate']:
 data={}
 for line in (r/f'check-{variant}.stderr').read_text().splitlines():
  m=re.fullmatch(r'READ_RESULT phase=(cold|warm) query=(\d+) (.*)',line)
  if m:
   key=(m[1],int(m[2])); assert key not in data
   data[key]=ast.literal_eval(m[3])
 assert len(data)==200
 assert all(len(v)==100 for v in data.values())
 outputs.append(data)
assert outputs[0]==outputs[1], 'ReturnedIDs, ordering or scorebits differ'
text='PASS: bothcold/warm passes,100queries perpass,100results each; all IDs, ordering and every float32 scorebit match on the same10M index.'
(r/'correctness-result.txt').write_text(text+'\n'); print(text)
PY
printf '%s\n' 0 > "$r/correctness-status"
