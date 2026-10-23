#!/usr/bin/env bash
set -euo pipefail
# Run only after the caller verifies sandbox account and approved instance ID.
cd /mnt/spann/source
export CARGO_HOME=/mnt/spann/cargo
export RUSTUP_HOME=/mnt/spann/rustup
export CARGO_TARGET_DIR=/mnt/spann/target
export TMPDIR=/mnt/spann/tmp
mkdir -p "$CARGO_HOME" "$RUSTUP_HOME" "$CARGO_TARGET_DIR" "$TMPDIR" /mnt/spann/results
if ! test -x "$CARGO_HOME/bin/rustup"; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /mnt/spann/rustup-init.sh
  sh /mnt/spann/rustup-init.sh -y --no-modify-path --profile minimal --default-toolchain 1.92.0
fi
export PATH="$CARGO_HOME/bin:$PATH"
rustup toolchain install 1.92.0 --profile minimal
rustc +1.92.0 -Vv > /mnt/spann/results/compiler.txt
/usr/bin/time -v cargo +1.92.0 bench -p chroma-index --bench hierarchical_spann_shard --no-run --locked 2>&1 | tee /mnt/spann/results/compile.log
python3 - <<'PY'
import pathlib,shutil,hashlib,json,os,re
root=pathlib.Path('/mnt/spann');matches=re.findall(r'Executable.*\(([^)]+hierarchical_spann_shard-[^)]+)\)',(root/'results/compile.log').read_text());assert matches,matches;candidates=[pathlib.Path(matches[-1])]
binary=root/'hierarchical_spann_shard';temporary=root/'hierarchical_spann_shard.next';shutil.copy2(candidates[0],temporary);os.replace(temporary,binary)
(root/'results/binary.json').write_text(json.dumps({'path':str(binary),'sha256':hashlib.sha256(binary.read_bytes()).hexdigest()},indent=2)+'\n')
PY
