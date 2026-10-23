#!/bin/bash
set -euo pipefail
# Run only on the separately provisioned, capped sandbox GPU. Fleet owner sets
# the independent stop timer before invoking this script.
GPU_ROOT=/mnt/spann
mkdir -p "$GPU_ROOT/gpu-runtime-evidence"
cd "$GPU_ROOT/source"
nvidia-smi -q -x > "$GPU_ROOT/gpu-runtime-evidence/nvidia-smi.xml"
python3 --version > "$GPU_ROOT/gpu-runtime-evidence/python-version.txt"
python3 -m venv "$GPU_ROOT/gpu-venv"
timeout 900 "$GPU_ROOT/gpu-venv/bin/python" -m pip install --only-binary=:all: --require-hashes --index-url https://download.pytorch.org/whl/cu124 --extra-index-url https://pypi.org/simple -r "$GPU_ROOT/gpu-requirements.lock" --report "$GPU_ROOT/gpu-runtime-evidence/pip-report.json" > "$GPU_ROOT/gpu-runtime-evidence/install.log" 2>&1
"$GPU_ROOT/gpu-venv/bin/python" -m pip freeze > "$GPU_ROOT/gpu-runtime-evidence/pip-freeze.txt"
export CUBLAS_WORKSPACE_CONFIG=:4096:8
export OPENBLAS_NUM_THREADS=8
export OMP_NUM_THREADS=8
timeout 60 "$GPU_ROOT/gpu-venv/bin/python" -m unittest tools.hierarchical_sharding.test_reference_bounds tools.hierarchical_sharding.test_reference_gpu -v > "$GPU_ROOT/gpu-runtime-evidence/local-conformance-tests.log" 2>&1
timeout 600 "$GPU_ROOT/gpu-venv/bin/python" -m tools.hierarchical_sharding.reference_gpu --prepared "$GPU_ROOT/gpu-pilot-data/input.json" --queries "$GPU_ROOT/gpu-pilot-data/queries.jsonl" --expected "$GPU_ROOT/gpu-pilot-data/expected.jsonl" --output "$GPU_ROOT/gpu-pilot-result-v1" --corpus-block 32768 --query-block 32 > "$GPU_ROOT/gpu-runtime-evidence/pilot.log" 2>&1
