#!/bin/bash
set -euo pipefail
# The fleet owner installs an independent absolute instance stop before this file.
GPU_ROOT=/mnt/spann
EVIDENCE="$GPU_ROOT/publication-bootstrap"
mkdir -p "$EVIDENCE"
trap 'status=$?; printf "%s\n" "$status" > "$EVIDENCE/exit-status.txt"' EXIT
cd "$GPU_ROOT/source"
nvidia-smi -q -x > "$EVIDENCE/nvidia-smi.xml"
python3 --version > "$EVIDENCE/python-version.txt"
# The isolated artifact-client stage installs and records python3.10-venv first.
python3.10 -m venv "$GPU_ROOT/publication-venv"
timeout --foreground 120 "$GPU_ROOT/publication-venv/bin/python" -m pip install --require-hashes --only-binary=:all: --index-url https://pypi.org/simple -r "$GPU_ROOT/gpu-pip-bootstrap.lock" > "$EVIDENCE/pip-bootstrap.log" 2>&1
export PIP_CACHE_DIR="$GPU_ROOT/package-cache"
timeout --foreground 600 "$GPU_ROOT/publication-venv/bin/python" -m pip install --only-binary=:all: --require-hashes --index-url https://download.pytorch.org/whl/cu124 --extra-index-url https://pypi.org/simple -r "$GPU_ROOT/publication-requirements.lock" --report "$EVIDENCE/pip-report.json" > "$EVIDENCE/install.log" 2>&1
"$GPU_ROOT/publication-venv/bin/python" -m pip freeze --all > "$EVIDENCE/pip-freeze.txt"
export CUBLAS_WORKSPACE_CONFIG=:4096:8
export OPENBLAS_NUM_THREADS=4
export OMP_NUM_THREADS=4
timeout --foreground 120 "$GPU_ROOT/publication-venv/bin/python" -m unittest tools.hierarchical_sharding.test_reference_bounds tools.hierarchical_sharding.test_reference_gpu tools.hierarchical_sharding.test_reference_device_filter tools.hierarchical_sharding.test_reference_device_engine tools.hierarchical_sharding.test_publication_queries tools.hierarchical_sharding.test_publication_reference tools.hierarchical_sharding.test_publication_gpu_job tools.hierarchical_sharding.test_publication_launch -v > "$EVIDENCE/cpu-gates.log" 2>&1
"$GPU_ROOT/publication-venv/bin/python" -m tools.hierarchical_sharding.publication_launch --config "$GPU_ROOT/publication-job.json" --output "$GPU_ROOT/publication-results" > "$EVIDENCE/job.log" 2>&1
