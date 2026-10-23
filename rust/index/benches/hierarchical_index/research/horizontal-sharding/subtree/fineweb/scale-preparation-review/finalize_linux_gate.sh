#!/bin/bash
set -euo pipefail
ROOT=/mnt/spann/prep100m-linux-gate-v3
cd "$ROOT/runtime/source"
"$ROOT/runtime/venv/bin/python" - "$ROOT" <<'PY'
from pathlib import Path
import json,sys,time,hashlib
from tools.hierarchical_sharding.resource_guard import inventory
root=Path(sys.argv[1]);report=json.loads((root/'control/guard-output/report.json').read_text());assert report['status']=='passed' and all(o['status']=='ok' and not o.get('unreconciled_children') for o in report['outcomes']);scopes=[dict(path=str(root),role='entire-copied-runtime-source-cache-fixtures-control')];final=inventory(scopes,1000000);assert not [p for p in root.rglob('*') if p.is_symlink()]
(root/'control/result.json').write_text(json.dumps(dict(status='passed',finished_unix=time.time(),guard_report_sha256=hashlib.sha256((root/'control/guard-output/report.json').read_bytes()).hexdigest(),runtime=json.loads((root/'provenance/runtime-proof.json').read_text()),final_inventory=final,scope='Installed hash-pinned CPython3.12 copied-venv; all CPU and real Linux process-cleanup tests; actual21-row preparation/reference and version-pinned archive/handoff fixtures run under a strict whole-root guard. The archive unit fixture uses an in-memory store; live large-object SDK/role restoration is separately proven by the preserved real10M archive.'),indent=2));print(json.dumps(dict(status='passed',files=final['inventory_entries'],logical_bytes=final['total_unique_inode_bytes'])))
PY
