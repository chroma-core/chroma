"""Bind successful build artifacts to the controlled integration specification."""
import hashlib,json
from pathlib import Path
r=Path('/mnt/spann/global-runner-integration-v2')
def desc(p):
 b=p.read_bytes();return dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
pkg=json.loads((r/'package.json').read_text());build=json.loads((r/'build-spec.json').read_text());prov=json.loads((r/'build/provenance.json').read_text())
assert (r/'source/rust/types/idl').readlink()==Path('../../idl')
assert prov['source_manifest']==pkg['manifests']['source-artifacts.json']
files=json.loads((r/'source-artifacts.json').read_text())['files']
files += [desc(r/'source-artifacts.json'),prov['compiler'],prov['binary'],desc(r/'build/provenance.json')]
a=r/'implementation-artifacts.json';a.write_text(json.dumps(dict(format_version=1,files=files),indent=2)+'\n')
base={k:build[k] for k in ('deadline_unix','min_free_bytes','max_process_rss_bytes','max_log_bytes','guard_timer')}
base.update(threads=2,runner_directory=str(r/'runner'),integration_output=str(r/'integration'),implementation_checkout=str(r/'source'),implementation_artifacts=desc(a),build_provenance=desc(r/'build/provenance.json'),binary=prov['binary']['path'],binary_sha256=prov['binary']['sha256'],setup_artifacts=pkg['manifests']['setup-artifacts.json'],runner_artifacts=pkg['manifests']['runner-artifacts.json'],cwd=str(r/'source'))
with (r/'integration-base.json').open('x') as f:json.dump(base,f,indent=2)
print('Successful build bound to integration base')
