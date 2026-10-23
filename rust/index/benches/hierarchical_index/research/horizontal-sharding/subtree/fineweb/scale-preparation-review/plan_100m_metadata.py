"""Bounded footer-only preflight. This never downloads vector columns or launches nodes."""
import argparse,json
from pathlib import Path
from tools.hierarchical_sharding.fineweb_source_plan import plan_prefix
from tools.hierarchical_sharding.fineweb_scale_job import validate_100m_plan
from tools.hierarchical_sharding.tree_manifest import canonical_bytes,artifact

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--manifest',required=True);p.add_argument('--output',required=True);a=p.parse_args()
 output=Path(a.output)
 if output.exists():raise FileExistsError(output)
 manifest=Path(a.manifest);plan=plan_prefix(json.loads(manifest.read_text()),100000000,max_source_bytes=128*1024**2,max_wall_s=300,max_files=512,max_column_read_bytes=256*1024**2)
 result=dict(source_manifest=artifact(manifest),plan=plan,admission=validate_100m_plan(plan),scope='Footer metadata only; no bulk corpus read, model fitting, or launch authorization.')
 with output.open('xb') as stream:stream.write(canonical_bytes(result))
 print(json.dumps(result['admission']))
if __name__=='__main__':main()
