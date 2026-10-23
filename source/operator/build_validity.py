"""Exact three blockstore refreshes plus one admitted new benchmark module."""
import hashlib,json,os,time
from pathlib import Path
from build_validity_benchmark import refresh as benchmark_refresh, require_recompiled
# Includes one exact new benchmark module; original baseline inventory stays immutable.
DEPENDENCIES=(
 'rust/index/benches/hierarchical_index/writer/stage_regroup.rs',
 'rust/blockstore/src/arrow/block/delta/single_column_storage.rs',
 'rust/blockstore/src/arrow/block/delta/ordered_block_delta.rs',
 'rust/blockstore/src/arrow/ordered_blockfile_writer.rs',
)
CONFIG=Path(__file__).with_name('cadence-source.json')
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def refresh(root,manifest,baseline):
 root=Path(root).resolve();contract=json.loads(CONFIG.read_text())
 if not contract.get('source_manifest_sha256')or not contract.get('changed_source_files'):raise ValueError('Dependency/source contract unfilled')
 if digest(root.parent/'source-manifest.json')!=contract['source_manifest_sha256']:raise ValueError('Source manifest differs from dependency contract')
 changed=sorted(name for name,row in manifest['files'].items()if row!=baseline['files'].get(name))
 if changed!=contract['changed_source_files']or not set(DEPENDENCIES).issubset(changed):raise ValueError('Exact reviewed source/dependency delta differs')
 if any(not name.startswith('rust/index/benches/')and name not in DEPENDENCIES for name in changed):raise ValueError('Unreviewed compiled dependency change')
 # The original validator stays byte-identical and verifies every file/symlink.
 # Mask only these admitted dependencies in its comparison, then refresh each too.
 masked=dict(baseline,files=dict(baseline['files']))
 for name in DEPENDENCIES:masked['files'][name]=manifest['files'][name]
 result=benchmark_refresh(root,manifest,masked)
 for name in DEPENDENCIES:
  dependency=root/name;before=dependency.stat().st_mtime_ns;stamp=time.time_ns();os.utime(dependency,ns=(stamp,stamp))
  if digest(dependency)!=manifest['files'][name]['sha256']:raise ValueError('Dependency changed during refresh')
  result['refreshed'].append({'path':name,'before_mtime_ns':before,'after_mtime_ns':dependency.stat().st_mtime_ns,'sha256':manifest['files'][name]['sha256']})
 result['changed_source_files']=changed
 result['scope']='Original immutable-byte validator plus three exact hash-bound blockstore refreshes and one new benchmark module refresh; benchmark consumer entry also refreshed. No target deletion. Both blockstore and benchmark Cargo artifacts must independently report fresh=false.'
 return result
