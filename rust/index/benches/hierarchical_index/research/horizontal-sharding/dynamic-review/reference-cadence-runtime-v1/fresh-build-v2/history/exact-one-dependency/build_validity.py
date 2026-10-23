"""Narrow extension for one explicitly pinned blockstore dependency change."""
import hashlib,json,os,time
from pathlib import Path
from build_validity_benchmark import refresh as benchmark_refresh, require_recompiled
DEPENDENCY='rust/blockstore/src/arrow/block/delta/single_column_storage.rs'
CONFIG=Path(__file__).with_name('cadence-source.json')
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def refresh(root,manifest,baseline):
 root=Path(root).resolve();contract=json.loads(CONFIG.read_text())
 if not contract.get('source_manifest_sha256')or not contract.get('changed_source_files'):raise ValueError('Dependency/source contract unfilled')
 if digest(root.parent/'source-manifest.json')!=contract['source_manifest_sha256']:raise ValueError('Source manifest differs from dependency contract')
 changed=sorted(name for name,row in manifest['files'].items()if row!=baseline['files'].get(name))
 if changed!=contract['changed_source_files']or DEPENDENCY not in changed:raise ValueError('Exact reviewed source/dependency delta differs')
 if any(not name.startswith('rust/index/benches/')and name!=DEPENDENCY for name in changed):raise ValueError('Unreviewed compiled dependency change')
 # The original validator remains byte-identical and verifies every file/symlink.
 # Mask only this admitted dependency in its baseline comparison, then refresh it too.
 masked=dict(baseline,files=dict(baseline['files']));masked['files'][DEPENDENCY]=manifest['files'][DEPENDENCY]
 result=benchmark_refresh(root,manifest,masked)
 dependency=root/DEPENDENCY;before=dependency.stat().st_mtime_ns;stamp=time.time_ns();os.utime(dependency,ns=(stamp,stamp))
 if digest(dependency)!=manifest['files'][DEPENDENCY]['sha256']:raise ValueError('Dependency changed during refresh')
 result['changed_source_files']=changed
 result['refreshed'].append({'path':DEPENDENCY,'before_mtime_ns':before,'after_mtime_ns':dependency.stat().st_mtime_ns,'sha256':manifest['files'][DEPENDENCY]['sha256']})
 result['scope']='Original immutable-byte validator plus one exact hash-bound dependency refresh; benchmark consumer entry also refreshed. No target deletion. Both blockstore and benchmark Cargo artifacts must independently report fresh=false.'
 return result
