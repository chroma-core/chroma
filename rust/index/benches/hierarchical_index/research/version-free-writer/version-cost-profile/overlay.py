from pathlib import Path
import difflib
r=Path('/mnt/msmarco/version-cost-profile-20261020');root=r/'instrumented';patch=[]
p=root/'rust/index/benches/build_wall_time.rs';before=p.read_text();helper='''
pub(crate) fn profile_mark(label: &str) {
    static ENABLED: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| std::env::var_os("VF_PROFILE_MARKERS").is_some());
    if *ENABLED {
        println!("VERSION_PROFILE_MARK unix={:.9} label={}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(),label);
    }
}
'''
after=before+helper;after=after.replace('        self.phase = phase;','        self.phase = phase;\n        profile_mark(LABELS[phase as usize]);');after=after.replace('        let elapsed = end.duration_since(self.start);','        profile_mark("BUILD_FINISHED");\n        let elapsed = end.duration_since(self.start);');p.write_text(after);patch.extend(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile=str(p.relative_to(root)),tofile=str(p.relative_to(root))))
p=root/'rust/index/benches/hierarchical_index/writer/persistence.rs';before=p.read_text();after=before
# Coarse elapsed intervals only; no per-ID clocks or counters.
def wrap(start,end,label):
 global after
 assert after.count(start)==1,start
 after=after.replace(start,'        crate::build_wall_time::profile_mark("'+label+'_BEGIN");\n'+start,1)
 assert after.count(end)==1,end
 after=after.replace(end,end+'\n        crate::build_wall_time::profile_mark("'+label+'_END");',1)
wrap('        let mut version_entries: Vec<(u32, u32)> = self','            .collect();\n        version_entries.sort_unstable_by_key(|(k, _)| *k);','VERSION_GATHER_SORT')
start='        for (data_id, version) in version_entries {';end='''                .set(PREFIX_VERSION, data_id, version)
                .await?;
        }''';wrap(start,end,'VERSION_UPSERT')
wrap('        let scalar_metadata_flusher = scalar_metadata_writer.commit::<u32, u32>().await?;','        let scalar_metadata_flusher = scalar_metadata_writer.commit::<u32, u32>().await?;','SCALAR_COMMIT')
wrap('        self.scalar_metadata_flusher.flush::<u32, u32>().await?;','        self.scalar_metadata_flusher.flush::<u32, u32>().await?;','SCALAR_FLUSH')
wrap('        self.dirty_versions.clear();','        self.dirty_versions.clear();','DIRTY_VERSIONS_CLEAR')
p.write_text(after);patch.extend(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile=str(p.relative_to(root)),tofile=str(p.relative_to(root))))
(r/'artifacts/profile-only.patch').write_text(''.join(patch))
