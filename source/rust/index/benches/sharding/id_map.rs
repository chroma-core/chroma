//! Immutable row-to-external-ID maps; vector data remains streamed separately.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdMapping {
    pub path: String,
    pub sha256: String,
    pub count: u64,
    pub bytes: u64,
}
pub struct PreparedMap {
    pub descriptor: IdMapping,
    file: tempfile::NamedTempFile,
    destination: PathBuf,
}
impl PreparedMap {
    pub fn publish(self) -> Result<IdMapping> {
        self.file
            .persist_noclobber(&self.destination)
            .map_err(|e| e.error)?;
        Ok(self.descriptor)
    }
}
pub fn prepare(
    input: Option<&Path>,
    old: Option<&IdMapping>,
    old_count: u64,
    total: u64,
    dir: &Path,
    generation: u64,
) -> Result<Option<PreparedMap>> {
    let Some(input) = input else {
        ensure!(
            old.is_none(),
            "explicit ID map is required when resuming mapped IDs"
        );
        return Ok(None);
    };
    ensure!(
        old_count == 0 || old.is_some(),
        "cannot change from strided IDs to an explicit map on resume"
    );
    let mut source = File::open(input)?;
    let bytes = total.checked_mul(8).context("ID map length overflow")?;
    ensure!(
        source.metadata()?.len() == bytes,
        "ID map must contain the full target row mapping (old prefix and appended rows)"
    );
    let mut file = tempfile::NamedTempFile::new_in(dir)?;
    let mut hash = Sha256::new();
    let mut prefix = Sha256::new();
    let mut copied = 0u64;
    let mut buffer = vec![0; 1024 * 1024];
    loop {
        let n = source.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        let prefix_remaining = (old_count * 8).saturating_sub(copied).min(n as u64) as usize;
        prefix.update(&buffer[..prefix_remaining]);
        file.write_all(&buffer[..n])?;
        copied += n as u64;
    }
    ensure!(copied == bytes, "ID map changed while copying");
    if let Some(old) = old {
        ensure!(
            old.count == old_count && format!("{:x}", prefix.finalize()) == old.sha256,
            "existing ID map prefix must remain unchanged"
        );
    }
    file.as_file().sync_all()?;
    let name = format!("id-map-{generation}.u64le");
    let descriptor = IdMapping {
        path: name.clone(),
        sha256: format!("{:x}", hash.finalize()),
        count: total,
        bytes,
    };
    Ok(Some(PreparedMap {
        descriptor,
        file,
        destination: dir.join(name),
    }))
}
pub struct IdLookup {
    file: File,
    count: u64,
}
impl IdLookup {
    pub fn open(dir: &Path, map: &IdMapping) -> Result<Self> {
        let path = Path::new(&map.path);
        ensure!(
            !path.is_absolute() && path.components().count() == 1,
            "ID map path must be one local filename"
        );
        let mut file = File::open(dir.join(path))?;
        ensure!(
            map.bytes == map.count.checked_mul(8).context("ID map count overflow")?
                && file.metadata()?.len() == map.bytes,
            "ID map length mismatch"
        );
        let mut hash = Sha256::new();
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        ensure!(
            format!("{:x}", hash.finalize()) == map.sha256,
            "ID map checksum mismatch"
        );
        Ok(Self {
            file,
            count: map.count,
        })
    }
    pub fn get(&mut self, id: u32) -> Result<u64> {
        ensure!(u64::from(id) < self.count, "local ID exceeds persisted map");
        self.file.seek(SeekFrom::Start(u64::from(id) * 8))?;
        let mut bytes = [0; 8];
        self.file.read_exact(&mut bytes)?;
        Ok(u64::from_le_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn write(path: &Path, ids: &[u64]) {
        let mut file = File::create(path).unwrap();
        for id in ids {
            file.write_all(&id.to_le_bytes()).unwrap();
        }
    }
    #[test]
    fn mapping_is_immutable_and_append_requires_the_exact_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("input");
        write(&input, &[100, 7]);
        let first = prepare(Some(&input), None, 0, 2, dir.path(), 1)
            .unwrap()
            .unwrap()
            .publish()
            .unwrap();
        let mut lookup = IdLookup::open(dir.path(), &first).unwrap();
        assert_eq!(lookup.get(0).unwrap(), 100);
        assert_eq!(lookup.get(1).unwrap(), 7);
        write(&input, &[100, 8, 44]);
        assert!(prepare(Some(&input), Some(&first), 2, 3, dir.path(), 2).is_err());
        write(&input, &[100, 7, 44]);
        let next = prepare(Some(&input), Some(&first), 2, 3, dir.path(), 2)
            .unwrap()
            .unwrap()
            .publish()
            .unwrap();
        assert_eq!(
            IdLookup::open(dir.path(), &next).unwrap().get(2).unwrap(),
            44
        );
        assert_eq!(
            IdLookup::open(dir.path(), &first).unwrap().get(0).unwrap(),
            100
        );
        assert!(prepare(None, Some(&first), 2, 3, dir.path(), 3).is_err());
    }
}
