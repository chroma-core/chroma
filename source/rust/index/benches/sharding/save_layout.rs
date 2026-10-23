//! Generation-only experiment outputs preserve old files and the convenience head.
use std::{fs,io::{self,Write},path::{Path,PathBuf}};
const EXECUTION_FILES:[&str;5]=["construction-input.f32","construction-input.u64le","build-provenance.json","compactor-execution.json","write-plan-spec.json"];
pub struct ExecutionPaths {pub vectors:PathBuf,pub mapping:PathBuf,pub proof:PathBuf}
pub fn prepare(dir:&Path,previous:u64,generation:u64,shared:bool)->io::Result<ExecutionPaths>{
    let target=if shared {let path=dir.join(format!("generation-{generation}"));fs::create_dir(&path)?;path}else{
        for name in EXECUTION_FILES {let source=dir.join(name);if source.exists(){let retained=dir.join(format!("retained-{previous}-{name}"));if retained.exists(){return Err(io::Error::new(io::ErrorKind::AlreadyExists,"retained execution input collision"));}fs::rename(source,retained)?;}}
        dir.to_path_buf()
    };
    Ok(ExecutionPaths{vectors:target.join("construction-input.f32"),mapping:target.join("construction-input.u64le"),proof:target.join("compactor-execution.json")})
}
/// Create an immutable generation manifest without touching manifest.json.
/// This is a fixed-input POC save, not an atomic production publication API.
pub fn publish_generation(dir:&Path,generation:u64,bytes:&[u8])->io::Result<()> {
    let mut file=fs::OpenOptions::new().write(true).create_new(true).open(dir.join(format!("manifest-{generation}.json")))?;
    file.write_all(bytes)?;file.sync_all()?;fs::File::open(dir)?.sync_all()?;Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_paths_and_manifest_preserve_every_old_file(){
        let temp=tempfile::tempdir().unwrap();let dir=temp.path();
        for name in EXECUTION_FILES.into_iter().chain(["manifest.json","manifest-1.json"]){fs::write(dir.join(name),name).unwrap();}
        let paths=prepare(dir,1,2,true).unwrap();assert_eq!(paths.vectors,dir.join("generation-2/construction-input.f32"));assert_eq!(paths.mapping,dir.join("generation-2/construction-input.u64le"));assert_eq!(paths.proof,dir.join("generation-2/compactor-execution.json"));
        publish_generation(dir,2,b"new").unwrap();
        for name in EXECUTION_FILES.into_iter().chain(["manifest.json","manifest-1.json"]){assert_eq!(fs::read(dir.join(name)).unwrap(),name.as_bytes());}
        assert_eq!(fs::read(dir.join("manifest-2.json")).unwrap(),b"new");assert!(!dir.join("manifest.json.tmp").exists());
    }
    #[test]
    fn legacy_paths_keep_the_existing_retention_layout(){
        let temp=tempfile::tempdir().unwrap();let dir=temp.path();for name in EXECUTION_FILES{fs::write(dir.join(name),name).unwrap();}
        let paths=prepare(dir,1,2,false).unwrap();assert_eq!(paths.vectors,dir.join("construction-input.f32"));assert_eq!(paths.proof,dir.join("compactor-execution.json"));
        for name in EXECUTION_FILES{assert!(!dir.join(name).exists());assert_eq!(fs::read(dir.join(format!("retained-1-{name}"))).unwrap(),name.as_bytes());}
    }
    #[test]
    fn generation_manifest_never_overwrites_an_existing_generation(){
        let temp=tempfile::tempdir().unwrap();publish_generation(temp.path(),2,b"first").unwrap();assert!(publish_generation(temp.path(),2,b"other").is_err());assert_eq!(fs::read(temp.path().join("manifest-2.json")).unwrap(),b"first");
    }
}
