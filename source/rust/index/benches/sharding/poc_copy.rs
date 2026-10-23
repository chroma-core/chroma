//! Count actual bytes copied into selected-owner POC checkpoints.
use anyhow::{ensure,Result};
use std::{fs::{self,File,OpenOptions},path::Path};
pub fn copy_tree(source:&Path,target:&Path,counts:&mut (u64,u64))->Result<()> {
    ensure!(!target.exists(),"fresh selected-owner copy required");fs::create_dir(target)?;
    for entry in fs::read_dir(source)? {
        let entry=entry?;let kind=entry.file_type()?;ensure!(!kind.is_symlink(),"POC input links unsupported");
        let destination=target.join(entry.file_name());
        if kind.is_dir(){copy_tree(&entry.path(),&destination,counts)?;}
        else {ensure!(kind.is_file(),"regular POC input required");
            let mut input=File::open(entry.path())?;let mut output=OpenOptions::new().write(true).create_new(true).open(destination)?;
            counts.0+=std::io::copy(&mut input,&mut output)?;counts.1+=1;
            ensure!(counts.0<=8*1024*1024 && counts.1<=2048,"observed selected-owner copy budget exceeded");
        }
    }Ok(())
}

#[cfg(test)]mod tests {
    use super::*;
    #[test]fn actual_copy_bytes_and_files_match_source(){
        let dir=tempfile::tempdir().unwrap();let source=dir.path().join("source");fs::create_dir(&source).unwrap();fs::create_dir(source.join("data")).unwrap();fs::write(source.join("manifest"),b"meta").unwrap();fs::write(source.join("data/block"),[0u8;32]).unwrap();let destination=dir.path().join("selected");let mut counts=(0,0);copy_tree(&source,&destination,&mut counts).unwrap();assert_eq!(counts,(36,2));assert_eq!(fs::read(destination.join("data/block")).unwrap(),[0u8;32]);assert_eq!(fs::read(source.join("manifest")).unwrap(),b"meta");
    }
}
