//! A fixed-input POC view; bootstrap admission assumes every span immutable.
//! Reads are counted, but span hashes are not re-read as a production safety check.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap,fs::File,io::{Read,Seek,SeekFrom},path::PathBuf};

#[derive(Clone,Debug,Deserialize,Serialize)]
#[serde(deny_unknown_fields)]
pub struct Span {
    pub path:PathBuf,
    pub global_start:u64,
    pub rows:u64,
    pub source_offset_bytes:u64,
    pub bytes:u64,
    pub sha256:String,
}
#[derive(Clone,Debug,Deserialize,Serialize)]
#[serde(deny_unknown_fields)]
pub struct ViewSpec {
    pub assumption:String,
    pub dimension:usize,
    pub rows:u64,
    pub spans:Vec<Span>,
}
#[derive(Default,Debug,Serialize)]
pub struct ReadCounts {pub returned_bytes:u64,pub calls:u64}
pub struct View {pub spec:ViewSpec,pub reads:BTreeMap<PathBuf,ReadCounts>}
impl View {
    pub fn new(spec:ViewSpec)->Result<Self> {
        ensure!(spec.assumption=="fixed-immutable-input-poc" && (1..=32).contains(&spec.dimension) && (1..=128).contains(&spec.rows) && (1..=8).contains(&spec.spans.len()),"bounded fixed-input view required");
        let row_bytes=spec.dimension as u64*4;let mut next=0u64;
        for span in &spec.spans {
            ensure!(span.path.is_absolute() && span.global_start==next && span.rows>0 && span.bytes==span.rows.checked_mul(row_bytes).context("span overflow")? && span.source_offset_bytes%row_bytes==0,"view gap/overlap/length differs");
            ensure!(span.sha256.len()==64 && span.sha256.bytes().all(|b|b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),"span digest required");
            let end=span.source_offset_bytes.checked_add(span.bytes).context("span offset overflow")?;
            ensure!(span.path.metadata()?.is_file() && end<=span.path.metadata()?.len(),"span outside source");
            next=next.checked_add(span.rows).context("row overflow")?;
        }
        ensure!(next==spec.rows,"view complete row coverage differs");
        Ok(Self{spec,reads:BTreeMap::new()})
    }
    pub fn read_rows(&mut self,start:u64,count:u64)->Result<Vec<u8>> {
        let end=start.checked_add(count).context("read range overflow")?;
        ensure!(count>0 && end<=self.spec.rows,"read outside view");
        let row_bytes=self.spec.dimension as u64*4;
        let mut output=Vec::with_capacity((count*row_bytes) as usize);
        for span in &self.spec.spans {
            let a=start.max(span.global_start);let b=end.min(span.global_start+span.rows);
            if a>=b {continue;}
            let offset=span.source_offset_bytes+(a-span.global_start)*row_bytes;let size=(b-a)*row_bytes;
            let mut file=File::open(&span.path)?;file.seek(SeekFrom::Start(offset))?;
            let initial=output.len();output.resize(initial+size as usize,0);file.read_exact(&mut output[initial..])?;
            let counts=self.reads.entry(span.path.clone()).or_default();counts.calls+=1;counts.returned_bytes+=size;
        }
        ensure!(output.len()==(count*row_bytes) as usize,"short view read");Ok(output)
    }
}
/// Exact canonical-row source shared by legacy File and the opt-in segmented view.
pub trait CanonicalRows {
    fn canonical_row(&mut self,id:u64,dimension:usize)->Result<Vec<u8>>;
}
impl CanonicalRows for File {
    fn canonical_row(&mut self,id:u64,dimension:usize)->Result<Vec<u8>> {
        let row_bytes=dimension.checked_mul(4).context("row byte overflow")?;
        self.seek(SeekFrom::Start(id.checked_mul(row_bytes as u64).context("row offset overflow")?))?;
        let mut bytes=vec![0;row_bytes];self.read_exact(&mut bytes)?;Ok(bytes)
    }
}
impl CanonicalRows for View {
    fn canonical_row(&mut self,id:u64,dimension:usize)->Result<Vec<u8>> {
        ensure!(dimension==self.spec.dimension,"canonical dimension differs");self.read_rows(id,1)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn fixture()->(tempfile::TempDir,ViewSpec,Vec<u8>) {
        let dir=tempfile::tempdir().unwrap();let a=dir.path().join("old");let b=dir.path().join("tail");
        let bytes:Vec<_>=[1.0f32,-0.0,3.0,4.0].iter().flat_map(|x|x.to_le_bytes()).collect();
        std::fs::write(&a,&bytes[..8]).unwrap();std::fs::write(&b,&bytes[8..]).unwrap();
        let spec=ViewSpec{assumption:"fixed-immutable-input-poc".into(),dimension:1,rows:4,spans:vec![Span{path:a,global_start:0,rows:2,source_offset_bytes:0,bytes:8,sha256:"0".repeat(64)},Span{path:b,global_start:2,rows:2,source_offset_bytes:0,bytes:8,sha256:"1".repeat(64)}]};(dir,spec,bytes)
    }
    #[test]fn exact_cross_span_and_signed_zero(){let (_d,s,b)=fixture();let mut v=View::new(s).unwrap();assert_eq!(v.read_rows(1,2).unwrap(),b[4..12]);assert_eq!(v.read_rows(0,4).unwrap(),b);assert_eq!(v.reads.values().map(|x|x.returned_bytes).sum::<u64>(),24);}
    #[test]fn exact_schema_ranges_and_no_full_materialization(){let (_d,s,_)=fixture();for changed in 0..4 {let mut t=s.clone();match changed {0=>t.spans[1].global_start=1,1=>t.spans[1].global_start=3,2=>t.spans[0].bytes=7,_=>t.spans[0].source_offset_bytes=u64::MAX};assert!(View::new(t).is_err());}let mut v=View::new(s).unwrap();assert_eq!(v.read_rows(3,1).unwrap().len(),4);assert_eq!(v.reads.len(),1);assert!(v.read_rows(4,1).is_err());}
    #[test]fn actual_96_prefix_two_tail_boundary_bits(){
        let directory=tempfile::tempdir().unwrap();let prefix=directory.path().join("96");let tail=directory.path().join("2");
        let mut bytes=vec![0u8;98*32*4];for row in 0..98 {bytes[row*128..row*128+4].copy_from_slice(&(row as f32).to_le_bytes());}bytes[96*128+8..96*128+12].copy_from_slice(&(-0.0f32).to_le_bytes());
        std::fs::write(&prefix,&bytes[..96*128]).unwrap();std::fs::write(&tail,&bytes[96*128..]).unwrap();
        let spec=ViewSpec{assumption:"fixed-immutable-input-poc".into(),dimension:32,rows:98,spans:vec![Span{path:prefix,global_start:0,rows:96,source_offset_bytes:0,bytes:96*128,sha256:"0".repeat(64)},Span{path:tail,global_start:96,rows:2,source_offset_bytes:0,bytes:2*128,sha256:"1".repeat(64)}]};
        let mut view=View::new(spec).unwrap();assert_eq!(view.read_rows(95,2).unwrap(),bytes[95*128..97*128]);assert_eq!(view.reads.values().map(|v|v.returned_bytes).sum::<u64>(),256);assert_eq!(view.reads.len(),2);
    }

    #[test]fn legacy_file_and_segmented_row_bytes_are_identical(){
        let (directory,spec,bytes)=fixture();let full=directory.path().join("full");std::fs::write(&full,&bytes).unwrap();
        let mut file=File::open(full).unwrap();let mut view=View::new(spec).unwrap();
        for id in [3,0,1,2] {assert_eq!(file.canonical_row(id,1).unwrap(),view.canonical_row(id,1).unwrap());}
    }

}
