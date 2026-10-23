//! Local immutable FP32 input for the unchanged buffered reference writer.
use super::{Dataset,Query};
use chroma_distance::DistanceFunction;
use sha2::{Digest,Sha256};
use std::{fs::File,io::{self,Read,Seek,SeekFrom},path::{Path,PathBuf},sync::Arc};
use chroma_blockstore::{provider::BlockfileProvider,arrow::provider::BlockfileReaderOptions};
use uuid::Uuid;
#[derive(Clone)]
pub struct RetainedF32 {path:PathBuf,dim:usize,rows:usize,sha:String,name:String}
fn invalid(message:&str)->io::Error{io::Error::new(io::ErrorKind::InvalidData,message)}
impl RetainedF32 {
    pub fn open(path:&Path,dim:usize,rows:usize,expected_sha:&str)->io::Result<Self>{
        if dim==0||rows==0||rows>u32::MAX as usize||expected_sha.len()!=64||!expected_sha.bytes().all(|v|v.is_ascii_hexdigit()){return Err(invalid("retained input shape/hash"));}
        let bytes=rows.checked_mul(dim).and_then(|v|v.checked_mul(4)).ok_or_else(||invalid("retained input extent overflow"))?;
        let mut file=File::open(path)?;if file.metadata()?.len()!=bytes as u64{return Err(invalid("retained input exact byte length"));}
        let mut digest=Sha256::new();let mut buffer=[0u8;65536];let mut remaining=bytes;while remaining>0{let n=remaining.min(buffer.len());file.read_exact(&mut buffer[..n])?;for b in buffer[..n].chunks_exact(4){if !f32::from_le_bytes(b.try_into().unwrap()).is_finite(){return Err(invalid("nonfinite retained vector"));}}digest.update(&buffer[..n]);remaining-=n;}if file.read(&mut buffer[..1])?!=0{return Err(invalid("retained input length changed"));}
        let sha=format!("{:x}",digest.finalize());if sha!=expected_sha.to_ascii_lowercase(){return Err(invalid("retained input SHA256 mismatch"));}
        Ok(Self{path:path.to_owned(),dim,rows,name:format!("retained-f32-{dim}d-{sha}"),sha})
    }
    /// Hash saved bits with the dynamic convention: ID LE64 then FP bits LE32.
    pub async fn verify_saved_prefix(&self,provider:&BlockfileProvider,root:Uuid,count:usize)->io::Result<String>{
        if count>self.rows{return Err(invalid("saved prefix beyond retained input"));}
        let reader=provider.read::<u32,&[f32]>(BlockfileReaderOptions::new(root,String::new())).await.map_err(|e|invalid(&e.to_string()))?;
        let mut bits_digest=Sha256::new();let mut source_digest=Sha256::new();for start in (0..count).step_by(64){for(id,expected)in self.load_range(start,(count-start).min(64))?{let value=reader.get("embedding",id).await.map_err(|e|invalid(&e.to_string()))?.ok_or_else(||invalid("missing saved embedding"))?;if value.len()!=self.dim||!value.iter().zip(expected.iter()).all(|(a,b)|a.to_bits()==b.to_bits()){return Err(invalid("saved vector FP bits mismatch"));}bits_digest.update(u64::from(id).to_le_bytes());for v in value{bits_digest.update(v.to_bits().to_le_bytes());source_digest.update(v.to_bits().to_le_bytes());}}reader.clear_loaded_blocks();}
        if count==self.rows&&format!("{:x}",source_digest.finalize())!=self.sha{return Err(invalid("saved source digest mismatch"));}Ok(format!("{:x}",bits_digest.finalize()))
    }
}
impl Dataset for RetainedF32{
    fn name(&self)->&str{&self.name}fn dimension(&self)->usize{self.dim}fn data_len(&self)->usize{self.rows}fn k(&self)->usize{100}
    fn queries(&self,_:DistanceFunction)->io::Result<Vec<Query>>{Ok(Vec::new())}
    fn load_range(&self,offset:usize,limit:usize)->io::Result<Vec<(u32,Arc<[f32]>)>>{
        if offset.checked_add(limit).is_none_or(|v|v>self.rows){return Err(invalid("retained range outside input"));}
        let row_bytes=self.dim.checked_mul(4).ok_or_else(||invalid("retained row extent"))?;let mut file=File::open(&self.path)?;if file.metadata()?.len()!=self.rows.checked_mul(row_bytes).ok_or_else(||invalid("retained extent overflow"))? as u64{return Err(invalid("retained input length changed"));}file.seek(SeekFrom::Start(offset.checked_mul(row_bytes).ok_or_else(||invalid("retained offset overflow"))? as u64))?;
        let mut row=vec![0;row_bytes];let mut result=Vec::with_capacity(limit);let mut digest=Sha256::new();for index in offset..offset+limit{file.read_exact(&mut row)?;digest.update(&row);let values:Vec<_>=row.chunks_exact(4).map(|b|f32::from_le_bytes(b.try_into().unwrap())).collect();if !values.iter().all(|v|v.is_finite()){return Err(invalid("nonfinite consumed retained vector"));}result.push((index as u32,Arc::from(values)));}
        if offset==0&&limit==self.rows&&format!("{:x}",digest.finalize())!=self.sha{return Err(invalid("consumed retained hash changed"));}Ok(result)
    }
}
