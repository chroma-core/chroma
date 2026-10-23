//! Bounded framing and in-process replay state shared by the actual writer CLI.
use anyhow::{bail, ensure, Result};
use serde::Deserialize;
use std::io::{BufRead, Read};

pub(super) const FRAME_BYTES: usize = 64 * 1024;
pub(super) const REPLY_BYTES: usize = 256 * 1024;
const MAX_COMMANDS: u64 = 256;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Frame {
    pub request_id: String, pub session_id: String, pub attempt_id: String,
    pub authorization: String, pub ownership_generation: u64,
    pub sequence: u64, pub row_id: Option<u64>, pub command: Command,
}
#[derive(Deserialize)]
#[serde(tag="op", rename_all="snake_case", deny_unknown_fields)]
pub(super) enum Command {
    Info {},
    Root {},
    Expand { parents: Vec<u32> },
    Count { level: usize, global_level_widths: Vec<usize>, sorted_distance_bits: Vec<u32> },
    Register { leaf: u32, local_id: u32, distance_bits: u32, route_sha256: String },
    Commit { routes: Vec<serde_json::Value> },
    Abort {},
}

pub(super) struct Scope<'a> {
    pub session_id: &'a str, pub attempt_id: &'a str,
    pub authorization: &'a str, pub ownership_generation: u64,
}
pub(super) enum Dispatch { New(Frame), Replay }
pub(super) struct Protocol<'a> {
    scope: Scope<'a>, expected: u64,
    previous: Option<(u64,Vec<u8>,Vec<u8>)>,
    pending: Option<(u64,Vec<u8>)>, failed: bool,
}
fn read_frame<R: BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    let mut data=Vec::new();
    let n=reader.take(FRAME_BYTES as u64+1).read_until(b'\n',&mut data)?;
    if n==0 { return Ok(None) }
    ensure!(n<=FRAME_BYTES && data.last()==Some(&b'\n'),"oversized or incomplete write frame");
    Ok(Some(data))
}
impl<'a> Protocol<'a> {
    pub fn new(scope:Scope<'a>)->Self {
        Self{scope,expected:1,previous:None,pending:None,failed:false}
    }
    pub fn fail(&mut self) { self.failed=true;self.pending=None; }
    pub fn next<R:BufRead>(&mut self,reader:&mut R)->Result<Option<Dispatch>> {
        let result:Result<Option<Dispatch>>=(|| {
            ensure!(!self.failed && self.pending.is_none(),"write protocol is failed or still dispatching");
            let Some(bytes)=read_frame(reader)? else {self.failed=true;return Ok(None)};
            let frame:Frame=serde_json::from_slice(&bytes)?;
            if let Some((sequence,prior,_))=&self.previous {
                if frame.sequence==*sequence {
                    ensure!(*prior==bytes,"conflicting write replay");
                    return Ok(Some(Dispatch::Replay));
                }
            }
            let scope=&self.scope;
            ensure!(frame.sequence==self.expected && self.expected<=MAX_COMMANDS
                && frame.session_id==scope.session_id && frame.attempt_id==scope.attempt_id
                && frame.authorization==scope.authorization && frame.ownership_generation==scope.ownership_generation,
                "write command scope/sequence differs");
            let row=frame.row_id.map(|id|id.to_string()).unwrap_or_else(||"all".into());
            ensure!(frame.request_id==format!("{}:{}:{}:{}",scope.session_id,scope.attempt_id,row,frame.sequence),"scoped request identity differs");
            self.pending=Some((frame.sequence,bytes));
            Ok(Some(Dispatch::New(frame)))
        })();
        if result.is_err() {self.fail()}
        result
    }
    pub fn finish(&mut self,reply:Vec<u8>)->Result<()> {
        let result:Result<()>=(|| {
            ensure!(!self.failed && reply.len()<REPLY_BYTES,"failed protocol or oversized write reply");
            let (sequence,bytes)=self.pending.take().ok_or_else(||anyhow::anyhow!("no command awaiting reply"))?;
            self.previous=Some((sequence,bytes,reply));self.expected+=1;Ok(())
        })();
        if result.is_err(){self.fail()}
        result
    }
    pub fn cached_reply(&self)->Result<&[u8]> {
        ensure!(!self.failed && self.pending.is_none(),"write protocol cannot replay now");
        match &self.previous {Some((_,_,reply))=>Ok(reply),None=>bail!("no previous write reply")}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    fn protocol()->Protocol<'static>{Protocol::new(Scope{session_id:"s",attempt_id:"a",authorization:"auth",ownership_generation:7})}
    fn bytes(sequence:u64)->Vec<u8>{
        let mut b=serde_json::to_vec(&serde_json::json!({"request_id":format!("s:a:all:{sequence}"),"session_id":"s","attempt_id":"a","authorization":"auth","ownership_generation":7,"sequence":sequence,"row_id":null,"command":{"op":"info"}})).unwrap();b.push(b'\n');b
    }
    fn next(p:&mut Protocol<'_>,b:Vec<u8>)->Result<Option<Dispatch>>{p.next(&mut Cursor::new(b))}
    #[test] fn framing_rejects_truncation_oversize_and_eof_is_terminal(){
        for b in [b"{}".to_vec(),vec![b'x';FRAME_BYTES+1],vec![b'\n'],vec![0xff,b'\n']] {
            let mut p=protocol();assert!(next(&mut p,b).is_err());assert!(next(&mut p,bytes(1)).is_err());
        }
        let mut p=protocol();assert!(next(&mut p,vec![]).unwrap().is_none());assert!(next(&mut p,bytes(1)).is_err());
        let mut exact=vec![b' ';FRAME_BYTES-3];exact.extend_from_slice(b"{}\n");assert_eq!(read_frame(&mut Cursor::new(exact)).unwrap().unwrap().len(),FRAME_BYTES);
    }
    #[test] fn real_serde_rejects_unknown_fields_boolean_ids_and_overflow(){
        for (path,value) in [("extra",serde_json::json!(1)),("sequence",serde_json::json!(true)),("ownership_generation",serde_json::json!(1e30f64))] {
            let mut v:serde_json::Value=serde_json::from_slice(&bytes(1)).unwrap();v[path]=value;let mut p=protocol();assert!(next(&mut p,serde_json::to_vec(&v).unwrap().into_iter().chain([b'\n']).collect()).is_err());
        }
        let overflow=String::from_utf8(bytes(1)).unwrap().replace("\"ownership_generation\":7","\"ownership_generation\":18446744073709551616");
        assert!(next(&mut protocol(),overflow.into_bytes()).is_err());
        for command in [serde_json::json!({"op":"register","leaf":true,"local_id":1,"distance_bits":0,"route_sha256":"x"}),serde_json::json!({"op":"expand","parents":[4294967296u64]}),serde_json::json!({"op":"info","unknown":1}),serde_json::json!({"op":"root","unknown":1}),serde_json::json!({"op":"abort","unknown":1})] {
            let mut v:serde_json::Value=serde_json::from_slice(&bytes(1)).unwrap();v["command"]=command;let mut p=protocol();assert!(next(&mut p,serde_json::to_vec(&v).unwrap().into_iter().chain([b'\n']).collect()).is_err());
        }
    }
    #[test] fn scope_epoch_request_identity_and_sequence_errors_poison(){
        for (field,value) in [("session_id",serde_json::json!("other")),("attempt_id",serde_json::json!("other")),("authorization",serde_json::json!("other")),("ownership_generation",serde_json::json!(8)),("request_id",serde_json::json!("other")),("sequence",serde_json::json!(2))] {
            let mut v:serde_json::Value=serde_json::from_slice(&bytes(1)).unwrap();v[field]=value;let mut p=protocol();assert!(next(&mut p,serde_json::to_vec(&v).unwrap().into_iter().chain([b'\n']).collect()).is_err());assert!(next(&mut p,bytes(1)).is_err());
        }
    }
    #[test] fn exact_last_bytes_replay_without_second_dispatch(){
        let mut p=protocol();let mut calls=0;
        for _ in 0..2 {match next(&mut p,bytes(1)).unwrap().unwrap(){Dispatch::New(_)=>{calls+=1;p.finish(b"same reply".to_vec()).unwrap()},Dispatch::Replay=>assert_eq!(p.cached_reply().unwrap(),b"same reply")}}
        assert_eq!(calls,1);
        let mut changed=bytes(1);changed.insert(0,b' ');assert!(next(&mut p,changed).is_err());assert!(p.cached_reply().is_err());
    }
    #[test] fn older_replay_and_dispatch_error_cannot_resume(){
        let mut p=protocol();for i in 1..=2 {assert!(matches!(next(&mut p,bytes(i)).unwrap(),Some(Dispatch::New(_))));p.finish(vec![]).unwrap()}
        assert!(next(&mut p,bytes(1)).is_err());
        let mut p=protocol();assert!(matches!(next(&mut p,bytes(1)).unwrap(),Some(Dispatch::New(_))));p.fail();assert!(p.finish(vec![]).is_err());assert!(next(&mut p,bytes(1)).is_err());
    }
    #[test] fn pending_dispatch_command_count_and_reply_bounds_are_enforced(){
        let mut p=protocol();next(&mut p,bytes(1)).unwrap();assert!(next(&mut p,bytes(1)).is_err());
        let mut p=protocol();for i in 1..=MAX_COMMANDS {next(&mut p,bytes(i)).unwrap();p.finish(vec![]).unwrap()};assert!(matches!(next(&mut p,bytes(MAX_COMMANDS)).unwrap(),Some(Dispatch::Replay)));assert!(next(&mut p,bytes(MAX_COMMANDS+1)).is_err());
        let mut p=protocol();next(&mut p,bytes(1)).unwrap();assert!(p.finish(vec![0;REPLY_BYTES]).is_err());assert!(next(&mut p,bytes(1)).is_err());
        let mut p=protocol();next(&mut p,bytes(1)).unwrap();p.finish(vec![0;REPLY_BYTES-1]).unwrap();assert_eq!(p.cached_reply().unwrap().len(),REPLY_BYTES-1);
    }
    #[test] fn valid_register_fields_and_multiple_frames_use_actual_parser(){
        let mut v:serde_json::Value=serde_json::from_slice(&bytes(1)).unwrap();v["row_id"]=serde_json::json!(18);v["request_id"]=serde_json::json!("s:a:18:1");v["command"]=serde_json::json!({"op":"register","leaf":4,"local_id":13,"distance_bits":0x3d800000u32,"route_sha256":"a".repeat(64)});
        let mut raw=serde_json::to_vec(&v).unwrap();raw.push(b'\n');raw.extend(bytes(2));let mut input=Cursor::new(raw);let mut p=protocol();
        match p.next(&mut input).unwrap().unwrap(){Dispatch::New(Frame{command:Command::Register{leaf,local_id,distance_bits,..},..})=>assert_eq!((leaf,local_id,distance_bits),(4,13,0x3d800000)),_=>panic!("actual register was not decoded")};p.finish(vec![]).unwrap();assert!(matches!(p.next(&mut input).unwrap(),Some(Dispatch::New(Frame{command:Command::Info{},..}))));
    }
}
