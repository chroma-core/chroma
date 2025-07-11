use std::str::FromStr;

use wal3::{FragmentSeqNo, Garbage, LogPosition, Manifest};

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 3 {
        eprintln!("USAGE: wal3-construct-garbage-from-manifest MANIFEST FRAG_SEQ_NO OFFSET");
        std::process::exit(13);
    }
    let manifest_json = std::fs::read_to_string(&args[0]).unwrap();
    let manifest: Manifest = serde_json::from_str(&manifest_json).unwrap();
    let seq_no = u64::from_str(&args[1]).unwrap();
    let offset = u64::from_str(&args[2]).unwrap();
    eprintln!("{manifest:#?}");
    eprintln!("seq_no: {seq_no:#?}");
    eprintln!("offset: {offset:#?}");
    let garbage = Garbage::bug_patch_construct_garbage_from_manifest(
        &manifest,
        FragmentSeqNo(seq_no),
        LogPosition::from_offset(offset),
    );
    println!("{}", serde_json::to_string(&garbage).unwrap());
}
