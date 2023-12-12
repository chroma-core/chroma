mod assignment_policy;
mod convert;
mod distributed_hnsw_segment;
pub mod index;
mod ingest_scheduler;
pub mod memberlist_provider;
mod messageid;
pub mod rendezvous_hash;
mod segment_manager;
mod storage;
mod sysdb;
mod types;
mod writer;

pub mod chroma_proto {
    tonic::include_proto!("chroma");
}

pub(crate) trait Component {
    fn start(&mut self);
    fn stop(&mut self);
}
