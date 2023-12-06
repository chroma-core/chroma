mod assignment_policy;
pub mod index;
mod ingest_scheduler;
pub mod memberlist_provider;
pub mod rendezvous_hash;
mod writer;

pub mod chroma_proto {
    tonic::include_proto!("chroma");
}
pub(crate) trait Component {
    fn start(&mut self);
    fn stop(&mut self);
}
