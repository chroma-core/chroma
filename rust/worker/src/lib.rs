use uuid::Uuid;

// Export index as a module
mod assignment_policy;
pub mod index;
pub mod memberlist_provider;
mod rendezvous_hash;
mod writer;

pub(crate) trait Component {
    fn start(&mut self);
    fn stop(&mut self);
}

// The following types mirror chroma.proto

// pub (crate) struct SubmitEmbeddingRecord {
//     pub (crate) id : Uuid,
//     pub (crate) embedding : Vec<f32>,
//     pub (crate) metadata :
// }
